import boto3
import json
import time
import requests
import os
from requests_aws4auth import AWS4Auth


# CONST:
service = 'es' # for AWS4Auth


# MAIN VARS:
region = os.environ['AWS_REGION']
table_name = os.environ['DDB_table_name']
elastic_domain_url = os.environ['ES_domain_url']


# GLOBAL context:
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


# FUNCTIONS:
def check_ddb_table_exist(table_name):
    client_ddb = boto3.client('dynamodb')
    try:
        response = client_ddb.describe_table(TableName=table_name)
        print(f'check_ddb_table_exist: dynamo table {table_name} exists')
        return True
    except:
        print(f'check_ddb_table_exist: dynamo table {table_name} does NOT exists')
        return False


def create_ddb_table(table_name):
    client_ddb = boto3.client('dynamodb')
    waiter = client_ddb.get_waiter('table_exists')
    params = {
        'TableName' : table_name,
        'KeySchema': [
            { 'AttributeName': "client_id", 'KeyType': "HASH"}    # Partition key
        ],
        'AttributeDefinitions': [
            { 'AttributeName': "client_id", 'AttributeType': "S" }
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    }
    try:
        response = client_ddb.create_table(**params)
        waiter.wait(
            TableName=table_name,
            WaiterConfig={
                'Delay': 3,
                'MaxAttempts': 15
            }
        )
        print(f'create_ddb_table: table {table_name} created in DynamoDB')
        return True
    except Exception as e:
        print(f'create_ddb_table: FAILED to create {table_name} in DynamoDB with exception {e}')
        return False


def get_uniq_ids_keepalive(host_url):
    # set vars:
    host_url_int = host_url+"/keepalive*/_search"
    # get_query:
    query = {
      "size":0,
      "query" : {
        "range" : {
          "machineData.machineTimeUTC" : {
            "gte" : "now-12h"
          }
        }
      },
      "aggs": {
        "one" : {
          "terms": {
            "field": "machineData.id.keyword",
            "size": 999999
          }
        }
      }
    }
    # ES 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    # Make the signed HTTP request
    response = requests.get(host_url_int, auth=awsauth, headers=headers, data=json.dumps(query))
    new_dict = response.json()
    id_list = []
    for each in (response.json())['aggregations']['one']['buckets']:
        id_list.append(each['key'])
    print(f'get_uniq_ids_keepalive: list of uniq id-s is retrived from {host_url}')
    return id_list


def get_names_for_ids(ids_list, host_url):
    int_ids_list = ids_list
    query_string = ""
    return_obj = {}

    host_url_int = host_url+"/keepalive*/_msearch"

    for each_id in int_ids_list:
        query_string += query_string + '{}\n{"size": 1, "query": { "match" : { "machineData.id.keyword" : "'+ str(each_id) +'"} } }\n'

    headers = { "Content-Type": "application/x-ndjson" }
    response = requests.get(host_url_int, auth=awsauth, headers=headers, data=query_string)
    ##### Check - response may be empty, add TRY-CATCH (if not items to add to ddb) ##################################################
    list_multiquery_responses = (response.json())['responses']
    # return list_multiquery_responses

    for each in list_multiquery_responses:
        current_id          = each['hits']['hits'][0]['_source']['machineData']['id']
        current_name        = each['hits']['hits'][0]['_source']['machineData']['name']
        current_callcentername = each['hits']['hits'][0]['_source']['machineData']['callCenterName']

        return_obj[current_id] = {
            "client_id"     : current_id,
            "client_name"   : current_name,
            "client_callcentername" : current_callcentername
        }

    if len(int_ids_list) != len(return_obj):
        print(f'get_names_for_ids: WARN length of output not equal to input. Some ids (queries) may be losted or skipped.')

    print(f'get_names_for_ids: return collected names and callcenter names for provide ID list')
    return return_obj


def put_uniq_ids_to_table(id_dict, table_name):
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    id_dict_int = id_dict
    id_processing_errors = []
    # current_time = str(time.time())
    for each in id_dict_int:
        try:
            response = table.put_item(
                Item = {
                    'client_id'             : id_dict_int[each]['client_id'],
                    'client_name'           : id_dict_int[each]['client_name'],
                    'client_callcentername' : id_dict_int[each]['client_callcentername']
                }
            )
        except Exception as e:
            id_processing_errors.append({'item': each, 'exception': e})
    if len(id_processing_errors) == 0:
        print(f'put_uniq_ids_to_table: new uniq id-s list writed to DynamoDB table {table_name}')
        return True
    else:
        print(f'put_uniq_ids_to_table: FAILED write uniq id-s to DynamoDB table {table_name} with exception: {id_processing_errors}')
        return id_processing_errors


def get_user_list_from_ddb(table_name):
    client = boto3.client('dynamodb')

    response = client.scan(
        TableName=table_name,
        ReturnConsumedCapacity='TOTAL'
    )
    print(f'get_user_list_from_ddb: return raw list of all records from DynamoDB table {table_name}')
    return response

# CHECK IT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def ddb_client_list_parser(ddb_raw_list):
    new_array = []
    for each in ddb_raw_list['Items']:
        new_array.append(each['client_id']['S'])
    print(f'ddb_client_list_parser: return formatted dict from raw list of clients from DynamoDB')
    return new_array

def list_add_to_ddb(list_ddb, list_es):
    print(f'list_add_to_ddb: return new items from ES which are still not in DynamoDB table')
    return [item for item in list_es if item not in list_ddb]

# Lambda execution starts here
def lambda_handler(event, context):

    try:
        invoked_method = event['invoke_type']
    except Exception as e:
        invoked_method = None

    if not check_ddb_table_exist(table_name):
        create_ddb_table(table_name)
        time.sleep(7)

    # print(f'Event call: {event["invoke_type"]} and invoked_method: {invoked_method} and they are equal { ( event["invoke_type"] == invoked_method ) }')
    # print("SOME TEST LOGS")

    if invoked_method == "update_ids_list":
        print(f'List on new id-s to add to dynamodb table: { event["id_list"] }')
        put_uniq_ids_to_table( event["id_list"], table_name)

    elif invoked_method == "init":
        if not check_ddb_table_exist(table_name):
            create_ddb_table(table_name)
            time.sleep(7)

        full01 = ddb_client_list_parser(get_user_list_from_ddb(table_name))
        full02 = get_uniq_ids_keepalive(elastic_domain_url)
        full03 = list_add_to_ddb(full01, full02)

        put_uniq_ids_to_table(full03, table_name)

    else:
        if not check_ddb_table_exist(table_name):
            create_ddb_table(table_name)
            time.sleep(7)

        full01 = ddb_client_list_parser(get_user_list_from_ddb(table_name))
        full02 = get_uniq_ids_keepalive(elastic_domain_url)
        full03 = list_add_to_ddb(full01, full02)
        if len(full03) == 0:
            return {"invoked method": invoked_method, "result": True, "event": event, "collected_ids_full03": full03}
        # check if response is empty! ################################################
        full04 = get_names_for_ids(full03, elastic_domain_url)

        put_uniq_ids_to_table(full04, table_name)

    return {"invoked method": invoked_method, "result": True, "event": event, "collected_ids_full03": full03, "collected_names_full04": full04}