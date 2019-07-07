import boto3
import json
import datetime
import time
import requests
import os
import logging
from requests_aws4auth import AWS4Auth

### CONST:
service = 'es' # for AWS4Auth

### MAIN VARS:
region = os.environ['AWS_REGION']
table_name = os.environ['DDB_table_name']
elastic_url = os.environ['ES_domain_url']
support_func_name = os.environ['support_func_name']
environment = os.environ['current_environment']
slack_channel_notify = os.environ['slack_notification_url']
log_level = os.environ['func_log_level']
es_index_prefix = os.environ['es_check_index_prefix']

### CONST:
big_time_delta = datetime.timedelta(hours=12)
small_time_delta = datetime.timedelta(seconds=300)
still_dead_alert_interval = datetime.timedelta(seconds=1800)
count_compare_number = 15 # num of keepalive messages in time interval

### GLOBAL context:
credentials = boto3.Session().get_credentials() # Get AWS credentials for services authorization 
awsauth = AWS4Auth(
    credentials.access_key, 
    credentials.secret_key, 
    region, 
    service, 
    session_token=credentials.token)
logger = logging.getLogger() # Set up logger for all execution:
logger.setLevel(log_level) 


### FUNCTIONS level 01:
def get_current_time_01():
    return datetime.datetime.utcnow()

def str_to_time_01(string):
    return datetime.datetime.strptime(string,'%Y-%m-%dT%H:%M:%S.%fZ')

def time_to_str_01(datetime_object):
    return datetime_object.strftime('%Y-%m-%dT%H:%M:%S.000Z')

def check_ddb_table_exist_01(table_name):
    client_ddb = boto3.client('dynamodb')
    try:
        response = client_ddb.describe_table(TableName=table_name)
        logger.info(f'check_ddb_table_exist_01: Table [{table_name}] exists')
        return True
    except:
        logger.info(f'check_ddb_table_exist_01: Table [{table_name}] does NOT exist')
        return False

def check_es_index_exists_01(es_url, index_name):
    full_url = f'{es_url}/{index_name}'
    # ES 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    # Make the signed HTTP request
    response = requests.get(full_url, auth=awsauth, headers=headers)
    if response.status_code == 404:
        logger.info(' '.join((f'check_es_index_exists_01: Return elastic index [{index_name}]',
            f'DOES NOT exist in elasticsearch cluster'
            )))
        return False
    else:
        logger.info(' '.join((f'check_es_index_exists_01: Return elastic index [{index_name}]',
            f'exists in elasticsearch cluster'
            )))
        return True

def invoke_support_func_01(json_load,asynccall=False):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.invoke
    logger.debug(' '.join((f'invoke_support_func_01: called with payload [{json_load}]',
        f'and asynccall as [{asynccall}]'
        )))
    invoketype = 'Event' if asynccall else 'RequestResponse'
    # if asynccall: 
    #     invoketype = 'Event' # asyncronous call
    # else:
    #     invoketype = 'RequestResponse' # sync call 
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName    = support_func_name,
        InvocationType  = invoketype,
        Payload         = json.dumps(json_load)
    )
    logger.info(' '.join((f'invoke_support_func_01: Return payload from invoked',
        f'support lambda function [{support_func_name}]'
        )))
    return response['StatusCode']
    # return json.loads(response['Payload'].read().decode())

def generate_invoke_payload_01(invoke_target):
    """ Function returns json format payload to call support lambda function """
    allowed_values = (
        'init',
        'update',
        'index_create',
        'id_list_update'
    )
    if invoke_target in allowed_values:
        logger.info(' '.join((f'generate_invoke_payload_01: Return [{invoke_target}] \
            as agrument to call support function'
            )))
        return { "why_call_me" : invoke_target }
    else:
        logger.error(' '.join((f'generate_invoke_payload_01: argument for support',
            f'function [{invoke_target}] is not supported. Allowed values [{allowed_values}]'
            )))
        raise Exception(' '.join((f'generate_invoke_payload_01: Called with wrong',
            f'argument [{invoke_target}]; allowed values [{allowed_values}]'
            )))

def get_raw_data_from_ddb_01(table_name):
    """ Function get all data from provided dynamo DB table with next format:
    {
        'Items': [{
                'client_id': {
                    'S': '725bad4b-d47a-4a4e-9f72-38bf56c1897d'
                },
                'client_name': {
                    'S': 'c-Lite Dev Station'
                },      
                'column' :  { ... }
            }, {} ], 
            etc....
    }
    """    
    client = boto3.client('dynamodb')
    response = client.scan(
        TableName=table_name
        #ReturnConsumedCapacity='TOTAL'
    )
    logger.info(' '.join((f'get_raw_data_from_ddb_01:',
        f'Return FULL scan from DynamoDB table [{table_name}]'
        )))
    
    return response

def raw_ddb_data_empty_01(ddb_raw_data):
    if len(ddb_raw_data['Items']) == 0:
        logger.warning(' '.join((f'raw_ddb_data_empty_01: Get empty response from DynamoDB',
            f' as [{ddb_raw_data}]'
            )))
        return True
    else:
        logger.info(' '.join((f'raw_ddb_data_empty_01: Get NOT empty response from',
            f'DynamoDB with [{len(ddb_raw_data["Items"])}] elements'
            )))
        return False
        
def ddb_raw_data_parser_02(ddb_raw_list,var_object):
    """
    Input: 
    - raw dict object from  [get_raw_data_from_ddb_01] function;
    - var object from lambda handler function (contains all session temp vars);

    This function return structured object with data from DynamoDB table about existing clients, as:
    {
        "id123456" : {
            "client_id"                 : "id123456",                   # exists after init/add 
            "client_name"               : "Mike",                       # exists after init/add
            "client_callcentername"     : "USA_callcenter",             # exists after init/add
            "update_ddb"                : False,
            "status"                    : "active" | "absent",
            "last_status_change"        : "2019-03-03T12:25:43.434Z",
            "last_ka_alert_notify"      : "2019-03-03T12:25:43.434Z",
            "last_restore_alert_notify" : "2019-03-03T12:25:43.434Z",
            "last_still_dead_notify"    : "2019-03-03T12:25:43.434Z",
            "last_update"               : "2019-03-03T12:25:43.434Z" 
        },
        ...
    }
    """
    current_time = str_to_time_01(var_object['shared_main_time'])
    temp_dict = {}

    #############################################################
    # invoke support func if get update list from DDB - init OR update
    # ADD CHECKs 
    #############################################################

    for each in ddb_raw_list['Items']:
        current_id = each['client_id']['S']
        temp_dict[current_id] = {
            "client_id" : current_id,
            "client_name" : each['client_name']['S'],
            "client_callcentername" : each['client_callcentername']['S'],
            "update_ddb" : False
        }
        try:
            temp_dict[current_id]['status'] = each['status']['S']
        except: # if field status not set to client because support func only add id-s to dynamodb table but not set extra attirbutes;
            temp_dict[current_id]['status'] = "absent"
        try:
            temp_dict[current_id]['last_status_change'] = each['last_status_change']['S']
        except: # if not set - apply current time
            temp_dict[current_id]['last_status_change'] = time_to_str_01(current_time) 
        try:
            temp_dict[current_id]['last_ka_alert_notify'] = each['last_ka_alert_notify']['S']
        except: # if not set - apply the oldest point of time (big check)
            temp_dict[current_id]['last_ka_alert_notify'] = time_to_str_01(current_time)
        try:
            temp_dict[current_id]['last_restore_alert_notify'] = each['last_restore_alert_notify']['S']
        except: # if not set - apply the oldest point of time (big check)
            temp_dict[current_id]['last_restore_alert_notify'] = time_to_str_01(current_time)
        try:
            temp_dict[current_id]['last_still_dead_notify'] = each['last_still_dead_notify']['S']
        except: # if not set - apply the oldest point of time (big check)
            temp_dict[current_id]['last_still_dead_notify'] = time_to_str_01(current_time)
        try:
            temp_dict[current_id]['last_update'] = each['last_update']['S']
        except: # if not set - apply current time
            temp_dict[current_id]['last_update'] = time_to_str_01(current_time) 

    logger.info(' '.join((f'ddb_raw_data_parser_02: Return parsed list of DynamoDB',
        f'Returned : [{temp_dict}]'
        )))
    return temp_dict

def get_es_raw_data_01(host_url):
    """ Function get raw json from elasticsearch with all messages for last N seconds as:
    {
    "took": 144,
    "timed_out": false,
    "_shards": {
        "total": 96,
        "successful": 96,
        "failed": 0
    },
    "hits": {
        "total": 10229,
        "max_score": 1,
        "hits": [
        {
            "_index": "keepalive-2019-05-06",
            "_type": "logs",
            "_id": "49593744063285510971078362215836050046374402535521779714.0",
            "_score": 1,
            "_source": {
            "logType": "Keepalive",
            "machineData": {
                "name": "mx-edomex-t-c-lite-06",
                "callCenterName": "MX-edomex-T-c-Lite",
                "machineTimeUTC": "2019-05-06T19:41:32.653Z",
                "id": "295a5ff3-a1d8-4705-9cfe-ffebd0a2e344",
                "stack": "prod",
                "machineTime": "2019-05-06T14:41:32.653-05:00"
            }
            }
        }, {} , ... 
        ] } } 
    """
    # set vars:
    host_url_int = f'{host_url}/keepalive*/_search'
    # get_query:
    query = {
      "size" : 9999, # default result window > 10000
      "query" : {
        "range" : {
          "machineData.machineTimeUTC" : {
            "gte" : "now-180s"
          }
        }
      }
    }
    # ES 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    # Make the signed HTTP request
    response = requests.get(host_url_int, auth=awsauth, headers=headers, data=json.dumps(query))
    logger.info(f'get_es_raw_data_01: Return  all records from elasticsearch for last 300 seconds')
    return response.json()

def es_raw_data_parser_keepalive_02(es_raw_list):
    """
    Function get raw json input from function get_es_raw_data_01 and 
    Return structured object of records in last (n) seconds from keepalive ES index, as :
    {
        client_id : {
            "client_id"         : "id123456",                   #
            "client_name"       : "Sigma client",               #
            "last_time_active"  : "2019-03-03T12:25:43.434Z",   # time of last keepalive message from client
            "id_count"          : 4                             # count how many times id appears in keepalive messages for last (n) seconds
        },
        ...
    }
    """
    temp_dict = {}
    # logger.debug(f'es_raw_data_parser_keepalive_02: input es_raw_list : [{es_raw_list}]')
    for each in es_raw_list['hits']['hits']:
        current_id = each['_source']['machineData']['id']
        current_id_time = each['_source']['machineData']['machineTimeUTC']
        if current_id not in temp_dict:
            temp_dict[current_id] = {
                "client_id" : current_id,
                "client_name" : each['_source']['machineData']['name'],
                "last_time_active" : current_id_time,
                "id_count" : 1
            }
        else:
            if str_to_time_01(current_id_time) > str_to_time_01(temp_dict[current_id]['last_time_active']):
                temp_dict[current_id]['last_time_active'] = current_id_time
            temp_dict[current_id]['id_count'] = int(temp_dict[current_id]['id_count']) + 1
    logger.info(' '.join((f'es_raw_data_parser_keepalive_02: return structured list,',
        f'count num of keepalive message for every client.',
        f'Returned temp_dict : [{temp_dict}]'
        )))
    return temp_dict

def compare_parsed_data_es_ddb_02(es_dict, ddb_dict, var_object):
    """
    Get input from [db_raw_data_parser_02] and [es_raw_data_parser_keepalive_02].
    This function compare parsed information of elasticsearch and dynamodb and
    return dict object with updates of status, timestamps
    Structure of output object:
    { "id123456":  {
        "client_id"                         : "id123456",                   #
        "status"                            : "active" | "absent",          #
        "status_changed"                    : True | False                  #
        "last_status_change"                : "2019-03-03T12:25:43.434Z",   #
        "last_ka_alert_notify"              : "2019-03-03T12:25:43.434Z",   #
        "send_ka_alert_now"                 : True | false                  #
        "last_restore_alert_notify"         : "2019-03-03T12:25:43.434Z",   #
        "send_restore_alert_now"            : True | false                  #
        "last_still_dead_notify"            : "2019-03-03T12:25:43.434Z",   #
        "send_still_dead_alert_now"         : True | false                  #
        "last_update"                       : "2019-03-03T12:25:43.434Z",   #
        "update_ddb"                        : True | False                  #
        } , { } , ...
    }
    """
    result_dict_compare = {}
    current_time = str_to_time_01(var_object['shared_main_time'])
    int_es_dict = es_dict
    int_ddb_dict = ddb_dict

    es_set = set(int_es_dict.keys()) # set of ids from elasticsearch
    ddb_set = set(int_es_dict.keys()) # set of ids from DynamoDB
    only_es_ids = es_set - ddb_set
    ####################################################################
    only_ddb_ids = ddb_set - es_set

    if len(only_es_ids) != 0:
    ################################################################### ADD function
    # invoke support to add new items to table (async)
        # send list of ids to support func to add them into ddb (async)
        logger.warning(' '.join((f'compare_parsed_data_es_ddb_02: Found new ids',
            f'from elasticsearch: [{only_es_ids}]'
            )))
    
    # logger.info(' '.join((f'compare_parsed_data_es_ddb_02: iterate over',
    #     f'int_es_dict : [{int_es_dict}]'
    #     )))
    
    for es_id in int_ddb_dict:
        if es_id in int_es_dict: # Elements from elastic last info
            if es_id in only_es_ids:
                logger.warning(' '.join((f'compare_parsed_data_es_ddb_02: Found new',
                f'element [{es_id}] from elasticsearch, not in DynamoDB. Need to update DynamoDB'
                )))
                continue # skip new element from elasticsearch, first update ddb; proceed them next time
        
            # logger.info(f'Error with id_count - value [{int_es_dict[es_id]}]')
            # if int(int_es_dict[es_id]['id_count']) > count_compare_number:
            #     current_status = "active"
            # else:
            #     current_status = "active"
    
            current_status = "active" if (int_es_dict[es_id]['id_count'] > count_compare_number) else "absent"
    
            if current_status != int_ddb_dict[es_id]['status']:
                current_time_last_status_change = time_to_str_01(current_time)
                current_status_changed = True
            else:
                current_time_last_status_change = int_ddb_dict[es_id]['last_status_change']
                current_status_changed = False
    
            if current_status == "absent" and int_ddb_dict[es_id]['status'] == "active":
                current_send_ka_alert_now = True
            else:
                current_send_ka_alert_now = False
    
            if current_status == "active" and int_ddb_dict[es_id]['status'] == "absent":
                current_send_restore_alert_now = True
            else:
                current_send_restore_alert_now = False
    
            try:
                result_dict_compare[es_id] = {
                    "client_id"                 : es_id,
                    "client_name"               : int_ddb_dict[es_id]['client_name'],
                    "client_callcentername"     : int_ddb_dict[es_id]['client_callcentername'],
                    "status"                    : current_status,
                    "status_changes"            : current_status_changed,
                    "last_status_change"        : current_time_last_status_change,
                    "last_ka_alert_notify"      : int_ddb_dict[es_id]['last_ka_alert_notify'],
                    "send_ka_alert_now"         : current_send_ka_alert_now,
                    "last_restore_alert_notify" : int_ddb_dict[es_id]['last_restore_alert_notify'],
                    "send_restore_alert_now"    : current_send_restore_alert_now,
                    "last_still_dead_notify"    : int_ddb_dict[es_id]['last_still_dead_notify'],
                    "send_still_dead_alert_now" : False,
                    "last_update"               :  int_ddb_dict[es_id]['last_update'],
                    "update_ddb"                : True
                }
            except Exception as e:
                logger.warning(' '.join((f'compare_parsed_data_es_ddb_02: failed add [{es_id}] to'
                f'returned list. Skipped. Exception: [{e}]'
                )))
                continue
        else: # elements no info from Elastic but present in DynamoDB
            if (int_ddb_dict[es_id]['status'] == "absent") and \
                str_to_time_01(int_ddb_dict[es_id]['last_still_dead_notify']) < (current_time - still_dead_alert_interval) and \
                str_to_time_01(int_ddb_dict[es_id]['last_status_change']) > (current_time - big_time_delta):
                current_send_still_dead_alert_now = True
                print(
                f"Current id:[{es_id}], last_still_dead_notify:[{int_ddb_dict[es_id]['last_still_dead_notify']}],",
                f"current_time:[{current_time}], alertINterval:[{still_dead_alert_interval}], sendStillDead:[{current_send_still_dead_alert_now}]"
                )
            else:
                current_send_still_dead_alert_now = False
                print(
                f"Current id:[{es_id}], last_still_dead_notify:[{int_ddb_dict[es_id]['last_still_dead_notify']}],",
                f"current_time:[{current_time}], alertINterval:[{still_dead_alert_interval}], sendStillDead:[{current_send_still_dead_alert_now}]"
                )

            result_dict_compare[es_id] = {
                "client_id"                     : es_id,
                "client_name"                   : int_ddb_dict[es_id]['client_name'],
                "client_callcentername"         : int_ddb_dict[es_id]['client_callcentername'],
                "status"                        : int_ddb_dict[es_id]['status'],
                "status_changes"                : False,
                "last_status_change"            : int_ddb_dict[es_id]['last_status_change'],
                "last_ka_alert_notify"          : int_ddb_dict[es_id]['last_ka_alert_notify'],
                "send_ka_alert_now"             : False,
                "last_restore_alert_notify"     : int_ddb_dict[es_id]['last_restore_alert_notify'],
                "send_restore_alert_now"        : False,
                "last_still_dead_notify"        : int_ddb_dict[es_id]['last_still_dead_notify'],
                "send_still_dead_alert_now"     : current_send_still_dead_alert_now,
                "last_update"                   : int_ddb_dict[es_id]['last_update'],
                "update_ddb"                    : True
            }
            
    # for es_id in int_es_dict:
    #     if es_id in only_es_ids:
    #         logger.warning(' '.join((f'compare_parsed_data_es_ddb_02: Found new',
    #         f'element [{es_id}] from elasticsearch, not in DynamoDB. Need to update DynamoDB'
    #         )))
    #         continue # skip new element from elasticsearch, first update ddb; proceed them next time
        
    #     # logger.info(f'Error with id_count - value [{int_es_dict[es_id]}]')
    #     # if int(int_es_dict[es_id]['id_count']) > count_compare_number:
    #     #     current_status = "active"
    #     # else:
    #     #     current_status = "active"

    #     current_status = "active" if (int_es_dict[es_id]['id_count'] > count_compare_number) else "absent"

    #     if current_status != int_ddb_dict[es_id]['status']:
    #         current_time_last_status_change = time_to_str_01(current_time)
    #         current_status_changed = True
    #     else:
    #         current_time_last_status_change = int_ddb_dict[es_id]['last_status_change']
    #         current_status_changed = False

    #     if current_status == "absent" and int_ddb_dict[es_id]['status'] == "active":
    #         current_send_ka_alert_now = True
    #     else:
    #         current_send_ka_alert_now = False

    #     if current_status == "active" and int_ddb_dict[es_id]['status'] == "absent":
    #         current_send_restore_alert_now = True
    #     else:
    #         current_send_restore_alert_now = False

    #     if (current_status == "absent" and int_ddb_dict[es_id]['status'] == "absent") and \
    #         str_to_time_01(int_ddb_dict[es_id]['last_still_dead_notify']) < (current_time - still_dead_alert_interval) and \
    #         str_to_time_01(int_ddb_dict[es_id]['last_status_change']) > (current_time - big_time_delta):
    #         current_send_still_dead_alert_now = True
    #     else:
    #         current_send_still_dead_alert_now = False

    #     try:
    #         result_dict_compare[es_id] = {
    #             "client_id" : es_id,
    #             "client_name" : int_ddb_dict[es_id]['client_name'],
    #             "client_callcentername" : int_ddb_dict[es_id]['client_callcentername'],
    #             "status" : current_status,
    #             "status_changes" : current_status_changed,
    #             "last_status_change" : current_time_last_status_change,
    #             "last_ka_alert_notify" : int_ddb_dict[es_id]['last_ka_alert_notify'],
    #             "send_ka_alert_now" : current_send_ka_alert_now,
    #             "last_restore_alert_notify" : int_ddb_dict[es_id]['last_restore_alert_notify'],
    #             "send_restore_alert_now" : current_send_restore_alert_now,
    #             "last_still_dead_notify" : int_ddb_dict[es_id]['last_still_dead_notify'],
    #             "send_still_dead_alert_now" : current_send_still_dead_alert_now,
    #             "last_update" :  int_ddb_dict[es_id]['last_update'],
    #             "update_ddb" : True
    #         }
    #     except Exception as e:
    #         logger.warning(' '.join((f'compare_parsed_data_es_ddb_02: failed add [{es_id}] to'
    #         f'returned list. Skipped. Exception: [{e}]'
    #         )))
    #         continue

    logger.info(' '.join((f'compare_parsed_data_es_ddb_02: Information from ELASTICSEARCH',
        f'and DynamoDb compared and prepared for next actions.',
        f'Returned result: [{result_dict_compare}]'
        )))
    return result_dict_compare

def slack_notification_01(message_title,message_text):
    """ Function sends one message to slack channel, given by ENV variable """

    slack_data = {
    	"text" : "Project(name):",
        "attachments": [
        {
			"author_name"   : f"ClientChecks-{region}-{environment}",
			"title"         : str(message_title),
			"text"          : str(message_text),
			"ts"            : f'{time.time()}'
        }
      ]
    }
    try:
        response = requests.post(
            slack_channel_notify, data = json.dumps(slack_data),
            headers = {'Content-Type': 'application/json'}
        )
        logger.info(' '.join((f'slack_notification_01: Sent notification',
            f'as [{message_title}]-[{message_text}] to slack'
            )))
        return True
    except:
        logger.info(' '.join((f'slack_notification_01: FAILED to send notification',
            f'as [{message_title}]-[{message_text}] to slack'
            )))
        return False

def all_notification_02(ids_dict, var_object):
    """ Function sends all notification based on previous collected data. 
    Each notification is included as separate function. 
    Gets input object from compare_parsed_data_es_ddb_02.
    """

        ####################################### CHECK IT IF FAIL !
        # IF EMPTY MESSAGES ? 
    
    keepalive_alerts_title = "KEEPALIVE alert for next clients:"
    keepalive_alerts = []
    restore_alerts_title = "RESTORE alert for next clients:"
    restore_alerts = []
    stilldead_alerts_title = "STILLDEAD, next clients are still not active:"
    stilldead_alerts = []

    dict_processed = ids_dict

    for each in dict_processed:

        appended_text = ''.join((
            f'[{dict_processed[each]["client_name"]}|',
            f'{dict_processed[each]["client_callcentername"]}]'
        ))

        if dict_processed[each]['send_ka_alert_now'] == True:
            keepalive_alerts.append(appended_text)
            dict_processed[each]['last_ka_alert_notify'] = var_object['shared_main_time']

        if dict_processed[each]['send_restore_alert_now'] == True:
            restore_alerts.append(appended_text)
            dict_processed[each]['last_restore_alert_notify'] = var_object['shared_main_time']

        if dict_processed[each]['send_still_dead_alert_now'] == True:
            stilldead_alerts.append(appended_text)
            # print(f"still_dead time for id:[{each}] BEFORE set new is : [{dict_processed[each]['last_still_dead_notify']}]")
            dict_processed[each]['last_still_dead_notify'] = var_object['shared_main_time']
            # print(f"still_dead time for id:[{each}] AFTER set new is : [{dict_processed[each]['last_still_dead_notify']}]")


    if  len(keepalive_alerts+restore_alerts+stilldead_alerts) != \
        len(set(keepalive_alerts+restore_alerts+stilldead_alerts)):
        logger.warning(' '.join((f'all_notification_02: double notification for clients, check ids list:'
            f'keepalive_alert : [{keepalive_alerts}];',
            f'restore_alerts : [{restore_alerts}];',
            f'stilldead_alerts : [{stilldead_alerts}];'
        )))

    try:
        if len(keepalive_alerts) != 0:
            # PUT/ADD here function to notify about KEEPALIVE alerts:
            slack_notification_01(keepalive_alerts_title,keepalive_alerts)
            logger.info(f'all_notification_02: SENT keepalive for [{len(keepalive_alerts)}] clients')

        if len(restore_alerts) != 0:
            # PUT/ADD here function to notify about RESTORE client:
            slack_notification_01(restore_alerts_title,restore_alerts)
            logger.info(f'all_notification_02: SENT restore for [{len(restore_alerts)}] clients')

        if len(stilldead_alerts) != 0:
            # PUT/ADD here function to notify about STILLDEAD alert:
            slack_notification_01(stilldead_alerts_title,stilldead_alerts)
            logger.info(f'all_notification_02: SENT stilldead for [{len(stilldead_alerts)}] clients')

    except Exception as e:
        logger.warning(f'all_notification: FAILED to send notification. Get [{e}].')
        pass
    return dict_processed

def post_to_elastic_01(query, var_object):
    """ Function sends one query to elasticsearch cluster """

    host_url_int = f'{var_object["elastic_url"]}/{var_object["full_es_index_name"]}/doc'
    # ES 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    # Make the signed HTTP request
    response = requests.post(host_url_int, auth=awsauth, headers=headers, data=json.dumps(query))
    logger.info(f'post_to_elastic_01: post to elastic - [{response.status_code}], query - [{query}], elastic - [{host_url_int}]')
    if int(response.status_code) == 200:
        return True
    else: 
        return False

def write_all_to_elastic_02(ids_dict, var_object):
    """ Function checks main store file and generate info (queries) for elasticsearch. 
    Output format (to elastic):
    {
        "time: "current_time",
        "clients" : { 
            "active" : [ 
                {
                    "id" : "id01",
                    "name" : "id01name",
                    "ccname" : "id01callcenter"
                }, 
                {
                    same_dict_for_other_client
                } 
            ],
            "absent"    : [ { ... }, { ... }, ... ],
            "keealive"  : [ { ... }, { ... }, ... ],
            "stilldead" : [ { ... }, { ... }, ... ],
            "restore"   : [ { ... }, { ... }, ... ]
        }
    }

    """
    int_ids_dict = ids_dict

    active_clients,             \
    absent_clients,             \
    keepalive_alert_clients,    \
    restore_alert_clients,      \
    stilldead_alert_clients = ([] for _ in range(5))
    
    for each in int_ids_dict:
        client_temp_obj = {
            "id"     : int_ids_dict[each]['client_id'],
            "name"   : int_ids_dict[each]['client_name'],
            "nm-cc"  : f'{int_ids_dict[each]["client_name"]}|{int_ids_dict[each]["client_callcentername"]}',
            "ccname" : int_ids_dict[each]['client_callcentername']
        }

        if int_ids_dict[each]['status'] == 'active':
            active_clients.append(client_temp_obj)
        elif int_ids_dict[each]['status'] == 'absent':
            absent_clients.append(client_temp_obj)
        else:
            logger.error(' '.join((f'write_all_to_elastic_02: get unsupported clients', 
                f'status for: [{int_ids_dict[each]}]'
                )))
            raise Exception(f'Get wrong status for client [{int_ids_dict[each]}]')
        
        if int_ids_dict[each]['send_ka_alert_now']:
            keepalive_alert_clients.append(client_temp_obj)
        
        if int_ids_dict[each]['send_restore_alert_now']:
            restore_alert_clients.append(client_temp_obj)
        
        if int_ids_dict[each]['send_still_dead_alert_now']:
            stilldead_alert_clients.append(client_temp_obj)
        
    output_query = {
        "time" : var_object['shared_main_time'],
        "clients" : {
            "active"    : active_clients,
            "absent"    : absent_clients,
            "keepalive" : keepalive_alert_clients,
            "restore"   : restore_alert_clients,
            "stilldead" : stilldead_alert_clients
        }
    }
    
    logger.info(f'write_all_to_elastic_02: fill query to update Elasticsearch records')
    post_to_elastic_01(output_query, var_object)

    return int_ids_dict

def update_element_to_ddb_01(input_element, table_name, var_object):
    """ Update one element in DynamoDB table, get input per element from compare_parsed_data_es_ddb_02 """
    
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)

    try:
        response = table.update_item(
            TableName = table_name, 
            ReturnValues = 'NONE', # TRY TO CHANGE TO AVOID TIME LOSS > "NONE" 
            Key = {
                'client_id' : input_element["client_id"] 
            },
            UpdateExpression = 'SET #cn = :cn, #cccn = :cccn, #sts = :sts,  #tlsc = :tlsc, \
                #tlkn = :tlkn, #tlrn = :tlrn, #tlsn = :tlsn, #tlu = :tlu',
            ExpressionAttributeNames = {
                '#cn'   : 'client_name',
                '#cccn' : 'client_callcentername',
                '#sts'  : 'status',
                '#tlsc' : 'last_status_change',
                '#tlkn' : 'last_ka_alert_notify',
                '#tlrn' : 'last_restore_alert_notify',
                '#tlsn' : 'last_still_dead_notify',
                '#tlu'  : 'last_update'
            },
            ExpressionAttributeValues = {
                ':cn'   : input_element['client_name'],
                ':cccn' : input_element['client_callcentername'],
                ':sts'  : input_element['status'],
                ':tlsc' : input_element['last_status_change'],
                ':tlkn' : input_element['last_ka_alert_notify'],
                ':tlrn' : input_element['last_restore_alert_notify'],
                ':tlsn' : input_element['last_still_dead_notify'],
                ':tlu'  : var_object['shared_main_time']
            }
        )
        logger.info(' '.join((f'update_element_to_ddb_01: UPDATE one element to',
          f'DDB - [{input_element}] with time [{var_object["shared_main_time"]}]'
          )))
        return True
    except Exception as e:
        logger.warning(' '.join((f'update_element_to_ddb_01: FAILED update element',
          f'[{input_element}] to DynamoDB table [{table_name}] as [{e}]'
          )))
        return False

def update_ddb_elements_02(ids_dict, var_object):
    """ Function iterate over given dict (main store of processed values) and update 
        items in DynamoDB (uses update_element_to_ddb_01 function) """

    int_ids_dict = ids_dict
    for each_id in int_ids_dict:
        if int_ids_dict[each_id]['update_ddb'] == True:
            data_to_ddb = {
                "client_id"                     : int_ids_dict[each_id]["client_id"],
                "client_name"                   : int_ids_dict[each_id]["client_name"],
                "client_callcentername"         : int_ids_dict[each_id]["client_callcentername"],
                "status"                        : int_ids_dict[each_id]["status"],
                "last_status_change"            : int_ids_dict[each_id]["last_status_change"],
                "last_ka_alert_notify"          : int_ids_dict[each_id]["last_ka_alert_notify"],
                "last_restore_alert_notify"     : int_ids_dict[each_id]["last_restore_alert_notify"],
                "last_still_dead_notify"        : int_ids_dict[each_id]["last_still_dead_notify"]
            }
            logger.debug(' '.join((f'update_ddb_elements_02: UPDATE DynamoDB for',
                f'[{int_ids_dict[each_id]["client_id"]}]=',
                f'[{int_ids_dict[each_id]["client_name"]}]-',
                f'[{int_ids_dict[each_id]["client_callcentername"]}]'
            )))
        else:
            logger.debug(' '.join((f'update_ddb_elements_02: SKIP UPDATE DynamoDB for', 
                f'[{int_ids_dict[each_id]["client_id"]}]=',
                f'[{int_ids_dict[each_id]["client_name"]}]-',
                f'[{int_ids_dict[each_id]["client_callcentername"]}]'
            )))
            continue
        update_element_to_ddb_01(data_to_ddb,table_name, var_object)
    logger.info(f'update_ddb_elements_02: ALL new info was written to DynamoDB')
    return int_ids_dict

def iterate_over_results_03(compared_dict, var_object):
    """Function execute notifications and update info at DynamoDB, Elastic. 
    Process main dict through notification and update functions.
    Return last state of main dict. 
    """

    int_compared_dict_01 = compared_dict
    int_compared_dict_02 = all_notification_02(int_compared_dict_01,var_object)
    int_compared_dict_03 = write_all_to_elastic_02(int_compared_dict_02, var_object)
    int_compared_dict_04 = update_ddb_elements_02(int_compared_dict_03,var_object)

    logger.info(f'iterare_over_results_03: Finished all checkings, elements updated. Final result : [{int_compared_dict_03}]')
    return int_compared_dict_04

def get_current_time_str_02():
    logger.debug('get_current_time_str_02: return current time as string')
    return time_to_str_01(get_current_time_01())

def get_today_day_prefix_str_02():
    logger.debug('get_today_day_prefix_str_02: return current year-month-day prefix as string')
    return (get_current_time_01()).strftime('%Y-%m-%d')

### MAIN EXECUTION STARTS HERE:
def lambda_handler(event, context):

    var_obj = {}
    var_obj['table_name']           = table_name
    var_obj['elastic_url']          = elastic_url
    var_obj['shared_main_time']     = get_current_time_str_02()
    var_obj['es_today_suffix_part'] = get_today_day_prefix_str_02()
    var_obj['full_es_index_name']   = es_index_prefix + '-' + var_obj['es_today_suffix_part']

    # logger.info('### ENVIRONMENT VARIABLES ###')
    # logger.info(os.environ)
    # logger.info('### EVENT ###')
    # logger.info(event)
    # logger.info(f'### var_obj content: [{var_obj}]')

    raw_ddb_data = get_raw_data_from_ddb_01(table_name)
    if raw_ddb_data_empty_01(raw_ddb_data):
        init_call = generate_invoke_payload_01('init')
        invoke_support_func_01(init_call)
        raw_ddb_data = get_raw_data_from_ddb_01(table_name)
    parsed_ddb_data = ddb_raw_data_parser_02(raw_ddb_data, var_obj)

    raw_es_data = get_es_raw_data_01(elastic_url)
    parsed_es_data = es_raw_data_parser_keepalive_02(raw_es_data)

    # try: 
    compared_data_before_actions = compare_parsed_data_es_ddb_02(
        parsed_es_data, 
        parsed_ddb_data, 
        var_obj
        )

    compared_data_after_actions = iterate_over_results_03(
        compared_data_before_actions,
        var_obj
        )
    # except Exception as e: 
    #     logger.error(f'lambda_handler: main cycle failed. Exception: [{e}]')
    #     return {
    #         "exit_with_error"   : True
    #     }

    # if len(compared_data_after_actions) != 0: 
    #     all_updated = True
    # else:
    #     # never happens
    #     all_updated = False
    
    return {
        "log_level"             : log_level,
        "current_time"          : var_obj['shared_main_time'],
        # "raw_ddb_data"          : str(raw_ddb_data),
        "parsed_ddb_data"       : parsed_ddb_data,
        # "raw_es_data"           : str(raw_es_data),
        "parsed_es_data"        : parsed_es_data,
        "data_before_act"       : compared_data_before_actions,
        "data_after_act"        : compared_data_after_actions
    }

### Write to elasticsearch:
#     POST tstx-2019-01-01/doc
# {
#   "test01":"test01"
# }

