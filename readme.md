## Custom checks for metrics from elasticsearch using AWS lambda + python 3

---
#### ! this part of infrastructe uses Serverless framework to deploy resources;
---
### Preparation:

- install serverless framework:
  `npm install -g serverless`

- install serverless plugins:
  `serverless plugin install -n serverless-python-requirements`
  (**Installed to this repo**)

Query - to get uniq ID for last 31 days:
```
GET keepalive*/_search
{
  "size":0,
  "query" : {
    "range" : {
      "machineData.machineTimeUTC" : {
        "gte" : "now-31d"
      }
    }
  },
  "aggs": {
    "machine_ids" : {
      "terms": {
        "field": "machineData.id.keyword",
        "size": 99999
      }
    }
  }
}
```

### Deal with requirements - lambda layers:

  Put all function requirements into `requirements.txt` file in root folder of `serverless` (same as `serverless.yaml` file). At this moment I found that only one layer can be built by this plugin (no multi requirements files).

  > AWS lambda limits for layer size: 50 Mb for updload zip, 250 Mb for unzipped files;


#### Build layer from requirements:

Following main folder structure:
`pip install -r requirements_base.txt -t ./base/python/lib/python3.7/site-packages/`
(more at https://medium.com/@qtangs/creating-new-aws-lambda-layer-for-python-pandas-library-348b126e9f3e)

#### Deploy serverless stack to aws:
`serverless deploy --aws-profile kibanadev --stage dev`
`serverless deploy --aws-profile kibanadev --stage qa`

#### Update function in AWS after changing (fast deploy):
`serverless deploy function --function CreateDynamoDBTable --aws-profile kibanadev`
`serverless deploy function --function MainFunc --aws-profile kibanadev`

#### Remove serverless stack from aws:
`serverless remove --aws-profile kibanadev --stage dev`

#### Info for current stack:
`serverless info --aws-profile kibanadev --stage dev`

<!-- #### Install scripts plugin:
`npm install --save serverless-plugin-scripts` -->
