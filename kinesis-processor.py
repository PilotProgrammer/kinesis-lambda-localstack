# 8/23/20. G Granacher: kinesis-producer.py and kinesis-processor.py are first set of scripts I wrote to test kinesis dedup strategy

from __future__ import print_function

import base64
import json
import hashlib
import boto3

# no fake error ~9:07 processing. but LOTS of duplicate invocations (no errors though so why so many records getting sent twice?!?)
# https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:logs-insights$3FqueryDetail$3D$257E$2528end$257E$25272020-08-23T13*3a10*3a00.000Z$257Estart$257E$25272020-08-23T13*3a05*3a00.000Z$257EtimeType$257E$2527ABSOLUTE$257Etz$257E$2527Local$257EeditorString$257E$2527fields*20*40timestamp*2c*20*40requestId*2c*20*40message*0a*7c*20filter*20*40message*20like*20*27*7e*7e*7e*7e*7e*7e*20DOING*20STUFF*20WITH*20THE*20NEW*20RECORD*27*0a*23*7c*20filter*20*40message*20like*20*27Received*20event*3a*27*0a*23*7c*20filter*20*40message*20not*20like*20*27partitionKey-04*27*0a*23*7c*20filter*20*40message*20like*20*27eyJrZXktMTAiOiAidmFsdWUtMTAifQ*3d*3d*27*0a*23*20*7c*20filter*20*40message*20like*20*27*2a*2a*2a*2a*2a*2a*20DUPLICATE*20RECORD*20--*20NOT*20PROCESSING*27*0a*7c*20sort*20*40timestamp*20desc*0a*7c*20limit*209999$257EisLiveTail$257Efalse$257EqueryId$257E$252764bc4320-4da8-4931-a191-04aafc5c9457$257Esource$257E$2528$257E$2527*2faws*2flambda*2fkinesis-processor$2529$2529$26tab$3Dlogs

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))
    
    idx = 0
    
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        partitionKey = record['kinesis']['partitionKey']
        sequenceNumber = record['kinesis']['sequenceNumber']
        payload = base64.b64decode(record['kinesis']['data'])
    
        print(str(idx) + '-- partitionKey: ' + partitionKey)
        print(str(idx) + '-- sequenceNumber: ' + sequenceNumber)
        print(str(idx) + '-- payload: ' + payload)
        
        hashOfData = hashlib.md5(payload.encode()) 
        hashOfData = hashlib.sha256(payload.encode()) 
        
        print(str(idx) + '-- The hexadecimal equivalent of hashOfData is : ', hashOfData.hexdigest()) 

        dynamodb = boto3.client('dynamodb')
        record = dynamodb.get_item(TableName='twogsw-dedup', ConsistentRead=True, Key={'partitionKey':{'S':partitionKey},'md5':{'S':hashOfData.hexdigest()}})
        # print(str(idx) + '-- Check for duplicate record: ', json.dumps(record, indent=2)) 
        print(str(idx) + '-- Check for duplicate record: ', json.dumps(record)) 
        
        payloadJson = json.loads(payload)

        # if 'throwError' in payloadJson.keys():
        #     raise Exception('WE GOT PROBLEMS! payload: ' + str(payload))

        if 'Item' not in record.keys():
            print(str(idx) + ' -- ~~~~~~ DOING STUFF WITH THE NEW RECORD!!! And then saving to dynamodb..')
            dynamodb.put_item(TableName='twogsw-dedup', Item={'partitionKey':{'S':partitionKey},'md5':{'S':hashOfData.hexdigest()},'payloadDecoded':{'S':payload}})
        else:
            print(str(idx) + ' -- ****** DUPLICATE RECORD -- NOT PROCESSING')

        idx = idx + 1

    return 'Successfully processed {} records.'.format(len(event['Records']))
