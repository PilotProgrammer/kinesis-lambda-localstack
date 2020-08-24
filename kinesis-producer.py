# 8/23/20. G Granacher: kinesis-producer.py and kinesis-processor.py are first set of scripts I wrote to test kinesis dedup strategy

import json
import boto3

def lambda_handler(event, context):
    kinesis = boto3.client('kinesis')
    partitionKeyIndex = 0
    recordsToPut = []
    mockErrorIndex = 3
    
    for recordIndex in range(1, 11):
        partitionKey = 'partition_key-' + str(partitionKeyIndex)
        key = 'key-' + str(recordIndex)
        value = 'value-' + str(recordIndex)
        data = { key: value }
        print('partitionKey: ' + partitionKey + ' key: ' + key + ' value: ' + value)
        
        if (recordIndex == mockErrorIndex):
            print('We R at: ' + str(mockErrorIndex))
            data['throwError'] = True
            # raise Exception('WE GOT PROBLEMS! mockErrorIndex: ' + str(mockErrorIndex))
            
    
        recordToPut = {'Data': bytes(json.dumps(data), 'utf-8'), 'PartitionKey': partitionKey }
        recordsToPut.append(recordToPut)
    
    # print('recordsToPut: ' + json.dumps(recordsToPut, indent=2))
    print("recordsToPut: " + str(recordsToPut))

    kinesis.put_records(StreamName="ExampleInputStream", Records=recordsToPut)
    return True

    #print("Received event: " + json.dumps(event, indent=2))
    # for record in event['Records']:
    #     print(record['eventID'])
    #     print(record['eventName'])
    #     print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
    # return 'Successfully processed {} records.'.format(len(event['Records']))
