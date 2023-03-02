import json
import boto3
import time

s3client = boto3.client('s3')
ddbclient = boto3.resource('dynamodb')

def lambda_handler(event, context):

    bucketname = event['Records'][0]['s3']['bucket']['name']
    jsonfilename = event['Records'][0]['s3']['object']['key'].strip()
    # Testing partition key
    partition_key_value = time.time_ns() // 1_000_000
    #print(bucketname)
    #print(jsonfilename)

    jsonobject = s3client.get_object(Bucket=bucketname,Key=jsonfilename)

    jsonfilereader = jsonobject['Body'].read()
    jsonDict = json.loads(jsonfilereader)
    
    #strip .json file tyle from the filename
    strippedFilename = jsonfilename.replace('.json', '')

    for string in jsonDict:
        partition_key_value = time.time_ns() // 1_000_000
        dateWithQuotes = f'{partition_key_value}'
        item = {'content': string, 'ID': dateWithQuotes , 'Document Name' : strippedFilename}
        print(item)
        table = ddbclient.Table('s3-json-textract-tablev2')
        table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': json.dumps('JSON Data Imported')
    }