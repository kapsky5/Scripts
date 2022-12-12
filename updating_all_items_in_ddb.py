import json
import boto3
import datetime
from datetime import timezone


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('MerchantRiskData')
    response = table.scan(AttributesToGet=['merchantIdentifier'])
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(AttributesToGet=[
                              'merchantIdentifier'], ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    print(len(data))

    temp_date = str(datetime.datetime.now(timezone.utc)).split(" ")
    temp_date = "T".join(temp_date).split("+")[0]
    date = temp_date + "Z[UTC]"
    print(date)
    for item in data:
        table.update_item(
            Key={
                'merchantIdentifier': item["merchantIdentifier"]
            },
            UpdateExpression='SET lastUpdatedDate = :val1',
            ExpressionAttributeValues={
                ':val1': date
            }
        )
