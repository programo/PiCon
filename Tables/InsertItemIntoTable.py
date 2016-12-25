import sys
import boto3
from  boto3.dynamodb.conditions import Key,Attr
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('Tasks')

table.put_item(
    Item={
        'taskid' : '1',
        'stat' : 0
    }
)
"""
#Quering the DB
response = table.query(KeyConditionExpression = Key('taskid').eq('2'))

items = response['Items']
print items
#print items[0]['status']
"""
#get an item from the DB
try:
    response = table.get_item(
        Key={
            'taskid' : '1',
        }
    )
    item = response['Item']
    print item['stat']
except:
    print sys.exc_info()[0] 


