import boto3

#Get the service resource
dynamodb = boto3.resource('dynamodb')

#Create the a table for storing the taskid and their execution status. 0 for not executed and 1 for executed.

table = dynamodb.create_table(
    TableName = 'Tasks',
    KeySchema = [
        {
            'AttributeName' : 'taskid',
            'KeyType'       : 'HASH'
        },
    ],
    AttributeDefinitions=[
        {
            'AttributeName' : 'taskid',
            'AttributeType' : 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits' : 5,
        'WriteCapacityUnits' : 5
    }
    
)

#Wait until the table exists
table.meta.client.get_waiter('table_exists').wait(TableName = 'Tasks')

#print out some data about the table

print (table.item_count)
