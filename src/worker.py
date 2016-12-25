import boto3
#Get the dynamodb service
dynamodb = boto3.resource('dynamodb')

#Get the table name
table = dynamodb.Table('Tasks')

#Get the sqs service
sqs = boto3.resource('sqs')

#Create a response queue
responseQueue = sqs.create_queue(QueueName = 'resQueue')

#Get the response queue
resQue = sqs.get_queue_by_name(QueueName = 'resQueue')

#Get the request queue
reqQue = sqs.get_queue_by_name(QueueName = 'reqQueue')

#Retrive the jobs in the request queue
received_message = reqQue.receive_messages(WaitTimeSeconds = 20,MaxNumberOfMessages = 1)

while (len(received_message) != 0): #Check the presence of messages in the queue
    for message in received_message:
        msg_split_list = message.body.split()#Split the message boddelimited by space and store it in a list
        if len(msg_split_list) != 0:#To avoid index out of bound exception
            #Find if the jobid is already present in DB.Removing the possible duplicate jobids that could arrive from SQS
            flag = 0 #To indicate the presence and absence of taskid in the table
            try:
                response = table.get_item(
                    Key = {
                        'taskid' : msg_split_list[0],
                    }
                )
                item = response['Item']
                #taskid = item['taskid']
                flag = 1
            except:
                flag = 0

            if flag == 1: #The TaskID already exists in the DB
                pass
            else:         #The TaskIDdoesnot exists in the DB
                #Insert the taskID into the table
                table.put_item(
                    Item={
                        'taskid' : msg_split_list[0],
                        'stat' : 0
                    }
                )
                #Execute the task
                t_executed = False
                try:
                    print "Executing the taskid:"+msg_split_list[0]
                    t_executed = True
                except:
                    pass
                #If task execution is successful, then update the status of the task in the table
                if t_executed == True:
                    table.update_item(
                        Key={
                            'taskid' : msg_split_list[0],
                        },
                        UpdateExpression = 'SET stat = :val1',
                        ExpressionAttributeValues={
                            ':val1': 1
                        }
                    )
                    #Posting the completed job ids into response Queue   
                    resQue.send_message(MessageBody = msg_split_list[0])



        message.delete()

    received_message = reqQue.receive_messages(WaitTimeSeconds = 20,MaxNumberOfMessages = 1)



