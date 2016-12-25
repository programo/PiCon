import boto3
import time
#Get the dynamodb service resource
dynamodb = boto3.resource('dynamodb')

#Get the table
table = dynamodb.Table('Tasks')

#Get service resource
sqs = boto3.resource('sqs')

#Create the request queue.Returns a sqs Queue Instance
resquestQueue = sqs.create_queue(QueueName = 'reqQueue')

#Get the request queue
reqQue = sqs.get_queue_by_name(QueueName = 'reqQueue')

#Post the jobs into the request queue
fobj = open("test.txt")
id = 0
initial_task_id_list = []
executed_task_id_list = []
for line in fobj:
    initial_task_id_list.append(str(id))
    reqQue.send_message(MessageBody = str(id) +" "+ line)
    id += 1
fobj.close()


#Retrive executed jobids from the response queue

#Get the response Queue
resQue = sqs.get_queue_by_name(QueueName = 'resQueue')

#Retrive the executed jobs from the response Queue
received_message = resQue.receive_messages(WaitTimeSeconds = 20, MaxNumberOfMessages = 1)

#Multiple calls on receive_messages needed to get all the message

while (len(received_message) != 0):#Check the presence of messages in the queue
    for message in received_message:
        msg_split_list = message.body.split()#Split the message body delimited by space and store it in a list
        if len(msg_split_list) != 0:#To avoid index out of bound exception
            #print msg_split_list[0]
            try:
                response = table.get_item(
                    Key={
                        'taskid' : msg_split_list[0]
                    }
                )
                item = response['Item']
                if item['stat'] == 1:
                    print "TaskID:"+msg_split_list[0] +"executed"
                    executed_task_id_list.append(msg_split_list[0])
                else:
                    print "TaskID:"+msg_split_list[0] +"not executed"
            except:
                print " The taskid:" + msg_split_list[0] + "is not created in the table by the worker"
        message.delete()
    received_message = resQue.receive_messages(WaitTimeSeconds = 20, MaxNumberOfMessages = 1)

