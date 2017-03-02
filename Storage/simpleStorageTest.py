import boto3
import requests
#Creating a S3 Connection
s3 = boto3.client('s3')
"""
#Creating a bucket
s3.create_bucket(Bucket = 'niwsa', CreateBucketConfiguration = {'LocationConstraint' : 'us-west-2'})

#Storing the video file.
s3.Object('niwsa','video.mpg').put(Body = open('/home/aswin/PiCon/Storage/video.mpg'))

#Accessing the bucket
bucket = s3.Bucket('niwsa')
exists = True
try:
    s3.meta.client.head_bucket(Bucket = 'niwsa')
except botocore.exceptions.ClientError as e:
    #If the error code is 404 then the bucket does not exists
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False
"""
for bucket in s3.buckets.all():
    for key in bucket.objects.all():
        url = s3.generate_presigned_url(
            ClientMethod = 'get_object',
            Params = {
                'Bucket' : bucket,
                'Key' : key
            }
        )
        print url



