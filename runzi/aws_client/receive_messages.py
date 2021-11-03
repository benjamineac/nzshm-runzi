import boto3
import json
from env import AWS_ACCESS_KEY, AWS_SECRET_KEY

def main():
    sqs = boto3.client('sqs', 
                        region_name='us-east-1',
                        aws_access_key_id=AWS_ACCESS_KEY,
                        aws_secret_access_key=AWS_SECRET_KEY)
    queueUrl="https://sqs.us-east-1.amazonaws.com/280294454685/runzi-inversion-diagnostics-queue.fifo"


    response = sqs.receive_message(
    QueueUrl=queueUrl,
    AttributeNames=[
        'All'
    ],
    MaxNumberOfMessages=1,
    VisibilityTimeout=100,
    WaitTimeSeconds=0)

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']

    sqs.delete_message(
        QueueUrl=queueUrl,
        ReceiptHandle=receipt_handle
    )
    print('Received and deleted message: %s' % message)


main()