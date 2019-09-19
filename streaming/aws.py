import json
import logging
import os
import boto3
from botocore.exceptions import ClientError

# SQS Client
from streaming.utils import DecimalEncoder

sqs_client = boto3.client('sqs')

QUEUE_URL = None
try:
    queue = sqs_client.get_queue_url(QueueName=os.environ['QUEUE_NAME'])
    QUEUE_URL = queue['QueueUrl']
except ClientError as e:
    logging.error("error on retrieving sqs")


# DynamoDB Resource
dynamo_db = boto3.resource('dynamodb')
table = None

try:
    table = dynamo_db.Table(os.environ['DB_TABLE_NAME'])
except ClientError as e:
    logging.error("error on retrieving dynamodb table")


def send_sqs_message(msg_body):
    """
    :param msg_body: String message body
    :return: Dictionary containing information about the sent message. If
        error, returns None.
    """

    try:
        print("que url", QUEUE_URL)
        sqs_client.send_message(QueueUrl=QUEUE_URL,
                                MessageBody=msg_body)
        print("Sending message", msg_body)
        return True
    except ClientError as e:
        logging.error(e)
    return False


def retrieve_sqs_messages(num_msgs=1, wait_time=0, visibility_time=5):
    """Retrieve messages from an SQS queue

    The retrieved messages are not deleted from the queue.
    :param num_msgs: Number of messages to retrieve (1-10)
    :param wait_time: Number of seconds to wait if no messages in queue
    :param visibility_time: Number of seconds to make retrieved messages
        hidden from subsequent retrieval requests
    :return: List of retrieved messages. If no messages are available, returned
        list is empty. If error, returns None.
    """

    # Validate number of messages to retrieve
    if num_msgs < 1:
        num_msgs = 1
    elif num_msgs > 10:
        num_msgs = 10

    try:
        msgs = sqs_client.receive_message(QueueUrl=QUEUE_URL,
                                          MaxNumberOfMessages=num_msgs,
                                          WaitTimeSeconds=wait_time,
                                          VisibilityTimeout=visibility_time)
    except ClientError as e:
        logging.error(e)
        return None

    # Return the list of retrieved messages
    return msgs.get('Messages')


def delete_sqs_message(msg_receipt_handle):
    """Delete a message from an SQS queue

    :param msg_receipt_handle: Receipt handle value of retrieved message
    """
    sqs_client.delete_message(QueueUrl=QUEUE_URL,
                              ReceiptHandle=msg_receipt_handle)


def insert_item_to_dynamo_db(messages):
    logging.info("Inserting to dynamoDB")
    for message in messages:
        receipt_handle = message['ReceiptHandle']
        body = json.loads(message['Body'])
        if receipt_handle:
            try:
                resp = table.put_item(Item=body)
            except ClientError as e:
                logging.error("DynamoDB insertion error", str(e))

            # Deleting message from sqs
            delete_sqs_message(receipt_handle)


def get_items_from_dynamo_db():
    result = table.scan(Limit=10)
    items = result.get("Items")
    return items

