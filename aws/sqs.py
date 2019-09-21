import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

sqs_client = boto3.client('sqs')

QUEUE_URL = None
try:
    queue = sqs_client.get_queue_url(QueueName=os.environ['QUEUE_NAME'])
    QUEUE_URL = queue['QueueUrl']
except ClientError as e:
    logging.error("error on retrieving sqs")


def send_sqs_message(msg_body):
    """
    :param msg_body: String message body
    :return: Dictionary containing information about the sent message. If
        error, returns None.
    """

    try:
        sqs_client.send_message(QueueUrl=QUEUE_URL,
                                MessageBody=msg_body)
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


def get_live_messages_from_sqs(num_msgs=10):
    messages = retrieve_sqs_messages(num_msgs=num_msgs)
    items = []
    if messages:
        for message in messages:
            items.append(json.loads(message['Body']))
    return items

