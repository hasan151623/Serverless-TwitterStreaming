import json
import logging
import os
from ast import literal_eval
from base64 import b64decode

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


# DynamoDB Resource
from aws.sqs import delete_sqs_message

dynamo_db = boto3.resource('dynamodb')
table = None

try:
    table = dynamo_db.Table(os.environ['DB_TABLE_NAME'])
except ClientError as e:
    logging.error("error on retrieving dynamodb table")


def insert_item_to_dynamo_db(messages):
    for message in messages:
        receipt_handle = message['ReceiptHandle']
        body = json.loads(message['Body'])

        if receipt_handle:
            # logging into cloudwatch logs
            logging.info(body)
            try:
                resp = table.put_item(Item=body)
            except ClientError as e:
                logging.error("DynamoDB insertion error", str(e))

            # Deleting message from sqs
            delete_sqs_message(receipt_handle)


def get_items_from_dynamo_db(date, tag, last_evaluated_key=None):
    params = {
        'KeyConditionExpression': Key('created_date').eq(date),
        'ScanIndexForward': False, "Limit": 10
    }
    if tag:
        params.update({'FilterExpression': Attr('text').contains(tag)})

    if last_evaluated_key:
        # last evaluated key is binary encoded. It needs to be decoded first and then convert bytes to dict

        params['ExclusiveStartKey'] = literal_eval(b64decode(last_evaluated_key).decode('utf-8'))

    result = table.query(**params)
    return result

