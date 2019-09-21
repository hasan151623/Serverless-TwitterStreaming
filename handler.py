import json
from datetime import datetime

from aws.dynamo_db import insert_item_to_dynamo_db, get_items_from_dynamo_db
from aws.sqs import retrieve_sqs_messages
from streaming.constants import NUMBER_OF_MESSAGES_TO_READ
from streaming.twitter_streaming import TwitterStreamer


# sls invoke -f process_queue_message
from streaming.utils import DecimalEncoder


def process_queue_message(event, context):
    messages = retrieve_sqs_messages(num_msgs=NUMBER_OF_MESSAGES_TO_READ)
    if messages:
        insert_item_to_dynamo_db(messages)

        response = {
            "statusCode": 200,
            "body": 'SUCCESS'
        }
    else:
        response = {
            "statusCode": 200,
            "body": "NO_MESSAGE"
        }

    return response


# sls invoke -f get_tweets

def get_tweets(event, context):
    params = event.get('queryStringParameters', None)
    date = None
    if params:
        date = params.get('date', None)

    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    items = get_items_from_dynamo_db(date)

    response = {
        "statusCode": 200,
        "body": json.dumps(items, cls=DecimalEncoder),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }
    return response

# sls invoke -f stream_tweets --data '{ "body": {"tags": ["donald"]}}'


def stream_tweets(event, context):
    body = event['body'] if isinstance(event['body'], dict) else json.loads(event['body'])

    tags = body.get("tags")

    streamer = TwitterStreamer()
    streamer.stream_tweets(tags=tags if tags else ['serverless'])

    response = {
        "statusCode": 200,
        "body": "Completed",
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }
    return response
