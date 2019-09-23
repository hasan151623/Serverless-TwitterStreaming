import json
import os
import time
from datetime import datetime

from aws.dynamo_db import insert_item_to_dynamo_db, get_items_from_dynamo_db
from aws.sqs import retrieve_sqs_messages, get_live_messages_from_sqs
from streaming.constants import NUMBER_OF_MESSAGES_TO_READ
from streaming.twitter_streaming import TwitterStreamer


# sls invoke -f process_queue_message
from streaming.utils import DecimalEncoder


def process_queue_message(event, context):
    iteration = 5
    while iteration > 0:
        messages = retrieve_sqs_messages(num_msgs=NUMBER_OF_MESSAGES_TO_READ)

        if messages:
            insert_item_to_dynamo_db(messages)
        iteration -= 1

    response = {
        "statusCode": 200,
        "body": 'SUCCESS'
    }

    return response


#  sls invoke local -f get_live_tweets --data '{ "queryStringParameters": {"tag":"donald"}}'

def get_live_tweets(event, context):
    params = event.get('queryStringParameters', None)
    tag = None
    if params:
        tag = params.get('tag', None)

    live_tweets = []
    tweet_count = 0
    start_time = time.time()
    time_limit = int(os.environ['SQS_MESSAGE_READ_TIMEOUT']) - 5

    while (time.time() - start_time) < time_limit:
        items = get_live_messages_from_sqs(num_msgs=NUMBER_OF_MESSAGES_TO_READ)
        if tag:
            items = [item for item in items if tag in item['text']]

        returned_tweet_count = len(items)
        if returned_tweet_count > 0:
            live_tweets.extend(items)
            tweet_count += returned_tweet_count
            break

    response = {
        "statusCode": 200,
        "body": json.dumps(live_tweets, cls=DecimalEncoder),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        },
    }
    return response


def get_past_tweets(event, context):
    params = event.get('queryStringParameters', None)
    date = tag = last_evaluated_key = None
    if params:
        date = params.get('date', None)
        tag = params.get('tag', None)
        last_evaluated_key = params.get('last_evaluated_key', None)

    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    items = get_items_from_dynamo_db(date, tag, last_evaluated_key)

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
