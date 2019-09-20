import json

from streaming.twitter_streaming import TwitterStreamer
from streaming.aws import retrieve_sqs_messages, insert_item_to_dynamo_db, get_items_from_dynamo_db


# sls invoke -f process_queue_message
from streaming.utils import DecimalEncoder


def process_queue_message(event, context):
    messages = retrieve_sqs_messages(num_msgs=10)
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
    items = get_items_from_dynamo_db()

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

    # To toggle between stream and un-stream twitter posts
    start_streaming = True

    streamer = TwitterStreamer()

    if start_streaming:
        streamer.stream_tweets(tags=tags if tags else ['serverless'])
    else:
        streamer.unstream_tweets()

    response = {
        "statusCode": 200,
        "body": "Completed",
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }
    return response
