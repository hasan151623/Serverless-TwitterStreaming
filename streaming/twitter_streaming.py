import html
import json
import os
import time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream

from streaming.constants import CONSUMER_KEY, CONSUMER_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
from streaming.aws import send_sqs_message


class Singleton(type):
    """
    Define an Instance operation that lets clients access its unique
    instance.
    """

    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class TwitterStreamer(metaclass=Singleton):
    """
    Streaming Live Data
    """
    def __init__(self):
        run_time = int(os.environ['TWITTER_STREAM_TIMEOUT']) - 5
        listener = TwitterStreamListener(run_time)

        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_KEY_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        self.stream = Stream(auth, listener)

    def stream_tweets(self, tags, for_testing=False):
        if for_testing:
            data = {"id": 1174614891228848128, "created_at": "Thu Sep 19 09:23:08 +0000 2019",
                    "text": "rt @brat2381: whos shocked by this? anyone? trump insider claims melania has a long-term boyfriend (and donald trump knows about it) htt",
                    "country": None}
            json_dump = json.dumps(data)
            send_sqs_message(json_dump)
        else:
            self.stream.filter(track=tags)

    def unstream_tweets(self):
        self.stream.disconnect()


class TwitterStreamListener(StreamListener):
    """
    Processing Live Data
    """

    def __init__(self, time_limit=20):
        self.start_time = time.time()
        self.time_limit = time_limit
        super().__init__()

    def on_data(self, raw_data):
        if (time.time() - self.start_time) > self.time_limit:
            return False

        decoded = json.loads(html.unescape(raw_data))
        if decoded:
            created_at = decoded.get('created_at', '')
            text = decoded['text'].lower().encode('ascii', 'ignore').decode('ascii')
            country = decoded.get('country')

            content = {
                'id':  decoded['id'],
                'created_at': created_at,
                'text': text,
                'country': country,
                'user': {
                    'name': decoded['user']['name'],
                    'profile_image_url': decoded['user']['profile_image_url']
                },
                'favorite_count': decoded['favorite_count'],
                'reply_count': decoded['reply_count']
            }

            json_dump = json.dumps(content)
            send_sqs_message(json_dump)
            return True

    def on_error(self, status_code):
        print("Error code", status_code)




