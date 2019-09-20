import html
import json
import os
import time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream

from streaming.constants import CONSUMER_KEY, CONSUMER_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
from streaming.aws import send_sqs_message


class TwitterStreamer(object):
    """
    Streaming Live Data
    """

    def __init__(self):
        run_time = int(os.environ['TWITTER_STREAM_TIMEOUT']) - 10
        listener = TwitterStreamListener(run_time)

        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_KEY_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        self.stream = Stream(auth, listener, timeout=run_time)

    def stream_tweets(self, tags):
        try:
            print("Started listening to twitter stream...")
            self.stream.filter(track=tags)
        # except (Timeout, SSLError, ReadTimeoutError, ConnectionError) as e:
        #     print("Network error occurred. Keep calm and carry on.", str(e))
        except Exception as e:
            print("Error on streaming!", e)
            pass
        # finally:
        #     print("Stream has crashed. System will restart twitter stream!")


class TwitterStreamListener(StreamListener):
    """
    Processing Live Data
    """

    def __init__(self, time_limit=10):
        self.start_time = time.time()
        self.time_limit = time_limit
        self.tweet_count = 0
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
                'id': decoded['id'],
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
            self.tweet_count += 1
            print(f'{self.tweet_count} tweets received')
            return True

    def on_timeout(self):
        """Called when stream connection times out"""
        return False

    def on_exception(self, exception):
        """Called when an unhandled exception occurs."""
        return False
