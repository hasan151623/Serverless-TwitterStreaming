import html
import json
import os
import re
import time
from datetime import datetime

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream

from aws.sqs import send_sqs_message
from streaming.constants import CONSUMER_KEY, CONSUMER_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET


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
            # print("Started listening to twitter stream...")
            self.stream.filter(track=tags)
        # except (Timeout, SSLError, ReadTimeoutError, ConnectionError) as e:
        #     print("Network error occurred. Keep calm and carry on.", str(e))
        except Exception as e:
            print("Error on streaming!", str(e))
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

        tweet = json.loads(html.unescape(raw_data))
        
        if tweet:
            text = tweet['text'].lower().encode('ascii', 'ignore').decode('ascii')
            country = tweet['place']['country'] if tweet.get('place', None) else None
            # created_at is Sat Sep 21 14:58:41 +0000 2019. Using regular expression to remove +0000 from it.˙
            created_at = re.sub(r"\+\d+\s", "", tweet.get('created_at', ''))
            created_date = datetime.strptime(created_at, "%a %b %d %H:%M:%S %Y").strftime("%Y-%m-%d")
            #
            content = {
                'tweet_id_str': tweet['id_str'],
                'created_date': created_date,  # partition key
                'text': text,
                'country': country,
                'user': {
                    'name': tweet['user']['name'],
                    'profile_image_url': tweet['user']['profile_image_url']
                },
                'favorite_count': tweet['favorite_count'],
                'reply_count': tweet['reply_count'],
                'timestamp_ms': int(tweet['timestamp_ms'])  # sort key
            }

            json_dump = json.dumps(content)
            send_sqs_message(json_dump)
            # self.tweet_count += 1
            # print(f'{self.tweet_count} tweets received')
            return True

    def on_timeout(self):
        """Called when stream connection times out"""
        return False

    def on_exception(self, exception):
        """Called when an unhandled exception occurs."""
        return False


# if __name__ == "__main__":
#     streamer = TwitterStreamer()
#     streamer.stream_tweets(tags=['donald'])
