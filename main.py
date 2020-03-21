import sys
import logging
import tweepy
from decouple import config


class StreamListener(tweepy.StreamListener):
    """Customize actions with each retrieved tweet."""
    def on_status(self, status):
        logging.info(status.id_str)

    def on_error(self, status_code):
        logging.error("Encountered streaming error (", status_code, ")")
        sys.exit()


# complete authorization and initialize API endpoint
auth = tweepy.OAuthHandler(config('API_KEY'), config('API_SECRET_KEY'))
auth.set_access_token(config('ACCESS_TOKEN'), config('ACCESS_TOKEN_SECRET'))
api = tweepy.API(auth)

# initialize stream
streamListener = StreamListener()
stream = tweepy.Stream(
    auth=api.auth, listener=streamListener, tweet_mode='extended')
