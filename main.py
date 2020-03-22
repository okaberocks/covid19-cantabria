"""Recover Twitter covid19 data in Cantabria."""

import re
from datetime import datetime

from decouple import config

import pandas as pd

import tweepy

from pyjstat import pyjstat

# global variables TODO: extract to config, maybe

status = {'altas': '',
          'positivo': '',
          'fallecido': '',
          'activo': '',
          'domicilio': '',
          'sanitario': ''}

hospitals = ['valdecilla', 'sierrallana', 'tresmares', 'laredo']

results_rows = []


def deEmojify(inputString):
    """Remove emojis and other annoying characters."""
    return inputString.encode('ascii', 'ignore').decode('ascii')


# MAIN SCRIPT

# complete authorization and initialize API endpoint
auth = tweepy.OAuthHandler(config('API_KEY'), config('API_SECRET_KEY'))
auth.set_access_token(config('ACCESS_TOKEN'), config('ACCESS_TOKEN_SECRET'))
api = tweepy.API(auth)

# retrieve tweets from the account providing the data
tweets = api.user_timeline(screen_name=config('USER_ID'),
                           include_rts=False,
                           count=200,
                           # Necessary to keep full_text
                           # otherwise only the first 140 words are extracted
                           tweet_mode='extended'
                           )

# generate dataframe with the interesting tweets
outtweets = [[tweet.id_str,
              tweet.created_at,
              tweet.favorite_count,
              tweet.retweet_count,
              tweet.full_text.encode("utf-8").decode("utf-8")]
             for idx, tweet in enumerate(tweets)]

df = pd.DataFrame(outtweets,
                  columns=['id',
                           'created_at',
                           'favorite_count',
                           'retweet_count',
                           'text'])
df = df[df['text'].str.contains(config('COVID_REGEX'))]
df['text'] = df['text'].str.lower()
df['text'] = df['text'].str.replace(' ', '')

for tweet_text, tweet_created_at in \
  zip(df['text'].tolist(), df['created_at'].tolist()):
    # remove undesired characters
    tweet_text = deEmojify(tweet_text)

    # date extraction
    date_match = re.search(r'\d{2}/\d{2}/\d{2}', tweet_text)
    date = datetime.strptime(date_match.group(), '%d/%m/%y').date()
    status['last_created'] = tweet_created_at.strftime("%d/%m/%Y %H:%M:%S")
    status['fecha'] = date.strftime("%d/%m/%Y")

    # data extraction
    for fragment in tweet_text.split('\n'):
        if 'hospitalizado' in fragment:
            subfragments = re.split(':|,|y', fragment)
            for subfragment in subfragments:
                if 'hospitalizado' in subfragment:
                    status['hospitalizado'] = re.findall('\d+', subfragment)[0]
                else:
                    for measure in hospitals:
                        if measure in subfragment:
                            values = re.findall('\d+', subfragment)
                            status[measure] = values[0]
                            status['uci_' +
                                   measure] = values[1] if (len(values) == 2)\
                                else ''
        else:
            for measure in status.keys():
                if measure in fragment:
                    status[measure] = re.findall('\d+', fragment)[0]
    results_rows.append(status.copy())

results_df = pd.DataFrame(results_rows, columns=status.keys())
print(results_df)
results_df.to_csv('./resultados.csv')
print(results_df[['fecha', 'altas']])
results_dataset = pyjstat.Dataset.read(results_df[['fecha', 'altas']], value='altas') 
print(results_dataset)
print(results_dataset.write())
