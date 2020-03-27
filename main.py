"""Recover Twitter covid19 data in Cantabria."""

import re
from datetime import datetime

from cfg import cfg

import pandas as pd

import tweepy

from pyjstat import pyjstat

# this dict is used in the code, so I'd rather not to extract it to config

status = {'altas': '',
          'positivo': '',
          'fallecido': '',
          'activo': '',
          'domicilio': '',
          'sanitario': ''}

results_rows = []


def deEmojify(inputString):
    """Remove emojis and other annoying characters."""
    return inputString.encode('ascii', 'ignore').decode('ascii')


def extract_hospital_value(variable):
    result = cfg.hospitals.get(variable)
    if result:
        return result['name']
    else:
        return cfg.hospitals[variable[4:]]['name']


def rename_variable_column(variable):
    if variable.startswith('uci_'):
        return 'UCI'
    else:
        return 'Ingresados'


def put_lat(variable):
    if variable.startswith('uci_'):
        return cfg.hospitals[variable[4:]]['lat']
    else:
        return cfg.hospitals.get(variable)['lat']


def put_lon(variable):
    if variable.startswith('uci_'):
        return cfg.hospitals[variable[4:]]['lon']
    else:
        return cfg.hospitals.get(variable)['lon']


def write_to_file(json_data, file_name):
    file = open(file_name, 'w')
    file.write(json_data)
    file.close()


def generate_coords_df():
    
    rows = []
    for key in cfg.hospitals.keys():
        row = {}
        row['Variables'] = 'Latitud'
        row['value'] = cfg.hospitals[key].lat
        row['Hospital'] = cfg.hospitals[key].name
        rows.append(row)
        row = {}
        row['Variables'] = 'Longitud'
        row['value'] = cfg.hospitals[key].lon
        row['Hospital'] = cfg.hospitals[key].name
        rows.append(row)
    return pd.DataFrame(rows, columns=['Variables', 'value', 'Hospital'])


# MAIN SCRIPT

# complete authorization and initialize API endpoint
auth = tweepy.OAuthHandler(cfg.twitter.api_key, cfg.twitter.api_secret_key)
auth.set_access_token(cfg.twitter.access_token,
                      cfg.twitter.access_token_secret)
api = tweepy.API(auth)

# retrieve tweets from the account providing the data
tweets = api.user_timeline(screen_name=cfg.twitter.user_id,
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
df = df[df['text'].str.contains(cfg.twitter.covid_regex)]
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
                    for measure in cfg.hospitals.keys():
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

# CURRENT SITUATION
current_sit_df = results_df[['fecha', 'hospitalizado',
                             'domicilio', 'altas']].loc[[0]]
current_sit_df = current_sit_df.melt(id_vars=['fecha'],
                                     var_name='Variables')

# HOSPITALS
# extract first row as a dataframe
hospitals_df = results_df[['valdecilla', 'uci_valdecilla',
                           'sierrallana', 'uci_sierrallana',
                           'tresmares', 'uci_tresmares',
                           'laredo', 'uci_laredo']].loc[[0]]
hospitals_df = hospitals_df.melt(var_name='Variables')
hospitals_df['Hospital'] = hospitals_df['Variables'].apply(
    extract_hospital_value)
# hospitals_df = hospitals_df.melt(id_vars=['Latitud', 'Longitud'])
hospitals_df['Variables'] = hospitals_df['Variables'].apply(
    rename_variable_column)
coords_df = generate_coords_df()

hospitals_df = hospitals_df.append(coords_df, ignore_index=True)
hospitals_df = hospitals_df[['Hospital', 'Variables', 'value']]
hospitals_df.sort_values(by=['Hospital'], inplace=True)
results_df.to_csv('./resultados.csv')

# pd.melt(results_df, id_vars=['fecha']

hospitals_dataset = pyjstat.Dataset.read(hospitals_df,
                                         source=('Consejería de Sanidad del '
                                                 'Gobierno de Cantabria'),
                                         updated=date)
current_sit_dataset = pyjstat.Dataset.read(current_sit_df,
                                           source=('Consejería de Sanidad del '
                                                   'Gobierno de Cantabria'),
                                           updated=date)
print(hospitals_df)
print(current_sit_df)
hospitals_dataset["role"] = {"metric": ["Variables"]}
current_sit_dataset["role"] = {"time": ["dia"], "metric": ["Variables"]}
hospitals_json = hospitals_dataset.write()
current_sit_json = current_sit_dataset.write()
print(hospitals_json)
print(current_sit_json)
write_to_file(hospitals_json,
              cfg.output.path + cfg.output.hospitals)
write_to_file(current_sit_json,
              cfg.output.path + cfg.output.current_situation)