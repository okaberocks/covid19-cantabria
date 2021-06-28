"""Generate current situation json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
from datetime import datetime

import utils


def to_json(df, id_vars, value_vars):
    """Export dataframe to JSON-Stat dataset.

        id_vars (list): index columns
        value_vars (list): numeric variables (metrics)
    """
    df = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='Variables')
    id_vars.append('Variables')
    df = df.sort_values(by=id_vars)
    # df = df.groupby(id_vars, as_index=False)['value'].sum()
    return df


# READ DATA FROM CSV.
# TODO: REFACTOR TO UTILS, SINCE THIS IS USED BY MORE MODULES
data = {}

df = pd.DataFrame()
df = utils.read_scs_historic_age()

df = df.drop(['Sexo'], axis=1)

df = df.groupby(['Fecha', 'Rango_edad'], as_index=False).sum()
df['Fecha'] = pd.to_datetime(
    df['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
df['Casos'] = df['Casos'].astype(int)
df['Casos nuevos14'] = df['Casos'].diff(periods=14*11)
df['Casos nuevos7'] = df['Casos'].diff(periods=7*11)
df['Casos nuevos'] = df['Casos'].diff(periods=1*11)
df = df.fillna(0)
df['Casos nuevos'] = df['Casos nuevos'].astype(int)

population = cfg.cantabria_population

df['IA 7 días'] = (df['Casos nuevos7'] / population) * 100000
df['IA 14 días'] = (df['Casos nuevos14'] / population) * 100000

df.to_csv('test.csv')

df_age_daily = df[['Fecha', 'Rango_edad', 'Casos nuevos']]
df_age_daily = to_json(df_age_daily, ['Fecha', 'Rango_edad'], ['Casos nuevos'])

# Publish datasets into Firebase
datasets = {}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

df_age_daily['Fecha'] = pd.to_datetime(
    df_age_daily['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')
datasets['Histórico_edad'] = pyjstat.Dataset.read(df_age_daily,
                                                  source=('Consejería de Sanidad '
                                                          ' del Gobierno de '
                                                          'Cantabria'))
datasets['Histórico_edad']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'age_historic', datasets['Histórico_edad'])


print('Age historic published')
