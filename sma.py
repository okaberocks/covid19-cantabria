import pandas as pd

from pyjstat import pyjstat

import requests

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

from cfg import cfg

import utils

cases = pd.read_csv(cfg.input.scs_data)
# cases = pd.read_csv('https://www.scsalud.es/documents/2162705/9255280/2020_covid19_historico.csv', na_filter=False, dtype={'CASOS RESIDENCIAS': object, 'AISLAMIENTO DOM.': object, 'TEST PCR': object, 'PERSONAS TEST': object})
""" cases = pd.read_excel(cfg.input.scs_data, na_filter=False,
                      dtype={'CASOS RESIDENCIAS': object,
                             'AISLAMIENTO DOM.': object,
                             'TEST PCR': object,
                             'PERSONAS TEST': object,
                             'FECHA': object}) """

cases = cases.loc[:, ~cases.columns.str.contains('^Unnamed')]
#cases[cases.columns.difference(['FECHA*', 'CASOS RESIDENCIAS', 'AISLAMIENTO DOM.', 'TEST PCR', 'PERSONAS TEST'])] = cases[cases.columns.difference(['FECHA*', 'CASOS RESIDENCIAS', 'AISLAMIENTO DOM.', 'TEST PCR', 'PERSONAS TEST'])].apply(
#    lambda x: pd.to_numeric(x, downcast='integer'))
cases.columns = cases.columns.str.title()
cases.columns = cases.columns.str.replace('Fecha\*', 'Fecha')
cases.columns = cases.columns.str.replace('Uci', 'UCI')
cases.columns = cases.columns.str.replace('Pcr', 'PCR')
cases.columns = cases.columns.str.replace('HOSP. ', '')
cases.columns = cases.columns.str.replace('Humv', 'Valdecilla')
print(cases)
cases['Fecha'] = cases['Fecha'].str.replace('**', '', regex=False)
print(cases)
cases.drop(cases.tail(3).index, inplace=True)

sma = cases[['Fecha', 'Casos Nuevos', 'Fallecidos', 'Curados']]


sma['deaths_diff'] = sma['Fallecidos'].astype(float).diff()
sma['recovered_diff'] = sma['Curados'].astype(float).diff()

sma['Casos'] = sma['Casos Nuevos'].rolling(7, center=True).mean().round()
sma['Fallecidos'] = sma['deaths_diff'].rolling(7, center=True).mean().round()
sma['Recuperados'] = sma['recovered_diff'].rolling(7, center=True).mean().round()

sma.drop(columns=['Casos Nuevos',
                  'deaths_diff', 'Curados',
                  'recovered_diff'], inplace=True)

sma.dropna(inplace=True)

sma = sma.melt(id_vars=['Fecha'], var_name='Variables')

sma['Fecha'] = pd.to_datetime(sma['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
sma.sort_values(by=['Fecha', 'Variables'], inplace=True)

sma_dataset = pyjstat.Dataset.read(sma,
                                   source=('Consejería de Sanidad '
                                           ' del Gobierno de '
                                           'Cantabria'))
sma_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}

print(sma_dataset)

utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)

utils.publish_firebase('saludcantabria',
                       'sma',
                       sma_dataset)
