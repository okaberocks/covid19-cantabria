"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
import numpy as np

import utils


arima = utils.read_restimation(cfg.input.path + cfg.input.arima)

print(arima)

arima = arima[['Fecha', 'POSITIVOS', 'POSITIVOS_LI', 'POSITIVOS_LS']]
 
arima = arima.melt(id_vars=['Fecha'], var_name='Variables')
arima['Fecha'] = pd.to_datetime(arima['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
arima.sort_values(by=['Fecha', 'Variables'], inplace=True)
arima['Fecha'] = pd.to_datetime(arima['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')


arima_dataset = pyjstat.Dataset.read(arima,
                                   source=('Consejería de Sanidad '
                                           ' del Gobierno de '
                                           'Cantabria'))
arima_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

utils.publish_firebase('saludcantabria',
                       'arima',
                       arima_dataset)
print('arima published')
