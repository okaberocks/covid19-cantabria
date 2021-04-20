"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
import numpy as np

import utils


loess = utils.read_restimation(cfg.input.loess)

loess = loess[['Fecha', 'Positivos', 'Positivos LI', 'Positivos LS']]
 
loess['Pronósticos'] = loess['Positivos']
loess.loc[loess.tail(14).index, 'Positivos'] = None
loess.loc[loess.head(len(loess) - 14).index, 'Pronósticos'] = None

loess = loess.melt(id_vars=['Fecha'], var_name='Variables')
loess['Fecha'] = pd.to_datetime(loess['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
loess.sort_values(by=['Fecha', 'Variables'], inplace=True)
loess['Fecha'] = pd.to_datetime(loess['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')


loess_dataset = pyjstat.Dataset.read(loess,
                                   source=('Consejería de Sanidad '
                                           ' del Gobierno de '
                                           'Cantabria'))
loess_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

utils.publish_firebase('saludcantabria',
                       'loess',
                       loess_dataset)
print('loess published')
