"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
import numpy as np

import utils


redneu = utils.read_restimation(cfg.input.path + cfg.input.redneu)

print(redneu)

redneu = redneu[['Fecha', 'POSITIVOS', 'POSITIVOS_LI', 'POSITIVOS_LS']]
 
redneu = redneu.melt(id_vars=['Fecha'], var_name='Variables')
redneu['Fecha'] = pd.to_datetime(redneu['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
redneu.sort_values(by=['Fecha', 'Variables'], inplace=True)
redneu['Fecha'] = pd.to_datetime(redneu['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')


redneu_dataset = pyjstat.Dataset.read(redneu,
                                   source=('Consejería de Sanidad '
                                           ' del Gobierno de '
                                           'Cantabria'))
redneu_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

utils.publish_firebase('saludcantabria',
                       'redneu',
                       redneu_dataset)
print('redneu published')
