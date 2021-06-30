"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
import numpy as np

import utils


rho = utils.read_rho_csv(cfg.input.rho)

rho = rho[['Fecha', 'Media (R)', 'Cuantil 0,025 (R)', 'Cuantil 0,975 (R)']]

rho = rho.melt(id_vars=['Fecha'], var_name='Variables')
rho['Fecha'] = pd.to_datetime(
    rho['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
rho.sort_values(by=['Fecha', 'Variables'], inplace=True)
rho['Fecha'] = pd.to_datetime(
    rho['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')


rho_dataset = pyjstat.Dataset.read(rho,
                                   source=('Consejería de Sanidad '
                                           ' del Gobierno de '
                                           'Cantabria'))
rho_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

utils.publish_firebase('saludcantabria',
                       'rho',
                       rho_dataset)
print('Rho published')
