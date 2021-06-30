"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils

cases = utils.read_scs_csv(cfg.input.scs_data)

data = {}
data['accumulated'] = cases[['Fecha', 'Casos', 'UCI', 'Fallecidos']]
data['accumulated'] = data['accumulated'].melt(
    id_vars=['Fecha'], var_name='Variables')
data['accumulated'].reset_index(inplace=True)
data['accumulated'].drop('index', axis=1, inplace=True)

# Publish datasets into Firebase
datasets = {}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

for key in cfg.output.accumulated:
    data[key]['Fecha'] = pd.to_datetime(
        data[key]['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
    data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    data[key]['Fecha'] = pd.to_datetime(
        data[key]['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')
    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    utils.publish_firebase('saludcantabria',
                           cfg.output.accumulated[key],
                           datasets[key])

print('Accumulated published')
