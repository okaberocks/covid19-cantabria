"""Calculate 7-days simple moving average for relevant magnitudes."""
from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils

cases = utils.read_scs_csv(cfg.input.scs_data)

sma = cases[['Fecha', 'Casos Nuevos PCR', 'Fallecidos', 'Curados']]

sma['deaths_diff'] = sma['Fallecidos'].astype(float).diff()
sma['recovered_diff'] = sma['Curados'].astype(float).diff()

sma['Casos'] = sma['Casos Nuevos PCR'].rolling(7, center=True).mean().round()
sma['Fallecidos'] = sma['deaths_diff'].rolling(7, center=True).mean().round()
sma['Recuperados'] = sma['recovered_diff'].rolling(
    7, center=True).mean().round()

sma.drop(columns=['Casos Nuevos PCR',
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
