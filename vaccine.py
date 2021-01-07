"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
import numpy as np

import utils


vaccine = utils.read_vaccine_csv(cfg.input.vaccine_data)
vaccine = vaccine.loc[vaccine['CCAA'] == 'Cantabria']
vaccine['Fecha'] = pd.to_datetime(
        vaccine['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')
vaccine['Dosis no administradas'] = vaccine['Dosis entregadas'] - vaccine['Dosis administradas']
print(vaccine)

last_vaccine = vaccine.tail(1)
vaccine_reference_date = last_vaccine['Fecha'].iloc[0]
vaccine_percentage = last_vaccine['% sobre entregadas'].iloc[0]
dosis_entregadas = last_vaccine['Dosis entregadas'].iloc[0]
dosis_administradas = last_vaccine['Dosis administradas'].iloc[0]

vaccine_reference_date = pd.DataFrame(np.array([[vaccine_reference_date, vaccine_reference_date, vaccine_reference_date]]),
                   columns=['Fecha', 'Variables', 'value'])

vaccine_percentage = pd.DataFrame(np.array([[vaccine_percentage, vaccine_percentage, vaccine_percentage]]),
                   columns=['Fecha', 'Variables', 'value'])

dosis_entregadas = pd.DataFrame(np.array([[dosis_entregadas, dosis_entregadas, dosis_entregadas]]),
                   columns=['Fecha', 'Variables', 'value'])

dosis_administradas = pd.DataFrame(np.array([[dosis_administradas, dosis_administradas, dosis_administradas]]),
                   columns=['Fecha', 'Variables', 'value'])

data = {}

data['dosis'] = vaccine[['Fecha', 'Dosis no administradas', 'Dosis administradas']]
data['dosis'] = data['dosis'].melt(
    id_vars=['Fecha'], var_name='Variables')

# Publish datasets into Firebase
datasets = {}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

datasets['Fecha'] = pyjstat.Dataset.read(vaccine_reference_date,
                                             source=('Ministerio de Sanidad'))
datasets['Fecha']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'vaccine_reference_date', datasets['Fecha'])

datasets['Porcentaje'] = pyjstat.Dataset.read(vaccine_percentage,
                                             source=('Ministerio de Sanidad'))
datasets['Porcentaje']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'vaccine_percentage', datasets['Porcentaje'])

datasets['Dosis entregadas'] = pyjstat.Dataset.read(dosis_entregadas,
                                             source=('Ministerio de Sanidad'))
datasets['Dosis entregadas']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'dosis_entregadas', datasets['Dosis entregadas'])

datasets['Dosis administradas'] = pyjstat.Dataset.read(dosis_administradas,
                                             source=('Ministerio de Sanidad'))
datasets['Dosis administradas']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'dosis_administradas', datasets['Dosis administradas'])
for key in cfg.output.vaccine:
    data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Ministerio de Sanidad'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}

    utils.publish_firebase('saludcantabria',
                           cfg.output.vaccine[key],
                           datasets[key])
print('Vaccine published')

