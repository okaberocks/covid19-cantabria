"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


cases = utils.read_scs_csv(cfg.input.scs_data)

data = {}

data['elder'] = cases[['Fecha', 'Casos Residencias']]
data['elder'] = data['elder'].melt(id_vars=['Fecha'], var_name='Variables')

data['test'] = cases[['Fecha', 'Total Test']]
data['test'] = data['test'].melt(id_vars=['Fecha'], var_name='Variables')

data['sanitarians'] = cases[['Fecha', 'Sanitarios']]
data['sanitarians'] = data['sanitarians'].melt(
    id_vars=['Fecha'], var_name='Variables')

hospitals = {}
ucis = {}
for key in cfg.hospitals.keys():
    hospitals[key] = cases[['Fecha'] +
                           [col for col in cases.columns
                               if cfg.hospitals[key].name in col
                               and 'UCI' not in col]]
  
    ucis[key] = cases[['Fecha'] +
                      [col for col in cases.columns
                          if cfg.hospitals[key].name in col
                          and 'UCI' in col]]

    # Format the dataframes for generating json-stat for stat-viewer
    hospitals[key] = hospitals[key].melt(
        id_vars=['Fecha'],
        var_name='Variables'
    )

    ucis[key] = ucis[key].melt(
        id_vars=['Fecha'],
        var_name='Variables'
    )

data['hospitalizations'] = pd.concat(list(hospitals.values()))
data['hospitalizations'].reset_index(inplace=True)
data['hospitalizations'].drop('index', axis=1, inplace=True)

data['ucis'] = pd.concat(list(ucis.values()))
data['ucis'].reset_index(inplace=True)
data['ucis'].drop('index', axis=1, inplace=True)

# Publish datasets into Firebase
datasets = {}
utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)

for key in cfg.output.historical:
    data[key]['Fecha'] = pd.to_datetime(
        data[key]['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
    data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    print(datasets[key].write())
    utils.publish_firebase('saludcantabria',
                           cfg.output.historical[key],
                           datasets[key])

