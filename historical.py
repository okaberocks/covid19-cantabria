"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


# cases = pd.read_csv(cfg.input.path + cfg.input.hospitals)
cases = pd.read_csv(cfg.input.scs_data, na_filter=False,
                    dtype={'CASOS RESIDENCIAS': object,
                           'AISLAMIENTO DOM.': object,
                           'TEST PCR': object,
                           'PERSONAS TEST': object})
""" cases = pd.read_excel(cfg.input.scs_data, na_filter=False,
                      dtype={'CASOS RESIDENCIAS': object,
                             'AISLAMIENTO DOM.': object,
                             'TEST PCR': object,
                             'PERSONAS TEST': object,
                             'FECHAS': object}) """
cases = cases.loc[:, ~cases.columns.str.contains('^Unnamed')]
cases.columns = cases.columns.str.title()
cases.columns = cases.columns.str.replace('Fecha\*', 'Fecha')
cases.columns = cases.columns.str.replace('Uci', 'UCI')
cases.columns = cases.columns.str.replace('Pcr', 'PCR')
cases.columns = cases.columns.str.replace('Hosp. ', '')
cases.columns = cases.columns.str.replace('Prof. ', '')
cases.columns = cases.columns.str.replace('Humv', 'Valdecilla')
cases['Fecha'] = cases['Fecha'].str.replace('\*\*', '')
cases.drop(cases.tail(3).index, inplace=True)

data = {}

data['elder'] = cases[['Fecha', 'Casos Residencias']]
data['elder'] = data['elder'].melt(id_vars=['Fecha'], var_name='Variables')

data['test'] = cases[['Fecha', 'Test PCR']]
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

    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    print(datasets[key].write())
    utils.publish_firebase('saludcantabria',
                           cfg.output.historical[key],
                           datasets[key])

