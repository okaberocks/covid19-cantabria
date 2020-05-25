"""Parse and load Cantabria's municipal covid19 data into Firebase."""
from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils

data = utils.read_scs_municipal(cfg.input.scs_data_municipal)
population = pd.read_csv(cfg.input.path + cfg.input.population)
data.sort_values(by=['Codigo'], inplace=True)
data.reset_index(inplace=True)
if data.at[52, 'Texto'] == '':
    data.at[52, 'Texto'] = 'Polaciones'

population['Codigo'] = population['Codigo'].apply(str)
print(data.dtypes)
print(population.dtypes)
data = pd.merge(data, population, on='Codigo')

print(data)
data['Tasa bruta de casos'] = (
    data['NumeroCasos'] / data['poblacion']) * 100000
data['Tasa bruta de activos'] = (
    data['NumeroCasosActivos'] / data['poblacion']) * 100000
data['Tasa bruta de altas'] = (
    data['NumeroCurados'] / data['poblacion']) * 100000
data['Tasa bruta de fallecidos'] = (
    data['NumeroFallecidos'] / data['poblacion']) * 100000
print(data)

measures = {}
datasets = {}
jsonstat = {}

utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)

for measure in cfg.municipalities.measures:
    measures[measure] = data[['Codigo',
                              'Texto',
                              cfg.municipalities.measures[measure].original]]
    measures[measure]['municipios'] = measures[measure]['Codigo'] + \
        ' - ' + measures[measure]['Texto']
    measures[measure].drop(columns=['Codigo', 'Texto'], inplace=True)
    measures[measure].rename(
        columns={cfg.municipalities.measures[measure].original:
                 cfg.municipalities.measures[measure].final}, inplace=True)
    measures[measure] = measures[measure].melt(
        id_vars='municipios',
        value_vars=[cfg.municipalities.measures[measure].final],
        var_name='Variables')
    datasets[measure] = pyjstat.Dataset.read(measures[measure],
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets[measure]["role"] = {"metric": ["Variables"]}
    utils.publish_firebase('saludcantabria',
                           cfg.output.municipalities[measure],
                           datasets[measure])
    print(datasets[measure].write())
