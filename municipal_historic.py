"""Parse and load Cantabria's municipal covid19 data into Firebase."""
from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils

def to_json(df, id_vars, value_vars):
    """Export dataframe to JSON-Stat dataset.
    
        id_vars (list): index columns
        value_vars (list): numeric variables (metrics)
    """
    df = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='Variables')
    id_vars.append('Variables')
    df = df.sort_values(by=id_vars)
    # df = df.groupby(['Fecha','municipios'], as_index=False)['value'].sum()
    return df

data = utils.read_scs_historic_municipal()
population = pd.read_csv(cfg.input.path + cfg.input.population)
# data.sort_values(by=['Codigo'], inplace=True)
data.reset_index(inplace=True)

data['Codigo'] = data['Codigo'].apply(str)
population['Codigo'] = population['Codigo'].apply(str)
data = pd.merge(data, population, on='Codigo')

data['Tasa bruta de activos'] = round((data['NumeroCasosActivos'] / data['poblacion']) * 100000, 2)
data['Fecha'] = data['Fecha'].astype(str)
measures = {}
datasets = {}
jsonstat = {}

data['Municipio'] = data['Codigo'] + ' - ' + data['Texto']
municipios = data['Municipio'].unique()
municipios_historico = {}
    
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass


for municipio in municipios:
    data_municipio = data.loc[data['Municipio'] == municipio]
    activos_municipio = data_municipio[['Fecha', 'Municipio', 'NumeroCasosActivos']]
    tasa_municipio = data_municipio[['Fecha', 'Municipio', 'Tasa bruta de activos']]
    activos_municipio = to_json(activos_municipio,
                                ['Fecha', 'Municipio'],
                                ['NumeroCasosActivos'])
    tasa_municipio = to_json(tasa_municipio,
                                ['Fecha', 'Municipio'],
                                ['Tasa bruta de activos'])
    # activos_municipio.drop(columns=['Variables', ], inplace=True)
    # tasa_municipio.drop(columns=['Variables', ], inplace=True)
    datasets['activos'] = pyjstat.Dataset.read(activos_municipio,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets['tasa'] = pyjstat.Dataset.read(tasa_municipio,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets['activos']["role"] = {"metric": ["Variables"]}
    datasets['tasa']["role"] = {"metric": ["Variables"]}
    utils.publish_firebase('saludcantabria/municipios/' + municipio,
                           'activos',
                           datasets['activos'])
    utils.publish_firebase('saludcantabria/municipios/' + municipio,
                           'tasa',
                           datasets['tasa'])

for measure in cfg.municipalities_historic.measures:
    measures[measure] = data[['Fecha', 'Codigo',
                              'Texto',
                              cfg.municipalities.measures[measure].original]]
    measures[measure]['municipios'] = measures[measure]['Codigo'] + \
        ' - ' + measures[measure]['Texto']
    measures[measure].drop(columns=['Codigo', 'Texto', ], inplace=True)
    measures[measure].rename(
        columns={cfg.municipalities_historic.measures[measure].original:
                 cfg.municipalities_historic.measures[measure].final}, inplace=True)
    measures[measure] = to_json(measures[measure],
                                ['Fecha', 'municipios'],
                                [cfg.municipalities_historic.measures[measure].final])
    # measures[measure].drop(columns=['Variables', ], inplace=True)
    datasets[measure] = pyjstat.Dataset.read(measures[measure],
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets[measure]["role"] = {"metric": ["Variables"]}
    # utils.publish_firebase('saludcantabria/test',
    #                        cfg.output.municipalities_historic[measure],
    #                        datasets[measure])
print('Municipal historic published')
