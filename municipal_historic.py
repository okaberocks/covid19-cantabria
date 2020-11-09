"""Parse and load Cantabria's municipal covid19 data into Firebase."""
from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
import numpy as np
from datetime import datetime
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

indice_municipios = pd.DataFrame(columns=['Municipio', 'Incidencia acumulada'])
indice_municipios7 = pd.DataFrame(columns=['Municipio', 'Incidencia acumulada'])
reference_date = datetime.strptime(data.tail(1)['Fecha'].iloc[0], "%Y-%m-%d")
reference_date = reference_date.strftime("%d-%m-%Y")

reference_date_df = pd.DataFrame(np.array([[reference_date, reference_date, reference_date]]),
                   columns=['Fecha', 'Variables', 'value'])

for municipio in municipios:
    data_municipio = data.loc[data['Municipio'] == municipio]
    casos_municipio = data_municipio[['Fecha', 'Municipio', 'NumeroCasos', 'poblacion']]

    indice_acumulado = casos_municipio.tail(14).iloc[[0, -1]]
    indice_acumulado['Casos nuevos'] = indice_acumulado['NumeroCasos'].diff().tail(1)
    indice_acumulado['Incidencia acumulada'] = round((indice_acumulado['Casos nuevos'] / indice_acumulado['poblacion']) * 100000, 2)
    indice_municipio = indice_acumulado[['Municipio', 'Incidencia acumulada']].tail(1)
    indice_municipios = indice_municipios.append(indice_municipio,ignore_index=True,sort=False)

    indice_acumulado7 = casos_municipio.tail(7).iloc[[0, -1]]
    indice_acumulado7['Casos nuevos'] = indice_acumulado7['NumeroCasos'].diff().tail(1)
    indice_acumulado7['Incidencia acumulada'] = round((indice_acumulado7['Casos nuevos'] / indice_acumulado7['poblacion']) * 100000, 2)
    indice_municipio7 = indice_acumulado7[['Municipio', 'Incidencia acumulada']].tail(1)
    indice_municipios7 = indice_municipios7.append(indice_municipio7,ignore_index=True,sort=False)

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
indice_municipios = to_json(indice_municipios,
                                ['Municipio'],
                                ['Incidencia acumulada'])
datasets['Incidencia acumulada'] = pyjstat.Dataset.read(indice_municipios,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
datasets['Incidencia acumulada']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'incidencia-acumulada', datasets['Incidencia acumulada'])
indice_municipios7 = to_json(indice_municipios7,
                                ['Municipio'],
                                ['Incidencia acumulada'])
datasets['Incidencia acumulada 7'] = pyjstat.Dataset.read(indice_municipios7,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
datasets['Incidencia acumulada 7']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'incidencia-acumulada-7', datasets['Incidencia acumulada 7'])

datasets['Fecha'] = pyjstat.Dataset.read(reference_date_df,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
datasets['Fecha']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'date', datasets['Fecha'])

print('Municipal historic published')
