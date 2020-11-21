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

incidencia_municipios14 = pd.DataFrame()
incidencia_municipios7 = pd.DataFrame()
reference_date = datetime.strptime(data.tail(1)['Fecha'].iloc[0], "%Y-%m-%d")
reference_date = reference_date.strftime("%d-%m-%Y")

reference_date_df = pd.DataFrame(np.array([[reference_date, reference_date, reference_date]]),
                   columns=['Fecha', 'Variables', 'value'])

for municipio in municipios:
    data_municipio = data.loc[data['Municipio'] == municipio]
    casos_municipio = data_municipio[['Fecha', 'Municipio', 'NumeroCasos', 'NumeroCasosActivos', 'NumeroFallecidos', 'poblacion']]

    casos_municipio['Casos nuevos7'] = casos_municipio['NumeroCasos'].diff(periods=6)
    casos_municipio['Casos nuevos14'] = casos_municipio['NumeroCasos'].diff(periods=13)
    casos_municipio['Casos nuevos'] = casos_municipio['NumeroCasos'].diff()
    casos_municipio['Fallecidos diarios'] = casos_municipio['NumeroFallecidos'].diff()
    
    casos_municipio['Incidencia acumulada 7 días'] = round((casos_municipio['Casos nuevos7'] / casos_municipio['poblacion']) * 100000, 2)
    casos_municipio['Incidencia acumulada 14 días'] = round((casos_municipio['Casos nuevos14'] / casos_municipio['poblacion']) * 100000, 2)

    incidencia_municipio14 = casos_municipio[['Municipio', 'Incidencia acumulada 14 días']].tail(1)
    incidencia_municipios14 = incidencia_municipios14.append(incidencia_municipio14,ignore_index=True,sort=False)
    
    incidencia_municipio7 = casos_municipio[['Municipio', 'Incidencia acumulada 7 días']].tail(1)
    incidencia_municipios7 = incidencia_municipios7.append(incidencia_municipio7,ignore_index=True,sort=False)

    incidencia_municipio14 = casos_municipio[['Fecha', 'Municipio', 'Incidencia acumulada 14 días']]
    incidencia_municipio7 = casos_municipio[['Fecha', 'Municipio', 'Incidencia acumulada 7 días']]
    
    casos_diarios_municipio = casos_municipio[['Fecha', 'Municipio', 'Casos nuevos']]
    fallecidos_diarios_municipio = casos_municipio[['Fecha', 'Municipio', 'Fallecidos diarios']]
    activos_municipio = data_municipio[['Fecha', 'Municipio', 'NumeroCasosActivos']]
    tasa_municipio = data_municipio[['Fecha', 'Municipio', 'Tasa bruta de activos']]
    incidencia_municipio14 = to_json(incidencia_municipio14,
                                ['Fecha'], ['Incidencia acumulada 14 días'])
    incidencia_municipio7 = to_json(incidencia_municipio7,
                                ['Fecha'], ['Incidencia acumulada 7 días'])
    casos_diarios_municipio = to_json(casos_diarios_municipio,
                                ['Fecha', 'Municipio'],
                                ['Casos nuevos'])
    fallecidos_diarios_municipio = to_json(fallecidos_diarios_municipio,
                                ['Fecha', 'Municipio'],
                                ['Fallecidos diarios'])
    activos_municipio = to_json(activos_municipio,
                                ['Fecha', 'Municipio'],
                                ['NumeroCasosActivos'])
    tasa_municipio = to_json(tasa_municipio,
                                ['Fecha', 'Municipio'],
                                ['Tasa bruta de activos'])
    datasets['casos_diarios'] = pyjstat.Dataset.read(casos_diarios_municipio,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets['fallecidos'] = pyjstat.Dataset.read(fallecidos_diarios_municipio,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets['activos'] = pyjstat.Dataset.read(activos_municipio,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets['tasa'] = pyjstat.Dataset.read(tasa_municipio,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets['incidencia7'] = pyjstat.Dataset.read(incidencia_municipio7,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets['incidencia14'] = pyjstat.Dataset.read(incidencia_municipio14,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets['casos_diarios']["role"] = {"metric": ["Variables"]}
    datasets['fallecidos']["role"] = {"metric": ["Variables"]}
    datasets['activos']["role"] = {"metric": ["Variables"]}
    datasets['tasa']["role"] = {"metric": ["Variables"]}
    datasets['incidencia7']["role"] = {"metric": ["Variables"]}
    datasets['incidencia14']["role"] = {"metric": ["Variables"]}
    utils.publish_firebase('saludcantabria/municipios/' + municipio,
                           'casos-diarios',
                           datasets['casos_diarios'])
    utils.publish_firebase('saludcantabria/municipios/' + municipio,
                           'fallecidos',
                           datasets['fallecidos'])
    utils.publish_firebase('saludcantabria/municipios/' + municipio,
                           'activos',
                           datasets['activos'])
    utils.publish_firebase('saludcantabria/municipios/' + municipio,
                           'tasa',
                           datasets['tasa'])
    utils.publish_firebase('saludcantabria/municipios/' + municipio,
                           'incidencia7',
                           datasets['incidencia7'])
    utils.publish_firebase('saludcantabria/municipios/' + municipio,
                           'incidencia14',
                           datasets['incidencia14'])

incidencia_municipios14 = to_json(incidencia_municipios14,
                                ['Municipio'],
                                ['Incidencia acumulada 14 días'])
datasets['Incidencia acumulada'] = pyjstat.Dataset.read(incidencia_municipios14,
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
datasets['Incidencia acumulada']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'incidencia-acumulada', datasets['Incidencia acumulada'])
incidencia_municipios7 = to_json(incidencia_municipios7,
                                ['Municipio'],
                                ['Incidencia acumulada 7 días'])
datasets['Incidencia acumulada 7'] = pyjstat.Dataset.read(incidencia_municipios7,
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
