"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
import numpy as np

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
    # df = df.groupby(id_vars, as_index=False)['value'].sum()
    return df


vaccine = utils.read_vaccine_general()
vaccine['Fecha'] = pd.to_datetime(
    vaccine['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')

vaccine_age = utils.read_vaccine_age()
vaccine_age['Fecha'] = pd.to_datetime(
    vaccine_age['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')
last_vaccine = vaccine.tail(1)

vaccine_reference_date = last_vaccine['Fecha'].iloc[0]

personas_vacunadas = last_vaccine[['Fecha', 'Personas vacunadas']]
personas_vacunadas = personas_vacunadas.melt(
    id_vars=['Fecha'], var_name='Variables')
personas_vacunadas['value'] = personas_vacunadas['value'].apply(
    lambda x: "{:,}".format(x).replace(',', '.'))

personas_inmunizadas = last_vaccine[['Fecha', 'Personas inmunizadas']]
personas_inmunizadas = personas_inmunizadas.melt(
    id_vars=['Fecha'], var_name='Variables')
personas_inmunizadas['value'] = personas_inmunizadas['value'].apply(
    lambda x: "{:,}".format(x).replace(',', '.'))

dosis_administradas = last_vaccine[['Fecha', 'Vacunas administradas']]
dosis_administradas = dosis_administradas.melt(
    id_vars=['Fecha'], var_name='Variables')
dosis_administradas['value'] = dosis_administradas['value'].apply(
    lambda x: "{:,}".format(x).replace(',', '.'))

poblacion_vacunada = last_vaccine[['Fecha', '% Población vacunada']]
poblacion_vacunada = poblacion_vacunada.melt(
    id_vars=['Fecha'], var_name='Variables')
poblacion_vacunada['value'] = poblacion_vacunada['value'].apply(
    lambda x: "{:,}".format(x).replace(',', '.'))

poblacion_inmunizada = last_vaccine[['Fecha', '% Población inmunizada']]
poblacion_inmunizada = poblacion_inmunizada.melt(
    id_vars=['Fecha'], var_name='Variables')
poblacion_inmunizada['value'] = poblacion_inmunizada['value'].apply(
    lambda x: "{:,}".format(x).replace(',', '.'))

today = vaccine_reference_date
vaccine_reference_date = pd.DataFrame(np.array([[vaccine_reference_date, vaccine_reference_date, vaccine_reference_date]]),
                                      columns=['Fecha', 'Variables', 'value'])
data = {}

# Publish datasets into Firebase
datasets = {}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

datasets['Fecha'] = pyjstat.Dataset.read(vaccine_reference_date,
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
datasets['Fecha']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'vaccine_reference_date', datasets['Fecha'])

datasets['Poblacion'] = pyjstat.Dataset.read(poblacion_vacunada,
                                             source=('Consejería de Sanidad '
                                                     ' del Gobierno de '
                                                     'Cantabria'))
datasets['Poblacion']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'vaccine_population', datasets['Poblacion'])

datasets['Poblacion completa'] = pyjstat.Dataset.read(poblacion_inmunizada,
                                                      source=('Consejería de Sanidad '
                                                              ' del Gobierno de '
                                                              'Cantabria'))
datasets['Poblacion completa']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'vaccine_population_complete', datasets['Poblacion completa'])

datasets['Personas vacunadas'] = pyjstat.Dataset.read(personas_vacunadas,
                                                      source=('Consejería de Sanidad '
                                                              ' del Gobierno de '
                                                              'Cantabria'))
datasets['Personas vacunadas']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'personas_vacunadas',
                       datasets['Personas vacunadas'])

datasets['Personas inmunizadas'] = pyjstat.Dataset.read(personas_inmunizadas,
                                                        source=('Consejería de Sanidad '
                                                                ' del Gobierno de '
                                                                'Cantabria'))
datasets['Personas inmunizadas']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'personas_inmunizadas',
                       datasets['Personas inmunizadas'])

datasets['Vacunas administradas'] = pyjstat.Dataset.read(dosis_administradas,
                                                         source=('Consejería de Sanidad '
                                                                 ' del Gobierno de '
                                                                 'Cantabria'))
datasets['Vacunas administradas']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'dosis_administradas', datasets['Vacunas administradas'])

vaccine_age = vaccine_age.loc[vaccine_age['Fecha'] == today]
vaccine_age = vaccine_age[['Edad', 'Tipo', '% Personas']]
data['vaccine_age'] = to_json(vaccine_age, ['Edad', 'Tipo'], ['% Personas'])

vaccine_types = utils.read_vaccine_types()
vaccine_types['Fecha'] = pd.to_datetime(
    vaccine_types['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')
vaccine_types_historic = vaccine_types

vaccine_types = vaccine_types.loc[vaccine_types['Fecha'] == today]
vaccine_types = vaccine_types[['Vacuna', 'Tipo', 'Dosis']]
data['vaccine_types'] = to_json(vaccine_types, ['Vacuna', 'Tipo'], ['Dosis'])

vaccine_week = utils.read_vaccine_week()
data['vaccine_week'] = to_json(vaccine_week, ['Semana'], [
                               'Inmunizados', '1ª dosis'])

vaccine_first = vaccine_types_historic['Tipo'] == 'Personas 1ª dosis'
vaccine_first = vaccine_types_historic.loc[vaccine_types_historic['Tipo']
                                           == 'Personas 1ª dosis']
vaccine_first['Fecha'] = pd.to_datetime(
    vaccine_first['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
vaccine_first.sort_values(by=['Fecha', 'Vacuna'], inplace=True)
vaccine_first['Dosis diarias'] = vaccine_first['Dosis'].diff(periods=4)
vaccine_first = vaccine_first[['Fecha', 'Vacuna', 'Dosis diarias']]
vaccine_first = vaccine_first.pivot(index='Fecha', columns='Vacuna', values='Dosis diarias')
vaccine_first = vaccine_first.reset_index()
data['daily_types_first'] = vaccine_first.melt(
    id_vars=['Fecha'], var_name='Variables')

vaccine_second = vaccine_types_historic['Tipo'] == 'Personas Inmunizadas'
vaccine_second = vaccine_types_historic.loc[vaccine_types_historic['Tipo']
                                           == 'Personas Inmunizadas']
vaccine_second['Fecha'] = pd.to_datetime(
    vaccine_second['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
vaccine_second.sort_values(by=['Fecha', 'Vacuna'], inplace=True)
vaccine_second['Dosis diarias'] = vaccine_second['Dosis'].diff(periods=4)
vaccine_second = vaccine_second[['Fecha', 'Vacuna', 'Dosis diarias']]
vaccine_second = vaccine_second.pivot(index='Fecha', columns='Vacuna', values='Dosis diarias')
vaccine_second = vaccine_second.reset_index()
data['daily_types_second'] = vaccine_second.melt(
    id_vars=['Fecha'], var_name='Variables')

for key in cfg.output.vaccine_new:

    # data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    datasets[key]['dimension']['Variables']['category']['unit'] = cfg.output.vaccine_new[key].units

    utils.publish_firebase('saludcantabria',
                           cfg.output.vaccine_new[key].name,
                           datasets[key])
print('Vaccine published')
