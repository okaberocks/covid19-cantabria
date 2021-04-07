"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
import numpy as np

import utils

vaccine = utils.read_vaccine_csv()
# vaccine = vaccine.loc[vaccine['CCAA'] == 'Cantabria']
vaccine['Fecha'] = pd.to_datetime(
    vaccine['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')
vaccine['Dosis no administradas'] = vaccine['Dosis entregadas'] - \
    vaccine['Dosis administradas']

population = 581078

last_vaccine = vaccine.tail(1)

dosis_administradas = int(last_vaccine['Dosis administradas'].iloc[0])

suma_dosis = int(last_vaccine['Residencias'].iloc[0]) + \
                int(last_vaccine['Instituciones sanitarias'].iloc[0]) + \
                int(last_vaccine['Otras instituciones'].iloc[0]) + \
                int(last_vaccine['Dependientes y +80 años'].iloc[0]) + \
                int(last_vaccine['Mayores de 70 años'].iloc[0])

if (suma_dosis != dosis_administradas):
    print('¡La suma no coincide!')

vaccine_reference_date = last_vaccine['Fecha'].iloc[0]
vaccine_percentage = last_vaccine['% sobre entregadas'].iloc[0] + '%'
vaccine_acceptance = last_vaccine['Aceptación vacuna'].iloc[0] + '%'

# pautas_completadas = int(last_vaccine['Sanitarias completadas'].iloc[0]) + int(last_vaccine['Residencias completadas'].iloc[0])
# vaccine_population = (str(round(pautas_completadas * 100 / population, 2)) + '%').replace('.', ',')

vaccine_population = vaccine['Dosis administradas'] * 100 / population
vaccine_population = (str(round(vaccine_population.iloc[-1], 2)) + '%').replace('.', ',')

vaccine_population = last_vaccine['Porcentaje población cántabra'].iloc[0] + '%'
vaccine_population_complete = last_vaccine['Porcentaje población completa'].iloc[0] + '%'

dosis_entregadas = last_vaccine[['Fecha', 'Dosis entregadas']]
dosis_entregadas = dosis_entregadas.melt(id_vars=['Fecha'], var_name='Variables')
dosis_entregadas['value'] = dosis_entregadas['value'].apply(lambda x : "{:,}".format(x).replace(',','.'))

dosis_administradas = last_vaccine[['Fecha', 'Dosis administradas']]
dosis_administradas = dosis_administradas.melt(id_vars=['Fecha'], var_name='Variables')
dosis_administradas['value'] = dosis_administradas['value'].apply(lambda x : "{:,}".format(x).replace(',','.'))

vaccine_reference_date = pd.DataFrame(np.array([[vaccine_reference_date, vaccine_reference_date, vaccine_reference_date]]),
                                      columns=['Fecha', 'Variables', 'value'])
vaccine_percentage = pd.DataFrame(np.array([[vaccine_percentage, vaccine_percentage, vaccine_percentage]]),
                                  columns=['Fecha', 'Variables', 'value'])

vaccine_acceptance = pd.DataFrame(np.array([[vaccine_acceptance, vaccine_acceptance, vaccine_acceptance]]),
                                      columns=['Fecha', 'Variables', 'value'])

vaccine_population = pd.DataFrame(np.array([[vaccine_population, vaccine_population, vaccine_population]]),
                                      columns=['Fecha', 'Variables', 'value'])
vaccine_population_complete = pd.DataFrame(np.array([[vaccine_population_complete, vaccine_population_complete, vaccine_population_complete]]),
                                      columns=['Fecha', 'Variables', 'value'])

data = {}

data['dosis'] = vaccine[['Fecha', 'Dosis administradas', 'Dosis entregadas']]

data['dosis'] = data['dosis'].melt(
    id_vars=['Fecha'], var_name='Variables')
data['dosis']['Fecha'] = pd.to_datetime(
    data['dosis']['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
data['dosis'].sort_values(by=['Fecha', 'Variables'], inplace=True)
data['dosis']['Fecha'] = pd.to_datetime(
    data['dosis']['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')


data['tipo_dosis'] = vaccine[['Fecha', 'Residencias',
                         'Instituciones sanitarias', 'Otras instituciones', 'Dependientes y +80 años', 'Mayores de 70 años']].iloc[6:].tail(1)
data['tipo_dosis']['Residencias'] = data['tipo_dosis']['Residencias'].astype(int)
data['tipo_dosis']['Instituciones sanitarias'] = data['tipo_dosis']['Instituciones sanitarias'].astype(int)
data['tipo_dosis']['Otras instituciones'] = data['tipo_dosis']['Otras instituciones'].astype(int)
data['tipo_dosis']['Dependientes y +80 años'] = data['tipo_dosis']['Dependientes y +80 años'].astype(int)
data['tipo_dosis']['Mayores de 70 años'] = data['tipo_dosis']['Mayores de 70 años'].astype(int)
data['tipo_dosis'] = data['tipo_dosis'].melt(
    id_vars=['Fecha'], var_name='Variables')

data['tipo_dosis_completa'] = vaccine[['Fecha', 'Residencias completa',
                         'Instituciones sanitarias completa', 'Otras instituciones completa', 'Dependientes y +80 años completa']].iloc[6:].tail(1)
data['tipo_dosis_completa'] = data['tipo_dosis_completa'].rename(columns={"Residencias completa": "Residencias",
                                                                             "Instituciones sanitarias completa": "Instituciones sanitarias",
                                                                             "Otras instituciones completa": "Otras instituciones",
                                                                             "Dependientes y +80 años completa": "Dependientes y +80 años"})
data['tipo_dosis_completa']['Residencias'] = data['tipo_dosis_completa']['Residencias'].astype(int)
data['tipo_dosis_completa']['Instituciones sanitarias'] = data['tipo_dosis_completa']['Instituciones sanitarias'].astype(int)
data['tipo_dosis_completa']['Otras instituciones'] = data['tipo_dosis_completa']['Otras instituciones'].astype(int)
data['tipo_dosis_completa']['Dependientes y +80 años'] = data['tipo_dosis_completa']['Dependientes y +80 años'].astype(int)

data['tipo_dosis_completa'] = data['tipo_dosis_completa'].melt(
    id_vars=['Fecha'], var_name='Variables')
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

datasets['Porcentaje'] = pyjstat.Dataset.read(vaccine_percentage,
                                              source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
datasets['Porcentaje']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'vaccine_percentage', datasets['Porcentaje'])

datasets['Aceptacion'] = pyjstat.Dataset.read(vaccine_acceptance,
                                              source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
datasets['Aceptacion']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'vaccine_acceptance', datasets['Aceptacion'])


datasets['Poblacion'] = pyjstat.Dataset.read(vaccine_population,
                                              source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
datasets['Poblacion']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'vaccine_population', datasets['Poblacion'])

datasets['Poblacion completa'] = pyjstat.Dataset.read(vaccine_population_complete,
                                              source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
datasets['Poblacion completa']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'vaccine_population_complete', datasets['Poblacion completa'])

datasets['Dosis entregadas'] = pyjstat.Dataset.read(dosis_entregadas,
                                                    source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
datasets['Dosis entregadas']["role"] = {"metric": ["Variables"]}
utils.publish_firebase('saludcantabria', 'dosis_entregadas',
                       datasets['Dosis entregadas'])

datasets['Dosis administradas'] = pyjstat.Dataset.read(dosis_administradas,
                                                       source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
datasets['Dosis administradas']["role"] = {"metric": ["Variables"]}
utils.publish_firebase(
    'saludcantabria', 'dosis_administradas', datasets['Dosis administradas'])
for key in cfg.output.vaccine:
    # data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    utils.publish_firebase('saludcantabria',
                           cfg.output.vaccine[key],
                           datasets[key])
print('Vaccine published')
