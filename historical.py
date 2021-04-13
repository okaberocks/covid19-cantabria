"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


cases = utils.read_scs_csv(cfg.input.scs_data)

data = {}

cases["Test PCR"] = pd.to_numeric(cases["Test PCR"])
cases["Test Anticuerpos"] = pd.to_numeric(cases["Test Anticuerpos"])
cases["Test Anticuerpos +"] = pd.to_numeric(cases["Test Anticuerpos +"])
cases["Test Antigenos +"] = pd.to_numeric(cases["Test Antigenos +"])
cases["Test Antigenos"] = pd.to_numeric(cases["Test Antigenos"])

cases['Casos nuevos dia'] = cases['Casos'].diff()
cases['Test PCR diarios'] = cases['Test PCR'].diff()
cases['Test Anticuerpos diarios'] = cases['Test Anticuerpos'].diff()
cases['Test Antigenos diarios'] = cases['Test Antigenos'].diff()
cases['Positividad'] = cases['Casos nuevos dia'] * 100 / cases['Test PCR diarios']
cases['Positividad'] = cases['Positividad'].iloc[3:]

data['daily_test'] = cases[['Fecha', 'Test PCR diarios', 'Test Anticuerpos diarios', 'Test Antigenos diarios']]
data['daily_test'] = data['daily_test'].melt(id_vars=['Fecha'], var_name='Variables')

data['positivity'] = cases[['Fecha', 'Positividad']]
data['positivity'] = data['positivity'].melt(id_vars=['Fecha'], var_name='Variables')

data['test'] = cases[['Fecha', 'Test PCR', 'Test Anticuerpos']]
data['test'] = data['test'].melt(id_vars=['Fecha'], var_name='Variables')

data['incidence'] = cases[['Fecha', 'Incidencia 14 dias']]
data['incidence'] = data['incidence'].melt(id_vars=['Fecha'], var_name='Variables')

data['active_sanitarians'] = cases[['Fecha', 'Sanitarios Activos']]
data['active_sanitarians'] = data['active_sanitarians'].melt(
    id_vars=['Fecha'], var_name='Variables')

data['active_elder'] = cases[['Fecha', 'Residencias Activos']]
data['active_elder'] = data['active_elder'].melt(
    id_vars=['Fecha'], var_name='Variables')

data['sanitarians'] = cases[['Fecha', 'Sanitarios', 'Sanitarios Activos']]
data['sanitarians'] = data['sanitarians'].rename(columns={"Sanitarios": "Sanitarios Acumulados"})
data['sanitarians'] = data['sanitarians'].melt(
    id_vars=['Fecha'], var_name='Variables')

data['elder'] = cases[['Fecha', 'Residencias Activos', 'Casos Residencias']]
data['elder'] = data['elder'].rename(columns={"Casos Residencias": "Residencias Acumulados"})
data['elder'] = data['elder'].melt(
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
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

for key in cfg.output.historical:
    data[key]['Fecha'] = pd.to_datetime(
        data[key]['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
    data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    data[key]['Fecha'] = pd.to_datetime(
        data[key]['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')
    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    # if key == "sanitarians":
    #     datasets[key]["note"] = [cfg.labels.daily_note]
    datasets[key]['dimension']['Variables']['category']['unit'] = cfg.output.historical[key].units
    utils.publish_firebase('saludcantabria',
                           cfg.output.historical[key].name,
                           datasets[key])
print('Historical published')

