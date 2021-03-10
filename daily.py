"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd
from pyjstat import pyjstat

import utils


cases = utils.read_scs_csv(cfg.input.scs_data)

data = {}
data['daily_cases'] = cases[['Fecha', 'Casos']]
data['daily_cases'] = data['daily_cases'].melt(id_vars=['Fecha'], var_name='Variables')
data['daily_cases']['value'] = data['daily_cases']['value'].diff()

# data['daily_discharged'] = cases[['Fecha', 'Recuperados']]
# data['daily_discharged'] = data['daily_discharged'].melt(id_vars=['Fecha'], var_name='Variables')
# data['daily_discharged']['value'] = data['daily_discharged']['value'].diff()

data['daily_deceases'] = cases[['Fecha', 'Fallecidos']]
data['daily_deceases'] = data['daily_deceases'].melt(id_vars=['Fecha'], var_name='Variables')
data['daily_deceases']['value'] = data['daily_deceases']['value'].diff()

data['daily_types'] = cases[['Fecha', 'Casos', 'Casos Residencias', 'Sanitarios']]
data['daily_types'] = data['daily_types'].iloc[17:]
data['daily_types']['Casos'] = data['daily_types']['Casos'].diff()
data['daily_types']['Casos Residencias'] = data['daily_types']['Casos Residencias'].astype(int)
data['daily_types']['Casos Residencias'] = data['daily_types']['Casos Residencias'].diff()
data['daily_types']['Sanitarios'] = data['daily_types']['Sanitarios'].diff()
data['daily_types']['Otros'] = data['daily_types']['Casos'] -\
                                data['daily_types']['Sanitarios'] -\
                                data['daily_types']['Casos Residencias']
data['daily_types'] = data['daily_types'][['Fecha', 'Otros', 'Casos Residencias', 'Sanitarios']]
data['daily_types'] = data['daily_types'].rename(columns={"Casos Residencias": "Residencias"})
# Set None if negative value
# data['daily_types'].loc[(data['daily_types']['Otros'] < 0),
#                     ['Otros', 'Residencias', 'Sanitarios']] = None
# data['daily_types'].loc[(data['daily_types']['Residencias'] < 0),
#                     ['Otros', 'Residencias', 'Sanitarios']] = None
# data['daily_types'].loc[(data['daily_types']['Sanitarios'] < 0),
#                     ['Otros', 'Residencias', 'Sanitarios']] = None

data['daily_types'] = data['daily_types'].melt(id_vars=['Fecha'], var_name='Variables')

datasets = {}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

for key in cfg.output.daily:
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
    # if key == "daily_types":
    #     datasets[key]["note"] = [cfg.labels.daily_note]
    utils.publish_firebase('saludcantabria',
                           cfg.output.daily[key],
                           datasets[key])
print('Daily published')

