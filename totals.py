"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


# Read CSV data
cases = utils.read_scs_csv(cfg.input.scs_data)
cases = cases.tail(1)
data = {}

data['actives'] = cases[['Fecha', 'Activos']]
data['actives'] = data['actives'].melt(id_vars=['Fecha'], var_name='Variables')

data['uci'] = cases[['Fecha', 'UCI']]
data['uci'] = data['uci'].melt(id_vars=['Fecha'], var_name='Variables')

data['test'] = cases[['Fecha', 'Test PCR']]
data['test'] = data['test'].melt(id_vars=['Fecha'], var_name='Variables')

data['cases'] = cases[['Fecha', 'Casos']]
data['cases'] = data['cases'].melt(id_vars=['Fecha'], var_name='Variables')

data['deceased'] = cases[['Fecha', 'Fallecidos']]
data['deceased'] = data['deceased'].melt(id_vars=['Fecha'], var_name='Variables')

data['discharged'] = cases[['Fecha', 'Recuperados']]
data['discharged'] = data['discharged'].melt(id_vars=['Fecha'], var_name='Variables')

data['sanitarians'] = cases[['Fecha', 'Sanitarios Activos']]
data['sanitarians'] = data['sanitarians'].melt(id_vars=['Fecha'], var_name='Variables')
data['sanitarians']['value'] = data['sanitarians']['value'].astype(int)

data['residences'] = cases[['Fecha', 'Residencias Activos']]
data['residences'] = data['residences'].melt(id_vars=['Fecha'], var_name='Variables')
data['residences']['value'] = data['residences']['value'].astype(int)

# Generate and publish json-stat
datasets = {}
utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)

for key in cfg.output.totals:
    data[key]['Fecha'] = pd.to_datetime(
        data[key]['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')

    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    # print(datasets[key])
    utils.publish_firebase('saludcantabria',
                           cfg.output.totals[key],
                           datasets[key])
