"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


# Read CSV data
cases = utils.read_scs_csv(cfg.input.scs_data)
cases = cases.tail(1)

data = {}

data['actives'] = cases[['Fecha']]
data['actives']['Casos activos'] = cases['Total Casos PCR+'].astype(
    int) - cases['Fallecidos'].astype(int) - cases['Recuperados'].astype(int)
data['actives'] = data['actives'].melt(id_vars=['Fecha'], var_name='Variables')

data['uci'] = cases[['Fecha', 'Hospitalizados UCI']]
data['uci'] = data['uci'].melt(id_vars=['Fecha'], var_name='Variables')

data['test'] = cases[['Fecha', 'Test PCR']]
data['test'] = data['test'].melt(id_vars=['Fecha'], var_name='Variables')

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
    print(datasets[key].write())
    utils.publish_firebase('saludcantabria',
                           cfg.output.totals[key],
                           datasets[key])
