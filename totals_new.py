"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


# Read CSV data
cases = pd.read_excel(cfg.input.path + cfg.input.new_indicadors,
                            sheet_name='Sheet1',
                            na_filter=False,
                            skiprows=[0])
data = {}
cases = cases.pivot_table('Nivel actual', ['Nivel actual']).T
cases['Fecha'] = pd.to_datetime("today").strftime('%d-%m-%Y')
for column in cases:
    if (column != "Fecha"):
        data[column] = cases[["Fecha", column]]
        data[column] = data[column].melt(id_vars=['Fecha'], var_name='Variables')

datasets = {}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

for key in cfg.output['totals-new']:
    datasets[key] = pyjstat.Dataset.read(data[key],
                                        source=('Consejería de Sanidad '
                                                ' del Gobierno de '
                                                'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    utils.publish_firebase('saludcantabria',
                        cfg.output['totals-new'][key],
                        datasets[key])
print('Totals published')
