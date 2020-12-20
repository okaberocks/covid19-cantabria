"""Generate multiplication rate json-stat."""

from cfg import cfg

import numpy as np

import pandas as pd

from pyjstat import pyjstat

import utils


# read and get input data ready
cases = {}
downsampling = {}
mult_rates = {}
mult_rates_ds = {}
mult_rates_json = {}
files = {}

try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

cases = utils.read_scs_csv(cfg.input.scs_data)

data = {}
data['cases'] = cases[['Fecha', 'Casos']]
data['cases'] = data['cases'].rename(columns={"Casos": "total"})
data['cases'] = data['cases'].melt(id_vars=['Fecha'], var_name='Variables')
data['deceased'] = cases[['Fecha', 'Fallecidos']]
data['deceased'] = data['deceased'].rename(columns={"Fallecidos": "total"})
data['deceased'] = data['deceased'].melt(id_vars=['Fecha'], var_name='Variables')
data['discharged'] = cases[['Fecha', 'Recuperados']]
data['discharged'] = data['discharged'].rename(columns={"Recuperados": "total"})
data['discharged'] = data['discharged'].melt(id_vars=['Fecha'], var_name='Variables')


datasets = {}
for key in cfg.output.mult_rate:
    data[key]['Fecha'] = pd.to_datetime(
        data[key]['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
    data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    
    # downsampling: get 1 record in 4 to avoid temporary effects
    data[key] = data[key].iloc[list(range(len(data[key])-1, 0, -14))]
    data[key]['mult_rate'] = data[key]['value'] / \
        data[key]['value'].shift(-1)

    data[key] = data[key].melt(
        id_vars=['Fecha'],
        value_vars=['mult_rate'],
        var_name='Variables'
    )

    data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    data[key]['Variables'].replace(cfg.labels, inplace=True)
    data[key].replace([np.inf, -np.inf], np.nan, inplace=True)
    data[key] = data[key].dropna()
    data[key] = data[key].iloc[2:]

    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    utils.publish_firebase('saludcantabria', cfg.output.mult_rate[key], datasets[key])
print('Mult rate published')