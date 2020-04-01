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

for key in cfg.github.mult_rate.keys():
    cases[key] = pd.read_csv(cfg.github.mult_rate[key].url)
    cases[key].drop(cases[key][cases[key].cod_ine != 6].index, inplace=True)
    cases[key].reset_index(inplace=True)
    cases[key].drop('index', axis=1, inplace=True)
    cases[key].drop('cod_ine', axis=1, inplace=True)
    cases[key].drop('CCAA', axis=1, inplace=True)

# downsampling: get 1 record in 4 to avoid temporary effects
    downsampling[key] = cases[key].iloc[list(range(len(cases[key])-1, 0, -4))]
    downsampling[key]['mult_rate'] = downsampling[key]['total'] / \
        downsampling[key]['total'].shift(-1)

# Format the dataframe for generating json-stat for stat-viewer

    mult_rates[key] = downsampling[key].melt(
        id_vars=['fecha'],
        value_vars=['mult_rate'],
        var_name='Variables'
    )
    mult_rates[key]['Variables'].replace(cfg.labels, inplace=True)
    mult_rates[key].sort_values(by=['fecha'], inplace=True)
    mult_rates[key].replace([np.inf, -np.inf], np.nan, inplace=True)
    

# Generate and publish json-stat

    mult_rates_ds[key] = pyjstat.Dataset.read(mult_rates[key],
                                              source=('Ministerio de Sanidad, '
                                                      'Consumo y Bienestar '
                                                      'Social. A partir de '
                                                      'ficheros de datos '
                                                      'elaborados por '
                                                      'DATADISTA.COM  '))
    mult_rates_ds[key]["role"] = {"time": ["fecha"], "metric": ["Variables"]}

    mult_rates_json[key] = mult_rates_ds[key].write()
    files[cfg.output.mult_rate[key]] = {'content': mult_rates_json[key]}

utils.publish_gist(files,
                   cfg.labels.mult_rate_gist,
                   cfg.github.mult_rate_gist_id)
