"""Generate current situation json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

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
    df = df.groupby(['Rango_edad','Sexo'], as_index=False)['value'].sum()
    return df

# READ DATA FROM CSV.
# TODO: REFACTOR TO UTILS, SINCE THIS IS USED BY MORE MODULES
data = {}
cases_age_sex = pd.read_csv(cfg.input.scs_data_age_sex, na_filter=False,
                    skipfooter=1,
                    dtype={'Fecha': object,
                           'Rango_edad': object,
                           'Sexo': object,
                           'Casos_confirmados': int,
                           'Hospitalizados': int,
                           'Ingresos_uci': int,
                           'Fallecidos': int})

cases_age_sex['Rango_edad'] = cases_age_sex['Rango_edad'].replace('100 y +', '90 y +')
cases_age_sex['Rango_edad'] = cases_age_sex['Rango_edad'].replace('90-99', '90 y +')

data['cases_age'] = cases_age_sex[['Rango_edad', 'Sexo', 'Casos_confirmados']].copy()
data['cases_age'] = to_json(data['cases_age'], ['Rango_edad', 'Sexo'], ['Casos_confirmados'])

data['hospitalizations_age'] = cases_age_sex[['Rango_edad', 'Sexo', 'Hospitalizados']].copy()
data['hospitalizations_age'] = to_json(data['hospitalizations_age'], ['Rango_edad', 'Sexo'], ['Hospitalizados'])

data['uci_age'] = cases_age_sex[['Rango_edad', 'Sexo', 'Ingresos_uci']].copy()
data['uci_age'] = to_json(data['uci_age'], ['Rango_edad', 'Sexo'], ['Ingresos_uci'])

data['deceased_age'] = cases_age_sex[['Rango_edad', 'Sexo', 'Fallecidos']].copy()
data['deceased_age'] = to_json(data['deceased_age'], ['Rango_edad', 'Sexo'], ['Fallecidos'])

# GENERATE JSON DATASETS
datasets = {}
utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
for key in cfg.output.age_sex:
    datasets[key] = pyjstat.Dataset.read(data[key], source=(
        'Consejería de Sanidad del Gobierno de Cantabria'))
    datasets[key]["role"] = {"time": ["fecha"], "metric": ["Variables"]}
    utils.publish_firebase('saludcantabria',
                           cfg.output.age_sex[key],
                           datasets[key])
