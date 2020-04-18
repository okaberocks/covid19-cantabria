"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


# TODO extract CSV reading
cases = pd.read_csv(cfg.input.scs_data,
                    na_filter=False,
                    dtype={'CASOS RESIDENCIAS': object,
                           'AISLAMIENTO DOM.': object,
                           'TEST PCR': object,
                           'PERSONAS TEST': object})
""" cases = pd.read_excel(cfg.input.scs_data, na_filter=False,
                      dtype={'CASOS RESIDENCIAS': object,
                             'AISLAMIENTO DOM.': object,
                             'TEST PCR': object,
                             'PERSONAS TEST': object,
                             'FECHAS': object}) """
cases = cases.loc[:, ~cases.columns.str.contains('^Unnamed')]
cases.columns = cases.columns.str.title()
cases.columns = cases.columns.str.replace('Fecha\*', 'Fecha')
cases.columns = cases.columns.str.replace('Uci', 'UCI')
cases.columns = cases.columns.str.replace('Pcr', 'PCR')
cases.columns = cases.columns.str.replace('HOSP. ', '')
cases.columns = cases.columns.str.replace('Humv', 'Valdecilla')
cases['Fecha'] = cases['Fecha'].str.replace('\*\*', '')
cases.drop(cases.tail(3).index, inplace=True)
cases = cases.tail(1)

data = {}

data['actives'] = cases[['Fecha']]
data['actives']['Casos activos'] = cases['Total Casos'].astype(
    int) - cases['Fallecidos'].astype(int) - cases['Curados'].astype(int)
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
