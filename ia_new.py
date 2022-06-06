"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


# Read CSV data
cases = pd.read_excel(cfg.input.path + cfg.input.new_ia,
                            sheet_name='Sheet1',
                            na_filter=False)
data = {}

# cases = cases.pivot_table('Riesgo Controlado', ['Nivel actual']).T

key = 'Incidencia acumulada 14d ≥ 60a'
data[key] = cases.melt(id_vars=['Fecha'], var_name='Variables')
datasets = {}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

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
datasets[key]['dimension']['Variables']['category']['unit'] = 'Índice'
utils.publish_firebase('saludcantabria',
                    'ia14-new',
                    datasets[key])
print('IA 14 published')
