"""Generate current situation json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat
from datetime import datetime

import utils

# READ DATA FROM CSV.
# TODO: REFACTOR TO UTILS, SINCE THIS IS USED BY MORE MODULES
data = {}

df = pd.DataFrame()
historic_age = utils.read_scs_historic_age()
for sheet in historic_age.keys():
    historic_age[sheet].columns = ['Rango_edad','Casos','total-hosp', 'total-uci', 'tota-deceased',
                         'women-cases','women-hosp', 'women-uci', 'women-deceased',
                         'men-cases','men-hosp', 'men-uci', 'men-deceased']
    data[sheet] = historic_age[sheet][['Rango_edad', 'Casos']].tail(11)

    date_time_obj = datetime.strptime(sheet, '%Y%m%d')
    data[sheet]['Fecha'] = date_time_obj.strftime('%d/%m/%Y')
    # data[sheet] = data[sheet].replace('90 <= x < 100', '90 y +')
    # data[sheet] = data[sheet].replace('100 <= x < 110', '90 y +')
    # data[sheet]["Casos"] = pd.to_numeric(data[sheet]["Casos"])
    # data[sheet].groupby(['Fecha', 'Rango_edad']).sum()
    # data[sheet].reset_index(level=0, inplace=True)
    df = df.append(data[sheet],ignore_index=True)


df = df.melt(id_vars=['Fecha', 'Rango_edad'], var_name='Variables')
df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
df = df.sort_values(by =['Rango_edad', 'Fecha'])
df["value"] = pd.to_numeric(df["value"])

df = df.pivot(index='Fecha',columns='Rango_edad', values='value')
df = df.diff(axis=0)

df = df.rolling(7).sum()
df = df.iloc[7:-1:7]

df.reset_index(level=0, inplace=True)

df = pd.melt(df, id_vars=["Fecha"], value_vars=['0 <= x < 10', '10 <= x < 20', '100 <= x < 110', '20 <= x < 30',
       '30 <= x < 40', '40 <= x < 50', '50 <= x < 60', '60 <= x < 70',
       '70 <= x < 80', '80 <= x < 90', '90 <= x < 100'], var_name='Variables')
       
data['age_sex_historic'] = df
# GENERATE JSON DATASETS
datasets = {}
try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

for key in cfg.output.age_sex_historic:
    data[key]['Fecha'] = pd.to_datetime(
        data[key]['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
    data[key].sort_values(by=['Fecha', 'Variables'], inplace=True)
    datasets[key] = pyjstat.Dataset.read(data[key],
                                         source=('Consejería de Sanidad '
                                                 ' del Gobierno de '
                                                 'Cantabria'))
    datasets[key]["role"] = {"time": ["Fecha"], "metric": ["Variables"]}
    utils.publish_firebase('saludcantabria',
                           cfg.output.age_sex_historic[key],
                           datasets[key])
                           
    # datasets[key] = pyjstat.Dataset.read(data[key], source=(
    #     'Consejería de Sanidad del Gobierno de Cantabria'))
    # datasets[key]["role"] = {"time": ["fecha"], "metric": ["Variables"]}
    # print('saludcantabria')
    # print(cfg.output.age_sex_historic[key])
    # utils.publish_firebase('saludcantabria',
    #                        cfg.output.age_sex_historic[key],
    #                        datasets[key])
print('Age and sex historic published')