"""Generate hospitals time series json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


# cases = pd.read_excel(cfg.input.path + cfg.input.hospitals)
cases = pd.read_csv('https://www.scsalud.es/documents/2162705/9255280/2020_covid19_historico.csv')
cases.columns = cases.columns.str.title()
cases.columns = cases.columns.str.replace('Fecha\*', 'Fecha')
cases.columns = cases.columns.str.replace('Uci', 'UCI')
cases.columns = cases.columns.str.replace('HOSP. ', '')
cases.columns = cases.columns.str.replace('Humv', 'Valdecilla')
cases['Fecha'] = cases['Fecha'].str.replace('\*\*', '')
cases.drop(columns='Unnamed: 15', inplace=True)
cases.drop(cases.tail(3).index, inplace=True)

elder = cases[['Fecha', 'Casos Residencias']]
elder = elder.melt(id_vars=['Fecha'], var_name='Variables')

cases.drop(columns='Casos Residencias', inplace=True)
cases[cases.columns[1:]] = cases[cases.columns[1:]].apply(lambda x: pd.to_numeric(x, downcast='integer'))

hospitals = {}
ucis = {}
sanitarians = cases[['Fecha', 'Prof. Sanitarios']]
sanitarians = sanitarians.melt(id_vars=['Fecha'], var_name='Variables')
print(cases.columns)
for key in cfg.hospitals.keys():
    hospitals[key] = cases[['Fecha'] +
                           [col for col in cases.columns
                               if cfg.hospitals[key].name in col
                               and 'UCI' not in col]]
  
    ucis[key] = cases[['Fecha'] +
                      [col for col in cases.columns
                          if cfg.hospitals[key].name in col
                          and 'UCI' in col]]

    # Format the dataframes for generating json-stat for stat-viewer
    hospitals[key] = hospitals[key].melt(
        id_vars=['Fecha'],
        var_name='Variables'
    )

    ucis[key] = ucis[key].melt(
        id_vars=['Fecha'],
        var_name='Variables'
    )


hospitalizations = pd.concat(list(hospitals.values()))
hospitalizations.reset_index(inplace=True)
hospitalizations.drop('index', axis=1, inplace=True)
hospitalizations['Fecha'] = pd.to_datetime(hospitalizations['Fecha']).dt.strftime('%Y-%m-%d')
hospitalizations.sort_values(by=['Fecha', 'Variables'], inplace=True)


ucis = pd.concat(list(ucis.values()))
ucis.reset_index(inplace=True)
ucis.drop('index', axis=1, inplace=True)
ucis['Fecha'] = pd.to_datetime(ucis['Fecha']).dt.strftime('%Y-%m-%d')
ucis.sort_values(by=['Fecha', 'Variables'], inplace=True)
ucis.reset_index(inplace=True)
ucis.drop('index', axis=1, inplace=True)

sanitarians['Fecha'] = pd.to_datetime(sanitarians['Fecha']).dt.strftime('%Y-%m-%d')
sanitarians.sort_values(by=['Fecha', 'Variables'], inplace=True)

elder['Fecha'] = pd.to_datetime(elder['Fecha']).dt.strftime('%Y-%m-%d')
elder.sort_values(by=['Fecha', 'Variables'], inplace=True)

# Generate and publish json-stat
hospitalizations_dataset = pyjstat.Dataset.read(hospitalizations,
                                                source=('Consejería de Sanidad'
                                                        ' del Gobierno de '
                                                        'Cantabria'))
hospitalizations_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}
hospitalizations_json = hospitalizations_dataset.write()

ucis_dataset = pyjstat.Dataset.read(ucis, source=('Consejería de Sanidad'
                                                  ' del Gobierno de '
                                                  'Cantabria'))
ucis_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}
ucis_json = ucis_dataset.write()

elder_dataset = pyjstat.Dataset.read(elder,
                                     source=('Consejería de Sanidad '
                                             ' del Gobierno de '
                                             'Cantabria'))
elder_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}
elder_json = elder_dataset.write()

sanitarians_dataset = pyjstat.Dataset.read(sanitarians,
                                           source=('Consejería de Sanidad '
                                                   ' del Gobierno de '
                                                   'Cantabria'))
sanitarians_dataset["role"] = {"time": ["fecha"], "metric": ["Variables"]}
sanitarians_json = sanitarians_dataset.write()

files = {
    cfg.output.hospitalizations: {
        "content": hospitalizations_json
    },
    cfg.output.ucis: {
        "content": ucis_json
    },
    cfg.output.sanitarians: {
        "content": sanitarians_json
    },
    cfg.output.elder: {
        "content": elder_json
    }
}

utils.publish_gist(files,
                   cfg.labels.hospitalizations_gist,
                   cfg.github.hospitalizations_gist_id)

print(ucis_json)
print(hospitalizations_json)
print(files) 
