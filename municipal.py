import requests
import pandas as pd

from cfg import cfg

from pyjstat import pyjstat

import utils

response = requests.get(cfg.input.municipalities)

json_data = response.json()

rows = []
for element in json_data['features']:
    rows.append(element['attributes'])
data = pd.DataFrame(rows, columns=['OBJECTID_1', 'Codigo',
                                    'Texto', 'Cod_Prov',
                                    'Cod_CCAA', 'NumeroCasosActivos',
                                    'NumeroCurados', 'NumeroFallecidos',
                                    'NumeroCasos', 'Shape__Area',
                                    'Shape__Length'])
data.sort_values(by=['Codigo'], inplace=True)
data.reset_index(inplace=True)
if data.at[52, 'Texto'] == '':
    data.at[52, 'Texto'] = 'Polaciones'

measures = {}
datasets = {}
jsonstat = {}
for measure in cfg.municipalities.measures:
    measures[measure] = data[['Codigo',
                              'Texto',
                              cfg.municipalities.measures[measure].original]]
    measures[measure]['municipios'] = measures[measure]['Codigo'] + \
        ' - ' + measures[measure]['Texto']
    measures[measure].drop(columns=['Codigo', 'Texto'], inplace=True)
    measures[measure].rename(columns={cfg.municipalities.measures[measure].original:
                                      cfg.municipalities.measures[measure].final},
                             inplace=True)
    measures[measure] = measures[measure].melt(
        id_vars='municipios',
        value_vars=[cfg.municipalities.measures[measure].final],
        var_name='Variables')
    datasets[measure] = pyjstat.Dataset.read(measures[measure],
                                             source=('Consejería de Sanidad'
                                                     ' del Gobierno de '
                                                     'Cantabria'))
    datasets[measure]["role"] = {"metric": ["Variables"]}
    jsonstat[measure] = datasets[measure].write()

files = {
    cfg.output.municipalities.cases: {
        "content": jsonstat['cases']
    },
    cfg.output.municipalities.actives: {
        "content": jsonstat['actives']
    },
    cfg.output.municipalities.deceased: {
        "content": jsonstat['deceased']
    },
    cfg.output.municipalities.discharged: {
        "content": jsonstat['discharged']
    }
}

utils.publish_gist(files,
                   cfg.labels.municipalities_absolute_gist,
                   cfg.github.municipalities_absolute_gist_id)
print(files)
