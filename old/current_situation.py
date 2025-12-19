"""Generate current situation json-stat."""

from cfg import cfg

import pandas as pd

from pyjstat import pyjstat

import utils


def extract_hospital_value(variable):
    """Extract hospital name from column, also when it's an UCI."""
    result = cfg.hospitals.get(variable)
    if result:
        return result['name']
    else:
        return cfg.hospitals[variable[3:]]['name']


def rename_variable_column(variable):
    """Rename variable column to an allowed measure name."""
    if variable.startswith('uci'):
        return 'uci'
    else:
        return 'Ingresados'


def put_lat(variable):
    """Put latitude extracted from configuration."""
    if variable.startswith('uci'):
        return cfg.hospitals[variable[3:]]['lat']
    else:
        return cfg.hospitals.get(variable)['lat']


def put_lon(variable):
    """Put longitude extracted from configuration."""
    if variable.startswith('uci'):
        return cfg.hospitals[variable[3:]]['lon']
    else:
        return cfg.hospitals.get(variable)['lon']


def generate_coords_df():
    """Generate coordinates dataframe from config data."""
    rows = []
    for key in cfg.hospitals.keys():
        row = {}
        row['Variables'] = 'Latitud'
        row['value'] = cfg.hospitals[key].lat
        row['Hospital'] = cfg.hospitals[key].name
        rows.append(row)
        row = {}
        row['Variables'] = 'Longitud'
        row['value'] = cfg.hospitals[key].lon
        row['Hospital'] = cfg.hospitals[key].name
        rows.append(row)
    return pd.DataFrame(rows, columns=['Variables', 'value', 'Hospital'])


# READ DATA FROM CSV.
cases = utils.read_scs_csv(cfg.input.scs_data)

data = {}

data['general'] = cases[['Fecha',
                         'Total Hospitalizados', 'Fallecidos']].tail(1)

data['general'] = data['general'].melt(id_vars=['Fecha'], var_name='Variables')

data['general']['Fecha'] = pd.to_datetime(
    data['general']['Fecha'], dayfirst=True).dt.strftime('%Y-%m-%d')
data['general']['value'] = data['general']['value'].astype(int)
data['general'] = data['general'].sort_values('value', ascending=False)
data['general']['Fecha'] = pd.to_datetime(
    data['general']['Fecha'], dayfirst=True).dt.strftime('%d-%m-%Y')
# Calculate current situation in hospitals

cases.columns = cases.columns.str.lower()
cases.columns = cases.columns.str.replace(' ', '')
data['hospitals'] = cases[['valdecilla', 'ucivaldecilla',
                           'sierrallana', 'ucisierrallana',
                           'liencres', 'laredo',
                           'tresmares']].tail(1)

data['hospitals'] = data['hospitals'].melt(var_name='Variables')

data['hospitals']['Hospital'] = data['hospitals']['Variables'].apply(
    extract_hospital_value)

data['hospitals']['Variables'] = data['hospitals']['Variables'].apply(
    rename_variable_column)

coords_df = generate_coords_df()

data['hospitals'] = data['hospitals'].append(coords_df, ignore_index=True)

data['hospitals'] = data['hospitals'][['Hospital', 'Variables', 'value']]

ucis = pd.DataFrame([['Laredo', 'uci', ''],
                     ['Liencres', 'uci', ''],
                     ['Tres Mares', 'uci', '']])
ucis.columns = ['Hospital', 'Variables', 'value']
data['hospitals'] = pd.concat([data['hospitals'], ucis], ignore_index=True)

data['hospitals'].sort_values(by=['Hospital', 'Variables'], inplace=True)
data['hospitals']['Variables'] = data['hospitals']['Variables'].replace('uci',
                                                                        'UCI')

# GENERATE JSON DATASETS
datasets = {}

try:
    utils.initialize_firebase_db(cfg.firebase.creds_path, cfg.firebase.db_url)
except:
    pass

for key in cfg.output.current_situation:
    datasets[key] = pyjstat.Dataset.read(data[key], source=(
        'Consejería de Sanidad del Gobierno de Cantabria'))
    datasets[key]["role"] = {"time": ["fecha"], "metric": ["Variables"]}
    # datasets[key]["note"] = [cfg.labels.current_sit_note]
    datasets[key]['dimension']['Variables']['category']['unit'] = cfg.output.current_situation[key].units
    utils.publish_firebase('saludcantabria',
                           cfg.output.current_situation[key].name,
                           datasets[key])
print('Current situation published')
