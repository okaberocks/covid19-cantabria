"""Common utils for extraction modules."""
import json
from cfg import cfg
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import pandas as pd
import requests
import ssl
import warnings

ssl._create_default_https_context = ssl._create_unverified_context
warnings.filterwarnings("ignore")


def publish_gist(json_files, description, gist_id):
    """Publish multiple files to Github gist."""
    # print headers,parameters,payload
    headers = {'Authorization': f'token {cfg.github.api_token}'}
    params = {'scope': 'gist'}
    payload = {"description": description,
               "public": True,
               "files": json_files
               }

    # make a request
    print(payload)
    res = requests.patch(cfg.github.api_url + gist_id,
                         headers=headers,
                         params=params,
                         data=json.dumps(payload))
    print(res)


def initialize_firebase_db(creds_path, db_url):
    """Initialize Firebase Realtime database."""
    cred = credentials.Certificate(creds_path)
    firebase_admin.initialize_app(cred, {'databaseURL': db_url})


def publish_firebase(category, filename, content):
    """Publish json file into Firebase Realtime db."""
    ref = db.reference()
    data_ref = ref.child(f"{category}/{filename}")
    data_ref.set(content)


def read_scs_csv(url):
    """Read CSV file with Cantabria's historical data from SCS."""
    # cases = pd.read_csv(cfg.input.path + cfg.input.hospitals)
    cases = pd.read_csv(url, na_filter=False, skipfooter=0,
                        sep=';',
                        engine='python',
                        dtype={'CASOS RESIDENCIAS': object,
                               'AISLAMIENTO DOM.': object,
                               'TOTAL TEST': object,
                               'TEST PCR': object})
    cases = cases.loc[:, ~cases.columns.str.contains('^Unnamed')]
    cases.columns = cases.columns.str.title()
    cases.columns = cases.columns.str.replace('Fecha\*', 'Fecha')
    cases.columns = cases.columns.str.replace('Casos Nuevos\*', 'Casos Nuevos')
    cases.columns = cases.columns.str.replace(
        'Casos Nuevos Pcr\*', 'Casos Nuevos')
    cases.columns = cases.columns.str.replace('Uci', 'UCI')
    cases.columns = cases.columns.str.replace('Hospitalizados UCI', 'UCI')
    cases.columns = cases.columns.str.replace('Pcr', 'PCR')
    cases.columns = cases.columns.str.replace('Hosp. ', '')
    cases.columns = cases.columns.str.replace('Prof. ', '')
    cases.columns = cases.columns.str.replace('Humv', 'Valdecilla')
    cases.columns = cases.columns.str.replace('Dom.', 'Domiciliario')
    cases.columns = cases.columns.str.replace('Total Casos', 'Casos')
    cases.columns = cases.columns.str.replace(
        'C. Resid. Activos', 'Residencias Activos')
    cases.columns = cases.columns.str.replace(
        'P. Sanit. Activos', 'Sanitarios Activos')
    cases.columns = cases.columns.str.replace(
        'Incidencia Ac 14', 'Incidencia 14 dias')
    # cases.columns = cases.columns.str.replace('Total Hospitalizados', 'Hospitalizados activos')
    cases['Fecha'] = cases['Fecha'].str.replace('\*\*', '')
    # cases.drop(cases.tail(3).index, inplace=True)
    return cases


def read_scs_municipal(url):
    """Read CSV file with Cantabria's municipal data from SCS."""
    # cases = pd.read_csv(cfg.input.path + cfg.input.hospitals)
    raw_data = pd.read_csv(url, na_filter=False, sep=';', skipfooter=1,
                           engine='python',
                           dtype={'Codigo': object})
    raw_data.columns = raw_data.columns.str.title()
    raw_data.columns = raw_data.columns.str.replace('Código', 'Codigo')
    raw_data.columns = raw_data.columns.str.replace('Municipio', 'Texto')
    raw_data.columns = raw_data.columns.str.replace('Casos',
                                                    'NumeroCasos')
    raw_data.columns = raw_data.columns.str.replace('Activos',
                                                    'NumeroCasosActivos')
    raw_data.columns = raw_data.columns.str.replace('Curados',
                                                    'NumeroCurados')
    raw_data.columns = raw_data.columns.str.replace('Fallecidos',
                                                    'NumeroFallecidos')
    return raw_data


def read_scs_age(url):
    """Read CSV file with Cantabria's municipal data from SCS."""
    raw_data = pd.read_csv(url, na_filter=False,
                           skipfooter=0,
                           sep=';',
                           dtype={'Fecha': object,
                                  'Rango_edad': object,
                                  'Sexo': object,
                                  'Casos_confirmados': int,
                                  'Hospitalizados': int,
                                  'Ingresos_uci': int,
                                  'Fallecidos': int})
    return raw_data


def read_scs_historic_age():
    raw_data = pd.read_csv('./data/input/covid19_edad_sexo.csv',
                           sep=';')
    return raw_data


def read_scs_historic_municipal():
    raw_data = pd.read_csv('./data/input/covid19_municipalizado.csv',
                           na_values=None,
                           sep=';',
                           dtype={'Codigo': object})
    raw_data.columns = raw_data.columns.str.title()
    raw_data.columns = raw_data.columns.str.replace('Código', 'Codigo')
    raw_data.columns = raw_data.columns.str.replace('Municipio', 'Texto')
    raw_data.columns = raw_data.columns.str.replace('Casos',
                                                    'NumeroCasos')
    raw_data.columns = raw_data.columns.str.replace('Activos',
                                                    'NumeroCasosActivos')
    raw_data.columns = raw_data.columns.str.replace('Curados',
                                                    'NumeroCurados')
    raw_data.columns = raw_data.columns.str.replace('Fallecidos',
                                                    'NumeroFallecidos')
    raw_data['Fecha'] = pd.to_datetime(
        raw_data['Fecha'], format='%d/%m/%Y', errors='coerce')
    raw_data['Fecha'] = raw_data['Fecha'].dt.strftime("%d-%m-%Y")
    return raw_data


def read_vaccine_csv():
    """Read CSV file with Cantabria's historical data from SCS."""
    vaccine = pd.read_csv(cfg.input.path + cfg.input.vaccine,
                          sep=';')
    vaccine = vaccine.fillna(0)
    return vaccine


def read_vaccine_general():
    """Read CSV file with Cantabria's historical data from SCS."""
    vaccine = pd.read_excel(cfg.input.path + cfg.input.vaccine,
                            sheet_name='general',
                            na_filter=False,)
    return vaccine


def read_vaccine_week():
    """Read CSV file with Cantabria's historical data from SCS."""
    vaccine = pd.read_excel(cfg.input.path + cfg.input.vaccine,
                            sheet_name='semanas',
                            na_filter=False,)
    return vaccine


def read_vaccine_age():
    """Read CSV file with Cantabria's historical data from SCS."""
    vaccine = pd.read_excel(cfg.input.path + cfg.input.vaccine,
                            sheet_name='edades',
                            na_filter=False,)
    return vaccine


def read_vaccine_types():
    """Read CSV file with Cantabria's historical data from SCS."""
    vaccine = pd.read_excel(cfg.input.path + cfg.input.vaccine,
                            sheet_name='tipos',
                            na_filter=False,)
    return vaccine


def read_rho_csv(url):
    """Read CSV file with Cantabria's historical data from SCS."""
    rho = pd.read_csv(url,
                      na_filter=False,
                      sep=';')
    rho.columns = rho.columns.str.replace('Date', 'Fecha')
    rho.columns = rho.columns.str.replace('Mean\(R\)', 'Media (R)')
    rho.columns = rho.columns.str.replace(
        'Quantile\.0\.025\(R\)', 'Cuantil 0,025 (R)')
    rho.columns = rho.columns.str.replace(
        'Quantile\.0\.975\(R\)', 'Cuantil 0,975 (R)')

    rho = rho.apply(lambda x: x.str.replace(',', '.'))

    return rho


def read_restimation(url):
    """Read CSV file with Cantabria's historical data from SCS."""
    estimation = pd.read_csv(url,
                             na_filter=False,
                             sep=',')
    estimation.columns = estimation.columns.str.replace('FECHA', 'Fecha')
    estimation.columns = estimation.columns.str.replace(
        'CASOS.NUEVOS.PCR.', 'Positivos')
    estimation.columns = estimation.columns.str.replace(
        'POSITIVOS_LS', 'Positivos LS')
    estimation.columns = estimation.columns.str.replace(
        'POSITIVOS_LI', 'Positivos LI')

    # rho.columns = rho.columns.str.replace('Quantile\.0\.975\(R\)', 'Cuantil 0,975 (R)')

    # arima = arima.apply(lambda x: x.str.replace(',','.'))

    return estimation
