"""Common utils for extraction modules."""
import json

from cfg import cfg

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

import pandas as pd

import requests


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
    """Read CSV file with Cantabria's data from SCS."""
    # cases = pd.read_csv(cfg.input.path + cfg.input.hospitals)
    cases = pd.read_csv(url, na_filter=False, skipfooter=3,
                        dtype={'CASOS RESIDENCIAS': object,
                               'AISLAMIENTO DOM.': object,
                               'TOTAL TEST': object,
                               'TEST PCR': object})
    """ cases = pd.read_excel(cfg.input.scs_data, na_filter=False,
                        dtype={'CASOS RESIDENCIAS': object,
                                'AISLAMIENTO DOM.': object,
                                'TOTAL TEST': object,
                                'TEST PCR': object,
                                'FECHAS': object}) """
    cases = cases.loc[:, ~cases.columns.str.contains('^Unnamed')]
    cases.columns = cases.columns.str.title()
    cases.columns = cases.columns.str.replace('Fecha\*', 'Fecha')
    cases.columns = cases.columns.str.replace('Uci', 'UCI')
    cases.columns = cases.columns.str.replace('Pcr', 'PCR')
    cases.columns = cases.columns.str.replace('Hosp. ', '')
    cases.columns = cases.columns.str.replace('Prof. ', '')
    cases.columns = cases.columns.str.replace('Humv', 'Valdecilla')
    cases.columns = cases.columns.str.replace('Dom.', 'Domiciliario')
    cases['Fecha'] = cases['Fecha'].str.replace('\*\*', '')
    # cases.drop(cases.tail(3).index, inplace=True)
    return cases
