"""Common utils for extraction modules."""
import json

from cfg import cfg

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

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
