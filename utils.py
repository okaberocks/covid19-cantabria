"""Common utils for extraction modules."""
import json

from cfg import cfg

import requests


def publish_gist(json_files, description, gist_id):
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