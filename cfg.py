from beautifuldict.baseconfig import Baseconfig
from decouple import config
from pkg_resources import resource_filename

params = {
    
    'output': {
        'path': resource_filename(__name__, 'data/output/'),
        'results': 'resultados.csv',
        'hospitals': 'hospitals.json',
        'current_situation': 'current-situation.json'
    },
    'github': {
        'api_url': 'https://api.github.com/gists/',
        'api_token': config('API_TOKEN'),
        'gist_id': 'bacd3c3d40bf9d63b66b150335f2073e'
    },
    'twitter': {
        'user_id': 'saludcantabria',
        'covid_regex': 'Situación #Covid19',
        'api_key': config('API_KEY'),
        'api_secret_key': config('API_SECRET_KEY'),
        'access_token': config('ACCESS_TOKEN'),
        'access_token_secret': config('ACCESS_TOKEN_SECRET')

    },
    'hospitals': {
        'valdecilla': {
            'name': 'Valdecilla',
            'lat': '43.455398',
            'lon': '-3.829655'
        },
        'sierrallana': {
            'name': 'Sierrallana',
            'lat': '43.361574',
            'lon': '-4.076808'
        },
        'tresmares': {
            'name': 'Tres Mares',
            'lat': '43.006395',
            'lon': '-4.132479'
        },
        'laredo': {
            'name': 'Laredo',
            'lat': '43.414092',
            'lon': '-3.44198'
        }
    }
}

cfg = Baseconfig(params)
