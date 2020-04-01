from beautifuldict.baseconfig import Baseconfig
from decouple import config
from pkg_resources import resource_filename

params = {
    
    'output': {
        'path': resource_filename(__name__, 'data/output/'),
        'results': 'resultados.csv',
        'hospitals': 'hospitals.json',
        'mult_rate': {
            'cases': 'cases-mult-rate.json',
            'deceased': 'deceased-mult-rate.json',
            'discharged': 'discharged-mult-rate.json'
        },
        'municipalities': {
            'cases': 'mun-cases.json',
            'actives': 'mun-actives.json',
            'deceased': 'mun-deceased.json',
            'discharged': 'mun-discharged.json'
        },
        'hospitalizations': 'hospitalizations.json',
        'ucis': 'ucis.json',
        'sanitarians': 'sanitarians.json',
        'elder': 'elder.json',
        'current_situation': 'current-situation.json'
    },
    'input': {
        'path': resource_filename(__name__, 'data/input/'),
        'hospitals': 'covid19_historico.xls',
        'municipalities': 'https://services3.arcgis.com/JW6jblFaBSUw9ROV/ArcGIS/rest/services/MunicpiosCantabria_covid/FeatureServer/0/query?f=json&where=1=1&returnGeometry=false&outFields=*'
    },
    'github': {
        'api_url': 'https://api.github.com/gists/',
        'api_token': config('API_TOKEN'),
        'hospitals_gist_id': 'bacd3c3d40bf9d63b66b150335f2073e',
        'mult_rate_gist_id': '4a622f00ea944085dde09d99def9b8cc',
        'hospitalizations_gist_id': 'ff0f50a499ed8a5832a21d505e20a06f',
        'municipalities_absolute_gist_id': '004c5c29602706a952c7af84124b6fec',
        'mult_rate': {
            'cases': {
                'url': 'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_casos_long.csv'
            },
            'deceased': {
                'url': 'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_fallecidos_long.csv'
            },
            'discharged': {
                'url': 'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_covid19_altas_long.csv'
            }
        }
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
    },
    'labels': {
        'mult_rate': 'Tasa de Multiplicación',
        'hospitals_gist': 'COVID19 hospital data in Cantabria',
        'mult_rate_gist': 'COVID19 case multiplication rate in Cantabria',
        'hospitalizations_gist': 'COVID19 hospitalization and UCI data in Cantabria',
        'municipalities_absolute_gist': 'COVID19 absolute values by municipalities',
        'current_sit_note': 'Los datos de situación actual corresponden al día anterior, debido a un desfase en las fuentes oficiales.' 
    },
    'municipalities': {
        'measures': {
            'cases': {
                'original': 'NumeroCasos',
                'final': 'Número de casos'
            },
            'actives': {
                'original': 'NumeroCasosActivos', 
                'final': 'Casos activos'
            },
            'deceased': {
                'original': 'NumeroFallecidos',
                'final': 'Fallecidos'
            },
            'discharged': {
                'original': 'NumeroCurados',
                'final': 'Altas'
            }
        }
    }
}

cfg = Baseconfig(params)
