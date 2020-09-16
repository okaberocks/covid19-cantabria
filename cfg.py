from beautifuldict.baseconfig import Baseconfig
from decouple import config
from pkg_resources import resource_filename
import urls

params = {
    
    'output': {
        'path': resource_filename(__name__, 'data/output/'),
        'results': 'resultados.csv',
        'hospitals': 'hospitals.json',
        'mult_rate': {
            'cases': 'cases-mult-rate',
            'deceased': 'deceased-mult-rate',
            'discharged': 'discharged-mult-rate'
        },
        'municipalities': {
            'cases': 'mun-cases',
            'actives': 'mun-actives',
            'deceased': 'mun-deceased',
            'discharged': 'mun-discharged',
            'cases_gross_rate': 'mun-cases-gross-rate',
            'actives_gross_rate': 'mun-actives-gross-rate',
            'deceased_gross_rate': 'mun-deceased-gross-rate',
            'discharged_gross_rate': 'mun-discharged-gross-rate'
        },
        'municipalities_historic': {
            'actives_gross_rate': 'mun-actives-gross-rate-historic'
        },
        'historical': {
            'hospitalizations': 'hospitalizations',
            'ucis': 'ucis',
            'sanitarians': 'sanitarians',
            'elder': 'elder',
            'test': 'test',
            'active_sanitarians': 'active-sanitarians',
            'active_elder': 'active-elder'
        },
        'sma': 'sma.json',
        'current_situation': {
            'general': 'current-situation',
            'hospitals': 'current-situation-hospitals'
        },
        'age_sex': {
            'cases_age': 'cases-age',
            'hospitalizations_age': 'hospitalizations-age',
            'uci_age': 'uci-age',
            'deceased_age': 'deceased-age'
        },
        'age_sex_historic': {
            'age_sex_historic': 'age-sex-historic'
        },
        'totals': {
            'actives': 'total-actives',
            'uci': 'total-uci',
            'test': 'total-test',
            'cases': 'total-cases',
            'deceased': 'total-deceased',
            'discharged': 'total-discharged',
            'sanitarians': 'total-sanitarians',
            'residences': 'total-residences'
        },
        'accumulated': {
            'accumulated': 'accumulated'
        },
        'daily': {
            'daily_cases': 'daily-cases',
            'daily_deceases': 'daily-deceases',
            'daily_discharged': 'daily-discharged',
            'daily_types': 'daily-types'
        }
    },
    'input': {
        'path': resource_filename(__name__, 'data/input/'),
        'scs_data': urls.historico_url,
        'scs_data_age_sex': urls.edad_url,
        'scs_data_municipal': urls.municipal_url,
        'hospitals': 'covid19_historico.csv',
        'population': 'poblacion_municipios.csv',
        'municipalities': 'https://services3.arcgis.com/JW6jblFaBSUw9ROV/ArcGIS/rest/services/MunicpiosCantabria_covid/FeatureServer/0/query?f=json&where=1=1&returnGeometry=false&outFields=*'
    },
    'github': {
        'api_url': 'https://api.github.com/gists/',
        # 'api_token': config('API_TOKEN'),
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
        # 'api_key': config('API_KEY'),
        # 'api_secret_key': config('API_SECRET_KEY'),
        # 'access_token': config('ACCESS_TOKEN'),
        # 'access_token_secret': config('ACCESS_TOKEN_SECRET')

    },
    'firebase': {
        'creds_path': config('FIREBASE_CREDS_PATH'),
        'db_url': 'https://covid19can-data.firebaseio.com/'
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
        },
        'liencres': {
            'name': 'Liencres',
            'lat': '43.4638654',
            'lon': '-3.9135864'
        }
    },
    'labels': {
        'mult_rate': 'Tasa de Multiplicación',
        'hospitals_gist': 'COVID19 hospital data in Cantabria',
        'mult_rate_gist': 'COVID19 case multiplication rate in Cantabria',
        'hospitalizations_gist': 'COVID19 hospitalization and UCI data in Cantabria',
        'municipalities_absolute_gist': 'COVID19 absolute values by municipalities',
        'current_sit_note': 'Los datos de situación actual corresponden al día anterior, debido a un desfase en las fuentes oficiales.' ,
        'daily_note': 'Los datos están pendiente de revisión por parte del Servicio Cántabro de Salud'
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
            },
            'cases_gross_rate': {
                'original': 'Tasa bruta de casos',
                'final': 'Tasa bruta de casos'
            },
            'actives_gross_rate': {
                'original': 'Tasa bruta de activos',
                'final': 'Tasa bruta de activos'
            },
            'deceased_gross_rate': {
                'original': 'Tasa bruta de fallecidos',
                'final': 'Tasa bruta de fallecidos'
            },
            'discharged_gross_rate': {
                'original': 'Tasa bruta de altas',
                'final': 'Tasa bruta de altas'
            }
        }
    },
    'municipalities_historic': {
        'measures': {
            'actives_gross_rate': {
                'original': 'Tasa bruta de activos',
                'final': 'Tasa bruta de activos'
            }
        }
    }
}

cfg = Baseconfig(params)
