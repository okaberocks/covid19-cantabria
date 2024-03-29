from beautifuldict.baseconfig import Baseconfig
from decouple import config
from pkg_resources import resource_filename

params = {
    'cantabria_population': 582905,
    'output': {
        'path': resource_filename(__name__, 'data/output/'),
        'results': 'resultados.csv',
        'hospitals': 'hospitals.json',
        'mult_rate': {
            'cases': 'cases-mult-rate',
            'deceased': 'deceased-mult-rate',
            # 'discharged': 'discharged-mult-rate'
        },
        'municipalities': {
            'cases': 'mun-cases',
            # 'actives': 'mun-actives',
            'deceased': 'mun-deceased',
            # 'discharged': 'mun-discharged',
            'cases_gross_rate': 'mun-cases-gross-rate',
            # 'actives_gross_rate': 'mun-actives-gross-rate',
            'deceased_gross_rate': 'mun-deceased-gross-rate',
            # 'discharged_gross_rate': 'mun-discharged-gross-rate'
        },
        'municipalities_historic': {
            'actives_gross_rate': 'mun-actives-gross-rate-historic'
        },
        'vaccine_new': {
            'vaccine_age': {
                'name': 'vaccine-age',
                'units': {
                    'Personas Inmunizadas': {
                        'decimals': 0, 'label': '%'}
                }
            },
            'vaccine_types': {
                'name': 'vaccine-types',
                'units': {
                    'Personas Inmunizadas': {
                        'decimals': 0, 'label': 'Dosis'}
                }
            },
            'vaccine_week': {
                'name': 'vaccine-week',
                'units': {
                    'Inmunizados': {
                        'decimals': 0, 'label': 'Dosis'}
                }
            },
            'daily_types_first': {
                'name': 'daily_types_first',
                'units': {
                    'Residencias': {
                        'decimals': 0, 'label': 'Primera dosis'}
                }
            },
            'daily_types_second': {
                'name': 'daily_types_second',
                'units': {
                    'Residencias': {
                        'decimals': 0, 'label': 'Segunda dosis'}
                }
            }

        },
        'vaccine': {
            'dosis': {
                'name': 'dosis',
                'units': {
                    'Dosis administradas': {
                        'decimals': 0, 'label': 'Dosis'}
                }
            },
            'tipo_dosis': {
                'name': 'tipo_dosis',
                'units': {
                    'Residencias': {
                        'decimals': 0, 'label': 'Personas vacunadas'}
                }
            },
            'tipo_dosis_completa': {
                'name': 'tipo_dosis_completa',
                'units': {
                    'Residencias': {
                        'decimals': 0, 'label': 'Personas vacunadas'}
                }
            },
            'daily_types_vaccine': {
                'name': 'daily_types_vaccine',
                'units': {
                    'Residencias': {
                        'decimals': 0, 'label': 'Personas vacunadas'}
                }
            }
        },
        'historical': {
            'hospitalizations': {
                'name': 'hospitalizations',
                'units': {
                    'Laredo': {
                        'decimals': 0, 'label': 'Hospitalizados'}
                }
            },
            'ucis': {
                'name': 'ucis',
                'units': {
                    'UCI Valdecilla': {
                        'decimals': 0, 'label': 'Hospitalizados'}
                }
            },
            'sanitarians': {
                'name': 'sanitarians',
                'units': {
                    'Sanitarios Acumulados': {
                        'decimals': 0, 'label': 'Personas'}
                }
            },
            'elder': {
                'name': 'elder',
                'units': {
                    'Residencias Acumulados': {
                        'decimals': 0, 'label': 'Personas'}
                }
            },
            'test': {
                'name': 'test',
                'units': {
                    'Test Anticuerpos diarios': {
                        'decimals': 0, 'label': 'Tests'}
                }
            },
            'active_sanitarians': {
                'name': 'active-sanitarians',
                'units': {
                    'Sanitarios Activos': {
                        'decimals': 0, 'label': 'Personas'}
                }
            },
            'active_elder': {
                'name': 'active-elder',
                'units': {
                    'Residencias Activos': {
                        'decimals': 0, 'label': 'Personas'}
                }
            },
            'incidence': {
                'name': 'incidence',
                'units': {
                    'Incidencia 14 dias': {
                        'decimals': 0, 'label': 'Incidencia 14 dias'}
                }
            },
            'daily_test': {
                'name': 'daily-test',
                'units': {
                    'Test PCR diarios': {
                        'decimals': 0, 'label': 'Tests'}
                }
            },
            'positivity': {
                'name': 'positivity',
                'units': {
                    'Positividad': {
                        'decimals': 0, 'label': 'Índice'}
                }
            }
        },
        'sma': 'sma.json',
        'current_situation': {
            'general': {
                'name': 'current-situation',
                'units': {
                    'Aislamiento Domiciliario': {
                        'decimals': 0, 'label': 'Personas'}
                }
            },
            'hospitals': {
                'name': 'current-situation-hospitals',
                'units': {
                }
            }
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
            # 'actives': 'total-actives',
            'uci': 'total-uci',
            'test': 'total-test',
            'cases': 'total-cases',
            'deceased': 'total-deceased',
            # 'discharged': 'total-discharged',
            'sanitarians': 'total-sanitarians',
            'residences': 'total-residences'
        },
        'totals-new': {
            'IA 14d ≥60a': 'ia14-60',
            'IA 7d ≥60a': 'ia7-60',
            'Casos AYER ≥60a': 'cases-last-day-60',
            'Casos 7d ≥60a': 'cases-7-60',
            'Casos 14d ≥60a': 'cases-14-60',
            'Fallecidos AYER': 'total-deceased-60-lastday',
            'Fallecidos 7d': 'total-deceased-7-60',
            'Fallecidos': 'total-deceased-60'
        },
        'accumulated': {
            'accumulated': 'accumulated'
        },
        'daily': {
            'daily_cases': {
                'name': 'daily-cases',
                'units': {
                    'Test Anticuerpos diarios': {
                        'decimals': 0, 'label': 'Personas'}
                }
            },
            'daily_deceases': {
                'name': 'daily-deceases',
                'units': {
                    'Fallecidos': {
                        'decimals': 0, 'label': 'Personas'}
                }
            },
            # 'daily_discharged': 'daily-discharged',
            'daily_types': {
                'name': 'daily-types',
                'units': {
                    'Sanitarios': {
                        'decimals': 0, 'label': 'Personas'}
                }
            },
        }
    },
    'input': {
        'path': resource_filename(__name__, 'data/input/'),
        'scs_data': 'https://serviweb.scsalud.es:10443/ficheros/COVID19_historico.csv',
        'scs_data_age_sex': 'https://serviweb.scsalud.es:10443/ficheros/COVID19_edadysexo.csv',
        'scs_data_municipal': 'https://serviweb.scsalud.es:10443/ficheros/COVID19_municipalizado.csv',
        'new_indicadors': 'indicadores.xlsx',
        'new_ia': 'incidencia14.xlsx',
        'vaccine_data': 'https://raw.githubusercontent.com/datadista/datasets/master/COVID%2019/ccaa_vacunas.csv',
        'rho': 'https://raw.githubusercontent.com/icane/covid19-rho/main/rho.csv',
        'arima': 'https://raw.githubusercontent.com/icane/covid19-rho/main/resultado_arima.csv',
        'loess': 'https://raw.githubusercontent.com/icane/covid19-rho/main/resultado_loess.csv',
        'redneu': 'https://raw.githubusercontent.com/icane/covid19-rho/main/resultado_redneu.csv',
        'vaccine': 'covid19_vacunas.xlsx',
        'hospitals': 'covid19_historico.csv',
        'population': 'poblacion_municipios.csv',
        'population_age': 'poblacion_edad.csv',
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
        'current_sit_note': 'Los datos de situación actual corresponden al día anterior, debido a un desfase en las fuentes oficiales.',
        'daily_note': 'Los datos están pendiente de revisión por parte del Servicio Cántabro de Salud'
    },
    'municipalities': {
        'measures': {
            'cases': {
                'original': 'NumeroCasos',
                'final': 'Número de casos'
            },
            # 'actives': {
            #     'original': 'NumeroCasosActivos',
            #     'final': 'Casos activos'
            # },
            'deceased': {
                'original': 'NumeroFallecidos',
                'final': 'Fallecidos'
            },
            # 'discharged': {
            #     'original': 'NumeroCurados',
            #     'final': 'Altas'
            # },
            'cases_gross_rate': {
                'original': 'Tasa bruta de casos',
                'final': 'Tasa bruta de casos'
            },
            # 'actives_gross_rate': {
            #     'original': 'Tasa bruta de activos',
            #     'final': 'Tasa bruta de activos'
            # },
            'deceased_gross_rate': {
                'original': 'Tasa bruta de fallecidos',
                'final': 'Tasa bruta de fallecidos'
            },
            # 'discharged_gross_rate': {
            #     'original': 'Tasa bruta de altas',
            #     'final': 'Tasa bruta de altas'
            # }
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
