"""Módulo etl.

Este módulo contiene la superclase Etl, que implementa operaciones
ETL comunes, como la lectura de ficheros fuente, procesamiento de dataframes
y carga en bases de datos.
"""

import logging
from abc import ABC

import utils

from icanemetadata.metadata import Metadata

import numpy as np

import pandas as pd

import pyjstat


class Etl(ABC):
    """Superclase de la que heredan los siguientes módulos.

    -ind_agrup_actividad
    -ind_dest_econ_bienes.

    Implementa un flujo ETL general en tres pasos:

        - Extracción, implementado en el constructor.
        - Remapeo de columnas en el método remap().
        - Asignación de valores nulos en el método _fillna().
        - Transformación, implementado en el método _transform().
        - Carga en BD, implementado en el método _load().
        - Actualización de metadatos en el método _metadata().

    El método público start() ejecuta el proceso completo.

    .. automethod:: __init__
    """

    def __init__(self, config, config_meta=None):
        """Devuelve un objeto Etl listo para procesar datos.

        El objeto es inicializado con objetos Baseconfig, que contienen
        un diccionario de variables de configuración, como cedenciales de
        base de datos, rutas de ficheros, codificaciones, etc.

        Argumentos:
            config (Baseconfig): almacena los parámetros de configuración
                                 en una estructura jerárquica. Proporciona
                                 acceso a los parámetros como claves de
                                 diccionario o como atributos del objeto.
            config_meta: almacena los parámetros de configuración para
                         realizar la actualizacion de los metadatos.

        Ejemplo:
        from ipietl.etl_micro import EtlMicro

        class xxx(EtlMicro):
            super().__init__(config)

            # sobrecargar con código específico de la subclase

        """
        self.cfg = config
        self.logger = logging.getLogger(__name__)
        raw_data = utils.read_scs_csv(self.cfg.input.scs_data)
        data = {}  # parsed data
        datasets = {}  # generted datasets in json-stat

    def _transform(self):
        pass

    def _load(self):
        """Ejecución de operaciones de inserción/actualización en la BD."""
        utils.initialize_firebase_db(self.cfg.firebase.creds_path,
                                     self.cfg.firebase.db_url)

        for key in cfg.output.current_situation:
            self.datasets[key] = pyjstat.Dataset.read(data[key], source=(
                'Consejería de Sanidad del Gobierno de Cantabria'))
            self.datasets[key]["role"] = {"time": ["fecha"],
                                          "metric": ["Variables"]}
            self[datasets[key]["note"] = [cfg.labels.current_sit_note]
            print(self.datasets[key])
            utils.publish_firebase('saludcantabria',
                                   cfg.output.current_situation[key],
                                   self.datasets[key])

    def start(self):
        """Proceso ETL completo."""
        self._fillna()

        self.remap()

        for item in self.cfg.col_cfg:
            self._transform(item, self.cfg.col_cfg[item])

        self.indDF = self.ipiDF

        self.indDF = self.indDF.reset_index(drop=True)

        self._load()

        if self.cfg_meta:
            self._metadata()
