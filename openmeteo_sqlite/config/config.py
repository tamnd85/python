"""
Módulo: config.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
Autor: Tamara
Descripción:
    Configuración centralizada del proyecto.

    Este módulo define:
        - Rutas base del proyecto y estructura de carpetas.
        - Ubicación de la base de datos SQLite.
        - Carga de variables de entorno desde .env.
        - Fechas por defecto para descargas históricas.
        - Lista de ciudades configuradas para descarga automática.
        - Parámetros por defecto para predicciones.

    Su objetivo es mantener toda la configuración en un único punto,
    evitando duplicación y facilitando la mantenibilidad del sistema.
"""

import os
from dotenv import load_dotenv
from datetime import date  

#---------------------------------------------------------------------
# RUTAS BASE DEL PROYECTO
#---------------------------------------------------------------------

# Carpeta raíz del proyecto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Carpeta donde se almacenará la base de datos
DATA_DIR = os.path.join(BASE_DIR, "datos")
os.makedirs(DATA_DIR, exist_ok=True)

# Ruta completa al archivo SQLite
DB_PATH = os.path.join(DATA_DIR, "openmeteo.db")

# Nombre de la tabla principal
TABLA_DB = "mediciones"

#---------------------------------------------------------------------
# VARIABLES DE ENTORNO
#---------------------------------------------------------------------
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)
API_KEY = os.getenv("API_KEY")

#---------------------------------------------------------------------
# CONFIGURACIÓN DE FECHAS
#---------------------------------------------------------------------

# Fecha inicial para histórico
START_DATE = "2000-01-01"

# Fecha final: HOY (dinámico)
END_DATE = str(date.today())

#---------------------------------------------------------------------
# CIUDADES A DESCARGAR
#---------------------------------------------------------------------
# Lista de ciudades configuradas para descarga automática 
# Cada entrada contiene nombre, latitud y longitud
CIUDADES = [
    #{"nombre": "Sevilla", "lat": 43.4623, "lon": -3.8099},
    {"nombre": "santander", "lat": 37.38, "lon": -5.98},  

]

#---------------------------------------------------------------------
# CONFIGURACIÓN DE PREVISIÓN
#---------------------------------------------------------------------
# Estación por defecto para consultas de predicción 
ESTACION_DEFAULT = "Santander" 
# Número de días de predicción por defecto 
DIAS_DEFAULT = 7