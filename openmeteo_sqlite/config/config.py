"""
Módulo: config.py
Autor: Tamara
Descripción:
    Archivo destinado a centralizar configuraciones relacionadas con el sistema.
    Actualmente actúa como módulo base para futuras constantes, parámetros o 
    ajustes globales que puedan necesitarse en distintos componentes del proyecto.
"""
import os
from dotenv import load_dotenv

# ============================
# RUTAS BASE DEL PROYECTO
# ============================

# Carpeta raíz del proyecto
# Se obetiene tomando la ruta del archivo actua y subiendo un nivel.
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Carpeta donde se almacenará a base de datos.
# Se crea automáticamente si no existe.
DATA_DIR = os.path.join(BASE_DIR, "datos")
os.makedirs(DATA_DIR, exist_ok=True)

# Ruta completa al archivo SQLite donde se guardan las mediciones
DB_PATH = os.path.join(DATA_DIR, "openmeteo.db")

# Nombre de la tabla principal dentro de la base de datos
TABLA_DB = "mediciones"

# ============================
# VARIABLES DE ENTORNO
# ============================

# Ruta al archivo .env ubicado en la raíz del proyecto.
ENV_PATH = os.path.join(BASE_DIR, ".env")

# Carga las variables de entorno desde el archivo .env
load_dotenv(ENV_PATH)

# API key utilizada para sevicios esternos
API_KEY = os.getenv("API_KEY")

# ============================
# CONFIGURACIÓN DE FECHAS
# ============================

# Fecha inicial para descargas históricas o consultas.
START_DATE = "2000-01-01"

# Fecha final para descargas o previsiones.
END_DATE = "2026-01-20"

# ============================
# CIUDADES A DESCARGAR
# ============================

# Lista de ciudades configuradas para descraga de datos.
# Cada entrada contiene nombre, latitud y longitud.
CIUDADES = [
    #{"nombre": "Burgos", "lat": 42.3439, "lon": -3.6969},
    {"nombre": "Santander", "lat": 43.4623, "lon": -3.8099},
]

# ============================
# CONFIGURACIÓN DE PREVISIÓN
# ============================

# Ciudad por defecto para generar previsones meteorológicas.
ESTACION_DEFAULT = "Santander"

# Número de días de previsión por defecto.
DIAS_DEFAULT = 7
