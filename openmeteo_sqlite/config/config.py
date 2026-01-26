import os
from dotenv import load_dotenv

# ============================
# RUTAS BASE DEL PROYECTO
# ============================

# Carpeta raíz del proyecto final80
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Carpeta donde guardamos la base de datos
DATA_DIR = os.path.join(BASE_DIR, "datos")
os.makedirs(DATA_DIR, exist_ok=True)

# Ruta completa a la base de datos SQLite
DB_PATH = os.path.join(DATA_DIR, "openmeteo.db")

# Nombre de la tabla
TABLA_DB = "mediciones"

# ============================
# VARIABLES DE ENTORNO
# ============================

ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

API_KEY = os.getenv("API_KEY")

# ============================
# CONFIGURACIÓN DE FECHAS
# ============================

START_DATE = "2000-01-01"
END_DATE = "2026-01-20"

# ============================
# CIUDADES A DESCARGAR
# ============================

CIUDADES = [
    {"nombre": "Burgos", "lat": 42.3439, "lon": -3.6969},
    #{"nombre": "Santander", "lat": 43.4623, "lon": -3.8099},
]

# ============================
# CONFIGURACIÓN DE PREVISIÓN
# ============================

ESTACION_DEFAULT = "Burgos"
DIAS_DEFAULT = 7
