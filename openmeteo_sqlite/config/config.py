import os
from dotenv import load_dotenv
from datetime import date  

# ============================
# RUTAS BASE DEL PROYECTO
# ============================

# Carpeta raíz del proyecto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Carpeta donde se almacenará la base de datos
DATA_DIR = os.path.join(BASE_DIR, "datos")
os.makedirs(DATA_DIR, exist_ok=True)

# Ruta completa al archivo SQLite
DB_PATH = os.path.join(DATA_DIR, "openmeteo.db")

# Nombre de la tabla principal
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

# Fecha inicial para histórico
START_DATE = "2000-01-01"

# Fecha final: HOY (dinámico)
# Gracias a 'from datetime import date', esto ya NO dará error
END_DATE = str(date.today())

# ============================
# CIUDADES A DESCARGAR
# ============================
CIUDADES = [
    #{"nombre": "Sevilla", "lat": 43.4623, "lon": -3.8099},
    {"nombre": "santander", "lat": 37.38, "lon": -5.98},  

]

# ============================
# CONFIGURACIÓN DE PREVISIÓN
# ============================
ESTACION_DEFAULT = "Santander"
DIAS_DEFAULT = 7