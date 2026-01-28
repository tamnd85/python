"""
Módulo: ingest_exog.py
Autor: tamara
Descripción:
    Ingesta completa de variables exógenas desde la API histórica de Open-Meteo.
    Este módulo descraga todas las variables diarias disponibles, valida la
    respuesta, filtra únicamente las columnas compatibles con la base de datos
    local y realiza la inserción en SQLite.
    
    Forma paret del sistema de ingesta masiva para enriquecer la base de datos
    con información meteorológica detallada. 
"""
import requests
import sqlite3
import pandas as pd
from datetime import date
from tqdm import tqdm

# Ruta a la base de datos SQLite
DB_PATH = "datos/openmeteo.db"

# Coordenadas de tus ciudades
COORDS = {
    #"Burgos": (42.3439, -3.6969),
    "Santander": (43.4623, -3.8099),
}

# Variables diarias completas solicitadas a Open-Meteo
DAILY_VARS = [
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_mean",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "dew_point_2m_mean",
    "shortwave_radiation_sum",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_probability_max",
    "sunshine_duration",
    "daylight_duration",
    "wind_speed_10m",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "relative_humidity_2m",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "uv_index_max",
    "sunrise",
    "sunset"
]

def ingest_exog():
    """
    Descarga e inserta en la base de datos todas las variables diarias exógenas
    disponibles para cada ciudad configurada
    
    Flujo:
        1. Conectar a SQLite y obtener columnas existentes eb la tabla.
        2. Descargar datos diarios completos desde Open-Meteo.
        3. validar la respuesta de la API.
        4. Filtrar columnas para que coincidan con la estructura de la BBDD.
        5. Insertar los datos en la tabla 'mediciones' 
    """
    
    print("=== INGESTA DIARIA COMPLETA (EXÓGENAS) ===")

    # Abrir conexión a SQLite
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Obtener las columnas reales de la tabla 'mediciones'
    # Esto permite filtrar solo las columnas compatibles  antes de insertar
    cur.execute("PRAGMA table_info(mediciones)")
    existing_cols = {row[1] for row in cur.fetchall()}

    print("\nColumnas detectadas en la tabla:")
    for col in existing_cols:
        print("  -", col)

    # Fechas válidas para Open-Meteo (NO permite futuro)
    start_date = "2000-01-01"
    end_date = str(date.today())

    # Recorrer todas las ciudades configuradas
    for city, (lat, lon) in COORDS.items():
        print(f"\nDescargando datos para {city}...")

        # Construcción de la URL con todas las variables diarias
        url = (
            "https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date={start_date}"
            f"&end_date={end_date}"
            f"&daily={','.join(DAILY_VARS)}"
            "&timezone=auto"
        )

        # Petición HTTP a la API
        r = requests.get(url)
        data = r.json()

        # Validación: si no hay bloque "daily", no se puede procesar
        if "daily" not in data:
            print(f"⚠ La API no devolvió datos diarios para {city}. Respuesta:")
            print(data)
            continue
        
        # convertir el bloque "daily" en el DataFrame
        df = pd.DataFrame(data["daily"])
        
        # Añadir columna con el nombre de la estación (ciudad)
        df["estacion"] = city

        # Filtrar solo columnas que existen en tu tabla Sqlite
        # Esto evita erorres se Open-Meteo añade nuevas variables
        df = df[[col for col in df.columns if col in existing_cols]]

        # Insertar los datos en la tabla 'mediciones'
        df.to_sql("mediciones", conn, if_exists="append", index=False)

        print(f"✔ {len(df)} filas insertadas para {city}")

    # Cerrar la conexión
    conn.close()
    print("\nIngesta completada.")
