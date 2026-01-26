import requests
import sqlite3
import pandas as pd
from datetime import date
from tqdm import tqdm

DB_PATH = "db/openmeteo.db"

# Coordenadas de tus ciudades
COORDS = {
    "Burgos": (42.3439, -3.6969),
    # "Santander": (43.4623, -3.8099),
}

# Variables diarias completas (Open-Meteo)
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
    print("=== INGESTA DIARIA COMPLETA (EXÓGENAS) ===")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Comprobar columnas existentes en la tabla
    cur.execute("PRAGMA table_info(mediciones)")
    existing_cols = {row[1] for row in cur.fetchall()}

    print("\nColumnas detectadas en la tabla:")
    for col in existing_cols:
        print("  -", col)

    # Fechas válidas para Open-Meteo (NO permite futuro)
    start_date = "2000-01-01"
    end_date = str(date.today())

    for city, (lat, lon) in COORDS.items():
        print(f"\nDescargando datos para {city}...")

        url = (
            "https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date={start_date}"
            f"&end_date={end_date}"
            f"&daily={','.join(DAILY_VARS)}"
            "&timezone=auto"
        )

        r = requests.get(url)
        data = r.json()

        # Validación robusta
        if "daily" not in data:
            print(f"⚠ La API no devolvió datos diarios para {city}. Respuesta:")
            print(data)
            continue

        df = pd.DataFrame(data["daily"])
        df["estacion"] = city

        # Filtrar solo columnas que existen en tu tabla
        df = df[[col for col in df.columns if col in existing_cols]]

        # Insertar en SQLite
        df.to_sql("mediciones", conn, if_exists="append", index=False)

        print(f"✔ {len(df)} filas insertadas para {city}")

    conn.close()
    print("\nIngesta completada.")
