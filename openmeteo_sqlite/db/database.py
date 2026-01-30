"""
================================================================================
MÓDULO: database.py (CORREGIDO - Case Insensitive)
================================================================================
"""

import sqlite3
import pandas as pd
from config.config import DB_PATH, TABLA_DB

# -----------------------------------------------------------------------------
# 1. GESTIÓN DEL ESQUEMA
# -----------------------------------------------------------------------------

def crear_tabla_si_no_existe():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_DB} (
            time TEXT,
            temperature_2m_mean REAL,
            temperature_2m_max REAL,
            temperature_2m_min REAL,
            apparent_temperature_mean REAL,
            shortwave_radiation_sum REAL,
            precipitation_sum REAL,
            sunshine_duration REAL,
            daylight_duration REAL,
            wind_direction_10m_dominant REAL,
            relative_humidity_2m REAL,
            surface_pressure REAL,
            wind_speed_10m REAL,
            cloud_cover REAL,
            estacion TEXT
        )
    """)
    conn.commit()
    conn.close()

# -----------------------------------------------------------------------------
# 2. LIMPIEZA DE REGISTROS (Normalizado a minúsculas)
# -----------------------------------------------------------------------------

def borrar_ciudad(ciudad):
    crear_tabla_si_no_existe()
    conn = sqlite3.connect(DB_PATH)
    # Forzamos minúsculas para asegurar el borrado
    ciudad_clean = ciudad.lower() 
    try:
        conn.execute(f"DELETE FROM {TABLA_DB} WHERE LOWER(estacion) = ?", (ciudad_clean,))
        conn.commit()
        print(f"🧹 Datos previos de {ciudad_clean} eliminados de la DB.")
    except Exception as e:
        print(f"⚠ Error al limpiar datos de {ciudad}: {e}")
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# 3. PERSISTENCIA DE DATOS (Normalizado a minúsculas)
# -----------------------------------------------------------------------------

def insertar_en_db(df, estacion):
    crear_tabla_si_no_existe()
    df = df.copy()

    # REGLA DE ORO: Fechas ISO y estación en minúsculas
    df['time'] = pd.to_datetime(df['time'], errors='coerce').dt.strftime('%Y-%m-%d')
    df["estacion"] = estacion.lower() # <--- GUARDAMOS SIEMPRE EN MINÚSCULAS
    
    df = df.dropna(subset=['time'])

    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql(TABLA_DB, conn, if_exists="append", index=False, 
                dtype={'time': 'TEXT'})
        conn.commit()
        print(f"💾 Guardados {len(df)} registros reales para {df['estacion'].iloc[0]}.")
    except Exception as e:
        print(f"⚠ Error al insertar: {e}")
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# 4. EXTRACCIÓN DE DATOS (Búsqueda robusta)
# -----------------------------------------------------------------------------

def load_from_db(estacion=None):
    """
    Recupera registros usando LOWER para ignorar mayúsculas/minúsculas.
    """
    crear_tabla_si_no_existe()
    conn = sqlite3.connect(DB_PATH)

    if estacion:
        # Buscamos usando LOWER en SQL para que coincida siempre
        estacion_clean = estacion.lower()
        query = f"SELECT * FROM {TABLA_DB} WHERE LOWER(estacion) = ?"
        df = pd.read_sql(query, conn, params=(estacion_clean,))
    else:
        query = f"SELECT * FROM {TABLA_DB}"
        df = pd.read_sql(query, conn)

    conn.close()
    return df