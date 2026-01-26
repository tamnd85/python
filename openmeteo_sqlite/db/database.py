"""
Módulo: database.py
Gestión optimizada de la base de datos SQLite:
    - Crear tabla si no existe
    - Insertar datos masivamente (rápido)
    - Crear índice único solo una vez
    - Cargar datos
"""

import sqlite3
import pandas as pd
from config.config import DB_PATH, TABLA_DB


# ============================================================
# CREAR TABLA (SIN ÍNDICE)
# ============================================================

def crear_tabla_si_no_existe():
    conn = sqlite3.connect(DB_PATH)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLA_DB} (
            time TEXT,
            temperature_2m_mean REAL,
            temperature_2m_min REAL,
            temperature_2m_max REAL,
            precipitation_sum REAL,
            relative_humidity_2m REAL,
            surface_pressure REAL,
            wind_speed_10m REAL,
            cloud_cover REAL,
            estacion TEXT
        )
    """)

    conn.commit()
    conn.close()


# ============================================================
# CREAR ÍNDICE ÚNICO (SOLO UNA VEZ)
# ============================================================

def crear_indice_unico():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(f"""
            CREATE UNIQUE INDEX idx_fecha_estacion
            ON {TABLA_DB}(time, estacion);
        """)
        print("Índice único creado correctamente.")
    except sqlite3.OperationalError:
        # El índice ya existe → no hacer nada
        pass
    conn.commit()
    conn.close()


# ============================================================
# INSERTAR EN DB (RÁPIDO, SIN DUPLICADOS)
# ============================================================

def insertar_en_db(df, estacion):
    crear_tabla_si_no_existe()

    df = df.copy()
    df["estacion"] = estacion
    df["time"] = df["time"].astype(str)

    conn = sqlite3.connect(DB_PATH)

    try:
        df.to_sql(TABLA_DB, conn, if_exists="append", index=False, method="multi")
    except Exception:
        # Si hay duplicados, el índice único los bloqueará
        pass

    conn.close()


# ============================================================
# CARGAR DATOS
# ============================================================

def load_from_db(estacion=None):
    crear_tabla_si_no_existe()
    conn = sqlite3.connect(DB_PATH)

    if estacion:
        query = f"SELECT * FROM {TABLA_DB} WHERE estacion = ?"
        df = pd.read_sql(query, conn, params=(estacion,))
    else:
        query = f"SELECT * FROM {TABLA_DB}"
        df = pd.read_sql(query, conn)

    conn.close()
    return df
