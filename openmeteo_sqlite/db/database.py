"""
Módulo: database.py
Autor: Tamara
Descripción:
    Gestión optimizada de la base de datos SQLite utilizada por el sistema
    meteorológico. Este módulo centraliza todas las operaciones relacionadas
    con la persistencia de datos.
"""

import sqlite3
import pandas as pd
from config.config import DB_PATH, TABLA_DB


#-----------------------------------------------------------------------------
# CREAR TABLA (CORREGIDA: SOLO LAS COLUMNAS REALES DEL DATAFRAME)
#-----------------------------------------------------------------------------

def crear_tabla_si_no_existe():
    """
    Crea la tabla principal de mediciones si aún no existe.
    Estructura ajustada EXACTAMENTE a las columnas reales del DataFrame.
    """
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


#-----------------------------------------------------------------------------------
# CREAR ÍNDICE ÚNICO
#-----------------------------------------------------------------------------------

def crear_indice_unico():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(f"""
            CREATE UNIQUE INDEX idx_fecha_estacion
            ON {TABLA_DB}(time, estacion);
        """)
        print("Índice único creado correctamente.")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()


#-----------------------------------------------------------------------------------
# INSERTAR EN DB (CON CHUNKS)
#-----------------------------------------------------------------------------------

def insertar_en_db(df, estacion):
    """
    Inserta un DataFrame en la base de datos en CHUNKS para evitar
    el error 'too many SQL variables' de SQLite.
    """
    crear_tabla_si_no_existe()

    df = df.copy()
    df["estacion"] = estacion
    df["time"] = df["time"].astype(str)

    conn = sqlite3.connect(DB_PATH)

    chunk_size = 300  # evita superar el límite de SQLite

    try:
        for start in range(0, len(df), chunk_size):
            end = start + chunk_size
            df_chunk = df.iloc[start:end]

            df_chunk.to_sql(
                TABLA_DB,
                conn,
                if_exists="append",
                index=False
            )

    except Exception as e:
        print("⚠ Error insertando en DB:", e)

    conn.close()


#-----------------------------------------------------------------------------------
# CARGAR DATOS
#-----------------------------------------------------------------------------------

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
