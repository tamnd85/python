"""
Módulo: database.py
proyecto: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
Autor: Tamara
Descripción:
    Módulo de gestión de persistencia en SQLite para el sistema meteorológico.
    
    Este módul implementa:
        1. Creación automática de esquema si no existe.
        2. Borrado seguro de registros por ciudad.
        3. Inserción robusta de datos limpios en la base de datos.
        4. REcuperación flexible de registros, ignorando mayúscula/minúscula.

Objetivos:
    - Gaerantizar consistencia en al bbdd.
    - Evitar duplicados y errores por diferencias de capitalización.
    - Asegurar que todas las fechas se almacenan en formato ISO (YYYY-MM-DD).
"""

import sqlite3
import pandas as pd
from config.config import DB_PATH, TABLA_DB

# -----------------------------------------------------------------------------
# 1. GESTIÓN DEL ESQUEMA
# -----------------------------------------------------------------------------

def crear_tabla_si_no_existe():
    """
    Crea la tabla principal del sistema si aún no existe.
    
    La tabla contiene todas las variables meteorológicas necesarias para entranamiento,
    forecast y análisis. Todas las fechas se alamacenan como  TEXTO en formato ISO para 
    evitar problemas de epcoh en SQLite.
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

# -----------------------------------------------------------------------------
# 2. LIMPIEZA DE REGISTROS (Normalizado a minúsculas)
# -----------------------------------------------------------------------------

def borrar_ciudad(ciudad):
    """
    Elimina todos lso registros asociados a una ciudad, ignorando mayúsculas.
    
    Parámetros:
        ciudad: str
            Nombre de la ciudad a eliminar ( se normaliza en minúscula).
    """
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
    """
    Inserta un dataFrame en la base de datos, normalizando fechas y estación.
    
    Reglas:
        - Las fechas se conviertn a ISo (YYYY-MM-DD).
        - La estación se almacena siempre en minúsculas.
        - Se descartan filas sin fecha válida.
        
    Parámetros:
        df: pd.DataFrame
            DataFrame limpio y listo para persistencia
        estacion: str
            Nombre de la ciudad/estación asociada a los registros.
    """
    crear_tabla_si_no_existe()
    df = df.copy()

    # Normalización obligatoria
    df['time'] = pd.to_datetime(df['time'], errors='coerce').dt.strftime('%Y-%m-%d')
    df["estacion"] = estacion.lower()
    
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
    Recupera registros desde SQLite, ignorando mayúsculas/minúsculas.
    
    Parámetros:
        estación: str, opcional
            Si se especifica, filtra por esa esatción.
            Si no, devuleve toda la tablas.
    
    Retorna:
        pd.DataFrame
            dataFrame con los registros solicitados.
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