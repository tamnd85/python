"""
================================================================================
MÓDULO: database.py
PROYECTO: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
AUTOR: Tamara
DESCRIPCIÓN:
    Gestión optimizada de la base de datos SQLite. Este módulo centraliza 
    las operaciones CRUD (Crear, Leer, Borrar) y asegura que la persistencia
    de datos mantenga la integridad de los tipos de datos físicos y temporales.

FUNCIONALIDADES CLAVE:
    1. Esquema Rígido: Define una estructura de tabla que coincide exactamente
       con las variables de Open-Meteo, evitando errores de desajuste de columnas.
    2. Manejo de Fechas ISO: Implementa la "Regla de Oro" para almacenar fechas
       como TEXTO, garantizando la compatibilidad con el ecosistema de Pandas.
    3. Idempotencia: Mediante la función 'borrar_ciudad', permite realizar
       cargas limpias (full-load) sin duplicar registros históricos.
    4. Flexibilidad de Carga: Soporta extracciones totales o filtradas por estación.

AVISO TÉCNICO:
    SQLite no dispone de un tipo 'DATE' nativo. El uso de 'TEXT' en formato 
    ISO (YYYY-MM-DD) es obligatorio para que las comparaciones de rangos
    y el ordenamiento cronológico funcionen correctamente.
================================================================================
"""

import sqlite3
import pandas as pd
from config.config import DB_PATH, TABLA_DB

# -----------------------------------------------------------------------------
# 1. GESTIÓN DEL ESQUEMA (DÉFINICIÓN DE TABLA)
# -----------------------------------------------------------------------------

def crear_tabla_si_no_existe():
    """
    Inicializa la base de datos y define el esquema de la tabla principal.
    Utiliza tipos REAL para datos meteorológicos y TEXT para dimensiones.
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
# 2. LIMPIEZA DE REGISTROS (MANTENIMIENTO)
# -----------------------------------------------------------------------------

def borrar_ciudad(ciudad):
    """
    Elimina registros existentes de una estación para prevenir solapamientos.
    Se ejecuta típicamente antes de una carga histórica masiva.
    """
    crear_tabla_si_no_existe()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(f"DELETE FROM {TABLA_DB} WHERE estacion = ?", (ciudad,))
        conn.commit()
        print(f"🧹 Datos previos de {ciudad} eliminados de la DB.")
    except Exception as e:
        print(f"⚠ Error al limpiar datos de {ciudad}: {e}")
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# 3. PERSISTENCIA DE DATOS (ESCRITURA)
# -----------------------------------------------------------------------------

def insertar_en_db(df, estacion):
    """
    Persiste un DataFrame en SQLite aplicando la regla de oro de fechas.
    """
    crear_tabla_si_no_existe()
    df = df.copy()

    # --- REGLA DE ORO ANTI-1970 ---
    # Convertimos la columna a datetime y luego a STRING individualmente.
    # Esto asegura que cada registro conserve su día en formato ISO legible.
    df['time'] = pd.to_datetime(df['time'], errors='coerce').dt.strftime('%Y-%m-%d')
    df["estacion"] = estacion
    
    # Limpieza de seguridad para evitar insertar nulos en la columna índice
    df = df.dropna(subset=['time'])

    conn = sqlite3.connect(DB_PATH)
    try:
        # Obligamos explícitamente a SQLite a tratar 'time' como TEXTO
        df.to_sql(TABLA_DB, conn, if_exists="append", index=False, 
                dtype={'time': 'TEXT'})
        conn.commit()
        print(f"💾 Guardados {len(df)} registros reales para {estacion}.")
    except Exception as e:
        print(f"⚠ Error al insertar: {e}")
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# 4. EXTRACCIÓN DE DATOS (LECTURA)
# -----------------------------------------------------------------------------

def load_from_db(estacion=None):
    """
    Recupera registros desde la DB y los devuelve como DataFrame.
    Permite filtrado por estación para optimizar el uso de memoria.
    """
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