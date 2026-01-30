"""
Módulo: get_data.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
Autor: Tamara
Descripción:
    Este módulo actúa como el director de orquesta del pipeline de datos. 
    Coordina secuencialmente la descarga (downloader), el saneamiento (cleaning) 
    y la persistencia en la base de datos (database).

Funcionalidades:
    1. Orquestación Secuencial:
        Gestiona el flujo completo desde la API hasta SQLite.
    2. Normalización ISO:
        Convierte todas las fechas a formato YYYY-MM-DD para evitar problemas
        con SQLite y garantizar consistencia.
    3. Gestión de Persistencia: 
        Permite elegir entre sobrescribir el histórico completo o añadir nuevos registros
        en modo incremental/forecast.
    4. Validación de Integridad: 
        Detecta fallos en la API o en la limpieza y evita insertar datos corruptos

Flujo de trabajoO:
    API -> Downloader -> Cleaning -> SQLite Persistence.
"""

from datetime import datetime, date
import pandas as pd
from data.downloader import descargar_datos_openmeteo
from data.cleaning import clean_df
from db.database import insertar_en_db, borrar_ciudad

#----------------------------------------------------------------------------------------------
# Función principal
#----------------------------------------------------------------------------------------------
def get_data(ciudad, lat, lon, fecha_ini=None, fecha_fin=None, modo_append=False):
    """
    Coordina la descarga, limpieza y persistencia de datos meteorológicos.

    Parámetros:
        ciudad: str 
            Nombre de la estación.
        lat: float
            Latitud de la ubicación.
        lon: float
            Longitud de la ubicación.
        fecha_ini: str
            Fecha de inicio del rango solicitado (YYYY-MM-DD).
        fecha_fin: str
            Fecha de fin del rango solicitado (YYYY-MM-DD).
        modo_append: bool: 
            Si es True, conserva datos previos y añade nuevos registros.
            Si es false, borra el histórico de esa ciudad antes de insertar.
    
        Retorna:
            pd.DataFrame or None
                DataFrame final procedado e inserción en SQLite.
                Retorna None si ocurre un fallo en cualquier fase.
    """
    # ---------------------------------------------------------------------------
    # 1. NORMALIZACIÓN DE PARÁMETROS TEMPORALES
    # ---------------------------------------------------------------------------
    # Forzamos formato YYYY-MM-DD para que la API de Open-Meteo no de errores.
    f_ini = pd.to_datetime(fecha_ini).strftime('%Y-%m-%d')
    f_fin = pd.to_datetime(fecha_fin).strftime('%Y-%m-%d')

    print(f"\n📡 --- INICIANDO PROCESO PARA: {ciudad} ---")
    print(f"📅 Rango solicitado: {f_ini} al {f_fin}")

    # ---------------------------------------------------------------------------
    # 2. FASE DE ADQUISICIÓN (API CALL)
    # ---------------------------------------------------------------------------
    df = descargar_datos_openmeteo(lat, lon, f_ini, f_fin)

    # Verificación de respuesta
    if df is None or df.empty:
        print(f"❌ La API no devolvió datos para {ciudad} en este rango.")
        return None
    
    print(f"📊 Datos brutos recibidos: {len(df)} registros.")

    # ---------------------------------------------------------------------------
    # 3. FASE DE SANEAMIENTO (CLEANING)
    # ---------------------------------------------------------------------------
    # Aplicamos la lógica de cleaning.py protegiendo la columna 'time'.
    df = clean_df(df)
    
    if df.empty:
        print(f"⚠ El proceso de limpieza eliminó todos los registros. Revisa cleaning.py")
        return None

    # ---------------------------------------------------------------------------
    # 4. PREPARACIÓN PARA SQLITE 
    # ---------------------------------------------------------------------------
    # SQLite no tiene tipo 'Date'. Convertimos el objeto Timestamp a String ISO.
    df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
    df["estacion"] = ciudad

    # ---------------------------------------------------------------------------
    # 5. GESTIÓN DE PERSISTENCIA EN BASE DE DATOS
    # ---------------------------------------------------------------------------
    # Si modo_append=False (Carga Histórica), limpiamos el histórico de esa ciudad.
    if not modo_append:
        print(f"🧹 Limpiando registros antiguos de {ciudad}...")
        borrar_ciudad(ciudad)
    
    # Inserción de los nuevos registros procesados
    insertar_en_db(df, ciudad)
    
    # Resumen de finalización
    print(f"✅ Finalizado: {len(df)} registros procesados (Desde {df['time'].min()} hasta {df['time'].max()})")
    return df