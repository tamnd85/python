"""
================================================================================
MÓDULO: download.py
PROYECTO: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
AUTOR: Tamara
DESCRIPCIÓN:
    Este módulo gestiona la adquisición de datos desde la API de Open-Meteo.
    Implementa una lógica dual para alternar entre datos históricos (Archive)
    y datos de previsión (Forecast) según el rango de fechas solicitado.

FUNCIONALIDADES CLAVE:
    1. Lógica Híbrida de API: Selecciona automáticamente el endpoint correcto 
       según si la fecha solicitada es pasada o futura.
    2. Agregación Temporal: Transforma datos horarios (hourly) en promedios 
       diarios para mantener la consistencia con la tabla de mediciones.
    3. Resiliencia: Implementa un sistema de reintentos (hasta 5) con pausas 
       estratégicas ante fallos de conexión o límites de tasa (Rate Limit).
    4. Mezcla de Datos (Merge): Combina variables diarias (temperaturas mín/máx)
       con variables horarias promediadas (humedad, presión).

FLUJO DE DATOS:
    Input:  Coordenadas (Lat/Lon) y Rango de Fechas.
    Process: Request HTTP -> JSON Parsing -> Pandas Aggregation -> Data Join.
    Output: DataFrame unificado y listo para el proceso de limpieza (cleaning.py).
================================================================================
"""

import time
import pandas as pd
import requests
from datetime import date
from config.config import START_DATE, END_DATE

def descargar_datos_openmeteo(lat, lon, fecha_ini=None, fecha_fin=None):
    """
    Descarga y unifica datos diarios y horarios de Open-Meteo.
    
    Args:
        lat (float): Latitud de la ubicación.
        lon (float): Longitud de la ubicación.
        fecha_ini (str, opcional): Fecha de inicio YYYY-MM-DD.
        fecha_fin (str, opcional): Fecha de fin YYYY-MM-DD.
        
    Returns:
        pd.DataFrame: Conjunto de datos combinado o DataFrame vacío si falla.
    """
    fecha_ini_str = str(fecha_ini or START_DATE)
    fecha_fin_str = str(fecha_fin or END_DATE)
    hoy_str = str(date.today())

    # ---------------------------------------------------------------------------
    # 1. SELECCIÓN DINÁMICA DE ENDPOINT (FORECAST VS ARCHIVE)
    # ---------------------------------------------------------------------------
    # Si la fecha final es hoy o futura, usamos el endpoint de Forecast.
    if fecha_fin_str >= hoy_str:
        print(f"📡 Usando API de Forecast para {fecha_fin_str}...")
        url = (
            f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
            "&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_mean,shortwave_radiation_sum,precipitation_sum,"
            "sunshine_duration,daylight_duration,wind_direction_10m_dominant"
            "&hourly=relative_humidity_2m,surface_pressure,wind_speed_10m,cloud_cover"
            "&timezone=auto&past_days=31"
        )
    else:
        # Para datos puramente históricos, usamos el endpoint de Archive.
        print(f"📚 Usando API de Archivo Histórico para el rango {fecha_ini_str} a {fecha_fin_str}...")
        url = (
            f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}"
            f"&start_date={fecha_ini_str}&end_date={fecha_fin_str}"
            "&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_mean,shortwave_radiation_sum,precipitation_sum,"
            "sunshine_duration,daylight_duration,wind_direction_10m_dominant"
            "&hourly=relative_humidity_2m,surface_pressure,wind_speed_10m,cloud_cover"
            "&timezone=auto"
        )

    # ---------------------------------------------------------------------------
    # 2. GESTIÓN DE PETICIONES Y REINTENTOS
    # ---------------------------------------------------------------------------
    for intento in range(5):
        try:
            r = requests.get(url, timeout=60) 
            data = r.json()

            if "daily" in data and "hourly" in data:
                # Procesamiento de Datos Diarios
                df_daily = pd.DataFrame(data["daily"])
                df_daily["time"] = pd.to_datetime(df_daily["time"])

                # Procesamiento de Datos Horarios (Agregación)
                df_hourly = pd.DataFrame(data["hourly"])
                df_hourly["time"] = pd.to_datetime(df_hourly["time"])
                df_hourly["date_tmp"] = df_hourly["time"].dt.date
                
                # Agregamos los datos horarios para obtener una media diaria única
                df_hourly_agg = df_hourly.groupby("date_tmp").agg({
                    "relative_humidity_2m": "mean",
                    "surface_pressure": "mean",
                    "wind_speed_10m": "mean",
                    "cloud_cover": "mean"
                }).reset_index()
                
                # Sincronizamos nombres de columna para el cruce (Merge)
                df_hourly_agg.rename(columns={"date_tmp": "time"}, inplace=True)
                df_hourly_agg["time"] = pd.to_datetime(df_hourly_agg["time"])

                # Unión de tablas: Daily + Hourly_Aggregated
                df_res = pd.merge(df_daily, df_hourly_agg, on="time", how="left")
                
                # -----------------------------------------------------------------------
                # 3. FILTRADO FINAL POR MÁSCARA TEMPORAL
                # -----------------------------------------------------------------------
                df_res['time_only'] = df_res['time'].dt.date
                f_ini_dt = pd.to_datetime(fecha_ini_str).date()
                f_fin_dt = pd.to_datetime(fecha_fin_str).date()
                
                mask = (df_res['time_only'] >= f_ini_dt) & (df_res['time_only'] <= f_fin_dt)
                
                # Si es forecast, devolvemos desde la fecha de inicio hasta el final de la serie
                if fecha_fin_str >= hoy_str:
                    return df_res[df_res['time_only'] >= f_ini_dt].drop(columns=['time_only'])
                
                return df_res.loc[mask].drop(columns=['time_only'])

            if "error" in data:
                print(f"❌ Error API: {data.get('reason', 'Desconocido')}")
                break

        except Exception as e:
            print(f"⚠️ Intento {intento+1} fallido: {e}")
            time.sleep(5) # Pausa de seguridad antes de reintentar

    return pd.DataFrame()