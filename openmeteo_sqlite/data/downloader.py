"""
Módulo: downloader.py
Autor: Tamara
Descripción:
    Descarga y agregación de datos mteorológicos desde la API histórica
    de Open-Meteo. este módulo gestiona la obtención de datos diaros y
    horarios, maneja límites de uso (rate limits) y fusiona la información
    en un único DataFrame listo para su procesamiento posterior.
    
    Forma parte del sistema de predicción y alertas meteorológicas.
"""

import time
import pandas as pd
import requests
from datetime import datetime

from config.config import START_DATE, END_DATE


def descargar_datos_openmeteo(lat, lon, fecha_ini=None, fecha_fin=None):
    """
    Descarga de datos históricos y horarios de Open-Meteo para una ubicación dada.
    
    Parámetros:
        lat (float): Latitud de la ubicación.
        lon (float): Longitud de la ubicación.
        fecha_ini (str): Fecha inicial (YYYY-MM-DD). Si no se pasa usa START_DATE.
        fecha_fin (str): Fecha final (YYYY-MM-DD). Si no se pasa, usa END_DATE.
    
    Flujo:
        1. Construcción de la Url con vaiables diarios y horarias.
        2. Hasta 10 intentos de descarga (manejo de rwate limit).
        3. Procesamiento de datos diarios y horarios,
        4. Agregación de datos horarios por día.
        5. Unión de ambos DataFrames.
    """
    
    # si no se pasan fechas, se usan las configuradas en el proyecto.
    fecha_ini = fecha_ini or START_DATE
    fecha_fin = fecha_fin or END_DATE
    # Construcción de la URL con parámetros diarios y horarios.
    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={fecha_ini}&end_date={fecha_fin}"
        "&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
        "apparent_temperature_mean,shortwave_radiation_sum,precipitation_sum,"
        "sunshine_duration,daylight_duration,wind_direction_10m_dominant"
        "&hourly=relative_humidity_2m,surface_pressure,wind_speed_10m,cloud_cover"
        "&timezone=auto"
    )

    # Hasta 10 intentos para manejar rate limits o fallos temporales.
    for intento in range(10):
        r = requests.get(url)
        data = r.json()

        #--------------------------------------------------------------
        # CASO OK
        #--------------------------------------------------------------
        if "daily" in data and "hourly" in data:
            # Procesamiento de datos diarios
            df_daily = pd.DataFrame(data["daily"])
            df_daily["time"] = pd.to_datetime(df_daily["time"])

            # procesamiento de datos horarios
            df_hourly = pd.DataFrame(data["hourly"])
            df_hourly["time"] = pd.to_datetime(df_hourly["time"])
            df_hourly["date"] = df_hourly["time"].dt.date

            # Agregación diaria de variables horarias
            df_hourly_daily = df_hourly.groupby("date").agg({
                "relative_humidity_2m": "mean",
                "surface_pressure": "mean",
                "wind_speed_10m": "mean",
                "cloud_cover": "mean"
            }).reset_index()

            # Renombrar 'date' -> 'time' para poder fusionar
            df_hourly_daily.rename(columns={"date": "time"}, inplace=True)
            df_hourly_daily["time"] = pd.to_datetime(df_hourly_daily["time"])

            # unión de datos diarios + agregados horarios
            df = pd.merge(df_daily, df_hourly_daily, on="time", how="left")
            return df

        #--------------------------------------------------------------
        # RATE LIMIT DETECTADO
        #--------------------------------------------------------------
        if "reason" in data and "limit" in data["reason"].lower():
            print("Límite de Open‑Meteo alcanzado. Esperando 60 segundos…")
            time.sleep(60)
            continue

        #--------------------------------------------------------------
        # OTRO ERROR
        #--------------------------------------------------------------
        raise ValueError(f"Error inesperado al descargar datos: {data}")

    # Si tras 10 intentos no se consigue respuesta válida, se aborta.
    raise ValueError("No se pudo descargar datos tras varios intentos.")

