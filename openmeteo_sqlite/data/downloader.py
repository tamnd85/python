# data/downloader.py

import time
import pandas as pd
import requests
from datetime import datetime

from config.config import START_DATE, END_DATE


def descargar_datos_openmeteo(lat, lon, fecha_ini=None, fecha_fin=None):
    """
    Descarga datos diarios + horarios desde Open-Meteo y devuelve
    un DataFrame combinado (daily + hourly agregada).
    """

    # Si no se pasan fechas → usar las del config
    fecha_ini = fecha_ini or START_DATE
    fecha_fin = fecha_fin or END_DATE

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

    for intento in range(5):
        r = requests.get(url)
        data = r.json()

        if "daily" in data and "hourly" in data:
            df_daily = pd.DataFrame(data["daily"])
            df_daily["time"] = pd.to_datetime(df_daily["time"])

            df_hourly = pd.DataFrame(data["hourly"])
            df_hourly["time"] = pd.to_datetime(df_hourly["time"])
            df_hourly["date"] = df_hourly["time"].dt.date

            df_hourly_daily = df_hourly.groupby("date").agg({
                "relative_humidity_2m": "mean",
                "surface_pressure": "mean",
                "wind_speed_10m": "mean",
                "cloud_cover": "mean"
            }).reset_index()

            df_hourly_daily.rename(columns={"date": "time"}, inplace=True)
            df_hourly_daily["time"] = pd.to_datetime(df_hourly_daily["time"])

            df = pd.merge(df_daily, df_hourly_daily, on="time", how="left")
            return df

        if "reason" in data and "limit" in data["reason"].lower():
            print("Rate limit. Esperando 10 segundos…")
            time.sleep(10)
            continue

        raise ValueError(f"Error inesperado al descargar datos: {data}")

    raise ValueError("No se pudo descargar datos tras varios intentos.")
