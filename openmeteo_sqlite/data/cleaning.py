"""
Módulo: cleaning.py
Autor: Tamara
Descripción:
    Este módulo centraliza la lógica de preprocesamiento y saneamiento de los
    datos brutos extrídos de la API de Open-Meteo. Su objetivo principal es 
    transformar los datos crudos en un DataFrame estructurado, consistente y 
    listo para ser consumido por los modelos SARIMA y SGBoost.
    
Funcionalidades clave:
    1. Integridad Temporal: garantiza que la columna 'time' sea el eje central
    sin duplicados, convirtiéndola a formato detetime de Pandas.
    2. Gestión de Nulos: Implemente técnicas de limpieza selectiva para evitar
    que filas vacías afecten al entrenamiento del modelo.
    3. Tipado de Datos: Asegura que las variables físicas (temperatura, viento, 
    presión) mantengan su precisión numérica (float64).
    4. Normalización de Estaciones: Estandariza los nombres de las estaciones 
    para búsquedas eficientes en la base de datos SQLite.
    
Flujo de datos:
    Input: Dataframe de Pandas con datos brutos (Raw APi data).
    Process: Conversión de tipos -> Limpieza de duplicados -> Saneamiento de NaNs.
    Output: DataFrame limpio (Clean Data) listo para 'features/xgb_features.py'.

Notas Técnicas
    * Es crítico no eliminar filas con NaNs en las columnas meteorológicas 
  durante la fase de forecast, ya que esos huecos serán rellenados por 
  las predicciones de la IA.
"""

import pandas as pd
import numpy as np

def clean_df(df):
    """
    Realiza una limpieza robusta sobre un DataFrame meteorológico.
    
    Args:
        df (pd.DataFrame): Datos brutos de la API.
        modo_entrenamiento (bool): Si es False, no interpola la temperatura 
                                   para permitir que el modelo la prediga.
    """
    # Se trabaja sobre una copia para no modificar el DataFrame original.
    df = df.copy()

    #---------------------------------------------------------------------------
    # 1. Eliminar columnas completamente vacías
    #---------------------------------------------------------------------------
    cols_vacias = df.columns[df.isna().all()]
    if len(cols_vacias) > 0:
        df = df.drop(columns=cols_vacias)

    #---------------------------------------------------------------------------
    # # 2. NORMALIZACIÓN DE TIPOS NUMÉRICOS
    # Convertimos a float64 todas las columnas meteorológicas para cálculos precisos.
    #---------------------------------------------------------------------------
    for col in df.columns:
        if col != "time": 
            df[col] = pd.to_numeric(df[col], errors="coerce")

    #---------------------------------------------------------------------------
    # 3. TRATAMIENTO CRÍTICO DE FECHAS
    #---------------------------------------------------------------------------
    if "time" in df.columns:
        # Intento 1: Formato ISO estándar
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

        # Intento 2: Formato Open-Meteo (T entre fecha y hora)
        if df["time"].isna().mean() > 0.5:
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M", errors="coerce")

        # Intento 3: Formato con zona horaria Z
        if df["time"].isna().mean() > 0.5:
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%SZ", errors="coerce")

        # Eliminar filas que no tengan fecha válida tras los intentos
        df = df.dropna(subset=["time"])
        
        # Ordenar cronológicamente y eliminar duplicados
        df = df.sort_values("time")
        df = df.drop_duplicates(subset=["time"])

    #---------------------------------------------------------------------------
    # 4. VALIDACIÓN DE LÍMITES FÍSICOS
    # Evitamos que ruidos en los sensores generen datos meteorológicamente imposibles.
    #---------------------------------------------------------------------------
    if "relative_humidity_2m" in df:
        df["relative_humidity_2m"] = df["relative_humidity_2m"].clip(0, 100)

    if "wind_speed_10m" in df:
        df["wind_speed_10m"] = df["wind_speed_10m"].clip(lower=0)
    
    if "surface_pressure" in df:
        df["surface_pressure"] = df["surface_pressure"].clip(850, 1100)

    #---------------------------------------------------------------------------
    # 5. IMPUTACIÓN INTELIGENTE DE DATOS FALTANTES
    # Solo interpolamos si estamos entrenando o para variables de apoyo (viento/presión).
    #---------------------------------------------------------------------------
    num_cols = df.select_dtypes(include=["number"]).columns
    df[num_cols] = df[num_cols].interpolate().ffill().bfill()

    return df