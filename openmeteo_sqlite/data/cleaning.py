"""
Módulo: cleaning.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
Autor: Tamara
Descripción:
    Este módulo centraliza la lógica de preprocesamiento y saneamiento de los
    datos brutos extrídos de la API de Open-Meteo. Su objetivo principal es 
    transformar los datos crudos en un DataFrame estructurado, consistente y 
    listo para ser consumido por los modelos SARIMA y SGBoost.
    
Funcionalidades clave:
    1. Integridad Temporal:
        - Conversión robusta de la columna 'time' a datetime.
        - Eliminación de duplicados y ordenación cronológica.
    2. Gestión de Nulos: 
        - Eliminación de columnas completamente vacías.
        - Interpolación inteligente de valores numéricos.
    3. Tipado de Datos:
        - Conversión de todas las variablea físicas a float64 para grantizar
        precisión numérica en los modelos.
    4. Validación física:
        - Corrección de valores imposibles (humedad >100%, presión se fuera de rango).
    
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

#-----------------------------------------------------------------------------------
# Función principal de limpieza
#-----------------------------------------------------------------------------------

def clean_df(df):
    """
    Realiza una limpieza robusta sobre un DataFrame meteorológico.
    
    Parámetros:
        df: pd.dataFrame
            datos brutos obtenidos de la API de OpenMeteo.
    
    Retorna:
        pd.DataFrame
            DataFrame limpio, consistenete y lkisto para la generación de features.
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