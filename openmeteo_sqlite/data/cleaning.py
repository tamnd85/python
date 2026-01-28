"""
Módulo: cleaning.py
Autor: Tamara
Descripción:
    Limpieza robusta de datos meteorológicos.
"""

import pandas as pd
import numpy as np

def clean_df(df):
    """
    Realiza una limpieza robusta sobre un DataFrame meteorológico.
    
    Pasos principales:
        1. eliminar columnas completamente vacías.
        2. Convertir columnas a tipo numérico cuando sea posible.
        3. Parseo robusto de fechas en la columna 'time'.
        4. Reporte de valores nulos antes de imputar.
        5. Corrección de rangos físicos básicos.
        6. Imputación numérica ,ediante interpolación + forward/backward fill.
        7. Reporte de nulos después de imputar
    """
    
    # Se trabaja sobre una copia para no modificar el DataFrame original.
    df = df.copy()

    #----------------------------------------------------------------------------
    # 1. Columnas completamente vacías
    #----------------------------------------------------------------------------
    # Identifica columnas done todos los valores son Nan.
    cols_vacias = df.columns[df.isna().all()]
    if len(cols_vacias) > 0:
        print("⚠ Columnas sin datos:", list(cols_vacias))
        # Se eliminan para evitar ruido y problemas posteriores.
        df = df.drop(columns=cols_vacias)

    #-----------------------------------------------------------------------------
    # 2. Convertir numéricos
    #-----------------------------------------------------------------------------
    # Intenta convertir cada columna a numérica.
    # errors='ignore' evita lanzar errores so la conversión no es posible
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    #-----------------------------------------------------------------------------
    # 3. Convertir fechas
    #-----------------------------------------------------------------------------
    if "time" in df.columns:
        # Intento 1: formato ISO estándar
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

        # Intento 2: si fallan muchas → probar formato Open-Meteo
        if df["time"].isna().mean() > 0.5:
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M", errors="coerce")

        # Intento 3: formato con segundos y Z
        if df["time"].isna().mean() > 0.5:
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%SZ", errors="coerce")

        # Eliminar filas sin fecha válida
        df = df.dropna(subset=["time"])
        
        # Ordenar por fecha para mantener coherencia temporal
        df = df.sort_values("time")
        
        # Eliminar duplicados basados en la columna temporal
        df = df.drop_duplicates(subset=["time"])


    #------------------------------------------------------------------------------
    # 4. Reporte de nulos antes
    #------------------------------------------------------------------------------
    print("Nulos antes de imputar:")
    print(df.isna().sum())

    #------------------------------------------------------------------------------
    # 5. Rango físico básico
    #------------------------------------------------------------------------------
    # Humedad relativa: 0-100%
    if "relative_humidity_2m" in df:
        df["relative_humidity_2m"] = df["relative_humidity_2m"].clip(0, 100)

    # Velocidad del viento: no puede ser negativa
    if "wind_speed_10m" in df:
        df["wind_speed_10m"] = df["wind_speed_10m"].clip(lower=0)
    
    # Presión superficial: rango físico razonable
    if "surface_pressure" in df:
        df["surface_pressure"] = df["surface_pressure"].clip(850, 1100)

    #------------------------------------------------------------------------------
    # 6. Imputación numérica
    #------------------------------------------------------------------------------
    # Selecciona solo columnas numéricas para imputación
    num_cols = df.select_dtypes(include=["number"]).columns
    
    # Interpolación lineal + forward fill + back fill.
    # Garantiza que no queden huecos en series numéricas.
    df[num_cols] = df[num_cols].interpolate().ffill().bfill()

    #-------------------------------------------------------------------------------
    # 7. Reporte de nulos después
    #-------------------------------------------------------------------------------
    print("Nulos después de imputar:")
    print(df.isna().sum())

    # Devuelve el DataFrame limpio y consistente.
    return df
