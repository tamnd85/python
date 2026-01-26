"""
Módulo: cleaning.py
Autor: Tamara
Descripción:
    Limpieza robusta de datos meteorológicos.
"""

import pandas as pd
import numpy as np

def clean_df(df):
    df = df.copy()

    # Convertir numéricos
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")

    # Convertir fechas
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df = df.dropna(subset=["time"])
        df = df.sort_values("time")

    # Imputación numérica
    num_cols = df.select_dtypes(include=["number"]).columns
    df[num_cols] = df[num_cols].interpolate().ffill().bfill()

    return df
