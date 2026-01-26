"""
Módulo: xgb_features.py
Autor: Tamara (versión estable multiciudad – FIX FORECAST)
Descripción:
    Generación de features para XGBoost basadas en:
        - Tiempo
        - SARIMA
        - Residuo (real o predicho)
        - Lags y rollings por ciudad
"""

import pandas as pd
import numpy as np


# ============================================================
# FEATURES PARA ENTRENAMIENTO / EVALUACIÓN
# ============================================================

def preparar_features_xgb(df, modo_entrenamiento=True):
    """
    Genera features XGBoost coherentes para entrenamiento o inferencia.
    IMPORTANTE: lags y rollings se calculan POR CIUDAD.
    """

    df = df.copy()
    df = df.sort_values(["estacion", "time"]).reset_index(drop=True)

    # ----------------------------
    # Features temporales
    # ----------------------------
    df["dayofyear"] = df["time"].dt.dayofyear
    df["month"] = df["time"].dt.month
    df["dayofweek"] = df["time"].dt.dayofweek

    df["sin_doy"] = np.sin(2 * np.pi * df["dayofyear"] / 365)
    df["cos_doy"] = np.cos(2 * np.pi * df["dayofyear"] / 365)

    # ----------------------------
    # Lags y rollings del RESIDUO (por ciudad)
    # ----------------------------
    lags = [1, 3, 7, 14]
    rollings = [3, 7, 14]

    for lag in lags:
        df[f"residuo_lag_{lag}"] = (
            df.groupby("estacion")["residuo"].shift(lag)
        )

    for r in rollings:
        serie = df.groupby("estacion")["residuo"].shift(1)

        df[f"residuo_roll_mean_{r}"] = serie.rolling(r).mean()
        df[f"residuo_roll_std_{r}"] = serie.rolling(r).std()

    # ----------------------------
    # SARIMA como feature
    # ----------------------------
    if "sarima_pred" in df.columns:
        df["sarima_pred"] = df["sarima_pred"].astype(float)

    # ----------------------------
    # Limpieza final
    # ----------------------------
    if modo_entrenamiento:
        columnas_requeridas = ["residuo"]
    else:
        columnas_requeridas = []

    df = df.dropna(
        subset=columnas_requeridas + [
            c for c in df.columns if "lag_" in c or "roll_" in c
        ]
    )

    return df.reset_index(drop=True)


# ============================================================
# FEATURES FUTURAS PARA FORECAST (FIX DEFINITIVO)
# ============================================================

def generar_features_futuras(historial, sarima_preds, step):
    """
    Genera UNA fila de features futuras para XGBoost
    (usada en forecast híbrido SARIMA + XGB)

    Lógica correcta:
        - Usa residuo_pred si existe
        - Si no, usa residuo real
        - Si no existe ninguno, usa 0 (primer paso)
    """

    df = historial.copy()
    df = df.sort_values(["estacion", "time"]).reset_index(drop=True)

    ultima = df.iloc[-1:].copy()

    fecha = pd.to_datetime(ultima["time"].values[0])

    # ----------------------------
    # Features temporales
    # ----------------------------
    ultima["dayofyear"] = fecha.dayofyear
    ultima["month"] = fecha.month
    ultima["dayofweek"] = fecha.dayofweek

    ultima["sin_doy"] = np.sin(2 * np.pi * ultima["dayofyear"] / 365)
    ultima["cos_doy"] = np.cos(2 * np.pi * ultima["dayofyear"] / 365)

    # ----------------------------
    # SARIMA futuro
    # ----------------------------
    ultima["sarima_pred"] = sarima_preds[step - 1]

    # ----------------------------
    # Residuo base (CLAVE)
    # ----------------------------
    if "residuo_pred" in df.columns:
        residuo_base = df["residuo_pred"]
    elif "residuo" in df.columns:
        residuo_base = df["residuo"]
    else:
        residuo_base = pd.Series(0.0, index=df.index)

    # ----------------------------
    # Lags de residuo
    # ----------------------------
    for lag in [1, 3, 7, 14]:
        ultima[f"residuo_lag_{lag}"] = residuo_base.shift(lag).iloc[-1]

    # ----------------------------
    # Rollings de residuo
    # ----------------------------
    for r in [3, 7, 14]:
        serie = residuo_base.shift(1)

        ultima[f"residuo_roll_mean_{r}"] = serie.rolling(r).mean().iloc[-1]
        ultima[f"residuo_roll_std_{r}"] = serie.rolling(r).std().iloc[-1]

    return ultima.reset_index(drop=True)
