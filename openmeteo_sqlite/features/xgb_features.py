"""
Módulo: xgb_features.py
Autor: Tamara (versión estable multiciudad – FIX FORECAST)
Descripción:
    Generación de features para XGBoost basadas en:
        - Tiempo (dayofyear, month, dayofweek, senos y cosenos estacionales)
        - Predicción SARIMA como feature adicional
        - Residuo (real o pedicho)
        - Lags y rollings del residuo por ciudad
        
    Este módulo prepare tanto:
        - Features opara entrenamiento/evaluación
            - Features futuras para forecast híbrido SARIMA + XGB
"""

import pandas as pd
import numpy as np


#---------------------------------------------------------------------------------
# FEATURES PARA ENTRENAMIENTO / EVALUACIÓN
#---------------------------------------------------------------------------------

def preparar_features_xgb(df, modo_entrenamiento=True):
    """
    Genera features XGBoost coherentes para entrenamiento.
    
    IMPORTANTE:
        - Los lags y rollings se calculan POR CIUDAD.
        - La función no altera el DataFrame original.
        - En modo entrenamiento se exige que exista 'residuo'.
        
    Parámetros:
        df: pd.dataFrame
            Debe contenr al menos:
                - 'time'
                - 'estación'
                - 'residuo' ( si modo_entrenamiento=True)
        
        modo_entrenamiento : bool
            Si True -> exige residuo y elimina filas con Nan en lags/sollrollings
            Si False -> permite nan en ersiduo, útil para forecast.
    
    Retorna:
        pd.DataFrame
            dataFrame con todas las features generadas.
    """

    # Copia defensiva para no modificar el DataFrame original
    df = df.copy()
    
    # Ordenar por ciudad u fecha para garantizar coherencia temporal
    df = df.sort_values(["estacion", "time"]).reset_index(drop=True)

    #---------------------------------------------------------------------------------
    # Features temporales
    #---------------------------------------------------------------------------------
    # Día del año
    df["dayofyear"] = df["time"].dt.dayofyear
    
    # Mes (1-12)
    df["month"] = df["time"].dt.month
    
    # Dia de la semana (0-6)
    df["dayofweek"] = df["time"].dt.dayofweek
    
    # Componentes estacionales (ciclo anual)
    df["sin_doy"] = np.sin(2 * np.pi * df["dayofyear"] / 365)
    df["cos_doy"] = np.cos(2 * np.pi * df["dayofyear"] / 365)

    #---------------------------------------------------------------------------------
    # Lags y rollings del RESIDUO (por ciudad)
    #---------------------------------------------------------------------------------
    lags = [1, 3, 7, 14]
    rollings = [3, 7, 14]

    # Lags del residuos
    for lag in lags:
        df[f"residuo_lag_{lag}"] = (
            df.groupby("estacion")["residuo"].shift(lag)
        )

    # Rollings del residuo (media y desviación)
    for r in rollings:
        # se usa shift(1) para evitar fuga de información
        serie = df.groupby("estacion")["residuo"].shift(1)

        df[f"residuo_roll_mean_{r}"] = serie.rolling(r).mean()
        df[f"residuo_roll_std_{r}"] = serie.rolling(r).std()

    #---------------------------------------------------------------------------------
    # SARIMA como feature
    #---------------------------------------------------------------------------------
    if "sarima_pred" in df.columns:
        df["sarima_pred"] = df["sarima_pred"].astype(float)

    #---------------------------------------------------------------------------------
    # Limpieza final
    #---------------------------------------------------------------------------------
    # En entrenamiento se exige residuo y todos los lags/rollings completos
    if modo_entrenamiento:
        columnas_requeridas = ["residuo"]
    else:
        columnas_requeridas = []

    # Eliminar filas con NaN en residuo o en cualquier lag/rolling
    df = df.dropna(
        subset=columnas_requeridas + [
            c for c in df.columns if "lag_" in c or "roll_" in c
        ]
    )

    return df.reset_index(drop=True)

#---------------------------------------------------------------------------------
# FEATURES FUTURAS PARA FORECAST (FIX DEFINITIVO)
#---------------------------------------------------------------------------------

def generar_features_futuras(historial, sarima_preds, step):
    """
    Genera UNA fila de features futuras para XGBoost
    Usado en forecast híbrido SARIMA + XGB.

    Lógica:
        - Usa residuo_pred si existe (forecast previo)
        - Si no, usa residuo real
        - Si no existe ninguno, usa 0 (primer paso)
        
    Parámetros:
        historical: pd.dataFrame
            Debe contener:
                - 'time'
                - 'estacion'
                - 'residuo' o 'residuo_pred'
        
        sarima_preds : list[float]
            predicciones SARIMA ya generadas para el horizonte completo
        
        step: int
            paso futuo actual (1 = primer día futuro).
    
    Retorna:
        pd.DataFrame
            Una sola fila con todas las features necesarias para XGB.
    """

    # Copia defensiva y orden temporal
    df = historial.copy()
    df = df.sort_values(["estacion", "time"]).reset_index(drop=True)

    # Última fila del historial ( base para generar la futura)
    ultima = df.iloc[-1:].copy()

    # Fecha futura asociada al paso actual
    fecha = pd.to_datetime(ultima["time"].values[0])

    #---------------------------------------------------------------------------------
    # Features temporales
    #---------------------------------------------------------------------------------
    ultima["dayofyear"] = fecha.dayofyear
    ultima["month"] = fecha.month
    ultima["dayofweek"] = fecha.dayofweek

    ultima["sin_doy"] = np.sin(2 * np.pi * ultima["dayofyear"] / 365)
    ultima["cos_doy"] = np.cos(2 * np.pi * ultima["dayofyear"] / 365)

    #---------------------------------------------------------------------------------
    # SARIMA futuro
    #---------------------------------------------------------------------------------
    ultima["sarima_pred"] = sarima_preds[step - 1]

    #---------------------------------------------------------------------------------
    # Residuo base (CLAVE)
    #---------------------------------------------------------------------------------
    if "residuo_pred" in df.columns:
        residuo_base = df["residuo_pred"]
    elif "residuo" in df.columns:
        residuo_base = df["residuo"]
    else:
        # Primer paso del forecast -> no hay residuo previo
        residuo_base = pd.Series(0.0, index=df.index)

    #---------------------------------------------------------------------------------
    # Lags de residuo
    #---------------------------------------------------------------------------------
    for lag in [1, 3, 7, 14]:
        ultima[f"residuo_lag_{lag}"] = residuo_base.shift(lag).iloc[-1]

    #---------------------------------------------------------------------------------
    # Rollings de residuo
    #---------------------------------------------------------------------------------
    for r in [3, 7, 14]:
        serie = residuo_base.shift(1)

        ultima[f"residuo_roll_mean_{r}"] = serie.rolling(r).mean().iloc[-1]
        ultima[f"residuo_roll_std_{r}"] = serie.rolling(r).std().iloc[-1]

    return ultima.reset_index(drop=True)
