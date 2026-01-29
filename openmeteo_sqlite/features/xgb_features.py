"""
================================================================================
MÓDULO: xgb_features.py
PROYECTO: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
AUTOR: Tamara
DESCRIPCIÓN:
    Este módulo realiza la Ingeniería de Variables (Feature Engineering). 
    Transforma las series temporales brutas en un conjunto de predictores 
    enriquecidos para el modelo XGBoost, incluyendo señales cíclicas, 
    tendencias meteorológicas y retardos de error (lags).

FUNCIONALIDADES CLAVE:
    1. Codificación Ciclo-Estacional: Uso de funciones seno/coseno para que el 
       modelo entienda que el 31 de diciembre y el 1 de enero son días cercanos.
    2. Memoria de Error (Lags): Captura la persistencia del residuo del SARIMA
       para corregir desviaciones sistemáticas a corto plazo.
    3. Análisis de Tendencia: Calcula diferencias (diff) y aceleraciones para 
       detectar cambios bruscos en la presión o la temperatura.
    4. Adaptación Geográfica: Lógica específica para Santander (viento norte/sur).

ESTRATEGIA DE PREDICCIÓN:
    Soporta generación recursiva (día a día) permitiendo inyectar pronósticos
    meteorológicos externos para mejorar la precisión del horizonte futuro.
================================================================================
"""

import pandas as pd
import numpy as np

def preparar_features_xgb(df, modo_entrenamiento=True):
    """
    Transforma el DataFrame original en una matriz de entrenamiento/predicción.
    """
    df = df.copy()
    # Aseguramos el orden cronológico por estación para que los 'diff' y 'shift' sean correctos
    df = df.sort_values(["estacion", "time"]).reset_index(drop=True)

    # ---------------------------------------------------------------------------
    # 1. FEATURES TEMPORALES (Ciclos Estacionales)
    # ---------------------------------------------------------------------------
    # Convertimos el día del año en coordenadas circulares
    df["dayofyear"] = df["time"].dt.dayofyear
    df["month"] = df["time"].dt.month
    df["dayofweek"] = df["time"].dt.dayofweek
    df["sin_doy"] = np.sin(2 * np.pi * df["dayofyear"] / 365.25)
    df["cos_doy"] = np.cos(2 * np.pi * df["dayofyear"] / 365.25)

    # ---------------------------------------------------------------------------
    # 2. LAGS DEL RESIDUO (Memoria de error del SARIMA)
    # ---------------------------------------------------------------------------
    # El modelo aprende del error que cometió ayer y anteayer
    lags_res = [1, 2] 
    if "residuo" in df.columns:
        for lag in lags_res:
            df[f"residuo_lag_{lag}"] = df.groupby("estacion")["residuo"].shift(lag)
    else:
        # En fase inicial o forecast puro donde no hay residuo real
        for lag in lags_res:
            df[f"residuo_lag_{lag}"] = 0.0

    # ---------------------------------------------------------------------------
    # 3. METEOROLOGÍA AVANZADA (Tendencias e Inercia)
    # ---------------------------------------------------------------------------
    meteo_cols = ["wind_direction_10m_dominant", "relative_humidity_2m", "surface_pressure", "wind_speed_10m"]

    for col in meteo_cols:
        if col in df.columns:
            df[f"feat_{col}"] = df[col].astype(float)
            # Diferencia simple: Detecta si la variable sube o baja respecto a ayer
            df[f"diff_{col}"] = df.groupby("estacion")[col].diff().fillna(0)
        else:
            df[f"feat_{col}"] = 0.0
            df[f"diff_{col}"] = 0.0

    # ---------------------------------------------------------------------------
    # 4. MEJORAS ESPECÍFICAS PARA SANTANDER (Lógica Geográfica)
    # ---------------------------------------------------------------------------
    if "wind_direction_10m_dominant" in df.columns:
        # Transformación circular: El viento de componente Norte (mar) suele ser
        # más fresco en verano y estable en invierno que el componente Sur.
        rad = np.deg2rad(df["wind_direction_10m_dominant"].astype(float))
        df["feat_viento_norte"] = np.cos(rad) # +1 es Norte puro, -1 es Sur puro
        df["feat_viento_este"] = np.sin(rad)

    if "surface_pressure" in df.columns:
        # Tendencia de presión a 3 días: Clave para detectar frentes atlánticos
        df["diff_pressure_3d"] = df.groupby("estacion")["surface_pressure"].diff(3).fillna(0)

    if "temperature_2m_mean" in df.columns:
        # Aceleración térmica: ¿Se está calentando el ambiente más rápido que ayer?
        df["accel_temp"] = df.groupby("estacion")["temperature_2m_mean"].diff().shift(1).fillna(0)

    # ---------------------------------------------------------------------------
    # 5. POST-PROCESAMIENTO Y LIMPIEZA
    # ---------------------------------------------------------------------------
    if "sarima_pred" in df.columns:
        df["sarima_pred"] = df["sarima_pred"].astype(float)

    if modo_entrenamiento:
        # Eliminamos filas iniciales donde los lags son NaN (sin historia previa)
        cols_con_lags = [c for c in df.columns if "lag_" in c]
        df = df.dropna(subset=cols_con_lags)
    else:
        # En modo producción rellenamos para evitar que el XGBoost rechace la fila
        df = df.ffill().bfill().fillna(0)

    return df.reset_index(drop=True)

def generar_features_futuras(historial, sarima_preds, step, meteo_futura=None):
    """
    Crea el conjunto de variables para el 'día siguiente' en un bucle recursivo.
    """
    df = historial.copy()
    ultima_fecha = df["time"].max()
    fecha_futura = ultima_fecha + pd.Timedelta(days=1)
    
    # Construcción de la fila de predicción
    nueva_fila_dict = {
        "time": fecha_futura,
        "estacion": df["estacion"].iloc[0],
        "sarima_pred": float(sarima_preds[step - 1]),
        "temperature_2m_mean": float(sarima_preds[step - 1]) 
    }

    # Integración de datos de pronóstico (ej: Open-Meteo Forecast)
    if meteo_futura is not None:
        for col, val in meteo_futura.items():
            nueva_fila_dict[col] = val
    
    nueva_fila = pd.DataFrame([nueva_fila_dict])
    
    # Unimos para que 'preparar_features_xgb' pueda calcular 'diff' y 'lags'
    df_completo = pd.concat([df, nueva_fila], ignore_index=True)
    df_feat = preparar_features_xgb(df_completo, modo_entrenamiento=False)
    
    # Solo devolvemos la última fila, que es la que el modelo debe procesar
    return df_feat.tail(1)