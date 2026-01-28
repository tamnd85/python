"""
Módulo: forecast.py
Autor: Tamara (versión extendida con soporte mensual)
Descripción:
    Predicción futura usando el modelo híbrido SARIMA + XGBoost.
    Ahora permite elegir entre:
        - modelos normales
        - modelos entrenados con muestreo mensual
        
    Este módulo implementa un frecast híbrido autoregresivo, garantizando:
        - Coherencia entre SARIMA in-sample y SARIMA futuro
        - Generación correcta de features futuras para XGB
        - Compatibilidad con modelos entrenados con muestreo mensual
"""

import pandas as pd
from datetime import timedelta

from db.database import load_from_db
from models.sarima import cargar_sarima, predecir_sarima_futuro
from models.xgboost_model import cargar_xgboost
from features.xgb_features import generar_features_futuras


#-------------------------------------------------------------
# PREDICCIÓN FUTURA HÍBRIDA
#-------------------------------------------------------------

def predecir_hibrido(ciudad, dias=7, modo="normal"):
    """
    Predicción futura híbrida SARIMA +XGB.
    
    Parámetros:
        ciudad: str
            Nombre de la ciudad.
        dias: int
            Número de días futuros a predecir.
        modo: str
            "normal" -> usa sarima_{ciudad}.pkl + xgb_multiciudad.pkl
            "mensual" -> usa sarima_{ciudad}_mensual.pkl + xgb_multiciudad_mensual.pkl
            
    Flujo detallado:
        1. cargar datos históricos desde SQLite.
        2. Cargar modelos SARIMA y XGB según el modo.
        3. Calcular residuo histórico (real-sarima_in_sample).
        4. Obtener predicciones SARIMA futuras.
        5. Forecast híbrido autoregresivo:
            - Generar features futuras coherentes
            - Predecir residuo_pred con XGB
            - Híbrido = sarima_pred + residuo_pred
            - Actualizar historial (autoregresivo)
    """

    # --------------------------------------------------------
    # 1. Datos históricos
    # --------------------------------------------------------
    df = load_from_db(estacion=ciudad)

    if df.empty:
        raise ValueError(f"No hay datos para la ciudad '{ciudad}'.")

    # Asegurar formato correcto de la fecha
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["temperature_2m_mean", "time"])
    df = df.sort_values("time").reset_index(drop=True)

    # --------------------------------------------------------
    # 2. Modelos (NORMAL o MENSUAL)
    # --------------------------------------------------------
    if modo == "normal":
        sarima_model = cargar_sarima(ciudad)
        xgb_model, features = cargar_xgboost("xgb_multiciudad")

    elif modo == "mensual":
        sarima_model = cargar_sarima(f"{ciudad}_mensual")
        xgb_model, features = cargar_xgboost("xgb_multiciudad_mensual")

    else:
        raise ValueError("modo debe ser 'normal' o 'mensual'.")

    # --------------------------------------------------------
    # 3. SARIMA in-sample (para residuo histórico)
    # --------------------------------------------------------
    # Predicción SARIMA alineada con el histórico
    sarima_insample = sarima_model.get_prediction(dynamic=False).predicted_mean
    sarima_insample = sarima_insample.reindex(df["time"]).reset_index(drop=True)

    # Construcción del historial con residuo real
    historial = df.copy()
    historial["sarima_pred"] = sarima_insample.values
    historial["residuo"] = (
        historial["temperature_2m_mean"] - historial["sarima_pred"]
    )

    # --------------------------------------------------------
    # 4. SARIMA futuro
    # --------------------------------------------------------
    df_sarima = predecir_sarima_futuro(df, sarima_model, dias)
    sarima_preds = df_sarima["sarima_pred"].values

    # --------------------------------------------------------
    # 5. Forecast híbrido autoregresivo
    # --------------------------------------------------------
    predicciones = []

    for i in range(dias):

        # Fecha futura = último día + 1
        fecha_pred = historial["time"].max() + timedelta(days=1)
        sarima_pred = sarima_preds[i]

        #-----------------------------------------------------
        # Features futuras coherentes
        #-----------------------------------------------------
        temp_feat = generar_features_futuras(
            historial=historial,
            sarima_preds=sarima_preds,
            step=i + 1
        )

        # CORRECCIÓN IMPORTANTE PARA EL MODO MENSUAL
        # Algunos modelos mensuales incluyen 'year' como feature
        if "year" in features and "year" not in temp_feat.columns:
            temp_feat["year"] = temp_feat["time"].dt.year

        # Validación estricta de features
        faltan = [c for c in features if c not in temp_feat.columns]
        if faltan:
            raise ValueError(f"Faltan features futuras: {faltan}")

        #-----------------------------------------------------
        # XGB predice residuo futuro
        #-----------------------------------------------------
        X = temp_feat[features]
        residuo_pred = xgb_model.predict(X)[0]

        #-----------------------------------------------------
        # Reconstrucción híbrida
        #-----------------------------------------------------
        pred_hibrida = sarima_pred + residuo_pred

        #-----------------------------------------------------
        # Actualizar historial ( autoregresivo)
        #-----------------------------------------------------
        nueva_fila = historial.iloc[-1].copy()
        nueva_fila["time"] = fecha_pred
        nueva_fila["temperature_2m_mean"] = pred_hibrida
        nueva_fila["sarima_pred"] = sarima_pred
        nueva_fila["residuo"] = residuo_pred

        historial = pd.concat(
            [historial, pd.DataFrame([nueva_fila])],
            ignore_index=True
        )

        # Guardar predicción
        predicciones.append({
            "time": fecha_pred,
            "sarima": sarima_pred,
            "residuo_pred": residuo_pred,
            "pred_hibrida": pred_hibrida
        })

    return pd.DataFrame(predicciones)
