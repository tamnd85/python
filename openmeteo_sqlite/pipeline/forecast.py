"""
Módulo: forecast.py
Autor: Tamara (versión estable y final)
Descripción:
    Predicción futura usando el modelo híbrido SARIMA + XGBoost.
"""

import pandas as pd
from datetime import timedelta

from db.database import load_from_db
from models.sarima import cargar_sarima, predecir_sarima_futuro
from models.xgboost_model import cargar_xgboost
from features.xgb_features import generar_features_futuras


# ============================================================
# PREDICCIÓN FUTURA HÍBRIDA
# ============================================================

def predecir_hibrido(ciudad, dias=7):

    # --------------------------------------------------------
    # 1. Datos históricos
    # --------------------------------------------------------
    df = load_from_db(estacion=ciudad)

    if df.empty:
        raise ValueError(f"No hay datos para la ciudad '{ciudad}'.")

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["temperature_2m_mean", "time"])
    df = df.sort_values("time").reset_index(drop=True)

    # --------------------------------------------------------
    # 2. Modelos
    # --------------------------------------------------------
    sarima_model = cargar_sarima(ciudad)
    xgb_model, features = cargar_xgboost("xgb_multiciudad")

    # --------------------------------------------------------
    # 3. SARIMA in-sample (para residuo histórico)
    # --------------------------------------------------------
    sarima_insample = sarima_model.get_prediction(dynamic=False).predicted_mean
    sarima_insample = sarima_insample.reindex(df["time"]).reset_index(drop=True)

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

        fecha_pred = historial["time"].max() + timedelta(days=1)
        sarima_pred = sarima_preds[i]

        # Features futuras coherentes
        temp_feat = generar_features_futuras(
            historial=historial,
            sarima_preds=sarima_preds,
            step=i + 1
        )

        # Validación
        faltan = [c for c in features if c not in temp_feat.columns]
        if faltan:
            raise ValueError(f"Faltan features futuras: {faltan}")

        # XGB predice residuo
        X = temp_feat[features]
        residuo_pred = xgb_model.predict(X)[0]

        # Híbrido final
        pred_hibrida = sarima_pred + residuo_pred

        # Actualizar historial (clave)
        nueva_fila = historial.iloc[-1].copy()
        nueva_fila["time"] = fecha_pred
        nueva_fila["temperature_2m_mean"] = pred_hibrida
        nueva_fila["sarima_pred"] = sarima_pred
        nueva_fila["residuo"] = residuo_pred

        historial = pd.concat(
            [historial, pd.DataFrame([nueva_fila])],
            ignore_index=True
        )

        predicciones.append({
            "time": fecha_pred,
            "sarima": sarima_pred,
            "residuo_pred": residuo_pred,
            "pred_hibrida": pred_hibrida
        })

    return pd.DataFrame(predicciones)
