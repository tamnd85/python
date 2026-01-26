"""
Módulo: train.py
Autor: Tamara (versión corregida)
Descripción:
    Entrenamiento completo del sistema de predicción meteorológica.
    Pipeline coherente con:
        - SARIMA por ciudad (estacionalidad anual)
        - Features avanzadas XGB
        - Modelo híbrido SARIMA + XGB basado en RESIDUOS
"""

import pandas as pd
from tqdm import tqdm

from db.database import load_from_db
from models.sarima import entrenar_sarima, guardar_sarima
from models.xgboost_model import entrenar_xgboost_train_only, guardar_xgboost
from features.xgb_features import preparar_features_xgb


# ============================================================
# ENTRENAMIENTO COMPLETO
# ============================================================

def entrenar_modelos():
    print(" Cargando datos desde SQLite...")
    df = load_from_db(estacion=None)

    # ----------------------------
    # Limpieza básica
    # ----------------------------
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["temperature_2m_mean", "time"])
    df = df.sort_values("time").reset_index(drop=True)

    if df.empty:
        raise ValueError("No hay datos válidos en la base.")

    print(f" Total de filas limpias: {len(df)}")

    # ============================================================
    # 1. ENTRENAR SARIMA POR CIUDAD
    # ============================================================
    print("\n Entrenando SARIMA por ciudad...\n")

    df["sarima_pred"] = pd.NA
    sarima_models = {}

    ciudades = df["estacion"].unique()

    for ciudad in tqdm(ciudades, desc="SARIMA", unit="ciudad"):
        df_city = (
            df[df["estacion"] == ciudad]
            .sort_values("time")
            .reset_index(drop=True)
        )

        # Requisito mínimo: 2 años
        if len(df_city) < 730:
            print(f"\n {ciudad}: datos insuficientes ({len(df_city)} filas), se omite.")
            continue

        print(f"\n➡ Entrenando SARIMA para {ciudad}...")
        sarima = entrenar_sarima(df_city)
        guardar_sarima(sarima, ciudad)
        sarima_models[ciudad] = sarima

        # Predicción in-sample alineada
        preds = sarima.get_prediction(dynamic=False).predicted_mean
        preds = preds.reindex(df_city["time"]).reset_index(drop=True)

        if len(preds) != len(df_city):
            print(f" {ciudad}: inconsistencia en predicciones SARIMA, se omite.")
            continue

        df.loc[df["estacion"] == ciudad, "sarima_pred"] = preds.values

    # Eliminar filas sin SARIMA
    df = df.dropna(subset=["sarima_pred"]).reset_index(drop=True)

    # ============================================================
    # 2. CALCULAR RESIDUO
    # ============================================================
    print("\n Calculando residuo SARIMA...")
    df["residuo"] = df["temperature_2m_mean"] - df["sarima_pred"]

    # ============================================================
    # 3. GENERAR FEATURES PARA XGBOOST
    # ============================================================
    print("\n Generando features para XGBoost...")
    df_feat = preparar_features_xgb(df, modo_entrenamiento=True)

    if df_feat.empty:
        raise ValueError("df_feat está vacío tras preparar_features_xgb().")

    # ============================================================
    # 4. ENTRENAR XGBOOST (MULTICIUDAD, TARGET = RESIDUO)
    # ============================================================
    print("\n Entrenando XGBoost multiciudad...\n")

    with tqdm(total=1, desc="XGBoost", unit="modelo") as pbar:
        xgb_model, features = entrenar_xgboost_train_only(
            df_feat,
            residuos_col="residuo"
        )
        pbar.update(1)

    guardar_xgboost(xgb_model, features, nombre="xgb_multiciudad")

    print("\n Entrenamiento completado correctamente.")
    return sarima_models, xgb_model, features


# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================

if __name__ == "__main__":
    entrenar_modelos()
