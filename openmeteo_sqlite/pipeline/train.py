"""
Módulo: train.py
Autor: Tamara
Descripción:
    Entrenamiento completo del sistema de predicción meteorológica.
    Pipeline coherente con:
        - SARIMA por ciudad (estacionalidad anual)
        - Features avanzadas XGB
        - Modelo híbrido SARIMA + XGB basado en RESIDUOS

    Incluye:
        - entrenar_modelos()              → pipeline normal
        - entrenar_modelos_mensual()      → pipeline con muestreo mensual
"""

import pandas as pd
from tqdm import tqdm

from db.database import load_from_db
from models.sarima import entrenar_sarima, guardar_sarima
from models.xgboost_model import entrenar_xgboost_train_only, guardar_xgboost
from features.xgb_features import preparar_features_xgb
from features.muestreo import muestreo_mensual


#-------------------------------------------------------------
# ENTRENAMIENTO COMPLETO NORMAL (TU PIPELINE ORIGINAL)
#-------------------------------------------------------------

def entrenar_modelos():
    """
    Pipeline completo de entrenamiento híbrido SARIMA + XGB (modo normal)
    
    Flujo:
        1. cargar datos desde SQLite.
        2. Limpieza básica y ordentemporal.
        3. Entrenar SARIMA por ciudad.
        4. Calcular residuo real.
        5. Generar features XGB multiciudad.
        6. Entrenar XGboost multiciudad.
        7. Guardar modelos entrenados.
    """
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
        
        # Subconjunto por ciudad
        df_city = (
            df[df["estacion"] == ciudad]
            .sort_values("time")
            .reset_index(drop=True)
        )

        # Requisito mínimo: 2 años de datos
        if len(df_city) < 730:
            print(f"\n {ciudad}: datos insuficientes ({len(df_city)} filas), se omite.")
            continue

        print(f"\n➡ Entrenando SARIMA para {ciudad}...")
        sarima = entrenar_sarima(df_city)
        guardar_sarima(sarima, ciudad)
        sarima_models[ciudad] = sarima

        # Predicción in-sample alineada con fechas
        preds = sarima.get_prediction(dynamic=False).predicted_mean
        preds = preds.reindex(df_city["time"]).reset_index(drop=True)

        # Validadción de consistencia
        if len(preds) != len(df_city):
            print(f" {ciudad}: inconsistencia en predicciones SARIMA, se omite.")
            continue

        # Insertar predicciones en el DataFrame global
        df.loc[df["estacion"] == ciudad, "sarima_pred"] = preds.values

    # Eliminar filas sin  predicción SARIMA
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
    # 4. ENTRENAR XGBOOST (MULTICIUDAD)
    # ============================================================
    print("\n Entrenando XGBoost multiciudad...\n")

    with tqdm(total=1, desc="XGBoost", unit="modelo") as pbar:
        xgb_model, features = entrenar_xgboost_train_only(
            df_feat,
            residuos_col="residuo"
        )
        pbar.update(1)

    # Guardar modelo multiciudad
    guardar_xgboost(xgb_model, features, nombre="xgb_multiciudad")

    print("\n Entrenamiento completado correctamente.")
    return sarima_models, xgb_model, features



# ============================================================
# ENTRENAMIENTO COMPLETO MENSUAL (NUEVO)
# ============================================================

def entrenar_modelos_mensual(dias_por_mes=20):
    """
    Pipeline completo de entrenamiento híbrido SARIMA + XGB
    usando muestreo mensual para balancear el dataset.
    
    Flujo:
        1. cargar datos desde SQLite.
        2. limpieza básica.
        3. Aplicar muestreo mensual.
        4. Entrenar SArima por ciudad.
        5. calcular residuo.
        6. generar features XGB.
        7. Entrenar XGB multiciudad mensual.
    """
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

    print(f" Total de filas limpias (antes de muestreo): {len(df)}")

    #-------------------------------------------------------------
    # 1. MUESTREO MENSUAL
    #-------------------------------------------------------------
    print(f"\n Aplicando muestreo mensual ({dias_por_mes} días por mes)...")
    df_bal = muestreo_mensual(df, dias_por_mes=dias_por_mes)
    print(f" Total de filas tras muestreo mensual: {len(df_bal)}")

    #-------------------------------------------------------------
    # 2. ENTRENAR SARIMA POR CIUDAD (MENSUAL)
    #-------------------------------------------------------------
    print("\n Entrenando SARIMA mensual por ciudad...\n")

    df_bal["sarima_pred"] = pd.NA
    sarima_models = {}

    ciudades = df_bal["estacion"].unique()

    for ciudad in tqdm(ciudades, desc="SARIMA mensual", unit="ciudad"):
        df_city = (
            df_bal[df_bal["estacion"] == ciudad]
            .sort_values("time")
            .reset_index(drop=True)
        )

        # Requisito mínimo
        if len(df_city) < 730:
            print(f"\n {ciudad}: datos insuficientes ({len(df_city)} filas), se omite.")
            continue

        print(f"\n➡ Entrenando SARIMA MENSUAL para {ciudad}...")
        sarima = entrenar_sarima(df_city)
        guardar_sarima(sarima, f"{ciudad}_mensual")
        sarima_models[ciudad] = sarima

        # predicción in-sample alineada
        preds = sarima.get_prediction(dynamic=False).predicted_mean
        preds = preds.reindex(df_city["time"]).reset_index(drop=True)

        if len(preds) != len(df_city):
            print(f" {ciudad}: inconsistencia en predicciones SARIMA mensual, se omite.")
            continue

        df_bal.loc[df_bal["estacion"] == ciudad, "sarima_pred"] = preds.values

    # Eliminar filas sin SARIMA mensual
    df_bal = df_bal.dropna(subset=["sarima_pred"]).reset_index(drop=True)

    #-------------------------------------------------------------
    # 3. CALCULAR RESIDUO
    #-------------------------------------------------------------
    print("\n Calculando residuo SARIMA mensual...")
    df_bal["residuo"] = df_bal["temperature_2m_mean"] - df_bal["sarima_pred"]

    #-------------------------------------------------------------
    # 4. GENERAR FEATURES PARA XGBOOST
    #-------------------------------------------------------------
    print("\n Generando features para XGBoost mensual...")
    df_feat = preparar_features_xgb(df_bal, modo_entrenamiento=True)

    if df_feat.empty:
        raise ValueError("df_feat está vacío tras preparar_features_xgb() (mensual).")

    #-------------------------------------------------------------
    # 5. ENTRENAR XGBOOST MULTICIUDAD (MENSUAL)
    #-------------------------------------------------------------
    print("\n Entrenando XGBoost multiciudad MENSUAL...\n")

    with tqdm(total=1, desc="XGBoost mensual", unit="modelo") as pbar:
        xgb_model, features = entrenar_xgboost_train_only(
            df_feat,
            residuos_col="residuo"
        )
        pbar.update(1)

    guardar_xgboost(xgb_model, features, nombre="xgb_multiciudad_mensual")

    print("\n Entrenamiento MENSUAL completado correctamente.")
    return sarima_models, xgb_model, features



#-------------------------------------------------------------
# EJECUCIÓN DIRECTA
#-------------------------------------------------------------

if __name__ == "__main__":
    # entrenar_modelos()              # Pipeline normal
    entrenar_modelos_mensual(20)      # Pipeline mensual
