"""
================================================================================
MÓDULO: train.py (Orquestador de Entrenamiento)
PROYECTO: Sistema de Predicción Meteorológica Híbrida
AUTOR: Tamara
DESCRIPCIÓN:
    Pipeline centralizado que coordina el flujo de datos entre SARIMA y XGBoost.
    Implementa la arquitectura de "Modelado de Residuos" de forma automatizada.

FLUJO DE TRABAJO:
    1. EXTRACCIÓN: Carga datos desde SQLite (históricos completos).
    2. SARIMA (Capa Local): Itera por cada ciudad entrenando modelos específicos
       para capturar la inercia térmica de cada ubicación.
    3. RESIDUOS: Calcula la "señal de error" (Real - SARIMA).
    4. XGBOOST (Capa Global): Entrena un único modelo multiciudad que aprende
       a corregir el error basándose en meteorología y lags.

MODOS DE EJECUCIÓN:
    - Normal: Entrenamiento exhaustivo con toda la serie.
    - Mensual: Entrenamiento optimizado mediante muestreo para evitar 
      estacionalidad sesgada y reducir carga computacional.
================================================================================
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
import os

from db.database import load_from_db
from models.sarima import entrenar_sarima, guardar_sarima
from models.xgboost_model import entrenar_xgboost_train_only, guardar_xgboost
from features.xgb_features import preparar_features_xgb
from features.muestreo import muestreo_mensual

# ------------------------------------------------------------------
# MOTOR DE ENTRENAMIENTO
# ------------------------------------------------------------------

def ejecutar_pipeline_entrenamiento(modo="normal", dias_por_mes=25):
    """
    Ejecuta el pipeline híbrido secuencial.
    """
    print(f"\n>>> INICIANDO PIPELINE DE ENTRENAMIENTO: MODO {modo.upper()} <<<")
    
    # 1. CARGA DE DATOS
    df = load_from_db(estacion=None)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    
    # Limpieza: Eliminamos registros sin target o sin fecha
    df = df.dropna(subset=["temperature_2m_mean", "time"]).sort_values(["estacion", "time"]).reset_index(drop=True)

    if df.empty:
        raise ValueError("Error: La base de datos está vacía.")

    # ------------------------------------------------------------------
    # 2. CAPA SARIMA: MODELADO BASE (POR CIUDAD)
    # ------------------------------------------------------------------
    
    print(f"--- Entrenando SARIMA (Serie completa) ---")
    df["sarima_pred"] = np.nan 
    ciudades = df["estacion"].unique()
    
    suffix = "_mensual" if modo == "mensual" else ""
    
    for ciudad in tqdm(ciudades, desc="Ciudades SARIMA"):
        idx_ciudad = df[df["estacion"] == ciudad].index
        df_city = df.loc[idx_ciudad].copy()
        
        # Validación de ventana temporal (Mínimo 2 años para estacionalidad)
        if len(df_city) < 730: 
            print(f"\nSaltando {ciudad}: Datos insuficientes (mínimo 730 días).")
            continue
        
        # Entrenamiento y guardado del modelo estadístico
        model_name = f"{ciudad}{suffix}"
        sarima_mod = entrenar_sarima(df_city)
        guardar_sarima(sarima_mod, model_name)
        
        # Mapeo de predicciones in-sample (fitted values)
        # Usamos un diccionario de fechas para asegurar alineación exacta
        preds = sarima_mod.get_prediction(dynamic=False).predicted_mean
        preds_dict = preds.to_dict()
        df.loc[idx_ciudad, "sarima_pred"] = df.loc[idx_ciudad, "time"].map(preds_dict)

    # ------------------------------------------------------------------
    # 3. CÁLCULO DE RESIDUOS (EL TARGET PARA EL SIGUIENTE NIVEL)
    # ------------------------------------------------------------------
    df = df.dropna(subset=["sarima_pred"]).reset_index(drop=True)
    df["residuo"] = df["temperature_2m_mean"].astype(float) - df["sarima_pred"].astype(float)

    if df.empty:
        raise ValueError("Error crítico: No hay residuos que procesar tras el entrenamiento SARIMA.")

    # ------------------------------------------------------------------
    # 4. CAPA XGBOOST: MODELADO DE CORRECCIÓN (GLOBAL)
    # ------------------------------------------------------------------
    
    # Muestreo estratificado si se elige modo mensual
    if modo == "mensual":
        print(f"--- Aplicando muestreo mensual ({dias_por_mes} días/mes) ---")
        df_to_xgb = muestreo_mensual(df, dias_por_mes=dias_por_mes)
    else:
        df_to_xgb = df

    # Generación de variables (Lags, rolling averages, meteorología circular)
    print(f"--- Generando features para XGBoost ({len(df_to_xgb)} filas) ---")
    df_feat = preparar_features_xgb(df_to_xgb, modo_entrenamiento=True)
    
    # Estrategia de recuperación si el dataset queda vacío por lags
    if df_feat.empty:
        print("⚠ Alerta: df_feat vacío. Intentando modo de recuperación...")
        df_feat = preparar_features_xgb(df_to_xgb, modo_entrenamiento=False)

    if df_feat.empty:
        raise ValueError("No se pudieron generar características para el modelo XGBoost.")

    # Entrenamiento del corrector global multiciudad
    xgb_name = "xgb_multiciudad_mensual" if modo == "mensual" else "xgb_multiciudad"
    print(f"--- Entrenando XGBoost: {xgb_name} ---")
    
    xgb_model, features = entrenar_xgboost_train_only(df_feat, residuos_col="residuo")
    guardar_xgboost(xgb_model, features, nombre=xgb_name)
    
    print(f"✔ Pipeline {modo} completado con éxito.\n")

# ------------------------------------------------------------------
# PUNTOS DE ACCESO
# ------------------------------------------------------------------

def entrenar_modelos():
    ejecutar_pipeline_entrenamiento(modo="normal")

def entrenar_modelos_mensual(dias_por_mes=25):
    ejecutar_pipeline_entrenamiento(modo="mensual", dias_por_mes=dias_por_mes)

if __name__ == "__main__":
    # Ejecución por defecto con balanceo mensual
    entrenar_modelos_mensual(25)