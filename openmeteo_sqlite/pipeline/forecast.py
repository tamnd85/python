"""
================================================================================
MÓDULO: predict.py
PROYECTO: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
AUTOR: Tamara
DESCRIPCIÓN:
    Orquestador de predicción en producción. Realiza el forecast híbrido 
    recursivo utilizando datos en tiempo real de la base de datos.

LÓGICA DE INFERENCIA:
    1. Carga Dinámica: Recupera los modelos (SARIMA y XGBoost) según el modo 
       (normal o mensual).
    2. Segmentación de Datos: Separa el historial (pasado) de los datos de 
       pronóstico (viento/presión futuros ya descargados).
    3. Ciclo Recursivo: Predice día a día. La salida de hoy se convierte en el 
       'lag' (antecedente) para la predicción de mañana.
    4. Potenciador Geográfico: Aplica correcciones al residuo según la dirección
       del viento (Efecto Foehn para el Sur, Efecto Marítimo para el Norte).

AVISO: Este script asume que la base de datos ya contiene el pronóstico 
meteorológico del viento para los próximos 7 días (Bloque 2 de ingesta).
================================================================================
"""

import pandas as pd
import numpy as np
from datetime import timedelta
from db.database import load_from_db
from models.sarima import cargar_sarima
from models.xgboost_model import cargar_xgboost
from features.xgb_features import preparar_features_xgb

def predecir_hibrido(ciudad, dias_forecast=7, modo="normal"):
    """
    Genera el pronóstico final combinado para una ciudad específica.
    """
    # ---------------------------------------------------------------------------
    # 1. CARGA DE MODELOS Y CONFIGURACIÓN
    # ---------------------------------------------------------------------------
    suffix = "_mensual" if modo == "mensual" else ""
    sarima = cargar_sarima(f"{ciudad}{suffix}")
    xgb_model, features_names = cargar_xgboost(f"xgb_multiciudad{suffix}")

    # ---------------------------------------------------------------------------
    # 2. CARGA Y SEGMENTACIÓN DE DATOS (Time-Splitting Real)
    # ---------------------------------------------------------------------------
    df_all = load_from_db(estacion=ciudad)
    df_all["time"] = pd.to_datetime(df_all["time"])
    df_all = df_all.sort_values("time")
    
    hoy = pd.Timestamp.now().normalize() 
    
    # Datos históricos (con temperatura real medida)
    df_hist = df_all[df_all["time"] < hoy].copy()
    # Datos de pronóstico (viento/presión conocidos, temperatura a predecir)
    df_futuro_meteo = df_all[df_all["time"] >= hoy].copy()

    # ---------------------------------------------------------------------------
    # 3. GENERACIÓN DE BASE ESTADÍSTICA (SARIMA)
    # ---------------------------------------------------------------------------
    sarima_forecast = sarima.get_forecast(steps=dias_forecast).predicted_mean
    fechas_futuras = [hoy + timedelta(days=i+1) for i in range(dias_forecast)]
    
    resultados = []
    df_dinamico = df_hist.copy()

    print(f"--- Iniciando Forecast Híbrido Real: {ciudad} ---")

    # ---------------------------------------------------------------------------
    # 4. BUCLE DE PREDICCIÓN RECURSIVA (Híbrido + Potenciador)
    # ---------------------------------------------------------------------------
    for i in range(dias_forecast):
        fecha_target = fechas_futuras[i]
        pred_base = sarima_forecast.iloc[i]
        
        # Intentar obtener meteorología externa (pronóstico de viento en BD)
        meteo_dia = df_futuro_meteo[df_futuro_meteo["time"] == fecha_target]
        
        if not meteo_dia.empty:
            nueva_fila = meteo_dia.iloc[:1].copy()
            es_meteo_real = True
        else:
            # Fallback en caso de falta de datos externos (persistencia climática)
            print(f"⚠️ Día {i+1} ({fecha_target.date()}): Usando persistencia.")
            nueva_fila = df_dinamico.iloc[-1:].copy()
            es_meteo_real = False

        # Preparar fila para el XGBoost
        nueva_fila["time"] = fecha_target
        nueva_fila["sarima_pred"] = pred_base
        nueva_fila["temperature_2m_mean"] = pred_base # Semilla para el cálculo
        nueva_fila["estacion"] = ciudad

        # Cálculo dinámico de features (Lags y Transformaciones Circulares)
        df_temp_total = pd.concat([df_dinamico, nueva_fila], ignore_index=True)
        temp_feat = preparar_features_xgb(df_temp_total, modo_entrenamiento=False)
        fila_input = temp_feat.tail(1)

        # Inferencia del residuo con XGBoost
        X = fila_input[features_names]
        residuo_pred = xgb_model.predict(X)[0]
        
        # -----------------------------------------------------------------------
        # POTENCIADOR ESPECÍFICO (Lógica de Vientos de Cantabria)
        # -----------------------------------------------------------------------
        v_dir = nueva_fila["wind_direction_10m_dominant"].values[0]
        
        if es_meteo_real:
            # Componente SUR: Suele traer aire seco y cálido (Efecto Foehn)
            if 160 <= v_dir <= 220:
                residuo_final = residuo_pred * 1.6 
            # Componente NORTE: Aire húmedo y fresco del Cantábrico
            elif v_dir <= 40 or v_dir >= 320:
                residuo_final = residuo_pred * 1.4
            else:
                residuo_final = residuo_pred
        else:
            residuo_final = residuo_pred

        # Reconstrucción Final: Tendencia + Ajuste Inteligente
        pred_final = pred_base + residuo_final
        
        print(f"Día {i+1} | Viento: {v_dir:.0f}° | SARIMA: {pred_base:.2f} | FINAL: {pred_final:.2f}")

        resultados.append({
            "fecha": fecha_target,
            "sarima": round(pred_base, 2),
            "viento_dir": round(v_dir, 0),
            "hibrida": round(pred_final, 2)
        })
        
        # ACTUALIZACIÓN RECURSIVA: La predicción se inyecta como dato histórico
        # para que el cálculo del 'lag' del día siguiente sea coherente.
        nueva_fila["temperature_2m_mean"] = pred_final
        df_dinamico = pd.concat([df_dinamico, nueva_fila], ignore_index=True)

    return pd.DataFrame(resultados)