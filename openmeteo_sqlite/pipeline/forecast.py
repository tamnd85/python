"""
Módulo: Forecast.py  (VERSIÓN AJUSTE DE REALIDAD - SANTANDER/SEVILLA)
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
Autor: Tamara
Descripción:
    Motor de predicción híbrido en modo producción. Combina:
        - SARIMA → tendencia + estacionalidad
        - XGBoost → corrección del residuo
        - Ajuste de Realidad → reglas físicas y geográficas específicas
            para evitar predicciones irreales, especialmente en Santander.

Objetivo:
    Generar un pronóstico híbrido robusto, estable y físicamente coherente,
    integrando:
        - Predicción estadística base
        - Corrección ML del residuo
        - Modulación por viento (dirección e intensidad)
        - Ajustes conservadores para evitar sobrecalentamientos artificiales

Características:
    - Compatible con modo normal y modo mensual.
    - Ajuste FOEHN para Santander (viento sur).
    - Corrección por viento oeste.
    - Clip de seguridad para evitar valores extremos.
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
    Genera el pronóstico híbrido para una ciudad, aplicando un corrector de
    realidad específico para Santander que evita que la predicción se dispare
    por encima de los valores físicamente plausibles.

    Parámetros:
        ciudad : str
            Nombre de la estación (ej: "santander", "sevilla").
        dias_forecast : int
            Número de días a predecir.
        modo : str
            "normal"  → modelo completo
            "mensual" → versión reducida entrenada con muestreo mensual

    Retorna:
        pd.DataFrame
            Tabla con columnas:
                - fecha
                - sarima
                - viento_dir
                - hibrida
    """
    suffix = "_mensual" if modo == "mensual" else ""
    try:
        sarima = cargar_sarima(f"{ciudad}{suffix}")
        xgb_model, features_names = cargar_xgboost(f"xgb_multiciudad{suffix}")
    except Exception as e:
        print(f"❌ Error cargando modelos: {e}")
        return pd.DataFrame()

    #----------------------------------------------------------------------
    # 1. CARGA DE DATOS
    #----------------------------------------------------------------------
    df_all = load_from_db(estacion=ciudad)
    if df_all.empty: return pd.DataFrame()

    df_all["time"] = pd.to_datetime(df_all["time"])
    df_all = df_all.sort_values("time")
    
    hoy = pd.Timestamp.now().normalize() 
    df_hist = df_all[df_all["time"] < hoy].copy()
    df_futuro_meteo = df_all[df_all["time"] >= hoy].copy()

    #----------------------------------------------------------------------
    # 2. BASE ESTADÍSTICA
    #----------------------------------------------------------------------
    sarima_forecast = sarima.get_forecast(steps=dias_forecast).predicted_mean
    fechas_futuras = [hoy + timedelta(days=i) for i in range(dias_forecast)]
    resultados = []
    df_dinamico = df_hist.copy()

    print(f"\n--- 🌪️ Generando Pronóstico con Ajuste de Realidad: {ciudad} ({modo.upper()}) ---")

    #----------------------------------------------------------------------
    # 3. BUCLE DE PREDICCIÓN
    #----------------------------------------------------------------------
    for i in range(dias_forecast):
        fecha_target = fechas_futuras[i]
        pred_base = float(sarima_forecast.iloc[i])
        meteo_dia = df_futuro_meteo[df_futuro_meteo["time"] == fecha_target]
        
        nueva_fila = meteo_dia.iloc[:1].copy() if not meteo_dia.empty else df_dinamico.iloc[-1:].copy()
        nueva_fila["time"], nueva_fila["sarima_pred"], nueva_fila["estacion"] = fecha_target, pred_base, ciudad
        nueva_fila["temperature_2m_mean"] = pred_base 

        df_temp_total = pd.concat([df_dinamico, nueva_fila], ignore_index=True).ffill().bfill().infer_objects(copy=False).fillna(0)
        fila_input = preparar_features_xgb(df_temp_total, modo_entrenamiento=False).tail(1)

        residuo_pred = float(xgb_model.predict(fila_input[features_names])[0]) if not fila_input[features_names].empty else 0.0
        
        # -----------------------------------------------------------------------
        # 4. MOTOR DE IMPACTO CON AJUSTE DE REALIDAD
        # -----------------------------------------------------------------------
        v_dir = float(nueva_fila["wind_direction_10m_dominant"].iloc[0])
        v_speed = float(nueva_fila.get("wind_speed_10m", pd.Series([12])).iloc[0])
        
        es_santander = (ciudad.lower() == "santander")
        fuerza_suave = 1 + (v_speed / 45.0) 
        
        if es_santander:
            # m_factor conservador (1.15) para no inflar el residuo base
            m_factor = 1.15 if modo == "normal" else 1.02
            mult_foehn = 1.12 # Bajado de 1.3 para suavizar el pico
            offset_corrector = -1.8 # Empuje hacia abajo para alinear con la realidad
        else:
            m_factor = 1.05
            mult_foehn = 1.0
            offset_corrector = 0.0

        ruido = np.random.uniform(0.98, 1.02)

        # Lógica de dirección
        if 150 <= v_dir <= 245 and es_santander: # FOEHN
            residuo_final = (abs(residuo_pred) * m_factor * fuerza_suave * mult_foehn) + offset_corrector
        elif 246 <= v_dir <= 310 and es_santander: # OESTE
            residuo_final = (-abs(residuo_pred) * m_factor * fuerza_suave) + offset_corrector
        else:
            residuo_final = (residuo_pred * m_factor) + offset_corrector

        # Clip de seguridad (ahora es más difícil que llegue al límite)
        residuo_final = np.clip(residuo_final, -6.0, 8.0)
        #----------------------------------------------------------------------
        # 5. RESULTADO FINAL
        #----------------------------------------------------------------------
        pred_final = pred_base + residuo_final + np.random.uniform(-0.05, 0.05)

        print(f"Día {i+1} | Viento: {v_dir:3.0f}° | SARIMA: {pred_base:5.2f} | RES: {residuo_final:+5.2f} | FINAL: {pred_final:5.2f}")

        resultados.append({
            "fecha": fecha_target, 
            "sarima": round(pred_base, 2), 
            "viento_dir": round(v_dir, 0), 
            "hibrida": round(pred_final, 2)
        })
        
        nueva_fila["temperature_2m_mean"] = pred_final
        df_dinamico = pd.concat([df_dinamico, nueva_fila], ignore_index=True)

    return pd.DataFrame(resultados)