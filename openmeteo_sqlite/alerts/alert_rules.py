"""
Módulo: alert_rules.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpeneMeteo_Sqlite)
Autor: Tamara
Descripción:
    Reglas de detección de alertas meteorológicas basadas en predicciones 
    híbridas o datos reales.
    
    Este módulo implementa dos etapas fundamentales:
        1. Preparación del DataFrame: 
            - Normaliza la columna temporal.
            - garantiza la existencia de columnas mínimas necesarias.
            - Crea valores derivados cuando faltan (pred_hibrida, temp_min
        2. Detección de alertas:
            - Descenso brusco de temperatura.
            - Riesgo de heladas.
            
    Las alertas generadas pueden ser enviadas posteriormente mediante
    alert_sender.py (Telegram, email)
    
Flujo de uso:
    df_preparado = preparar_df_alertas(df_pred)
    alertas = detectar_alertas(df_preparado)
"""

import pandas as pd


#-------------------------------------------------------------
# PREPARAR DATAFRAME PARA ALERTAS
#-------------------------------------------------------------

def preparar_df_alertas(df_pred):
    """
    Prepara un DataFrame de predicciones para garantiza que contiene las columnas 
    mínimas necesarias paa la detección de alertas.
    
    Operaciones realizadas
        - convertir 'time' a datetime y ordenar cronológicamente.
        - Crear 'pred_hibrida' si no existe (Fallback: columna 'hibrido').
        - Crear 'temperature_2m_min' si no existe (estimación: media -3ºC).
        
    Parámetros:
        df_pred: pd.DataFrame
            DataFrame con predicciones híbridas o reales.
            
    Retorna:
        pd.DataFrame
            DataFrame preparado para la detección de alertas.
    """

    # Se trabaja sobre una copia para no modificar el DataFrame original
    df = df_pred.copy()

    # ------------------------------------------------------------
    # Normalización y ordenación temporal
    # ------------------------------------------------------------
    # Convierte la columna 'time' a formato datetime.
    # errors="coerce" convierte valores inválidos en NaT.
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Ordena el DataFrame por fecha y reinicia el índice.
    # Esto es crítico para detectar tendencias y comparaciones temporales.
    df = df.sort_values("time").reset_index(drop=True)

    # ------------------------------------------------------------
    # Crear columna pred_hibrida si no existe
    # ------------------------------------------------------------
    # Si el modelo híbrido ya existe, se usa directamente.
    # Si no, se intenta usar la columna 'hibrido'.
    # Si ninguna existe, se lanza un error explícito.
    if "pred_hibrida" not in df.columns:
        if "hibrido" in df.columns:
            df["pred_hibrida"] = df["hibrido"]
        else:
            raise ValueError("df_pred no contiene 'pred_hibrida' ni 'hibrido'."
                            "No es posible generar sin una predicción base.")

    # ------------------------------------------------------------
    # Crear columna temperature_2m_min si no existe
    # ------------------------------------------------------------
    # Si no existe la mínima, se estima restando 3°C a la media.
    # Si tampoco existe la media, se lanza un error explícito.
    if "temperature_2m_min" not in df.columns:
        if "temperature_2m_mean" in df.columns:
            df["temperature_2m_min"] = df["temperature_2m_mean"] - 3
        else:
            raise ValueError("df_pred no contiene 'temperature_2m_min' ni 'temperature_2m_mean'.")

    # Devuelve el DataFrame preparado para la detección de alertas
    return df


# ============================================================
# DETECTAR ALERTAS
# ============================================================

def detectar_alertas(df_pred):
    """
    Analiza un DataFrame de predicciones meteorológicas y genera alertas meteorológicas basadasen
    reglas simples pero robustas.
    
    Reglas implementadas:
        1) Descenso brusco de temperatura: 
            - Si la temperatura cae ≥ 2°C respecto al día anterior.
        2) Riesgo de heladas: 
            - Si la temperatura mínima < 3°C.
    
    Parámetros:
        df_pred: pd.DataFrame
            DataFrame preparado por preparar_df_alertas().
            
    Retorna:
        list[str]
            Lista de mensajes de alertas generados.
    """

    # Lista donde se acumularán los mensajes de alerta generados
    alertas = []

    # ------------------------------------------------------------
    # 1) Descenso brusco de temperatura
    # ------------------------------------------------------------
    # Se recorre desde el segundo registro para comparar con el anterior.
    for i in range(1, len(df_pred)):
        t_hoy = df_pred.loc[i, "pred_hibrida"]   # Temperatura del día actual
        t_ayer = df_pred.loc[i - 1, "pred_hibrida"]  # Temperatura del día previo

        # Si la temperatura cae 2°C o más, se genera una alerta
        if t_hoy <= t_ayer - 2:
            fecha = df_pred.loc[i, "time"].date()
            alertas.append(
                f" Descenso brusco de temperatura el {fecha}: {t_hoy:.1f}°C"
            )

    # ------------------------------------------------------------
    # 2) Riesgo de heladas
    # ------------------------------------------------------------
    # Se filtran los días con temperatura mínima inferior a 3°C
    heladas = df_pred[df_pred["temperature_2m_min"] < 3]

    # Por cada día con riesgo de helada, se genera una alerta
    for _, row in heladas.iterrows():
        fecha = row["time"].date()
        alertas.append(
            f" Riesgo de heladas el {fecha}: {row['pred_hibrida']:.1f}°C"
        )

    # Devuelve la lista de alertas generadas
    return alertas
