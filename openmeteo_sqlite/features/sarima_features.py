"""
Módulo: sarima_features.py
Autor: Tamara
Descripción:
    Funciones para preparar la serie temporal univariada utilizada por el
    modelo SARIMA. Se encarga de:
        - seleccionar columnas relevantes
        - limpiar fechas inválidas
        - eliminar duplicados
        - asegurar frecuencia diaria
        - interpolar huecos
"""

import pandas as pd


def preparar_serie_sarima(df):
    """
    Prepara la serie temporal para entrenar un modelo SARIMA.

    Este proceso garantiza:
        - Serie univariada limpia (solo temperatura)
        - Frecuencia diaria sin huecos
        - Fechas ordenadas y sin duplicados
        - Interpolación de valores faltantes

    Parámetros
    ----------
    df : pd.DataFrame
        Debe contener al menos:
        - 'time'
        - 'temperature_2m_mean'

    Retorna
    -------
    serie : pd.Series
        Serie temporal diaria, limpia y lista para SARIMA.

    Excepciones
    -----------
    ValueError
        Si la serie queda vacía tras el preprocesado.
    """

    # Seleccionar columnas necesarias
    df_sarima = df[["time", "temperature_2m_mean"]].copy()

    # Asegurar datetime y eliminar fechas inválidas
    df_sarima["time"] = pd.to_datetime(df_sarima["time"], errors="coerce")
    df_sarima = df_sarima.dropna(subset=["time"])

    # Ordenar por fecha
    df_sarima = df_sarima.sort_values("time")

    # Agrupar por fecha (evita duplicados)
    df_sarima = df_sarima.groupby("time").mean()

    # Asegurar frecuencia diaria
    df_sarima = df_sarima.asfreq("D")

    # Interpolación y forward fill
    df_sarima["temperature_2m_mean"] = (
        df_sarima["temperature_2m_mean"]
        .interpolate()
        .ffill()
    )

    # Validación final
    if df_sarima.empty:
        raise ValueError(
            "La serie SARIMA está vacía después de preparar_serie_sarima. "
            "Revisa si el DataFrame original contiene datos válidos."
        )

    # Devolver solo la serie univariada
    return df_sarima["temperature_2m_mean"]
