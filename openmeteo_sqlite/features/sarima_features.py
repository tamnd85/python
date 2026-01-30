"""
Módulo: sarima_features.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
Autor: Tamara
Descripción:
    Funciones para preparar la serie temporal univariada utilizada por el
    modelo SARIMA. Este módulo garantiza que la serie resultante sea:
        -Consistente temporalmente
        - Continua (sin huecos)
        - Univariada ( solo temperatura)
        -Libre de duplicados
        - Con frecuencia diaria fija
        - Sin valores faltantes tras interpolación
        
    Los modelos SARIMA requieren series limpias, ordenadas y con frecuencia
    estrictamente regular. Este módulo asegura todas esas condiciones.
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
    #--------------------------------------------------------------------------------
    # 1. Seleccionar únicamente las columnas necesarias
    #    Esto evita arratar columnas irrelevantes al modelo SARIMA
    #--------------------------------------------------------------------------------
    df_sarima = df[["time", "temperature_2m_mean"]].copy()

    #--------------------------------------------------------------------------------
    # 2. Convertir 'time' a datetime y eliminar fechas inválidas
    #    errors="coerce" convierte valores no parseables en NaT.
    #--------------------------------------------------------------------------------
    df_sarima["time"] = pd.to_datetime(df_sarima["time"], errors="coerce")
    df_sarima = df_sarima.dropna(subset=["time"])

    #--------------------------------------------------------------------------------
    # 3. Ordenar por fecha para garantizar consistencia temporal
    #--------------------------------------------------------------------------------
    df_sarima = df_sarima.sort_values("time")

    #--------------------------------------------------------------------------------
    # 4. Agrupar por fecha (día) y hacer media
    #    Esto elimina duplicados y asegura un valor único por día.
    #--------------------------------------------------------------------------------
    df_sarima = df_sarima.groupby("time").mean()

    #--------------------------------------------------------------------------------
    # 5. Asegurar frecuencia diaria fija
    #    asfreq("D") crea una fila por día, insertando NaN donde falten datos
    #--------------------------------------------------------------------------------
    df_sarima = df_sarima.asfreq("D")

    #--------------------------------------------------------------------------------
    # 6. Interpolación + forward fill
    #    - interpolate(): rellena huecos suavemente
    #    - ffill(): rellena valores iniciales se empiezan en NaN
    #--------------------------------------------------------------------------------
    df_sarima["temperature_2m_mean"] = (
        df_sarima["temperature_2m_mean"]
        .interpolate()
        .ffill()
    )

    #--------------------------------------------------------------------------------
    # 7. Validación final: la serie no puede quedar vacia
    #--------------------------------------------------------------------------------
    if df_sarima.empty:
        raise ValueError(
            "La serie SARIMA está vacía después de preparar_serie_sarima. "
            "Revisa si el DataFrame original contiene datos válidos."
        )

    #--------------------------------------------------------------------------------
    # 8. Devolver solo la serie univariada
    #--------------------------------------------------------------------------------
    return df_sarima["temperature_2m_mean"]
