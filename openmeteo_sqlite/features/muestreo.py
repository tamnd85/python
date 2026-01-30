"""
Módulo: muestreo.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
Autor: Tamara
Descripción:
    Implementa estrategias de muestreo temporal para el balanceo y la reducción
    eficiente de datasets meteorológicos de gran escala (series desde el 2000).
    
Lógica de negocio:
    A diferencia de un muestreo aleatorio tradicional, este módulo utiliza un
    enfoque de "Ventana Final Mensual". Selecciona un bloque consecutivo de días
    al final de cada mes, lo que permite:
    1. Mantener la estructura de serie temporal (localmente).
    2. Evitar el sesgo de estacionalidad (todos los meses pesan lo mismo).
    3. Reducir el coste computacional del entrenamiento del XGBoost.

Casos de uso
    - Entrenamiento de modelos en "Modo Mensual".
    - Validación cruzada manteniendo la coherencia temporal.
    - Creación de datasets de prueba equilibrados.
"""

import pandas as pd

#----------------------------------------------------------------------------------
# Función principal: muestreo mensual estratificado
#----------------------------------------------------------------------------------

def muestreo_mensual(df, dias_por_mes=20):
    """
    Realiza un muestreo estratificado por mes, seleccionando bloques finales
    consecutivos para preservar la inercia climática local.

    Parámetros:
        df: pd.DataFrame
            Dataset original con columna 'time'.
        dias_por_mes: int
            Cantidad de registros consecutivos a extraer por mes.
            Por defecto 20 días (aprox. 66% del mes).

    Retorna:
        pd.DataFrame: 
            Dataset equilibrado y ordenado cronológicamente.
    """
    # ---------------------------------------------------------------------------
    # 1. PREPARACIÓN Y ORDENAMIENTO
    # ---------------------------------------------------------------------------
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    
    # El orden cronológico es crítico antes de aplicar iloc
    df = df.sort_values("time")
    
    # ---------------------------------------------------------------------------
    # 2. APLICACIÓN DE MUESTREO POR VENTANA (STRATIFIED TAIL)
    # ---------------------------------------------------------------------------
    # Agrupamos por año y mes para tratar cada bloque mensual como una unidad.
    # Usamos .apply con iloc negativo para capturar el cierre de cada mes.
    df_bal = df.groupby(
        [df["time"].dt.year, df["time"].dt.month], 
        group_keys=False
    ).apply(
        lambda x: x.iloc[-dias_por_mes:] if len(x) >= dias_por_mes else x
    )
    
    # ---------------------------------------------------------------------------
    # 3. LIMPIEZA DE ÍNDICES
    # ---------------------------------------------------------------------------
    # Devolvemos un DataFrame limpio, listo para ser inyectado en el modelo.
    print(f"📉 Muestreo completado: Dataset reducido a {len(df_bal)} registros.")
    return df_bal.reset_index(drop=True)