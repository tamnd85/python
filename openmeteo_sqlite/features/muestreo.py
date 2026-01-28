"""
Módulo: muestreo.py
Autor: Tamara
Descripción:
    Funciones de muestreo temporal para equilibrar datasets meteorológicos.
    Este módulo implementa un muestreo mensual real, seleccionando un número
    fijo de días por cada combinación año-mes, manteniendo la estructura 
    temporal y evitando sesgos por meses con más datos.
    
    Útil para:
        - Entrenamiento de modelos mensuales.
        - Balanceo de datasets históricos.
        - Reducción controlada del tamaño del dataset
"""

import pandas as pd

def muestreo_mensual(df, dias_por_mes=20, random_state=42):
    """
    Realiza un muestreo equilibrado por mes y año.
    
    Parámetros:
        df (pd.dataFrame): dataFrame con una columna 'time'.
        dias_por_mes (int): Número máximo de días a muestrear por mes
        random_state (int): semilla para garantizar reproducibilidad.
    
    Flujo detallado:
        1. Copiar el DataFrame para no modificar el origunal.
        2. Convertir la columna 'time' a datetime.
        3. Eliminar filas sin fecha válida.
        5. Agrupar por (año, mes).
        6. En cada grupo, muestrea hasta 'dias_por_mes' filas.
        7. Unir los resultados en jun DataFrame final balanceado.
    """
    
    # Ciopia defensiva para evitar modificar el DataFrame original.
    df = df.copy()
    
    # Convertir la columna 'time' a datetime; valores inválidos -> NaT
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    
    # Eliminar filas sin fecha válida
    df = df.dropna(subset=["time"])

    # Extraer año y mes para agrupar correctamente
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month

    # Muestreo mensual REAL:
    #   - Agrupa por año y mes
    #   - En cada grupo, toma un máximo de 'dias_por_mes' muestras
    #   -Si el mes tiene menos días disponibles, toma todos
    df_bal = (
        df.groupby(["year", "month"], group_keys=False)
            .apply(lambda x: x.sample(n=min(dias_por_mes, len(x)), random_state=random_state))
            .reset_index(drop=True)
    )

    return df_bal
