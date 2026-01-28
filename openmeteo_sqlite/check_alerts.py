"""
Script: check_alerts.py
Autor: Tamara
Descripción:
    Script ejecutable para probar el sistema de alertas.
    
    Este script:
        - Recibe un dataFrame con predicciones (híbrido o real)
        - Preparar el dataFrame según las reglas de alertas
        - detecta alertas basdas en um brales definidos
        - Envía las alertas mediante el módulo alert_sender
    
    Se utiliza para validar rápidamente el pipeline de alertas.
"""

import pandas as pd

from alerts.alert_rules import preparar_df_alertas, detectar_alertas
from alerts.alert_sender import enviar_alertas


def check_alerts(df_pred):
    """
    Ejecuta el pipeline completo de alertas sobre un DataFrae de predicciones.
    
    Parámetros:
        df_pred: pd.dataFrame
            Debe contener al menos:
                -'time'
                -'híbrido' o 'temperature_2m_mean
            Representa las predicciones o valores reales sobre los que se evaluarásn las alertas.
            
    Flujo:
        1. Preparar DataFrame según reglas de alertas.
        2. Detectar alertas activsa.
        3. imprimir alertas encontradas
        4. Enviar alertas mediante alert_sender.
    """
    print("\n=== CHECK ALERTS ===")

    # Preparación del dataFrame según reglas internas
    df = preparar_df_alertas(df_pred)
    
    # Detección de alertas según umbrales definidos
    alertas = detectar_alertas(df)

    print("\nAlertas detectadas:")
    if not alertas:
        print(" - Ninguna alerta detectada")
    else:
        for a in alertas:
            print(" -", a)

    # Envíoi de alertas (Telegram, email)
    enviar_alertas(alertas)

    print("\n=== FIN CHECK ALERTS ===")


#---------------------------------------------------------------
# EJECUCIÓN DIRECTA
#---------------------------------------------------------------

if __name__ == "__main__":
    # DataFrame de prueba para avlidar el sistema de alertas
    df_pred = pd.DataFrame({
        "time": pd.to_datetime([
            "2025-01-01",
            "2025-01-02",
            "2025-01-03",
            "2025-01-04"
        ]),
        "hibrido": [10, 9, 6, 1],               # predicción híbrida
        "temperature_2m_mean": [10, 9, 6, 1]    # Valores reales
    })

    check_alerts(df_pred)
