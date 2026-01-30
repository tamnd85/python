"""
Módulo: run.py
Autor: Tamara
Descripción:
    Script rápido para generar predicciones híbridas sin usar la interfaz CLI.
    
    Este archivo permite:
        - Ejecutar directamente el forecast híbrido SARIMA + XGB
        - Probar el pipeline de predicción sin necsidad de argumentos.
        - obtener un dataFrame con las predicciones futuras
    
Uso típico:
    python run.py
    
Nota:
    Este módulo es ideal para pruebas rápidas, validación de modelos y
    automatizaciones ligeras sin pasar por el CLI completo.
"""

from pipeline.forecast import predecir_hibrido

if __name__ == "__main__":
    # Ciudad objetivo para la predicción
    ciudad = "Santander"
    
    # Númro de días futuros a predecir
    dias = 7

    print(f"Generando predicción híbrida para {ciudad} ({dias} días)...\n")

    # Llamada directa al pipeline híbrido
    df_pred = predecir_hibrido(ciudad, dias)
    
    # Mostrar resultados en consola
    print(df_pred)
