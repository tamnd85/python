"""
run.py
Script rápido para generar predicciones híbridas sin usar CLI.
"""

from pipeline.forecast import predecir_hibrido

if __name__ == "__main__":
    ciudad = "Burgos"
    dias = 7

    print(f"Generando predicción híbrida para {ciudad} ({dias} días)...\n")

    df_pred = predecir_hibrido(ciudad, dias)
    print(df_pred)
