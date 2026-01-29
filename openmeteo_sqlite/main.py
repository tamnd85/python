"""
================================================================================
MÓDULO: main.py
PROYECTO: Sistema de Predicción Meteorológica Híbrida (CLI)
AUTOR: Tamara
DESCRIPCIÓN:
    Interfaz de línea de comandos que orquesta las tres fases del proyecto:
    Ingesta, Entrenamiento y Predicción.

FLUJO DE TRABAJO DINÁMICO:
    - Ingest: Sincroniza la base de datos local con la API de OpenMeteo.
    - Train: Ejecuta el pipeline dual (SARIMA por ciudad + XGBoost global).
    - Forecast: Genera el pronóstico híbrido aplicando la corrección por viento.
    - All: Ejecuta el ciclo completo de vida de los datos.

USO DESDE TERMINAL:
    python main.py forecast --ciudad "Santander" --dias 7
================================================================================
"""

import argparse
import sys

from data.ingest import ingest
from pipeline.forecast import predecir_hibrido
from pipeline.train import entrenar_modelos, entrenar_modelos_mensual

# Valores por defecto centralizados para facilitar el mantenimiento
from config.config import ESTACION_DEFAULT, DIAS_DEFAULT

def main():
    parser = argparse.ArgumentParser(
        description="Sistema de predicción meteorológica híbrido OpenMeteo + SARIMA + XGBoost"
    )

    # Argumento principal: define qué motor del sistema encender
    parser.add_argument(
        "accion",
        choices=["ingest", "train", "forecast", "all"],
        help="Acción a ejecutar: ingest (datos), train (modelos), forecast (predicción) o all (ciclo completo)"
    )

    # Argumentos opcionales para personalizar la ejecución
    parser.add_argument(
        "--ciudad",
        type=str,
        help="Nombre de la estación/ciudad para el forecast (por defecto configurada en config.py)"
    )

    parser.add_argument(
        "--dias",
        type=int,
        help="Número de días a predecir (máximo recomendado: 7-14 días)"
    )

    args = parser.parse_args()

    # Resolución de parámetros: Prioridad -> Argumento de consola > Configuración por defecto
    ciudad = args.ciudad if args.ciudad else ESTACION_DEFAULT
    dias = args.dias if args.dias else DIAS_DEFAULT

    #---------------------------------------------------------------------------
    # ORQUESTACIÓN DE ACCIONES
    #---------------------------------------------------------------------------

    # 1. INGESTA: Sincronización de BD
    if args.accion == "ingest":
        print(">>> 🔄 Ejecutando sincronización de datos (Histórico + Forecast de Viento)...")
        ingest()

    # 2. ENTRENAMIENTO: Re-ajuste de pesos y estacionalidad
    elif args.accion == "train":
        print(">>> 🧠 Iniciando entrenamiento DUAL...")
        print("1. Entrenando modelos NORMALES (Serie completa)...")
        entrenar_modelos()
        
        print("\n2. Entrenando modelos MENSUALES (Muestreo de tendencia)...")
        entrenar_modelos_mensual(dias_por_mes=25)
        print("\n[OK] Modelos actualizados y listos para inferencia.")

    # 3. PREDICCIÓN: El corazón del sistema híbrido
    elif args.accion == "forecast":
        # Estrategia de frescura: si pedimos predicción a corto plazo,
        # obligamos a descargar el viento más reciente para mayor precisión.
        if dias <= 7:
            print(f">>> 📡 Refrescando pronóstico de viento real para {ciudad}...")
            ingest() 

        print(f"\n>>> 🔮 Generando predicción para: {ciudad} ({dias} días)")

        # Inferencia Modo Normal: Ajuste fino y corrección de "zigzag"
        print("\n--- PREDICCIÓN NORMAL (7 DÍAS REALISTAS) ---")
        df_pred = predecir_hibrido(ciudad, dias, modo="normal")
        print(df_pred)
        
        # Inferencia Modo Mensual: Visión de largo plazo / tendencia
        try:
            print("\n--- PREDICCIÓN MENSUAL (TENDENCIA) ---")
            df_pred_mensual = predecir_hibrido(ciudad, dias, modo="mensual")
            print(df_pred_mensual)
        except Exception as e:
            print(f"\n[!] Modelo mensual no disponible o error en datos: {e}")

    # 4. ALL: Automatización total
    elif args.accion == "all":
        print("=== 🚀 INICIANDO PIPELINE COMPLETO (End-to-End) ===")
        
        print("\n[PASO 1] INGEST & SYNC")
        ingest()

        print("\n[PASO 2] TRAIN (DUAL)")
        entrenar_modelos()
        entrenar_modelos_mensual(dias_por_mes=25)

        print("\n[PASO 3] FORECAST FINAL")
        df_pred = predecir_hibrido(ciudad, dias, modo="normal")
        print(df_pred)

#---------------------------------------------------------------------------
# ENTRY POINT
#---------------------------------------------------------------------------
if __name__ == "__main__":
    main()