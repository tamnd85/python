"""
Módulo: main.py
Autor: tamara
Descripción:
    Punto de entrada principal del proyecto OpenMeteo.
    
    Permite ejecutar desde el terminal:
        - ingest: poblar la base de datos con datos hitóricos
        - train: entrenar modelos SARIMA + XGB
        - forecast: predecir futuro para una ciudad concreta
        - all: ejecutar ingest + train + forecast en una sola llamada
    
    Este script actúa como interfaz CLI del sistema completo
"""

import argparse

from data.ingest import ingest
from pipeline.train import entrenar_modelos
from pipeline.forecast import predecir_hibrido

# Valores por defcto definidos en config
from config.config import ESTACION_DEFAULT, DIAS_DEFAULT


def main():
    """
    Punto de entrada del CLI
    
    Flujo:
        1. Crear parse de arvumentos.
        2. Leer acción solicitada por el usuario.
        3. Ejecutar la acción correspondiente:
            - ingest
            - train
            -forecast
            -all
    """
    parser = argparse.ArgumentParser(
        description="Sistema de predicción meteorológica OpenMeteo"
    )

    parser.add_argument(
        "accion",
        choices=["ingest", "train", "forecast", "all"],
        help="Acción a ejecutar"
    )

    # Ciudad opcinal (solo para forecast)
    parser.add_argument(
        "--ciudad",
        type=str,
        help="Ciudad para forecast (opcional, usa config si no se pasa)"
    )

    # Número de dás opcional (solo para forecast)
    parser.add_argument(
        "--dias",
        type=int,
        help="Número de días a predecir (opcional, usa config si no se pasa)"
    )

    args = parser.parse_args()

    #-------------------------------------------------------------
    #  ACCIONES DISPONIBLES
    #-------------------------------------------------------------

    if args.accion == "ingest":
        print("Ejecutando ingest base...")
        ingest()

    elif args.accion == "train":
        entrenar_modelos()

    elif args.accion == "forecast":
        # Si no se pasan argumentos → usar valores del config
        ciudad = args.ciudad if args.ciudad else ESTACION_DEFAULT
        dias = args.dias if args.dias else DIAS_DEFAULT

        print(f"Usando ciudad: {ciudad}")
        print(f"Usando días: {dias}")

        # Ejecutar forecast híbrido
        df_pred = predecir_hibrido(ciudad, dias)
        print(df_pred)

    elif args.accion == "all":
        # Pipeline completo
        print("=== INGEST ===")
        ingest()

        print("=== TRAIN ===")
        entrenar_modelos()

        ciudad = args.ciudad if args.ciudad else ESTACION_DEFAULT
        dias = args.dias if args.dias else DIAS_DEFAULT

        print("=== FORECAST ===")
        df_pred = predecir_hibrido(ciudad, dias)
        print(df_pred)

#-------------------------------------------------------------
#  EJECUCCIÖN DIRECTA
#-------------------------------------------------------------

if __name__ == "__main__":
    main()
