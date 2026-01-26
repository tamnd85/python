"""
main.py
Punto de entrada principal del proyecto OpenMeteo.
Permite ejecutar:
    - ingest: poblar la base de datos
    - train: entrenar SARIMA + XGB
    - forecast: predecir futuro para una ciudad
    - all: ejecutar ingest + train + forecast de una vez
"""

import argparse

from data.ingest import ingest
from pipeline.train import entrenar_modelos
from pipeline.forecast import predecir_hibrido

# Importamos los valores por defecto del config
from config.config import ESTACION_DEFAULT, DIAS_DEFAULT


def main():
    parser = argparse.ArgumentParser(description="Sistema de predicción meteorológica OpenMeteo")

    parser.add_argument(
        "accion",
        choices=["ingest", "train", "forecast", "all"],
        help="Acción a ejecutar"
    )

    parser.add_argument(
        "--ciudad",
        type=str,
        help="Ciudad para forecast (opcional, usa config si no se pasa)"
    )

    parser.add_argument(
        "--dias",
        type=int,
        help="Número de días a predecir (opcional, usa config si no se pasa)"
    )

    args = parser.parse_args()

    # ============================================================
    # ACCIONES
    # ============================================================

    if args.accion == "ingest":
        print("Ejecutando ingest base...")
        ingest()

        print("Ejecutando ingest exógenas...")
        from data.ingest_exog import ingest_exog
        ingest_exog()


    elif args.accion == "train":
        entrenar_modelos()

    elif args.accion == "forecast":

        # Si no se pasan argumentos → usar valores del config
        ciudad = args.ciudad if args.ciudad else ESTACION_DEFAULT
        dias = args.dias if args.dias else DIAS_DEFAULT

        print(f"Usando ciudad: {ciudad}")
        print(f"Usando días: {dias}")

        df_pred = predecir_hibrido(ciudad, dias)
        print(df_pred)

    elif args.accion == "all":
        print("=== INGEST ===")
        ingest()

        print("=== INGEST EXÓGENAS ===")
        from data.ingest_exog import ingest_exog
        ingest_exog()

        print("=== TRAIN ===")
        entrenar_modelos()

        ciudad = args.ciudad if args.ciudad else ESTACION_DEFAULT
        dias = args.dias if args.dias else DIAS_DEFAULT

        print("=== FORECAST ===")
        df_pred = predecir_hibrido(ciudad, dias)
        print(df_pred)



if __name__ == "__main__":
    main()
