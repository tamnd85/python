"""
Módulo: ingest.py
Autor: Tamara
Descripción:
    Ingestión automática de datos meteorológicos para todas las ciudades
    definidas en la configuración del proyecto.

    Este módulo:
        - Recorre la lista CIUDADES del config
        - Descarga datos históricos desde Open-Meteo (downloader oficial)
        - Añade el nombre de la estación
        - Guarda los datos en SQLite mediante insertar_en_db()

    Se utiliza para poblar la base de datos completa de forma masiva.
"""

from data.downloader import descargar_datos_openmeteo
from data.cleaning import clean_df
from db.database import insertar_en_db
from config.config import CIUDADES


def ingest():
    """
    Descarga y guarda datos para todas las ciudades definidas en CIUDADES.
    """

    for ciudad in CIUDADES:
        nombre = ciudad["nombre"]
        lat = ciudad["lat"]
        lon = ciudad["lon"]

        print(f"\n Descargando datos de {nombre}...")

        df = descargar_datos_openmeteo(lat, lon)

        if df.empty:
            print(f" ⚠ No se guardaron datos para {nombre} (vacío).")
            continue

        # Limpiar
        df = clean_df(df)

        # Añadir nombre de estación
        df["estacion"] = nombre

        # Guardar en DB
        insertar_en_db(df, nombre)

        print(f" ✔ Datos de {nombre} guardados correctamente.")


if __name__ == "__main__":
    ingest()
