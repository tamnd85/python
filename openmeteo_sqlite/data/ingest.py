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
    
    Flujo detallado:
        1. iterar sobre cada ciudad definida en la configuración.
        2. Extraer nombre, latitud y longitud.
        3. Descargar datos hitóricos usando el downloades oficial.
        4. Validar que la descarga no esté vacía.
        5. Limpiar el DataFrame con clean_df().
        6. Añadir la columna 'estación' para identificar la ciudad.
        7. Insertar los datos en la base de datis mediante insertar_en_db().
    """

    # Recorrer todas las ciudades configuradas en config.CIUDADES
    for ciudad in CIUDADES:
        # Extraer parámetros de la ciudad
        nombre = ciudad["nombre"]
        lat = ciudad["lat"]
        lon = ciudad["lon"]

        print(f"\n Descargando datos de {nombre}...")

        # Descargar datos históricos desde Open-Meteo
        df = descargar_datos_openmeteo(lat, lon)

        # Validación: si la Api devolvió un DataFrame vacío, no se inserta nada
        if df.empty:
            print(f" ⚠ No se guardaron datos para {nombre} (vacío).")
            continue

        # Limpiar en DF (manejo de nulos, fechas, rangos físicos, etc)
        df = clean_df(df)

        # Añadir columna con el nombre de la estación para identificar la ciudad
        df["estacion"] = nombre

        # Insertar los datos limpios en la base de datos SQLite
        insertar_en_db(df, nombre)

        print(f" ✔ Datos de {nombre} guardados correctamente.")

# Permite ejecutar la ingesta directamente desde terminal.
if __name__ == "__main__":
    ingest()
