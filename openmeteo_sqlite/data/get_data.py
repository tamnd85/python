"""
Módulo: get_data.py
Autor: Tamara
Descripción:
    Orquesta el proceso completo de obtención de datos meteorológicos para una 
    ciudad concreta. este módulo coordina la descarga desde Open-Meteo, la
    limpieza robusta del dataFrame y su posterior inserción en la base de datos.
     
     Forma parte del pipeline principal del sistema de predicción y alertas 
     meteorológicas.
"""

from datetime import datetime

from data.downloader import descargar_datos_openmeteo
from data.cleaning import clean_df
from db.database import insertar_en_db


def get_data(ciudad, lat, lon, fecha_ini=None, fecha_fin=None):
    """
    Descarga, limpia y guarda datos meteorológicos para una ciudad concreta.
    
    Parámetros:
        ciudad (str): Nombre de la ciudad.
        lat (float): Latitud.
        lon (float): Longitud.
        fecha_ini (str|datetime): Fecha inicial opcional.
        fecha_fin (str|datetime): Fecha final opcional.
        
    Flujo:
        1. Convertir fechas si vienen como string.
        2. Descargar datos usando la función oficial del downloader.
        3. Validar que la serie no está vacía.
        4. Limpiar el DataFrame con clean_df().
        5. Borrar datos previos de esa ciudad en la DB.
        6. Insertar los nuevos datos.
    """

    # Convertir fechas si vienen como string
    if isinstance(fecha_ini, str):
        fecha_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")

    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")

    print(f"\n Descargando datos para {ciudad}...")

    #---------------------------------------------------------------------
    # USAR LA FUNCIÓN OFICIAL
    #---------------------------------------------------------------------
    df = descargar_datos_openmeteo(lat, lon, fecha_ini, fecha_fin)

    if df.empty:
        raise ValueError(f"La serie descargada para {ciudad} está vacía.")

    # Limpiar
    df = clean_df(df)

    # Guardar en DB
    borrar_ciudad(ciudad)
    insertar_en_db(df, ciudad)

    print(f"✔ Datos de {ciudad} guardados correctamente.")
    return df
