# data/get_data.py

from datetime import datetime

from data.downloader import descargar_datos_openmeteo
from data.cleaning import clean_df
from db.database import insertar_en_db


def get_data(ciudad, lat, lon, fecha_ini=None, fecha_fin=None):
    """
    Descarga, limpia y guarda datos meteorológicos para una ciudad concreta.
    """

    # Convertir fechas si vienen como string
    if isinstance(fecha_ini, str):
        fecha_ini = datetime.strptime(fecha_ini, "%Y-%m-%d")

    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")

    print(f"\n Descargando datos para {ciudad}...")

    # --- USAR LA FUNCIÓN OFICIAL ---
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
