"""
Módulo: ingest.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpenMeteo-SQLite)
Autor: Tamara
Descripción:
    Script de alto nivel encargado de ejecutar la ingesta masiva de datos. 
    Implementa una estrategia de carga en dos fases para optimizar las llamadas
    a la API de Open-Meteo y garantizar que no existan lagunas temporales.

Estrategia de carga:
    1. Fase Histórica (Bloque 1):
        Descarga masiva desde el año 2000 hasta ayer.
        Utiliza 'modo_append=False' para limpiar la base de datos y evitar 
        duplicados antiguos.
    2. Fase de Actualización (Bloque 2): 
        Descarga el día actual y el horizonte de pronóstico. 
        Utiliza 'modo_append=True' para añadir esta información al bloque histórico 
        sin borrarlo.

Seguridad:
    - Implementa pausas de cortesía (time.sleep) para cumplir con las políticas
      de uso de la API gratuita y evitar bloqueos por execso de peticiones (429).
      
Fujo general:
    Para cada ciudad configurada:
        -> bloque 1 (histórico)
        -> Pausa
        -> Bloque 2(forecast + datos recientes)
"""

import time
from datetime import date, timedelta
from config.config import CIUDADES, START_DATE, END_DATE
from data.get_data import get_data

def ingest():
    """
    Ejecuta el ciclo completo de descarga , limpieza y almacenamiento para todas 
    las ciudades configuradas en config.py
    
    Flujo:
        1. Calcular la fecha de ayer para cerrar el bloque histórico.
        2. Para cada ciudad:
            - Ejecuta el bloque histórico (2000-> ayer).
            - Espera 5 segundo para evitar saturación de la API.
            - Ejecuta el bloque de forecast (hoy -> hoy).
    """
    print(f">>> 🔄 INICIANDO CARGA TOTAL (2000 - PRESENTE)")
    
    # Calculamos la fecha de ayer para cerrar el bloque histórico de la API Archive
    ayer = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    for ciudad in CIUDADES:
        nombre = ciudad["nombre"]
        lat, lon = ciudad["lat"], ciudad["lon"]

        # -----------------------------------------------------------------------
        # BLOQUE 1: PROCESAMIENTO HISTÓRICO
        # -----------------------------------------------------------------------
        # Este bloque descarga el grueso de los datos (años de registros).
        # Se usa la API de Archivo Histórico de Open-Meteo.
        print(f"\n📚 Bloque 1: Procesando historial para {nombre}...")
        get_data(nombre, lat, lon, fecha_ini=START_DATE, fecha_fin=ayer, modo_append=False)
        
        # Pausa de seguridad: Vital para prevenir errores 429 (Too Many Requests)
        print("☕ Esperando 5 segundos para refrescar conexión...")
        time.sleep(5)

        # -----------------------------------------------------------------------
        # BLOQUE 2: PROCESAMIENTO DE FORECAST Y DATOS RECIENTES
        # -----------------------------------------------------------------------
        # Este bloque cubre el día de hoy y los días futuros de pronóstico.
        # Al usar modo_append=True, estos datos se "pegan" al final del histórico.
        print(f"📡 Bloque 2: Añadiendo datos recientes y pronóstico...")
        get_data(nombre, lat, lon, fecha_ini=END_DATE, fecha_fin=END_DATE, modo_append=True)

if __name__ == "__main__":
    # Punto de entrada para ejecución manual: 'python ingest.py'
    ingest()