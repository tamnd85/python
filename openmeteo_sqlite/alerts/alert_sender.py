"""
Módulo: alert_sender.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpeneMeteo_Sqlite)
Autor: Tamara
Descripción:
    Envía alertas por Telegram y Email.
    
    Este módulo centraliza el envío de notificaciones meteorológicas  para las alertas
    generadas por alert_rules.py. El comportamiento de cada canal de envío 
    depende de variables de entorno, lo que permite activas po desactivar
    Telegram y/o Email son midificar el código.

Funcionamiento:
    - Si no hay alertas, no se envía nada.
    - Se construye un mensaje único con todas las alertas.
    - Si TELEGRAM_ENABLED=True -> se envía por Telegram.
    - SI ALARM_EMAIL_ENABLED=True -> se envía por Email.
    
Requisitos:
    - Variables de entorno configuradas en .env:
        TELEGRAM_ENABLED=True/False
        ALARM_EMAIL_ENABLED=True/False
    Módulos:
        alerts.telegram.enviar_telegram()
        alerts.email.enviar_email()
"""

import os
from alerts.telegram import enviar_telegram
from alerts.email import enviar_email


def enviar_alertas(alertas):
    """
    Envía una lista de alertas a los canales configurados.
    
    Flujo:
        1. Validación: si no hay alertas, se aborta el envío.
        2. CONstrucción de un único mensaje con todas las alertas.
        3. Envío por Telegram si TELEGRAM_ENABLED=True.
        4. Envío por Email si ALARM_EMAIL_ENABLED=True.
        
    Parámetros:
        alertas: lis[str]
            Lista de amensajes de alerta generados por alert_rules detectar_alertas().
            
    Retorna:
        None
            No retorna nada; solo ejecuta los envíos cnfigurados.
    """

    #------------------------------------------------------------- 
    #  1. Validación: si no hay alertas, no se envía nada 
    #-------------------------------------------------------------
    if not alertas:
        print("No hay alertas que enviar.")
        return

    #------------------------------------------------------------- 
    # 2. Construcción del mensaje final 
    #-------------------------------------------------------------
    mensaje = "\n".join(alertas)

    #----------------------------------------------------------------------
    # 3. Envío por Telegram
    #----------------------------------------------------------------------
    # La variable de entorno TELEGRAM_ENABLED controla si se envía o no.
    # Se compara como string porque las varibañes de entorno siempre son texto.
    
    if os.getenv("TELEGRAM_ENABLED", "False") == "True":
        print("📨 Enviando alertas por Telegram...")
        enviar_telegram(mensaje)
    else:
        print("Telegram deshabilitado en .env")

    #----------------------------------------------------------------------
    # 4. Envío por Email
    #----------------------------------------------------------------------
    # Similar al caso anterior, pero usando ALARM_EMAIL_ENABLED.
    
    if os.getenv("ALARM_EMAIL_ENABLED", "False") == "True":
        print("📧 Enviando alertas por Email...")
        enviar_email("⚠️ Alertas meteorológicas", mensaje)
    else:
        print("Email deshabilitado en .env")
