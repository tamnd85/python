"""
Módulo: alert_sender.py
Autor: Tamara
Descripción:
    Envía alertas por Telegram y Email.
    
    Este módulo centraliza el envío de notificaciones meteorológicas 
    generadas por alert_rules.py. El envío depende de varables de entorno 
    que permiten activar o descativar cada canal sin modificcar el código
"""

import os
from alerts.telegram import enviar_telegram
from alerts.email import enviar_email


def enviar_alertas(alertas):
    """
    Envía una lista de alertas a los canales configurados.
    
    Flujo:
        1. Si no hay alertas, no se envía nada.
        2. Construye un único mensaje con todas las alertas.
        3. Envía por Telegram si TELEGRAM_ENABLED=True en el entorno.
        4. Envía por Email si ALARM_EMAIL_ENABLED=True en el entorno.
        
    Parámetros:
        alertas: lis[str]
            Lista de amensajes de alerta generados por alert_rules detectar_alertas()
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
