"""
Módulo: email.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpeneMeteo_Sqlite)
Autor: Tamara
Descripción:
    Envío de mensajes de alerta a Telegram usando la API oficial.
    
    Este módulo prporciona una interfaz sencilla ara enviar notificaciones
    meteorológicas a un chat de Telegram. El envío depende de credenciales
    definidas en el archivo .env, lo que permite activar o desactivar el canal
    sin modificar el código.
    
Funcionalidades:
    - Carga de creadenciales desde variables de entorno.
    - Construcciñon de peticiones HTTP POST a la API oficial de telegram.
    - Envío de mensajes de texto a un chat concreto.
    - Interpretación de códigos de errores comunes ( 400, 401, 403).
    
Requisitos en .env:
    TELEGRAM_BOT_TOKEN=token_del_bot
    TELEGRAM_CHAT_ID= id_del_chat
    TELEGRAM_ENABLED=True/False (controlado por alert_sender.py)
    
Nota:
    El usuario debe haber pulsado START en el bot para permitir el envío.
"""
import os
import requests
from dotenv import load_dotenv

# Carga las variables de entorno desde el archivo .env
load_dotenv()
#----------------------------------------------------------------------------
# Función principal: envío de mensajes a Telegram
#----------------------------------------------------------------------------

def enviar_telegram(mensaje: str) -> None:
    """
    Envía un mensaje de texto a un chat de Telegram usando la API oficial

    Flujo:
        1. Cargar token y chat_id desde variables de entorno.
        2. Validar que existen.
        3. Construir la URL de la API y el payload.
        4. Enviar el mensaje mediante POST.
        5. Interpretar códigos de error comunes
        
    Parámetros:
        mensaje: str
            texto plano que se enviará al chat de Telegram.
            
    Retorna:
        None
            No retorna nada; solo ejecuta el envío si las credenciales son válidas.
    """
    
    #----------------------------------------------------------------------------
    # 1. Cargar credenciales desde el entorno
    #----------------------------------------------------------------------------
    
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    # Validación básica: si falta token o chat_id, no se puede enviar nada
    if not bot_token or not chat_id:
        print("⚠️ Falta TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID en el .env")
        return

    #--------------------------------------------------------------------------
    # 2. Construcción de la petición a la API  de Telegram
    #--------------------------------------------------------------------------
    # Endpoint oficial para enviar mensajes
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # Playload mínimo requerido por Telegram: chat_id + texto
    payload = {"chat_id": chat_id, "text": mensaje}

    #--------------------------------------------------------------------------
    # 3. Envío del mensaje
    #--------------------------------------------------------------------------
    try:
        resp = requests.post(url, data=payload)

        #---------------------------------------------------------------------
        # 4. Interpretación de respuestas HTTP
        #---------------------------------------------------------------------
        if resp.status_code == 200:
            # Envío correcto
            print("📨 Mensaje enviado a Telegram correctamente")
            return

        # Errores comunes de la API
        elif resp.status_code == 400:
            print("❌ Error 400: CHAT_ID incorrecto")
        elif resp.status_code == 401:
            print("❌ Error 401: TOKEN incorrecto")
        elif resp.status_code == 403:
            print("❌ Error 403: El bot NO tiene permiso para escribirte")
            print("   ➤ Abre Telegram y pulsa START en tu bot")
        else:
            # Otros errores no contemplados explícitamente
            print(f"⚠️ Error al enviar mensaje a Telegram: {resp.status_code} - {resp.text}")

    #--------------------------------------------------------------------------
    # 5. Manejo de errores de conexión
    #--------------------------------------------------------------------------
    except Exception as e:
        print(f"⚠️ Error de conexión al enviar mensaje a Telegram: {e}")
