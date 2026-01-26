import os
import requests
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()

def enviar_telegram(mensaje: str) -> None:
    """
    Envía un mensaje de texto a Telegram usando BOT_TOKEN y CHAT_ID
    definidos en el archivo .env
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print("⚠️ Falta TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID en el .env")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": mensaje}

    resp = requests.post(url, data=payload)

    if resp.status_code != 200:
        print(f"⚠️ Error al enviar mensaje a Telegram: {resp.status_code} - {resp.text}")
