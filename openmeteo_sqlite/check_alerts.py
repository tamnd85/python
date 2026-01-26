import os
from alerts.telegram import enviar_telegram
from alerts.email import enviar_email

def enviar_alertas(alertas):
    if not alertas:
        print("No hay alertas que enviar.")
        return

    mensaje = "\n".join(alertas)

    # Telegram
    if os.getenv("TELEGRAM_ENABLED", "False") == "True":
        enviar_telegram(mensaje)

    # Email
    if os.getenv("ALARM_EMAIL_ENABLED", "False") == "True":
        enviar_email("⚠️ Alertas meteorológicas", mensaje)
