import os
from alerts.telegram import enviar_telegram

def enviar_alertas(alertas):
    """
    Recibe una lista de alertas y las envía por Telegram
    (más adelante añadiremos email).
    """
    if not alertas:
        print("No hay alertas que enviar.")
        return

    mensaje = "\n".join(alertas)

    # Telegram habilitado?
    if os.getenv("TELEGRAM_ENABLED", "False") == "True":
        enviar_telegram(mensaje)
