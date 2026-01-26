import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

def enviar_email(asunto: str, mensaje: str) -> None:
    """
    Envía un email usando las credenciales definidas en el .env
    """
    if os.getenv("ALARM_EMAIL_ENABLED", "False") != "True":
        print("Email deshabilitado en .env")
        return

    remitente = os.getenv("ALARM_EMAIL_FROM")
    destinatario = os.getenv("ALARM_EMAIL_TO")
    password = os.getenv("ALARM_EMAIL_PASSWORD")
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))

    if not remitente or not destinatario or not password:
        print("⚠️ Faltan credenciales de email en el .env")
        return

    msg = MIMEText(mensaje)
    msg["Subject"] = asunto
    msg["From"] = remitente
    msg["To"] = destinatario

    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(remitente, password)
            server.sendmail(remitente, destinatario, msg.as_string())
        print("📧 Email enviado correctamente")
    except Exception as e:
        print(f"⚠️ Error enviando email: {e}")
