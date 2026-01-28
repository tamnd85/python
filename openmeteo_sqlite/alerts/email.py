"""
Módulo: email.py
Autor: Tamara
Descripción:
    Configuración y envío de correos electrónicos para el sistema de alertas
    
    Este módulo:
        - Carga redenciales desde variables de entorno (.env)
        - Construye mensajes MIME de texto plano
        - Envía correos mediante SMTP seguro (SSL)
        - Controla si el envío estña habilitadi mediante ALARM_EMAIL_ENABLED 
"""
import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Carga las variables de entorno desde el archivo .env
load_dotenv()

def enviar_email(asunto: str, mensaje: str) -> None:
    """
    Envía un email con un asunto y un mensaje de texto plano.

    Requisitos:
        - ALARM_EMAIL_ENABLED=True en el .env
        - Variables ALARM_MAIL_FROM, ALARM_EMAIL_TO y ALARM_EMAIL_PASSWORD configuradas
        - Servidor SMTP accesible (por defecto Gmail)
        
    Flujo:
        1. Verifica si el envío de email está habilitado.
        2. Carga credenciales y configuración SMTP desde el entorno.
        3. Construye en mensaje MIME.
        4. Intenta enviar el correo usando SMTP_SSL.
        5. Maneja errores comunes (autenticación, otros).
    """
    
    #--------------------------------------------------------------------
    # 1. Verificar si el envío de email está habilitado
    #--------------------------------------------------------------------
    # La variable ALARM_EMAIL_ENABLED controla si se envían correos.
    # Si no está en "True", se aborta el envío.
    
    if os.getenv("ALARM_EMAIL_ENABLED", "False") != "True":
        print("📭 Email deshabilitado en .env")
        return

    #--------------------------------------------------------------------
    # 2. Cargar credenciales y configuración SMTP
    #--------------------------------------------------------------------
    remitente = os.getenv("ALARM_EMAIL_FROM")
    destinatario = os.getenv("ALARM_EMAIL_TO")
    password = os.getenv("ALARM_EMAIL_PASSWORD")
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))

    # Validación básica de credenciales
    if not remitente or not destinatario or not password:
        print("⚠️ Faltan credenciales de email en el .env")
        return

    #--------------------------------------------------------------------
    # 3. Construcción del mensaje MIME
    #--------------------------------------------------------------------
    # MIMEText crea un email de texto plano.
    msg = MIMEText(mensaje)
    msg["Subject"] = asunto
    msg["From"] = remitente
    msg["To"] = destinatario

    #--------------------------------------------------------------------
    # 4. Envío del email usando SMTP con SSL
    #--------------------------------------------------------------------
    try:
        # Se abre una conexión segura cone l servidor SMTP
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            #Autenticación con las credenciales del remitente
            server.login(remitente, password)
            
            # Envío del mensaje ya fromateado
            server.sendmail(remitente, destinatario, msg.as_string())

        print("📧 Email enviado correctamente")

    #--------------------------------------------------------------------
    # 5. Manejo de errores
    #--------------------------------------------------------------------
    except smtplib.SMTPAuthenticationError:
        # Error típico cuando la contraseña/ token es incorrecto
        print("❌ Error de autenticación SMTP.")
    except Exception as e:
        # Cualquier otro error inesperado
        print(f"⚠️ Error enviando email: {e}")
