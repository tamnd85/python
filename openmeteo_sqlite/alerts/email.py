"""
Módulo: email.py
Proyecto: Sistema de Predicción Meteorológica Híbrida (OpeneMeteo_Sqlite)
Autor: Tamara
Descripción:
    Configuración y envío de correos electrónicos para el sistema de alertas
    
    Este módulo prporciona una interfaz simple y segura para enviar alertas
    meteorológicas por correo electrónico. El comportamiento del envío depende 
    de variables de entorno definidas en el archivo .env, lo que permite activas
    o desactivar el canal sin modificar el código.
    
Funcionalidades:
    - carga de credenciales desde variables del entorno.
    - Construcción de mensajes MIME de texto plano.
    - Envío mediante SMTP seguro (SSL).
    - Control de activación mediante ALARM_EMAIL_ENABLED.
    
Requisitos en .env:
    ALARM_EMAIL_ENABLED=True/False
    ALARM_EMAIL_FROM=correo_remitente
    ALARM_EMAIL_TO=correo_destinatario
    ALARM_:EMAIL:PASSWORD=contraseña_o_token
    SMTP_SERVER=stmp.gmail.com (por defecto)
    STMP_PORT=465 (por defecto)
"""
import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Carga las variables de entorno desde el archivo .env
load_dotenv()

#----------------------------------------------------------------------------
# Función principal: envío de emails
#----------------------------------------------------------------------------
def enviar_email(asunto: str, mensaje: str) -> None:
    """
    Envía un email con un asunto y un mensaje de texto plano.

    Flujo:
        1. Verifica si el envío de email está habilitado.
        2. Carga credenciales y configuración SMTP desde el entorno.
        3. Construye en mensaje MIME.
        4. Intenta enviar el correo usando SMTP_SSL.
        5. Maneja errores comunes (autenticación, otros).
        
    Parámetros:
        asunto: str
            Titulo del correo electrónico.
        mensaje: str
            Contenido del mensaje en texto plano
            
    Retorna
        None
            No retorna nada; solo ejecuta el envío si está habilitado.
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
    # MIMEText crea un email de texto plano con cabeceras estándar.
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
