# Sistema de Análisis y Alertas Meteorológicas

Procesamiento, análisis y notificación de eventos meteorológicos con Python

Este proyecto implementa un sistema modular para descargar, procesar, analizar y alertar sobre datos meteorológicos, utilizando fuentes abiertas como Open‑Meteo, y generando notificaciones mediante Telegram y correo electrónico.

El objetivo es disponer de un pipeline profesional, reproducible y mantenible, con módulos independientes y fácilmente ampliables.



## Descripción general

    El sistema OpenMeteo_SQLite combina técnicas de series temporales y machine learning para generar predicciones meteorológicas robustas:

        SARIMA (modelado estacional anual)

        XGBoost (aprendizaje de residuos del modelo SARIMA)

        Modelo híbrido estable: predicción = SARIMA + residuo_predicho

        El sistema descarga datos históricos desde Open-Meteo, los limpia y transforma, los almacena en SQLite, genera features para SARIMA y XGBOOST, entrena modelos por ciudad , genera predicciones futuras de forma estable y coherente, evalúa las reglas meteorológicas y envía alertas por Telegram y email.

## Funcionalidades principales

### Descarga da datos meteorológico

El módulo downloader.py obtine datos de Open-Meteo:
        - Temperatura ( min, max, media)
        - Humedad
        - Precipitación 
        - Presión atmosféricqa
        - Viento
        - Horas de sol
Los data se guardan en data/ y se ingresan en SQLite.

### Procesamiento  y limpieza

El módulo cleaning.py:
        - Convierte fechas
        - Maneja valores nulos
        - Normaliza columnas
        - Calcula variables derivadas ( año, mes y día del año)
        - Prepara el dataset para modelado

### Modelo predictivo

El sistema entrena dos modelos:
        - SARIMA
                - Captura estacionalidad anual
                - Modela tendencia y ciclos
        -XGBOOST
                - Aprende los residuos de SARIMA
                - Captura patrones no lineales
        -Modelo HÍBRIDO
                Combina ambos para obtener predicciones más estables y precisas.
Los modelos se guardan en 
        -models/sarima/sarima.pkl
        -models/xgboost/xgb.pkl

### Análisis y visualización

Incluye:
        - Gráficas de temperatura, humedad y precipitación
        - Boxplots por estaciones
        - Precipitación acumulada
        - Correlaciones entre años
        - Notebooks exploratorios

### Sistema de alertas

El sistema evalúa reglas definidas en alerts/alert_rules.py, como:
        - Temperatura mínima < 3ºC  -> alerta de helada
        - Descenso de temperaturas temop_media  desciende 2 ºC -> alerta de descenso de temperaturas
        - Precipitaciones > 20 mm -> lluvia intensa
        - Nubosidad > 50% -> dia nublado

## Estructura del proyecto

openmeteo_sqlite/
├── alerts/
│   ├── alert_rules.py
│   ├── alert_sender.py
│   ├── email.py
│   └── telegram.py
├── config/
│   └── config.py
├── data/
│   ├── cleaning.py
│   ├── downloader.py
│   ├── get_data.py
│   ├── ingest_exog.py
│   └── ingest.py
├── datos/
│   └── openmeteo.db
├── db/
│   └── database.py
├── feautures/
│   ├── sarima_features.py
│   └── xgb_features.py
├── models/
│   ├── sarima
│   │   └── sarima.pkl
│   ├── xgboost
│   │   └── xgb.pkl
│   ├── hybrid.py
│   ├── sarima.py
│   └── xgboost_model.py
├── notebooks/
│   └── graficos.ipynb
├── pipeline/
│   ├── forecast.py
│   └── train.py
├── .env
├── check_alerts.py
├── main.py
├── readme.md
└── run.py



##  Instalación

1. Clonar el repositorio:

        git clone https://github.com/tu_usuario/openmeteo_sqlite.git


##  Ejecución

### Ejecutar el pipeline completo:

        python src/pipeline.py


### Ejecutar módulos individuales:


        python data/get_data.py
        python pipeline/train.py
        python pipeline/forecast.py
        python check_alerts.py






##  Base de datos

El sistema utiliza SQLite paar almacenar:
        - Datos meteorológicos históricos
        - Variables exógenas
        - Predicciones
        - Logs de ejecución

La base de datos se crea automáticamente en datos/openemeteo.db


##  Requisitos

- Python 3.10 o superior    
- Conexión a internet para acceder a la API  
- Librerías: pandas, numpy, statsmodels, xgboost, request, sqlite3, python.dotenv....



##  Mejoras futuras

- Añadir logging avanzado  
- Exportación a CSV/Parquet  
- Dashboard interactivo (Streamlit / Dash)
- Automatización con cron, Airflow o Perfect
- Nuevos modelos (Prophet, LSTM, RandomForestregressor)
- API REST para servir predicciones  



##  Autor

Proyecto desarrollado por **Tamara** como parte de su portfolio de proyectos Python.
