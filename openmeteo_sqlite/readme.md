# Sistema de Análisis y Alertas Meteorológicas

Procesamiento, análisis y notificación de eventos meteorológicos con Python

Este proyecto implementa un sistema modular para descargar, procesar, analizar y alertar sobre datos meteorológicos, utilizando fuentes abiertas como Open‑Meteo, y generando notificaciones mediante Telegram y correo electrónico.

El objetivo es disponer de un pipeline profesional, reproducible y mantenible, con módulos independientes y fácilmente ampliables.



## Descripción general

    El sistema OpenMeteo_SQLite combina técnicas de series temporales y machine learning para generar predicciones meteorológicas robustas:

        SARIMA: MOdelado estacional que captura la tendencia anual y lo ciclos climáticos.

        XGBoost: "Corrector" inteligente que aprende los residuos (errores) del SARIMA basándose en variables exógenas como el viento, la presión y al humedad

        Modelo híbrido estable: Predicción Final = SARIMA + residuo_XGBoost

    El pipeline:

        1.Descarga datos históricos desde Open‑Meteo

        2.Limpia y transforma el dataset

        3.Lo almacena en SQLite

        4.Genera features para SARIMA y XGBoost

        5.Entrena modelos por ciudad

        6.Produce predicciones futuras

        7.Evalúa reglas meteorológicas

        8.Envía alertas por Telegram y email

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

#### Tipos de entrenamiento

        - Entrenamiento multiciudad:
                El modelo XGBoost se entrena con datos combinados de todas las estaciones, permitiendo que la "física" aprendida en un lugar (ej. relación presión-temperatura) ayude a predecir otros.

        - Validación a ciegas:
                Estrategia estaciones_random donde el modelo se entrena con $N-1$ ciudades y se evalúa en una ciudad totalmente desconocida, garantizando la capacidad de generalización
        -Optimización por Muestreo Aleatorio (Monthly Sampling): 
                En el pipeline mensual, se extraen $X$ días al azar de cada mes (ej. 25 días). Esto actúa como una técnica de regularización, obligando al modelo a aprender la variabilidad térmica mensual sin depender de una secuencia lineal.
        Validación Robusta
                Split Temporal 80/20: 
                        Respeta estrictamente el orden cronológico. El 80% inicial entrena y el 20% final valida, evitando el data leakage (uso de información del futuro para predecir el pasado).
#### Modelos
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

#### Entrenamiento mensual
Pipeline mensual:
        1.Agregación mensual del dataset
        2.Entrenamiento de SARIMA mensual
        3.Cálculo del residuo mensual
        4.Entrenamiento de XGBoost mensual sobre el residuo
        5.Predicción híbrida mensual

Modelo generado:

models/xgboost/xgb_multiciudad_mensual.pkl

Este modelo permite análisis de tendencia a largo plazo y predicciones más estables en horizontes amplios.

Optimización por Muestreo Aleatorio (Monthly Sampling): 
        Para evitar el sobreajuste y mejorar la eficiencia del entrenamiento, el pipeline mensual implementa un algoritmo de selección aleatoria de días.

        De cada mes, se extraen X días al azar (por defecto 25).

        Esto garantiza que el modelo aprenda la variabilidad térmica del mes sin depender de una secuencia lineal, actuando como una técnica de regularización que mejora la generalización en predicciones de largo plazo.

#### Entrenamiento y validación (80/20)
El sistema utiliza un split temporal 80/20, respetando el orden cronológico:
        80% inicial → entrenamiento
        20% final → validación
        Sin mezcla aleatoria (time‑series safe)

Esto garantiza que los modelos no vean información futura durante el entrenamiento.

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
│   ├── muestreo.py
│   ├── sarima_features.py
│   └── xgb_features.py
├── models/
│   ├── sarima
│   │   ├── sarima_mensual.pkl
│   │   └── sarima.pkl
│   ├── xgboost
│   │   ├── xgb_multiciudad_features.pkl
│   │   ├── xgb_multiciudad_mensual.pkl
│   │   ├── xgb_multiciudad_features.pkl
│   │   └── xgb_multiciudad.pkl
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

### Entrenamiento del Modelo
        - Entrenamiento Multiciudad:
                Entrena con todos los datos disponibles en la base de datos, usando un spilt temporal

        python main.py train --modo normal --split temporal

        - Validación Ciega:
                Entrena con todas las ciudades excepto una aleatoria para evaluar la capacidad de generalización del modelo

        python main.py train --modo normal --split estaciones_random

### Generar Predicciones (Forecast)
        Calcula la temperatura hibrida(SARIMA+XGBoost) integrando el pronostrico del viento y presión en tiempo real

        python main.py forecast --ciudad "" --dias 7

### Sistema de alertas
        Evalúa las condiciones meteorológicas y envía notificaciones a Telegram y Email

        python main.py alerts


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
