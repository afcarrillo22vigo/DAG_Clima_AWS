# Data Lake Builder: Arquitectura Medallón con Airflow y MinIO

Este proyecto implementa un pipeline de datos completo (ETL) utilizando la **Arquitectura Medallón** (Bronce, Plata, Oro). Extrae datos meteorológicos en tiempo real a través de una API, los procesa en memoria utilizando Pandas y los almacena en un Data Lake local basado en MinIO (compatible con AWS S3).

## Stack Tecnológico
* **Orquestación:** Apache Airflow
* **Almacenamiento (Data Lake):** MinIO (S3 API)
* **Procesamiento:** Python (Pandas, Boto3, Requests)
* **Infraestructura:** Docker & Docker Compose

## Arquitectura del Pipeline
1. **Capa Bronce (Raw):** Ingesta de datos crudos en formato JSON desde la API de Open-Meteo para múltiples ciudades.
2. **Capa Plata (Cleaned):** Transformación, limpieza y normalización de datos JSON a formato tabular (CSV) procesado 100% en memoria.
3. **Capa Oro (Curated):** Agregación de datos de múltiples fuentes utilizando `pd.concat` para generar un reporte analítico nacional ordenado por temperatura.

## Cómo ejecutarlo
1. Clona este repositorio.
2. Ejecuta `docker-compose up -d`.
3. Accede a Airflow en `localhost:8080` y a MinIO en `localhost:9001`.
4. Activa el DAG `pipeline_nacional_medallon`.