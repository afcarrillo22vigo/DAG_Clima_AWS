# 🚲 Urban Mobility Data Lake & Analytics Pipeline

Este proyecto es un pipeline de datos "End-to-End" diseñado para monitorizar en tiempo real el estado de los sistemas de alquiler de bicicletas públicas en varias ciudades europeas (Madrid, Londres, Barcelona). 

Implementa una **Arquitectura Medallón** automatizada que extrae datos anidados, los aplanan en un Data Lake y finalmente los sirve en un Data Warehouse para su visualización.

## 🛠️ Stack Tecnológico
* **Orquestación:** Apache Airflow
* **Data Lake:** MinIO (Compatible con AWS S3)
* **Data Warehouse (Serving Layer):** PostgreSQL
* **Procesamiento ETL:** Python (Pandas, Boto3, Requests, SQLAlchemy)
* **Visualización (BI):** Metabase
* **Infraestructura:** Docker & Docker Compose

## 🏗️ Arquitectura del Flujo de Datos
1. **Capa Bronce (Raw):** Ingesta horaria de datos crudos (JSON complejos) desde la API REST de CityBikes, almacenados directamente en MinIO.
2. **Capa Plata (Cleaned):** Aplanamiento de estructuras JSON anidadas utilizando `pd.json_normalize`, limpieza de columnas y almacenamiento tabular (CSV) en el Data Lake.
3. **Capa Oro & Serving:** Agrupación y filtrado de datos (Top 5 de estaciones y resúmenes globales). Los datos finales se inyectan dinámicamente en una base de datos **PostgreSQL** mediante `SQLAlchemy`.
4. **Capa de Visualización:** Conexión de **Metabase** a PostgreSQL para generar cuadros de mando (Dashboards) interactivos para los usuarios de negocio.

## 🚀 Cómo ejecutar el proyecto
1. Clona el repositorio.
2. Levanta la infraestructura con `docker-compose up -d`.
3. Accede a Airflow en `localhost:8080` y activa el DAG `bikes_datalake`.
4. Accede a Metabase en `localhost:3000` para explorar el dashboard interactivo.