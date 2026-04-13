import logging
from airflow.sdk import dag, task
import pendulum
import requests
import boto3
import json
import pandas as pd
from io import BytesIO

CIUDADES = [
    {"nombre": "Ourense", "lat": 42.3367, "lon": -7.8641},
    {"nombre": "Madrid", "lat": 40.4165, "lon": -3.7026},
    {"nombre": "Barcelona", "lat": 41.3888, "lon": 2.159},
]


@dag(
    dag_id="clima_datalake",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    tags=["clima", "nube"],
)
def pipeline_clima():

    def conectarse_bd():
        s3_client = boto3.client(
            "s3",  # ¿Nombre?
            endpoint_url="http://minio:9000",  # El nombre del contenedor en Docker
            aws_access_key_id="admin",  # Usuario escrito en el docker-compose
            aws_secret_access_key="password123",  # Contraseña escrita en el docker-compose
            region_name="us-east-1",  # Región por defecto
        )
        return s3_client

    def importar_json_capa_bronce(url, ciudad):
        s3_client = conectarse_bd()
        respuesta = requests.get(url)
        datos_json = respuesta.json()

        nombre_archivo = f"clima_{ciudad}_bruto.json"

        s3_client.put_object(
            Bucket="capa-bronce",  # Nombre del BUCKET creado en la interfaz (localhost:9001)
            Key=nombre_archivo,
            Body=json.dumps(datos_json),  # Convertimos los datos a texto JSON
        )

        logging.info(f"Archivo {nombre_archivo} subido a MinIO.")
        return nombre_archivo

    def obtener_archivo(file):
        s3_client = conectarse_bd()
        # 1. Conectarse a MiniIO y descargar JSON bruto
        object = s3_client.get_object(Bucket="capa-bronce", Key=file)
        data_bytes = object["Body"].read()
        data_dict = json.loads(data_bytes.decode("utf-8"))

        return data_dict

    def transformar_archivo(file, ciudad):
        s3_client = conectarse_bd()
        data_dict = obtener_archivo(file)

        # 2. Pasárselo a Pandas y extraer la información necesaria
        data_extracted = {
            "Ciudad": [ciudad],
            "latitud": [data_dict["latitude"]],
            "longitud": [data_dict["longitude"]],
            "temperatura_c": [data_dict["current"]["temperature_2m"]],
            "humedad_pct": [data_dict["current"]["relative_humidity_2m"]],
            "viento_kmh": [data_dict["current"]["wind_speed_10m"]],
            "fecha_hora": [data_dict["current"]["time"]],
        }
        df = pd.DataFrame(data_extracted)

        # 3. Convertir a archivo limpio (.csv)
        csv_file = df.to_csv(mode="w", index=False, header=True)

        # 4. Subir el .csv a nuevo Bucket
        nombre_archivo = f"clima_{ciudad}_clean.csv"
        s3_client.put_object(
            Bucket="capa-plata",  # Nombre del BUCKET creado en la interfaz (localhost:9001)
            Key=nombre_archivo,
            Body=csv_file,  # Convertimos los datos a texto CSV
        )

        return nombre_archivo

    @task
    def extraer_y_subir_a_nube():
        lista_archivos = []
        for ciudad in CIUDADES:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={ciudad['lat']}&longitude={ciudad['lon']}&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
            lista_archivos.append(importar_json_capa_bronce(url, ciudad.get("nombre")))

        # devolver lista de nombre de archivos
        return lista_archivos

    @task
    def descargar_limpiar_datos(lista_archivos):
        archivos_limpios = []
        for archivo, ciudad in zip(lista_archivos, CIUDADES):
            name = transformar_archivo(archivo, ciudad.get("nombre"))
            archivos_limpios.append(name)

        return archivos_limpios

    @task
    def juntar_datos(lista_archivos):
        s3_client = conectarse_bd()
        # Juntar en una sola tabla
        lista_df = []
        for file in lista_archivos:
            # object = obtener_archivo(file) NO PODEMOS LLAMAR A LA FUNCION (revisa la capa bronce y en JSON)
            object = s3_client.get_object(Bucket="capa-plata", Key=file)
            df_ciudad = pd.read_csv(BytesIO(object["Body"].read()))
            lista_df.append(df_ciudad)

        df_final = pd.concat(lista_df, ignore_index=True)
        df_final = df_final.sort_values(by="temperatura_c")

        csv_final = df_final.to_csv(index=False)
        nombre_archivo = "reporte_nacional_clima.csv"
        s3_client.put_object(
            Bucket="capa-oro",
            Key=nombre_archivo,
            Body=csv_final,
        )

        logging.info(f"Reporte Nacional Creado!\n{df_final}")
        return "reporte_nacional_clima.csv"

    # Orden de ejecución
    lista_archivos = extraer_y_subir_a_nube()
    lista_csv = descargar_limpiar_datos(lista_archivos)
    reporte_oro = juntar_datos(lista_csv)


# Arrancamos el motor
pipeline_clima()
