import logging
from airflow.sdk import dag, task
import pendulum
import requests
import boto3
import json
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine

CIUDADES = [
    {"nombre": "Londres", "id": "santander-cycles"},
    {"nombre": "Madrid", "id": "bicimad"},
    {"nombre": "Barcelona", "id": "bicing"},
]


@dag(
    dag_id="bikes_datalake",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    tags=["clima", "nube"],
)
def pipeline_bikes():

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

        nombre_archivo = f"bikes_{ciudad}_bruto.json"

        s3_client.put_object(
            Bucket="bikes-bronce",  # Nombre del BUCKET creado en la interfaz (localhost:9001)
            Key=nombre_archivo,
            Body=json.dumps(datos_json),  # Convertimos los datos a texto JSON
        )

        logging.info(f"Archivo {nombre_archivo} subido a MinIO.")
        return nombre_archivo

    def obtener_archivo(file):
        s3_client = conectarse_bd()
        # 1. Conectarse a MiniIO y descargar JSON bruto
        object = s3_client.get_object(Bucket="bikes-bronce", Key=file)
        data_bytes = object["Body"].read()
        data_dict = json.loads(data_bytes.decode("utf-8"))

        return data_dict

    def transformar_archivo(file, ciudad):
        s3_client = conectarse_bd()
        data_dict = obtener_archivo(file)

        # 2. Pasárselo a Pandas y extraer la información necesaria
        lista_estaciones = data_dict["network"]["stations"]

        # json_normalize convierte esa lista de diccionarios en una tabla perfecta
        df = pd.json_normalize(lista_estaciones)
        df["Ciudad"] = ciudad

        columnas_utiles = ["Ciudad", "name", "free_bikes", "empty_slots", "timestamp"]
        df_clean = df[columnas_utiles]

        # 3. Convertir a archivo limpio (.csv)
        csv_file = df_clean.to_csv(index=False)

        # 4. Subir el .csv a nuevo Bucket
        nombre_archivo = f"bikes_{ciudad}_clean.csv"
        s3_client.put_object(
            Bucket="bikes-plata",  # Nombre del BUCKET creado en la interfaz (localhost:9001)
            Key=nombre_archivo,
            Body=csv_file,  # Convertimos los datos a texto CSV
        )

        return nombre_archivo

    @task
    def extraer_bronce():
        lista_archivos = []

        for ciudad in CIUDADES:
            url = f"http://api.citybik.es/v2/networks/{ciudad.get('id')}"

            nombre_generado = importar_json_capa_bronce(url, ciudad.get("nombre"))

            datos_bronce = {
                "nombre_archivo": nombre_generado,
                "nombre_ciudad": ciudad.get("nombre"),
            }

            lista_archivos.append(datos_bronce)

        return lista_archivos

    @task
    def limpiar_plata(lista_archivos):
        archivos_limpios = []
        for archivo in lista_archivos:
            nombre_archivo = archivo.get("nombre_archivo")
            ciudad = archivo.get("nombre_ciudad")
            f = transformar_archivo(nombre_archivo, ciudad)
            archivos_limpios.append(f)

        return archivos_limpios

    @task
    def final_oro(lista_archivos):
        s3_client = conectarse_bd()
        # Juntar en una sola tabla
        lista_df = []
        for file in lista_archivos:
            # object = obtener_archivo(file) NO PODEMOS LLAMAR A LA FUNCION (revisa la capa bronce y en JSON)
            object = s3_client.get_object(Bucket="bikes-plata", Key=file)
            df_ciudad = pd.read_csv(BytesIO(object["Body"].read()))
            lista_df.append(df_ciudad)

        df_global = pd.concat(lista_df, ignore_index=True)
        logging.info(f"\n{df_global}")

        # Agrupamos por ciudad y sumamos las bicis y los huecos
        df_resumen = (
            df_global.groupby("Ciudad")[["free_bikes", "empty_slots"]]
            .sum()
            .reset_index()
        )

        csv_final = df_resumen.to_csv(index=False)
        nombre_archivo = "report_bikes.csv"
        s3_client.put_object(
            Bucket="bikes-oro",
            Key=nombre_archivo,
            Body=csv_final,
        )

        # --- TOP 5 CALLES POR CIUDAD ---
        ciudades = df_global["Ciudad"].unique()  # Lista de ciudades sin repetidos

        lista_top5 = []
        for ciudad in ciudades:
            # Filtramos la tabla gigante para quedarnos solo con las filas de esa ciudad
            df_filtro = df_global[df_global["Ciudad"] == ciudad]

            # Ordenamos por bicis libres (de más a menos)
            df_top5 = df_filtro.sort_values(by="free_bikes", ascending=False).head(5)

            # Subir a MiniIO
            nombre_top5 = f"top5_calles_{ciudad}.csv"
            s3_client.put_object(
                Bucket="bikes-oro",
                Key=nombre_top5,
                Body=df_top5.to_csv(index=False),
            )
            lista_top5.append(df_top5)

        # --- SUBIR A POSTGRES SQL --- usuario:contraseña@servidor:puerto/base_de_datos
        motor_sql = create_engine(
            "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        )

        df_resumen.to_sql(
            "resumen_ciudades", motor_sql, if_exists="replace", index=False
        )
        logging.info("Tabla resumen ciudades creada en PostgresSQL")

        df_top5_global = pd.concat(lista_top5, ignore_index=True)
        df_top5_global.to_sql(
            "top5_estaciones", motor_sql, if_exists="replace", index=False
        )
        logging.info("Tabla top5_estaciones creada en PostgresSQL")

        logging.info(f"Reporte Creado!\n{df_resumen}\n")
        logging.info("TOP 5 Calles de cada ciudad creado!")
        return nombre_archivo

    archivos = extraer_bronce()
    archivos_plata = limpiar_plata(archivos)
    final_oro(archivos_plata)


pipeline_bikes()
