import os
import pandas as pd
import pyspark
import requests
import json
import time

from sqlalchemy import create_engine
from typing import Optional
from pyspark.sql import DataFrame as SparkDataFrame
from config import APP_NAME, TIMEOUT, API_URL, LOG_FILE, MAX_RETRIES, RATE_LIMIT_DELAY, db_config

from logger import get_logger

LOGGER = get_logger()

def _init_spark():
    """
    Funcion para inializar spark...
    """
    spark = (
        pyspark.sql.SparkSession.builder.appName(APP_NAME).getOrCreate()
    )

    return spark

class API_postcodes:
    """
        Esta clase es para la conexion con la API de postcodes y generar un archivo de log de error en caso de fallo...
    """
    def __init__(self):
        self.api_url = API_URL
        self.log_file = LOG_FILE
        self.timeout = TIMEOUT
        self.MAX_RETRIES = MAX_RETRIES
        self.RATE_LIMIT_DELAY = RATE_LIMIT_DELAY
    
    def log_error(self, lat: float, lon: float, error: str):
        with open(self.log_file, "a") as f:
            json.dump({
                "lat": lat,
                "lon": lon,
                "error": error,
                "timestamp": time.time()
            }, f)
            f.write("\n")

    def get_postcode_from_coords_get(self, latitud: float, longitud: float) -> Optional[dict]:
        """
            Funcion para recibir informacion a partir de las cordenadas entregadas a la API... 
        """
        for _ in range(self.MAX_RETRIES):
            try:
                response = requests.get(
                    f"{self.api_url}/postcodes",
                    params={"lat": latitud, "lon": longitud},
                    timeout=self.timeout
                )

                if response.status_code == 429:  # Estado para "too Many Request"
                    retry_after = int(response.headers.get("Retry-After", 1))
                    time.sleep(retry_after)
                    return API_postcodes.get_postcode_from_coords(latitud, longitud)

                response.raise_for_status()
                data = response.json()

                if data["status"] == 200 and data["result"]:
                    # Se añade la latitud y la longitud que le introducimos con el fin de comparar y saber cuales no responden...
                    selected = {
                        "lat": latitud,
                        "lon": longitud,
                        "postcode": data["result"][0]["postcode"],
                        "admin_district": data["result"][0]["admin_district"],
                        "region": data["result"][0]["region"],
                        "latitude": data["result"][0]["latitude"],
                        "longitude": data["result"][0]["longitude"],
                        "estado_api" : data["status"],
                        "descripcion_estado": "ok",
                    }
                    return selected 
                else:
                    self.log_error(latitud, longitud, "No se encontro ningun resultado")
                    selected = {
                        "lat": latitud,
                        "lon": longitud,
                        "postcode": None,
                        "admin_district": None,
                        "region": None,
                        "latitude": None,
                        "longitude": None,
                        "estado_api" : data["status"],
                        "descripcion_estado": "OK",
                    }
                    return selected

            # Tipos de errores más comunes... 
            except requests.exceptions.Timeout:
                self.log_error(latitud, longitud, "Timeout...")
                time.sleep(self.RATE_LIMIT_DELAY)
                selected = {
                    "lat": latitud,
                    "lon": longitud,
                    "postcode": None,
                    "admin_district": None,
                    "region": None,
                    "latitude": None,
                    "longitude": None,
                    "estado_api" : -1,
                    "descripcion_estado": "timeout",
                }

                return selected
                
            except requests.exceptions.RequestException as e:
                self.log_error(latitud, longitud, f"Error de conexion: {str(e)}")
                time.sleep(self.RATE_LIMIT_DELAY)
                selected = {
                    "lat": latitud,
                    "lon": longitud,
                    "postcode": None,
                    "admin_district": None,
                    "region": None,
                    "latitude": None,
                    "longitude": None,
                    "estado_api" : -2,
                    "descripcion_estado": "error de conexion",
                }

                return selected
            except Exception as e:
                self.log_error(latitud, longitud, f"Error inesperado: {str(e)}")
                time.sleep(self.RATE_LIMIT_DELAY)
                selected = {
                    "lat": latitud,
                    "lon": longitud,
                    "postcode": None,
                    "admin_district": None,
                    "region": None,
                    "latitude": None,
                    "longitude": None,
                    "estado_api" : 0,
                    "descripcion_estado": "error inesperado",
                }

                return selected

        return None

    def get_postcode_from_coords_post(self, df) -> Optional[dict]:
        for _ in range(self.MAX_RETRIES):
            try:
                json_data = df[['latitude', 'longitude']].to_dict(orient='records')
                response = requests.post(
                    f"{self.api_url}/postcodes",
                    json={"geolocations": json_data},
                    timeout=self.timeout
                )
            
                data = response.json()
                if data["status"] == 200:
                    return data["result"]
                else:
                    return {"error": f"Status {response.status_code}", "batch": json_data}
                
            except Exception as e:
                return {"error": str(e), "batch": df.to_dict(orient="records")}  
            except requests.exceptions.RequestException as e:
                self.log_error(f"Error de conexion: {str(e)} ,batch: { df.to_dict(orient='records')}")
                time.sleep(self.RATE_LIMIT_DELAY)
            except Exception as e:
                self.log_error(f"Error inesperado: {str(e)} ,batch: { df.to_dict(orient='records')}")

        return None

class extract_load:
    """
        Esta clase es para leer y escribir en pyspark o pandas archivos CSV... 
    """

    def __init__(
        self, 
        use_spark=False
    ):  
        self.db_config = db_config
        self.use_spark = use_spark
        self.spark = None
        if self.use_spark:
            self.spark = _init_spark()

    def read_dataset(
        self, 
        file_path, 
        **kwargs
    ):
        """
            Funcion para leer dataframes  (pandas /Pyspark)...
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo '{file_path}' no existe.")

        if self.use_spark:
            LOGGER.info(f"Leyendo archivo: {file_path} con PySpark...")
            return self.spark.read.csv(file_path, header=True, inferSchema=True, **kwargs)
        else:
            LOGGER.info(f"Leyendo archivo: {file_path} con Pandas...")
            return pd.read_csv(file_path, **kwargs)

    def write_dataset(
        self, 
        df, 
        output_path, 
        **kwargs
    ):
        """
            Funcion para guardar los dataframes en csv (se puede usar para pandas o pyspark)...
        """
        if self.use_spark:
            if not isinstance(df, SparkDataFrame):
                raise TypeError("Se esperaba un DataFrame de PySpark.")
            LOGGER.info(f"Escribiendo archivo con PySpark en: {output_path}")
            df.write.csv(output_path, header=True, mode="overwrite", **kwargs)
        else:
            if not isinstance(df, pd.DataFrame):
                raise TypeError("Se esperaba un DataFrame de Pandas.")
            LOGGER.info(f"Escribiendo archivo con Pandas en: {output_path}")
            df.to_csv(output_path, index=False, **kwargs)

    def limpieza_carpeta(
        self,
        folder_path
    ):
        """
            funcion para limpiar una carpeta...
        """
        for archivo in os.listdir(folder_path):
            ruta = os.path.join(folder_path, archivo)
            
            if os.path.isfile(ruta) and archivo.lower().endswith('.csv'):
                try:
                    os.remove(ruta)
                    print(f"Eliminado: {archivo}")
                except Exception as e:
                    print(f"Error al eliminar {archivo}: {e}")

    def insertar_en_db(
        self, 
        df
    ):
        """
            Funcion para insertar datos en la base de datos de postgres, a partir de un dataframe de pandas...
        """
        host = self.db_config["host"]
        puerto = self.db_config["port"]
        db_name = self.db_config["dbname"]
        name = self.db_config["user"]
        password = self.db_config["password"]

        engine = create_engine(f"postgresql+psycopg2://{name}:{password}@{host}:{puerto}/{db_name}")

        df.to_sql("postcodes", engine, if_exists="append", index=False)

    def leer_query(
        self, 
        query
    ):
        """
            Funcion para leer un query en la base de datos con ayuda de python y devuelve un df en pandas... 
        """
        host = self.db_config["host"]
        puerto = self.db_config["port"]
        db_name = self.db_config["dbname"]
        name = self.db_config["user"]
        password = self.db_config["password"]

        engine = create_engine(f"postgresql+psycopg2://{name}:{password}@{host}:{puerto}/{db_name}")

        df = pd.read_sql_query(query, engine)

        return df