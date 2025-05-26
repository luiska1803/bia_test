import os
import pandas as pd
from typing import List
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col
from config import APP_NAME, CSV_FILE, USE_SPARK, RESULTS_DIR, CHUNK_SIZE, WORKERS, MAX_API_SIZE

from concurrent.futures import ThreadPoolExecutor, as_completed
from data_io import extract_load, API_postcodes
from limpieza import limpieza_df

from logger import get_logger
LOGGER = get_logger()

def toma_de_coordenadas_UK(df):
    """
        Solo se tomaran las latitudes entre 49.9 y 60.9 y las longitudes entre -8.6 y 1.8 
        Debido a que estas son los valores aproximados de UK...
    """
    if isinstance(df, SparkDataFrame):
        df_filtrado = df.filter(
            (col("latitude") >= 49.9) & (col("latitude") <= 60.9) &
            (col("longitude") >= -8.6) & (col("longitude") <= 1.8)
        )
    else:
        df_filtrado = df[
            (df['latitude'] >= 49.9) & (df['latitude'] <= 60.9) &
            (df['longitude'] >= -8.6) & (df['longitude'] <= 1.8)
        ]
    
    return df_filtrado

def dividir_dataframe(
    df: pd.DataFrame, 
    partes: int
) -> List[pd.DataFrame]:
    """ 
        Funcion para dividir el dataframe total en partes iguales...
    """
    tamaño_parte = len(df) // partes
    LOGGER.info(f"Tamaño de los Dataframes a procesar: {tamaño_parte} registros c/u")
    return [df.iloc[i:i + tamaño_parte] for i in range(0, len(df), tamaño_parte)]

def generar_batches(df, tamaño):
    for i in range(0, len(df), tamaño):
        yield df.iloc[i:i + tamaño]

def procesar_coordenadas_paralelo(
    df
):
    """ 
        Funcion para procesar la informacion de la API en paralelo...
    """
    if isinstance(df, SparkDataFrame):
        coords = df.select("latitude", "longitude").rdd.map(lambda row: (row["latitude"], row["longitude"])).collect()
    else: 
        coords = list(zip(df['latitude'], df['longitude']))

    resultados = []
    api_conn = API_postcodes()
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        future_to_coord = {executor.submit(api_conn.get_postcode_from_coords, lat, lon): (lat, lon) for lat, lon in coords}
        for future in as_completed(future_to_coord):
            result = future.result()
            if result:
                resultados.append(result)

    return resultados


def procesar_coordenadas_paralelo_post(
    df
):
    """ 
        Funcion para procesar la informacion de la API en paralelo...
    """

    api_conn = API_postcodes()       
    resultados = []
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(api_conn.get_postcode_from_coords_post, batch) for batch in generar_batches(df, tamaño=MAX_API_SIZE)]
        
        for future in as_completed(futures):
            resultados.append(future.result())

    return resultados

########### EJECUCION DE PROCESO #################

def proceso_get():

    LOGGER.info(f"Ejecutando la app {APP_NAME}")
    LOGGER.info(f"Leyendo:  {CSV_FILE}")

    lector = extract_load(use_spark=USE_SPARK)
    df_coordenadas = lector.read_dataset(file_path=CSV_FILE)

    df_limpio = (
        limpieza_df(df_coordenadas)
        .eliminar_nulos()
        .eliminar_duplicados()
        .convertir_tipos({"lat": "float", "lon": "float"})
        .renombrar_columnas({"lat": "latitude", "lon": "longitude"})
        .resultado()
    )

    LOGGER.info(f"Tomando coordenadas aprox de UK.")
    df_filtrado = toma_de_coordenadas_UK(df_limpio)

    os.makedirs(RESULTS_DIR, exist_ok=True) #creamos la carpeta output (en caso de que no este creada)
    archivos = os.listdir(RESULTS_DIR)
    archivos_procesados = [f for f in archivos if f.startswith("chunk_") and f.endswith(".csv")]

    # dividimos en CHUNK_SIZE para guardarlo por partes...
    df_por_partes = dividir_dataframe(df_filtrado, CHUNK_SIZE)
    
    no_archivo = 0 #iniciamos el numero de archivos en 0 para ir guardando, dependiendo si tiene o no archivos guardados
    if archivos_procesados:
        x_procesados = len(archivos_procesados)
        LOGGER.info(f"Archivos procesados: {x_procesados}")
        # quito el numero de dataframes que ya se tienen al inicio de la lista 
        # (esto se puede porque se tiene el mismo df ordenado desde el inicio)
        df_por_partes = df_por_partes[x_procesados:] 
        no_archivo = x_procesados
    
    
    for n in range(0, len(df_por_partes)):
        LOGGER.info(f"Iniciando con el dataframe no. {n+1} de {len(df_por_partes)}")
        resultados = procesar_coordenadas_paralelo(df_por_partes[n])
        if resultados:
            df_result = pd.DataFrame(resultados)
            
            lector.write_dataset(pd.DataFrame(resultados), f"{RESULTS_DIR}/chunk_{no_archivo+n}.csv")
            LOGGER.info(f"Resultados guardados en: {RESULTS_DIR}/chunk_{no_archivo+n}.csv")
            
            lector.insertar_en_db(df=df_result)
            LOGGER.info("Resultados guardados en la base de datos. ")
    
    LOGGER.info("Proceso finalizado :) ")


def proceso_post():

    LOGGER.info(f"Ejecutando la app {APP_NAME}")
    LOGGER.info(f"Leyendo:  {CSV_FILE}")

    lector = extract_load(use_spark=USE_SPARK)
    df_coordenadas = lector.read_dataset(file_path=CSV_FILE)

    df_limpio = (
        limpieza_df(df_coordenadas)
        .eliminar_nulos()
        .eliminar_duplicados()
        .convertir_tipos({"lat": "float", "lon": "float"})
        .renombrar_columnas({"lat": "latitude", "lon": "longitude"})
        .resultado()
    )

    LOGGER.info(f"Tomando coordenadas aprox de UK.")
    df_filtrado = toma_de_coordenadas_UK(df_limpio)

    os.makedirs(RESULTS_DIR, exist_ok=True) #creamos la carpeta output (en caso de que no este creada)
    archivos = os.listdir(RESULTS_DIR)
    archivos_procesados = [f for f in archivos if f.startswith("chunk_") and f.endswith(".csv")]

    # dividimos en CHUNK_SIZE para guardarlo por partes...
    df_por_partes = dividir_dataframe(df_filtrado, CHUNK_SIZE)
    
    no_archivo = 0 #iniciamos el numero de archivos en 0 para ir guardando, dependiendo si tiene o no archivos guardados
    if archivos_procesados:
        x_procesados = len(archivos_procesados)
        LOGGER.info(f"Archivos procesados: {x_procesados}")
        # quito el numero de dataframes que ya se tienen al inicio de la lista 
        # (esto se puede porque se tiene el mismo df ordenado desde el inicio)
        df_por_partes = df_por_partes[x_procesados:] 
        no_archivo = x_procesados
    
    for n in range(0, len(df_por_partes)):
        LOGGER.info(f"Iniciando con el dataframe no. {n+1} de {len(df_por_partes)}")
        resultados = procesar_coordenadas_paralelo_post(df_por_partes[n])
        if resultados:
            df_result = pd.DataFrame(resultados)
            
            lector.write_dataset(pd.DataFrame(resultados), f"{RESULTS_DIR}/chunk_{no_archivo+n}.csv")
            LOGGER.info(f"Resultados guardados en: {RESULTS_DIR}/chunk_{no_archivo+n}.csv")
            
            lector.insertar_en_db(df=df_result)
            LOGGER.info("Resultados guardados en la base de datos. ")
    
    LOGGER.info("Proceso finalizado :) ")