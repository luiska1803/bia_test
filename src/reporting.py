import os
import pandas as pd
from pathlib import Path

from config import USE_SPARK, RESULTS_DIR

from data_io import extract_load

from logger import get_logger
LOGGER = get_logger()

def leer_df(
    lector, 
    output_path
):
    """ 
        Funcion para leer los dataframes de una carpeta... 
    """
    dfs = []
    LOGGER.info(f"Leyendo archivos de: {output_path}/")
    for archivo in Path(output_path).glob('*.csv'):
        LOGGER.info(f"Leyendo archivo: {archivo}")
        df = lector.read_dataset(file_path=archivo)
        dfs.append(df)

    df_completo = pd.concat(dfs, ignore_index=True)

    return df_completo


def obtener_postcodes_mas_comunes(
    lector
):
    """ 
        funcion para obtener los postcodes mas comunes a partir de un query...
    """

    query = """
        SELECT 
            postcode, 
            COUNT(*) AS frecuencia
        FROM 
            postcodes
        WHERE 
            postcode IS NOT NULL
        GROUP BY 
            postcode
        ORDER BY 
            frecuencia DESC
        LIMIT 10;
    """
    df = lector.leer_query(query)

    return df


def generar_estadisticas_calidad(
    df, 
    lista_analisis
):
    """ 
        Funcion para generar las estadisticas del dataframe... 
    """
    total = len(df)
    df_stats = []
    for col in lista_analisis:
        unicos = df[col].nunique()
        duplicados = df[col].duplicated().sum()
        suma_nulos = df[col].isnull().sum()
        porcentaje_nulos = round(100 * suma_nulos / total, 2)
        porcentaje_unicos = round(100 * unicos / total, 2)
        mas_comun = df[col].mode().iloc[0]
        menos_comun = df[col].value_counts(ascending=True).index[0]

        stats = {
            "total_registros": total,
            "columna": col,
            "cantidad_nulos": suma_nulos,
            "porcentaje_nulos": porcentaje_nulos,
            "valores_unicos": unicos,
            "porcentaje_unicos": porcentaje_unicos,
            "duplicados": duplicados,
            "registro_mas_comun": mas_comun,
            "registro_menos_comun": menos_comun,
        }

        df_ = pd.DataFrame([stats])
        df_stats.append(df_)

    df_completo = pd.concat(df_stats, ignore_index=True)
    
    return df_completo


########### EJECUCION DE REPORTING #################

def proceso_reporting():

    lector = extract_load(use_spark=USE_SPARK)
    df_total = leer_df(lector=lector, output_path=RESULTS_DIR)
    lector.limpieza_carpeta(RESULTS_DIR) # Vaciamos la carpeta para guardar un unico archivo final...
    lector.write_dataset(pd.DataFrame(df_total), f"{RESULTS_DIR}/archivo_enriquecido_final.csv")
    LOGGER.info(f"registros insertados en la DB")
    LOGGER.info(f"Archivo enriquecido final guardando en: {RESULTS_DIR}/archivo_enriquecido_final.csv")

    df_stats = generar_estadisticas_calidad(df_total, ['postcode','admin_district','region','latitude','longitude','estado_api','descripcion_estado'])
    lector.write_dataset(pd.DataFrame(df_stats), f"{RESULTS_DIR}/archivo_stats.csv")
    LOGGER.info(f"Archivo stats guardado en: {RESULTS_DIR}/archivo_stats.csv")

    df_top_postcodes = obtener_postcodes_mas_comunes(lector)
    lector.write_dataset(df_top_postcodes, f"{RESULTS_DIR}/postales_mas_comunes.csv")
    LOGGER.info(f"Archivo postales mas comunes guardado en: {RESULTS_DIR}/postales_mas_comunes.csv")
    LOGGER.info("Proceso Reporting finalizado :) ")

    