import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import DataFrame as SparkDataFrame
from logger import get_logger

LOGGER = get_logger()

class limpieza_df: 
    
    def __init__(
        self, 
        df
    ):
        self.df = df
        self.is_spark = isinstance(df, SparkDataFrame)
        self.is_pandas = isinstance(df, pd.DataFrame)
        
        if not self.is_spark and not self.is_pandas:
            LOGGER.info(f"El dataframe debe ser de tipo pandas o Pyspark")
            raise TypeError("El DataFrame debe ser de tipo pandas o Pyspark.")
            
    def eliminar_nulos(
        self, 
        columnas: list = None
    ):
        """ 
            Funcion para eliminar nulos... 
        """
        LOGGER.info(f"Eliminando Valores Nulos...")
        if self.is_spark:
            if columnas:
                self.df = self.df.dropna(subset=columnas)
            else:
                self.df = self.df.dropna()
        else:
            self.df = self.df.dropna(subset=columnas) if columnas else self.df.dropna()
        return self

    def eliminar_duplicados(
        self, 
        columnas: list = None
    ):
        """ 
            Funcion para eliminar duplicados... 
        """
        LOGGER.info(f"Eliminando Valores duplicados...")
        if self.is_spark:
            self.df = self.df.dropDuplicates(columnas) if columnas else self.df.dropDuplicates()
        else:
            self.df = self.df.drop_duplicates(subset=columnas)
        return self

    def convertir_tipos(
        self, 
        conversiones: dict
    ):
        """ 
            Funcion para convertir tipos de datos de las columnas que se especifican ... 
        """
        if self.is_spark:
            for col_name, tipo in conversiones.items():
                LOGGER.info(f"convirtiendo columna: {col_name} en tipo: {tipo}")
                self.df = self.df.withColumn(col_name, f.col(col_name).cast(tipo))
        else:
            for col_name, tipo in conversiones.items():
                LOGGER.info(f"convirtiendo columna: {col_name} en tipo: {tipo}")
                self.df[col_name] = self.df[col_name].astype(tipo)

        return self
    
    def renombrar_columnas(
        self, 
        col_renom: dict
    ):
        """ 
            Funcion para renombrar columbas que se especifiquen...
        """
        if self.is_spark:
            LOGGER.info(f"renombrando columnas: {col_renom}")
            for col_vieja, col_nueva in col_renom.items():
                self.df = self.df.withColumnRenamed(col_vieja, col_nueva)
        else:
            LOGGER.info(f"renombrando columnas: {col_renom}")
            self.df = self.df.rename(columns=col_renom)
        
        return self


    def resultado(
        self
    ):
        """ 
            funcion para devolver el resultado de la clase cuando se realiza en secuencia... 
        """
        return self.df