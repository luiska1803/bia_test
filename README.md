Autor: Luis Carlos Sanchez

DESCRIPCIÓN:

    Ejecución: 

        Nota: 
            Se requiere que la carpeta de origen se llame "coord_UK" ya que docker tomara esta carpeta como el directorio de trabajo, en caso de que se llame diferente, 
            modificar el archivo de Dockerfile y colocar el nombre de la carpeta correspondiente.

        Para ejecutar con docker:
        
            - docker-compose -f docker-compose.yml up -d

            Nota: Si se requiere ejecutar en forma local: 
                    El archivo config.py: 
                    - cambiar CSV_FILE = "source/postcodesgeo.csv" por la ruta en donde se encuentre el archivo "postcodesgeo.csv"
        
            Tener encuenta que esto ejecutara todo el proceso, por lo que tomara bastante tiempo en procesar todo el archivo.
            
        Si se requiere ejecutar una parte en especifico, se puede modificar el archivo main.py e indicar cual parte del codigo se quiere ejecutar, sin embargo, el proceso completo es para un sola ejecucion, es decir, todo lo que se ejecute se guardara en la carpeta "output" y si se requiere ejecutar desde cero es necesario que esta este vacia al inicio de la ejecucion. 

        Para observar en las bases de datos: 
            En el archivo config.py se encuentra el db_config con las credenciales de la base de datos y el puerto de conexión. 
            De igual forma se tiene un pgadmin instalado con docker en caso de que quieran ingresar al local host y observar mas "visaul" la base de datos, 
            para ingresar a pgadmin: 
                1. Abrir navegador en: 
                    http://localhost:5000/
                2. Credenciales: 
                    user: admin@admin.com 
                    password: admin
                3. Agregar la base de datos de postgres con las credenciales mencionadas en el archivo config.py 
            

Pasos propuestos en la prueba: 
Ingesta de datos:
    1. Leer el archivo poscodes_geo.csv, asegurando una carga eficiente (manejo de errores, validación de datos, detección de duplicados).
    2. Transformar los datos para que sean consistentes en formato y estructura.
    3. Almacenar los datos en una base de datos relacional o no relacional de tu elección.

    Proceso Realizado: 
    1. Se crea una función que puede leer tanto en pandas como en pyspark, 
        Nota: para la ejecución con pyspark cambiar:
            - "USE_SPARK=False" por "USE_SPARK=True" en el archivo config.py. 
            
            Tener en cuenta que pyspark puede que lea y procese mejor los archivos, sin embargo, para la escritura tardara más, por lo que recomiendo 
            ejecutarlo en pandas
        
        La funcion de lectura se puede encontrar en el archivo data_io.py en la clase "extract_load", esta clase lee y guarda los dataframes en formato csv
        y tambien lee y guarda en base de datos. 

        La funcion cargara de manera eficiente el archivo propuesto en CSV_FILE = "source/postcodesgeo.csv" en el archivo config.py, si el archivo no existe 
        la funcion retornara un error de tipo FileNotFound y avisara por log en terminal que el archivo especificado no existe. 
        Para la validacion de datos se puede añadir un "dtype=dtypes" especificando el tipo de datos que se espera, sin embargo, debido al EDA realizado previamente,
        sabemos que los valores estan en formato flotante (float). 
    
    2. Para la transformación de datos, se crea limpieza.py y proceso.py:

        - limpieza.py: 
            En este archivo se genera una clase llamada limpieza_df, en donde simplemente con especificar el dataframe y las columnas se realiza todo el proceso de limpieza. Dentro de esta clase, se crean funciones para eliminacion de datos null, dropeo de duplicados y cambio de tipo de columna a cada columna especificada. 

                - eliminar_nulos: Funcion en donde se eliminan datos nulos del dataframe, puede ser pandas o pyspark
                
                - eliminar_duplicados: Funcion en donde se eliminan datos duplicados del dataframe, puede ser pandas o pyspark
                
                - convertir_tipos: Funcion en donde se intercambia el tipo de columna especificada por el tipo que dato que se especifique, puede ser pandas o pyspark.
                
                - resultado: es una funcion para el llamado final de todo el proceso propuesto en la clase.
        
        - proceso.py: 
            Este archivo contiene toda el proceso que se realiza para el manejo con la API, este archivo es el que contiene todo el proceso ejecutado por main.py (archivo principal). dentro de este archivo se tiene las funciones de:

                - toma_de_coordenadas_UK: Esta funcion filtra las latitudes entre 49.9 y 60.9 y las longitudes entre -8.6 y 1.8, debido a que estos son los valores aproximados de UK, esto hace que se filtre el dataframe un poco más y se eviten peticiones a la API con coordenadas que no se encontraran en la API. 

                - dividir_dataframe: Esta funcion se realiza debido a que se tiene demasiados registros por procesar, al dividir en dataframes en partes (valor CHUNK_SIZE en el archivo config.py) se puede hacer un guardado de registros segun se vaya completando los dataframes particionados.
                
                - procesar_coordenadas_paralelo: Esta funcion se realiza para poder ejecutar en paralelo las peticiones a la API, esto lo realizo debido a que la API no permite peticiones de diccionarios y se requiere de 1 peticion por registro, lo cual para un archivo tan grande es muy tardio. La funcion trabajara con el numero de WORKERS propuestos en el archivo config.py, esto hara que en lugar de 1 peticion por segundo, se hagan el numero de WORKERS en peticiones por segundo, lo cual incrementa bastante el procesamiento para API. sin embargo, se debe tener cuidado de no hacer demasiadas peticiones debido a que la API puede bloquear la conexion. Se coloca el numero de WORKERS en 15 debido a que la APi permite 15 peticiones por segundo, se podrian aumentar pero se corre el riesgo de que la API bloquee del todo la IP de peticion. 
        
    3. Para amacenar los datos, elegi postgres debido a que las librerias de conectividad con python son faciles de manejar y no requiere tantas configuraciones para su uso con python. Para crear la base de datos se realiza con el archivo docker-compose.yml. en donde se especifica que se usara el archivo init.sql para crear el schema, la tabla y los index propuestos en la prueba. 

        3.1. Para el guardado en la base de datos, el archivo data_io.py contiene la funcion de insertar_en_db() en la clase "extract_load", lo que simplemente tomara lo que se tenga en un dataframe de pandas y se guardara en la base de datos.

Enriquecimiento con API Externa
    1. Consumir la API de postcodes.io para obtener la información detallada del código postal más cercano a cada coordenada.
    2. Manejar errores de API (timeouts, respuestas vacías, fallos en conexión).
    3. Asegurar que todas las coordenadas tengan un código postal. Si no es posible obtenerlo, almacenar el error en un log estructurado.

    Proceso Realizado: 
    Archivo data_io.py: 
    1.Se crea una clase llamada API_postcodes:
        Esta clase es una que trabajara con la configuracion dada en el archivo config.py, se tendra en cuenta un archivo LOG en donde se pondran todos los errores que se encuentren cuando se realicen las peticiones a la API, un timeout que espera en segundos un tiempo para indicar que la peticion fallo, un numero maximo de reintentos con el fin de realizar de nuevo una peticion y un tiempo de delay entre peticiones (con el fin de evitar que la API bloquee las peticiones).
            - get_postcode_from_coords: la funcion  toma las latencias y longitudes dadas y entrega un json con los valores relevantes, se decidio que se entregara las latitudes tanto de entrada como de salida, con el fin de ver cuales coodenadas responden con un request diferente de 200, de igual forma el status, con el fin de analizar cuantas peticiones tienen estados diferentes a 200 y que clase de estados, la API responde muchas veces con mas de un codigo postal por lo que se toma la primera posicion del conjunto que retorna la API, haciendo referencia al codigo postal mas cercano a la coordenada.
    2. log_error: esta funcion toma en cuenta todos los errores que se tengan con las peticiones a la API y las almacenara en un archivo log (LOG_FILE propuesto en el archivo config.py), en este archivo se encuentra todas las coordenadas que tuvieron fallos, el tipo de error que se presento y la hora en la que se presento el error. 
    3. como se menciona anteriormente la funcion ya almacena el log estructurado, ademas de que la funcion get_postcode_from_coords, devolvera todas las coordenadas con el fin de saber cuales tuvieron errores y en caso hipotetico, en un futuro se requiere hacer algo con esta informacion se pueda hacer. 

Optimización y Modelado de Datos
    1. Diseñar un esquema eficiente en la base de datos para almacenar los datos enriquecidos.
    2. Crear índices y optimizaciones para mejorar el rendimiento en consultas futuras.
    
    Proceso Realizado: 
    RTA: el esquema propuesto se puede encontrar en el archivo init.sql, de igual forma se crean indices para busquedas por coordenadas y codigos postales. 

        El diseño del esquema, se realiza teniendo en cuenta las latitudes y longitudes tanto de entrada como de salida de la API, esto es para observar cuantas coordenadas se tienen sin codigo postal y poder hacer un filtro de las mismas a futuro, de igual forma se toma los datos mas relevantes como el codigo postal, el distrito, la region y el estado de la api a la hora de la busqueda, tambien se pone un timestamp para saber la hora a la que se  inserta cada registro. 


Generación de Reportes
    1. Crear una consulta optimizada para obtener los códigos postales más comunes en el dataset.
    2. Calcular estadísticas de calidad de datos (ejemplo: porcentaje de coordenadas sin código postal).
    3. Generar un archivo CSV con los datos enriquecidos y estadísticas generales

    Proceso Realizado:
        Reportes guardados en la carpeta de Output, si se requiere generar de nuevo, ejecutar todo el codigo desde cero pero tener en cuenta que la carpeta "output" debe de estar vacia al inicio de la ejecucion ya que la parte de "reporting" tomara lo que haya en esta carpeta y lo procesara para hacer los reportes, por lo que tomara de nuevo los archivos y generara reportes con data duiplicada o incorrecta. 

Documentación y Entrega
    1. README.md explicando la solución, la arquitectura y los pasos para ejecutar el proyecto.
    2. Diagrama de arquitectura y flujo de datos.
    3. Explicar cualquier decisión clave tomada en el diseño del proceso.

    Proceso Realizado: 
    2. DIAGRAMA: 

        [CSV de coordenadas (postcodesgeo.csv)]      [API (postcodes.io)]                          ***** Fuentes de Datos *****
                        |                                |
                        v                                v
                    [Script Python: procesamiento y limpieza]                                   ***** Proceso de Extracción ******
                                    |
                                    v
                    [Consulta API con manejo de errores]                                         ***** Proceso de Transformación ***** 
                                    |   
                                    +--> [Log de errores (JSON o CSV)]
                                    |
                                    v
                        [Datos enriquecidos (DataFrame)]                                            ***** Proceso de Carga *****
                                    |
                                    +--> [Guardar CSV]
                                    +--> [Guardar en PostgreSQL]
                                    +--> [Generar estadísticas calidad (archivos csv)]


    3. Como decisiones importantes:
    
        1. tome al decision con ThreadPoolExecutor para poder trabajar con varias peticiones al mismo tiempo, esto con el fin de acelerar bastante el proceso, porque se tiene un archivo muy grande y al trabajar en mi maquina local no puedo procesar en forma mas rapida, de igual forma el hacer varias peticiones al tiempo en lugar de una, acelera el proceso bastante. 
        
        2. Tome la decision de trabajar con solo una parte del dataframe, debido a que tratandose de un archivo con mas de 2 millones de registros, aunque se haga de forma paralela las peticiones, se tardaria demasiado tiempo y el proceso de recoleccion de informacion se tomaria mas del tiempo requerido, de igual forma creo que no se requiere el total de coordenadas, sino que se podria analizar con una parte la tendendia y aproximar para saber en que region se tienen mas codigos postales u otro analisis que se ocurra. 
        El porcentaje del dataframe fue del 10% del total, es decir, un total de 246.503 registros.

        3. El colocar un archivo config.py, a pesar de que es un proyecto pequeño, hace que el tener una configuracion en un archivo facil de cambiar, facilitara demasiado el saber si se tiene un error de codigo o de configuracion de parametros, de igual forma si se quiere cambiar un valor para probar alguna funcion, es mas facil cambiarla en un archivo que en todos los pasos que requiera la funcion.


    Adicional: 

        Se coloca un EDA en un notebook con el fin de analizar como esta la data en el dataframe, este EDA lo realizo de manera manual, y tambien de manera automatica con la libraria de pandas, en donde tambien lo exporto a un archivo html, esto es con el fin de analizar mas facilmente los dataframes, sin embargo para este caso, tratandose de coordenadas, no hay nada relevante que este archivo pueda comentar, sin embargo, es util para observar los comportamientos de las variables, su correlacion y valores. 
    