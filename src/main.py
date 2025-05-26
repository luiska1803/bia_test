from proceso import proceso_get, proceso_post
from reporting import proceso_reporting

from logger import get_logger
LOGGER = get_logger()

if __name__ == "__main__":
    LOGGER.info("########### INICIO DE PROCESO ############")
    # se puede hacer por cualquier proceso, get es coordenada a coordenada y POST en un maximo de 100 en 100 (max. tamaño que permite la API).
    proceso_get()
    #proceso_post() 
    LOGGER.info("########### INICIO DE REPORTING ###########")
    proceso_reporting()
    