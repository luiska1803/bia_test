from proceso import proceso
from reporting import proceso_reporting

from logger import get_logger
LOGGER = get_logger()

if __name__ == "__main__":
    LOGGER.info("########### INICIO DE PROCESO ############")
    proceso()
    LOGGER.info("########### INICIO DE REPORTING ###########")
    proceso_reporting()
    