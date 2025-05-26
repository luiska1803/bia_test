# ---------- CONFIGURACIÓN ----------
APP_NAME= "coord_UK"
USE_SPARK=False

# ---------- API ---------------------
API_URL = "https://api.postcodes.io"
MAX_RETRIES = 3
TIMEOUT = 5  # En segundos
RATE_LIMIT_DELAY = 0.3 # (3 peticiones por segundo)
CHUNK_SIZE = 500
WORKERS = 15
MAX_API_SIZE = 100 #Tamaño maximo de peticiones tipo post a la API

# --------- CONFIGURACION DE DB (POSTGRES) --------- 
db_config = {
    "host": "postgres_coord_UK",
    "port": 5432,
    "dbname": "coord_UK_db",
    "user": "admin",
    "password": "pass"
}

# ---------- Archivos y rutas -----------------------------
LOG_FILE = "postcode_errors.log"
CSV_FILE = "source/postcodesgeo.csv"
RESULTS_DIR = "output"
