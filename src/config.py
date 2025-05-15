# ---------- CONFIGURACIÓN ----------
APP_NAME= "Bia_APP"
USE_SPARK=False

# ---------- API ---------------------
API_URL = "https://api.postcodes.io"
MAX_RETRIES = 3
TIMEOUT = 5  # En segundos
RATE_LIMIT_DELAY = 0.3 # (3 peticiones por segundo)
CHUNK_SIZE = 500
WORKERS = 15

# --------- CONFIGURACION DE DB (POSTGRES) --------- 
db_config = {
    "host": "postgres_bia",
    "port": 5432,
    "dbname": "bia_db",
    "user": "admin",
    "password": "pass"
}

# ---------- Archivos y rutas -----------------------------
LOG_FILE = "postcode_errors.log"
CSV_FILE = "source/postcodesgeo.csv"
RESULTS_DIR = "output"
