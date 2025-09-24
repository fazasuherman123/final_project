import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

# Set credentials path sebelum library GCP dipanggil
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if credentials_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# BigQuery config
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW", "jcdeol005_finalproject_faza_raw") 
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-southeast2")  


# Airflow
AIRFLOW_UID = os.getenv("AIRFLOW_UID", "50000")

# Base folder untuk data CSV
BASE_FOLDER = os.getenv("BASE_FOLDER", "/opt/airflow/data/raw")
