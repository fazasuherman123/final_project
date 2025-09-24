import pandas as pd
import glob
import os
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from config import GCP_PROJECT_ID, BQ_DATASET_RAW, BASE_FOLDER, BQ_LOCATION

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def create_dataset_if_not_exists(client, dataset_id):
    """Buat dataset BQ kalau belum ada."""
    try:
        client.get_dataset(dataset_id)
        logging.info(f"Dataset `{dataset_id}` already exists.")
    except NotFound:
        dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset_ref.location = BQ_LOCATION
        client.create_dataset(dataset_ref)
        logging.info(f"Created dataset `{dataset_id}` in location `{BQ_LOCATION}`.")

def upload_to_bq(df, table_name, client):
    """Upload DataFrame ke BigQuery dengan logging jumlah baris."""
    temp_file = None
    try:
        create_dataset_if_not_exists(client, BQ_DATASET_RAW)
        table_ref = client.dataset(BQ_DATASET_RAW).table(table_name)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True
        )

        temp_file = f"{table_name}_temp.parquet"
        df.to_parquet(temp_file, index=False)
        logging.info(f"Uploading {len(df)} rows into `{BQ_DATASET_RAW}.{table_name}`")

        with open(temp_file, "rb") as f:
            load_job = client.load_table_from_file(f, table_ref, job_config=job_config)
        load_job.result()

        table = client.get_table(table_ref)
        logging.info(f"Uploaded. Table `{table_name}` now has {table.num_rows} rows in BigQuery.")

    except Exception as e:
        logging.error(f"Failed to load {table_name}: {e}")
    finally:
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)

def load_csvs_by_date(table_names):
    """Gabungkan CSV per tanggal untuk tiap table lalu upload ke BQ."""
    client = bigquery.Client(GCP_PROJECT_ID)
    for table_name in table_names:
        logging.info(f"Processing table: {table_name}")
        all_files = glob.glob(f"{BASE_FOLDER}/*/{table_name}.csv")

        if not all_files:
            logging.warning(f"No CSV files found for {table_name} in {BASE_FOLDER}")
            continue

        df_list = []
        for file in all_files:
            try:
                df = pd.read_csv(file)
                date_str = os.path.basename(os.path.dirname(file))
                df["date"] = date_str
                df_list.append(df)
                logging.info(f"Read {len(df)} rows from {file}")
            except Exception as e:
                logging.error(f"Failed to read {file}: {e}")

        if df_list:
            try:
                df_combined = pd.concat(df_list, ignore_index=True, sort=True)
                logging.info(f"Combined {len(all_files)} files → {len(df_combined)} rows total")
                upload_to_bq(df_combined, table_name, client)
            except Exception as e:
                logging.error(f"Failed to combine/upload {table_name}: {e}")

def load_single_csv(file_path, table_name):
    """Load single CSV (non-date folder) ke BQ."""
    if not os.path.exists(file_path):
        logging.warning(f"File {file_path} not found")
        return

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Read {len(df)} rows from {file_path}")
        client = bigquery.Client(GCP_PROJECT_ID)
        upload_to_bq(df, table_name, client)
    except Exception as e:
        logging.error(f"Failed to load {table_name} from {file_path}: {e}")

if __name__ == "__main__":
    tables_by_date = ["addresses", "customers", "order_items", "orders", "payments"]
    load_csvs_by_date(tables_by_date)

    products_file = os.path.join(BASE_FOLDER, "products.csv")
    load_single_csv(products_file, "products")
