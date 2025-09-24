from google.cloud import bigquery

def create_datasets():
    client = bigquery.Client()
    print("🟢 Using project:", client.project)

    datasets = [
        "jcdeol005_finalproject_faza_staging",
        "jcdeol005_finalproject_faza_model",
        "jcdeol005_finalproject_faza_marts"
    ]

    for ds in datasets:
        dataset_id = f"{client.project}.{ds}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-southeast2"
        created = client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Created dataset: {created.full_dataset_id} in {created.location}")

if __name__ == "__main__":
    create_datasets()
