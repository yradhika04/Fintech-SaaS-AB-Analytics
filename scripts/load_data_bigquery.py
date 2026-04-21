"""
This script uploads the fintech SaaS csv files to BigQuery
"""

import os
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

project_id = "project-157130f5-0b8a-414d-a34"
dataset_id = "fintech_saas_raw"  # using one dataset for all source tables
location = "EU"
data_dir = "../data/input_data/"


#source table schemas
schemas = {
    "users": [
        bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("signup_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("device", "STRING", mode="NULLABLE"),  #it has around 2% null values
        bigquery.SchemaField("acquisition_channel", "STRING", mode="NULLABLE"),  #it has around 2% null values
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    ],
    "experiment_assignments": [
        bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("experiment_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("variant", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("assigned_at", "TIMESTAMP", mode="REQUIRED"),
    ],
    "events": [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("event_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("variant", "STRING", mode="REQUIRED"),
    ],
    "subscriptions": [
        bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("variant", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("plan", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mrr", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("sub_start_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("churned", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("churn_date", "TIMESTAMP", mode="NULLABLE"),
    ],
}


def create_dataset(client: bigquery.Client):
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = location
    try:
        client.create_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' created")
    except Conflict:
        print(f"Dataset '{dataset_id}' already exists")


def upload_table(client: bigquery.Client, table_name: str):
    csv_path = os.path.join(data_dir, f"{table_name}.csv")
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    schema = schemas[table_name]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  #to skip the header row
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(csv_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)

    job.result()

    table = client.get_table(table_ref)
    print(f"{table_name}: {table.num_rows:,} rows loaded into {table_ref}")


def main():
    client = bigquery.Client(project=project_id)

    print("Creating dataset")
    create_dataset(client)

    print("\nUploading tables")
    for table_name in schemas:
        upload_table(client, table_name)

    print("\nAll tables uploaded")


if __name__ == "__main__":
    main()