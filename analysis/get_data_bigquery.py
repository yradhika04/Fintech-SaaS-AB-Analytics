import os
import pandas as pd
from google.cloud import bigquery

project_id = "project-157130f5-0b8a-414d-a34"
dataset_id = "fintech_saas_dev"
location = "EU"
data_dir = "../data/transformed_data/"

if not os.path.exists(data_dir):
    os.makedirs(data_dir)


client = bigquery.Client(project=project_id)

user_journey = client.query(f"SELECT * FROM `{client.project}.{dataset_id}.mart_core__user_journey`").to_dataframe()
experiment_results = client.query(f"SELECT * FROM `{client.project}.{dataset_id}.mart_core__experiment_results`").to_dataframe()
subscriptions = client.query(f"SELECT * FROM `{client.project}.{dataset_id}.mart_core__subscriptions`").to_dataframe()

user_journey.to_csv(f"{data_dir}user_journey.csv")
experiment_results.to_csv(f"{data_dir}experiment_results.csv")
subscriptions.to_csv(f"{data_dir}subscriptions.csv")