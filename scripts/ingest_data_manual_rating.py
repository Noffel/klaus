import os
import pandas as pd
from google.cloud import bigquery


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/noffel/Documents/klaus-data-model-be6acd563463.json'
client = bigquery.Client(project="klaus-data-model")

df_manual_rating = pd.read_csv(r'C:\Users\noffel\klaus-bigquery-ingestion\data\manual_rating_test - manual_rating.csv')

df_manual_rating['category_id'] = df_manual_rating['category_id'].astype(int)

table_id_manual_rating = "klaus-data-model.klaus_dataset.ManualRating"

load_job_manual_rating = client.load_table_from_dataframe(df_manual_rating, table_id_manual_rating)
load_job_manual_rating.result()  
print(f"Loaded {load_job_manual_rating.output_rows} rows into {table_id_manual_rating}.")