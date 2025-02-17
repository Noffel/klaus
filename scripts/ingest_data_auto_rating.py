import os
import pandas as pd
from google.cloud import bigquery


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/noffel/Documents/klaus-data-model-be6acd563463.json'
client = bigquery.Client(project="klaus-data-model")

def preprocess_autoqa_ratings(csv_file_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_file_path)
    print("Initial AutoQARatings Preview:")
    print(df.head())
    
    # Ensuring the identifier columns are strings
    df["autoqa_review_id"] = df["autoqa_review_id"].astype(str)
    df["autoqa_rating_id"] = df["autoqa_rating_id"].astype(str)
    df["external_ticket_id"] = df["external_ticket_id"].astype(str)
    df["rating_category_name"] = df["rating_category_name"].astype(str)
    
    # Converting numeric columns with proper eror handling and null support
    df["payment_id"] = pd.to_numeric(df["payment_id"], errors='coerce').astype("Int64")
    df["team_id"] = pd.to_numeric(df["team_id"], errors='coerce').astype("Int64")
    df["payment_token_id"] = pd.to_numeric(df["payment_token_id"], errors='coerce').astype("Int64")
    df["rating_category_id"] = pd.to_numeric(df["rating_category_id"], errors='coerce').astype("Int64")
    df["rating_scale_score"] = pd.to_numeric(df["rating_scale_score"], errors='coerce')
    df["score"] = pd.to_numeric(df["score"], errors='coerce')
    df["reviewee_internal_id"] = pd.to_numeric(df["reviewee_internal_id"], errors='coerce').astype("Int64")
    
    print("AutoQARatings Data Types After Preprocessing:")
    print(df.dtypes)
    return df

def load_autoqa_ratings(df: pd.DataFrame, table_id: str):
    # Loading the DataFrame into the BigQuery table
    load_job = client.load_table_from_dataframe(df, table_id)
    load_job.result() 
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")

def main():
    csv_file_path = r"C:\Users\noffel\klaus-bigquery-ingestion\data\autoqa_ratings_test - autoqa_ratings.csv"
    table_id = "klaus-data-model.klaus_dataset.AutoQARating"
    
    df_autoqa_ratings = preprocess_autoqa_ratings(csv_file_path)
    load_autoqa_ratings(df_autoqa_ratings, table_id)

if __name__ == "__main__":
    main()