import os
import pandas as pd
from google.cloud import bigquery


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/noffel/Documents/klaus-data-model-be6acd563463.json'
client = bigquery.Client(project="klaus-data-model")

def preprocess_autoqa_reviews(csv_file_path: str) -> pd.DataFrame:
    # Read the CSV file.
    df = pd.read_csv(csv_file_path)
    print("Initial autoQA Reviews Data Preview:")
    print(df.head())

    # String columns
    df["autoqa_review_id"] = df["autoqa_review_id"].astype(str)
    df["external_ticket_id"] = df["external_ticket_id"].astype(str)

    # Numeric columns
    df["payment_id"] = pd.to_numeric(df["payment_id"], errors='coerce').astype("Int64")
    df["payment_token_id"] = pd.to_numeric(df["payment_token_id"], errors='coerce').astype("Int64")
    df["team_id"] = pd.to_numeric(df["team_id"], errors='coerce').astype("Int64")
    df["reviewee_internal_id"] = pd.to_numeric(df["reviewee_internal_id"], errors='coerce').astype("Int64")

    # Timestamp columns
    df["created_at"] = pd.to_datetime(df["created_at"], errors='coerce')
    df["conversation_created_at"] = pd.to_datetime(df["conversation_created_at"], errors='coerce')
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors='coerce')

    # Convertibng conversation_created_date to DATE object
    df["conversation_created_date"] = pd.to_datetime(
        df["conversation_created_date"], format='%Y-%m-%d', errors='coerce'
    ).dt.date

    print("Data types after preprocessing:")
    print(df.dtypes)
    return df

def load_autoqa_reviews(df: pd.DataFrame, table_id: str):
    load_job = client.load_table_from_dataframe(df, table_id)
    load_job.result()  
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")

def main():
    csv_file_path = r"C:\Users\noffel\klaus-bigquery-ingestion\data\autoqa_reviews_test - autoqa_reviews_test.csv"
    table_id = "klaus-data-model.klaus_dataset.AutoQAReview"
    
    df_autoqa = preprocess_autoqa_reviews(csv_file_path)
    load_autoqa_reviews(df_autoqa, table_id)

if __name__ == "__main__":
    main()