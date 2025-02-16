import os
import pandas as pd
from google.cloud import bigquery


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/noffel/Documents/klaus-data-model-be6acd563463.json'
client = bigquery.Client(project="klaus-data-model")

# Preprocessing of the CSV Data with Converters
def preprocess_manual_review(csv_file_path: str) -> pd.DataFrame:
    # Defining a converter for the conversation_created_date column.
    def convert_conversation_created_date(x):
        # Cleaning whitespace and checking if the str is empty.
        x = str(x).strip()
        if x == '' or x.lower() == 'nan':
            return None
        # Attempt to convert to datetime using the expected format YYYY-MM-DD.
        try:
            dt = pd.to_datetime(x, format='%Y-%m-%d', errors='coerce')
        except Exception:
            dt = None
        return dt.date() if pd.notnull(dt) else None

    # Using the converters parameter to handle conversation_created_date.
    converters = {
        'conversation_created_date': convert_conversation_created_date
    }
    
    df = pd.read_csv(csv_file_path, converters=converters)
    
    dtype_spec = {
        'review_id': 'Int64',
        'payment_id': 'Int64',
        'payment_token_id': 'Int64',
        'team_id': 'Int64',
        'reviewer_id': 'Int64',
        'reviewee_id': 'Int64',
        'comment_id': 'Int64',
        'scorecard_id': 'Int64',
        'score': 'float64',
        'updated_by': 'Int64',
        'review_time_seconds': 'Int64'
    }
    df = df.astype(dtype_spec, errors='ignore')
    
    # Converting date/time columns to datetime objects.
    for col in ['created', 'conversation_created_at', 'updated_at', 'imported_at']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Ensuring that conversation_external_id is treated as string.
    df['conversation_external_id'] = df['conversation_external_id'].astype(str)
    
    print("Data types after preprocessing:")
    print(df.dtypes)
    return df

#  Inserting Data into BigQuery
def insert_manual_review_data(df: pd.DataFrame, table_id: str):
    load_job = client.load_table_from_dataframe(df, table_id)
    load_job.result() 
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")

#   Main func
def main():
    manual_review_csv = r'C:\Users\noffel\klaus-bigquery-ingestion\data\manual_reviews_test - manual_reviews.csv'
    manual_review_table_id = "klaus-data-model.klaus_dataset.ManualReview"
    
    df_manual_reviews = preprocess_manual_review(manual_review_csv)
    insert_manual_review_data(df_manual_reviews, manual_review_table_id)

if __name__ == "__main__":
    main()
