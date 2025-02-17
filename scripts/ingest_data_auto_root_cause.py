import os
import pandas as pd
from google.cloud import bigquery


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/noffel/Documents/klaus-data-model-be6acd563463.json'
client = bigquery.Client(project="klaus-data-model")
def preprocess_autoqa_root_cause(csv_file_path: str) -> pd.DataFrame:
    df = pd.read_csv(
        csv_file_path,
        sep=",",
        engine="python",
        on_bad_lines="skip",
        skip_blank_lines=True
    )
    
    print("Initial AutoQARootCause Data Preview:")
    print(df.head())
    
    df['autoqa_rating_id'] = df['autoqa_rating_id'].astype(str)
    df['category'] = df['category'].fillna('').astype(str)
    df['count'] = pd.to_numeric(df['count'], errors='coerce').astype("Int64")
    df['root_cause'] = df['root_cause'].astype(str)
    
    print("Data types after preprocessing:")
    print(df.dtypes)
    return df

def load_autoqa_root_cause(df: pd.DataFrame, table_id: str):
    load_job = client.load_table_from_dataframe(df, table_id)
    load_job.result() 
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")

def main():
    csv_file_path = r"C:\Users\noffel\klaus-bigquery-ingestion\data\autoqa_root_cause_test - autoqa_root_cause_test.csv"
    table_id = "klaus-data-model.klaus_dataset.AutoQARootCause"
    
    df_root_cause = preprocess_autoqa_root_cause(csv_file_path)
    load_autoqa_root_cause(df_root_cause, table_id)

if __name__ == "__main__":
    main()