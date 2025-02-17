import os
import pandas as pd
from google.cloud import bigquery


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/noffel/Documents/klaus-data-model-be6acd563463.json'
client = bigquery.Client(project="klaus-data-model")

def preprocess_conversations(csv_file_path: str) -> pd.DataFrame:
    df = pd.read_csv(
        csv_file_path, 
        engine='python', 
        on_bad_lines='skip', 
        skip_blank_lines=True
    )
    print("Initial Conversations Data Preview:")
    print(df.head())
    
    numeric_columns = [
        'payment_id', 'payment_token_id', 'assignee_id', 'message_count',
        'unique_public_agent_count', 'private_message_count', 'public_message_count',
        'agent_most_public_messages', 'first_response_time', 
        'first_resolution_time_seconds', 'full_resolution_time_seconds',
        'most_active_internal_user_id'
    ]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")
    
    float_columns = ['public_mean_character_count', 'public_mean_word_count']
    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    timestamp_columns = [
        'conversation_created_at', 'updated_at', 'closed_at', 
        'last_reply_at', 'imported_at', 'deleted_at'
    ]
    for col in timestamp_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    df['conversation_created_at_date'] = pd.to_datetime(
        df['conversation_created_at_date'], errors='coerce'
    ).dt.date
    
    df['is_closed'] = df['is_closed'].astype(bool)
    
    string_columns = ['external_ticket_id', 'channel', 'language', 'klaus_sentiment']
    for col in string_columns:
        df[col] = df[col].astype(str).fillna('')
    
    print("Data Types After Preprocessing:")
    print(df.dtypes)
    
    return df

def load_conversations(df: pd.DataFrame, table_id: str):
    load_job = client.load_table_from_dataframe(df, table_id)
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")

def main():
    csv_file_path = r"C:\Users\noffel\klaus-bigquery-ingestion\data\conversations_test - conversations.csv"
    table_id = "klaus-data-model.klaus_dataset.Conversations"
    
    df_conversations = preprocess_conversations(csv_file_path)
    load_conversations(df_conversations, table_id)

if __name__ == "__main__":
    main()