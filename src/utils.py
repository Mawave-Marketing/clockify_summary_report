# utils.py - Shared utilities for all Clockify functions

import json
from google.cloud import bigquery
from google.cloud import storage
import os
import logging
from datetime import datetime
import pandas as pd
import tempfile
import csv
import re
import requests
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def clean_column_name(name):
    cleaned = re.sub(r'[^\w\s]', '', name)
    cleaned = re.sub(r'\s+', '_', cleaned).lower()
    return cleaned

def detect_delimiter(file_path):
    with open(file_path, 'r', newline='') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        return dialect.delimiter

def get_clockify_headers():
    """Get the headers for Clockify API requests"""
    api_key = os.environ.get('CLOCKIFY_API_KEY')
    return {
        'X-Api-Key': api_key,
        'Content-Type': 'application/json'
    }

def upload_to_gcs(df, filename, bucket_name, project_id, folder='clockify_data'):
    """Upload a DataFrame to Google Cloud Storage with retry logic"""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # Create temporary directory
    output_dir = tempfile.mkdtemp()
    parquet_file = os.path.join(output_dir, filename)

    # Save to parquet
    df.to_parquet(parquet_file, index=False)
    logging.info(f"Saved to parquet: {parquet_file}")

    # Upload to GCS with retry logic
    today = datetime.now().strftime('%Y-%m-%d')
    blob_name = f'{folder}/{today}/{filename}'
    blob = bucket.blob(blob_name)

    # Retry configuration
    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            blob.upload_from_filename(parquet_file)
            gcs_uri = f"gs://{bucket_name}/{blob_name}"
            logging.info(f"Uploaded to GCS: {gcs_uri}")
            return gcs_uri
        except Exception as e:
            error_msg = str(e)
            if attempt < max_retries - 1 and ('SSL' in error_msg or 'Connection' in error_msg):
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                logging.warning(f"Upload attempt {attempt + 1} failed with {type(e).__name__}: {error_msg}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"Upload failed after {attempt + 1} attempts: {error_msg}")
                raise

def load_to_bigquery(gcs_uri, table_id, temp_table_id, schema, project_id, merge_keys):
    """Load data from GCS to BigQuery and merge with existing data"""
    client = bigquery.Client(project=project_id)
    
    # First load new data into a temporary table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    load_job = client.load_table_from_uri(
        gcs_uri,
        temp_table_id,
        job_config=job_config
    )
    load_job.result()
    logging.info("Loaded data into temporary table")
    
    # Check if main table exists, if not create it
    try:
        client.get_table(table_id)
    except Exception as e:
        logging.info(f"Main table does not exist, creating it")
        # Create the main table with the same schema
        main_table = bigquery.Table(table_id, schema=job_config.schema)
        client.create_table(main_table)
    
    # Build the ON clause for merging
    on_clause = " AND ".join([f"T.{key} = S.{key}" for key in merge_keys])
    
    # Build the column list for updates and inserts
    all_columns = [field.name for field in schema]
    update_set = ", ".join([f"{col} = S.{col}" for col in all_columns if col not in merge_keys])
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join([col for col in all_columns])
    
    # Perform merge operation
    merge_query = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S
    ON {on_clause}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({insert_columns})
        VALUES({insert_values})
    """
    
    merge_job = client.query(merge_query)
    merge_job.result()
    logging.info("Completed merge operation")
    
    # Clean up temporary table
    client.delete_table(temp_table_id, not_found_ok=True)
    logging.info("Cleaned up temporary table")
    
    # Get final row count
    table = client.get_table(table_id)
    return table.num_rows