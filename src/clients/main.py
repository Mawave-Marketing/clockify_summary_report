import json
from google.cloud import bigquery
import os
import logging
from datetime import datetime
import pandas as pd
import time
import requests
import base64

# Import shared utilities
from utils import get_clockify_headers, upload_to_gcs, load_to_bigquery

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_clockify_clients():
    """Fetch all clients from Clockify API"""
    workspace_id = os.environ.get('CLOCKIFY_WORKSPACE_ID')
    headers = get_clockify_headers()
    base_url = f'https://api.clockify.me/api/v1/workspaces/{workspace_id}/clients'
    
    all_clients = []
    page = 1
    page_size = 50
    
    while True:
        url = f"{base_url}?page={page}&page-size={page_size}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logging.error(f"Failed to retrieve clients: {response.status_code}, {response.text}")
            break
        
        clients = response.json()
        if not clients:
            break
            
        all_clients.extend(clients)
        page += 1
        
        # Rate limiting
        time.sleep(1)
    
    # Convert to DataFrame
    if all_clients:
        df = pd.DataFrame(all_clients)
        # Add timestamp
        df['import_timestamp'] = datetime.now()
        return df
    else:
        return pd.DataFrame()

def process_clients():
    """Process Clockify clients data and upload to BigQuery"""
    try:
        logging.info("Starting clients processing")
        
        # Get configuration
        project_id = os.environ.get('GCP_PROJECT_ID')
        bucket_name = os.environ.get('GCS_BUCKET_NAME')

        if not all([project_id, bucket_name]):
            raise Exception("Missing required environment variables")
        
        # Fetch clients data
        clients_df = fetch_clockify_clients()
        
        if clients_df.empty:
            logging.info("No clients data retrieved")
            return "No clients data to process"
        
        # Upload to GCS
        gcs_uri = upload_to_gcs(
            clients_df, 
            "clockify_clients.parquet", 
            bucket_name, 
            project_id
        )
        
        # Define schema for BigQuery
        schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("workspaceId", "STRING"),
            bigquery.SchemaField("archived", "BOOLEAN"),
            # Note: Some fields might be nullable
            bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("import_timestamp", "TIMESTAMP"),
        ]
        
        # Load to BigQuery
        table_id = f'{project_id}.dl_clockify.clients'
        temp_table_id = f'{project_id}.dl_clockify.temp_clients'
        merge_keys = ['id']
        
        num_rows = load_to_bigquery(
            gcs_uri,
            table_id,
            temp_table_id,
            schema,
            project_id,
            merge_keys
        )
        
        result = f"Updated clients data. Table now has {num_rows} total rows"
        logging.info(result)
        
        return result
        
    except Exception as e:
        logging.error(f"Error in clients processing: {str(e)}")
        raise e

def main(event, context):
    """Cloud Function entry point"""
    start_time = datetime.now()
    logging.info(f"Starting clients import job at {start_time}")
    
    try:
        results = process_clients()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Clients import completed in {duration}")
        
        return results
        
    except Exception as e:
        logging.error(f"Failed to import clients data: {str(e)}")
        raise  # Raise the exception to indicate failure to Pub/Sub