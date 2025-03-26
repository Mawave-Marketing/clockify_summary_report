import json
from google.cloud import bigquery
import logging
from datetime import datetime
import pandas as pd
import time
import requests
import base64

# Import shared utilities
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_clockify_headers, upload_to_gcs, load_to_bigquery

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_clockify_projects():
    """Fetch all projects from Clockify API"""
    workspace_id = os.environ.get('CLOCKIFY_WORKSPACE_ID')
    headers = get_clockify_headers()
    base_url = f'https://api.clockify.me/api/v1/workspaces/{workspace_id}/projects'
    
    all_projects = []
    page = 1
    page_size = 50
    
    while True:
        url = f"{base_url}?page={page}&page-size={page_size}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logging.error(f"Failed to retrieve projects: {response.status_code}, {response.text}")
            break
        
        projects = response.json()
        if not projects:
            break
            
        all_projects.extend(projects)
        page += 1
        
        # Rate limiting
        time.sleep(1)
    
    # Convert to DataFrame
    if all_projects:
        df = pd.DataFrame(all_projects)
        # Handle nested objects like memberships, estimate, timeEstimate, etc.
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                # Extract nested fields and create new columns
                nested_df = pd.json_normalize(df[col])
                nested_df.columns = [f"{col}_{subcol}" for subcol in nested_df.columns]
                for nested_col in nested_df.columns:
                    df[nested_col] = nested_df[nested_col]
                # Drop the original nested column
                df = df.drop(columns=[col])
        
        # Add timestamp
        df['import_timestamp'] = datetime.now()
        return df
    else:
        return pd.DataFrame()

def process_projects():
    """Process Clockify projects data and upload to BigQuery"""
    try:
        logging.info("Starting projects processing")
        
        # Get configuration
        project_id = os.environ.get('GCP_PROJECT_ID')
        bucket_name = os.environ.get('GCS_BUCKET_NAME')

        if not all([project_id, bucket_name]):
            raise Exception("Missing required environment variables")
        
        # Fetch projects data
        projects_df = fetch_clockify_projects()
        
        if projects_df.empty:
            logging.info("No projects data retrieved")
            return "No projects data to process"
        
        # Upload to GCS
        gcs_uri = upload_to_gcs(
            projects_df, 
            "clockify_projects.parquet", 
            bucket_name, 
            project_id
        )
        
        # Define schema for BigQuery based on the actual columns in the DataFrame
        schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("workspaceId", "STRING"),
            bigquery.SchemaField("clientId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("archived", "BOOLEAN"),
            bigquery.SchemaField("billable", "BOOLEAN"),
            bigquery.SchemaField("public", "BOOLEAN"),
            bigquery.SchemaField("color", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("hourlyRate_amount", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("hourlyRate_currency", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("estimate_estimate", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("estimate_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timeEstimate_estimate", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("timeEstimate_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("import_timestamp", "TIMESTAMP"),
        ]
        
        # Load to BigQuery
        table_id = f'{project_id}.dl_clockify.projects'
        temp_table_id = f'{project_id}.dl_clockify.temp_projects'
        merge_keys = ['id']
        
        num_rows = load_to_bigquery(
            gcs_uri,
            table_id,
            temp_table_id,
            schema,
            project_id,
            merge_keys
        )
        
        result = f"Updated projects data. Table now has {num_rows} total rows"
        logging.info(result)
        
        return result
        
    except Exception as e:
        logging.error(f"Error in projects processing: {str(e)}")
        raise e

def main(event, context):
    """Cloud Function entry point"""
    start_time = datetime.now()
    logging.info(f"Starting projects import job at {start_time}")
    
    try:
        results = process_projects()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Projects import completed in {duration}")
        
        return results
        
    except Exception as e:
        logging.error(f"Failed to import projects data: {str(e)}")
        raise  # Raise the exception to indicate failure to Pub/Sub