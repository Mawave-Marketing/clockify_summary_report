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

def fetch_clockify_users():
    """Fetch all users from Clockify API"""
    workspace_id = os.environ.get('CLOCKIFY_WORKSPACE_ID')
    headers = get_clockify_headers()
    base_url = f'https://api.clockify.me/api/v1/workspaces/{workspace_id}/users'
    
    all_users = []
    page = 1
    page_size = 50
    
    while True:
        url = f"{base_url}?page={page}&page-size={page_size}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            logging.error(f"Failed to retrieve users: {response.status_code}, {response.text}")
            break
        
        users = response.json()
        if not users:
            break
            
        all_users.extend(users)
        page += 1
        
        # Rate limiting
        time.sleep(1)
    
    # Convert to DataFrame with flattened memberships
    if all_users:
        # First create a DataFrame from the basic user data without memberships
        user_dfs = []
        for user in all_users:
            # Extract and remove memberships to handle separately
            memberships = user.pop('memberships', [])
            
            # Create a base user row
            user_df = pd.DataFrame([user])
            
            # Process memberships if they exist
            if memberships:
                # Get the first membership (most relevant for workspace)
                membership = memberships[0]
                # Flatten the membership data with prefixes
                for key, value in membership.items():
                    # Handle nested structures like hourlyRate
                    if isinstance(value, dict):
                        for subkey, subvalue in value.items():
                            user_df[f"membership_{key}_{subkey}"] = subvalue
                    else:
                        user_df[f"membership_{key}"] = value
            
            user_dfs.append(user_df)
        
        # Combine all user DataFrames
        df = pd.concat(user_dfs, ignore_index=True)
        
        # Add timestamp
        df['import_timestamp'] = datetime.now()
        return df
    else:
        return pd.DataFrame()

def process_users():
    """Process Clockify users data and upload to BigQuery"""
    try:
        logging.info("Starting users processing")
        
        # Get configuration
        project_id = os.environ.get('GCP_PROJECT_ID')
        bucket_name = os.environ.get('GCS_BUCKET_NAME')

        if not all([project_id, bucket_name]):
            raise Exception("Missing required environment variables")
        
        # Fetch users data
        users_df = fetch_clockify_users()
        
        if users_df.empty:
            logging.info("No users data retrieved")
            return "No users data to process"
        
        # Upload to GCS
        gcs_uri = upload_to_gcs(
            users_df, 
            "clockify_users.parquet", 
            bucket_name, 
            project_id
        )
        
        # Define schema for BigQuery based on the actual columns in the DataFrame
        schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("profilePicture", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("activeWorkspace", "STRING"),
            bigquery.SchemaField("defaultWorkspace", "STRING"),
            bigquery.SchemaField("settings_weekStart", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("settings_timeZone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("settings_dateFormat", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("settings_timeFormat", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("settings_sendNewsletter", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("settings_weeklyUpdates", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("settings_longRunning", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("settings_scheduledReports", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("settings_approval", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("settings_pto", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("settings_alerts", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("settings_onboarding", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("settings_projectPickerSpecialFilter", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("membership_hourlyRate_amount", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("membership_hourlyRate_currency", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("membership_membershipStatus", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("membership_membershipType", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("membership_targetId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("import_timestamp", "TIMESTAMP"),
        ]
        
        # Load to BigQuery
        table_id = f'{project_id}.dl_clockify.users'
        temp_table_id = f'{project_id}.dl_clockify.temp_users'
        merge_keys = ['id']
        
        num_rows = load_to_bigquery(
            gcs_uri,
            table_id,
            temp_table_id,
            schema,
            project_id,
            merge_keys
        )
        
        result = f"Updated users data. Table now has {num_rows} total rows"
        logging.info(result)
        
        return result
        
    except Exception as e:
        logging.error(f"Error in users processing: {str(e)}")
        raise e

def main(event, context):
    """Cloud Function entry point"""
    start_time = datetime.now()
    logging.info(f"Starting users import job at {start_time}")
    
    try:
        results = process_users()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Users import completed in {duration}")
        
        return results
        
    except Exception as e:
        logging.error(f"Failed to import users data: {str(e)}")
        raise  # Raise the exception to indicate failure to Pub/Sub