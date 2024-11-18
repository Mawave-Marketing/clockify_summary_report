import functions_framework
import json
import pandas as pd
import os
from datetime import datetime, timedelta, date
import time
import tempfile
import csv
import re
from google.cloud import storage
from google.cloud import bigquery
import requests
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration from environment variables
API_KEY = os.environ.get('CLOCKIFY_API_KEY')
WORKSPACE_ID = os.environ.get('CLOCKIFY_WORKSPACE_ID')
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME')
BASE_URL = f'https://reports.api.clockify.me/v1/workspaces/{WORKSPACE_ID}/reports/summary'

def request_summary_report(start_date, end_date, output_dir):
    """Request Clockify report for a date range"""
    payload = {
        "amountShown": "EARNED",
        "dateRangeStart": start_date.isoformat() + "Z",
        "dateRangeEnd": end_date.isoformat() + "Z",
        "dateRangeType": "ABSOLUTE",
        "exportType": "CSV",
        "summaryFilter": {
            "groups": ["USER", "PROJECT"]
        }
    }
    headers = {
        'X-Api-Key': API_KEY,
        'Content-Type': 'application/json'
    }
    
    response = requests.post(BASE_URL, headers=headers, data=json.dumps(payload))
    
    if response.status_code == 200:
        filename = f"clockify_summary_report_{start_date.strftime('%Y-%m-%d')}.csv"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "wb") as file:
            file.write(response.content)
        logging.info(f"Report downloaded successfully for {start_date.strftime('%Y-%m-%d')}")
        return filepath
    else:
        logging.error(f"Failed to retrieve report: {response.status_code}, {response.text}")
        return None

def process_clockify_data(tmp_dir):
    """Process Clockify data and return DataFrame"""
    start_date = datetime(2024, 1, 1)
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    csv_files = []
    
    current_date = start_date
    while current_date < end_date:
        next_day = current_date + timedelta(days=1)
        csv_file = request_summary_report(current_date, next_day, tmp_dir)
        if csv_file:
            csv_files.append((csv_file, current_date))
        current_date = next_day
        time.sleep(2)
    
    all_data = []
    for file, file_date in csv_files:
        df = pd.read_csv(file, sep=',', encoding='utf-8')
        df['date'] = (file_date.date() - date(1970, 1, 1)).days
        all_data.append(df)
    
    if not all_data:
        return None
        
    combined_df = pd.concat(all_data, ignore_index=True)
    
    column_mapping = {
        'benutzer': 'user',
        'projekt': 'project',
        'kunde': 'client',
        'zeit_h': 'time_hours',
        'zeit_dezimal': 'time_decimal',
        'betrag_eur': 'amount_eur'
    }
    combined_df.rename(columns=column_mapping, inplace=True)
    
    combined_df['time_decimal'] = pd.to_numeric(combined_df['time_decimal'], errors='coerce')
    combined_df['amount_eur'] = pd.to_numeric(combined_df['amount_eur'], errors='coerce')
    combined_df['date'] = combined_df['date'].astype(int)
    
    return combined_df

@functions_framework.http
def main(request):
    """
    Main entry point for the Cloud Function.
    Args:
        request: The request object from Cloud Functions
    Returns:
        str: Operation results or error message
    """
    start_time = datetime.now()
    logging.info(f"Starting Clockify import job at {start_time}")
    
    try:
        # Create temporary directory
        tmp_dir = tempfile.mkdtemp()
        
        # Process the data
        df = process_clockify_data(tmp_dir)
        if df is None:
            return json.dumps({
                'status': 'error',
                'message': 'No data processed'
            }), 400
        
        # Save to parquet
        parquet_file = os.path.join(tmp_dir, "combined_clockify_report.parquet")
        df.to_parquet(parquet_file, index=False)
        
        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        today = datetime.now().strftime('%Y-%m-%d')
        blob_name = f'clockify_data/{today}/combined_clockify_report.parquet'
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(parquet_file)
        logging.info(f"Uploaded to GCS: {blob_name}")
        
        # Load to BigQuery
        client = bigquery.Client()
        table_id = f'{PROJECT_ID}.dl_clockify.summary_time_entry_report'
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("user", "STRING"),
                bigquery.SchemaField("project", "STRING"),
                bigquery.SchemaField("client", "STRING"),
                bigquery.SchemaField("time_hours", "STRING"),
                bigquery.SchemaField("time_decimal", "FLOAT"),
                bigquery.SchemaField("amount_eur", "FLOAT"),
                bigquery.SchemaField("date", "INTEGER"),
            ]
        )
        
        uri = f"gs://{BUCKET_NAME}/{blob_name}"
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()
        logging.info("BigQuery load completed")
        
        # Convert date format
        query = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.dl_clockify.summary_time_entry_report` AS
        SELECT
            user,
            project,
            client,
            time_hours,
            time_decimal,
            amount_eur,
            DATE_ADD('1970-01-01', INTERVAL date DAY) AS date
        FROM `{PROJECT_ID}.dl_clockify.summary_time_entry_report`
        """
        
        query_job = client.query(query)
        query_job.result()
        logging.info("Date conversion completed")
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Import completed in {duration}")
        
        return json.dumps({
            'status': 'success',
            'message': f"Data processing completed successfully in {duration}",
            'duration': str(duration)
        })
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return json.dumps({
            'status': 'error',
            'message': str(e)
        }), 500