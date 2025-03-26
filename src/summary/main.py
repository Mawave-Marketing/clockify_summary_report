import json
from google.cloud import bigquery
import logging
from datetime import datetime, timedelta
import pandas as pd
import tempfile
import csv
import time
import requests
import base64

# Import shared utilities
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import clean_column_name, detect_delimiter, get_clockify_headers, upload_to_gcs, load_to_bigquery

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def request_summary_report(start_date, end_date, output_dir):
    """Request Clockify report for a date range and save to file"""
    workspace_id = os.environ.get('CLOCKIFY_WORKSPACE_ID')
    base_url = f'https://reports.api.clockify.me/v1/workspaces/{workspace_id}/reports/summary'
    
    payload = {
        "amountShown": "EARNED",
        "dateRangeStart": start_date.isoformat() + "Z",
        "dateRangeEnd": end_date.isoformat() + "Z",
        "dateRangeType": "ABSOLUTE",
        "exportType": "CSV",
        "summaryFilter": {
            "groups": ["USER", "PROJECT", "TAG"]
        },
        "tags": {
            "containedInTimeentry": "CONTAINS",
            "contains": "CONTAINS",
            "ids": [],
            "status": "ACTIVE"
        }
    }
    
    headers = get_clockify_headers()
    response = requests.post(base_url, headers=headers, data=json.dumps(payload))
    
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

def process_summary_report():
    """Process Clockify summary report data and upload to BigQuery"""
    try:
        logging.info("Starting summary report processing")
        
        # Get configuration
        project_id = os.environ.get('GCP_PROJECT_ID')
        bucket_name = os.environ.get('GCS_BUCKET_NAME')

        if not all([project_id, bucket_name]):
            raise Exception("Missing required environment variables")

        # Create temporary directory
        output_dir = tempfile.mkdtemp()
        logging.info(f"Created temporary directory: {output_dir}")
        
        # Calculate date range for last 8 weeks
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(weeks=8)
        logging.info(f"Fetching data from {start_date} to {end_date}")
        
        all_data = []

        # Download all data day by day
        current_date = start_date
        while current_date < end_date:
            next_day = current_date + timedelta(days=1)
            csv_file = request_summary_report(current_date, next_day, output_dir)
            
            if csv_file:
                logging.info(f"Processing CSV file: {csv_file}")
                delimiter = detect_delimiter(csv_file)
                df = pd.read_csv(csv_file, sep=delimiter, encoding='utf-8')
                df['date'] = current_date.date()
                all_data.append(df)
            
            current_date = next_day
            time.sleep(1)  # Rate limiting
            
        if not all_data:
            raise Exception("No data collected")
            
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Clean and rename columns
        combined_df.columns = [clean_column_name(col) for col in combined_df.columns]
        column_mapping = {
            'benutzer': 'user',
            'projekt': 'project',
            'kunde': 'client',
            'tag': 'tags',
            'zeit_h': 'time_hours',
            'zeit_dezimal': 'time_decimal',
            'betrag_eur': 'amount_eur'
        }
        
        combined_df.rename(columns=column_mapping, inplace=True)
        
        # Convert numeric columns
        for col in ['time_decimal', 'amount_eur']:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
        
        # Upload to GCS
        gcs_uri = upload_to_gcs(
            combined_df, 
            "combined_clockify_report.parquet", 
            bucket_name, 
            project_id
        )
        
        # Define schema for BigQuery
        schema = [
            bigquery.SchemaField("user", "STRING"),
            bigquery.SchemaField("project", "STRING"),
            bigquery.SchemaField("client", "STRING"),
            bigquery.SchemaField("tags", "STRING"),
            bigquery.SchemaField("time_hours", "STRING"),
            bigquery.SchemaField("time_decimal", "FLOAT"),
            bigquery.SchemaField("amount_eur", "FLOAT"),
            bigquery.SchemaField("date", "DATE"),
        ]
        
        # Load to BigQuery
        table_id = f'{project_id}.dl_clockify.summary_time_entry_report'
        temp_table_id = f'{project_id}.dl_clockify.temp_summary_time_entry_report'
        merge_keys = ['date', 'user', 'project', 'tags']
        
        num_rows = load_to_bigquery(
            gcs_uri,
            table_id,
            temp_table_id,
            schema,
            project_id,
            merge_keys
        )
        
        result = f"Updated summary report data for date range {start_date.date()} to {end_date.date()}. Table now has {num_rows} total rows"
        logging.info(result)
        
        return result
        
    except Exception as e:
        logging.error(f"Error in summary report processing: {str(e)}")
        raise e

def main(event, context):
    """Cloud Function entry point"""
    start_time = datetime.now()
    logging.info(f"Starting summary report import job at {start_time}")
    
    try:
        results = process_summary_report()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Summary report import completed in {duration}")
        
        return results
        
    except Exception as e:
        logging.error(f"Failed to import summary report data: {str(e)}")
        raise  # Raise the exception to indicate failure to Pub/Sub