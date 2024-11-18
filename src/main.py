import json
from google.cloud import bigquery
from google.cloud import storage
import os
import logging
from datetime import datetime, timedelta, date
import pandas as pd
import tempfile
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def clockify_to_bigquery():
    """
    Core function to import data from Clockify to BigQuery via GCS
    Returns:
        str: Operation results
    """
    try:
        logging.info("Starting clockify_to_bigquery function")
        
        # Get configuration from environment variables
        api_key = os.environ.get('CLOCKIFY_API_KEY')
        workspace_id = os.environ.get('CLOCKIFY_WORKSPACE_ID')
        project_id = os.environ.get('GCP_PROJECT_ID')
        bucket_name = os.environ.get('GCS_BUCKET_NAME')

        if not all([api_key, workspace_id, project_id, bucket_name]):
            raise Exception("Missing required environment variables")

        # Initialize clients
        logging.info("Initializing clients")
        bigquery_client = bigquery.Client(project=project_id)
        storage_client = storage.Client(project=project_id)
        
        # Process Clockify data
        start_date = datetime(2024, 1, 1)
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        all_data = []
        base_url = f'https://reports.api.clockify.me/v1/workspaces/{workspace_id}/reports/summary'
        
        current_date = start_date
        while current_date < end_date:
            next_day = current_date + timedelta(days=1)
            
            payload = {
                "amountShown": "EARNED",
                "dateRangeStart": current_date.isoformat() + "Z",
                "dateRangeEnd": next_day.isoformat() + "Z",
                "dateRangeType": "ABSOLUTE",
                "exportType": "CSV",
                "summaryFilter": {
                    "groups": ["USER", "PROJECT"]
                }
            }
            
            headers = {
                'X-Api-Key': api_key,
                'Content-Type': 'application/json'
            }
            
            response = requests.post(base_url, headers=headers, data=json.dumps(payload))
            
            if response.status_code == 200:
                df = pd.read_csv(pd.io.common.BytesIO(response.content))
                df['date'] = current_date.date()
                all_data.append(df)
                logging.info(f"Retrieved data for {current_date.date()}")
            else:
                logging.error(f"Failed to retrieve data for {current_date.date()}: {response.status_code}")
            
            current_date = next_day
        
        if not all_data:
            raise Exception("No data retrieved from Clockify")
            
        # Process the combined data
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
        
        # Save to GCS
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        parquet_filename = f"clockify_data/{timestamp}/combined_clockify_report.parquet"
        
        with tempfile.NamedTemporaryFile() as tmp:
            combined_df.to_parquet(tmp.name, index=False)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(parquet_filename)
            blob.upload_from_filename(tmp.name)
            
        gcs_uri = f"gs://{bucket_name}/{parquet_filename}"
        logging.info(f"Uploaded data to {gcs_uri}")
        
        # Load to BigQuery
        table_id = f'{project_id}.dl_clockify.summary_time_entry_report'
        
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
                bigquery.SchemaField("date", "DATE"),
            ]
        )
        
        load_job = bigquery_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        
        load_job.result()
        logging.info("BigQuery load completed")
        
        table = bigquery_client.get_table(table_id)
        result = f"Loaded {table.num_rows} rows into {table_id}"
        logging.info(result)
        
        return result
        
    except Exception as e:
        logging.error(f"Error in clockify_to_bigquery: {str(e)}")
        raise e

def main(request):
    """
    Main entry point for the Cloud Function.
    Args:
        request: The request object from Cloud Functions
    Returns:
        str: Operation results or error message
    """
    start_time = datetime.now()
    logging.info(f"Starting import job at {start_time}")
    
    try:
        results = clockify_to_bigquery()
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Import completed in {duration}")
        
        return json.dumps({
            'status': 'success',
            'message': results,
            'duration': str(duration)
        })
        
    except Exception as e:
        logging.error(f"Failed to import data: {str(e)}")
        return json.dumps({
            'status': 'error',
            'message': str(e)
        }), 500