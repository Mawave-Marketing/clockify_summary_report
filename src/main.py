import json
from google.cloud import bigquery
from google.cloud import storage
import os
import logging
from datetime import datetime, timedelta, date
import pandas as pd
import tempfile
import csv
import re
import requests
import time
import base64

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

def request_summary_report(start_date, end_date, output_dir):
    """Request Clockify report for a date range and save to file"""
    api_key = os.environ.get('CLOCKIFY_API_KEY')
    workspace_id = os.environ.get('CLOCKIFY_WORKSPACE_ID')
    base_url = f'https://reports.api.clockify.me/v1/workspaces/{workspace_id}/reports/summary'
    
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
        'X-Api-Key': api_key,
        'Content-Type': 'application/json'
    }
    
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

def clockify_to_bigquery():
    """Process Clockify data and upload to BigQuery"""
    try:
        logging.info("Starting clockify_to_bigquery function")
        
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
            'zeit_h': 'time_hours',
            'zeit_dezimal': 'time_decimal',
            'betrag_eur': 'amount_eur'
        }
        
        logging.info(f"Original columns: {combined_df.columns.tolist()}")
        combined_df.rename(columns=column_mapping, inplace=True)
        logging.info(f"Renamed columns: {combined_df.columns.tolist()}")
        
        # Convert numeric columns
        for col in ['time_decimal', 'amount_eur']:
            try:
                combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
                logging.info(f"Converted {col} to numeric")
            except Exception as e:
                logging.error(f"Error converting {col}: {str(e)}")
                raise
        
        # Save to parquet
        parquet_file = os.path.join(output_dir, "combined_clockify_report.parquet")
        combined_df.to_parquet(parquet_file, index=False)
        logging.info(f"Saved to parquet: {parquet_file}")
        
        # Upload to GCS
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        today = datetime.now().strftime('%Y-%m-%d')
        blob_name = f'clockify_data/{today}/combined_clockify_report.parquet'
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(parquet_file)
        
        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        logging.info(f"Uploaded to GCS: {gcs_uri}")
        
        # Load to BigQuery with merge operation
        client = bigquery.Client(project=project_id)
        table_id = f'{project_id}.dl_clockify.summary_time_entry_report'
        temp_table_id = f'{project_id}.dl_clockify.temp_summary_time_entry_report'
        
        # First load new data into a temporary table
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
        
        # Perform merge operation
        merge_query = f"""
        MERGE `{table_id}` T
        USING `{temp_table_id}` S
        ON T.date = S.date 
            AND T.user = S.user 
            AND T.project = S.project
        WHEN MATCHED THEN
            UPDATE SET 
                client = S.client,
                time_hours = S.time_hours,
                time_decimal = S.time_decimal,
                amount_eur = S.amount_eur
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (user, project, client, time_hours, time_decimal, amount_eur, date)
            VALUES(user, project, client, time_hours, time_decimal, amount_eur, date)
        WHEN NOT MATCHED BY SOURCE AND T.date BETWEEN '{start_date.date()}' AND '{end_date.date()}' THEN
            DELETE
        """
        
        merge_job = client.query(merge_query)
        merge_job.result()
        logging.info("Completed merge operation")
        
        # Clean up temporary table
        client.delete_table(temp_table_id, not_found_ok=True)
        logging.info("Cleaned up temporary table")
        
        # Get final row count
        table = client.get_table(table_id)
        result = f"Updated data for date range {start_date.date()} to {end_date.date()}. Table now has {table.num_rows} total rows"
        logging.info(result)
        
        return result
        
    except Exception as e:
        logging.error(f"Error in clockify_to_bigquery: {str(e)}")
        raise e

def main(event, context):
    """Cloud Function Pub/Sub entry point"""
    start_time = datetime.now()
    logging.info(f"Starting import job at {start_time}")
    
    try:
        results = clockify_to_bigquery()
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Import completed in {duration}")
        
        return 'Success'  # Simple success response for Pub/Sub
        
    except Exception as e:
        logging.error(f"Failed to import data: {str(e)}")
        raise  # Raise the exception to indicate failure to Pub/Sub