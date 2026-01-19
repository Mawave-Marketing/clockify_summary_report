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
from utils import get_clockify_headers, upload_to_gcs, load_to_bigquery

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def parse_summary_json(data, date):
    """
    Parse the nested JSON structure from Clockify summary report.
    The structure has groups defined as: USER, PROJECT, TAG
    Returns a list of flattened records with entity IDs.
    """
    records = []

    # The response has a 'groupOne' array with nested children
    # groupOne = USER, groupTwo = PROJECT (children), groupThree = TAG (children of children)
    group_one = data.get('groupOne', [])

    for user_group in group_one:
        user_id = user_group.get('_id')
        user_name = user_group.get('name', '')

        # Get project groups (children of user)
        project_groups = user_group.get('children', [])

        for project_group in project_groups:
            project_id = project_group.get('_id')
            project_name = project_group.get('name', '')

            # Get client info from project if available
            client_id = project_group.get('clientId')
            client_name = project_group.get('clientName', '')

            # Get tag groups (children of project)
            tag_groups = project_group.get('children', [])

            if tag_groups:
                for tag_group in tag_groups:
                    tag_id = tag_group.get('_id')
                    tag_name = tag_group.get('name', '')
                    duration = tag_group.get('duration', 0)
                    amount = tag_group.get('amount', 0)

                    records.append({
                        'user_id': user_id,
                        'user': user_name,
                        'project_id': project_id,
                        'project': project_name,
                        'client_id': client_id,
                        'client': client_name,
                        'tag_id': tag_id,
                        'tags': tag_name,
                        'duration_ms': duration,
                        'time_decimal': duration / 3600000 if duration else 0,  # Convert ms to hours
                        'amount_eur': amount,
                        'date': date
                    })
            else:
                # No tags, create record without tag info
                duration = project_group.get('duration', 0)
                amount = project_group.get('amount', 0)

                records.append({
                    'user_id': user_id,
                    'user': user_name,
                    'project_id': project_id,
                    'project': project_name,
                    'client_id': client_id,
                    'client': client_name,
                    'tag_id': None,
                    'tags': '',
                    'duration_ms': duration,
                    'time_decimal': duration / 3600000 if duration else 0,
                    'amount_eur': amount,
                    'date': date
                })

    return records

def request_summary_report(start_date, end_date, output_dir):
    """Request Clockify report for a date range and return JSON data"""
    workspace_id = os.environ.get('CLOCKIFY_WORKSPACE_ID')
    base_url = f'https://reports.api.clockify.me/v1/workspaces/{workspace_id}/reports/summary'

    payload = {
        "amountShown": "EARNED",
        "dateRangeStart": start_date.isoformat() + "Z",
        "dateRangeEnd": end_date.isoformat() + "Z",
        "dateRangeType": "ABSOLUTE",
        "exportType": "JSON",
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
        filename = f"clockify_summary_report_{start_date.strftime('%Y-%m-%d')}.json"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w") as file:
            json.dump(response.json(), file)
        logging.info(f"Report downloaded successfully for {start_date.strftime('%Y-%m-%d')}")
        return filepath
    else:
        logging.error(f"Failed to retrieve report: {response.status_code}, {response.text}")
        return None

def process_batch(batch_data, batch_number, project_id, bucket_name):
    """Process and upload a batch of data to BigQuery"""
    if not batch_data:
        logging.info(f"Batch {batch_number}: No data to process")
        return 0

    # Create DataFrame from records
    batch_df = pd.DataFrame(batch_data)

    # Ensure numeric columns are proper type
    for col in ['time_decimal', 'amount_eur', 'duration_ms']:
        if col in batch_df.columns:
            batch_df[col] = pd.to_numeric(batch_df[col], errors='coerce')

    # Upload to GCS with batch number in filename
    filename = f"clockify_summary_batch_{batch_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    gcs_uri = upload_to_gcs(batch_df, filename, bucket_name, project_id)

    # Define schema for BigQuery
    schema = [
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("user", "STRING"),
        bigquery.SchemaField("project_id", "STRING"),
        bigquery.SchemaField("project", "STRING"),
        bigquery.SchemaField("client_id", "STRING"),
        bigquery.SchemaField("client", "STRING"),
        bigquery.SchemaField("tag_id", "STRING"),
        bigquery.SchemaField("tags", "STRING"),
        bigquery.SchemaField("duration_ms", "INTEGER"),
        bigquery.SchemaField("time_decimal", "FLOAT"),
        bigquery.SchemaField("amount_eur", "FLOAT"),
        bigquery.SchemaField("date", "DATE"),
    ]

    # Load to BigQuery
    table_id = f'{project_id}.dl_clockify.summary_time_entry_report'
    temp_table_id = f'{project_id}.dl_clockify.temp_summary_time_entry_report_{batch_number}'
    merge_keys = ['user_id', 'project_id', 'client', 'tags', 'date']

    num_rows = load_to_bigquery(gcs_uri, table_id, temp_table_id, schema, project_id, merge_keys)

    logging.info(f"Batch {batch_number}: Processed {len(batch_df)} records. Table now has {num_rows} total rows")
    return len(batch_df)

def process_summary_report():
    """Process Clockify summary report data and upload to BigQuery in batches"""
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

        # Calculate date range - can now safely use 52 weeks with batch processing
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(weeks=26)
        logging.info(f"Fetching data from {start_date} to {end_date}")

        # Batch configuration
        BATCH_SIZE_WEEKS = 4  # Process 4 weeks at a time
        batch_data = []
        batch_number = 1
        total_records = 0
        batch_start_date = start_date

        # Download and process data day by day, uploading in batches
        current_date = start_date
        while current_date < end_date:
            next_day = current_date + timedelta(days=1)
            json_file = request_summary_report(current_date, next_day, output_dir)

            if json_file:
                logging.info(f"Processing JSON file: {json_file}")
                with open(json_file, 'r') as f:
                    data = json.load(f)

                # Parse JSON and flatten the nested structure
                records = parse_summary_json(data, current_date.date())
                if records:
                    batch_data.extend(records)

            current_date = next_day
            time.sleep(1)  # Rate limiting

            # Check if we've completed a batch (every BATCH_SIZE_WEEKS weeks)
            weeks_processed = (current_date - batch_start_date).days / 7
            if weeks_processed >= BATCH_SIZE_WEEKS or current_date >= end_date:
                # Process and upload this batch
                records_processed = process_batch(batch_data, batch_number, project_id, bucket_name)
                total_records += records_processed

                # Reset for next batch
                batch_data = []
                batch_number += 1
                batch_start_date = current_date

        # Process any remaining data in the final batch
        if batch_data:
            records_processed = process_batch(batch_data, batch_number, project_id, bucket_name)
            total_records += records_processed

        result = f"Updated summary report data for date range {start_date.date()} to {end_date.date()}. Processed {total_records} total records in {batch_number} batches"
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