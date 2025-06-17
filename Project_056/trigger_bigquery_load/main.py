import logging
from google.cloud import bigquery
import os

def trigger_bigquery_load(event, context):
    bucket_name = event['bucket']
    file_path = event['name']
    
    logging.info(f"File uploaded: gs://{bucket_name}/{file_path}")
    
    # Get folder prefix from the uploaded file (e.g., 'post_process/all/')
    folder_prefix = os.path.dirname(file_path)

    # Reconstruct wildcard URI to load all files in the folder
    gcs_uri = f"gs://{bucket_name}/{folder_prefix}/*.avro"

    # Determine table name based on folder
    folder_to_table = {
        'post_process/all': 'all',
        'post_process/ip_location': 'ip_location',
        'post_process/product': 'product',
    }

    table_id = folder_to_table.get(folder_prefix)
    if not table_id:
        logging.warning(f"No table mapping found for folder: {folder_prefix}")
        return

    # Load into BigQuery
    bq_client = bigquery.Client()
    table_ref = f"dataengineeringjourney-458915.glamira_post_process.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # overwrite
    )

    try:
        load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()
        logging.info(f"✅ Loaded all files from {folder_prefix} into {table_ref}")
    except Exception as e:
        logging.error(f"❌ Load failed: {e}")
