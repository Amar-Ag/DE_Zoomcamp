import requests
import gzip
from google.cloud import storage, bigquery
from io import BytesIO
import os

from google.oauth2 import service_account
import json
import base64

# Attempt to load service account JSON from env vars. Priority:
# 1) GCP_SA_KEY (raw JSON)
# 2) GCP_SA_KEY_B64 (base64-encoded JSON)
# If present, create credentials and use them. Otherwise fall back to Application Default Credentials (ADC).
key_raw = os.getenv("GCP_SA_KEY")
key_b64 = os.getenv("GCP_SA_KEY_B64")
credentials = None
project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "kestrademo-485701")

if key_raw:
    try:
        info = json.loads(key_raw)
        credentials = service_account.Credentials.from_service_account_info(info)
        project_id = info.get("project_id", project_id)
    except Exception as e:
        print("Failed to parse GCP_SA_KEY (raw JSON):", e)
        raise
elif key_b64:
    try:
        info = json.loads(base64.b64decode(key_b64))
        credentials = service_account.Credentials.from_service_account_info(info)
        project_id = info.get("project_id", project_id)
    except Exception as e:
        print("Failed to parse GCP_SA_KEY_B64:", e)
        raise

# Change this to your bucket name
BUCKET_NAME = "aa10-kestra"


# Configuration
# PROJECT_ID = 'kestrademo-485701'
BUCKET_NAME = 'aa10-kestra'  # Change this to your bucket
DATASET_ID = 'zoomcamp'
TABLE_NAME = 'fhv_tripdata_2019'
YEAR = 2019

# Initialize clients
storage.Client(project=project_id, credentials=credentials)

storage_client = storage.Client(project=project_id, credentials=credentials)
bq_client = bigquery.Client(project=project_id, credentials=credentials)

# Create bucket if it doesn't exist
try:
    bucket = storage_client.get_bucket(BUCKET_NAME)
    print(f"Using existing bucket: {BUCKET_NAME}")
except:
    bucket = storage_client.create_bucket(BUCKET_NAME, location='US')
    print(f"Created bucket: {BUCKET_NAME}")

# Download and upload each month
base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02d}.csv.gz"

print("Downloading and uploading files to GCS...")
for month in range(1, 13):
    url = base_url.format(year=YEAR, month=month)
    gcs_path = f"fhv/{YEAR}/fhv_tripdata_{YEAR}-{month:02d}.csv.gz"
    
    try:
        print(f"Downloading month {month:02d}... ", end='')
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        # Upload to GCS
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(response.content, content_type='application/gzip')
        print(f"✓ Uploaded to gs://{BUCKET_NAME}/{gcs_path}")
        
    except Exception as e:
        print(f"✗ Failed: {e}")

# Load into BigQuery
print("\nLoading data into BigQuery...")
table_id = f"{project_id}.{DATASET_ID}.{TABLE_NAME}"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

uri = f"gs://{BUCKET_NAME}/fhv/{YEAR}/*.csv.gz"

load_job = bq_client.load_table_from_uri(
    uri,
    table_id,
    job_config=job_config
)

load_job.result()  # Wait for the job to complete

# Verify
table = bq_client.get_table(table_id)
print(f"\n✓ Loaded {table.num_rows:,} rows into {table_id}")
print(f"Table size: {table.num_bytes / (1024**3):.2f} GB")

# Create partitioned and clustered version (optional)
print("\nCreating partitioned and clustered table...")
partitioned_table_id = f"{table_id}_partitioned_clustered"

query = f"""
CREATE OR REPLACE TABLE `{partitioned_table_id}`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num
AS SELECT * FROM `{table_id}`
"""

query_job = bq_client.query(query)
query_job.result()
print(f"✓ Created partitioned table: {partitioned_table_id}")