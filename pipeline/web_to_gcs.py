import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

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

# Initialize client using provided credentials or ADC
try:
    if credentials:
        client = storage.Client(project=project_id, credentials=credentials)
    else:
        client = storage.Client(project=project_id)
except Exception as e:
    print(
        "Google Cloud credentials not found. Provide `GCP_SA_KEY_B64` (base64-encoded service account JSON), "
        "or set `GOOGLE_APPLICATION_CREDENTIALS`, or run `gcloud auth application-default login` to set ADC."
    )
    raise


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")