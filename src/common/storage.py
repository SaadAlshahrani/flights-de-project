import json
import pandas as pd
from io import BytesIO
from google.cloud import storage
from decouple import config

BUCKET_NAME = config("GCS_BUCKET_NAME")
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)


def write_json(blob_path: str, payload: dict):
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(payload),
        content_type="application/json"
    )
    

def load_json(blob_path: str) -> dict:
    blob = bucket.blob(blob_path)
    return json.loads(blob.download_as_text())


def write_parquet(blob_path: str, df: pd.DataFrame):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob = bucket.blob(blob_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")