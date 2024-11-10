import pandas as pd
from io import StringIO
from google.cloud import storage
import logging

def upload_dataframe_to_gcs(df, bucket_name, blob_name):
  try:
    client = storage.Client()

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"DataFrame successfully uploaded to {bucket_name}/{blob_name}")
  except Exception as e:
    logging.error(f"Error uploading DataFrame to GCS: {str(e)}")
  
upload_dataframe_to_gcs("df", 'your_bucket_name', 'your_blob_name.csv')