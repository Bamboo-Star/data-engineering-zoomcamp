import requests
import pandas as pd
from google.cloud import storage


URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
CREDENTIALS_FILE = "/Users/xiaozhuxin/.google/credentials/google_credentials.json"  
BUCKET_NAME = "terraform-introduction-449023-terra-bucket"


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs_file(service, file_name):
        # download it using requests via a pandas df
        request_url = f"{URL_PREFIX}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # convert into a parquet file
        df = pd.read_csv(file_name, compression='gzip')
        if 'green' in file_name:
            df = df.astype({'VendorID': 'float',
                            'RatecodeID': 'float',
                            'PULocationID': 'float',
                            'DOLocationID': 'float',
                            'passenger_count': 'float',
                            'trip_distance': 'float',
                            'fare_amount': 'float',
                            'extra': 'float',
                            'mta_tax': 'float',
                            'tip_amount': 'float',
                            'tolls_amount': 'float',
                            'improvement_surcharge': 'float',
                            'total_amount': 'float',
                            'payment_type': 'float',
                            'congestion_surcharge': 'float',
                            'ehail_fee': 'float',
                            'trip_type': 'float',})
        elif 'yellow' in file_name:
            df = df.astype({'VendorID': 'float',
                            'RatecodeID': 'float',
                            'PULocationID': 'float',
                            'DOLocationID': 'float',
                            'passenger_count': 'float',
                            'trip_distance': 'float',
                            'fare_amount': 'float',
                            'extra': 'float',
                            'mta_tax': 'float',
                            'tip_amount': 'float',
                            'tolls_amount': 'float',
                            'improvement_surcharge': 'float',
                            'total_amount': 'float',
                            'payment_type': 'float',
                            'congestion_surcharge': 'float',})
        elif 'fhv' in file_name:
            df = df.astype({'PUlocationID': 'float',
                            'DOlocationID': 'float',
                            'SR_Flag': 'float',})
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs
        upload_to_gcs(BUCKET_NAME, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")

def web_to_gcs(service, year, month=None):
    if month:
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        web_to_gcs_file(service, file_name)
        return
    
    for i in range(12):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        web_to_gcs_file(service, file_name)
    return


# web_to_gcs('green', '2019')
# web_to_gcs('green', '2020')
web_to_gcs('yellow', '2019')
# web_to_gcs('yellow', '2020')
# web_to_gcs('fhv', '2019')