#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
import logging
import os
import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage


GCS_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

TAXI_COLOR = '{{ dag_run.conf["TAXI_COLOR"] }}'
INPUT_FILE_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
INPUT_DATE_TEMPLATE = '{{ execution_date.strftime(\'%Y-%m\') }}'
INPUT_FILE_TEMPLATE = f"{TAXI_COLOR}_tripdata_{INPUT_DATE_TEMPLATE}.csv.gz"
URL_TEMPLATE = f"{INPUT_FILE_PREFIX}/{TAXI_COLOR}/{INPUT_FILE_TEMPLATE}"
OUTPUT_FILE_TEMPLATE = INPUT_FILE_TEMPLATE.replace(".gz", "")
GCS_FILE_TEMPLATE = OUTPUT_FILE_TEMPLATE.replace("-", "_").replace(".csv", "")

def format_to_csv(input_file, output_file):
    if not input_file.endswith('.csv.gz'):
        logging.error("Can only accept source files in CSV Gzip format, for the moment")
        return

    df = pd.read_csv(input_file, compression='gzip')
    df.to_csv(output_file, index=False)
    print(f"Saving '{input_file}' to {output_file}")

    file_size = os.path.getsize(output_file)
    print(f"The size of '{output_file}' is: {file_size} bytes")
    return

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    return

gcs_workflow = DAG(
    "GcsIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2020, 1, 1)
)

with gcs_workflow:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"wget '{URL_TEMPLATE}' -O '{AIRFLOW_HOME}/{INPUT_FILE_TEMPLATE}'"
    )

    format_to_csv_task = PythonOperator(
        task_id="format_to_csv_task",
        python_callable=format_to_csv,
        op_kwargs={
            "input_file": f"{AIRFLOW_HOME}/{INPUT_FILE_TEMPLATE}",
            "output_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": GCS_BUCKET,
            "object_name": f"raw/{OUTPUT_FILE_TEMPLATE}",
            "local_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": GCS_PROJECT_ID,
                "datasetId": "ny_taxi",
                "tableId": GCS_FILE_TEMPLATE,
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{GCS_BUCKET}/raw/{OUTPUT_FILE_TEMPLATE}"],
                "autodetect": True
            },
        },
    )

    download_dataset_task >> format_to_csv_task >> local_to_gcs_task >> bigquery_external_table_task