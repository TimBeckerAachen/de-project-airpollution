import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'airpollution_data_all')
files_to_upload = ['Measurement_info',
                   'Measurement_item_info',
                   'Measurement_station_info']


def convert_to_parquet():
    print(os.getcwd())
    directory = './AirPollutionSeoul/Original Data/'
    for filename in os.listdir(directory):
        print(filename)
        print(f'{directory}{filename}')
        if filename.endswith('.csv'):
            table = pv.read_csv(f'{directory}{filename}')
            if filename == 'Measurement_info.csv':
                table = table.rename_columns(['measurement_date',
                                              'station_code',
                                              'item_code',
                                              'average_value',
                                              'instrument_status'])
            elif filename == 'Measurement_item_info.csv':
                table = table.rename_columns(['item_code',
                                              'item_name',
                                              'unit',
                                              'good',
                                              'normal',
                                              'bad',
                                              'very_bad'])
            elif filename == 'Measurement_station_info.csv':
                table = table.rename_columns(['station_code',
                                              'station_name',
                                              'address',
                                              'latitude',
                                              'longitude'])
            pq_filename = filename.split('/')[-1].replace('.csv', '.parquet')
            pq.write_table(table, pq_filename)
            logging.info(f'{filename} to {pq_filename}')
        else:
            logging.error(f'Could not convert {filename}! Can only accept source files in CSV format, for the moment.')


def upload_parquet_to_gcs(bucket):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :return:
    """
    print(f'start upload to {bucket}')
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    for filename in os.listdir(os.getcwd()):
        if filename.split('.')[-1] == 'parquet':
            blob = bucket.blob(f'airpollution/{filename}')
            blob.upload_from_filename(filename)
            print(f'file: {filename} upload successful to {bucket}')
        else:
            print(f'dont upload {filename}')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="airpollution-project-dag",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['airpolution'],
) as dag:
    install_kaggle_task = BashOperator(
        task_id='install_kaggle_task',
        bash_command='pip install kaggle'
    )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"kaggle datasets download bappekim/air-pollution-in-seoul -p {path_to_local_home} --unzip"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=convert_to_parquet,
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_parquet_to_gcs,
        op_kwargs={
            "bucket": BUCKET
        },
    )

    external_table_tasks = []
    for file in files_to_upload:
        # TODO: file too small??
        bq_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_external_table_{file}_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"airpollution_external_table_{file}",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/airpollution/{file}.parquet"],
                },
            },
        )

        external_table_tasks.append(bq_external_table_task)

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.airpollution \
                PARTITION BY DATE(Measurement_date) \
                AS \
                SELECT * FROM {BIGQUERY_DATASET}.airpollution_external_table_Measurement_info;"
    )

    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    cleanup_task = BashOperator(
         task_id="delete_data",
         bash_command=f"rm -r {path_to_local_home}/AirPollutionSeoul {path_to_local_home}/*.parquet"
    )

    install_kaggle_task >> download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> external_table_tasks >> bq_create_partitioned_table_job >> cleanup_task


