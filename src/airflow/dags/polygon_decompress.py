from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import boto3
import io
import gzip
from minio import Minio
from botocore.config import Config
import pandas as pd
import asyncio

# Constants
MINIO_BUCKET = 'polygon'
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = Variable.get("minio_access_key")
MINIO_SECRET_KEY = Variable.get("minio_secret_key")

# Minio Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

async def process_file(obj, minio_bucket):
    zipped_byte_stream = io.BytesIO()
    # Download object from Minio
    data = await asyncio.to_thread(minio_client.get_object, minio_bucket, obj.object_name)
    zipped_byte_stream.write(data.read())
    zipped_byte_stream.seek(0)
    # Unzipping
    unzipped_file = await asyncio.to_thread(gzip.decompress, zipped_byte_stream.read())
    # Converting CSV to Parquet
    df = await asyncio.to_thread(pd.read_csv, io.BytesIO(unzipped_file))
    parquet_byte_stream = io.BytesIO()
    await asyncio.to_thread(df.to_parquet, parquet_byte_stream, engine="pyarrow")
    parquet_byte_stream.seek(0)
    # Uploading the parquet file to Minio
    await asyncio.to_thread(
        minio_client.put_object,
        minio_bucket,
        "processed/"+'/'.join(obj.object_name.split('/')[1:]).split('.')[0]+".parquet",
        parquet_byte_stream,
        length=-1,
        part_size=1024*1024*5
    )

async def process_all_objects(minio_client, minio_bucket, prefix):
    objects = await asyncio.to_thread(minio_client.list_objects, minio_bucket, recursive=True, prefix=prefix)
    tasks = []
    processed_objects = set()
    for obj in objects:
        if obj.object_name not in processed_objects:
            tasks.append(process_file(obj, minio_bucket))
            processed_objects.add(obj.object_name)
    await asyncio.gather(*tasks)

def run_main(**kwargs):
    # Retrieve the prefix from the DAG run configuration
    prefix = kwargs['dag_run'].conf.get('prefix', '')
    if not prefix:
        raise ValueError("Prefix not provided in dag_run conf")
    
    minio_bucket = MINIO_BUCKET
    asyncio.run(process_all_objects(minio_client, minio_bucket, prefix))

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'polygon_to_minio_conversion',
    default_args=default_args,
    description='DAG to process and convert files from Minio with configurable prefix',
    schedule_interval=None,
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id='process_files_from_minio',
        python_callable=run_main,
        provide_context=True  # Allows access to 'dag_run' configuration
    )

    transfer_task
