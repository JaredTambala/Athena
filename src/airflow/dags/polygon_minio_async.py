from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import boto3
import io
from minio import Minio
from botocore.config import Config
import asyncio
import threading

DATA_BUCKET = 'flatfiles'
MINIO_ACCESS_KEY = Variable.get("minio_access_key")
MINIO_SECRET_KEY = Variable.get("minio_secret_key")
POLYGON_ACCESS_KEY = Variable.get("polygon_access_key")
POLYGON_SECRET_KEY = Variable.get("polygon_secret_key")
POLYGON_ENDPOINT='https://files.polygon.io'
MINIO_BUCKET = 'polygon'
MINIO_ENDPOINT = 'minio:9000'
MAX_CONCURRENT_TASKS = 10  # Set maximum number of concurrent tasks



semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

def run_async_task_in_thread(async_func, *args):
    thread = threading.Thread(target=lambda: asyncio.run(async_func(*args)))
    thread.start()
    thread.join()

def transfer_s3_to_minio(prefix: str):
    """
    Top-level function for data transfer task.
    """
    # Poylgon Session and S3 Client
    polygon_session = boto3.Session(
        aws_access_key_id=POLYGON_ACCESS_KEY,
        aws_secret_access_key=POLYGON_SECRET_KEY,
    )
    polygon_s3_client = polygon_session.client(
        's3',
        endpoint_url=POLYGON_ENDPOINT,
        config=Config(
            signature_version='s3v4',
            max_pool_connections=100
        ),
    )
    # Minio Client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    paginator = polygon_s3_client.get_paginator('list_objects_v2')
    PREFIX=prefix
    async def transfer_file(obj_key, data_bucket, minio_bucket):
        """
        Awaitable function which downloads data from S3 and uploads to Minio.
        """
        async with semaphore:
            data = io.BytesIO()
            await asyncio.to_thread(polygon_s3_client.download_fileobj, data_bucket, obj_key, data)
            print(f"{obj_key} download complete...")
            data.seek(0)
            await asyncio.to_thread(minio_client.put_object, minio_bucket, "raw/"+obj_key, data, length=-1, part_size=1024*1024*5)
            print(f"{obj_key} upload complete...")

    async def process_page(page, data_bucket, minio_bucket):
        """
        Gathers S3 objects from data bucket and creates transfer_file tasks.
        """
        tasks = []
        for obj in page['Contents']:
            obj_key = obj['Key']
            tasks.append(transfer_file(obj_key, data_bucket, minio_bucket))
        await asyncio.gather(*tasks)
    
    async def process_all_pages(paginator, data_bucket, minio_bucket):
        """
        Awaitable function which calls process_page on each page. Useful
        for large data buckets.
        """
        for page in paginator.paginate(Bucket=data_bucket, Prefix=PREFIX):
            await process_page(page, data_bucket, minio_bucket)
    
    async def main():
        """
        Awaits processing all pages
        """
        await process_all_pages(paginator, DATA_BUCKET, MINIO_BUCKET)

    # runs in separate thread to not block Airflow
    run_async_task_in_thread(main)

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'polygon_to_minio_async',
    default_args=default_args,
    description='DAG to transfer files from Polygon S3 to Minio, using a PREFIX from dag_run conf',
    schedule_interval=None,  # Manually triggered
    catchup=False,
) as dag:

    # Task to run the transfer process, with PREFIX passed from dag_run conf
    def transfer_with_conf(**kwargs):
        # Fetch the PREFIX from the dag_run conf
        prefix = kwargs['dag_run'].conf.get('prefix', '')
        if not prefix:
            raise ValueError("Prefix not provided in dag_run conf")
        
        transfer_s3_to_minio(prefix)

    transfer_task = PythonOperator(
        task_id='transfer_s3_to_minio',
        python_callable=transfer_with_conf,
        provide_context=True  # Passes context with 'dag_run' to the function
    )

    transfer_task
