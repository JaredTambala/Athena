from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.api.client.local_client import Client
from datetime import datetime, timedelta
import boto3
import io
import json
from minio import Minio
from botocore.config import Config
import asyncio
import threading
import logging
import re

# Airflow Variables
MINIO_ACCESS_KEY = Variable.get("minio_access_key")
MINIO_SECRET_KEY = Variable.get("minio_secret_key")
POLYGON_BUCKET = 'polygon'
CONFIG_BUCKET = 'airflow'
MINIO_ENDPOINT = 'minio:9000'
CONFIG_FILE_PATH = 'dag_configs/polygon_raw_loader.json'
MAX_CONCURRENT_TASKS = 25


def run_async_task_in_thread(async_func, *args):

    exception_holder = []
    def wrapper():
        try:
            asyncio.run(async_func(*args))
        except Exception as e:
            exception_holder.append(e)

    thread = threading.Thread(target=wrapper)
    thread.start()
    thread.join()
    # Raise the exception in the main thread if it occurred
    if exception_holder:
        raise exception_holder[0]

@dag(
    dag_id="s3_to_s3_batch_worker",
    schedule=None,
    start_date=None,
    catchup=False
)
def execute_s3_to_s3_batch_job():
    
    # Minio Client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    ) 
    # Semaphore for limiting concurrency
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    
    @task()
    def find_files_to_download(**kwargs):
        
        #Pull config
        config = kwargs['dag_run'].conf
        job = config['job']
        details = config['details']
        source_bucket_name = details["source_bucket_name"]
        data_type = details["type"]
        latest_file_loaded = details["latest_file_loaded"]

        # Polygon Client
        polygon_session = boto3.Session(
            aws_access_key_id=Variable.get("polygon_access_key"),
            aws_secret_access_key=Variable.get("polygon_secret_key"),
        )
        polygon_s3_client = polygon_session.client(
            's3',
            endpoint_url='https://files.polygon.io',
            config=Config(signature_version='s3v4', max_pool_connections=100),
        )

        #List all files from location with correct prefix
        paginator = polygon_s3_client.get_paginator('list_objects_v2')
        all_file_list = [
            obj["Key"]
            for page in paginator.paginate(Bucket=source_bucket_name, Prefix=data_type)
            for obj in page['Contents']
        ]

        #Calculate files to download last_file_loaded and current date
        def generate_file_suffix_list(start_date, end_date=None):
        # Convert start_date to a datetime object
            start = datetime.strptime(start_date, "%Y-%m-%d")
            # Set the end_date to today if not provided
            end = datetime.today() if end_date is None else datetime.strptime(end_date, "%Y-%m-%d")
            # Generate file names for each date in the range
            file_names = [
                f"{date.year}/{date.month:02d}/{date.strftime('%Y-%m-%d')}.csv.gz"
                for days in range((end - start).days + 1)
                for date in [start + timedelta(days=days)]
            ]
            return file_names

        # Generate list of file suffixes and intersect them with available files
        generated_file_suffix_list = generate_file_suffix_list(latest_file_loaded)
        fetched_file_suffix_list = ["/".join(x.split("/")[2:]) for x in all_file_list]
        merged_file_suffix_list = []
        for x in fetched_file_suffix_list:
            if x in generated_file_suffix_list:
                merged_file_suffix_list.append(x)
        files_to_download_list = ["/".join([data_type, x]) for x in merged_file_suffix_list]
        return files_to_download_list

    @task()
    def transfer_files_from_polygon_to_minio(download_file_list, **kwargs):

        #Pull config
        config = kwargs['dag_run'].conf
        job = config['job']
        details = config['details']
        source_bucket_name = details["source_bucket_name"]
        data_type = details["type"]
        latest_file_loaded = details["latest_file_loaded"]
        destination_bucket_name = details["destination_bucket_name"]
        destination_folder = details["destination_folder"]

        # Polygon Client
        polygon_session = boto3.Session(
            aws_access_key_id=Variable.get("polygon_access_key"),
            aws_secret_access_key=Variable.get("polygon_secret_key"),
        )
        polygon_s3_client = polygon_session.client(
            's3',
            endpoint_url='https://files.polygon.io',
            config=Config(signature_version='s3v4', max_pool_connections=100),
        )
        
        async def transfer_file(obj_key):
            """
            Downloads and uploads a file from S3 to MinIO.
            """
            async with semaphore:
                data = io.BytesIO()
                print(f"Downloading {obj_key}...")
                await asyncio.to_thread(
                    polygon_s3_client.download_fileobj,
                    source_bucket_name,
                    obj_key,
                    data
                )
                print(f"{obj_key} download complete. Uploading to MinIO...")
                data.seek(0)
                await asyncio.to_thread(
                    minio_client.put_object,
                    destination_bucket_name,
                    f"{destination_folder}/{obj_key}",
                    data,
                    length=-1,
                    part_size=1024*1024*5
                )
                print(f"{obj_key} upload complete...")
                
        async def process_file_list(file_list):
            await asyncio.gather(*[transfer_file(file_name) for file_name in file_list])
                
        async def main():
            await process_file_list(download_file_list)
            
        # Runs in a separate thread to not block Airflow
        run_async_task_in_thread(main)

    @task()
    def update_latest_file_uploaded(download_file_list, **kwargs):

        #Pull config
        config = kwargs['dag_run'].conf
        job = config['job']
        details = config['details']
        source_bucket_name = details["source_bucket_name"]
        data_type = details["type"]
        latest_file_loaded = details["latest_file_loaded"]
        destination_bucket_name = details["destination_bucket_name"]
        destination_folder = details["destination_folder"]
        
        # Check if config is in bucket
        configs = [x.object_name for x in list(minio_client.list_objects(
            bucket_name=CONFIG_BUCKET,
            prefix="dag_configs/"
        ))]
        if not(CONFIG_FILE_PATH in configs):
            raise FileNotFoundError(f"{CONFIG_FILE_PATH} not found in Airflow bucket!")
        # Load config from bucket
        print(f"Configuration {CONFIG_FILE_PATH} found. Loading...")
        data = minio_client.get_object(
            CONFIG_BUCKET, CONFIG_FILE_PATH
        )
        config = data.json()
        # Update config with latest date
        config[job]["latest_file_loaded"] = re.search(r'\d{4}-\d{2}-\d{2}', download_file_list[-1]).group()
        # upload new config
        config_str = json.dumps(config).encode('utf-8')
        print(config_str)
        minio_client.put_object(
            CONFIG_BUCKET, CONFIG_FILE_PATH, io.BytesIO(config_str), length=len(config_str)
        )
    file_list = find_files_to_download()
    transfer_task = transfer_files_from_polygon_to_minio(file_list)
    update_task = update_latest_file_uploaded(file_list)

    transfer_task >> update_task
    
execute_s3_to_s3_batch_job()   
    