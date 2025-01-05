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
import pandas as pd
from minio import Minio
from botocore.config import Config
import asyncio
import threading
import logging
import re
import gzip

# Airflow Variables
MINIO_ACCESS_KEY = Variable.get("minio_access_key")
MINIO_SECRET_KEY = Variable.get("minio_secret_key")
POLYGON_BUCKET = 'polygon'
CONFIG_BUCKET = 'airflow'
MINIO_ENDPOINT = 'minio:9000'
CONFIG_FILE_PATH = 'dag_configs/polygon_unzip_to_parquet.json'
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
    dag_id="polygon_unzip_to_parquet_worker",
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
    
    # Produce list of files in storage location with date >= start date
    @task()
    def find_files_to_unzip(**kwargs):

        # Pull job config
        config = kwargs['dag_run'].conf
        job = config['job']
        details = config['details']
        source_folder = details["source_folder"]
        data_type = details["type"]
        start_date = details["start_date"]
        destination_fodler = details["destination_folder"]
        # subtract one day from date to handle MinIO start_after logic
        start_after_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        # List of all files starting from that date
        file_list = [x.object_name for x in minio_client.list_objects(
            bucket_name=POLYGON_BUCKET,
            prefix="/".join([source_folder, data_type]),
            recursive=True,
            start_after="/".join(
                [
                    source_folder,
                    data_type,
                    start_after_date[:4],
                    start_after_date[5:7],
                    f"{start_after_date}.csv.gz"
                ]
            )
        )]
        print(f"Files to unzip: {file_list}")
        return file_list
    
    # Download files, unzip, convert to parquet, load in destination
    @task()
    def unzip_files_and_load_parquet(file_list, **kwargs):

        # Pull job config
        config = kwargs['dag_run'].conf
        job = config['job']
        details = config['details']
        source_folder = details["source_folder"]
        data_type = details["type"]
        start_date = details["start_date"]
        destination_folder = details["destination_folder"]

        def convert_to_parquet_path(raw_path):
            # Extract the relevant part of the path
            parts = raw_path.split('/')
            year = parts[-3]
            month = parts[-2]
            day = re.search(r"(\d{4})-(\d{2})-(\d{2})\.csv\.gz", raw_path).group(3)
            file_name = parts[-1].replace('.csv.gz', '.parquet')
            # Construct the new path
            return f"year={year}/month={month}/day={day}/{file_name}"
        
        async def process_window_start_column(df):
            """
            Processes window_start nanosecond columns to produce a DataFrame with
            a readable timestamp.
            """
            df = df.rename(columns={"window_start": "window_start_ns"})
            df["window_start"] = pd.to_datetime(df["window_start_ns"], unit="ns")
            return df

        async def process_file(file_name):
            zipped_byte_stream = io.BytesIO()
            print(f"Downloading {file_name}...")
            # Download object from Minio
            data = await asyncio.to_thread(
                minio_client.get_object, POLYGON_BUCKET, file_name
            )
            zipped_byte_stream.write(data.read())
            zipped_byte_stream.seek(0)
            # Unzipping
            print(f"Unzipping {file_name}...")
            unzipped_file = await asyncio.to_thread(gzip.decompress, zipped_byte_stream.read())
            # Converting CSV to Parquet (this is CPU-bound, so offloading to a thread)
            print(f"Converting {file_name} to parquet...")
            df = await asyncio.to_thread(pd.read_csv, io.BytesIO(unzipped_file))
            if "window_start" in df.columns:
                print(f"Processing date column on {file_name}...")
                df =  await process_window_start_column(df)
            parquet_byte_stream = io.BytesIO()
            await asyncio.to_thread(df.to_parquet, parquet_byte_stream, engine="pyarrow")
            parquet_byte_stream.seek(0)
            # Uploading the parquet file to MinIO
            destination_path = convert_to_parquet_path(file_name)
            print(f"Writing {destination_path}")
            await asyncio.to_thread(
                minio_client.put_object,
                POLYGON_BUCKET,
                "/".join([destination_folder, data_type, destination_path]),
                parquet_byte_stream,
                length=-1,
                part_size=1024 * 1024 * 5,
            )
            print("File uploaded successfully!")

        async def process_file_list(file_list):
            await asyncio.gather(*[process_file(file_name) for file_name in file_list])
                
        async def main():
            await process_file_list(file_list)

        # Runs in a separate thread to not block Airflow
        run_async_task_in_thread(main)

    # Update config with new start_date
    @task()
    def update_config(file_list, **kwargs):

        #Pull config
        config = kwargs['dag_run'].conf
        job = config['job']
        details = config['details']        
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
        config[job]["start_date"] = re.search(r'\d{4}-\d{2}-\d{2}', file_list[-1]).group()
        # upload new config
        config_str = json.dumps(config).encode('utf-8')
        print(config_str)
        minio_client.put_object(
            CONFIG_BUCKET, CONFIG_FILE_PATH, io.BytesIO(config_str), length=len(config_str)
        )

    file_list = find_files_to_unzip()
    transfer_task = unzip_files_and_load_parquet(file_list)
    update_config_task = update_config(file_list)

    transfer_task >> update_config_task

execute_s3_to_s3_batch_job()   
    