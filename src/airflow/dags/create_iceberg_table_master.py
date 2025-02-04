from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task, task_group
from airflow.models import Variable, TaskInstance
from airflow.utils.dates import days_ago
from airflow.api.client.local_client import Client
import boto3
import io
import json
from minio import Minio
from botocore.config import Config
import asyncio
import threading
import logging

# Airflow Variables
MINIO_ACCESS_KEY = Variable.get("minio_access_key")
MINIO_SECRET_KEY = Variable.get("minio_secret_key")
CONFIG_BUCKET = 'airflow'
MINIO_ENDPOINT = 'minio:9000'
CONFIG_FILE_PATH = 'dag_configs/iceberg_table_config.json'


@dag(
    dag_id="create_iceberg_tables_master",
    schedule=None,
    start_date=None,
    catchup=False,
)
def create_s3_to_s3_batch_jobs():
    
    # Minio Client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    ) 
    
    @task()
    def fetch_config_from_minio_and_build_jobs():
        """
        Fetch and parse JSON configuration file from MinIO.
        """
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
        jobs_list = [{"job": job, "details": details} for job, details in config.items()]
        return jobs_list

    
    trigger_jobs = TriggerDagRunOperator.partial(
        task_id = "create_iceberg_table_job",
        trigger_dag_id="create_iceberg_table_worker",
        wait_for_completion=True
    ).expand(
        conf=fetch_config_from_minio_and_build_jobs()
    )
    #fetch_config_from_minio() >> trigger_jobs

create_s3_to_s3_batch_jobs()   
    