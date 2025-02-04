from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    dag_id="create_iceberg_table_worker",
    schedule=None,
    start_date=None,
    catchup=False
)
def execute_create_table_job(**kwargs):

    """
    DAG to create Iceberg tables dynamically based on passed configuration.
    """
    
    def get_dag_run_conf(**kwargs):
        # Retrieve the dag_run.conf (runtime configuration)
        dag_run_conf = kwargs["dag_run"].conf
        if not dag_run_conf:
            raise ValueError("No configuration provided in dag_run.conf")
        return dag_run_conf

    # PythonOperator to fetch configuration
    def fetch_config(**kwargs):
        config = get_dag_run_conf(**kwargs)
        table_name = config["job"]
        details = config["details"]
        return (table_name, details)

    # Task to run SparkSubmitOperator dynamically
    def spark_submit_task(table_name, details):
        # Define SparkSubmitOperator
        return SparkSubmitOperator(
            task_id=f"create_{table_name}_job",
            conn_id="spark_conn",
            application="s3a://spark/jobs/create_iceberg_table.py",
            jars='~/.local/lib/python3.11/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar,~/.local/lib/python3.11/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar'
            name="CreateIcebergTable",
            application_args=[
                "--catalog_name", details["catalog_name"],
                "--catalog_uri", details["catalog_uri"],
                "--catalog_warehouse", details["catalog_warehouse"],
                "--table_name", table_name,
                "--namespace", details["namespace"],
                "--schema", json.dumps(details["schema_definition"]),
                "--partition_by", json.dumps(details["partition_by"]),
                "--access_key", MINIO_ACCESS_KEY,
                "--secret_key", MINIO_SECRET_KEY,
        
            ],
            deploy_mode=""
        )

    # Pull configuration and create tasks dynamically
    config_task = PythonOperator(
        task_id="fetch_config",
        python_callable=fetch_config,
        provide_context=True,
    )

    table_name, details = *config_task.output
    create_table_task = spark_submit_task(table_name, details)
    config_task >> create_table_task

execute_create_table_job()

