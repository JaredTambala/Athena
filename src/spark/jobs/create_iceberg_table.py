import os
print("Classpath:", os.environ.get("SPARK_CLASSPATH"))
print("Hadoop Classpath:", os.environ.get("HADOOP_CLASSPATH"))


import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description="Dynamic Iceberg Table Loader")
parser.add_argument("--catalog_name", required=True, help="Catalog name")
parser.add_argument("--catalog_uri", required=True, help="Hive Metastore URI")
parser.add_argument("--catalog_warehouse", required=True, help="Iceberg warehouse location")
parser.add_argument("--table_name", required=True, help="Table name")
parser.add_argument("--namespace", required=True, help="Table namespace")
parser.add_argument("--schema", required=True, help="Schema definition")
parser.add_argument("--partition_by", required=True, help="Partition columns")
parser.add_argument("--data_path", required=True, help="S3 location for input data")
parser.add_argument("--packages", required=True, help="External dependencies")
parser.add_argument("--access_key", required=True, help="S3 Access Key")
parser.add_argument("--secret_key", required=True, help="S3 Secret Key")

args = parser.parse_args()

# Parse schema
import json
schema_definition = json.loads(args.schema)
partition_by = json.loads(args.partition_by)

# Create Spark Session
spark = SparkSession.builder \
    .appName(f"CreateIcebergTable-{args.table_name}") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config(f"spark.sql.catalog.{args.catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{args.catalog_name}.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config(f"spark.sql.catalog.{args.catalog_name}.type", "hive") \
    .config(f"spark.sql.catalog.{args.catalog_name}.uri", args.catalog_uri) \
    .config(f"spark.sql.catalog.{args.catalog_name}.warehouse", args.catalog_warehouse) \
    .config("spark.jars.packages", args.packages) \
    .config(f"spark.sql.catalog.{args.catalog_name}.fs.s3a.access.key", args.access_key) \
    .config(f"spark.sql.catalog.{args.catalog_name}.fs.s3a.secret.key", args.secret_key) \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

# Create Database and Table
spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog_name}.{args.namespace}")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {args.catalog_name}.{args.namespace}.{args.table_name}
    ({', '.join([f'{col} {dtype}' for col, dtype in schema_definition.items()])})
    USING iceberg
    PARTITIONED BY ({', '.join(partition_by)})
    """
)