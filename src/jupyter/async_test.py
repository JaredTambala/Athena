import boto3
import io
from minio import Minio
from botocore.config import Config
import pandas as pd
import gzip
from io import BytesIO
import asyncio

MINIO_BUCKET = "polygon"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "jared"
MINIO_SECRET_KEY = "password"
PREFIX = "us_options_opra/minute_aggs_v1/2019/01/"
DATA_BUCKET = "flatfiles"
POLYGON_ACCESS_KEY = "9e922ec1-743d-4916-b10e-ad3d0f3855aa"
POLYGON_SECRET_KEY = "aFJoRuGWM1sxaaOyaiZBlRPNocTH8uap"
POLYGON_ENDPOINT = "https://files.polygon.io"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)
polygon_session = boto3.Session(
    aws_access_key_id=POLYGON_ACCESS_KEY,
    aws_secret_access_key=POLYGON_SECRET_KEY,
)
polygon_s3_client = polygon_session.client(
    "s3",
    endpoint_url=POLYGON_ENDPOINT,
    config=Config(signature_version="s3v4", max_pool_connections=500),
)
zipped_byte_stream = BytesIO()
parquet_byte_stream = BytesIO()
paginator = polygon_s3_client.get_paginator("list_objects_v2")


async def process_file(obj, minio_bucket):
    zipped_byte_stream = io.BytesIO()
    # Download object from Minio (offloading to a thread to make it async)
    print(f"Downloading {obj.object_name}...")
    data = await asyncio.to_thread(
        minio_client.get_object, minio_bucket, obj.object_name
    )
    zipped_byte_stream.write(data.read())
    zipped_byte_stream.seek(0)
    # Unzipping (asynchronous decompression)
    print(f"Unzipping {obj.object_name}...")
    unzipped_file = await asyncio.to_thread(gzip.decompress, zipped_byte_stream.read())
    # Converting CSV to Parquet (this is CPU-bound, so offloading to a thread)
    print(f"Converting {obj.object_name} to parquet...")
    df = await asyncio.to_thread(pd.read_csv, io.BytesIO(unzipped_file))
    parquet_byte_stream = io.BytesIO()
    await asyncio.to_thread(df.to_parquet, parquet_byte_stream, engine="pyarrow")
    parquet_byte_stream.seek(0)
    # Uploading the parquet file to Minio (offloading to a thread)
    print("Writing file...")
    await asyncio.to_thread(
        minio_client.put_object,
        minio_bucket,
        "processed/"
        + "/".join(obj.object_name.split("/")[1:]).split(".")[0]
        + ".parquet",
        parquet_byte_stream,
        length=-1,
        part_size=1024 * 1024 * 5,
    )
    print("File uploaded successfully!")


async def process_all_objects(minio_client, minio_bucket, prefix):
    # Get list of objects asynchronously
    objects = await asyncio.to_thread(
        minio_client.list_objects, minio_bucket, recursive=True, prefix=prefix
    )
    tasks = []
    processed_objects = set()  # Track processed objects to avoid duplicates
    for obj in objects:
        # Ensure that each file is processed only once
        if obj.object_name not in processed_objects:
            tasks.append(process_file(obj, minio_bucket))
            processed_objects.add(obj.object_name)
    # Run tasks concurrently
    await asyncio.gather(*tasks)


# Entry point
async def main():
    # Replace with your actual bucket and prefix
    minio_bucket = MINIO_BUCKET
    prefix = "raw/us_options_opra/day_aggs_v1/"
    await process_all_objects(minio_client, minio_bucket, prefix)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())

asyncio.run(main())
