import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

access_key = os.environ.get("MINIO_ACCESS_KEY")
secret_key = os.environ.get("MINIO_SECRET_KEY")
minio_ip = os.environ.get("MINIO_IP")
postgres_ip = os.environ.get("POSTGRES_IP")
postgres_user = os.environ.get("POSTGRES_USER")
postgres_pass = os.environ.get("POSTGRES_PASSWORD")
s3_bucket = os.environ.get("S3_BUCKET")

run_date = sys.argv[1]

# Connect to the local MinIO endpoint
spark = (
    SparkSession.builder.appName("Silver_to_Gold_Loader")
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_ip}:9000")
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)

# 1. Read the Silver Parquet partitions
print("Reading Parquet data from RustFS...")
df = spark.read.parquet(f"s3a://{s3_bucket}/silver_sales_by_city")

df = df.withColumn("load_date", lit(run_date))

# 2. Define the Postgres Warehouse connection
POSTGRES_URL = f"jdbc:postgresql://{postgres_ip}:5433/gold_zone"
POSTGRES_PROPERTIES = {
    "user": postgres_user,
    "password": postgres_pass,
    "driver": "org.postgresql.Driver",
}

# 3. Push the data to the Staging table
print("Pushing data to Postgres Warehouse via JDBC...")
df.write.jdbc(
    url=POSTGRES_URL,
    table="staging.sales_by_city",
    # mode="append",
    mode="overwrite",
    properties=POSTGRES_PROPERTIES,
)

print("Load complete.")
