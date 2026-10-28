import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

run_date = sys.argv[1]

# Connect to the local MinIO endpoint
spark = (
    SparkSession.builder.appName("Silver_to_Gold_Loader")
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.0.2.10:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
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
df = spark.read.parquet("s3a://spark-data/silver_sales_by_city")

df = df.withColumn("load_date", lit(run_date))

# 2. Define the Postgres Warehouse connection
POSTGRES_URL = "jdbc:postgresql://10.0.2.13:5433/gold_zone"
POSTGRES_PROPERTIES = {
    "user": "dw_admin",
    "password": "dw_password",
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
