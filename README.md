# E-Commerce Data Pipeline: Bronze to Gold ETL

## Project Overview
This project is an automated, batch-oriented ETL pipeline that processes daily e-commerce transaction logs. It extracts JSON data from an S3-compatible Data Lake (MinIO), processes it using a distributed Apache Spark cluster, and loads it into a PostgreSQL Data Warehouse. Final business aggregations are handled via dbt (Data Build Tool), with the entire workflow orchestrated by Apache Airflow.

## Repository Structure
```text
├── dags/
│   └── bronze_to_silver_spark_etl.py     # Airflow DAG definition
├── spark_jobs/
│   ├── spark_total_by_city_workers.py    # Bronze to Silver PySpark script
│   └── spark_silver_to_gold.py           # Silver to Gold PySpark script
├── dbt_project/
│   ├── models/                           # dbt SQL models
│   ├── dbt_project.yml                   # dbt configuration
│   └── profiles.yml.example              # Template for DB credentials
├── infrastructure/
│   └── docker-compose.yml                # Docker configs for Postgres (and Metabase)
├── requirements.txt
└── README.md
```

## How to Recreate This Environment

### 1. Infrastructure (Postgres)
Spin up the Gold zone database on the Airflow host machine:

```
cd infrastructure
docker-compose up -d
```

### 2. Configure Airflow
Copy dags/bronze_to_silver_spark_etl.py to your Airflow dags/ folder. Before unpausing the DAG, create the following connections in the Airflow UI (Admin -> Connections):

1. **`rustfs_s3_conn`** (Type: Amazon Web Services)
   * Extra: `{"endpoint_url": "http://10.0.2.10:9000", "aws_access_key_id": "...", "aws_secret_access_key": "..."}`
2. **`spark_master_ssh`** (Type: SSH)
   * Host: `10.0.2.X` (Your Spark VM IP)
   * Username: `sparkuser`
   * Password/Key: `[Your SSH Key]`
3. **`airflow_host_ssh`** (Type: SSH)
   * Host: `10.0.2.13`
   * Username: `airflow`

### 3. Deploy Spark Jobs
Copy the contents of the spark_jobs/ directory into /home/sparkuser/ on your Spark Master VM.

4. Deploy dbt
- Copy the dbt_project/ folder to /home/airflow/airflow/gold_models on the Airflow VM.
- Create your local ~/.dbt/profiles.yml using profiles.yml.example as a template and insert your actual Postgres credentials.

## Pipeline Execution

The pipeline is designed to run completely hands-off.

1. The Airflow DAG triggers daily at midnight.
2. The `wait_for_transaction_logs` S3KeySensor polls MinIO using `mode='reschedule'` to preserve worker memory.
