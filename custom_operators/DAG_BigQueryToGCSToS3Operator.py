from airflow import DAG
from Airflow_Things.custom_operators.BigQueryToGCSToS3Operator import BigQueryToGCSToS3Operator
from datetime import datetime

default_args = {"owner": "airflow", "start_date": datetime(2025, 1, 1)}

with DAG("bigquery_to_gcs_to_s3_example", default_args=default_args, schedule_interval=None) as dag:
    export_task = BigQueryToGCSToS3Operator(
        task_id="export_to_gcs_and_s3",
        sql_query="SELECT * FROM `project.dataset.table` WHERE date = '{{ params.date }}'",
        parameters={"date": "2025-01-01"},
        gcs_bucket="my-gcs-bucket",
        gcs_path="data/export.csv",
        output_format="csv",
        s3_transfer=True,
        s3_bucket="my-s3-bucket",
        s3_key="data/export.csv",
        gcp_conn_id="google_cloud_default",
        aws_conn_id="aws_default",
    )
    export_task
