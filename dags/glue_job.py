import os
from datetime import timedelta

from dotenv import load_dotenv

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.utils.dates import days_ago

# Load environment variables
load_dotenv()

# Configurations
CRAWLER_NAME = os.getenv("CRAWLER_NAME")
AWS_CONN_ID = "aws_default"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="TRANSFORMED_S3_to_GLUE_ATHENA",
    default_args=default_args,
    description="Load S3 output folder into Athena",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["Project", "s3", "athena"],
) as dag:

    # Trigger Glue crawler to update Athena tables
    trigger_crawler = GlueCrawlerOperator(
        task_id="trigger_glue_crawler",
        config={"Name": CRAWLER_NAME},
        aws_conn_id=AWS_CONN_ID,
    )

    # Wait until Glue crawler finishes
    wait_for_crawler = GlueCrawlerSensor(
        task_id="wait_for_crawler_completion",
        crawler_name=CRAWLER_NAME,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 30,
    )

    # Set DAG task dependency
    trigger_crawler >> wait_for_crawler
