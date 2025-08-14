import os
from datetime import datetime, timedelta

import pytz
from dotenv import load_dotenv

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# Load environment variables from .env file

# added as Connections in UI
AWS_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "local_postgres"

# Roles - EMR and EC2
EMR_EC2_INSTANCE_PROFILE = os.getenv("EMR_EC2_INSTANCE_PROFILE")
EMR_SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE")

# locations for pyfile, output, logs
LOG_URI = os.getenv("LOG_URI")
PYFILE_S3 = os.getenv("PYFILE_S3")
OUTPUT_S3 = os.getenv("OUTPUT_S3")
LOOKUP_S3 = os.getenv("LOOKUP_S3")

# Convert file name pulled from previous dag into an Input S3 URI for EMR to use
def prepare_input_location(**context):
    dag_run = context.get('dag_run')
    if not dag_run:
        raise ValueError("No dag_run context available.")
    
    input_file = dag_run.conf.get('input_file')

    if not input_file:
        raise ValueError("No 'input_file' found in dag_run.conf")

    BUCKET_NAME = os.getenv("BUCKET_NAME")
    PREFIX_FOLDER = os.getenv("PREFIX_FOLDER")
    s3_path = f"s3://{BUCKET_NAME}/{PREFIX_FOLDER}/{input_file}"

    ti = context['ti']
    ti.xcom_push(key='input_s3_path', value=s3_path)
    ti.xcom_push(key='input_file_name', value=input_file)  
    return s3_path

# Specs of the Cluster
JOB_FLOW_OVERRIDES = {
    "Name": "MyCluster",
    "ReleaseLabel": "emr-7.9.0",
    "LogUri": LOG_URI,
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Instances": {
        "Ec2SubnetId": os.getenv("EC2_SUBNET_ID"),
        "EmrManagedMasterSecurityGroup": os.getenv("MASTER_SG"),
        "EmrManagedSlaveSecurityGroup": os.getenv("SLAVE_SG"),
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": EMR_EC2_INSTANCE_PROFILE,
    "ServiceRole": EMR_SERVICE_ROLE,
    "VisibleToAllUsers": True,
}

# Function to create Spark Job
def build_spark_steps(**context):
    ti = context['ti']
    INPUT_S3 = ti.xcom_pull(task_ids='prepare_input_location', key='input_s3_path')
    return [
        {
            "Name": "nyc-taxi-job",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    PYFILE_S3,
                    INPUT_S3,
                    OUTPUT_S3,
                    LOOKUP_S3,
                        ],
                    },
                }
            ]

# Function to update file status when EMR job runs successfully
def update_postgres_status(**context):
    ti = context['ti']
    filename = ti.xcom_pull(task_ids='prepare_input_location', key='input_file_name')
    if not filename:
        raise ValueError("No input file name found in XCom")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    update_sql = """
        UPDATE s3_file_tracker
        SET status = 'done',
            transformed_at = %s
        WHERE file_name = %s
    """
    pg_hook.run(update_sql, parameters=(now_ist, filename))

# Dag args, Alter if needed
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Dag Specifications
with DAG(
    dag_id="EMR_to_Transformed_S3",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["Project", "emr", "spark", "s3"],
) as dag:
    
    # task for getting Input Location
    prepare_input = PythonOperator(
        task_id="prepare_input_location",
        python_callable=prepare_input_location,
        provide_context=True,
    )

    # task for Creating Cluster
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    # Sense change in Cluster from starting to Waiting
    wait_for_cluster = EmrJobFlowSensor(
        task_id="wait_for_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 30,
        target_states=["WAITING", "RUNNING"],
        failed_states=["TERMINATED_WITH_ERRORS", "TERMINATED"],
    )

    # task sending created Spark Job to EMR
    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        steps="{{ ti.xcom_pull(task_ids='build_spark_steps') }}",
        aws_conn_id=AWS_CONN_ID,
    )

    # This task calls def build_spark_steps function -> passes the spark job specs (via XCOM) to the above task add_spark_step
    # This is a necessary workaround to dynamically pass Input Paths of the new file added
    build_spark_steps_task = PythonOperator(
        task_id="build_spark_steps",
        python_callable=build_spark_steps,
        provide_context=True,
    )

    # Sensor to wait for Cluster status - Completed
    wait_for_step = EmrStepSensor(
        task_id="wait_for_step_completion",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('add_spark_step', key='return_value')[0] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=60 * 60 * 6,
    )

    # task to Terminate cluster in job completion
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done"
    )

    # task to update File Status
    update_status_task = PythonOperator(
    task_id='update_postgres_status',
    python_callable=update_postgres_status,
    provide_context=True
    )

    # task to trigger glue job via next dag
    trigger_glue = TriggerDagRunOperator(
    task_id="trigger_glue",
    trigger_dag_id="TRANSFORMED_S3_to_GLUE_ATHENA",
    wait_for_completion=False
    )

    # dependencies
    prepare_input >> create_cluster >> wait_for_cluster >> build_spark_steps_task >> add_spark_step >> wait_for_step
    wait_for_step >> terminate_cluster >> update_status_task >> trigger_glue

