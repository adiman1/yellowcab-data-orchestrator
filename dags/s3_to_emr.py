from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from dotenv import load_dotenv

load_dotenv()

# added as Connections in Airflow UI
POSTGRES_CONN_ID = "local_postgres"
AWS_CONN_ID = "aws_default"

BUCKET_NAME = os.getenv("BUCKET_NAME")
PREFIX_FOLDER = os.getenv("PREFIX_FOLDER")

# Function to get files prev run via this dag to avoid reprocessing.
def get_existing_files_from_db():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.execute("SELECT file_name FROM s3_file_tracker")
    existing_files = set(row[0] for row in cur.fetchall())
    cur.close()
    conn.close()
    return existing_files

# Current file names in s3
def get_files_from_s3():
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    files = s3.list_keys(bucket_name=BUCKET_NAME, prefix=PREFIX_FOLDER) or []
    s3_filenames = set()
    for f in files:
        if f:
            short_name = f.split("/")[-1].strip() # new_york/input/2022_taxi_data.csv -> we parse only "2022_taxi_data.csv"
            if short_name:
                s3_filenames.add(short_name)
    return s3_filenames

# Difference to find new files
def find_new_files(existing_files, s3_filenames):
    return s3_filenames - existing_files

# Insert new files to be processed logic
def insert_new_file_to_db(new_files):
    if not new_files:
        return
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    for fname in new_files:
        cur.execute(
            "INSERT INTO s3_file_tracker (file_name, status) VALUES (%s, 'new')",
            (fname,)
        )
    conn.commit()
    cur.close()
    conn.close()

# Checking idempotency via use of the above helper functions done here, then branched as per scenario using return values.
def check_and_branch(**context):
    existing_files = get_existing_files_from_db()
    s3_filenames = get_files_from_s3()
    new_files = find_new_files(existing_files, s3_filenames)

    new_files_count = len(new_files)
    print(f"New files found: {new_files_count}")

    # for no new file  
    if new_files_count == 0:
        return "no_new_files_msg"

    # for multiple files
    if new_files_count > 1:
        context['ti'].xcom_push(key='new_files', value=list(new_files))
        return "too_many_files_msg"
    
    # for single file
    single_file = list(new_files)[0]
    context['ti'].xcom_push(key='single_file', value=single_file)

    insert_new_file_to_db(new_files)
    return "inserted_one_file_msg"

# different scenarios
def no_new_files_message():
    print("No new files found in S3 compared to PostgreSQL.")

def too_many_files_message(**context):
    new_files = context['ti'].xcom_pull(key='new_files', task_ids='decide_next_task')
    print(f"Too many new files found: {new_files}. Please review manually.")

def inserted_one_file_message(**context):
    single_file = context['ti'].xcom_pull(key='single_file', task_ids='decide_next_task')
    print(f"Exactly one new file inserted: {single_file}")
    return single_file

# dag definition
with DAG(
    dag_id="Raw_S3_to_EMR",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["Project", "s3", "postgres", "metadata"],
) as dag:

    # branching task
    decide_next_task = BranchPythonOperator(
        task_id="decide_next_task",
        python_callable=check_and_branch,
        provide_context=True,
    )

    # when above task returns -> no_new_files_msg -> appropriate func [no_new_files_message] executes
    no_new_files_msg = PythonOperator(
        task_id="no_new_files_msg",
        python_callable=no_new_files_message,
    )

    too_many_files_msg = PythonOperator(
        task_id="too_many_files_msg",
        python_callable=too_many_files_message,
        provide_context=True,
    )

    inserted_one_file_msg = PythonOperator(
        task_id="inserted_one_file_msg",
        python_callable=inserted_one_file_message,
        provide_context=True
    )

    # task to send file location to EMR for processing
    trigger_emr = TriggerDagRunOperator(
        task_id="trigger_emr",
        trigger_dag_id="EMR_to_Transformed_S3",
        conf={
            "input_file": "{{ ti.xcom_pull(task_ids='inserted_one_file_msg', key='return_value') }}"
        },
        wait_for_completion=False
    )

    # dependencies
    decide_next_task >> [no_new_files_msg, too_many_files_msg, inserted_one_file_msg]
    inserted_one_file_msg >> trigger_emr
