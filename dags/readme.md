## Execution Flow

```
Raw Data (S3)
     │
     ▼
s3_to_emr DAG
     │
     ├─ Decide Next Task (BranchPythonOperator)
     │       ├─ No New Files (PythonOperator: no_new_files_message)
     │       ├─ Too Many Files (PythonOperator: too_many_files_message)
     │       └─ Exactly One New File (PythonOperator: inserted_one_file_message)
     │                │
     │                ▼
     │           Trigger EMR_to_Transformed_S3 DAG (TriggerDagRunOperator)
     │
     ▼
emr_to_cleaned_s3 DAG
     │
     ├─ Prepare Input Location (PythonOperator)
     ├─ Create EMR Cluster (EmrCreateJobFlowOperator)
     ├─ Wait for Cluster (EmrJobFlowSensor)
     ├─ Build Spark Steps (PythonOperator)
     ├─ Add Spark Step (EmrAddStepsOperator)
     ├─ Wait for Step Completion (EmrStepSensor)
     ├─ Terminate EMR Cluster (EmrTerminateJobFlowOperator)
     ├─ Update Postgres Status (PythonOperator)
     └─ Trigger Glue DAG (TriggerDagRunOperator)
           │
           ▼
glue_job DAG
           │
           ├─ Trigger Glue Crawler (GlueCrawlerOperator)
           └─ Wait for Crawler Completion (GlueCrawlerSensor)
           │
           ▼
     Athena Queryable Table
```

