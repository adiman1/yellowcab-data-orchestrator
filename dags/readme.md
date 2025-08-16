This document explains the flow of execution of the dags.
[](https://github.com/adiman1/yellowcab-data-orchestrator/blob/d3bf102f600a389e40b5d4836aac1f51fee83a6d/media/airflow_orc-page-001.jpg)
---

## **1. `s3_to_emr` DAG**
This DAG detects new files in S3 and initiates the EMR processing workflow.

### **Tasks**
1. **Check for New Files** (`PythonOperator`)
   - Scans the landing S3 bucket for unprocessed CSV files.
   - Compares the file name against the tracking table in PostgreSQL.

2. **Branch Decision** (`BranchPythonOperator`)
   - **No new files** → Runs `no_new_files_message` and stops.
   - **Too many files** → Runs `too_many_files_message` and stops.
   - **Exactly one new file** → Runs `inserted_one_file_message` and **proceeds**.

3. **Insert File Record in PostgreSQL** (`PythonOperator`)
   - Inserts metadata for the file (name, status, created timestamp) into tracking table.

4. **Trigger `emr_to_clean_s3` DAG** (`TriggerDagRunOperator`)
   - Passes file name and metadata as parameters to downstream DAG.

---

## **2. `emr_to_clean_s3` DAG**
This DAG handles the main data processing on EMR, converting raw CSV to cleaned Parquet.

### **Tasks**
1. **Prepare Input Location** (`PythonOperator`)
   - Creates temporary S3 paths for processing.
   - Validates input file existence.

2. **Create EMR Cluster** (`EmrCreateJobFlowOperator`)
   - Spins up a transient EMR cluster with the required Spark configuration.

3. **Wait for Cluster** (`EmrJobFlowSensor`)
   - Waits until the EMR cluster is ready for job submission.

4. **Build Spark Steps** (`PythonOperator`)
   - Defines Spark jobs for data cleaning, type conversions, filtering, and partitioning.

5. **Add Spark Step** (`EmrAddStepsOperator`)
   - Submits Spark jobs to the EMR cluster.

6. **Wait for Step Completion** (`EmrStepSensor`)
   - Monitors Spark job completion status.

7. **Terminate EMR Cluster** (`EmrTerminateJobFlowOperator`)
   - Shuts down the EMR cluster to save costs.

8. **Update Postgres Status** (`PythonOperator`)
   - Marks file processing status as `done` in PostgreSQL tracking table.

9. **Trigger `glue_job` DAG** (`TriggerDagRunOperator`)
   - Initiates Glue workflow for cataloging the cleaned data.

---

## **3. `glue_job` DAG**
This DAG catalogs the cleaned Parquet data for query access in Athena.

### **Tasks**
1. **Trigger Glue Crawler** (`GlueCrawlerOperator`)
   - Starts a Glue crawler to scan cleaned S3 Parquet files.
   - Updates the Glue Data Catalog with schema metadata.

2. **Wait for Crawler Completion** (`GlueCrawlerSensor`)
   - Ensures the Glue crawler finishes before allowing downstream queries.

### **Output**
- Athena can now query the cleaned, partitioned Yellow Taxi data efficiently.
