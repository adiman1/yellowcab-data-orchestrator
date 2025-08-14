# Yellow Cab Data Orchestrator

End-to-end, cross-platform ETL pipeline for NYC Taxi trip data using S3, Postgres, EMR, Glue/Athena, orchestrated with Airflow

---

## 📝 Purpose

- Ingest raw CSV taxi trip data into a Landing Zone S3 Bucket.  
- Validate files added in bucket and track processing using PostgreSQL.  
- Process and aggregate data added files on EMR using Spark.
- Store cleaned/aggregated data back in a Transformed S3 Bucket.
- Crawl the transformed bucket and Make processed data queryable via AWS Glue and Athena.  
- Orchestrate all workflows, dependencies, and monitoring via Airflow.  

---

## 🗂️ Project Structure

```

airflow-orc/
│
├── dags/                        # Airflow DAGs (E2E workflows)
│   ├── emr_s3.py                # DAG: EMR → S3
│   ├── s3_emr.py                # DAG: S3 → EMR
│   └── glue_job.py              # DAG: Glue ETL job
│
├── logs/                        # Airflow logs
├── plugins/                     # Airflow custom operators/hooks (none used here)
├── .env                         # Environment variables for connections
├── docker-compose.yaml          # Containerized Airflow setup
└── README.md                    # Project description & instructions

```
---

## ⚡ Tech Stack

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)  
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apache-spark&logoColor=white)  
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-red?logo=apache-airflow&logoColor=white)  
![AWS S3](https://img.shields.io/badge/AWS%20S3-orange?logo=amazon-aws&logoColor=white)  
![AWS EMR](https://img.shields.io/badge/AWS%20EMR-orange?logo=amazon-aws&logoColor=white)  
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-orange?logo=amazon-aws&logoColor=white)  
![Athena](https://img.shields.io/badge/Athena-purple?logo=amazon-aws&logoColor=white)  
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue?logo=postgresql&logoColor=white)  
![License](https://img.shields.io/badge/License-MIT-lightgrey)

---
