# Yellow Cab Data Orchestrator

End-to-end, cross-platform ETL pipeline for the NYC Taxi trip data.

---

## 📝 Dataset and Purpose

We are using the **Annual Trips Done** raw datasets provided by unnamed yellow taxi Technology Service Providers (TSPs).

The **NYC Taxi Trip datasets** contains detailed records of taxi rides in New York City, including:  

- **Trip details:** pickup/dropoff timestamps, locations, trip distance  
- **Fare information:** fare amount, tip, total amount, and payment type  
- **Vendor information:** vendor ID, rate code, and other metadata  
- **Purpose:** Ideal for analyzing ride patterns, fare trends, outlier detection, and city-wide transportation insights  
- **Source:** Publicly available from the **NYC Taxi & Limousine Commission (TLC)** 

These datasets are **large scale and time series oriented**, making it perfect for demonstrating **ETL pipelines, cross platform orchestration, and big data analytics**.

They are available on **NYC OpenData Website**. Sample: [2022 Yellow Taxi Trip Data](https://data.cityofnewyork.us/Transportation/2022-Yellow-Taxi-Trip-Data/qp3b-zxtp/about_data)


---

## 📊 Pipeline Architecture

- Ingest raw CSV taxi trip data into a Landing Zone S3 Bucket.  
- Validate files added in bucket and track processing status (for idempotency) using PostgreSQL.  
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

## ⚙️ Tech Stack

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)  
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apache-spark&logoColor=white)  
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-red?logo=apache-airflow&logoColor=white)  
![AWS S3](https://img.shields.io/badge/AWS%20S3-orange?logo=amazon-aws&logoColor=white)  
![AWS EMR](https://img.shields.io/badge/AWS%20EMR-orange?logo=amazon-aws&logoColor=white)  
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-orange?logo=amazon-aws&logoColor=white)  
![Athena](https://img.shields.io/badge/Athena-purple?logo=amazon-aws&logoColor=white)  
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue?logo=postgresql&logoColor=white)  

---
