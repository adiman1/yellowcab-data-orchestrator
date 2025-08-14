# ![Taxi Icon](https://img.icons8.com/color/48/000000/taxi.png) YellowCab Data Orchestrator

**End-to-end, cross-platform ETL pipeline for NYC Taxi trip data using S3, Postgres, EMR, Glue/Athena, orchestrated with Airflow**

---

## Badges

![Python](https://img.shields.io/badge/Python-3.11-blue)  
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange)  
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-red)  
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20EMR%20%7C%20Glue-green)  
![License](https://img.shields.io/badge/License-MIT-lightgrey)  

---

## Project Overview

`yellowcab-data-orchestrator` automates the **full lifecycle of NYC taxi trip data processing**: ingestion, validation, transformation, aggregation, and analytics. It demonstrates **cloud-native ETL best practices** and **cross-platform orchestration** using Airflow.  

**High-level pipeline:**  

```text
S3 (raw CSVs) 
   ↓
PostgreSQL (file validation & tracking)
   ↓
EMR (Spark processing & aggregation)
   ↓
S3 (processed output)
   ↓
Glue / Athena (query & analytics)
   ↑
Airflow (orchestration & monitoring)
