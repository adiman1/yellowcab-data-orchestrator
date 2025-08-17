# Yellow Cab Data Orchestrator

End-to-end, cross-platform ETL pipeline for the NYC Taxi trip data.

**Click on image to view the orchestration**

[![Click to watch the Orchestration Flow](https://github.com/adiman1/yellowcab-data-orchestrator/blob/eaec50acc14122c666d5aebb8457e821d0cb4b18/media/airflow_orc_f1.jpg)](https://drive.google.com/file/d/1UU1n2W-04XUJgCoFnwu9zQH2Fp8T259i/view?usp=drive_link)

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

## 🗂️ Project Structure

```
airflow-orc/
│
├── dags/                        # Airflow DAGs (E2E workflows)
│   ├── s3_to_emr.py             # DAG: Raw S3 → EMR
│   ├── emr_to_cleaned_s3.py     # DAG: EMR → Cleaned S3
│   └── glue_job.py              # DAG: Glue Crawler job to read new data
│
├── logs/                        # Airflow logs
├── plugins/                     # Airflow custom operators/hooks (none used here)
├── .env                         # Environment variables for connections
├── docker-compose.yaml          # Containerized Airflow setup
└── README.md                    # Project description & instructions

```

The project file structure will be **automatically generated** when you run the Docker Compose setup. Just add the dags, env variables.

---

## ⚙️ Setup

1. **Configure AWS credentials** to access your IAM account for S3, EMR, Glue, and Athena.  
- These can be set in the `.env` file or directly in **Airflow Connections**.  
- Ensure the IAM user/role has sufficient permissions for:

   - Reading/writing to S3 buckets  
   - Submitting jobs to EMR  
   - Running Glue crawlers  
   - Querying Athena

2. Install and run **Docker Desktop**

3. **Run Docker compose file** via your preferred IDE to start Airflow and PostgreSQL:

```bash
docker-compose up --build
```

4. **Access Airflow UI** at [http://localhost:8080](http://localhost:8080) to:

- Trigger DAGs
- Monitor task progress
- Manage connections and credentials
- Upload raw CSV files to your Landing S3 bucket to start the ETL workflow

5. **Upload raw CSV** files to your Landing S3 bucket to start the ETL workflow
   
---

**Note:** 
- This Project was done with minimal config for quick execution
- This Project was done using a Lightweight version of the Official Airflow image 
- For Prod, we'd config stricter execution roles, specific VPC's, EC2 key pair (if SSH needed) etc

---

## (🎶) Orchestrating the Project

- Trigger Airflow DAGs to start the ETL workflow.  
- DAGs perform: 
   - Validate file added in Landing Zone S3 Bucket and track processing status (for idempotency) using PostgreSQL.  
   - Processing of added file on EMR using Spark.
   - Store the cleaned data back in a Transformed S3 Bucket.
   - Crawl the transformed bucket and Make processed data queryable via AWS Glue and Athena.   

**Note:** This Pipeline was built for batch processing.

---

## 📕 Database Integration

- PostgreSQL tracks processed files for **idempotency**  
- Stores metadata about ingestion, processing timestamps, and DAG status

---

## 🔜 Future Improvements
 
- Add **Slack/Email alerts** for DAG failures  
- Parameterize DAGs for multi-year/month processing
- Integration with Columnar DB's (e.g. Redshift)
- Integration with BI dashboards (e.g., QuickSight, Tableau)

---

## 🖥️ Tech Stack

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)  
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-blue?logo=apache-airflow&logoColor=white)  
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue?logo=postgresql&logoColor=white) 
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apache-spark&logoColor=white)  
![AWS S3](https://img.shields.io/badge/AWS%20S3-green?logo=amazon-aws&logoColor=white)  
![AWS EMR](https://img.shields.io/badge/AWS%20EMR-green?logo=amazon-aws&logoColor=white)  
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-green?logo=amazon-aws&logoColor=white)  
![AWS Athena](https://img.shields.io/badge/Athena-green?logo=amazon-aws&logoColor=white)  
![AWS Redshift](https://img.shields.io/badge/Redshift-blue?logo=amazon-aws&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-yellow?logo=power-bi&logoColor=black)

---

## Improvements implemented

**1) Redshift**

- The 2 converted parquet folders for 2022 & 2023 annual data contain about 70 Million records
- Hence a columnar DB is needed for fast aggregation queries
- Therefore Redshift integrated. Sample query below

![Integration with Redshift](https://github.com/adiman1/yellowcab-data-orchestrator/blob/1a251fb26013350979ca6b75bcfe34b6295fd94d/media/redshift_integration.png)

---

**2) PBI**

Integration with PBI established to create a Dashboard to show important annual metrics on revenue and location basis

![Integration with PBI](https://github.com/adiman1/yellowcab-data-orchestrator/blob/6898e1c246dff2a079ba4cd5384113e82c5b62da/media/pbi_e2e_aflw.jpg)

