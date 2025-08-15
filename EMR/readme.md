## ⚡ EMR/Spark - Data Transformations Done & Rationale

- The raw NYC Taxi data is transformed to ensure quality and analytics readiness for further downstream purposes.
- The code for the same is available in this folder. Use this in the EMR Step.

Transformations done:

1. **Extract Year & Month:** Derived from filename and pickup datetime to enable time-based filtering and partitioning.  
2. **Timestamp Conversion:** Converts pickup/dropoff strings to timestamps for accurate duration calculations.  
3. **Filter Invalid Trips:** Removes trips with zero distance, negative duration, or zero passengers.  
4. **Compute Effective Fare:** Aggregates fare components to handle missing values.  
5. **Outlier Removal:** Filters extreme values using percentiles to improve data reliability.  
6. **Map Payment Type & Flags:** Converts codes to meaningful labels and boolean flags.  
7. **Join Lookup Data:** Enriches pickup location info with borough, zone, and service zone.  
8. **Output:** Writes clean data to S3 in Parquet, partitioned by month.

---

## Some Example Benefits

### 1) Compression 

Converting CSV to Parquet **reduces file sizes by roughly 1/4**, improving storage efficiency and query performance.

   **a) Original CSV Sizes:**  
   ![Original CSV Sizes](https://github.com/adiman1/yellowcab-data-orchestrator/raw/f5fe7e518bf0b8e9900fc5ad6221bc9fe6d5a65f/media/raw_sizes_csv.png)

   **b) Parquet File Sizes:**  
   ![Parquet Folders Size](https://github.com/adiman1/yellowcab-data-orchestrator/blob/5558b508430bfdac1d8b2d2d56fac6ce78f5b6f6/media/parquet_folders_sizes.png)

   **c) Partition Parquet Structure:**
   ![Partitioned files](https://github.com/adiman1/yellowcab-data-orchestrator/blob/7d71bd8817fbbb0ff5ca1b5c427c0f1d2fd8ec15/media/parquet_files_in_partition.PNG)

---

### 2) Partitioning

Parquet files are partitioned by `month`, which improves query performance by **reading only relevant partitions** instead of the full dataset.

  **a) Reading via Existing Timestamp Column:**
  ![Query via Normal Column](https://github.com/adiman1/yellowcab-data-orchestrator/raw/f3826e245e3dd8d85b295ccfc4fd1e0a2b44c5ad/media/raw_query_on_existingcol.PNG)

```text
Reading Only 2022 data. Both queries were done post 1st DAG Run.
```

  **b) Reading via Partition:**
  ![Query via Partition](https://github.com/adiman1/yellowcab-data-orchestrator/raw/f3826e245e3dd8d85b295ccfc4fd1e0a2b44c5ad/media/query_via_partition.PNG)

---

### 3) Improved Data Quality

The transformations help us clear outlier values that would affect the analysis on the data. 

For e.g. 
- In 2023, the minimum fare in NYC would start around 5 usd.
- Why ? - Cause of the mandatory base fare and taxes paid.
- Hence fares having values less than this does not make sense.
- Therfore using percentiles [0.005, 0.995] we clean the very extreme outliers
- Similarly we tracked same location dropoffs, zero or negative travel distances and other similar issues and removed them.
  
