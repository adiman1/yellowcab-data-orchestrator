## ⚡ EMR/Spark - Data Transformations Done & Rationale

The raw NYC Taxi data is transformed to ensure quality and analytics readiness:

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

**Compression:** Converting CSV to Parquet reduces file sizes by roughly 1/4, improving storage efficiency and query performance.
&nbsp;

   **Original CSV Sizes:**  
   ![Original CSV Sizes](https://github.com/adiman1/yellowcab-data-orchestrator/raw/f5fe7e518bf0b8e9900fc5ad6221bc9fe6d5a65f/media/raw_sizes_csv.png)


   **Parquet File Sizes:**  
   ![Parquet Folders Size](https://github.com/adiman1/yellowcab-data-orchestrator/blob/5558b508430bfdac1d8b2d2d56fac6ce78f5b6f6/media/parquet_folders_sizes.png)

---

**Partitioning:** Parquet files are partitioned by `month`, which improves query performance by reading only relevant partitions instead of the full dataset.

  **Reading via Existing Timestamp Column:**
  ![Query via Normal Column](https://github.com/adiman1/yellowcab-data-orchestrator/raw/f3826e245e3dd8d85b295ccfc4fd1e0a2b44c5ad/media/raw_query_on_existingcol.PNG)
  

  **Reading via Partition:**
  ![Query via Partition](https://github.com/adiman1/yellowcab-data-orchestrator/raw/f3826e245e3dd8d85b295ccfc4fd1e0a2b44c5ad/media/query_via_partition.PNG)
