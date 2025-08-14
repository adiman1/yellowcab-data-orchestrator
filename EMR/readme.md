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

1. **Compression:** A 1/4th reduction in File Sizes.
   
![Original CSV Sizes](https://github.com/adiman1/yellowcab-data-orchestrator/raw/f5fe7e518bf0b8e9900fc5ad6221bc9fe6d5a65f/media/raw_sizes_csv.png)

![Parquet Folders Size](https://github.com/adiman1/yellowcab-data-orchestrator/raw/f5fe7e518bf0b8e9900fc5ad6221bc9fe6d5a65f/media/parquet_folders_size.png)


