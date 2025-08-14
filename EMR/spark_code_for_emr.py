import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# 1) Define schema
schema = T.StructType(
    [
        T.StructField("vendor_id", T.ByteType(), True),
        T.StructField("tpep_pickup_datetime", T.StringType(), True),
        T.StructField("tpep_dropoff_datetime", T.StringType(), True),
        T.StructField("passenger_count", T.ByteType(), True),
        T.StructField("trip_distance", T.FloatType(), True),
        T.StructField("rate_code_id", T.ByteType(), True),
        T.StructField("store_and_fwd_flag", T.StringType(), True),
        T.StructField("pu_location_id", T.ShortType(), True),
        T.StructField("do_location_id", T.ShortType(), True),
        T.StructField("payment_type", T.ByteType(), True),
        T.StructField("fare_amount", T.FloatType(), True),
        T.StructField("extra", T.FloatType(), True),
        T.StructField("mta_tax", T.FloatType(), True),
        T.StructField("tip_amount", T.FloatType(), True),
        T.StructField("tolls_amount", T.FloatType(), True),
        T.StructField("improvement_surcharge", T.FloatType(), True),
        T.StructField("total_amount", T.FloatType(), True),
        T.StructField("congestion_surcharge", T.FloatType(), True),
    ]
)


def main(input_csv, output_path, lookup_csv):
    spark = SparkSession.builder.appName("NYC Taxi ETL EMR").getOrCreate()

    # 2) Extract year from filename
    file_name = input_csv.split("/")[-1]
    year = int(file_name.split("_")[0])

    # 3) Read main CSV from S3
    df = spark.read.option("header", True).schema(schema).csv(input_csv)

    # 4) Convert pickup/dropoff datetime to timestamp & extract month
    df = df.withColumn(
        "tpep_pickup_datetime",
        F.to_timestamp("tpep_pickup_datetime", "MM/dd/yyyy hh:mm:ss a"),
    )
    df = df.withColumn(
        "tpep_dropoff_datetime",
        F.to_timestamp("tpep_dropoff_datetime", "MM/dd/yyyy hh:mm:ss a"),
    )
    df = df.withColumn("month", F.month("tpep_pickup_datetime"))

    # 5) Filter by year
    df = df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(f"{year}-01-01 00:00:00"))
        & (F.col("tpep_pickup_datetime") < F.lit(f"{year + 1}-01-01 00:00:00"))
    )

    # 6) Filter trips with positive distance, duration, passengers
    df = df.filter(
        (F.col("trip_distance") > 0) &
        ((F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) > 0) &
        (F.col("passenger_count") >= 1)
    )

    # 7) Compute effective fare
    df = df.withColumn(
        "effective_fare",
        F.coalesce(F.col("fare_amount"), F.lit(0.0))
        + F.coalesce(F.col("extra"), F.lit(0.0))
        + F.coalesce(F.col("mta_tax"), F.lit(0.0))
        + F.coalesce(F.col("improvement_surcharge"), F.lit(0.0))
        + F.coalesce(F.col("congestion_surcharge"), F.lit(0.0)),
    )

    # 8) Safe min_fare
    filtered_df = df.filter(F.col("effective_fare") > 0)
    min_fare = 0.0
    if not filtered_df.rdd.isEmpty():
        min_fare_list = filtered_df.approxQuantile("effective_fare", [0.01], 0.001)
        if min_fare_list:
            min_fare = min_fare_list[0]

    # 9) Percentiles for outlier removal
    percentiles = [0.005, 0.995]
    cols_to_clean = [
        "trip_distance",
        "total_amount",
        "fare_amount",
        "tip_amount",
        "extra",
        "tolls_amount",
        "improvement_surcharge",
    ]
    quantile_values = df.approxQuantile(cols_to_clean, percentiles, 0.001)

    # 10) Build safe quantile dict
    quantile_dict = {}
    for i, col in enumerate(cols_to_clean):
        if quantile_values[i]:
            quantile_dict[col] = quantile_values[i]
        else:
            quantile_dict[col] = [0.0, float("inf")]

    # 11) Filter outliers
    df = df.filter(
        (F.col("trip_distance") >= quantile_dict["trip_distance"][0])
        & (F.col("trip_distance") <= quantile_dict["trip_distance"][1])
        & (F.col("total_amount") >= max(quantile_dict["total_amount"][0], min_fare))
        & (F.col("total_amount") <= quantile_dict["total_amount"][1])
    )

    # 12) Map payment_type codes
    payment_type_map = {
        0: "Unknown",
        1: "Credit Card",
        2: "Cash",
        3: "No Charge",
        4: "Dispute",
        5: "Voided Trip",
    }
    mapping_expr = F.create_map(*[F.lit(kv) for kv in sum(payment_type_map.items(), ())])
    df = df.withColumn("payment_type_name", mapping_expr[F.col("payment_type")])

    # 13) Convert store_and_fwd_flag to boolean
    df = df.withColumn(
        "store_and_fwd_flag",
        F.when(F.col("store_and_fwd_flag") == "Y", True).otherwise(False),
    )

    # 14) Read lookup CSV from S3, alias joining cols & broadcast join
    df_lookup = (
        spark.read.option("header", True)
        .csv(lookup_csv)
        .select(
            F.col("LocationID").alias("pu_location_id"),
            "Borough",
            "Zone",
            "service_zone",
        )
    )

    df = df.join(F.broadcast(df_lookup), on="pu_location_id", how="left")

    # 15) Drop effective_fare
    df = df.drop("effective_fare")

    # 16) Write to S3 in Parquet partitioned by month
    output_full_path = f"{output_path}/year={year}"
    df.write.mode("overwrite").partitionBy("month").parquet(output_full_path)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark-submit test_emr.py <input_path> <output_path> <lookup_csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_path = sys.argv[2]
    lookup_csv = sys.argv[3]

    main(input_csv, output_path, lookup_csv)
