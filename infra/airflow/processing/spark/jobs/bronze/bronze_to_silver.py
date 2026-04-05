"""
PySpark job: Bronze → Silver layer.

Đọc JSON thô từ MinIO Bronze, làm sạch, ghi Silver (Iceberg).
Chạy qua spark-submit; đường dẫn trong container: /opt/spark/jobs/bronze_to_silver.py
"""
import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BRONZE_BUCKET = "s3a://tourism-bronze"
SILVER_BUCKET = "s3a://tourism-silver"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("HanoiTourism_BronzeToSilver")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", f"{SILVER_BUCKET}/iceberg_warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def clean_attractions(spark: SparkSession, date_str: str):
    """Bronze → Silver: Google Places attractions data."""
    logger.info(f"Processing attractions for date={date_str}")

    bronze_path = f"{BRONZE_BUCKET}/source=google_places/date={date_str}/"
    try:
        df_raw = spark.read.json(bronze_path)
    except Exception as e:
        logger.error(f"Cannot read bronze path {bronze_path}: {e}")
        return

    logger.info(f"Raw records: {df_raw.count()}")

    df = df_raw.select(
        F.col("place_id"),
        F.col("name"),
        F.col("vicinity").alias("address"),
        F.col("rating").cast(DoubleType()),
        F.col("user_ratings_total").cast(IntegerType()).alias("total_ratings"),
        F.col("price_level").cast(IntegerType()),
        F.col("geometry.location.lat").cast(DoubleType()).alias("latitude"),
        F.col("geometry.location.lng").cast(DoubleType()).alias("longitude"),
        F.col("types").cast(StringType()),
        F.col("fetched_type").alias("primary_category"),
        F.col("business_status"),
        F.col("ingested_at"),
        F.lit(date_str).alias("partition_date"),
    )

    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("place_id")
        .orderBy(F.col("ingested_at").desc())
    )
    df = (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    logger.info(f"After dedup: {df.count()}")

    df = df.filter(
        F.col("place_id").isNotNull()
        & F.col("name").isNotNull()
        & (F.col("latitude").between(-90, 90))
        & (F.col("longitude").between(-180, 180))
    )

    df = df.withColumn(
        "rating",
        F.when(F.col("rating").between(0, 5), F.col("rating")).otherwise(None),
    )

    df = df.withColumn(
        "district",
        F.regexp_extract(F.col("address"), r"(Quận|Huyện|Thị xã)\s+([\w\s]+),", 2),
    ).withColumn(
        "district",
        F.when(F.col("district") == "", None).otherwise(F.trim(F.col("district"))),
    )

    df = df.withColumn(
        "is_overcrowded",
        F.col("total_ratings") > 1000,
    )

    df = df.withColumn("processed_at", F.current_timestamp())

    (
        df.writeTo("local.silver.attractions")
        .using("iceberg")
        .partitionedBy("partition_date")
        .createOrReplace()
    )
    logger.info(f"Silver attractions written. Records: {df.count()}")


def clean_weather(spark: SparkSession, date_str: str):
    """Bronze → Silver: OpenWeather data."""
    logger.info(f"Processing weather for date={date_str}")

    bronze_path = f"{BRONZE_BUCKET}/source=openweather/date={date_str}/"
    try:
        df_raw = spark.read.json(bronze_path)
    except Exception as e:
        logger.warning(f"No weather data for {date_str}: {e}")
        return

    df = df_raw.select(
        F.col("temperature_celsius").cast(DoubleType()),
        F.col("feels_like").cast(DoubleType()),
        F.col("humidity").cast(IntegerType()),
        F.col("weather_condition"),
        F.col("weather_description"),
        F.col("wind_speed").cast(DoubleType()),
        F.to_timestamp(F.col("snapshot_time")).alias("snapshot_time"),
        F.lit(date_str).alias("partition_date"),
        F.md5(F.col("snapshot_time")).alias("snapshot_id"),
    )

    df.writeTo("local.silver.weather_snapshots").using("iceberg").partitionedBy(
        "partition_date"
    ).createOrReplace()

    logger.info(f"Silver weather written. Records: {df.count()}")


def run_quality_report(spark: SparkSession, date_str: str):
    """In báo cáo chất lượng dữ liệu ra log."""
    try:
        df = spark.read.table("local.silver.attractions")
        df_today = df.filter(F.col("partition_date") == date_str)
        total = df_today.count()

        report = {
            "total_records": total,
            "null_rating_pct": round(
                df_today.filter(F.col("rating").isNull()).count() / max(total, 1) * 100, 2
            ),
            "null_district_pct": round(
                df_today.filter(F.col("district").isNull()).count() / max(total, 1) * 100, 2
            ),
            "avg_rating": df_today.agg(F.avg("rating")).collect()[0][0],
            "overcrowded_count": df_today.filter(F.col("is_overcrowded")).count(),
        }
        logger.info(f"Quality Report [{date_str}]: {report}")
        return report
    except Exception as e:
        logger.warning(f"Quality report failed: {e}")
        return {}


def main():
    parser = argparse.ArgumentParser(description="Bronze to Silver transformation")
    parser.add_argument("--date", default=date.today().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    logger.info(f"Starting Bronze→Silver for date={args.date}")
    spark = create_spark_session()

    try:
        clean_attractions(spark, args.date)
        clean_weather(spark, args.date)
        run_quality_report(spark, args.date)
        logger.info("Bronze→Silver completed successfully")
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
