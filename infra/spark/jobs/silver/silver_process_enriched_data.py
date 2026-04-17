# File: infra/spark/jobs/silver/silver_process_enriched_data.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def create_spark_session():
    return (SparkSession.builder
            .appName("EnrichedToSilverTransformation")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate())

def main():
    spark = create_spark_session()
    
    # 1. Read raw enriched JSON from Bronze
    # Path uses the new snapshot structure created by the Airflow Collector
    bronze_path = "s3a://tourism-bronze/source=osm_google_enriched/snapshot_date=*/run_id=*/*.json"
    
    try:
        raw_df = spark.read.json(bronze_path)
    except Exception as e:
        print(f"Error reading bronze data: {e}. Might be empty.")
        spark.stop()
        return

    # 2. Flatten and Transform
    # We prioritize Google data for name/location/rating.
    # We check if fields exist in google_data to avoid AnalysisException.
    google_fields = []
    if "google_data" in raw_df.columns:
        fields = [f.name for f in raw_df.schema["google_data"].dataType.fields]
        
        silver_df = raw_df.select(
            F.col("osm_data.osm_id").alias("osm_id"),
            F.coalesce(F.col("google_data.name"), F.col("osm_data.name")).alias("name"),
            (F.col("google_data.formatted_address") if "formatted_address" in fields else F.lit(None)).alias("address"),
            F.coalesce(
                F.col("google_data.geometry.location.lat") if "geometry" in fields else F.lit(None), 
                F.col("osm_data.lat")
            ).cast(DoubleType()).alias("latitude"),
            F.coalesce(
                F.col("google_data.geometry.location.lng") if "geometry" in fields else F.lit(None), 
                F.col("osm_data.lon")
            ).cast(DoubleType()).alias("longitude"),
            (F.col("google_data.rating").cast(DoubleType()) if "rating" in fields else F.lit(None).cast(DoubleType())).alias("rating"),
            (F.col("google_data.user_ratings_total") if "user_ratings_total" in fields else F.lit(None).cast("int")).alias("review_count"),
            (F.col("google_data.website") if "website" in fields else F.lit(None).cast("string")).alias("website"),
            (F.col("google_data.international_phone_number") if "international_phone_number" in fields else F.lit(None).cast("string")).alias("phone"),
            (F.col("google_data.types") if "types" in fields else F.lit(None).cast("array<string>")).alias("poi_types"),
            (F.col("run_id") if "run_id" in raw_df.columns else F.lit("manual")).alias("run_id"),
            (F.col("snapshot_date") if "snapshot_date" in raw_df.columns else F.lit("1970-01-01")).alias("snapshot_date"),
            F.current_timestamp().alias("source_ingested_at"),
            F.current_timestamp().alias("silver_processed_at")
        ).filter("latitude IS NOT NULL AND longitude IS NOT NULL")
    else:
        silver_df = raw_df.select(
            F.col("osm_data.osm_id").alias("osm_id"),
            F.col("osm_data.name").alias("name"),
            F.lit(None).alias("address"),
            F.col("osm_data.lat").cast(DoubleType()).alias("latitude"),
            F.col("osm_data.lon").cast(DoubleType()).alias("longitude"),
            F.lit(None).cast(DoubleType()).alias("rating"),
            F.lit(None).cast("int").alias("review_count"),
            F.lit(None).alias("website"),
            F.lit(None).alias("phone"),
            F.lit(None).alias("poi_types"),
            (F.col("run_id") if "run_id" in raw_df.columns else F.lit("manual")).alias("run_id"),
            (F.col("snapshot_date") if "snapshot_date" in raw_df.columns else F.lit("1970-01-01")).alias("snapshot_date"),
            F.current_timestamp().alias("source_ingested_at"),
            F.current_timestamp().alias("silver_processed_at")
        ).filter("latitude IS NOT NULL AND longitude IS NOT NULL")
    
    # Clean name (Initcap)
    silver_df = silver_df.withColumn("name", F.initcap(F.trim(F.col("name"))))
    
    # Basic data quality: Deduplicate using deterministic window
    from pyspark.sql.window import Window
    
    silver_df = silver_df.withColumn(
        "business_key",
        F.coalesce(
            F.col("osm_id").cast("string"),
            F.sha2(F.concat_ws("_", F.lower(F.col("name")), F.round(F.col("latitude"), 4), F.round(F.col("longitude"), 4)), 256)
        )
    )
    
    window_spec = Window.partitionBy("business_key").orderBy(F.col("source_ingested_at").desc(), F.col("run_id").desc())
    silver_df = silver_df.withColumn("rn", F.row_number().over(window_spec)) \
                         .filter(F.col("rn") == 1) \
                         .drop("rn")

    # 3. Write to Silver Iceberg Table
    # Using 'createOrReplace' for initial setup, but in prod we use 'mergeInto' or 'append'
    silver_df.writeTo("iceberg.silver.attractions_enriched") \
        .tableProperty("write.format.default", "parquet") \
        .partitionedBy(F.days("silver_processed_at")) \
        .createOrReplace()

    print(f"✅ Successfully processed {silver_df.count()} enriched records into Silver layer.")
    spark.stop()

if __name__ == "__main__":
    main()
