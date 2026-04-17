# File: infra/spark/jobs/gold/gold_process_tourism_marts.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    return (SparkSession.builder
            .appName("GoldMartsGeneration")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate())

def extract_district(address):
    if not address: return "Khác"
    hanoi_districts = [
        "Hoàn Kiếm", "Ba Đình", "Tây Hồ", "Hai Bà Trưng", "Đống Đa", 
        "Cầu Giấy", "Thanh Xuân", "Long Biên", "Hoàng Mai", "Nam Từ Liêm", 
        "Bắc Từ Liêm", "Hà Đông", "Sơn Tây", "Ba Vì", "Chương Mỹ", 
        "Phúc Thọ", "Đan Phượng", "Đông Anh", "Gia Lâm", "Hoài Đức", 
        "Mê Linh", "Mỹ Đức", "Phú Xuyên", "Quốc Oai", "Sóc Sơn", 
        "Thạch Thất", "Thanh Oai", "Thanh Trì", "Thường Tín", "Ứng Hòa"
    ]
    for d in hanoi_districts:
        if d.lower() in address.lower():
            return d
    return "Khác"

def main():
    spark = create_spark_session()
    
    # Register UDF for district extraction
    district_udf = F. udf(extract_district)
    
    # 1. Load Silver Data
    silver_df = spark.table("iceberg.silver.attractions_enriched")
    
    # Enrich silver with district (if not already present)
    silver_with_district = silver_df.withColumn("district", district_udf(F.col("address")))
    
    # 2. Mart 1: District Statistics
    district_stats = silver_with_district.groupBy("district").agg(
        F.count("osm_id").alias("total_attractions"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("review_count").alias("total_reviews"),
        F.first("name").alias("featured_attraction")
    ).orderBy(F.desc("total_attractions"))
    
    district_stats.writeTo("iceberg.gold.mart_district_stats") \
        .tableProperty("write.format.default", "parquet") \
        .createOrReplace()
        
    # 3. Mart 2: Tourism Heatmap
    # Popularity Score = Rating * log10(Review Count + 1)
    heatmap_df = silver_with_district.select(
        "name", "latitude", "longitude", "rating", "review_count", "district"
    ).withColumn(
        "popularity_score", 
        F.round(F.col("rating") * F.log10(F.col("review_count") + 1), 2)
    )
    
    heatmap_df.writeTo("iceberg.gold.mart_tourism_heatmap") \
        .tableProperty("write.format.default", "parquet") \
        .createOrReplace()

    print("✅ Gold Marts generated successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
