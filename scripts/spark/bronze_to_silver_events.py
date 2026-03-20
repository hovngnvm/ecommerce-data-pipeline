import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, to_date, split

def main():
    spark = SparkSession.builder \
        .appName("ECommerce_Bronze_To_Silver") \
        .getOrCreate()
        
    INPUT_PATH = sys.argv[1]
    OUTPUT_PATH = sys.argv[2]
    QUARANTINE_PATH = sys.argv[3]

    # Instead of 'inferSchema=True' which is time-consuming for 5GB, we define the schema upfront.
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category_id", StringType(), True), 
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_session", StringType(), True)
    ])

    raw_df = spark.read.csv(INPUT_PATH, header=True, schema=schema)

    na_df = raw_df.filter(col("event_time").isNull() | col("event_type").isNull() | col("user_id").isNull() | col("product_id").isNull() |col("price").isNull())

    clean_df = raw_df \
        .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")) \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("category", split(col("category_code"), "\.").getItem(0)) \
        .withColumn("sub_category", split(col("category_code"), "\.").getItem(1)) \
        .fillna({"brand": "unknown", "category": "unknown", "sub_category": "unknown"}) \
        .drop("category_code") \
        .dropna(subset=["event_time", "event_type", "user_id", "product_id", "price"])

    na_df.write \
        .mode("overwrite") \
        .parquet(QUARANTINE_PATH)
    
    # Partition the data into folders by date (partitionBy)
    clean_df.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(OUTPUT_PATH)

    spark.stop()

if __name__ == "__main__":
    main()