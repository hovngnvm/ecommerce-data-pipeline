import json, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_date
def main():
    spark = SparkSession.builder \
        .appName("API_Bronze_To_Silver") \
        .getOrCreate()
        
    INPUT_FILE = sys.argv[1]
    OUTPUT_PATH = sys.argv[2]    
    
    # spark.read.option("multiline", "true").json(INPUT_FILE)
    with open(INPUT_FILE, 'r') as f:
        raw_api = json.load(f)
        
    # Flatten the JSON data
    rates_list = []
    for date_str, rates in raw_api.get('rates', {}).items():
        rates_list.append({
            "event_date": date_str,  
            "rate_EUR": float(rates.get("EUR", 1.0)),
            "rate_JPY": float(rates.get("JPY", 1.0))
        })
    
    # Define Schema for API data
    schema_api = StructType([
        StructField("event_date", StringType(), True),
        StructField("rate_EUR", DoubleType(), True),
        StructField("rate_JPY", DoubleType(), True)
    ])

    rates_df = spark.createDataFrame(rates_list, schema=schema_api) \
                    .withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))
    
    # Save as Parquet, since the data is small, coalesce to 1 partition to optimize I/O
    rates_df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH)

    spark.stop()

if __name__ == "__main__":
    main()