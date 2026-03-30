import os, sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Silver_To_RDBMS") \
        .getOrCreate()
    
    ecommerce_events_silver_path = sys.argv[1]
    exchange_rate_silver_path = sys.argv[2]
    
    cleaned_df = spark.read.parquet(ecommerce_events_silver_path)
    exchange_rates_df = spark.read.parquet(exchange_rate_silver_path)
    
    # DB connection info
    db_url = f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB')}"
    db_properties = {
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
        "batchsize": "10000"
    }

    cleaned_df.write \
        .jdbc(url=db_url,
              table="ecommerce_data",
              mode="overwrite", # since this is not real-time data, using 'overwrite' will optimize for testing.
                                # if it's production, we might switch to 'append' and need to add deduplication logic
              properties=db_properties)

    exchange_rates_df.write \
        .jdbc(url=db_url,
              table="exchange_rates",
              mode="overwrite",
              properties=db_properties)
        
    spark.stop()

if __name__ == "__main__":
    main()