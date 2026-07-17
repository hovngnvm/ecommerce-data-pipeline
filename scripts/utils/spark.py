from pyspark.sql import SparkSession
from utils.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

def get_spark_session(app_name: str, memory_limit: str = "1536M") -> SparkSession:
    """
    Initializes and returns a configured local Spark Session with S3/MinIO & Delta Lake settings.
    Suitable for running via spark-submit or directly via python.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", memory_limit) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter") \
        .getOrCreate()
    return spark
