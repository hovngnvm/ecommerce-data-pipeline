from pyspark.sql import SparkSession
from scripts.config.settings import settings

SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.6.0"

def get_spark_session(app_name: str, memory_limit: str = "1536M") -> SparkSession:
    """
    Initializes and returns a configured local Spark Session with S3/MinIO & Delta Lake settings.
    Tuned for local/Docker execution with optimized shuffle partitions.
    """

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", memory_limit) \
        .config("spark.jars.packages", SPARK_PACKAGES) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter") \
        .getOrCreate()
    return spark
