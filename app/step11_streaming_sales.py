from pyspark.sql import SparkSession
from pyspark.sql.types import *
import time

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("Step11-Streaming-Sales")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

sales_path = "s3a://data/warehouse/sales"

# ---------------- Schema ----------------
sales_schema = StructType([
    StructField("sale_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("sale_date", StringType())
])

# ---------------- Read as Stream ----------------
sales_stream = (
    spark.readStream
    .schema(sales_schema)
    .option("header", "true")
    .csv("/data/sales.csv")
)

# ---------------- Write Stream to Delta ----------------
query = (
    sales_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/sales_checkpoint")
    .start(sales_path)
)

# Run briefly then stop
time.sleep(10)
query.stop()

print("✅ STEP 11 Streaming ingestion completed")

spark.stop()
