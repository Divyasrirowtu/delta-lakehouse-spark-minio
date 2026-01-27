import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp

# ---------------------------
# 1️⃣ Configure Spark with Delta
# ---------------------------
builder = SparkSession.builder \
    .appName("DeltaLakehouseApp") \
    .master(os.getenv("SPARK_MASTER_URL", "local[*]")) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ---------------------------
# 2️⃣ Create MinIO bucket if not exists
# ---------------------------
import boto3
s3 = boto3.resource(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket_name = "data"
if not s3.Bucket(bucket_name) in s3.buckets.all():
    s3.create_bucket(Bucket=bucket_name)
print(f"Bucket '{bucket_name}' is ready!")

# ---------------------------
# 3️⃣ Batch Ingestion: Products
# ---------------------------
products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True)
])

products_df = spark.read.csv(
    "data/products.csv",
    header=True,
    schema=products_schema
)

products_path = f"s3a://{bucket_name}/warehouse/products/"

products_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("category") \
    .option("overwriteSchema", "true") \
    .save(products_path)

print("Products Delta table created!")

# ---------------------------
# 4️⃣ Batch Ingestion: Customers
# ---------------------------
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("last_updated_date", StringType(), True)
])

customers_df = spark.read.csv("data/customers.csv", header=True, schema=customers_schema)
customers_path = f"s3a://{bucket_name}/warehouse/customers/"

customers_df.write.format("delta").mode("overwrite").save(customers_path)
print("Customers Delta table created!")

# ---------------------------
# 5️⃣ MERGE Operation: Update Customers
# ---------------------------
from delta.tables import DeltaTable

updates_df = spark.read.csv("data/updates.csv", header=True, schema=customers_schema)
customers_table = DeltaTable.forPath(spark, customers_path)

customers_table.alias("tgt").merge(
    updates_df.alias("src"),
    "tgt.customer_id = src.customer_id"
).whenMatchedUpdate(set={
    "email": "src.email",
    "last_updated_date": "src.last_updated_date"
}).whenNotMatchedInsertAll().execute()

print("Customers Delta table updated with MERGE!")

# ---------------------------
# 6️⃣ Time Travel Example
# ---------------------------
print("Time Travel Example (Version 0):")
time_travel_df = spark.read.format("delta").option("versionAsOf", 0).load(customers_path)
time_travel_df.show()

# ---------------------------
# 7️⃣ OPTIMIZE Products Table (ZORDER)
# ---------------------------
spark.sql(f"OPTIMIZE delta.`{products_path}` ZORDER BY (product_id)")
print("Products table optimized with ZORDER!")

# ---------------------------
# 8️⃣ VACUUM Customers Table
# ---------------------------
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM delta.`{customers_path}` RETAIN 0 HOURS")
print("Customers table vacuumed!")

# ---------------------------
# 9️⃣ Streaming Ingestion: Sales
# ---------------------------
sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("sale_date", StringType(), True)
])

sales_df = spark.readStream.schema(sales_schema).option("maxFilesPerTrigger", 1).csv("data/sales.csv")

sales_path = f"s3a://{bucket_name}/warehouse/sales/"

query = sales_df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{sales_path}_checkpoint") \
    .start(sales_path)

query.awaitTermination(30)  # Run streaming for 30 seconds
print("Streaming sales ingestion completed!")

spark.stop()
print("ETL Pipeline completed successfully!")
