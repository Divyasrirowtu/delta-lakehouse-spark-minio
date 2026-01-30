from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("Step7-Customer-Merge")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# ---------------- Paths ----------------
customers_path = "s3a://data/warehouse/customers"

# ---------------- Read updates.csv ----------------
updates_df = (
    spark.read
    .option("header", "true")
    .csv("/data/updates.csv")
)

# ---------------- Load Delta Table ----------------
customers_delta = DeltaTable.forPath(spark, customers_path)

# ---------------- MERGE Operation ----------------
customers_delta.alias("target").merge(
    updates_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdate(set={
    "email": "source.email",
    "last_updated_date": "source.last_updated_date"
}).whenNotMatchedInsertAll().execute()

print("✅ STEP 7 MERGE completed successfully")

spark.stop()
