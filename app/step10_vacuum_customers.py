from pyspark.sql import SparkSession

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("Step10-Vacuum-Customers")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

customers_path = "s3a://data/warehouse/customers"

# ---------------- Disable Safety Check ----------------
spark.conf.set(
    "spark.databricks.delta.retentionDurationCheck.enabled", "false"
)

# ---------------- VACUUM ----------------
spark.sql(f"""
VACUUM delta.`{customers_path}`
RETAIN 0 HOURS
""")

print("✅ STEP 10 VACUUM completed successfully")

spark.stop()
