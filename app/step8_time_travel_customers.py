from pyspark.sql import SparkSession

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("Step8-Time-Travel-Customers")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

customers_path = "s3a://data/warehouse/customers"

# ---------------- Latest Version ----------------
print("🔹 Latest version (after MERGE)")
spark.sql(f"""
SELECT customer_id, email
FROM delta.`{customers_path}`
WHERE customer_id = 5
""").show()

# ---------------- Time Travel (Version 0) ----------------
print("🔹 Time travel query (VERSION AS OF 0)")
spark.sql(f"""
SELECT customer_id, email
FROM delta.`{customers_path}` VERSION AS OF 0
WHERE customer_id = 5
""").show()

spark.stop()
