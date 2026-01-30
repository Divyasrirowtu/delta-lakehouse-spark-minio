from pyspark.sql import SparkSession

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("Step9-Optimize-Products")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

products_path = "s3a://data/warehouse/products"

# ---------------- OPTIMIZE with ZORDER ----------------
spark.sql(f"""
OPTIMIZE delta.`{products_path}`
ZORDER BY (product_id)
""")

print("✅ STEP 9 OPTIMIZE with ZORDER completed")

spark.stop()
