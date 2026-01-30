from pyspark.sql import SparkSession

# ---------------- Spark Session ----------------
spark = (
    SparkSession.builder
    .appName("Step12-DataQuality-Products")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

products_path = "s3a://data/warehouse/products"

# ---------------- Add Data Quality Constraint ----------------
spark.sql(f"""
ALTER TABLE delta.`{products_path}`
ADD CONSTRAINT price_positive CHECK (price > 0)
""")

print("✅ STEP 12: Constraint price > 0 added successfully")

# ---------------- Test Constraint ----------------
from pyspark.sql.utils import AnalysisException

try:
    spark.sql(f"""
    INSERT INTO delta.`{products_path}` VALUES (99999, 'Invalid Product', 'Test', -10, 100)
    """)
except AnalysisException as e:
    print("⚠ Constraint violation caught as expected:")
    print(e)

spark.stop()
