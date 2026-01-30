# E-Commerce Data Lakehouse Project with Delta Lake & Apache Spark

## Project Objective
Build a **data lakehouse solution** for an e-commerce business using **Delta Lake** and **Apache Spark**.  
This project demonstrates:
- ACID transactions
- Schema enforcement and evolution
- Time travel queries
- Unified batch and streaming processing
- Data quality constraints
- Optimization with ZORDER and VACUUM

The project runs in a **Dockerized environment** with **MinIO** simulating S3 storage.

---

## Directory Structure

├── app/
│ ├── step7_merge_customers.py
│ ├── step8_time_travel_customers.py
│ ├── step9_optimize_products.py
│ ├── step10_vacuum_customers.py
│ ├── step11_streaming_sales.py
│ └── step12_data_quality_products.py
├── data/
│ ├── products.csv
│ ├── customers.csv
│ ├── updates.csv
│ └── sales.csv
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── requirements.txt
└── README.md


---

## Prerequisites

- **Docker** & **Docker Compose**
- **Python 3.8+**
- Ports:
  - Spark UI: `8080`
  - Spark Master: `7077`
  - MinIO Console: `9001`
- `.env.example` configured with:
  ```env
  MINIO_ACCESS_KEY=minioadmin
  MINIO_SECRET_KEY=minioadmin
  SPARK_MASTER_URL=spark://spark-master:7077

Setup & Run

1.Clone the repository:

git clone <your-repo-url>
cd lakehouse-delta-ecommerce


2.Build and start Docker services:

docker-compose up --build


3.Verify services:

Spark UI: http://localhost:8080

MinIO Console: http://localhost:9001

4.Each script in app/ can also be run individually:

docker exec -it spark-app spark-submit /app/step7_merge_customers.py

Project Steps Implemented
Step 7: MERGE Customers Table

Upsert updates.csv into customers Delta table

Updates existing email & last_updated_date

Inserts new customers

✅ ACID-compliant, idempotent

Step 8: Time Travel Query

Query latest version vs VERSION AS OF 0

Verifies MERGE before/after

✅ Supports audit and rollback

Step 9: OPTIMIZE Products Table

Run OPTIMIZE on products Delta table

ZORDER by product_id for faster queries

✅ Improves read performance

Step 10: VACUUM Customers Table

Removes stale files after updates

Retention period: 0 HOURS

✅ Saves storage, maintains Delta log integrity

Step 11: Streaming Sales Ingestion

Simulate streaming ingestion using sales.csv

Spark Structured Streaming → Delta table

✅ Append mode, finite execution for verification

Step 12: Data Quality Constraint

Add CHECK(price > 0) on products table

Prevents invalid data insertion

✅ Ensures product price integrity

Verification Commands

Run in Spark SQL:

-- Step 7: Updated customer
SELECT customer_id, email FROM delta.`s3a://data/warehouse/customers` WHERE customer_id = 5;

-- Step 8: Time travel query
SELECT customer_id, email FROM delta.`s3a://data/warehouse/customers` VERSION AS OF 0 WHERE customer_id = 5;

-- Step 9: Verify OPTIMIZE/ZORDER in _delta_log

-- Step 10: VACUUM verified by _delta_log entries

-- Step 11: Check sales records
SELECT COUNT(*) FROM delta.`s3a://data/warehouse/sales`;

-- Step 12: Test constraint
INSERT INTO delta.`s3a://data/warehouse/products` VALUES (99999, 'Invalid', 'Test', -10, 100);

Key Concepts Demonstrated

Delta Lake ACID transactions (MERGE/upsert)

Schema enforcement & evolution

Time travel queries for auditing

Batch + Streaming unification

Data quality enforcement (CHECK constraints)

Performance optimization (ZORDER + VACUUM)

References

Delta Lake Documentation

Apache Spark Structured Streaming

MinIO Documentation

Submission Checklist

 docker-compose.yml configured

 .env.example present

 Dockerfile exists

 app/ contains scripts for steps 7–12

 data/ contains CSV files

 requirements.txt with Spark & Delta dependencies

 README.md explaining setup, steps, verification

 Scripts pass automated evaluation

Author

Divya Sri Rowtu
E-Commerce Lakehouse Project – Delta Lake & Apache Spark