import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# --- Configuration ---
CSV_FILE_PATH = "hdfs:///user/dennis/cell_towers.csv"
ICEBERG_CATALOG = "hive_catalog"
ICEBERG_DATABASE = "db1"
ICEBERG_TABLE_NAME = "celltowers" # Using Iceberg format now
FULL_TABLE_NAME = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_TABLE_NAME}"


# --- Spark Session Initialization for Iceberg ---
# Configurations for an Iceberg-enabled Spark session
spark = SparkSession.builder \
    .appName("AppendToIcebergJob") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created for Iceberg Append-Only job.")

# --- Data Load and Append Logic ---
try:
    print(f"\n--- Reading data from {CSV_FILE_PATH} ---")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(CSV_FILE_PATH) \
        .withColumn("processed_date", lit("2025-08-11"))
    
    # This script now only appends data, assuming the table already exists.
    print(f"Appending data to existing Iceberg table: {FULL_TABLE_NAME}...")
    df.writeTo(FULL_TABLE_NAME).append()
    print("Append operation successful.")
    
    print("\nVerifying data in the table...")
    final_df = spark.table(FULL_TABLE_NAME)
    final_df.show(5, truncate=False)
    print(f"Total records in table after append: {final_df.count()}")


except Exception as e:
    print(f"An error occurred during append: {e}")

finally:
    spark.stop()
    print("\nSpark Session stopped.")
