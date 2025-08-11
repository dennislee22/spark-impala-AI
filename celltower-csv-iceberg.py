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
# Re-added the necessary configurations for Iceberg
spark = SparkSession.builder \
    .appName("CreateOrAppendIcebergJob") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created for Iceberg Create-or-Append job.")

# --- Data Load and Write Logic ---
try:
    print(f"\n--- Reading data from {CSV_FILE_PATH} ---")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(CSV_FILE_PATH) \
        .withColumn("processed_date", lit("2025-08-11"))
    
    # Ensure the database exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DATABASE}")

    # Check if the Iceberg table already exists
    if spark.catalog.tableExists(FULL_TABLE_NAME):
        # If the table exists, append the new data using the Iceberg API
        print(f"Iceberg table {FULL_TABLE_NAME} exists. Appending data...")
        df.writeTo(FULL_TABLE_NAME).append()
        print("Append operation successful.")
    else:
        # If the table does not exist, create it using the Iceberg API
        print(f"Iceberg table {FULL_TABLE_NAME} does not exist. Creating new table...")
        df.writeTo(FULL_TABLE_NAME) \
            .using("iceberg") \
            .create()
        print("Table created successfully.")
    
    print("\nVerifying data in the table...")
    final_df = spark.table(FULL_TABLE_NAME)
    final_df.show(5, truncate=False)
    print(f"Total records in table: {final_df.count()}")


except Exception as e:
    print(f"An error occurred: {e}")

finally:
    spark.stop()
    print("\nSpark Session stopped.")
