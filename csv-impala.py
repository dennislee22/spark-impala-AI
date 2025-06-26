import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

CSV_FILE_PATH = "hdfs:///user/dennis/data.csv"
HIVE_IMPALA_DATABASE = "db2"
HIVE_IMPALA_TABLE_NAME = "user_country_parquet" # Changed table name to indicate Parquet format

spark = SparkSession.builder \
    .appName("CSVToImpalaParquetTableJob") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created with Hive support, ready for Impala-compatible tables.")

try:
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(CSV_FILE_PATH)

    print(f"Successfully read CSV from: {CSV_FILE_PATH}")
    print("Original DataFrame Schema:")
    df.printSchema()
    print("Original DataFrame Sample:")
    df.show(5)

except Exception as e:
    print(f"Error reading CSV file: {e}")
    spark.stop()
    exit()

if "value" in df.columns:
    df_transformed = df.withColumnRenamed("value", "amount")
else:
    df_transformed = df # No 'value' column to rename, proceed with original df

df_transformed = df_transformed.withColumn("processed_date", lit("2025-06-25")) \
                               .withColumn("id", col("id").cast("integer")) \
                               .withColumn("name", col("name").cast("string"))

print("\nTransformed DataFrame Schema:")
df_transformed.printSchema()
print("Transformed DataFrame Sample:")
df_transformed.show(5)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_IMPALA_DATABASE}")

try:
    print(f"\nWriting data to table in Parquet format: {HIVE_IMPALA_DATABASE}.{HIVE_IMPALA_TABLE_NAME}...")
    df_transformed.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{HIVE_IMPALA_DATABASE}.{HIVE_IMPALA_TABLE_NAME}")

    print(f"Successfully wrote data to table in Parquet format: {HIVE_IMPALA_DATABASE}.{HIVE_IMPALA_TABLE_NAME}")

    print(f"\nVerifying data in table: {HIVE_IMPALA_DATABASE}.{HIVE_IMPALA_TABLE_NAME}")
    df_table = spark.table(f"{HIVE_IMPALA_DATABASE}.{HIVE_IMPALA_TABLE_NAME}")
    df_table.show(5)
    print("Verification complete.")

except Exception as e:
    print(f"Error writing to table: {e}")

finally:
    spark.stop()
    print("\nSpark Session stopped.")
