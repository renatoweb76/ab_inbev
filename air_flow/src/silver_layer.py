from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, coalesce, lit
import argparse
import logging
import sys

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    """
    Creates or retrieves a Spark Session.
    """
    return SparkSession.builder \
        .appName("Breweries_Silver_Layer") \
        .getOrCreate()

def run_silver(execution_date):
    """
    Reads raw data from Bronze, applies transformations, and saves to Silver (Parquet).
    """
    spark = get_spark_session()
    
    # If no date is provided, we process all available history (Full Load fallback)
    date_str = execution_date if execution_date else "*"
    input_path = f"/opt/airflow/files/bronze/ingestion_date={date_str}/*.json"
    output_path = "/opt/airflow/files/silver/breweries"

    logger.info(f"Reading raw data from: {input_path}")
    
    try:
        # Read JSON with multiline option enabled
        df = spark.read.option("multiline", "true").json(input_path)
        
        # Select and Clean columns
        # Using trim() to remove whitespace and aliasing for schema consistency
        df_clean = df.select(
            col("id").alias("brewery_id"),
            trim(col("name")).alias("name"),
            trim(col("brewery_type")).alias("brewery_type"),
            trim(col("street")).alias("street"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            trim(col("postal_code")).alias("postal_code"),
            trim(col("country")).alias("country"),
            col("longitude").cast("double"),
            col("latitude").cast("double"),
            col("phone"),
            col("website_url")
        )

        # Handle Nulls in Partition Columns
        # Spark cannot partition by null values, so we default them to 'Unknown'
        df_clean = df_clean.withColumn("country", coalesce(col("country"), lit("Unknown")))
        df_clean = df_clean.withColumn("state", coalesce(col("state"), lit("Unknown")))
        
        # Deduplication logic
        # Assuming 'brewery_id' is the unique primary key from the source system
        df_dedup = df_clean.dropDuplicates(["brewery_id"])

        logger.info("Writing partitioned Parquet data to Silver Layer...")
        
        # Write mode 'overwrite' ensures Idempotency (re-running doesn't duplicate data)
        df_dedup.write \
            .mode("overwrite") \
            .partitionBy("country", "state") \
            .parquet(output_path)
            
        logger.info("Silver Layer processing completed successfully.")

    except Exception as e:
        logger.error(f"Silver Layer processing failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Execution Date YYYY-MM-DD")
    args = parser.parse_args()
    
    run_silver(args.date)