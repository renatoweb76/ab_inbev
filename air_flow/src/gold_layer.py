from pyspark.sql import SparkSession
from pyspark.sql.functions import count, current_timestamp
import logging
import sys

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_gold():
    """
    Aggregates data from Silver Layer to create the Analytical View (Gold).
    Objective: Quantity of breweries per type and location.
    """
    spark = SparkSession.builder.appName("Breweries_Gold_Layer").getOrCreate()
    
    input_path = "/opt/airflow/files/silver/breweries"
    output_path = "/opt/airflow/files/gold/breweries_agg"

    try:
        logger.info("Reading data from Silver Layer...")
        df = spark.read.parquet(input_path)

        # Performing Aggregation
        logger.info("Aggregating data by Country, State, City, and Type...")
        df_agg = df.groupBy("country", "state", "city", "brewery_type") \
            .agg(count("brewery_id").alias("qty_breweries")) \
            .withColumn("updated_at", current_timestamp())

        logger.info("Persisting Gold Layer data...")
        
        # Coalesce to 1 file for smaller datasets (easier for humans to read), 
        # remove this for Large Scale Big Data.
        df_agg.coalesce(1).write \
            .mode("overwrite") \
            .parquet(output_path)
            
        logger.info("Gold Layer generated successfully.")

    except Exception as e:
        logger.error(f"Gold Layer failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold()