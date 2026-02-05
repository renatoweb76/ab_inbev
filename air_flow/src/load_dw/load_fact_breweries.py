from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, lit, current_timestamp
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_URL = "jdbc:postgresql://postgres:5432/breweries_dw"
DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

def get_spark_session():
    return SparkSession.builder \
        .appName("Load_Fact_Breweries") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.2.18.jar")\
        .getOrCreate()

def run_load():
    spark = get_spark_session()
    try:
        logger.info("Starting load for Fact Table: Breweries")

        # 1. Read Source Data (Silver)
        logger.info("Reading Silver Layer data...")
        df_silver = spark.read.parquet("/opt/airflow/files/silver/breweries")

        # 2. Read Dimensions from Database (to ensure Referential Integrity)
        # We read back the IDs we just generated in the dimension scripts
        logger.info("Fetching Dimensions from DW...")
        
        df_dim_type = spark.read.jdbc(url=DB_URL, table="dw.dim_brewery_type", properties=DB_PROPERTIES)
        df_dim_loc = spark.read.jdbc(url=DB_URL, table="dw.dim_location", properties=DB_PROPERTIES)
        df_dim_name = spark.read.jdbc(url=DB_URL, table="dw.dim_brewery_name", properties=DB_PROPERTIES)

        # 3. Join Silver Data with Dimensions
        # Using broadcast joins for performance as dimensions are usually smaller than facts
        logger.info("Joining Fact with Dimensions...")
        
        df_fact = df_silver.alias("s") \
            .join(broadcast(df_dim_type).alias("t"), 
                  col("s.brewery_type") == col("t.brewery_type"), "left") \
            .join(broadcast(df_dim_loc).alias("l"), 
                  (col("s.city") == col("l.city")) & 
                  (col("s.state") == col("l.state")) & 
                  (col("s.country") == col("l.country")), "left") \
            .join(broadcast(df_dim_name).alias("n"), 
                  col("s.name") == col("n.name"), "left")

        # 4. Select Final Columns and Add Metrics
        # Note: 'qty_breweries' is usually 1 at this granularity (individual brewery), 
        # but useful for summation in BI tools.
        df_fact_final = df_fact.select(
            col("n.brewery_name_id"),
            col("l.location_id"),
            col("t.brewery_type_id"),
            col("s.latitude"),
            col("s.longitude"),
            lit(1).alias("brewery_count"),
            current_timestamp().alias("loaded_at")
        )

        # 5. Write to Database
        logger.info("Writing data to dw.fact_breweries...")
        df_fact_final.write.jdbc(
            url=DB_URL, 
            table="dw.fact_breweries", 
            mode="overwrite", 
            properties=DB_PROPERTIES
        )
        logger.info("Fact Breweries loaded successfully.")

    except Exception as e:
        logger.critical(f"Failed to load Fact Breweries: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_load()