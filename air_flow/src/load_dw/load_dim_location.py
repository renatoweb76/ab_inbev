from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import logging
import sys

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Configuration
DB_URL = "jdbc:postgresql://postgres:5432/breweries_dw"
DB_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

def get_spark_session():
    return SparkSession.builder \
        .appName("Load_Dim_Location") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

def run_load():
    spark = get_spark_session()
    try:
        logger.info("Starting load for Dimension: Location")
        
        input_path = "/opt/airflow/files/silver/breweries"
        df_silver = spark.read.parquet(input_path)
        
        # Select unique combinations of Country, State, and City
        df_dim = df_silver.select("city", "state", "country") \
            .distinct() \
            .filter(col("country").isNotNull())

        # Generate Surrogate Key (ID)
        w = Window.orderBy("country", "state", "city")
        df_dim = df_dim.withColumn("location_id", row_number().over(w))

        # Write to Database
        logger.info("Writing data to dw.dim_location...")
        df_dim.write.jdbc(
            url=DB_URL, 
            table="dw.dim_location", 
            mode="overwrite", 
            properties=DB_PROPERTIES
        )
        logger.info("Dimension Location loaded successfully.")

    except Exception as e:
        logger.critical(f"Failed to load Dim Location: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_load()