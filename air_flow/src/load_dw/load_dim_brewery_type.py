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
    """Initializes Spark with JDBC Driver."""
    return SparkSession.builder \
        .appName("Load_Dim_Brewery_Type") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.2.18.jar")\
        .getOrCreate()

def run_load():
    spark = get_spark_session()
    try:
        logger.info("Starting load for Dimension: Brewery Type")
        
        # Read unique types from Silver Layer
        input_path = "/opt/airflow/files/silver/breweries"
        df_silver = spark.read.parquet(input_path)
        
        df_dim = df_silver.select("brewery_type") \
            .distinct() \
            .filter(col("brewery_type").isNotNull())

        # Generate Surrogate Key (ID)
        w = Window.orderBy("brewery_type")
        df_dim = df_dim.withColumn("brewery_type_id", row_number().over(w))

        # Write to Database (Overwrite/Full Load)
        logger.info("Writing data to dw.dim_brewery_type...")
        df_dim.write.jdbc(
            url=DB_URL, 
            table="dw.dim_brewery_type", 
            mode="overwrite", 
            properties=DB_PROPERTIES
        )
        logger.info("Dimension Brewery Type loaded successfully.")

    except Exception as e:
        logger.critical(f"Failed to load Dim Brewery Type: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_load()