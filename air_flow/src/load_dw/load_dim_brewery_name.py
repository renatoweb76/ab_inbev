from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, trim
from pyspark.sql.window import Window
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
        .appName("Load_Dim_Brewery_Name") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.2.18.jar") \
        .getOrCreate()

def run_load():
    spark = get_spark_session()
    try:
        logger.info("Starting load for Dimension: Brewery Name")
        
        input_path = "/opt/airflow/files/silver/breweries"
        df_silver = spark.read.parquet(input_path)
        
        # We use 'id' from API as the business key, but generate our own SK
        df_dim = df_silver.select(col("brewery_id").alias("api_id"), col("name")) \
            .distinct() \
            .filter(col("name").isNotNull())

        w = Window.orderBy("name")
        df_dim = df_dim.withColumn("brewery_name_id", row_number().over(w))

        logger.info("Writing data to dw.dim_brewery_name...")
        df_dim.write.jdbc(
            url=DB_URL, 
            table="dw.dim_brewery_name", 
            mode="overwrite", 
            properties=DB_PROPERTIES
        )
        logger.info("Dimension Brewery Name loaded successfully.")

    except Exception as e:
        logger.critical(f"Failed to load Dim Brewery Name: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_load()