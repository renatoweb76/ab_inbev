import requests
import json
import os
import argparse
import logging
from datetime import datetime

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_data_from_api():
    """
    Fetches data from the Open Brewery DB API handling pagination.
    Returns:
        list: A list of dictionaries containing all brewery records.
    """
    base_url = "https://api.openbrewerydb.org/v1/breweries"
    all_data = []
    page = 1
    
    while True:
        try:
            logger.info(f"Fetching page {page} from API...")
            # Added a timeout to prevent the process from hanging indefinitely
            response = requests.get(base_url, params={"page": page, "per_page": 200}, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # If the list is empty, we reached the end of the pagination
            if not data:
                break
                
            all_data.extend(data)
            page += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API Request failed on page {page}: {e}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error on page {page}: {e}")
            raise e
            
    return all_data

def run_bronze(execution_date):
    """
    Orchestrates the Bronze Layer execution.
    Saves raw data partitioned by ingestion date (YYYY-MM-DD).
    
    Args:
        execution_date (str): The Logical Date from Airflow (YYYY-MM-DD).
    """
    try:
        logger.info("Starting Bronze Layer ingestion...")
        raw_data = fetch_data_from_api()
        
        # Determine partition date (use Airflow execution date if provided, else current date)
        date_str = execution_date if execution_date else datetime.now().strftime('%Y-%m-%d')
        
        # Path structure: bronze/ingestion_date=YYYY-MM-DD/
        output_dir = f"/opt/airflow/files/bronze/ingestion_date={date_str}"
        os.makedirs(output_dir, exist_ok=True)
        
        file_path = os.path.join(output_dir, "breweries_raw.json")
        
        # Save raw JSON (No transformation applied at this stage, purely EL)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False)
            
        logger.info(f"Bronze Layer finished successfully. File saved at: {file_path}")
        logger.info(f"Total records ingested: {len(raw_data)}")
        
    except Exception as e:
        logger.critical(f"Bronze Layer failed: {e}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Execution Date (YYYY-MM-DD)", default=None)
    args = parser.parse_args()
    
    run_bronze(args.date)