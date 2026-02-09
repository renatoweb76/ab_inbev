# Breweries Data Engineering Pipeline

![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-blue)
![Spark](https://img.shields.io/badge/Processing-Apache%20Spark-orange)
![Docker](https://img.shields.io/badge/Containerization-Docker-2496ED)
![Postgres](https://img.shields.io/badge/Data%20Warehouse-PostgreSQL-336791)
![Python](https://img.shields.io/badge/Language-Python%203.9-yellow)

## 1. Overview

The **Breweries Data Pipeline** is a scalable, production-ready ELT (Extract, Load, Transform) solution designed to ingest data from the [Open Brewery DB API](https://www.openbrewerydb.org/), process it through a Medallion Architecture (Bronze, Silver, Gold), and persist it into a dimensional Data Warehouse.

**Key Technical Shifts:**
This project utilizes a distributed computing architecture using **Apache Spark (PySpark)** orchestrated by **Airflow**. This ensures the pipeline can handle massive datasets efficiently, leveraging distributed processing for transformations and database ingestion via JDBC.

### Tech Stack
* **Orchestration:** Apache Airflow (Dockerized)
* **Processing Engine:** Apache Spark 3.5 (PySpark)
* **Data Warehouse:** PostgreSQL 15
* **Containerization:** Docker & Docker Compose
* **Visualization:** Power BI
* **Language:** Python 3.9

---

## 2. Architecture & Design Patterns

The solution follows the **Medallion Architecture**, ensuring data quality progression and traceability.

### 2.1 Data Layers

1.  **Bronze Layer (Raw Ingestion):**
    * **Format:** JSON (Raw)
    * **Strategy:** Daily Snapshot / Incremental Append.
    * **Path:** `files/bronze/ingestion_date=YYYY-MM-DD/`
    * **Goal:** Immutable storage of raw data exactly as received from the source.

2.  **Silver Layer (Cleansed & Partitioned):**
    * **Format:** Parquet (Snappy compression)
    * **Strategy:** Idempotent Overwrite per Partition.
    * **Partitioning:** `country` / `state`
    * **Goal:** Data is cleaned (trimmed, casted), deduplicated, and organized by location for efficient querying.

3.  **Gold Layer (Aggregated Business Views):**
    * **Format:** Parquet
    * **Strategy:** Full Aggregation.
    * **Goal:** Business-ready aggregations (e.g., *Quantity of breweries per type and location*).

4.  **Data Warehouse (Star Schema):**
    * **Engine:** PostgreSQL
    * **Strategy:** Spark JDBC Batch Inserts.
    * **Goal:** Serves the BI tool (Power BI) with high-performance dimensional modeling.

---

## 3. Engineering Decisions & Features

### 3.1 Idempotency & Rollback Strategy
Instead of complex "undo" scripts, the pipeline is designed to be **Idempotent**.
* **Silver/Gold:** Uses Spark's `mode("overwrite")`. If a job fails or data needs correction, simply re-running the DAG overwrites the specific partitions or tables with the correct data. No duplicate data is ever generated.
* **Data Warehouse:** Dimensions and Facts are reloaded (SCD Type 1 logic) or appended with integrity checks, ensuring the DW always reflects the "Single Source of Truth" (Silver Layer).

### 3.2 Incremental vs. Full Load
The Airflow DAG supports two modes of operation via logical branching:

* **Incremental Load (Default):**
    * Triggered automatically by the schedule (`0 1 * * *`).
    * Uses the Airflow `{{ ds }}` (Logical Date).
    * **Behavior:** Ingests only the data for that day, processes that specific day's folder in Silver, and updates the DW.

* **Full Load (Manual Trigger):**
    * Triggered manually via Airflow UI with config: `{"full_load": true}`.
    * **Behavior:** The Silver Layer ignores the date filter and processes **all historical JSON files** found in Bronze, rebuilding the entire dataset and refreshing the DW.

### 3.3 Spark & JDBC Optimization
* **Batch Writes:** We use Spark's `JDBCOutputFormat` instead of standard SQL `INSERT` statements. This reduces network overhead and drastically speeds up the loading process into PostgreSQL.
* **Broadcast Joins:** When loading the Fact table, we broadcast the Dimension tables (which are smaller) to all worker nodes, minimizing data shuffling across the cluster.

---

## 4. Data Warehouse Model

The DW follows a **Star Schema** design to optimize analytical queries.

### Fact Table
* **`dw.fact_breweries`**: Central table containing metrics (`brewery_count`) and Foreign Keys to dimensions.

### Dimension Tables
* **`dw.dim_location`**: Surrogate keys for `Country`, `State`, `City`.
* **`dw.dim_brewery_type`**: Catalog of brewery types (micro, large, brewpub, etc.).
* **`dw.dim_brewery_name`**: Catalog of unique brewery names.

---

## 5. Setup & Installation

### Prerequisites
* Docker & Docker Compose installed.
* **PostgreSQL JDBC Driver:** You **must** download the driver manually (due to licensing/distribution reasons).

### Step-by-Step

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/renatoweb76/ab_inbev.git](https://github.com/renatoweb76/ab_inbev.git)
    cd ab_inbev
    ```

2.  **Download JDBC Driver (Crucial Step):**
    Download `postgresql-42.2.18.jar` and place it in the `air_flow/jars/` folder.
    * **Download Link:** [https://jdbc.postgresql.org/download/postgresql-42.2.18.jar](https://jdbc.postgresql.org/download/postgresql-42.2.18.jar)
    * **Check Path:** Ensure the file exists at `ab_inbev/air_flow/jars/postgresql-42.2.18.jar`.

3.  **Start Environment:**
    ```bash
    docker-compose up --build -d
    ```

4.  **Access Services:**
    * **Airflow UI:** [http://localhost:8080](http://localhost:8080) (User: `admin` | Pass: `admin`)
    * **Spark Master UI:** [http://localhost:9090](http://localhost:9090)
    * **Postgres Database:**
        * Host: `localhost`
        * Port: `5435` (Mapped to local machine)
        * User: `airflow`
        * Pass: `airflow`
        * DB: `breweries_dw`

---

## 6. How to Run the Pipeline

### Option A: Standard Daily Run (Incremental)
1.  Enable the DAG `breweries_etl_spark` in the Airflow UI.
2.  It will run automatically based on the schedule.
3.  To test manually: Click the **"Play"** button -> **"Trigger DAG"**.

### Option B: Full Reprocessing (Full Load)
1.  In Airflow, click the **"Play"** button on the DAG.
2.  Select **"Trigger DAG w/ config"**.
3.  Enter the following JSON Configuration:
    ```json
    {"full_load": true}
    ```
4.  Click **Trigger**. This will force Spark to read the entire Bronze history and rebuild the Silver/Gold layers.

### Option C: Reprocessing a Specific Past Date (Backfill)
1.  Go to the **Grid View** of the DAG.
2.  Select the column for the execution date you want to fix (e.g., `2024-01-01`).
3.  Click **"Clear"**.
4.  Airflow will retry that specific date, effectively "rolling back" and fixing that day's data partition without duplication.

---

## 7. Project Structure

```text
ab_inbev/
├── docker-compose.yml       # Orchestrates Airflow, Spark, and Postgres
├── Dockerfile               # Custom Airflow image with Java/Spark support
├── README.md                # Documentation
└── air_flow/
    ├── dags/                # Airflow DAG definition
    │   └── breweries_etl_spark.py
    ├── jars/                # External Drivers
    │   └── postgresql-42.2.18.jar
    ├── src/                 # PySpark ETL Scripts
    │   ├── bronze_layer.py
    │   ├── silver_layer.py
    │   ├── gold_layer.py
    │   └── load_dw/         # DW Loading Scripts
    │       ├── load_dim_brewery_type.py
    │       ├── load_dim_location.py
    │       ├── load_dim_brewery_name.py
    │       └── load_fact_breweries.py
    ├── files/               # Local Data Lake Storage (Bronze/Silver/Gold)
    └── logs/                # Execution logs 
```

--- 

## 8. Analytics (Power BI)

### A Power BI dashboard template is available in the dashboard/ folder.
1. Connection: Connects to the local PostgreSQL Data Warehouse.
2. Metrics: Distribution of breweries by State, Top Brewery Types, and Geographical Analysis.