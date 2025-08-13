# Portfolio Project: Data Lakehouse for Vehicle Specifications

## 📖 Overview

This project demonstrates the construction of an end-to-end data pipeline, implementing a Data Lakehouse architecture to process a vehicle specifications dataset. The goal is to showcase practical knowledge in Big Data technologies, data engineering, and best practices for ETL development.

The pipeline ingests raw data, processes it through a **Medallion** architecture (Raw/Bronze, Trusted/Silver, Refined/Gold), and makes it available for querying in an analytical layer. This entire process is orchestrated by a custom Python library that ensures robustness and abstraction for the ETL processes.

# 🚀 Technologies Used

The project was built using the following technologies:

- **Trino**: For high-performance, federated SQL queries directly on the Data Lakehouse.
- **MinIO**: As an S3-compatible Object Storage solution, acting as our Data Lake.
- **PySpark**: For distributed data processing and transformation.
- **Apache Iceberg**: As the open table format for the Data Lakehouse, ensuring ACID transactions, time travel, and schema evolution.
- **Medallion Architecture**: For the logical and progressive organization of data into layers (Raw/Bronze, Trusted/Silver, Refined/Gold).
- **ETL Library (Python)**: A custom library to abstract and standardize the Extraction, Transformation, and Load processes.

## 🏗️ Project Architecture

The project's architecture follows the **Medallion** pattern, which organizes data into three logical layers, each representing a level of quality and transformation.

- **Raw Layer (Bronze):**
  - **Location:** `s3a://raw/vehicles/`
  - **Format:** [e.g., CSV, JSON]
  - **Description:** Stores raw data exactly as it was ingested from the source, without any transformations. This layer serves as a historical backup and allows for data reprocessing at any time.

- **Trusted Layer (Silver):**
  - **Location:** `s3a://trusted/vehicles/`
  - **Format:** Apache Iceberg
  - **Description:** Contains a clean, validated, and standardized version of the data from the Bronze layer.

- **Refined Layer (Gold):**
  - **Location:** `s3a://refined/analytics/`
  - **Format:** Apache Iceberg
  - **Description:** Stores aggregated and enriched data tables, ready for consumption by BI tools, dashboards.

## 📊 Dataset

The project uses the **[Comprehensive Vehicle Specifications Dataset](https://www.kaggle.com/datasets/adarsh1077/comprehensive-vehicle-specifications-dataset)**. This dataset contains detailed information about cars and motorcycles.

- **Source:** [Kaggle](https://www.kaggle.com/)
- **License:** [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## 🐍 ETL Library

To ensure the maintainability, reusability, and robustness of the ETL pipelines, a custom, decorator-based Python library was developed. The library abstracts the complexity of Spark initialization, data reading, and writing, allowing developers to focus solely on the transformation logic.

**Main Components:**

- `ETLManager`: The main class that manages pipeline execution. The `run_all` method discovers and executes all classes marked with the `@transform` decorator.
- `@transform()`: A decorator that registers a class as an ETL step to be executed by the `ETLManager`.
- `extract()`: A declarative function used to define data sources. It instantiates a Spark DataFrame from an Iceberg table or a file (JSON, CSV, etc.).
- `@load()`: A decorator applied to the main method of the transformation class (e.g., `exec`). It defines the destination (table, format, write mode) for the DataFrame returned by the method.

**Usage Example (based on `refined/vehicles.py`):**

The following script reads the car and motorcycle tables from the `trusted` layer, unions them, and saves the result to the `refined` layer.

```python
from datalib.etl import ETLManager, extract, load, transform

# Registers the class as an ETL pipeline
@transform()
class UnifiesVehicles:
    # Declares the data sources (DataFrames) to be read
    bikes_df = extract(table="trusted.vehicles.bikes", format="iceberg")
    cars_df = extract(table="trusted.vehicles.cars", format="iceberg")

    # The @load decorator defines the destination for the DataFrame returned by this method
    @load(format="iceberg", mode="overwrite", table="refined.analytics.vehicles", keys=["name"])
    def exec(self, **kwargs):
        # Transformation logic: union the two DataFrames
        return self.bikes_df.unionByName(self.cars_df, allowMissingColumns=True)

# Entry point for pipeline execution via spark-submit
if __name__ == "__main__":
    ETLManager.run_all(name="RefinedAnalyticsVehicles")
````

## 🛠️ Installation and Configuration

To run this project, you will need [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed.

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/s4mukka/comprehensive-vehicle-specification.git
    cd comprehensive-vehicle-specification
    ```

2.  **Start the services with Docker Compose:**
    This command will start the containers for Trino, MinIO, and a Spark environment.

    ```bash
    docker-compose up -d
    ```

3.  **Access MinIO:**

      - Open `http://localhost:9001` in your browser.
      - Use the credentials defined for development:
          - User: `minioadmin`
          - Password: `minioadmin`

## 🏃‍♀️ How to Run the ETLs

1.  **Access the PySpark environment:**

    ```sh
    docker exec -it pyspark_dev_container bash
    ```

2.  **Run the ETLs using the shell script:**

    ```sh
    ./scripts/run.sh <path/to/script.py>
    ```

    Execute the scripts in the following order:

    1.  **Raw Layer:** `./scripts/run.sh ./raw/cars.py` and `./scripts/run.sh ./raw/bikes.py`
    2.  **Trusted Layer:** `./scripts/run.sh ./trusted/cars.py` and `./scripts/run.sh ./trusted/bikes.py`
    3.  **Refined Layer:** `./scripts/run.sh ./refined/vehicles.py`

> **Note:** Simple transformations were performed for portfolio purposes only.

## 🔍 Querying the Data with Trino

You can use your favorite SQL client (DBeaver, DataGrip, etc.) or the Trino CLI to query the data in the Trusted and Refined layers.

- **Host:** `localhost`
- **Port:** `8080`
- **User:** `admin`
- **Catalog:** `raw`, `trusted`, or `refined`
- **Schema:** `vehicles` (raw and trusted) or `analytics` (refined)

**Query Example:**

```sql
SELECT
  *
FROM
  raw.vehicles.bikes;
```

## 📈 Future Improvements

This project serves as a solid foundation. Some possible improvements include:

  * [ ] **Pipeline Orchestration:** Add an orchestrator like [Airflow](https://airflow.apache.org/) to schedule and monitor ETL execution.
  * [ ] **Data Quality:** Implement a data quality tool like [Great Expectations](https://greatexpectations.io/) to validate data in each layer.
  * [ ] **CI/CD:** Create a Continuous Integration and Continuous Delivery (CI/CD) pipeline with [GitHub Actions](https://github.com/features/actions) to automate testing and deployment of changes.
  * [ ] **BI Dashboard:** Connect a Business Intelligence tool, such as [Metabase](https://www.metabase.com/) or [Superset](https://superset.apache.org/), to the Gold layer to create interactive visualizations.
  * [ ] **More Complex Transformations:** Implement more complex transformations to deliver real business value.

## 📄 License

This project is under the [MIT](https://www.google.com/search?q=LICENSE) license.
