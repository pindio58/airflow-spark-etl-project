# Airflow-Spark ETL Project

A modern data engineering project that demonstrates the integration of Apache Airflow, Apache Spark, and MinIO for building robust ETL (Extract, Transform, Load) pipelines. This project processes retail sales data through multiple transformation stages while following data engineering best practices.

## Overview

This project showcases a complete ETL pipeline that:
- Processes BigMart sales data through multiple transformation stages
- Utilizes Apache Airflow for workflow orchestration
- Leverages Apache Spark for distributed data processing
- Uses MinIO as a data lake solution
- Runs entirely in containerized environments using Docker

## Architecture

The project is built using the following components:

- **Apache Airflow**: Orchestrates the ETL workflow
  - Manages task dependencies
  - Schedules pipeline execution
  - Monitors task status
  - [View Airflow Configuration](./Dockerfile.airfbase)

- **Apache Spark**: Handles data processing
  - Performs data cleaning
  - Adds feature engineering
  - Validates data quality
  - Computes aggregations
  - [View Spark Configuration](./Dockerfile.sparkbase)

- **MinIO**: Acts as a data lake
  - Stores raw input data
  - Maintains processed data versions
  - Provides S3-compatible storage

## Data Pipeline

The ETL pipeline consists of several stages:

1. **Data Cleaning**
   - Handles missing values
   - Standardizes categorical variables
   - Removes duplicates

2. **Feature Engineering**
   - Calculates item age
   - Creates sales categories
   - Derives business metrics

3. **Data Validation**
   - Checks primary key integrity
   - Validates categorical values
   - Ensures data quality standards

4. **Aggregation**
   - Computes outlet-level metrics
   - Calculates item-type statistics
   - Generates cumulative sales data

## Project Structure

```
airflow-spark-etl-project/
├── config/               # Configuration files
├── dags/                # Airflow DAG definitions
├── notebooks/           # Jupyter notebooks for analysis
├── plugins/             # Airflow plugins
├── shared/             # Shared utilities and settings
│   ├── data/           # Data files
│   └── utils/          # Utility functions
└── spark_jobs/         # Spark processing scripts
```

## Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/pindio58/airflow-spark-etl-project.git
   ```

2. Start the services:
   ```bash
   docker-compose up -d
   ```

3. Access the services:
   - Airflow UI: http://localhost:8080
   - MinIO Console: http://localhost:9001

## Technology Stack

- [Apache Airflow](https://airflow.apache.org): Workflow orchestration
- [Apache Spark](https://spark.apache.org): Data processing
- [MinIO](https://www.min.io): S3-compatible storage
- [Docker](https://www.docker.com): Containerization

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

This project is open source and available under the [MIT License](LICENSE).
