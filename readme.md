# Crypto Data Engineering Pipeline

Complete crypto data pipeline: ingestion, transformation, dimensional modeling, Snowflake loading and Tableau visualization.

## Architecture

CoinGecko API → Bronze (JSON) → Silver (Parquet) → Gold (Star Schema) → Snowflake → Tableau

## Tech Stack

- Ingestion: CoinGecko API + httpx
- Data Lake: MinIO (Bronze / Silver / Gold)
- Orchestration: Apache Airflow (daily DAG)
- Data Warehouse: Snowflake (Star Schema)
- Visualization: Tableau Desktop
- Containerization: Docker

## Project Structure

- dags/ : Airflow DAG with 4 pipeline tasks
- ingestion/ : Bronze script (raw JSON from CoinGecko)
- transformation/ : Silver script (cleaning and normalization with Pandas)
- modeling/ : Gold script (dimensional model construction)
- snowflake_dwh/ : Loading tables into Snowflake
- utils/ : Shared MinIO client
- tableau/ : .twb file and dashboard screenshots
- docker-compose.yml : Local infrastructure (Airflow + MinIO + PostgreSQL)
- requirements.txt : Python dependencies

## Running the Pipeline

Copy .env.example to .env and fill in the variables, then start the infrastructure with docker-compose up -d. Airflow is available at localhost:8080 and MinIO at localhost:9001. Trigger the crypto_pipeline_dag DAG from the Airflow interface.

## Dimensional Model

Star Schema with one fact table FACT_CRYPTO_PRICE linked to two dimensions DIM_CRYPTO and DIM_DATE. Granularity is one row per crypto per collection hour.

## Environment Variables

SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD

## Tableau Dashboard

File: tableau/crypto.twb. The main dashboard contains price evolution, top volume, scatter plot and KPIs. The detail dashboard contains the heatmap and detailed view per crypto.