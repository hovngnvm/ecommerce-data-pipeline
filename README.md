# 🛒 E-Commerce Medallion Pipeline (Batch Pipeline)

## 📌 Project Overview

A batch **Data Engineering pipeline** built on the Medallion Architecture that ingests raw e-commerce event data, processes and enriches it through Bronze → Silver → Gold layers using Apache Spark, orchestrates daily runs with Apache Airflow, and exposes a clean star schema via dbt for analytics.

**Business Goal:** Transform raw e-commerce behavioral events (views, cart additions, purchases) into an analytics-ready star schema — enabling business intelligence on sales performance, user behavior, and multi-currency revenue across product categories.

## 🏗️ Architecture & Tech Stack

```
┌──────────────────────────────────────────────────────────────────┐
│         Raw Sources: CSV Events + Frankfurter Exchange Rate API  │
└──────────────┬────────────────────────────┬──────────────────────┘
               │                            │
   ┌───────────▼──────────┐    ┌────────────▼──────────────┐
   │   Bronze Layer       │    │   Bronze Layer            │
   │  (Raw CSV Events)    │    │  (Raw JSON Exchange Rates)│
   └───────────┬──────────┘    └────────────┬──────────────┘
               │  Spark                      │  Spark
   ┌───────────▼──────────┐    ┌────────────▼──────────────┐
   │   Silver Layer       │    │   Silver Layer            │
   │  (Cleaned + Typed    │    │  (Flattened + Typed       │
   │   Partitioned Parquet│    │   Exchange Rates Parquet) │
   └───────────┬──────────┘    └────────────┬──────────────┘
               │                            │
               └───────────┬────────────────┘
                           │ JDBC (Spark)
               ┌───────────▼─────────────────────┐
               │     PostgreSQL (Docker)         │
               │  ecommerce_data + exchange_rates│
               └───────────┬─────────────────────┘
                           │
               ┌───────────▼───────────────────┐
               │   Gold Layer (dbt)            │
               │  dim_users · dim_products     │
               │  fact_sales (multi-currency)  │
               └───────────┬───────────────────┘
                           │
               ┌───────────▼───────────────────┐
               │   Apache Airflow              │
               │   Daily Orchestration + DAG   │
               │   Telegram Failure Alerts     │
               └───────────────────────────────┘
```

- **Orchestration:** ![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1-017CEE?style=flat&logo=apacheairflow&logoColor=white)
- **Batch Processing:** ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.0-E25A1C?style=flat&logo=apachespark&logoColor=white)
- **Transformation:** ![dbt](https://img.shields.io/badge/dbt-1.10-FF694B?style=flat&logo=dbt&logoColor=white)
- **Data Warehouse:** ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat&logo=postgresql&logoColor=white)
- **Containerization:** ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)

## 🗂️ Data Sources

**E-Commerce Events (CSV):** Raw behavioral event logs containing user interactions (view, cart, purchase) with product metadata.

**Frankfurter Exchange Rate API:** [`https://api.frankfurter.app`](https://api.frankfurter.app) — Historical daily USD → EUR, JPY rates for October 2019, used to enrich sales data with multi-currency pricing.

## 📁 Project Structure

```
ecommerce-medallion-pipeline/
│
├── dags/
│   └── ecommerce_medallion_dag.py     # Airflow DAG definition + Telegram alerts
│
├── scripts/
│   ├── python/
│   │   └── fetch_exchange_rates.py    # API ingestion → Bronze JSON
│   └── spark/
│       ├── bronze_to_silver_events.py # Clean & partition event CSV
│       ├── bronze_to_silver_api.py    # Flatten & type exchange rate JSON
│       └── silver_to_rdbms.py        # Write Silver Parquet → PostgreSQL
│
├── ecommerce_dbt/
│   ├── models/gold/
│   │   ├── dim_users.sql              # User dimension
│   │   ├── dim_products.sql           # Product dimension
│   │   ├── fact_sales.sql             # Purchase fact table (multi-currency)
│   │   └── schema.yml                 # Sources, models & dbt tests
│   ├── dbt_project.yml
│   └── profiles.yml                   # PostgreSQL connection config
│
├── data/
│   ├── bronze/
│   │   ├── ecommerce_events/          # Raw CSV files (mounted volume)
│   │   └── exchange_rates/            # Raw JSON from API
│   ├── silver/
│   │   ├── ecommerce_events/          # Cleaned Parquet (partitioned by date)
│   │   └── exchange_rates/            # Typed exchange rate Parquet
│   └── quarantine/                    # Rows failing null checks
│
├── dockerfile                         # Airflow + Java + Spark image
├── docker-compose.yml                 # Full stack orchestration
├── requirements.txt
└── .env.example                       # Environment variable template
```

## ⚙️ Pipeline Workflow

### 1. Ingest — Exchange Rate API → Bronze

- Fetches historical USD → EUR, JPY rates for October 2019 from the Frankfurter API
- Saves raw JSON response to `data/bronze/exchange_rates/`
- Triggered daily by Airflow as the first step in the DAG

### 2. Bronze → Silver (Spark)

**Events (`bronze_to_silver_events.py`):**

- Reads raw CSV with a predefined schema (avoids slow `inferSchema` on large files)
- Casts `event_time` to `TIMESTAMP`, derives `event_date`
- Splits `category_code` → `category` + `sub_category` columns
- Fills missing `brand`, `category`, `sub_category` with `"unknown"`
- Routes rows with null critical fields to a **quarantine** Parquet path
- Writes clean data as Parquet, **partitioned by `event_date`** for query efficiency

**Exchange Rates (`bronze_to_silver_api.py`):**

- Flattens the nested JSON rates object into a flat list of `{event_date, rate_EUR, rate_JPY}` rows
- Applies explicit schema, casts dates, coalesces to 1 partition (small dataset)
- Writes to `data/silver/exchange_rates/`

### 3. Silver → PostgreSQL (Spark JDBC)

- Reads both Silver Parquet datasets
- Writes to PostgreSQL tables `ecommerce_data` and `exchange_rates` via JDBC
- Uses `batchsize: 10000` for optimized bulk inserts
- Mode: `overwrite` (suitable for batch reloads; switch to `append` + deduplication for production)

### 4. Gold Layer — dbt Star Schema

- **`dim_users`** — unique users with session counts, first/last seen timestamps
- **`dim_products`** — unique products with category, sub-category, brand
- **`fact_sales`** — purchase events joined with exchange rates for EUR/JPY pricing
- All models materialized as `TABLE`; dbt tests enforce `unique` + `not_null` constraints

### 5. Orchestration — Apache Airflow

- Runs daily on a `timedelta(days=1)` schedule
- DAG flow:
  ```
  ingest_api → api_bronze_to_silver ──┐
  events_bronze_to_silver ────────────┴──► silver_to_rdbms → dbt_run → dbt_test
  ```
- On failure: sends a Telegram alert with task ID, execution date, and truncated error message via Airflow Variables

## 🚀 Key Engineering Highlights

| Feature                       | Details                                                                                     |
| ----------------------------- | ------------------------------------------------------------------------------------------- |
| **Medallion Architecture**    | Bronze → Silver → Gold layers enforce clear data quality boundaries                         |
| **Schema-on-Read**            | Explicit PySpark schemas on raw CSV avoid costly `inferSchema` scans on large event files   |
| **Date Partitioning**         | Silver events partitioned by `event_date` — enables partition pruning in downstream queries |
| **Quarantine Layer**          | Null/invalid rows isolated to a dedicated path instead of being silently dropped            |
| **Multi-Currency Enrichment** | Exchange rates joined at the Gold layer via dbt for EUR/JPY revenue reporting               |
| **dbt Data Quality**          | Automated `unique` + `not_null` tests on all dimension keys and fact measures               |
| **Telegram Alerting**         | Airflow failure callbacks post structured alerts to a Telegram bot via Airflow Variables    |
| **Containerized**             | All services (Airflow, Spark, Postgres, dbt) run via a single `docker-compose up`           |

## 🛠️ How to Run

### Prerequisites

- Docker & Docker Compose

No local Python, Java, or Spark install required — everything runs inside Docker.

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd ecommerce-medallion-pipeline
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
# Then edit .env with your credentials
```

`.env.example`:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_db_name
POSTGRES_PORT=5433

TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

### 3. Place Raw Event Data

Drop your raw e-commerce CSV file(s) into:

```
data/bronze/ecommerce_events/
```

### 4. Start the Full Stack

```bash
docker-compose up -d
```

This will start all services in order:

- **`setup-env`** — creates required data directories with correct permissions
- **`postgres`** — main data warehouse
- **`postgres-airflow`** — Airflow metadata database
- **`airflow-init`** — runs `db migrate` to initialize Airflow schema
- **`airflow-api-server`** — REST API + UI at `http://localhost:8080`
- **`airflow-dag-processor`** — parses and registers DAGs
- **`airflow-scheduler`** — triggers DAG runs on schedule

### 5. Access the Airflow UI

This project uses **Apache Airflow 3**, which introduces a new security architecture (`SimpleAuthManager`). Instead of a hard-coded password, Airflow automatically generates a randomized password for the default admin user on the first run.

1. Open your browser and navigate to: **http://localhost:8080**
2. Retrieve your auto-generated credentials by running:

```bash
docker exec -it airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

### 6. Trigger the Pipeline

Find `ecommerce_medallion_pipeline` in the Airflow UI and trigger a manual run — or wait for the daily schedule.

### 7. Monitor Logs

```bash
# Watch Airflow scheduler activity
docker logs -f airflow-scheduler

# Watch API server logs
docker logs -f airflow-api-server
```

## 📊 Database Schema

### Silver Tables (written by Spark)

#### `ecommerce_data`

Cleaned and typed e-commerce event records.

| Column         | Type             | Description                          |
| -------------- | ---------------- | ------------------------------------ |
| `event_time`   | TIMESTAMP        | Event timestamp                      |
| `event_date`   | DATE             | Derived from `event_time`            |
| `event_type`   | VARCHAR          | `view`, `cart`, or `purchase`        |
| `product_id`   | INTEGER          | Product identifier                   |
| `category_id`  | VARCHAR          | Raw category identifier              |
| `category`     | VARCHAR          | Top-level category (split from code) |
| `sub_category` | VARCHAR          | Sub-category (split from code)       |
| `brand`        | VARCHAR          | Product brand (`unknown` if missing) |
| `price`        | DOUBLE PRECISION | Product price in USD                 |
| `user_id`      | INTEGER          | User identifier                      |
| `user_session` | VARCHAR          | Session UUID                         |

#### `exchange_rates`

Daily USD exchange rates from the Frankfurter API.

| Column       | Type             | Description    |
| ------------ | ---------------- | -------------- |
| `event_date` | DATE             | Rate date      |
| `rate_EUR`   | DOUBLE PRECISION | USD → EUR rate |
| `rate_JPY`   | DOUBLE PRECISION | USD → JPY rate |

### Gold Tables (materialized by dbt)

#### `dim_users`

| Column           | Type      | Description                 |
| ---------------- | --------- | --------------------------- |
| `user_id`        | INTEGER   | Unique user identifier (PK) |
| `total_sessions` | BIGINT    | Number of distinct sessions |
| `first_seen`     | TIMESTAMP | Earliest recorded event     |
| `last_seen`      | TIMESTAMP | Most recent recorded event  |

#### `dim_products`

| Column         | Type    | Description                    |
| -------------- | ------- | ------------------------------ |
| `product_id`   | INTEGER | Unique product identifier (PK) |
| `category_id`  | VARCHAR | Raw category identifier        |
| `category`     | VARCHAR | Top-level category             |
| `sub_category` | VARCHAR | Sub-category                   |
| `brand`        | VARCHAR | Product brand                  |

#### `fact_sales`

| Column       | Type             | Description                        |
| ------------ | ---------------- | ---------------------------------- |
| `sale_id`    | BIGINT           | Surrogate key (row number)         |
| `event_time` | TIMESTAMP        | Purchase timestamp                 |
| `event_type` | VARCHAR          | Always `purchase`                  |
| `user_id`    | INTEGER          | FK → `dim_users`                   |
| `product_id` | INTEGER          | FK → `dim_products`                |
| `price`      | DOUBLE PRECISION | Price in USD                       |
| `rate_EUR`   | DOUBLE PRECISION | EUR exchange rate on purchase date |
| `rate_JPY`   | DOUBLE PRECISION | JPY exchange rate on purchase date |

## 📄 License

This project is licensed under the MIT License.
