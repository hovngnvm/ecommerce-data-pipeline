# 🛒 End-to-End E-Commerce Data Pipeline (Medallion Architecture)

## 📌 Project Overview

An end-to-end **Data Engineering pipeline** that processes e-commerce clickstream events and multi-currency exchange rate data, following the **Medallion Architecture (Bronze → Silver → Gold)** to ensure data quality, scalability, and BI-readiness.

**Business Goal:** Build an automated, fault-tolerant pipeline that transforms raw clickstream and purchase data into a structured Star Schema, enabling business analysts to monitor sales performance across different currencies.

## 🏗️ Architecture & Tech Stack

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Apache Airflow (Orchestration)                   │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
          ┌────────────────────────▼────────────────────────┐
          │                  BRONZE LAYER                   │
          │    CSV (Kaggle)  +  Frankfurter API (REST)      │
          │           Raw ingest → stored as-is             │
          └────────────────────────┬────────────────────────┘
                                   │
          ┌────────────────────────▼─────────────────────────┐
          │                  SILVER LAYER                    │
          │    PySpark: schema casting, dedup, null handling │
          │    Output: Parquet, partitioned by event_date    │
          │    Invalid records → /quarantine                 │
          └────────────────────────┬─────────────────────────┘
                                   │ Spark JDBC
          ┌────────────────────────▼────────────────────────┐
          │               PostgreSQL (Docker)               │
          └────────────────────────┬────────────────────────┘
                                   │
          ┌────────────────────────▼─────────────────────────┐
          │                    GOLD LAYER                    │
          │  dbt: Star Schema modeling + data quality tests  │
          │     fact_sales, dim_users, dim_products          │
          └──────────────────────────────────────────────────┘
                                   │
          ┌────────────────────────▼────────────────────────┐
          │                     Metabase                    │
          │            BI Dashboards & Reporting            │
          └─────────────────────────────────────────────────┘
```

* **Orchestration:** ![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1-017CEE?style=flat&logo=apacheairflow&logoColor=white)
* **Data Processing:** ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.0-E25A1C?style=flat&logo=apachespark&logoColor=white)
* **Data Modeling & Testing:** ![dbt](https://img.shields.io/badge/dbt-1.10-FF694B?style=flat&logo=dbt&logoColor=white)
* **Data Warehouse:** ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat&logo=postgresql&logoColor=white)
* **Alerting / Monitoring:** Telegram Bot API
* **Visualization:** Metabase

## 🗂️ Data Source

1. **E-Commerce Events:** [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
2. **Exchange Rates:** [Frankfurter API](https://api.frankfurter.app/)

> ⚠️ The raw CSV is too large to host on GitHub. Download it manually from Kaggle and place it under `data/bronze/ecommerce_events`.

## 📁 Project Structure

```
ecommerce-data-pipeline/
│
├── dags/                          # Airflow DAG definitions
│   └── ecommerce_medallion_dag.py
│
├── scripts/         
│   ├── spark/                     # PySpark transformation scripts
│   │   ├── bronze_to_silver_api.py
│   │   ├── bronze_to_silver_events.py
│   │   └── silver_to_rdbms.py
│   └── python/
│       └── fetch_exchange_rates.py
│
├── ecommerce_dbt/                 # dbt models & tests
│   ├── models/
│   │   ├── gold/
│   │   │   ├── fact_sales.sql
│   │   │   ├── dim_users.sql
│   │   │   ├── dim_products.sql
│   │   │   └── schema.yml
│   ├── tests/
│   └── dbt_project.yml
│
├── data/
│   ├── bronze/                    # Raw CSV & JSON (gitignored)
│   ├── silver/                    # Cleaned Parquet files
│   └── quarantine/                # Invalid/rejected records
│
├── .env.example                   # Environment variable template
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## ⚙️ Pipeline Workflow

### 1. Bronze — Raw Ingestion
- Simulates raw e-commerce event logs (CSV/JSON)
- Fetches historical exchange rates via **Frankfurter API** using Python `requests`
- Data stored as-is for full auditability

### 2. Silver — Transformation (PySpark)
- Schema casting & type enforcement
- Null value handling & deduplication
- Invalid records routed to `/quarantine`
- Output: **Parquet**, partitioned by `event_date`

### 3. Load — Postgres
- Cleaned Silver data loaded into **PostgreSQL** via **Spark JDBC**

### 4. Gold — Modeling (dbt)
- Builds **Star Schema**: `fact_sales`, `dim_users`, `dim_products`
- `dbt test` enforces: **non-null**, **unique**, **referential integrity**

### 5. DataOps & Monitoring
- Full pipeline scheduled **daily** via **Airflow**
- **Telegram Bot** pushes real-time failure alerts with execution context and exception trace

## 🚀 Key Engineering Highlights

| Feature | Details |
|---|---|
| **Dynamic Paths** | Eliminated hardcoded paths using `os.path` + `sys.argv` — 100% portable |
| **Security** | Credentials decoupled via Airflow `Variables` + `.env` — never in codebase |
| **Fault Tolerance** | Airflow `execution_timeout` prevents deadlocks; quarantine zone captures bad records |
| **Data Quality** | dbt `relationships`, `not_null`, `unique` tests run automatically on every pipeline run |
| **Partitioning** | Silver Parquet files partitioned by `event_date` for optimized I/O on large datasets |

## 🛠️ How to Run

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Apache Spark 4.0 + Java JDK 11+
- Apache Airflow 3.1

### 1. Clone the Repository

```bash
git clone https://github.com/hovngnvm/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

### 2. Set Up Python Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
cp .env.example .env
# Then edit .env with your credentials
```

`.env.example`:
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database_name
POSTGRES_PORT=5433
```

> Also configure `telegram_bot_token` and `telegram_chat_id` in the **Airflow UI → Admin → Variables**.

### 4. Start Infrastructure

```bash
# Start PostgreSQL
docker-compose up -d

# Initialize and start Airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
airflow standalone
```

### 5. Trigger the Pipeline

Access the Airflow UI at `http://localhost:8080` and enable the **`ecommerce_medallion_pipeline`** DAG. It will run automatically on schedule (daily), or you can trigger it manually.

## 📄 License

This project is licensed under the MIT License.