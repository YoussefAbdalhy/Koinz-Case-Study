# 🚀 Incremental ETL Pipeline: PostgreSQL to ClickHouse

[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.1-red.svg)](https://airflow.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.4.1-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)](https://www.docker.com/)

## 📝 Project Overview
This repository contains a production-grade ETL pipeline designed to synchronize user visit data from a **PostgreSQL (OLTP)** database to a **ClickHouse (OLAP)** data warehouse. 

The solution is fully containerized and orchestrated via **Apache Airflow**, utilizing **Apache Spark** for distributed processing and idempotent data loading.

---

## 🏗 Architecture & Tech Stack



| Component | Technology | Role |
| :--- | :--- | :--- |
| **Orchestrator** | Apache Airflow 2.8.1 | Scheduling & Task Management |
| **Compute Engine** | Apache Spark 3.4.1 | Distributed Processing & ETL |
| **Source DB** | PostgreSQL 15 | Transactional Fact Data |
| **Sink DB** | ClickHouse 23 | Analytical Data Warehouse |
| **Isolation** | Docker | Containerized Environment |

---

## 🌟 Key Features

### 1. High-Watermark Incremental Logic
To ensure efficiency, the pipeline avoids full table scans. It dynamically fetches the `MAX(updated_at)` from ClickHouse and extract only new/modified records from Postgres. 
> **Note:** A 15-minute look-back window is implemented to mitigate data loss from late-arriving transactions.

### 2. Idempotency & Exact-Once Semantics
I leverage ClickHouse's `ReplacingMergeTree` engine combined with PySpark Window functions (`row_number()`). This ensures that even if a job is re-run, data remains consistent without duplicates.

### 3. Parallel JDBC Extraction
To maximize throughput, the Spark job utilizes parallel JDBC connections by partitioning the load across `updated_at`. This prevents the driver node from becoming a bottleneck during large data transfers.

### 4. Resource Isolation (DockerOperator)
By using the `DockerOperator`, Spark execution is decoupled from the Airflow worker. This isolates dependencies (JARs, Python libraries) and ensures the orchestrator remains lightweight and stable.

---

## 📂 Project Structure

```bash
Koinz_casestudy/
├── airflow/
│   ├── dags/
│   │   └── postgres_to_clickhouse_incremental.py  # Orchestration logic
│   └── docker-compose.airflow.yml                # Airflow service stack
├── spark/
│   ├── jars/                                     # JDBC Drivers (PG & ClickHouse)
│   ├── jobs/
│   │   └── postgres_to_clickhouse.py             # Distributed ETL logic
│   └── Dockerfile                                # Custom Spark image definition
├── docker-compose.yml                            # Core DB Infrastructure
└── README.md                                     # Project Documentation
```

# 🚀 Project Setup & Execution

This guide provides step-by-step instructions to set up the infrastructure, initialize the orchestration layer, and understand the data modeling used in this project.

## 1. Prerequisites
* **Docker & Docker Compose** installed.
* **Minimum 4GB RAM** allocated to Docker for stability.

---

## 2. Launch Infrastructure
Start the storage layer containers (PostgreSQL and ClickHouse).

```bash
docker-compose up -d
```
## 3. Initialize Airflow
Build the custom Spark image and start the Airflow orchestration layer.

```bash
# Build Spark image first (if not already built)
cd spark
docker build -t spark-job:latest .
cd ..
```
# Start Airflow services
```bash
docker-compose -f airflow/docker-compose.airflow.yml up -d
```
## 4. Monitoring Endpoints
Once the containers are healthy, you can access the following interfaces:

| Service | Endpoint | Credentials |
| :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **ClickHouse HTTP** | [http://localhost:8123](http://localhost:8123) | Default (no password) |
| **PostgreSQL** | `localhost:5432` | `postgres` / `postgres` |

## 📊 Data Modeling

### ClickHouse DDL
The target table uses the **ReplacingMergeTree** engine.
* **Partitioning:** Data is partitioned by month using `toYYYYMM` to optimize query performance and facilitate easy data retention management.
* **Ordering:** The table is ordered by `id` to ensure fast lookups and efficient merging of updated records.

```sql
CREATE TABLE app_user_visits_fact
(
    -- Primary & Foreign Keys
    id               String,
    customer_id      String,
    branch_id        String,
    store_id         String,
    cashier_id       String,
    phone_number     Nullable(String),
    seen             Nullable(Int32),
    state            Nullable(Int32),
    points           Nullable(Float64),
    receipt          Nullable(Float64),
    countryCode      Nullable(String),
    remaining        Nullable(Float64),
    created_at       Int64,
    updated_at       Int64,
    expired          Nullable(Int32),
    expires_at       Nullable(Int64),
    order_id         Nullable(String),
    is_deleted       Int8,
    is_fraud         Int8,
    sync_mechanism   Nullable(String),
    is_bulk_points   Nullable(String)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(fromUnixTimestamp(intDiv(updated_at, 1000)))
ORDER BY id;
```
