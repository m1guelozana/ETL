# ETL Pipeline – Bronze, Silver and Gold Architecture

This project demonstrates a complete **ETL (Extract, Transform, Load) pipeline**, following a **Data Lake architecture** with **Bronze, Silver and Gold layers**, built using **Python, Pandas and PostgreSQL**.

The goal of this project is to practice real-world **Data Engineering concepts**, including data ingestion, normalization, storage optimization, relational modeling and analytical consumption.

---

## 📌 Project Overview

The pipeline processes data from multiple sources and evolves it across layers:

- **Bronze**: Raw data ingestion (CSV and JSON)
- **Silver**: Data normalization and optimization (Parquet)
- **Gold**: Relational modeling and analytical queries (PostgreSQL)

The final output is a **clean, enriched and queryable dataset**, ready for analytics and visualization.

---

## 🥉 Bronze Layer – Raw Data Ingestion

**Purpose:**  
Store raw data with minimal transformation, preserving original structure.

### Implemented features:
- Read user data from CSV files
- Consume external API (ViaCEP) to enrich data with address information
- Handle API errors (timeouts, invalid CEPs, connection issues)
- Persist raw outputs into the Bronze layer

**Technologies:**
- Python
- Pandas
- Requests
- REST API (ViaCEP)

---

## 🥈 Silver Layer – Data Normalization

**Purpose:**  
Clean, standardize and optimize data for analytical processing.

### Implemented features:
- Automatic ingestion of CSV and JSON files from Bronze
- Conversion to columnar format (**Parquet**)
- Removal of duplicate records
- Handling of complex columns (lists converted to strings)
- Generic pipeline capable of processing multiple files

**Benefits:**
- Improved performance and compression
- Consistent schemas
- Data ready for relational modeling

**Technologies:**
- Python
- Pandas
- Parquet

---

## 🥇 Gold Layer – Analytical Data Model

**Purpose:**  
Provide data ready for business analysis and visualization.

### Implemented features:
- Load Parquet files from Silver into **PostgreSQL**
- Dynamic table creation based on DataFrame schema
- Programmatic data insertion using `psycopg2`
- Relational modeling using SQL joins
- Analytical query joining users with address data

### Example analytical query:
- Enrich user data with geographic attributes (city, state, region)
- Use of `LEFT JOIN` to preserve all user records
- Removal of duplicates with `DISTINCT`

### Consumption:
- Jupyter Notebook for data exploration and visualization
- Ready for dashboards or BI tools

**Technologies:**
- PostgreSQL
- SQL
- psycopg2
- Pandas

---

## 🧱 Project Structure

```text
.
├── bronze/
│   ├── users.csv
│   └── cep_info.json
├── silver/
│   └── *.parquet
├── gold/
│   ├── db.py
│   └── load_to_postgres.py
├── notebooks/
│   └── data_exploration.ipynb
├── README.md
