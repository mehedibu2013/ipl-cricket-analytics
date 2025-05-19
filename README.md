# 🏏 End-to-End Cricket Analytics Pipeline Using Snowflake

This project demonstrates an **end-to-end data engineering pipeline** using **Snowflake** and **Python**, analyzing real-world **IPL cricket match data**.

From raw CSV ingestion to Snowflake transformations and visualizing insights via **Streamlit**, this repo is perfect for learning Snowflake, data pipelines, and analytics dashboards.

## 📁 Folder Structure

```
cricket-analytics/
│
├── data/
│   ├── matches.csv           # Raw IPL match data
│   └── deliveries.csv        # Ball-by-ball delivery data
│
├── config.py                 # 🔐 User-defined Snowflake credentials
├── ingest_data.py            # Ingest CSV files into Snowflake
├── transform_data.sql        # Create transformed views in Snowflake (e.g., top teams, batsmen)
├── dashboard.py              # Streamlit dashboard for visualization
├── README.md                 # 👋 This file
└── requirements.txt          # Python dependencies
```

## 🧱 Project Architecture

```
CSV Files → Ingestion (Python) → Snowflake (RAW Schema)
         → Transformation (SQL) → Snowflake (STAGING Schema)
         → Visualization → Streamlit Dashboard
```

## 🛠️ Features

- ✅ Ingests `matches.csv` and `deliveries.csv` into Snowflake
- ✅ Transforms data into meaningful metrics:
  - Top 10 winning teams
  - Top batsmen by batting average
- ✅ Visualizes results with **Streamlit**
- ✅ Fully modular & ready to extend

## 📦 Prerequisites

1. **Snowflake Account**: Free trial works fine.
2. **Python 3.8+**
3. **Pip Packages** (see `requirements.txt`)
4. **IPL Dataset**:
   - `matches.csv`: [Kaggle Link](https://www.kaggle.com/nowke9/ipldata/data)
   - `deliveries.csv`: [Kaggle Link](https://www.kaggle.com/nowke9/ipldata/data)

## 🚀 How to Run

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Snowflake Credentials

Update `config.py` with your own Snowflake credentials:

```python
# config.py
SNOWFLAKE_CONFIG = {
    "user": "your_username",
    "password": "your_password",
    "account": "your_account_id",  # e.g., abc12345.us-east-2.aws
    "warehouse": "COMPUTE_WH",
    "database": "CRICKET_DB",
    "schema": "RAW"
}
```

### 3. Ingest Data into Snowflake

```bash
python ingest_data.py
```

This:
- Creates database/schema
- Uploads CSV files
- Loads data into Snowflake tables

### 4. Run Transformations in Snowflake

Run the SQL in `transform_data.sql` using:
- Snowflake UI (Snowsight), or
- SnowSQL CLI, or
- Python script (`run_sql.py` if added)

### 5. Launch the Dashboard

```bash
streamlit run dashboard.py
```

Open the local URL shown in the terminal (usually: http://localhost:8501)

## 📊 What You'll See

- 📊 Bar chart of **Top 10 Winning Teams**
- 📈 Line chart of **Top Batsmen by Batting Average**
- 📋 Table views for both charts

## 📝 Summary

A complete, well-documented, and GitHub-ready project structure that includes:
- Ingestion
- Transformation
- Visualization
- Configurable credentials
- Readme with instructions.