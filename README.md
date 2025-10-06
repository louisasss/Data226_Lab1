# Data226 Lab 1

**Course:** Data226  
**Project:** Lab 1 — Stock ETL + Forecasting with Airflow & Snowflake  
**Authors:** Liana Pakingan, Louisa Stumpf   

---

## Overview
This repo contains two Airflow DAGs that demonstrate an end-to-end pipeline using **Snowflake**:

1. **ETL DAG (`180DayStockData`)**  
   - Extracts 180 days of stock prices from Yahoo Finance.  
   - Transforms data into a clean tabular format.  
   - Loads it into Snowflake (`raw.lab1_market_data`).  

2. **Forecasting DAG (`TrainPredict`)**  
   - Creates a view on the raw data.  
   - Trains a Snowflake ML Forecast model.  
   - Stores predictions in a forecast + final output table.  

---

## Setup
- Configure a Snowflake connection in Airflow (`snowflake_conn`) with account, user, password, warehouse, database, and role.  
- Install required Python packages: `yfinance`, `snowflake-connector-python`, `pandas`.

---

## Running
1. Start Airflow (`docker-compose up -d` if using the provided config).  
2. Add your DAGs to the `dags/` folder.  
3. Trigger `180DayStockData` first, then `TrainPredict`.  
   - Or set up a dependency so `TrainPredict` waits for `180DayStockData`.  

---



