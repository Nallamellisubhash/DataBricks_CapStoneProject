# 🚀 OmniRetail Data Engineering Project (Databricks + S3)

## 📌 Project Overview

This project builds an **end-to-end data pipeline** using:

* **Amazon S3** → Raw & Incremental Data Storage
* **Databricks (Unity Catalog)** → Data Processing
* **PySpark & SQL** → Transformations
* **Delta Lake** → ACID + Incremental Loading

👉 Goal:

> Integrate **online (Olist)** and **offline (Superstore)** datasets to generate business insights.

---

# 🧱 Architecture

```
S3 (Raw + Incremental Landing)
        ↓
Bronze Layer (Raw Tables)
        ↓
Silver Layer (Clean + Transform + Merge)
        ↓
Gold Layer (KPIs / Analytics)
        ↓
Dashboard / Insights
```

---

# 📁 S3 Folder Structure

```
s3://omniretail-subhash/
    ├── raw/
    │     ├── olist/
    │     └── superstore/
    │
    └── incremental/
          └── orders/
               ├── landing/
               └── processed/
```

---

# ⚙️ Technologies Used

* Databricks (Unity Catalog)
* PySpark
* SQL
* Amazon S3
* Delta Lake

---

# 🥉 Bronze Layer (Ingestion)

### 🔹 What it does:

* Reads raw data from S3
* Stores as Delta tables
* Handles incremental append

### 🔹 Key Features:

* No transformations
* Raw data preservation
* Incremental ingestion from landing folder

---

# 🥈 Silver Layer (Transformation)

### 🔹 What it does:

* Cleans data (nulls, duplicates, invalid values)
* Joins Olist datasets
* Combines with Superstore
* Creates:

  * `dim_customers`
  * `dim_products`
  * `fact_sales`

### 🔹 Data Cleaning:

* Removed null values
* Handled missing categories
* Removed duplicates
* Filtered invalid price values

### 🔹 Incremental Logic:

* Uses **MERGE (Delta Lake)**
* Updates existing records
* Inserts new records

---

# 🥇 Gold Layer (Business Insights)

### 📊 Tables Created:

1. **Revenue by Source**
2. **Top Customers**
3. **Top Products**
4. **Total Orders**
5. **Average Order Value**
6. **Channel Split (%)**

---

# 🔁 Incremental Loading

### 📥 How it works:

1. New data is added to:

```
incremental/orders/landing/
```

2. Bronze:

* Appends new data

3. Silver:

* Uses MERGE to update fact table

4. Gold:

* KPIs auto-update

---

# 🧪 Testing Incremental Load

### Step 1:

Upload test CSV files to:

```
landing/
```

### Step 2:

Run:

* Bronze Notebook
* Silver Notebook

### Step 3:

Verify:

```sql
SELECT * 
FROM retail_cat.silver.fact_sales
WHERE order_id LIKE 'INC%'
```

---

# ⭐ Data Model (Star Schema)

```
          dim_customers
                |
                |
dim_products — fact_sales
```

---

# 💡 Key Concepts Implemented

* Medallion Architecture (Bronze, Silver, Gold)
* Star Schema (Fact + Dimension)
* Incremental Loading (MERGE)
* Data Cleaning & Transformation
* Multi-source Data Integration

---

# 🎯 Business Insights

* Online vs Offline Revenue
* Customer Spending Patterns
* Product Performance
* Order Trends

