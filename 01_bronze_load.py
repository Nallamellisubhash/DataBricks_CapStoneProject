# Databricks notebook source
# DBTITLE 1,Bronze Layer - Batch CSV Loading
# ==============================
# Bronze Layer - Data Ingestion
# ==============================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ==============================
# 1. Olist Customers
# ==============================
@dp.materialized_view(
    name="retail_cat.bronze.olist_customers"
)
def olist_customers():
    return spark.read.csv(
        "s3a://omniretail-subhash/raw/olist/olist_customers_dataset.csv",
        header=True,
        inferSchema=True
    )

# ==============================
# 2. Olist Orders
# ==============================
@dp.materialized_view(
    name="retail_cat.bronze.olist_orders"
)
def olist_orders():
    return spark.read.csv(
        "s3a://omniretail-subhash/raw/olist/olist_orders_dataset.csv",
        header=True,
        inferSchema=True
    )

# ==============================
# 3. Olist Order Items
# ==============================
@dp.materialized_view(
    name="retail_cat.bronze.olist_order_items"
)
def olist_order_items():
    return spark.read.csv(
        "s3a://omniretail-subhash/raw/olist/olist_order_items_dataset.csv",
        header=True,
        inferSchema=True
    )

# ==============================
# 4. Olist Payments
# ==============================
@dp.materialized_view(
    name="retail_cat.bronze.olist_order_payments"
)
def olist_order_payments():
    return spark.read.csv(
        "s3a://omniretail-subhash/raw/olist/olist_order_payments_dataset.csv",
        header=True,
        inferSchema=True
    )

# ==============================
# 5. Olist Products
# ==============================
@dp.materialized_view(
    name="retail_cat.bronze.olist_products"
)
def olist_products():
    return spark.read.csv(
        "s3a://omniretail-subhash/raw/olist/olist_products_dataset.csv",
        header=True,
        inferSchema=True
    )

# ==============================
# 6. Superstore
# ==============================
@dp.materialized_view(
    name="retail_cat.bronze.superstore"
)
def superstore():
    df = spark.read.csv(
        "s3://omniretail-subhash/raw/superstore/Sample - Superstore.csv",
        header=True,
        inferSchema=True
    )
    
    # Clean column names by replacing spaces with underscores
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.replace(" ", "_"))
    
    return df

# COMMAND ----------

# DBTITLE 1,Cell 2 - No longer needed
# This cell is no longer needed
# Superstore loading is now handled in Cell 1 using @dp.materialized_view decorator
pass

# COMMAND ----------

# DBTITLE 1,Cell 3 - No longer needed
# This cell is no longer needed
# Incremental loading is now handled in Cell 1 using @dp.materialized_view decorator
# The pipeline will automatically handle file processing
pass