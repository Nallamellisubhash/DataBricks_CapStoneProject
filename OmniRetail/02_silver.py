# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

# COMMAND ----------

# DBTITLE 1,Cell 2 - DIM_CUSTOMERS
# ==============================
# 1. DIM_CUSTOMERS (CLEANING)
# ==============================

@dp.materialized_view(name="retail_cat.silver.dim_customers")
def dim_customers():
    olist = spark.read.table("retail_cat.bronze.olist_customers")

    # Remove nulls
    olist = olist.filter(col("customer_id").isNotNull())

    # Remove duplicates
    olist = olist.dropDuplicates(["customer_id"])

    # Clean city (null handling)
    olist = olist.withColumn(
        "customer_city",
        when(col("customer_city").isNull(), "unknown")
        .otherwise(initcap(col("customer_city")))
    )

    olist = olist.selectExpr(
        "customer_id",
        "customer_city as city"
    )

    # Superstore cleaning
    superstore = spark.read.table("retail_cat.bronze.superstore")

    super_cust = superstore.selectExpr(
        "Customer_ID as customer_id",
        "Customer_Name as customer_name",
        "City as city"
    )

    super_cust = super_cust.dropDuplicates(["customer_id"])

    # Combine
    olist = olist.withColumn("customer_name", lit(None).cast("string")).withColumn("source", lit("online"))
    super_cust = super_cust.withColumn("source", lit("offline"))

    return olist.unionByName(super_cust)

# COMMAND ----------

# DBTITLE 1,Cell 3 (Commented - for interactive use only)
# Display cell - commented out for pipeline execution
# Uncomment for interactive development only
# dim_customers = spark.table("dim_customers")
# display(dim_customers)

# COMMAND ----------

# DBTITLE 1,Cell 4 - DIM_PRODUCTS
# ==============================
# 2. DIM_PRODUCTS (CLEANING)
# ==============================

@dp.materialized_view(name="retail_cat.silver.dim_products")
def dim_products():
    products = spark.read.table("retail_cat.bronze.olist_products")

    products = products.dropDuplicates(["product_id"])

    products = products.withColumn(
        "product_category_name",
        when(col("product_category_name").isNull(), "unknown")
        .otherwise(initcap(col("product_category_name")))
    )

    return products

# COMMAND ----------

# DBTITLE 1,Cell 5 (Commented - for interactive use only)
# Display cell - commented out for pipeline execution
# Uncomment for interactive development only
# dim_products = spark.table("dim_products")
# display(dim_products)

# COMMAND ----------

# DBTITLE 1,Cell 6 (Commented - logic moved to fact_sales function)
# ==============================
# 3. OLIST FACT (CLEANING)
# ==============================
# NOTE: This cell is redundant - logic is now in fact_sales() function
# Commented out to avoid execution during pipeline run

# orders = spark.read.table("retail_cat.bronze.olist_orders")
# items = spark.read.table("retail_cat.bronze.olist_order_items")
# payments = spark.read.table("retail_cat.bronze.olist_order_payments")

# olist_sales = orders \
#     .join(items, "order_id") \
#     .join(payments, "order_id")

# # Cleaning
# olist_sales = olist_sales.filter(col("price") > 0)
# olist_sales = olist_sales.dropDuplicates(["order_id","product_id"])

# olist_sales = olist_sales.select(
#     "order_id",
#     "customer_id",
#     "product_id",
#     col("price").cast("double")
# ).withColumn("source", lit("online"))

# COMMAND ----------

# DBTITLE 1,Cell 7 (Commented - logic moved to fact_sales function)
# ==============================
# 4. SUPERSTORE FACT (CLEANING)
# ==============================
# NOTE: This cell is redundant - logic is now in fact_sales() function
# Commented out to avoid execution during pipeline run

# super_sales = spark.read.table("retail_cat.bronze.superstore") \
#     .selectExpr(
#         "Order_ID as order_id",
#         "Customer_ID as customer_id",
#         "Product_ID as product_id",
#         "TRY_CAST(Sales as double) as price"
#     )

# super_sales = super_sales.filter(col("price") > 0)

# super_sales = super_sales.withColumn("source", lit("offline"))

# COMMAND ----------

# DBTITLE 1,Cell 8 - FACT_SALES
# ==============================
# 4. FACT_SALES (FINAL)
# ==============================

@dp.materialized_view(name="retail_cat.silver.fact_sales")
def fact_sales():
    # Recompute olist_sales (intermediate transformation)
    orders = spark.read.table("retail_cat.bronze.olist_orders")
    items = spark.read.table("retail_cat.bronze.olist_order_items")
    payments = spark.read.table("retail_cat.bronze.olist_order_payments")

    olist_sales = orders \
        .join(items, "order_id") \
        .join(payments, "order_id")

    olist_sales = olist_sales.filter(col("price") > 0)
    olist_sales = olist_sales.dropDuplicates(["order_id","product_id"])

    olist_sales = olist_sales.select(
        "order_id",
        "customer_id",
        "product_id",
        col("price").cast("double")
    ).withColumn("source", lit("online"))

    # Recompute super_sales (intermediate transformation)
    super_sales = spark.read.table("retail_cat.bronze.superstore") \
        .selectExpr(
            "Order_ID as order_id",
            "Customer_ID as customer_id",
            "Product_ID as product_id",
            "TRY_CAST(Sales as double) as price"
        )

    super_sales = super_sales.filter(col("price") > 0)
    super_sales = super_sales.withColumn("source", lit("offline"))

    # Union both sources
    return olist_sales.unionByName(super_sales)

# COMMAND ----------

# DBTITLE 1,Cell 9 (Commented - for interactive use only)
# Display cell - commented out for pipeline execution
# Uncomment for interactive development only
# fact_sales = spark.table("fact_sales")
# display(fact_sales)

# COMMAND ----------

