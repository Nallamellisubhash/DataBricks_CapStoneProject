# Databricks notebook source
# DBTITLE 1,Imports
from pyspark import pipelines as dp
from pyspark.sql.functions import sum, col, round, countDistinct, avg

# COMMAND ----------

# DBTITLE 1,Revenue by Source
@dp.materialized_view(
    name="retail_cat.gold.revenue"
)
def revenue():
    df = spark.read.table("retail_cat.silver.fact_sales")
    return df.groupBy("source") \
        .sum("price") \
        .withColumnRenamed("sum(price)", "total_revenue")

# COMMAND ----------

# DBTITLE 1,Top Customers
@dp.materialized_view(
    name="retail_cat.gold.top_customers"
)
def top_customers():
    df = spark.read.table("retail_cat.silver.fact_sales")
    return df.groupBy("customer_id") \
        .sum("price") \
        .withColumnRenamed("sum(price)", "total_spent") \
        .orderBy("total_spent", ascending=False)

# COMMAND ----------

# DBTITLE 1,Top Products
@dp.materialized_view(
    name="retail_cat.gold.top_products"
)
def top_products():
    df = spark.read.table("retail_cat.silver.fact_sales")
    return df.groupBy("product_id") \
        .sum("price") \
        .withColumnRenamed("sum(price)", "total_sales") \
        .orderBy("total_sales", ascending=False)

# COMMAND ----------

# DBTITLE 1,Orders Count
@dp.materialized_view(
    name="retail_cat.gold.order_count"
)
def order_count():
    df = spark.read.table("retail_cat.silver.fact_sales")
    return df.select(countDistinct("order_id").alias("total_orders"))

# COMMAND ----------

# DBTITLE 1,Average Order Value
@dp.materialized_view(
    name="retail_cat.gold.avg_order_value"
)
def avg_order_value():
    df = spark.read.table("retail_cat.silver.fact_sales")
    return df.select(avg("price").alias("avg_order_value"))

# COMMAND ----------

# DBTITLE 1,Channel Split
@dp.materialized_view(
    name="retail_cat.gold.channel_split"
)
def channel_split():
    df = spark.read.table("retail_cat.silver.fact_sales")
    total_revenue = df.agg(sum("price").alias("total")).collect()[0]["total"]
    
    return df.groupBy("source") \
        .sum("price") \
        .withColumnRenamed("sum(price)", "total_revenue") \
        .withColumn(
            "percentage",
            round((col("total_revenue") / total_revenue) * 100, 2)
        )

# COMMAND ----------

# DBTITLE 1,Cell 8
# Display calls are not valid in SDP pipeline code
# To view data, query the materialized view after pipeline runs

# COMMAND ----------

