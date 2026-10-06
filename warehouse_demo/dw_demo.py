from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    count,
    year,
    month,
    dayofmonth
)

# -------------------------------------------------------
# 1. CREATE SPARK SESSION
# -------------------------------------------------------

spark = SparkSession.builder \
    .appName("LocalDataWarehouseDemo") \
    .config("spark.sql.warehouse.dir", "./warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

print("Spark Session Started")


# -------------------------------------------------------
# 2. CREATE DATABASE
# -------------------------------------------------------

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_dw")

spark.sql("USE ecommerce_dw")


# -------------------------------------------------------
# 3. EXTRACT
# -------------------------------------------------------

print("\nReading source data...")


customers_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("./data/customers.csv")


products_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("./data/products.csv")


sales_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("./data/sales.csv")


print("\nCustomers")
customers_df.show()

print("\nProducts")
products_df.show()

print("\nSales")
sales_df.show()


# -------------------------------------------------------
# 4. TRANSFORM - CLEAN SALES DATA
# -------------------------------------------------------

sales_df = sales_df \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("quantity") > 0)


# -------------------------------------------------------
# 5. CREATE DIMENSION TABLES
# -------------------------------------------------------

# Dimension Customer
dim_customer = customers_df.select(
    "customer_id",
    "customer_name",
    "city",
    "state"
)

# Dimension Product
dim_product = products_df.select(
    "product_id",
    "product_name",
    "category",
    "price"
)


# -------------------------------------------------------
# 6. CREATE FACT TABLE
# -------------------------------------------------------

fact_sales = sales_df.join(
    dim_product,
    "product_id",
    "inner"
)

fact_sales = fact_sales.withColumn(
    "total_amount",
    col("quantity") * col("price")
)

fact_sales = fact_sales.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "price",
    "total_amount"
)


# -------------------------------------------------------
# 7. CREATE DATE DIMENSION
# -------------------------------------------------------

dim_date = sales_df.select(
    "order_date"
).distinct()

dim_date = dim_date \
    .withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .withColumn("day", dayofmonth("order_date"))


# -------------------------------------------------------
# 8. LOAD INTO DATA WAREHOUSE
# -------------------------------------------------------

print("\nWriting Dimension Tables...")


dim_customer.write \
    .mode("overwrite") \
    .saveAsTable("dim_customer")


dim_product.write \
    .mode("overwrite") \
    .saveAsTable("dim_product")


dim_date.write \
    .mode("overwrite") \
    .saveAsTable("dim_date")


print("Writing Fact Table...")


fact_sales.write \
    .mode("overwrite") \
    .saveAsTable("fact_sales")


print("Data Warehouse Load Completed")


# -------------------------------------------------------
# 9. CREATE ANALYTICAL SUMMARY TABLE
# -------------------------------------------------------

daily_sales_summary = fact_sales.join(
    dim_product,
    "product_id"
)

daily_sales_summary = daily_sales_summary.groupBy(
    "order_date",
    "category"
).agg(
    _sum("total_amount").alias("daily_revenue"),
    _sum("quantity").alias("total_units_sold"),
    count("order_id").alias("number_of_orders")
)


daily_sales_summary.write \
    .mode("overwrite") \
    .saveAsTable("daily_sales_summary")


# -------------------------------------------------------
# 10. DISPLAY WAREHOUSE TABLES
# -------------------------------------------------------

print("\nTables in Data Warehouse:")

spark.sql("SHOW TABLES").show()


# -------------------------------------------------------
# 11. ANALYTICAL QUERIES
# -------------------------------------------------------

print("\n==============================")
print("QUERY 1: Total Revenue")
print("==============================")


spark.sql("""
SELECT
    SUM(total_amount) AS total_revenue
FROM fact_sales
""").show()


print("\n==============================")
print("QUERY 2: Revenue by Category")
print("==============================")


spark.sql("""
SELECT
    p.category,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p
    ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC
""").show()


print("\n==============================")
print("QUERY 3: Revenue by Customer")
print("==============================")


spark.sql("""
SELECT
    c.customer_name,
    SUM(f.total_amount) AS total_spent
FROM fact_sales f
JOIN dim_customer c
    ON f.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_spent DESC
""").show()


print("\n==============================")
print("QUERY 4: Daily Revenue")
print("==============================")


spark.sql("""
SELECT
    order_date,
    SUM(total_amount) AS revenue
FROM fact_sales
GROUP BY order_date
ORDER BY order_date
""").show()


print("\n==============================")
print("QUERY 5: City-wise Revenue")
print("==============================")


spark.sql("""
SELECT
    c.city,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_customer c
    ON f.customer_id = c.customer_id
GROUP BY c.city
ORDER BY revenue DESC
""").show()


# -------------------------------------------------------
# 12. STOP SPARK
# -------------------------------------------------------

spark.stop()
