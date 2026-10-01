from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ------------------------------------------------
# 1. Create Spark Session
# ------------------------------------------------

spark = (
    SparkSession.builder
    .appName("DataLakeDemo")
    .master("local[*]")
    .getOrCreate()
)


# ------------------------------------------------
# 2. Define Data Lake paths
# ------------------------------------------------

RAW_CUSTOMERS = "data_lake/raw/customers"
RAW_ORDERS = "data_lake/raw/orders"

PROCESSED_CUSTOMERS = "data_lake/processed/customers"
PROCESSED_ORDERS = "data_lake/processed/orders"

CURATED_SALES = "data_lake/curated/sales"


# ------------------------------------------------
# 3. Read Customer CSV
# ------------------------------------------------

customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_CUSTOMERS)
)

print("\n===== CUSTOMERS =====")

customers_df.show()

customers_df.printSchema()


# ------------------------------------------------
# 4. Read Orders JSON
# ------------------------------------------------

orders_df = spark.read.json(RAW_ORDERS)

print("\n===== ORDERS =====")

orders_df.show()

orders_df.printSchema()


# ------------------------------------------------
# 5. Clean Customer Data
# ------------------------------------------------

customers_processed = (
    customers_df
    .withColumn(
        "name",
        col("name").cast("string")
    )
    .withColumn(
        "city",
        col("city").cast("string")
    )
)


# ------------------------------------------------
# 6. Clean Orders Data
# ------------------------------------------------

orders_processed = (
    orders_df
    .withColumn(
        "quantity",
        col("quantity").cast("integer")
    )
    .withColumn(
        "price",
        col("price").cast("double")
    )
)


# ------------------------------------------------
# 7. Write Processed Data
# ------------------------------------------------

customers_processed.write \
    .mode("overwrite") \
    .parquet(PROCESSED_CUSTOMERS)


orders_processed.write \
    .mode("overwrite") \
    .parquet(PROCESSED_ORDERS)


print("\nProcessed data written successfully.")


# ------------------------------------------------
# 8. Join Customers and Orders
# ------------------------------------------------

sales_df = (
    orders_processed
    .join(
        customers_processed,
        orders_processed.customer_id ==
        customers_processed.customer_id,
        "left"
    )
)


# ------------------------------------------------
# 9. Calculate Total Amount
# ------------------------------------------------

sales_df = sales_df.withColumn(
    "total_amount",
    col("quantity") * col("price")
)


# ------------------------------------------------
# 10. Select Final Columns
# ------------------------------------------------

sales_df = sales_df.select(
    orders_processed.order_id,
    orders_processed.customer_id,
    customers_processed.name,
    customers_processed.city,
    orders_processed.product,
    orders_processed.quantity,
    orders_processed.price,
    col("total_amount")
)


# ------------------------------------------------
# 11. Display Curated Data
# ------------------------------------------------

print("\n===== CURATED SALES =====")

sales_df.show()


# ------------------------------------------------
# 12. Write Curated Data
# ------------------------------------------------

sales_df.write \
    .mode("overwrite") \
    .parquet(CURATED_SALES)


print("\nCurated sales data written successfully.")


# ------------------------------------------------
# 13. Stop Spark
# ------------------------------------------------

spark.stop()
