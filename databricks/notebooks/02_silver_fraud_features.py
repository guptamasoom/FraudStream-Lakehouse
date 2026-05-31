# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Fraud Features
# MAGIC Production notebook placeholder for schema enforcement, quality checks, feature creation, and quarantine writes.

# COMMAND ----------

from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

transaction_schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("payment_method", StringType()),
        StructField("device_id", StringType()),
        StructField("ip_country", StringType()),
        StructField("billing_country", StringType()),
        StructField("shipping_country", StringType()),
        StructField("is_card_present", BooleanType()),
        StructField("auth_result", StringType()),
        StructField("is_known_fraud", BooleanType()),
    ]
)

bronze = spark.table("fraudstream_bronze.raw_events").where(col("topic") == "fraud_transactions")
transactions = bronze.select(from_json(col("raw_json"), transaction_schema).alias("event")).select("event.*")

silver = (
    transactions.where(col("event_id").isNotNull() & col("customer_id").isNotNull() & (col("amount") >= 0))
    .withColumn("event_date", to_date(col("event_timestamp")))
)

silver.write.mode("append").format("delta").saveAsTable("fraudstream_silver.transactions")

