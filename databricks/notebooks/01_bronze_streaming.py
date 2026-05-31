# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Streaming Ingestion
# MAGIC Reads Kafka fraud events and stores raw JSON in Delta Bronze tables.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, date_format

kafka_bootstrap_servers = dbutils.secrets.get("fraudstream", "kafka_bootstrap_servers")
lakehouse_root = dbutils.widgets.get("lakehouse_root")
topics = "fraud_transactions,fraud_logins,fraud_devices,fraud_chargebacks"

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topics)
    .option("startingOffsets", "latest")
    .load()
)

bronze = (
    raw_stream.select(
        col("topic"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("raw_json"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
    )
    .withColumn("processing_date", date_format(col("ingested_at"), "yyyy-MM-dd"))
)

(
    bronze.writeStream.format("delta")
    .option("checkpointLocation", f"{lakehouse_root}/checkpoints/bronze")
    .partitionBy("topic", "processing_date")
    .outputMode("append")
    .toTable("fraudstream_bronze.raw_events")
)

