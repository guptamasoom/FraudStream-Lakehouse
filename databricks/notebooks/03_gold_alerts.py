# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Alerts
# MAGIC Aggregates high-risk transactions into dashboard and analyst-ready Delta tables.

# COMMAND ----------

from pyspark.sql.functions import avg, countDistinct, desc, sum as spark_sum

tx = spark.table("fraudstream_silver.transactions")

daily_summary = (
    tx.groupBy("event_date")
    .agg(
        countDistinct("transaction_id").alias("transactions"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_amount"),
    )
)

daily_summary.write.mode("overwrite").format("delta").saveAsTable("fraudstream_gold.daily_fraud_summary")

tx.orderBy(desc("amount")).limit(100).write.mode("overwrite").format("delta").saveAsTable("fraudstream_gold.realtime_alerts")

