# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

silver_df = spark.read.table("`e-commerce_orders_practice`.silver.orders")
silver_df.count()

# COMMAND ----------

# DBTITLE 1,DAILY SALES METRICS
# MAGIC %md DAILY SALES METRICS
# MAGIC

# COMMAND ----------

daily_sales = (
    silver_df.filter((col("status") == "Delivered") & (~col("is_refund")))
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_amount"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_quantity"),
    )
    .withColumn(
        "revenue_per_customer", round(col("total_revenue") / col("unique_customers"), 2)
    )
    .orderBy("order_date")
)
daily_sales.write.format("delta").mode("overwrite").saveAsTable(
    "`e-commerce_orders_practice`.gold.daily_sales"
)

# COMMAND ----------

# MAGIC %md  CUSTOMER LIFETIME VALUE (CLV)

# COMMAND ----------

customer_clv = (
    silver_df.filter((col("status") == "Delivered") & (~col("is_refund")))
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("lifetime_value"),
        avg("amount").alias("average_order_value"),
        min("order_date").alias("first_order_date"),
        max("order_date").alias("last_order_date"),
        countDistinct("product_name").alias("unique_product_purchased"),
        countDistinct("city").alias("cities_ordered_from"),
    )
    .withColumn(
        "customer_tenure_dayes",
        datediff(col("last_order_date"), col("first_order_date")),
    )
    .withColumn(
        "customer_segment",
        when(col("lifetime_value") > 100000, lit("VIP"))
        .when(col("lifetime_value") > 50000, lit("Premium"))
        .when(col("lifetime_value") > 20000, lit("Regular"))
        .otherwise(lit("New")),
    )
    .withColumn(
        "order_frequency",
        round(col("total_orders") / (col("customer_tenure_dayes") + 1), 2),
    )
)

customer_clv.write.format("delta").mode("overwrite").saveAsTable(
    "`e-commerce_orders_practice`.gold.customer_clv"
)

# COMMAND ----------

# MAGIC %md PRODUCT PERFORMANCE

# COMMAND ----------

from pyspark.sql.window import *

product_performance = (
    silver_df.filter((col("status") == "Delivered") & ~col("is_refund"))
    .groupBy("product_name")
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        sum("quantity").alias("total_unit_sold"),
        avg("amount").alias("average_selling_price"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn(
        "revenue_per_unit", round(col("total_revenue") / col("total_unit_sold"), 2)
    )
    .orderBy(col("total_revenue").desc())
)

window_spec = Window.partitionBy(col("product_name")).orderBy(
    col("total_revenue").desc()
)

product_performance = product_performance.withColumn(
    "revenue_rank", row_number().over(window_spec)
)
product_performance.write.format("delta").mode("overwrite").saveAsTable(
    "`e-commerce_orders_practice`.gold.product_performance"
)

# COMMAND ----------

# MAGIC %md CITY-WISE ANALYSIS

# COMMAND ----------

city_performance = (
    silver_df.filter((col("status") == "Delivered") & ~col("is_refund"))
    .groupBy("city")
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("average_order_value"),
        countDistinct("customer_id").alias("unique_customer"),
        countDistinct("product_name").alias("product_sold"),
    )
    .withColumn(
        "order_per_customer", round(col("total_revenue") / col("unique_customer"), 2)
    )
    .orderBy(col("total_revenue").desc())
)

city_performance.write.format("delta").mode("overwrite").saveAsTable(
    "`e-commerce_orders_practice`.gold.city_performance"
)

# COMMAND ----------

# MAGIC %md PAYMENT METHOD ANALYSIS

# COMMAND ----------

payment_analysis = (
    silver_df.filter((col("status") == "Delivered") & ~col("is_refund"))
    .groupBy("payment_method")
    .agg(
        count("order_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_transaction_value"),
        sum(when(col("is_refund"), 1).otherwise(0)).alias("refund_count"),
        sum(when(col("is_refund"), col("amount")).otherwise(0)).alias("refund_amount"),
    )
    .withColumn(
        "refund_rate", round((col("refund_count") / col("total_transactions")) * 100, 2)
    )
    .orderBy(col("total_amount").desc())
)

payment_analysis.write.format("delta").mode("overwrite").saveAsTable(
    "`e-commerce_orders_practice`.gold.payment_analysis"
)

# COMMAND ----------

# MAGIC %md MONTHLY SALES TREND

# COMMAND ----------

monthly_trend = (
    silver_df.filter((col("status") == "Delivered") & (~col("is_refund")))
    .withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy(col("year_month").asc())
)

window_spec = Window.orderBy(col("year_month").asc())

monthly_trend = monthly_trend.withColumn(
    "prev_month_revenue", lag(col("total_revenue")).over(window_spec)
).withColumn(
    "mom_growth_pct",
    round(
        when(col("prev_month_revenue").isNotNull() & (col("prev_month_revenue") != 0),
             ((col("total_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100
        ).otherwise(lit(None)), 2
    ),
)

monthly_trend.write.mode('overwrite').format('delta').saveAsTable('`e-commerce_orders_practice`.gold.monthly_trend')

# COMMAND ----------

# MAGIC %md ORDER STATUS SUMMARY

# COMMAND ----------

status_summary = (
    silver_df.groupBy("status")
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
    )
    .withColumn(
        "percentage",
        round(
            (col("order_count") / sum("order_count").over(Window.partitionBy())) * 100,
            2,
        ),
    )
)

status_summary.write.mode("overwrite").format("delta").saveAsTable(
    "`e-commerce_orders_practice`.gold.status_summary"
)


# COMMAND ----------

# MAGIC %md DELIVERY PERFORMANCE

# COMMAND ----------

delivery_performance = silver_df.filter(
    (col('status') == 'Delivered') & 
    (~col("delivery_before_order")) & 
    (col('delivery_date').isNotNull())
).withColumn(
    'delivery_days', datediff(col("delivery_date"), col("order_date"))
).groupBy('city').agg(
    count('order_id').alias('order_count'),
    avg('delivery_days').alias('avg_delivery_days'),
    min('delivery_days').alias('minimum_delivery_days'),
    max('delivery_days').alias('maximum_delivery_days'),
    percentile_approx('delivery_days', 0.5).alias('median_delivery_days')
).orderBy('avg_delivery_days')

delivery_performance.write.mode('overwrite').format('delta').saveAsTable('`e-commerce_orders_practice`.gold.delivery_performance')

# COMMAND ----------

# MAGIC %md CUSTOMER COHORT ANALYSIS

# COMMAND ----------

customer_cohort = (
    silver_df.filter(col("status") == "Delivered")
    .groupBy("customer_id")
    .agg(min("order_date").alias("first_order_date"))
    .withColumn("cohort_month", date_format(col("first_order_date"), "yyyy-MM"))
)

cohort_analysis = (
    silver_df.filter(col("status") == "Delivered")
    .join(customer_cohort, "customer_id")
    .withColumn("order_month", date_format(col("order_date"), "yyyy-MM"))
    .groupBy("cohort_month", "order_month")
    .agg(
        countDistinct("customer_id").alias("active_customers"),
        sum("amount").alias("cohort_revenue"),
    )
    .orderBy(col("cohort_month"), col("order_month"))
)

cohort_analysis.write.mode('overwrite').format('delta').saveAsTable('`e-commerce_orders_practice`.gold.cohort_analysis')

# COMMAND ----------

# MAGIC %md  DATA QUALITY DASHBOARD

# COMMAND ----------

quality_dashboard = silver_df.agg(
   count("*").alias("total_records"),
    sum(when(col("is_valid_phone"), 1).otherwise(0)).alias("valid_phones"),
    sum(when(col("is_refund"), 1).otherwise(0)).alias("refund_records"),
    sum(when(col("delivery_before_order"), 1).otherwise(0)).alias("delivery_date_errors"),
    countDistinct("customer_id").alias("unique_customers"),
    countDistinct("order_id").alias("unique_orders")
).withColumn(
    "refund_rate_pct", round((col("refund_records") / col("total_records")) * 100, 2)
)

quality_dashboard.write.mode('overwrite').format('delta').saveAsTable('`e-commerce_orders_practice`.gold.quality_dashboard')

# COMMAND ----------

