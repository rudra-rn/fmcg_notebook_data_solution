# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronze_df = spark.table("`e-commerce_orders_practice`.bronze.orders")
bronze_df.show(10)

# COMMAND ----------

bronze_df.count()

# COMMAND ----------

silver_df = bronze_df.dropDuplicates(['order_id','customer_id','order_date'])
silver_df.count()

# COMMAND ----------

silver_df = silver_df.filter(col('order_id').isNotNull())
silver_df.count()

# COMMAND ----------

silver_df = silver_df.withColumn(
    'order_date',
    coalesce(
        try_to_date(col('order_date'), 'yyyy-MM-dd'),
        try_to_date(col('order_date'), 'dd/MM/yyyy'),
        try_to_date(col('order_date'), 'MM/dd/yyyy'),
        try_to_date(col('order_date'), 'yyyy/MM/dd'),
        try_to_date(col('order_date')) 
    )
).withColumn('delivery_date', coalesce(try_to_date(col('delivery_date'),'yyyy-MM-dd')))

# COMMAND ----------

silver_df.select("order_date",'delivery_date').show()

# COMMAND ----------

silver_df = silver_df.filter(col("order_date").isNotNull())
silver_df.count()

# COMMAND ----------

silver_df.filter(col("quantity") <= 0).show()
silver_df = silver_df.withColumn('quantity',  when(col("quantity") > 0, col("quantity")).otherwise(1))
silver_df.count()

# COMMAND ----------

silver_df = silver_df.withColumn('is_refund', when(col('amount') < 0, True).otherwise(False))

# COMMAND ----------

silver_df = silver_df.withColumn('amount', when(col("amount").isNull(), 0).when(col("amount") < 0, abs(col("amount"))).otherwise(col("amount")))

# COMMAND ----------

silver_df = silver_df.withColumn('status', when(col("status").isNull() | (col('status') == 'Unknown'), lit('Unknown')).otherwise(initcap(col("status"))))


# COMMAND ----------

silver_df = silver_df.withColumn('city',when(col("city").isNull(),lit('Unknown')).otherwise(initcap(trim(col("city")))))

# COMMAND ----------

silver_df = silver_df.withColumn('email',when(col("email").contains('@'),trim(lower(col("email")))).otherwise(None)).filter(col("email").isNotNull())

# COMMAND ----------

silver_df = silver_df.withColumn('phone',when(length(regexp_replace(col("phone"), "[^0-9]", "")) == 12, col("phone")).otherwise(None)).filter(col("phone").isNotNull())

# COMMAND ----------

silver_df = silver_df.withColumn('is_valid_phone', when(col('phone').isNotNull(), True).otherwise(False))

# COMMAND ----------

silver_df.select('payment_method').distinct().show()

# COMMAND ----------

silver_df = silver_df.withColumn('payment_method',when(col("payment_method").isNull(),lit('Unknown')).otherwise(initcap(trim(col("payment_method")))))

# COMMAND ----------

silver_df = silver_df.withColumn(
    "delivery_before_order",
    when(
        (col("delivery_date").isNotNull()) & 
        (col("delivery_date") < col("order_date")), 
        True
    ).otherwise(False)
).withColumn('delivery_date', when(col("delivery_date").isNotNull(), col("delivery_date")).otherwise(col("order_date")))

# COMMAND ----------

silver_df = silver_df.withColumn('processed_timestamp', current_timestamp()).withColumn('data_source', lit('ecommerce_orders'))

# COMMAND ----------

display(silver_df.limit(10))

# COMMAND ----------

silver_final = silver_df.select('order_id', 'customer_id', 'order_date', 'product_name', 'quantity', 'amount', 'status', 'city', 'payment_method','is_refund','is_valid_phone','email','phone','delivery_date','delivery_before_order','processed_timestamp','data_source')

# COMMAND ----------

silver_final.write.mode('overwrite').format('delta').saveAsTable('`e-commerce_orders_practice`.silver.orders')


# COMMAND ----------

