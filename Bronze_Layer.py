# Databricks notebook source
# Create realistic RAW data with all the problems you'll see in production
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

# Raw data with real-world issues
raw_data = []
products = ['Laptop', 'Mobile Phone', 'Tablet', 'Headphones', 'Smart Watch', 'Camera', 'Keyboard', 'Mouse']
cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', None]  # Note: Nulls
payment_methods = ['Credit Card', 'Debit Card', 'UPI', 'Cash on Delivery', 'Net Banking', 'wallet', None]

for i in range(1, 5001):
    # Introducing real-world data quality issues
    order_id = f"ORD{i:05d}" if i % 20 != 0 else None  # Some null order IDs
    customer_id = f"CUST{random.randint(1, 500):04d}" if i % 15 != 0 else f"CUST{random.randint(1, 500):04d}"  # Some duplicates
    
    # Inconsistent date formats
    if i % 3 == 0:
        order_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
    elif i % 3 == 1:
        order_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%d/%m/%Y")
    else:
        order_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y/%m/%d")
    
    product_name = random.choice(products)
    
    # Negative and zero quantities (data entry errors)
    quantity = random.choice([1, 2, 3, 4, 5, -1, 0]) if i % 30 == 0 else random.randint(1, 5)
    
    # Negative amounts (refunds/errors), nulls, wrong decimals
    if i % 25 == 0:
        amount = None
    elif i % 40 == 0:
        amount = -random.randint(100, 5000)  # Refunds
    else:
        amount = random.randint(500, 50000)
    
    # Inconsistent status values
    status_options = ['Delivered', 'delivered', 'DELIVERED', 'Pending', 'pending', 'Cancelled', 'CANCELLED', 'Returned', None, 'Unknown']
    status = random.choice(status_options)
    
    # Mixed case cities, nulls, extra spaces
    city = random.choice(cities)
    if city and i % 10 == 0:
        city = f"  {city.lower()}  "  # Extra spaces, lowercase
    
    # Inconsistent payment methods
    payment_method = random.choice(payment_methods)
    
    # Email issues - some invalid, some nulls
    if i % 20 == 0:
        email = None
    elif i % 15 == 0:
        email = f"customer{random.randint(1, 500)}"  # Invalid email (no @)
    else:
        email = f"customer{random.randint(1, 500)}@email.com"
    
    # Phone number issues
    if i % 18 == 0:
        phone = None
    elif i % 12 == 0:
        phone = f"{random.randint(1000000, 9999999)}"  # Wrong format (7 digits)
    else:
        phone = f"+91{random.randint(7000000000, 9999999999)}"
    
    # Delivery date (sometimes before order date - data error)
    if status and 'deliver' in status.lower():
        days_to_deliver = random.randint(-5, 20)  # Negative means data error
        delivery_date = (
            datetime.strptime(
                order_date,
                "%Y-%m-%d" if "-" in order_date else "%d/%m/%Y" if "/" in order_date and order_date[2] == "/" else "%Y/%m/%d"
            ) + timedelta(days=days_to_deliver)
        ).strftime("%Y-%m-%d")
    else:
        delivery_date = None
    
    # Duplicate records (same order_id)
    raw_data.append((
        order_id, customer_id, order_date, product_name, quantity, 
        amount, status, city, payment_method, email, phone, delivery_date
    ))
    
    # Add duplicate for some records
    if i % 50 == 0:
        raw_data.append((
            order_id, customer_id, order_date, product_name, quantity, 
            amount, status, city, payment_method, email, phone, delivery_date
        ))

# Create DataFrame
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("city", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("delivery_date", StringType(), True)
])

bronze_df = spark.createDataFrame(raw_data, schema)

# Save to Bronze table
bronze_df.write.format("delta").mode("overwrite").saveAsTable("`e-commerce_orders_practice`.`bronze`.`orders`")

print(f"✅ Bronze table created with {bronze_df.count()} records (including duplicates and bad data)")
print("\n🔍 Data Quality Issues Present:")
print("- Null order IDs")
print("- Inconsistent date formats")
print("- Negative/zero quantities")
print("- Negative amounts (refunds)")
print("- Null amounts")
print("- Inconsistent status values (mixed case)")
print("- Cities with extra spaces and mixed case")
print("- Null cities")
print("- Invalid emails (no @)")
print("- Invalid phone numbers")
print("- Duplicate records")
print("- Delivery dates before order dates")

display(bronze_df.limit(20))

# COMMAND ----------

