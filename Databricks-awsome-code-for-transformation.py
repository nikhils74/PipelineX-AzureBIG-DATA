# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connecting to Storage Account using the App Registration Creds

# COMMAND ----------

storage_account = "dsfsdfsdf"
application_id = "ejejjeej"
directory_id = "hehehhe"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "hehehehehhehe")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the data from Storage/bronze(files)

# COMMAND ----------

base_path = "abfss://olistdata@olistdatastorageaccountn.dfs.core.windows.net/bronze/"
orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path= base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

orders_df = spark.read.format("csv").option("header", "true").load(orders_path)
payments_df = spark.read.format("csv").option("header", "true").load (payments_path)
reviews_df = spark.read.format("csv").option("header", "true").load(reviews_path)
items_df = spark.read.format("csv").option("header", "true").load(items_path)
customers_df = spark.read.format("csv").option("header", "true").load(customers_path)
sellers_df = spark.read.format("csv").option("header", "true").load(sellers_path)
geolocation_df = spark.read.format("csv").option("header", "true").load(geolocation_path)
products_df = spark.read.format("csv").option("header", "true").load(products_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connecting to MongoDB using MonogClient & Credentials 

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "gfdgdfg"
database ="
port = ""
username = "
password = "it "

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------


import pandas as pd 
collection = mydatabase ['product_categories']

mongo_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

products_df.display()

# COMMAND ----------

mongo_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning the Data

# COMMAND ----------

from pyspark.sql.functions import col,to_date,datediff,current_date,to_date

# COMMAND ----------

def clean_dataframe(df,name):
    print("Cleaning "+name)
    return df.dropDuplicates().na.drop('all')

orders_df =clean_dataframe(orders_df,"Orders")
display(orders_df)

# COMMAND ----------

# Convert Data Columns

orders_df = orders_df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))\
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))\
        .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))

# COMMAND ----------

# Calculate Delivery and Time Delays

orders_df = orders_df.withColumn("actual_delivery_time", datediff(col("order_delivered_customer_date"), "order_purchase_timestamp"))
orders_df = orders_df.withColumn("estimated_delivery_time", datediff(col("order_estimated_delivery_date"), "order_purchase_timestamp"))
orders_df = orders_df.withColumn("Delay_Time", col("estimated_delivery_time") - col("actual_delivery_time")) 

display(orders_df)

# COMMAND ----------

display(orders_df.tail(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Datasets

# COMMAND ----------

orders_customers_df = orders_df.join(customers_df, orders_df.customer_id == customers_df.customer_id, "left")

order_payments_df = orders_customers_df.join(payments_df, orders_customers_df.order_id == payments_df.order_id, "left")

order_items_df = order_payments_df.join(items_df, on=['order_id'], how='left') 

orders_items_products_df = order_items_df.join(products_df, order_items_df.product_id == products_df.product_id, "left")

final_df = orders_items_products_df.join(sellers_df, orders_items_products_df.seller_id == sellers_df.seller_id, "left")



# COMMAND ----------

display(final_df)

# COMMAND ----------

mongo_data.drop('_id',axis=1,inplace=True)

mongo_spark_df = spark.createDataFrame(mongo_data)
display(mongo_spark_df) 

# COMMAND ----------

final_df = final_df.join(mongo_spark_df, "product_category_name","left") 

# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_duplicate_colums(df):
    columns = df.columns

    seem_columns = set()
    columns_to_drop = []

    for column in columns:
        if column in seem_columns:
            columns_to_drop.append(column)  

        else:
            seem_columns.add(column)    

    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

final_df = remove_duplicate_colums(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastorageaccountn.dfs.core.windows.net/silver")