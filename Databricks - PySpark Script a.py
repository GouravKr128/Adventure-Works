# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Access using service principal

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("ad_works")

# COMMAND ----------

appId = dbutils.secrets.get(scope = "ad_works", key = "appId")
appSecret = dbutils.secrets.get(scope = "ad_works", key = "appSecret")
tenantId = dbutils.secrets.get(scope = "ad_works", key = "tenantId")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.gk111datalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.gk111datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.gk111datalake.dfs.core.windows.net", "1e2d3a5b-6650-40bb-b6b5-ff639e79e9f8")
spark.conf.set("fs.azure.account.oauth2.client.secret.gk111datalake.dfs.core.windows.net", appSecret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.gk111datalake.dfs.core.windows.net", "https://login.microsoftonline.com/d0ea523a-f994-4c05-be65-c0a341007b93/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@gk111datalake.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation 

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###### File -> Calendar.csv

# COMMAND ----------

df = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@gk111datalake.dfs.core.windows.net/Calendar')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('Month',month(col('Date')))\
            .withColumn('Year',year(col('Date')))

# COMMAND ----------

df.display()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@gk111datalake.dfs.core.windows.net/Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### File -> Customers.csv

# COMMAND ----------

df = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@gk111datalake.dfs.core.windows.net/Customers')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("fullName",concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName')))

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@gk111datalake.dfs.core.windows.net/Customers")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### File -> Product_Subcategories.csv

# COMMAND ----------

df = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@gk111datalake.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@gk111datalake.dfs.core.windows.net/SubCategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### File -> Product_Categories.csv

# COMMAND ----------

df = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@gk111datalake.dfs.core.windows.net/Product_Categories')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@gk111datalake.dfs.core.windows.net/Product_Categories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### File -> Products.csv

# COMMAND ----------

df = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@gk111datalake.dfs.core.windows.net/Products')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@gk111datalake.dfs.core.windows.net/Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Files -> Sales_2015.csv, Sales_2016.csv, Sales_2017.csv

# COMMAND ----------

df = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@gk111datalake.dfs.core.windows.net/Sales*')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@gk111datalake.dfs.core.windows.net/Sales")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### File -> Returns.csv

# COMMAND ----------

df = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@gk111datalake.dfs.core.windows.net/Returns')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@gk111datalake.dfs.core.windows.net/Returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### File -> Territories.csv

# COMMAND ----------

df = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@gk111datalake.dfs.core.windows.net/Territories')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@gk111datalake.dfs.core.windows.net/Territories")\
            .save()

# COMMAND ----------

