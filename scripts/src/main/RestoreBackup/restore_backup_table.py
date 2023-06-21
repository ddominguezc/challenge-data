# Databricks notebook source
#Librerias
from pyspark.sql import SparkSession

# COMMAND ----------

#Load the Avro import SparkSession
#declare widgets
dbutils.widgets.text("name_table", "departments")
dbutils.widgets.text("fec_restore", "2023-06-18")
server = 'challenge-data.database.windows.net'
database = 'prueba-01'
username = 'main'
password = 'Loquita9277$'
jdbc_url = f'jdbc:sqlserver://{server};database={database};user={username};password={password}'



# COMMAND ----------

#seteo widgets
name_table_f = dbutils.widgets.get("name_table")
fec_restore_f = dbutils.widgets.get("fec_restore")
path = (f"dbfs:///FileStore/challenge/backups/backup_{name_table_f}_{fec_restore_f}/")

# COMMAND ----------

#Create spark sesion
spark = SparkSession.builder \
    .appName('Avro Restore') \
    .getOrCreate()

# Read Avro file into DataFrame
df = spark.read.format("avro").load(path)

# COMMAND ----------

#ingest data
df.write \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("dbtable", f"CHALLENGE.{name_table_f}") \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .mode("overwrite") \
  .save()

