# Databricks notebook source
# MAGIC %md Proceso de backup BD
# MAGIC

# COMMAND ----------

#libraries
from pyspark.sql import SparkSession
from datetime import date
from pyspark.sql.avro.functions import  to_avro

# COMMAND ----------

# declaration variables 
server = 'challenge-data.database.windows.net'
database = 'prueba-01'
username =  'main'
password =  'Loquita9277$' # Se podria usar Azure Key vault para registrar la clave
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
table_nm = "CHALLENGE.HIRED_EMPLOYEES"
current_date = str(date.today())
output_path = (f"dbfs:///FileStore/challenge/backups/backup_hired_employees_{current_date}")

#connection_string = f"Driver={driver};Server={server};Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;"

# COMMAND ----------

spark = SparkSession.builder \
    .appName('SQL Server Connection') \
    .getOrCreate()

url = f'jdbc:sqlserver://{server};databaseName={database};user={username};password={password}'


# COMMAND ----------

# Genera backup y guardado en formato AVRO
data = spark.read \
    .format('jdbc') \
    .option('url', url) \
    .option('dbtable', table_nm) \
    .option('driver', driver) \
    .load()


# COMMAND ----------

# Guardar data en avro
data_avro = data.selectExpr("*").toDF(*[c.replace(" ", "_") for c in data.columns])
data_avro.write.format("avro").mode("overwrite").save(output_path)
