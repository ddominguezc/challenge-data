# Databricks notebook source
# MAGIC %md Lectura e ingesta de los datos
# MAGIC

# COMMAND ----------

from pyspark.sql import types as ty

# COMMAND ----------

# definimos un schema departments
schema_deparments = ty.StructType(
[
    ty.StructField("ID", ty.IntegerType(), True),
    ty.StructField("DEPARTMENT", ty.StringType(), True)    
])

# COMMAND ----------

df_table = spark.read.options( header= False, delimiter = ",")\
            .schema(schema_deparments)\
            .csv("dbfs:///FileStore/challenge/departments.csv")

# COMMAND ----------

df_table.write.format("jdbc").options(
url="jdbc:sqlserver://challenge-data.database.windows.net:1433;database=prueba-01;user=main@challenge-data;password=Loquita9277$;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30",
driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
user="main",
password="Loquita9277$",
dbtable="CHALLENGE.DEPARTMENTS"
).mode("overwrite").save()
