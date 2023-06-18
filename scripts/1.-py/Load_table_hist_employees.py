# Databricks notebook source
# MAGIC %md Lectura e ingesta de los datos
# MAGIC

# COMMAND ----------

from pyspark.sql import types as ty

# COMMAND ----------

# definimos un schema empoyees
schema_hire_employee = ty.StructType(
[
    ty.StructField("ID", ty.IntegerType(), True),
    ty.StructField("NAME", ty.StringType(), True),
    ty.StructField("DATETIME", ty.StringType(), True),
    ty.StructField("DEPARTMENT_ID", ty.IntegerType(), True),
    ty.StructField("JOB_ID", ty.IntegerType(), True)
    
])

# COMMAND ----------

df_table = spark.read.options(header = False, delimiter = ",")\
            .schema(schema_hire_employee)\
            .csv("dbfs:///FileStore/challenge/hired_employees__1_.csv")

# COMMAND ----------

df_table.show(truncate=False)

# COMMAND ----------

df_table.write.format("jdbc").options(
url="jdbc:sqlserver://challenge-data.database.windows.net:1433;database=prueba-01;user=main@challenge-data;password=Loquita9277$;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30",
driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
user="main",
password="Loquita9277$",
dbtable="CHALLENGE.HIRED_EMPLOYEES"
).mode("overwrite").save()
