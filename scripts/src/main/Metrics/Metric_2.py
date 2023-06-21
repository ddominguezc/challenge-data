# Databricks notebook source
#libraries
from flask import Flask, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, quarter, when, mean
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

# variables
server = 'challenge-data.database.windows.net'
database = 'prueba-01'
username =  'main'
password =  'Loquita9277$' # Se podria usar Azure Key vault para registrar la clave
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
table_hired_employee = "CHALLENGE.HIRED_EMPLOYEES"
table_department = "CHALLENGE.DEPARTMENTS"
table_job = "CHALLENGE.JOBS"

# COMMAND ----------

# making a connection
spark = SparkSession.builder \
    .appName('SQL Server Connection') \
    .getOrCreate()

url = f'jdbc:sqlserver://{server};databaseName={database};user={username};password={password}'

app = Flask(__name__)


# COMMAND ----------


@app.route('/employees/mean', methods=['GET'])

def department_stats():

    data_emp_df = spark.read \
        .format('jdbc') \
        .option('url', url) \
        .option('dbtable', table_hired_employee) \
        .option('driver', driver) \
        .load()

    data_dep_df = spark.read \
        .format('jdbc') \
        .option('url', url) \
        .option('dbtable', table_department) \
        .option('driver', driver) \
        .load()

    data_job_df = spark.read \
        .format('jdbc') \
        .option('url', url) \
        .option('dbtable', table_job) \
        .option('driver', driver) \
        .load()

    data_filtered_df = data_emp_df.alias("emp") \
        .join(data_dep_df.alias("dep"), (col("emp.DEPARTMENT_ID") == col("dep.ID")), 'inner') \
        .filter(col("emp.DATETIME").between('2021-01-01', '2022-01-01')) \
        .filter(col("JOB_ID").isNotNull()) \
        .groupBy('dep.ID', 'DEPARTMENT') \
        .count()

    mean_employees = data_filtered_df.select(mean('count')).first()[0]
    departments_hired_more = data_filtered_df.filter(col('count') > mean_employees)
    department_stats = departments_hired_more.groupBy('ID', 'DEPARTMENT').agg({'count': 'sum'})
    department_stats = department_stats.orderBy(col('sum(count)').desc())

    department_stats = department_stats.withColumnRenamed('sum(count)', 'HIRED')


    return jsonify(department_stats)



# COMMAND ----------

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

# http://localhost:5000/employees/mean

