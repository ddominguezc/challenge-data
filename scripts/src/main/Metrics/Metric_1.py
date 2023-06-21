# Databricks notebook source
#libraries
from flask import Flask, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, quarter, when
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

# variables
server = 'challenge-data.database.windows.net'
database = 'prueba-01'
username =  'main'
password =  'Loquita9277$' # Se podria usar Azure Key vault para registrar la clave
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'|
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

@app.route('/employees/hired', methods=['GET'])

def data_metric_1():

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

    data_filtered_df = data_emp_df.alias("emp")\
    .join(data_dep_df.alias("dep"), (col("emp.DEPARTMENT_ID") == col("dep.ID")),'inner')\
    .join(data_job_df.alias("job"), (col("emp.DEPARTMENT_ID") == col("job.ID")), 'inner')\
    .filter(col("emp.DATETIME").between('2021-01-01','2022-01-01'))\
    .groupBy('DEPARTMENT', 'JOB', quarter('DATETIME').alias('quarter'))\
    .count().withColumnRenamed('count', 'number_employees')

    data_filtered_f_df = data_filtered_df.withColumn('quarter_name', when(data_filtered_df.quarter == 1, 'Q1')
                                                     .when(data_filtered_df.quarter == 2, 'Q2')
                                                     .when(data_filtered_df.quarter == 3, 'Q3')
                                                     .when(data_filtered_df.quarter == 4, 'Q4'))
    
    data_metric_1 = data_filtered_f_df.groupBy('DEPARTMENT', 'JOB').pivot('quarter_name').agg(F.first('number_employees'))
    data_metric_1 = data_metric_1.orderBy('DEPARTMENT', 'JOB')

    return jsonify(data_metric_1)

