# Databricks notebook source
from pyspark.sql.types import *

circuit_schema=StructType(fields=(
        [StructField("circuitId",IntegerType(),False),
        StructField("circuitRef",StringType(),False),
        StructField("name",StringType(),False),
        StructField("location",StringType(),False),
        StructField("country",StringType(),False),
        StructField("lat",DoubleType(),False),
        StructField("lon",DoubleType(),False),
        StructField("alt",IntegerType(),False),
        StructField("url",StringType(),False)]))

# COMMAND ----------

df = spark.read.option("header", "true")\
        .schema(circuit_schema)\
        .csv('abfss://raw@kartheekuntdl.dfs.core.windows.net/2021-03-21/circuits.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

# 2. Reading CSV
df=spark.read.option("header","true")\
    .format("csv")\
        .load("abfss://raw@kartheekuntdl.dfs.core.windows.net/2021-03-21/circuits.csv")

# COMMAND ----------

# 3.filter rows in a dataset
from pyspark.sql.functions import col,desc
df.filter(col("circuitId")=="1").show()

# COMMAND ----------

# 4. selecting particular columns from a dataframe

df.select("circuitId","circuitRef","name").show()

#performing the column level operations on the selected columns
df.selectExpr("circuitId","circuitId+1 as cirId").show()

# COMMAND ----------

# 5.renaming a column in a PySpark DataFrame?

df.withColumnRenamed("circuitId","newID").show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# 6.sorting column from a PySpark DataFrame

df.sort(desc("circuitId"))\
    .select("circuitId")\
        .show()

df.orderBy(col("circuitId").desc())\
    .show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import sum, avg
df.groupBy("country")\
    .agg(sum("circuitId"),avg("circuitId"))\
        .orderBy(col("country").desc())\
        .show()

# COMMAND ----------

from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()

data1=[("kartheek",27),("yashwanth",26)]
schema1=["name","age"]

data2=[("kartheek","nellore"),("yashwanth","Hyderabad")]
schema2=["name","city"]

df1=spark.createDataFrame(data=data1,schema=schema1)
df2=spark.createDataFrame(data=data2,schema=schema2)

# COMMAND ----------

df1.join(df2,df1.name==df2.name,"inner")\
    .show()
    # .select(df1["name"],df1["age"],df2["city"])\
       

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

employees_schema=StructType([StructField("emp_id",IntegerType(),True),\
                             StructField("name",StringType(),True),\
                                 StructField("age",IntegerType(),True),\
                                     StructField("gender",StringType(),True),\
                                         StructField("dept_id",IntegerType(),True),\
                                             StructField("manager_id",IntegerType(),True)])
                                             

# COMMAND ----------

df_employees = spark.read.format('csv') \
    .schema(employees_schema) \
    .option('header', True) \
    .load('abfss://raw@kartheekuntdl.dfs.core.windows.net/demo/Employees.csv')

# COMMAND ----------



# Create initial employees DataFrame
employees_data = [
    (1, "Alice", 101, 5000),
    (2, "Bob", 102, 4000),
    (3, "Charlie", 103, 4500),
    (4, "Dave", 104, 7000),
    (5, "Eve", 105, 6000)
]
employees_columns = ["emp_id", "emp_name", "dept_id", "salary"]
employees_df = spark.createDataFrame(employees_data, employees_columns)

# Save as Delta table
employees_df.write.format("delta").mode("overwrite").save("abfss://raw@kartheekuntdl.dfs.core.windows.net/demo/Employees_merge.csv")

# Create updates DataFrame
updates_data = [
    (2, "Robert", 102, 4500),   # Update Bob's salary
    (3, "Charlie", 103, 5000),  # Update Charlie's salary
    (6, "Frank", 106, 4000)     # Insert new employee
]
updates_columns = ["emp_id", "emp_name", "dept_id", "salary"]
updates_df = spark.createDataFrame(updates_data, updates_columns)

# Save updates as Delta table
updates_df.write.format("delta").mode("overwrite").save("abfss://raw@kartheekuntdl.dfs.core.windows.net/demo/updates_merge.csv")


# COMMAND ----------

spark.sql("CREATE TABLE employees USING DELTA LOCATION 'abfss://raw@kartheekuntdl.dfs.core.windows.net/demo/Employees_merge.csv'")
spark.sql("CREATE TABLE updates USING DELTA LOCATION 'abfss://raw@kartheekuntdl.dfs.core.windows.net/demo/updates_merge.csv'")

# COMMAND ----------

spark.sql("SELECT * FROM employees").show()
spark.sql("SELECT * FROM updates").show()

# COMMAND ----------

from delta.tables import DeltaTable

employees_table=DeltaTable.forPath(spark, "abfss://raw@kartheekuntdl.dfs.core.windows.net/demo/Employees_merge.csv")
updates_df=spark.read.format("delta").load("abfss://raw@kartheekuntdl.dfs.core.windows.net/demo/updates_merge.csv")


# COMMAND ----------

emp_df=employees_table.toDF()

# COMMAND ----------

display(updates_df)
display(emp_df)

# COMMAND ----------

from pyspark.sql.functions import col

employees_table.alias("employees").merge(
    updates_df.alias("updates"),
    col("updates.emp_id") == col("employees.emp_id")
).whenMatchedUpdate(
    set={"salary": col("updates.salary")}
).whenNotMatchedInsertAll().execute()

# COMMAND ----------

employees_table.toDF().show()

# COMMAND ----------

from pyspark.sql.functions import col
employees_table.alias("main").merge(updates_df.alias("updates"), col("main.emp_id") == col("updates.emp_id"))\
    .whenMatchedUpdate(set={"salary": col("updates.salary")})\
        .whenNotMatchedInsertAll()

# COMMAND ----------

employees_table.toDF().show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName("WindowFunctionsPractice").getOrCreate()

data = [
    (1, "Alice", "HR", 70000, 2),
    (2, "Bob", "Finance", 90000, 3),
    (3, "Charlie", "HR", 75000, 2),
    (4, "David", "Finance", 80000, 4),
    (5, "Eve", "IT", 95000, 5),
    (6, "Frank", "IT", 85000, 5),
    (7, "Grace", "HR", 72000, 2),
    (8, "Hank", "Finance", 87000, 3),
    (9, "Ivy", "IT", 99000, 5),
    (10, "new", "Finance", 90000, 3),
    (10, "new", "Finance", 90000, 3)
]

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("experience", IntegerType(), True),
])

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

#Rank Employees by Salary Within Each Department Write a query to rank employees by salary within each department.
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,col
windowspec= Window.partitionBy(col("department")).orderBy(col("salary").desc())
df.withColumn("rank",rank().over(windowspec)).show()

# COMMAND ----------

#Calculate Running Total of Salaries in Each Department Find the cumulative salary for each department.
from pyspark.sql.window import Window
from pyspark.sql.functions import sum,col
windowspec= Window.partitionBy("department").orderBy(col("salary").desc())
df.withColumn("running_total",sum("salary").over(windowspec)).show()

# COMMAND ----------

#Assign a Unique Row Number to Each Employee Generate a row number for each employee within their department based on salary.

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col
columns=df.columns
windowspec= Window.partitionBy(*columns).orderBy(col("salary").desc())
df.withColumn("row_number",row_number().over(windowspec)).show()

# COMMAND ----------

from pyspark.sql.functions import max, min

window_spec = Window.partitionBy("department")


df.withColumn("max_salary", max("salary").over(window_spec)) \
  .withColumn("min_salary", min("salary").over(window_spec)).show()


# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,desc,percent_rank

windowspec=Window.partitionBy(col("department")).orderBy(col("salary"))

df.withColumn("rank",percent_rank().over(windowspec)).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,desc,percent_rank,avg

windowspec=Window.partitionBy(col("department")).orderBy(col("salary")).rowsBetween(-2,0)

df=df.withColumn("rank",avg(col("salary")).over(windowspec)).show()
