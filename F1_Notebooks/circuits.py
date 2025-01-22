# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("source","")
otp=dbutils.widgets.get("source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

display(v_file_date)

# COMMAND ----------

otp

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Ingesting data of circuits.csv

# COMMAND ----------

circuits_data=spark.read.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

type(circuits_data)

# COMMAND ----------

circuits_data.show()

# COMMAND ----------

display(circuits_data)

# COMMAND ----------

circuits_data=spark.read.option("header", True).csv(f'abfss://raw@kartheekuntdl.dfs.core.windows.net/{v_file_date}/circuits.csv')

# COMMAND ----------

circuits_data.show()

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,DoubleType,StringType

# COMMAND ----------

circuit_schema=StructType(fields=(
        [StructField("circuitId",IntegerType(),False),
        StructField("circuitRef",StringType(),False),
        StructField("name",StringType(),False),
        StructField("location",StringType(),False),
        StructField("country",StringType(),False),
        StructField("lat",DoubleType(),False),
        StructField("lon",DoubleType(),False),
        StructField("alt",IntegerType(),False),
        StructField("url",StringType(),False)]
                                )
                          )
        

# COMMAND ----------

circuits_data=spark.read.option('header', True)\
                        .schema(circuit_schema)\
                        .csv(f'abfss://raw@kartheekuntdl.dfs.core.windows.net/{v_file_date}/circuits.csv')

# COMMAND ----------

display(circuits_data.describe())

# COMMAND ----------

circuits_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_columns=circuits_data.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lon"),col("alt"))

# COMMAND ----------

circuits_selected_columns.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_selected_columns=circuits_selected_columns.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_ref")\
        .withColumnRenamed("lat","latitude")\
            .withColumnRenamed("lon","longitude")\
                .withColumnRenamed("alt","altitude")\
                    .withColumn("otp",lit(otp))\
                        .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_selected_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating new column called ingestion_date to the data frame

# COMMAND ----------

Circuits_final_columns=adding_ingestiondate(circuits_selected_columns)

# COMMAND ----------

display(Circuits_final_columns)

# COMMAND ----------

processed_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### writing the above dataframe into processed layer as parquet file

# COMMAND ----------

Circuits_final_columns.write.mode("overWrite").parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

display(spark.read.parquet('abfss://processed@kartheekuntdl.dfs.core.windows.net/circuits'))

# COMMAND ----------

dbutils.notebook.exit("success")
