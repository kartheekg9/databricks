# Databricks notebook source
from pyspark.sql.functions import col,lit

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Ingesting the races.csv

# COMMAND ----------

races=spark.read.csv(f"abfss://raw@kartheekuntdl.dfs.core.windows.net/{v_file_date}/races.csv") 

# COMMAND ----------

display(races)

# COMMAND ----------

races_columns=spark.read.option("header",True)\
                        .csv(f"abfss://raw@kartheekuntdl.dfs.core.windows.net/{v_file_date}/races.csv")

# COMMAND ----------

display(races_columns)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

races_schema=StructType(fields=([StructField("raceId",IntegerType(),False),
                                 StructField("year",IntegerType(),False),
                                 StructField("round",IntegerType(),False),
                                 StructField("circuitId",IntegerType(),False),
                                 StructField("name",StringType(),False),
                                 StructField("date",StringType(),False),
                                 StructField("time",StringType(),False),
                                 StructField("url",StringType(),False),]))

# COMMAND ----------

races_columns=spark.read.option("header",True)\
                         .schema(races_schema)\
                             .csv(f'abfss://raw@kartheekuntdl.dfs.core.windows.net/{v_file_date}/races.csv')
    

# COMMAND ----------



# COMMAND ----------

races_selected_columns=races_columns.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))

# COMMAND ----------

display(races_selected_columns)

# COMMAND ----------

races_renamed_columns=races_selected_columns.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("circuitId","circuit_id")\
        .withColumnRenamed("year","race_year")\
            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(races_renamed_columns)

# COMMAND ----------

from pyspark.sql.functions import concat,to_timestamp,lit

# COMMAND ----------

races_combined_column=races_renamed_columns.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_combined_column)

# COMMAND ----------


races_combined_column.write.mode('overwrite').parquet(
    'abfss://processed@kartheekuntdl.dfs.core.windows.net/races'
)

# COMMAND ----------

races_combined_column.write.mode('overWrite').partitionBy('race_year').parquet(f'abfss://processed@kartheekuntdl.dfs.core.windows.net/{v_file_date}/races')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

display(races_combined_column)

# COMMAND ----------

races_final_columns=races_combined_column.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(races_final_columns)

# COMMAND ----------

races_final_columns.write.mode('overWrite').partitionBy('race_year').parquet(f'abfss://processed@kartheekuntdl.dfs.core.windows.net/races')
