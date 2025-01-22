# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

data_schema=StructType(fields=[StructField("raceId",IntegerType(),True),\
    StructField("driverId",IntegerType(),True),\
        StructField("stop",IntegerType(),True),\
            StructField("lap",IntegerType(),True),\
                StructField("time",StringType(),True),\
                    StructField("duration",StringType(),True),\
                        StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

pitstops_data=spark.read.schema(data_schema)\
    .option("multiLine",True)\
    .json("abfss://raw@kartheekuntdl.dfs.core.windows.net/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstops_data.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(pitstops_data)

# COMMAND ----------

pitstops_data.write.mode("overWrite").parquet("abfss://processed@kartheekuntdl.dfs.core.windows.net/pit_stops")

# COMMAND ----------

display("abfss://processed@kartheekuntdl.dfs.core.windows.net/pit_stops")
