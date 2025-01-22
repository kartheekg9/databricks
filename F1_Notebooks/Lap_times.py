# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

lap_times_schema=StructType(fields=[StructField("raceId",IntegerType(),True),\
    StructField("driverId",IntegerType(),True),\
        StructField("lap",IntegerType(),True),\
            StructField("position",IntegerType(),True),\
                StructField("time",StringType(),True),\
                    StructField("milliseconds",IntegerType(),True),\
                        ])

# COMMAND ----------

lap_times=spark.read.schema(lap_times_schema)\
.csv("abfss://raw@kartheekuntdl.dfs.core.windows.net/lap_times/lap_times*")

# COMMAND ----------

display(lap_times)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_renamed=lap_times.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(lap_times_renamed)

# COMMAND ----------

lap_times_renamed.write.mode("overWrite").parquet("abfss://processed@kartheekuntdl.dfs.core.windows.net/laptimes")
