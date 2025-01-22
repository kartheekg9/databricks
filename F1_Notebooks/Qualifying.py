# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

Qualifying_schema=StructType(fields=[StructField("qualifyingId",IntegerType(),True),\
    StructField("raceId",IntegerType(),True),\
        StructField("driverId",IntegerType(),True),\
            StructField("constructorId",IntegerType(),True),\
                StructField("number",IntegerType(),True),\
                    StructField("position",IntegerType(),True),\
                        StructField("q1",StringType(),True),\
                            StructField("q2",StringType(),True),\
                                StructField("q3",StringType(),True)
                        ])

# COMMAND ----------

Qualifying_data=spark.read.schema(Qualifying_schema)\
    .option("multiLine",True)\
.json("abfss://raw@kartheekuntdl.dfs.core.windows.net/qualifying")


# COMMAND ----------

display(Qualifying_data)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifyed_renamed=Qualifying_data.withColumnRenamed("qualifyingId","qualifying_id")\
    .withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(qualifyed_renamed)

# COMMAND ----------

qualifyed_renamed.write.mode("overWrite").parquet("abfss://processed@kartheekuntdl.dfs.core.windows.net/qualifying")
