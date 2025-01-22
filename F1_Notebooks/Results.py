# Databricks notebook source
dbutils.widgets.text("p_date_field","")
v_date_field=dbutils.widgets.get("p_date_field")
                      

# COMMAND ----------

display(v_date_field)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType,StringType,FloatType

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(), True),\
    
        StructField("raceId",IntegerType(), True),\
            StructField("driverId",IntegerType(), True),\
                StructField("constructorId",IntegerType(), True),\
                    StructField("number",IntegerType(), True),\
                        StructField("grid",IntegerType(), True),\
                            StructField("position",IntegerType(), True),\
                                StructField("positionText",StringType(), True),\
                                    StructField("posistionOrder",IntegerType(), True),\
                                        StructField("points",FloatType(), True),\
                                            StructField("laps",IntegerType(), True),\
                                                StructField("time",StringType(), True),\
                                                    StructField(",milliSeconds",IntegerType(), True),\
                                                        StructField("fastestLap",IntegerType(), True),\
                                                            StructField("rank",IntegerType(), True),\
                                                                StructField("fastestLapTime",StringType(), True),\
                                                                    StructField("fastestLapSpeed",FloatType(), True),\
                                                                        StructField("statusId",IntegerType(), True)])
                                                                    

# COMMAND ----------

display(v_date_field)

# COMMAND ----------

results=spark.read.schema(results_schema)\
    .json(f"abfss://raw@kartheekuntdl.dfs.core.windows.net/{v_date_field}/results.json")

# COMMAND ----------

display(results.count())

# COMMAND ----------

results.createOrReplaceTempView("v_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceid,count(1)
# MAGIC  from v_results
# MAGIC  group by raceid
# MAGIC  order by raceid desc;

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

results_final_data= results.withColumnRenamed("resultId","result_id")\
    .withColumnRenamed("raceId","race_id")\
        .withColumnRenamed("constructorId","Constructor_id")\
            .withColumnRenamed("positionText","position_text")\
                .withColumnRenamed("positionOrder","position_order")\
                    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                        .withColumnRenamed("fastestLap","fastest_lap")\
                            .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                .drop(col("statusId"))\
                                    .withColumn("ingestion_date",current_timestamp())\
                                        .withColumn("date_field",lit(v_date_field))

# COMMAND ----------

display(results_final_data)

# COMMAND ----------

results_final_data.schema

# COMMAND ----------

result_final_data=results_final_data.withColumnRenamed(",milliSeconds","milliSeconds")

# COMMAND ----------

display(result_final_data)

# COMMAND ----------

# result_final_data.write.saveAsTable("results_processed")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverWriteMode","dynamic")

# COMMAND ----------

if(spark._jsparkSession.catalog().tableExists("results_processed")):
    result_final_data.write.mode("overWrite").insertInto("results_processed")
else:
    result_final_data.write.mode("overWrite").partitionBy("race_id").format("parquet").saveAsTable("results_processed")

# COMMAND ----------

spark.sql("""select distinct race_id
 from results_processed
""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC  from results_processed
# MAGIC  group by race_id
# MAGIC  order by race_id desc;

# COMMAND ----------

# results_final_data.write.mode("overWrite").partitionBy("race_id").parquet("abfss://processed@kartheekuntdl.dfs.core.windows.net/results")

# COMMAND ----------

spark.set.conf("spark.sql.sources.partitionOverWriteMode","dynamic")

# COMMAND ----------


    
