# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),\
    StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema=StructType(fields=[StructField("driverId",IntegerType(),True),\
    StructField("driverRef",StringType(),True),\
        StructField("number",IntegerType(),True),\
            StructField("code",IntegerType(),True),\
                StructField("name",name_schema),\
                    StructField("dob",IntegerType(),True),\
                        StructField("nationality",StringType(),True),\
                             StructField("url",StringType(),True)])

# COMMAND ----------



# COMMAND ----------

driver_data=spark.read.json(f'abfss://raw@kartheekuntdl.dfs.core.windows.net/{v_file_date}/drivers.json')

# COMMAND ----------

driver_data_dropped=driver_data.drop("url")

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit

# COMMAND ----------

driver_data_renamed=driver_data.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_Ref")\
        .withColumn("Ingestion_date",current_timestamp())\
            .withColumn("file_date",lit(v_file_date))\
            .withColumn("name",concat(col("name.forename"),lit(' '),concat(col("name.surname"))))

# COMMAND ----------

display(driver_data_renamed)

# COMMAND ----------

driver_data_renamed.write.mode("overWrite").parquet("abfss://processed@kartheekuntdl.dfs.core.windows.net/drivers")

# COMMAND ----------


