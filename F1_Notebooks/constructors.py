# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_schema="constructorId Int, contructorRef String, name String, nationality string, url string"

# COMMAND ----------

constructor_df=spark.read.schema(constructors_schema)\
    .json(f'abfss://raw@kartheekuntdl.dfs.core.windows.net/{v_file_date}/constructors.json')

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

constructor_dropped_df=constructor_df.drop('url')

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_renamed_Added_column=constructor_dropped_df.withColumnRenamed("contructorId","contructor_id")\
    .withColumnRenamed("constructorRef","contsructor_ref")\
        .withColumn("ingestion_date",current_timestamp())\
            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(constructor_renamed_Added_column)

# COMMAND ----------

constructor_renamed_Added_column.write.mode("overWrite").parquet("abfss://processed@kartheekuntdl.dfs.core.windows.net/constructors")
