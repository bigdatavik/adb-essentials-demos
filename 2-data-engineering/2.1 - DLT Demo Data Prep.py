# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Loans Data as CSV files
# MAGIC 
# MAGIC Note: This might take a few minutes to run

# COMMAND ----------

df = spark.read.format('parquet').load("/databricks-datasets/samples/lending_club/parquet/")

# file_path = "/adbessentials/loan_stats_csv"
# file_path = "abfss://<container-name>@<account-name>.dfs.core.windows.net"
# file_path = "abfss://data@dltdemostorage.dfs.core.windows.net/adbessentials/loan_stats_csv"
#file_path = "abfss://files@vm186007.dfs.core.windows.net/adbessentials/loan_stats_csv"

file_path = "abfss://files@vm186007.dfs.core.windows.net/loan_stats_csv"

# spark.hadoop.fs.azure.account.key.vm186007.dfs.core.windows.net

# spark.databricks.enableWsfs false
# spark.hadoop.fs.azure.account.key.vm186007.dfs.core.windows.net {{secrets/access_creds_vkm/adlsDltDemoStorageAccessKey}}

# "spark.hadoop.fs.azure.account.key.dltdemo<storageaccountname>.dfs.core.windows.net": "{{secrets/access_creds/adlsDltDemoStorageAccessKey}}"


# spark.databricks.delta.preview.enabled true


df.repartition(7200).write.mode('overwrite').format('csv').option("header", "true").save(file_path)

# COMMAND ----------

dbutils.fs.ls(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ls /adbessentials/loan_stats_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test reading the Loans CSV data

# COMMAND ----------

# MAGIC %md
# MAGIC loans_df = spark.read.format('csv').option("header", "true").option("inferSchema","true").load("/adbessentials/loan_stats_csv")
# MAGIC display(loans_df)

# COMMAND ----------

loans_df = spark.read.format('csv').option("header", "true").option("inferSchema","true").load(file_path)
display(loans_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ls /adbessentials/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/vm186007/files

# COMMAND ----------

#file_path = "/mnt/vm186007/files/adbessentials/loan_stats_csv"
file_path = "/mnt/vm186007/files/loan_stats_csv"

# COMMAND ----------

dbutils.fs.ls(file_path)

# COMMAND ----------

loans_df = spark.read.format('csv').option("header", "true").option("inferSchema","true").load(file_path)
display(loans_df)

# COMMAND ----------


