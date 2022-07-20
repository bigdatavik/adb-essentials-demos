-- Databricks notebook source
--show tables in dlt_loans_delta_vkm
show tables in delta_adb_essentials_vkm

-- COMMAND ----------

-- select * from dlt_loans_delta.loans_stats_stream_bronze
select * from delta_adb_essentials_vkm.loans_stats_stream_bronze

-- COMMAND ----------

-- select * from dlt_loans_delta.loans_stats_stream_silver
select * from delta_adb_essentials_vkm.loans_stats_stream_silver

-- COMMAND ----------

-- select * from dlt_loans_delta.loans_stats_stream_gold
select * from delta_adb_essentials_vkm.loans_stats_stream_gold

-- COMMAND ----------


