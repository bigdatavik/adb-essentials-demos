-- Databricks notebook source
--SHOW TABLES IN delta_adb_essentials_dlt;
SHOW TABLES IN delta_adb_essentials_rt_vkm;

-- COMMAND ----------

--USE delta_adb_essentials_dlt;
USE delta_adb_essentials_rt_vkm;

-- COMMAND ----------

-- vik
select count(*) from loans_events_raw

-- COMMAND ----------

--SELECT * FROM lendingclub_payments_gold
SELECT * FROM 
loans_events_gold_complete

-- COMMAND ----------

--SELECT * FROM lendingclub_payments_gold_window

SELECT *
FROM loans_events_gold_window

-- COMMAND ----------

--DESCRIBE loans_gold_incr_agg_partitioned

DESCRIBE loans_events_gold_incr_agg_partitioned


-- COMMAND ----------

-- DESCRIBE DETAIL loans_gold_incr_agg_partitioned
DESCRIBE DETAIL loans_events_gold_incr_agg_partitioned

-- COMMAND ----------

-- DESCRIBE HISTORY loans_gold_incr_agg_partitioned
DESCRIBE HISTORY loans_events_gold_incr_agg_partitioned


-- COMMAND ----------

DESCRIBE HISTORY lendingclub_payments_gold_incr_agg

-- COMMAND ----------

-- SELECT *
-- FROM loans_gold_incr_agg_partitioned

SELECT *
FROM loans_events_gold_incr_agg_partitioned

-- COMMAND ----------

-- SELECT eventdate, COUNT(*)
-- FROM loans_gold_incr_agg_partitioned
-- GROUP BY eventdate
-- ORDER BY eventdate


SELECT eventdate, COUNT(*)
FROM loans_events_gold_incr_agg_partitioned
GROUP BY eventdate
ORDER BY eventdate

-- COMMAND ----------


