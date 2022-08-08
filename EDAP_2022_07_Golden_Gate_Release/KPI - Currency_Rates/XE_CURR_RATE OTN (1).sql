-- Databricks notebook source
-- DBTITLE 1,Backup Existing configs & SQLs

dbutils.fs.mv('dbfs:/FileStore/tables/config/currency_rates/ns_rms_currency_exchange_rate_config_l2.json','dbfs:/FileStore/tables/config/currency_rates/bkp_eda2207/ns_rms_currency_exchange_rate_config_l2.json');


dbutils.fs.mv('dbfs:/FileStore/tables/sql/currency_rates/cer_merge_into_ndp.sql','dbfs:/FileStore/tables/sql/currency_rates/bkp_eda2207/cer_merge_into_ndp.sql');

-- COMMAND ----------

-- DBTITLE 1,Insert New Configs & SQLs into the paths
--dbfs:/FileStore/tables/config/currency_rates

--dbfs:/FileStore/tables/sql/currency_rates

-- COMMAND ----------

-- DBTITLE 1,Upload the Ntbk into Prod Wsp
-- Upload Ntbk
--Verify Path of Ntbk in ADF Activity

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from delta.`dbfs:/mnt/data/teams/godavari/governed/l1/xefile_currency_exchange_rate/file/lookup_table`

-- COMMAND ----------

-- DBTITLE 1,Import Lookup Table into Prod
-- MAGIC %python
-- MAGIC dbutils.fs.cp('dbfs:/mnt/data/teams/godavari/governed/l1/xefile_currency_exchange_rate/file/lookup_table','dbfs:/mnt/data/governed/l1/xefile_currency_exchange_rate/file/lookup_table',True)

-- COMMAND ----------

select * from delta.`dbfs:/mnt/data/governed/l1/xefile_currency_exchange_rate/file/lookup_table`

-- COMMAND ----------

-- DBTITLE 1,Create Lookup table
CREATE TABLE `L2_STAGE`.`XE_CURRENCY_EXCHANGE_RATE_LOOKUP` (
  `FROM_CURRENCY_CD` STRING,
  `CURRENCY_CD` STRING)
USING delta
LOCATION 'dbfs:/mnt/data/governed/l1/xefile_currency_exchange_rate/file/lookup_table'

-- COMMAND ----------

select * from L2_STAGE.XE_CURRENCY_EXCHANGE_RATE_LOOKUP

-- COMMAND ----------

-- DBTITLE 1,Hist Data
select * from delta.`dbfs:/mnt/data/teams/godavari/governed/l2/analytics/currency_exchange_rate/xe_hist/`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('dbfs:/mnt/data/teams/godavari/governed/l2/analytics/currency_exchange_rate/xe_hist/','dbfs:/mnt/data/governed/l2/analytics/currency_exchange_rate/xe_hist/',True)

-- COMMAND ----------

select * from delta.`dbfs:/mnt/data/governed/l2/analytics/currency_exchange_rate/xe_hist/`

-- COMMAND ----------

-- DBTITLE 1,Take Backup of PROD Table (Change Path name)
CREATE TABLE l2_analytics_tables.CURRENCY_EXCHANGE_RATE_20220509
DEEP CLONE l2_analytics_tables.CURRENCY_EXCHANGE_RATE 
LOCATION 'dbfs:/mnt/data/governed/l2/analytics/location/bkp_eda2207/currency_exchange_rate'

-- COMMAND ----------

INSERT INTO L2_ANALYTICS_TABLES.CURRENCY_EXCHANGE_RATE
(SELECT
      HIST.SOURCE_SYSTEM as SOURCE_SYSTEM,
      HIST.CURRENCY_CD AS CURRENCY_CD,
      HIST.FROM_CURRENCY_CD as FROM_CURRENCY_CD,
      HIST.EXCHANGE_RATE_TYPE_CD AS EXCHANGE_RATE_TYPE_CD,
      HIST.EXCHANGE_RATE_START_DT AS EXCHANGE_RATE_START_DT,
      HIST.EXCHANGE_RATE_END_DT AS EXCHANGE_RATE_END_DT,
      HIST.EXCHANGE_RATE AS EXCHANGE_RATE,
      HIST.INSERT_TS AS INSERT_TS,
      HIST.UPDATE_TS AS UPDATE_TS  FROM delta.`dbfs:/mnt/data/governed/l2/analytics/currency_exchange_rate/xe_hist/` HIST
)


-- COMMAND ----------

select * from L2_ANALYTICS_TABLES.CURRENCY_EXCHANGE_RATE where SOURCE_SYSTEM = 'XE' order by EXCHANGE_RATE_START_DT desc LIMIT 100

-- COMMAND ----------

select count(*) from L2_ANALYTICS_TABLES.CURRENCY_EXCHANGE_RATE where SOURCE_SYSTEM = 'XE'

-- COMMAND ----------

select count(*) from delta.`dbfs:/mnt/data/governed/l2/analytics/currency_exchange_rate/xe_hist/`

-- COMMAND ----------

-- DBTITLE 1,Create Stage & Landing Table

CREATE TABLE `L2_STAGE`.`XE_CURRENCY_EXCHANGE_RATE_STAGE` (   `SOURCE_SYSTEM` STRING,   `CURRENCY_CD` STRING,   `FROM_CURRENCY_CD` STRING,   `EXCHANGE_RATE_TYPE_CD` STRING,   `EXCHANGE_RATE_START_DT` DATE,   `EXCHANGE_RATE` DECIMAL(15,8),   `INSERT_TS` TIMESTAMP,   `UPDATE_TS` TIMESTAMP) USING delta LOCATION 'dbfs:/mnt/data/governed/l2/stage/currency_exchange_rate/xe_currency_exchange_rate_stage' TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- COMMAND ----------


CREATE TABLE `L2_STAGE`.`XE_CURRENCY_EXCHANGE_RATE_LANDING` (   `CURRENCY_CD` STRING,   `FROM_CURRENCY_CD` STRING,   `EXCHANGE_RATE` DECIMAL(15,8),   `INV_EXCHANGE_RATE` DECIMAL(15,8)) USING delta LOCATION 'dbfs:/mnt/data/governed/l2/stage/currency_exchange_rate/xe_currency_exchange_rate_landing'

-- COMMAND ----------

-- DBTITLE 1,Bkp of PROD snoflk
CREATE TABLE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."CURRENCY_EXCHANGE_RATE" CLONE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."CURRENCY_EXCHANGE_RATE_EDA2207_BKP"

-- COMMAND ----------

-- DBTITLE 1,Create Directory Folder For L0 copy to land in (Ensure this is correct)
-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("dbfs:/mnt/data/governed/l0/xefile_currency_exchange_rate/file/inbound/")

-- COMMAND ----------

-- DBTITLE 1,Delete Data from Blob Landing-Zone
--delete data

-- COMMAND ----------

-- DBTITLE 1,Add ADF Json
--Add ADF Json in Prod ADF.
