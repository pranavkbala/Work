-- Databricks notebook source
TRUNCATE TABLE L2_STAGE.XE_CURRENCY_EXCHANGE_RATE_LANDING

-- COMMAND ----------

-- DBTITLE 1,Insert Value from L1 into Landing Table - USD to Currencies Data inserted
INSERT INTO TABLE L2_STAGE.XE_CURRENCY_EXCHANGE_RATE_LANDING(
select 
ip.CURRENCY_CD as CURRENCY_CD,
'USD' as FROM_CURRENCY_CD,
ip.EXCHANGE_RATE as EXCHANGE_RATE,
ip.INV_EXCHANGE_RATE as INV_EXCHANGE_RATE
from parquet.`dbfs:/mnt/data/governed/l1/xefile_currency_exchange_rate/file/inbound/` ip)

-- COMMAND ----------

-- DBTITLE 1,Final Landing Table - Currencies to USD Data inserted
INSERT INTO TABLE L2_STAGE.XE_CURRENCY_EXCHANGE_RATE_LANDING(
select
'USD' as CURRENCY_CD,
ip.CURRENCY_CD as FROM_CURRENCY_CD,
ip.INV_EXCHANGE_RATE as EXCHANGE_RATE,
ip.EXCHANGE_RATE as INV_EXCHANGE_RATE
from parquet.`dbfs:/mnt/data/governed/l1/xefile_currency_exchange_rate/file/inbound/` ip)

-- COMMAND ----------

TRUNCATE TABLE L2_STAGE.XE_CURRENCY_EXCHANGE_RATE_STAGE

-- COMMAND ----------

-- DBTITLE 1,Insert Processed Data into Stage table after comparing Currencies with Lookup Table
INSERT INTO TABLE L2_STAGE.XE_CURRENCY_EXCHANGE_RATE_STAGE(
select
CAST('XE' AS STRING) as SOURCE_SYSTEM,
ip.CURRENCY_CD AS CURRENCY_CD,
ip.FROM_CURRENCY_CD AS FROM_CURRENCY_CD,
CAST('XE_ORCL' AS STRING) AS EXCHANGE_RATE_TYPE_CD,
CAST(from_utc_timestamp(current_timestamp(), 'PST') AS DATE)-1 AS EXCHANGE_RATE_START_DT,
CAST(trim(ip.EXCHANGE_RATE) AS DECIMAL(15,8)) AS EXCHANGE_RATE,
current_timestamp() AS INSERT_TS,
current_timestamp() AS UPDATE_TS
from L2_STAGE.XE_CURRENCY_EXCHANGE_RATE_LANDING ip
inner join L2_STAGE.XE_CURRENCY_EXCHANGE_RATE_LOOKUP lkp on ip.CURRENCY_CD =lkp.CURRENCY_CD and ip.FROM_CURRENCY_CD = lkp.FROM_CURRENCY_CD);
