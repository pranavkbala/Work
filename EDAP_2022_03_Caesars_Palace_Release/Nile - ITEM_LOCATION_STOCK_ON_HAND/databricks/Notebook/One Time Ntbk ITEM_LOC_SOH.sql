-- Databricks notebook source
-- DBTITLE 1,Move L1 L2 config to bkp
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/config/soh/config_rms_Item_loc_soh_L2_Hubble.json","dbfs:/FileStore/tables/config/bkp_20220314/soh/config_rms_Item_loc_soh_L2_Hubble.json")

-- COMMAND ----------

-- DBTITLE 1,Move Transformation Json to bkp
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/config/soh/location_soh_transformtionL2Stage_Hubble.json","dbfs:/FileStore/tables/config/bkp_20220314/soh/location_soh_transformtionL2Stage_Hubble.json")

-- COMMAND ----------

-- DBTITLE 1,Move L2 Merge sql to bkp location
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/soh/item_location_soh_queries_Hubble.sql","dbfs:/FileStore/tables/sql/bkp_20220314/soh/item_location_soh_queries_Hubble.sql")

-- COMMAND ----------

-- DBTITLE 1,Upload 2 configs & 1 sql file to below locations
--dbfs:/FileStore/tables/config/soh/config_rms_Item_loc_soh_L2_Hubble.json

--dbfs:/FileStore/tables/config/soh/location_soh_transformtionL2Stage_Hubble.json

--dbfs:/FileStore/tables/sql/soh/item_location_soh_queries_Hubble.sql


-- COMMAND ----------

-- DBTITLE 1,Take Bkp of Inbound Archive
-- MAGIC %sql
-- MAGIC create table L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND_INBOUND_ARCHIVE_BKP
-- MAGIC DEEP CLONE delta.`dbfs:/mnt/data/governed/l1/rms/inventory/inbound_archive/table/item_loc_soh`
-- MAGIC LOCATION 'dbfs:/mnt/data/governed/l2/analytics/bkp_20220314/inventory/item_loc_soh/inbound_archive'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from  L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND_INBOUND_ARCHIVE_BKP

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from  L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND_INBOUND_ARCHIVE_BKP

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from delta.`dbfs:/mnt/data/governed/l1/rms/inventory/inbound_archive/table/item_loc_soh`

-- COMMAND ----------

-- DBTITLE 1,Delete inbound_archive
-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/mnt/data/governed/l1/rms/inventory/inbound_archive/table/item_loc_soh/',True)

-- COMMAND ----------

-- DBTITLE 1,Drop & Recreate L2 Stage Table
-- MAGIC %sql
-- MAGIC drop table L2_STAGE.ITEM_LOCATION_STOCK_ON_HAND_STAGE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/mnt/data/governed/l2/stage/inventory/item_location_stock_on_hand_stage',True)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE `L2_STAGE`.`ITEM_LOCATION_STOCK_ON_HAND_STAGE` (
-- MAGIC   `PACK_COMPONENT_SOH_QTY` DECIMAL(12,4),
-- MAGIC   `CHANNEL_CD` DECIMAL(4,0),
-- MAGIC   `CHANNEL_TYPE_CD` STRING,
-- MAGIC   `CHANNEL_DESC` STRING,
-- MAGIC   `PACK_COMPONENT_IN_TRANSIT_QTY` DECIMAL(12,4),
-- MAGIC   `IN_TRANSIT_QTY` DECIMAL(12,4),
-- MAGIC   `CUSTOMER_RESERVE_QTY` DECIMAL(12,4),
-- MAGIC   `WSI_RESERVED_QTY` DECIMAL(12,4),
-- MAGIC   `TRANSFERRED_EXPECTED_QTY` DECIMAL(12,4),
-- MAGIC   `AVG_COST_AMT` DECIMAL(20,4),
-- MAGIC   `UNIT_COST_AMT` DECIMAL(20,4),
-- MAGIC   `STOCK_ON_HAND_QTY` DECIMAL(12,4),
-- MAGIC   `FIRST_EFFECTIVE_TS` TIMESTAMP,
-- MAGIC   `LOCATION_TYPE_CD` STRING,
-- MAGIC   `LOCATION_ID` DECIMAL(10,0),
-- MAGIC   `ITEM_ID` STRING,
-- MAGIC   `SOH_UPDATE_DATETIME` TIMESTAMP)
-- MAGIC USING DELTA
-- MAGIC LOCATION 'dbfs:/mnt/data/governed/l2/stage/inventory/item_location_stock_on_hand_stage'

-- COMMAND ----------

-- DBTITLE 1,Take Bkp of L2 Target table
-- MAGIC %sql
-- MAGIC CREATE TABLE l2_analytics_tables.ITEM_LOCATION_STOCK_ON_HAND_BKP_20220314
-- MAGIC DEEP CLONE l2_analytics_tables.ITEM_LOCATION_STOCK_ON_HAND
-- MAGIC LOCATION 'dbfs:/mnt/data/governed/l2/analytics/bkp_20220314/inventory/item_loc_soh

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from l2_analytics_tables.ITEM_LOCATION_STOCK_ON_HAND_BKP_20220314

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from l2_analytics_tables.ITEM_LOCATION_STOCK_ON_HAND

-- COMMAND ----------

-- DBTITLE 1,Clear content of L2 Target table for Fresh data
-- MAGIC %sql
-- MAGIC delete from L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND

-- COMMAND ----------

-- DBTITLE 1,Replace L2 Active & Hist Views
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW L2_ANALYTICS.ITEM_LOCATION_STOCK_ON_HAND
-- MAGIC AS 
-- MAGIC SELECT * FROM L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND
-- MAGIC WHERE LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW L2_ANALYTICS.ITEM_LOCATION_STOCK_ON_HAND_HIST
-- MAGIC AS 
-- MAGIC SELECT * FROM L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND;	

-- COMMAND ----------

-- DBTITLE 1,Snowflake Part: Drop Stage Table
DROP TABLE "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."ITEM_LOCATION_STOCK_ON_HAND_STAGE"

-- COMMAND ----------

-- DBTITLE 1,Take Bkp of PROD Table
CREATE TABLE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM_LOCATION_STOCK_ON_HAND_20220314_BCKUP"
AS SELECT * FROM "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM_LOCATION_STOCK_ON_HAND";

-- COMMAND ----------

select count(*) from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM_LOCATION_STOCK_ON_HAND_20220314_BCKUP"
select count(*) from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM_LOCATION_STOCK_ON_HAND"

-- COMMAND ----------

-- DBTITLE 1,Delete contents of PROD table
delete from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM_LOCATION_STOCK_ON_HAND"

-- COMMAND ----------

-- DBTITLE 1,Replace Prod Views
CREATE OR REPLACE VIEW "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS"."ITEM_LOCATION_STOCK_ON_HAND" AS   
SELECT * FROM "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM_LOCATION_STOCK_ON_HAND" WHERE LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000';

-- COMMAND ----------

CREATE OR REPLACE VIEW "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS"."ITEM_LOCATION_STOCK_ON_HAND_HIST" AS SELECT * FROM "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM_LOCATION_STOCK_ON_HAND";	
