-- Databricks notebook source
-- DBTITLE 1,-- JSON Files Liverpool
dbfs:/FileStore/tables/config/global_franchise/liverpool/location/store_hierarchy/franchise_liverpool_store_hierarchy_l1.json
dbfs:/FileStore/tables/config/global_franchise/liverpool/location/store_hierarchy/config_franchise_liverpool_store_hierarchy_stage_l2.json
/dbfs/FileStore/tables/config/global_franchise/liverpool/location/store_hierarchy/franchise_location_liverpool_dq_check.json    

dbfs:/FileStore/tables/config/global_franchise/liverpool/location/store_hierarchy/config_franchise_store_hierarchy_l2.json  dbfs:/FileStore/tables/config/global_franchise/liverpool/location/store_hierarchy/franchise_store_hierarchy_config_sf.json 
/dbfs/FileStore/tables/config/global_franchise/location_dq/franchise_location_dq_check.json

-- COMMAND ----------

-- DBTITLE 1,-- JSON Files Alshaya
dbfs:/FileStore/tables/config/global_franchise/alshaya/location/store_hierarchy/franchise_alshaya_store_hierarchy_l1.json
dbfs:/FileStore/tables/config/global_franchise/alshaya/location/store_hierarchy/config_franchise_alshaya_store_hierarchy_stage_l2.json
/dbfs/FileStore/tables/config/global_franchise/alshaya/location/store_hierarchy/franchise_location_alshaya_dq_check.json 

-- COMMAND ----------

-- DBTITLE 1,-- JSON Files Reliance
dbfs:/FileStore/tables/config/global_franchise/reliance/location/store_hierarchy/franchise_store_reliance_hierarchy_l1.json
dbfs:/FileStore/tables/config/global_franchise/reliance/location/store_hierarchy/config_franchise_reliance_store_hierarchy_stage_l2.json
/dbfs/FileStore/tables/config/global_franchise/reliance/location/store_hierarchy/franchise_location_reliance_dq_check.json

-- COMMAND ----------

-- DBTITLE 1,-- JSON Files Livart
dbfs:/FileStore/tables/config/global_franchise/livart/location/store_hierarchy/franchise_livart_store_hierarchy_l1.json dbfs:/FileStore/tables/config/global_franchise/livart/location/store_hierarchy/config_franchise_livart_store_hierarchy_stage_l2.json
/dbfs/FileStore/tables/config/global_franchise/livart/location/store_hierarchy/franchise_location_livart_dq_check.json


-- COMMAND ----------

-- DBTITLE 1,-- SQL Files
/dbfs/FileStore/tables/sql/global_franchise/liverpool/location/store_hierarchy/franchise_store_hierarchy_merge_into_sf.sql
/dbfs/FileStore/tables/sql/global_franchise/liverpool/location/store_hierarchy/franchise_store_hierarchy_merge_into_ndp.sql


-- COMMAND ----------

-- DBTITLE 1,-- Views for NDP table
CREATE OR REPLACE VIEW L2_ANALYTICS.FRANCHISE_STORE_HIERARCHY AS SELECT * FROM L2_ANALYTICS_TABLES.FRANCHISE_STORE_HIERARCHY WHERE LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000+0000'

-- COMMAND ----------

-- DBTITLE 1,-- SnowFlake Views
CREATE OR REPLACE VIEW "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."LOCATION" AS SELECT * FROM "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."LOCATION" WHERE LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000+0000'
