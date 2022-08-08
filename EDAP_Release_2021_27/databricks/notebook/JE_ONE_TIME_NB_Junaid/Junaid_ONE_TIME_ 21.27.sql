-- Databricks notebook source
-- DBTITLE 1,itemSiteAtt
-- # %python 
-- # dbutils.fs.mv("dbfs:/FileStore/tables/config/je/itemSiteAtt/itemSiteAtt_IL_transform_l1_l2.json", "/FileStore/tables/config/je/itemSiteAtt/bkp_2022_01_31/itemSiteAtt_IL_transform_l1_l2.json")

-- COMMAND ----------

-- /FileStore/tables/config/je/itemSiteAtt/itemSiteAtt_IL_transform_l1_l2.json

-- COMMAND ----------

-- DBTITLE 1,SiteAttributeCEA
/dbfs/FileStore/tables/config/je/SiteAttributeCEA/SiteAttributeCEA_FL_IL_config.json
/dbfs/FileStore/tables/config/je/SiteAttributeCEA/SiteAttributeCEA_transform_l1_l2.json
/dbfs/FileStore/tables/config/je/SiteAttributeCEA/SiteAttributeCEA_dq_check.json

/dbfs/FileStore/tables/sql/je/SiteAttributeCEA/SiteAttributeCEA_delete_data_fl.sql
/dbfs/FileStore/tables/sql/je/SiteAttributeCEA/SiteAttributeCEA_merge_into_ndp.sql
/dbfs/FileStore/tables/sql/je/SiteAttributeCEA/SiteAttributeCEA_surrogate_key_gen.sql
/dbfs/FileStore/tables/sql/je/SiteAttributeCEA/SiteAttributeCEA_IL_merge_into_sf.sql

-- COMMAND ----------

-- drop table L2_ANALYTICS_TABLES.INV_FCST_SITE_ATTR_CEA;
-- drop table L2_STAGE.INV_FCST_SITE_ATTR_CEA_STG

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # dbutils.fs.rm("dbfs:/mnt/data/governed/l2/analytics/je/SiteAttributeCEA",True)

-- COMMAND ----------

CREATE TABLE L2_ANALYTICS_TABLES.INV_FCST_SITE_ATTR_CEA (
INV_FCST_SITE_ATTR_CEA_KEY   BIGINT NOT NULL,
LOCATION_KEY    BIGINT,
LOCATION_ID    STRING,
LOCATION_TYPE_CD    STRING,
CONCEPT_CD	  STRING NOT NULL,
VIRTUAL_WAREHOUSE_KEY       BIGINT,
VIRTUAL_WAREHOUSE_ID		STRING,
VIRTUAL_WAREHOUSE_NM		STRING,
SHIP_NODE_KEY     BIGINT,
SHIP_NODE_ID		STRING,
FIRST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
LAST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
SITE_ATTR_NM	STRING,
SITE_ATTR_VAL	STRING,
INSERT_TS 	TIMESTAMP 	NOT NULL,
UPDATE_TS 	TIMESTAMP 	NOT NULL)
USING delta
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/analytics/je/SiteAttributeCEA'
)
PARTITIONED BY (UPDATE_TS)

-- COMMAND ----------

CREATE TABLE L2_STAGE.INV_FCST_SITE_ATTR_CEA_STG (
INV_FCST_SITE_ATTR_CEA_KEY   BIGINT NOT NULL,
LOCATION_KEY    BIGINT,
LOCATION_ID    STRING,
LOCATION_TYPE_CD    STRING,
CONCEPT_CD	  STRING NOT NULL,
VIRTUAL_WAREHOUSE_KEY       BIGINT,
VIRTUAL_WAREHOUSE_ID		STRING,
VIRTUAL_WAREHOUSE_NM		STRING,
SHIP_NODE_KEY     BIGINT,
SHIP_NODE_ID		STRING,
FIRST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
LAST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
SITE_ATTR_NM	STRING,
SITE_ATTR_VAL	STRING,
INSERT_TS 	TIMESTAMP 	NOT NULL,
UPDATE_TS 	TIMESTAMP 	NOT NULL)
USING delta
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/analytics/je/SiteAttributeCEA_STG'
)

-- COMMAND ----------

create or replace TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INV_FCST_SITE_ATTR_CEA (
INV_FCST_SITE_ATTR_CEA_KEY   BIGINT NOT NULL,
LOCATION_KEY    BIGINT,
LOCATION_ID    STRING,
LOCATION_TYPE_CD    STRING,
CONCEPT_CD	  STRING NOT NULL,
VIRTUAL_WAREHOUSE_KEY       BIGINT,
VIRTUAL_WAREHOUSE_ID		STRING,
VIRTUAL_WAREHOUSE_NM		STRING,
SHIP_NODE_KEY     BIGINT,
SHIP_NODE_ID		STRING,
FIRST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
LAST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
SITE_ATTR_NM	STRING,
SITE_ATTR_VAL	STRING,
INSERT_TS 	TIMESTAMP 	NOT NULL,
UPDATE_TS 	TIMESTAMP 	NOT NULL)

-- COMMAND ----------

create or replace procedure PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.PROC_INV_FCST_SITE_ATTR_CEA_PIVOT()
returns string
language javascript
execute as caller as
$$
  var cols_query = `select distinct SITE_ATTR_NM as SITE_ATTR_NM from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INV_FCST_SITE_ATTR_CEA`;
  var stmt1 = snowflake.createStatement({sqlText: cols_query});
  var results1 = stmt1.execute();
  var col_list = "";
  while(results1.next())
    col_list = col_list +"'"+ results1.getColumnValue(1)+"',";
  col_list= col_list.slice(0, -1);
  pivot_query = `CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.PROC_INV_FCST_SITE_ATTR_CEA_PIVOT_V AS (select * from  PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INV_FCST_SITE_ATTR_CEA
         pivot(max(SITE_ATTR_VAL) for SITE_ATTR_NM in (${col_list})))`
  var stmt2 = snowflake.createStatement({sqlText: pivot_query});
  stmt2.execute();
  return pivot_query;
$$

-- COMMAND ----------

select * from L2_ANALYTICS_TABLES.INV_FCST_SITE_ATTR_CEA

-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 1,INV_FCST_ITEM_SITE_ATTR_CEA


-- COMMAND ----------

/dbfs/FileStore/tables/config/je/ItemSiteAttributeCEA/ItemSiteAttributeCEA_FL_IL_config.json
/dbfs/FileStore/tables/config/je/ItemSiteAttributeCEA/ItemSiteAttributeCEA_transform_l1_l2.json
/dbfs/FileStore/tables/config/je/ItemSiteAttributeCEA/temSiteAttributeCEA_dq_check.json

/dbfs/FileStore/tables/sql/je/ItemSiteAttributeCEA/ItemSiteAttributeCEA_delete_data_fl.sql
/dbfs/FileStore/tables/sql/je/ItemSiteAttributeCEA/ItemSiteAttributeCEA_merge_into_ndp.sql
/dbfs/FileStore/tables/sql/je/ItemSiteAttributeCEA/ItemSiteAttributeCEA_surrogate_key_gen.sql
/dbfs/FileStore/tables/sql/je/ItemSiteAttributeCEA/ItemSiteAttributeCEA_IL_merge_into_sf.sql

-- COMMAND ----------

CREATE TABLE L2_ANALYTICS_TABLES.INV_FCST_ITEM_SITE_ATTR_CEA (
INV_FCST_ITEM_SITE_ATTR_CEA_KEY   BIGINT NOT NULL,
LOCATION_KEY    BIGINT,
LOCATION_ID    STRING,
LOCATION_TYPE_CD    STRING,
CONCEPT_CD	  STRING NOT NULL,
ITEM_KEY 	BIGINT,
ITEM_ID		STRING,
MARKET_CD	  STRING,
VIRTUAL_WAREHOUSE_KEY       BIGINT,
VIRTUAL_WAREHOUSE_ID		STRING,
VIRTUAL_WAREHOUSE_NM		STRING,
SHIP_NODE_KEY     BIGINT,
SHIP_NODE_ID		STRING,
FIRST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
LAST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
ITEM_SITE_ATTR_NM	STRING,
ITEM_SITE_ATTR_VAL	STRING,
INSERT_TS 	TIMESTAMP 	NOT NULL,
UPDATE_TS 	TIMESTAMP 	NOT NULL)
USING delta
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/analytics/je/ItemSiteAttributeCEA'
)
PARTITIONED BY (UPDATE_TS)

-- COMMAND ----------

CREATE TABLE L2_STAGE.INV_FCST_ITEM_SITE_ATTR_CEA_STG (
INV_FCST_ITEM_SITE_ATTR_CEA_KEY   BIGINT NOT NULL,
LOCATION_KEY    BIGINT,
LOCATION_ID    STRING,
LOCATION_TYPE_CD    STRING,
CONCEPT_CD	  STRING NOT NULL,
ITEM_KEY 	BIGINT,
ITEM_ID		STRING,
MARKET_CD	  STRING,
VIRTUAL_WAREHOUSE_KEY       BIGINT,
VIRTUAL_WAREHOUSE_ID		STRING,
VIRTUAL_WAREHOUSE_NM		STRING,
SHIP_NODE_KEY     BIGINT,
SHIP_NODE_ID		STRING,
FIRST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
LAST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
ITEM_SITE_ATTR_NM	STRING,
ITEM_SITE_ATTR_VAL	STRING,
INSERT_TS 	TIMESTAMP 	NOT NULL,
UPDATE_TS 	TIMESTAMP 	NOT NULL)
USING delta
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/analytics/je/ItemSiteAttributeCEA_STG'
)

-- COMMAND ----------

create or replace TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INV_FCST_ITEM_SITE_ATTR_CEA (
INV_FCST_ITEM_SITE_ATTR_CEA_KEY   BIGINT NOT NULL,
LOCATION_KEY    BIGINT,
LOCATION_ID    STRING,
LOCATION_TYPE_CD    STRING,
CONCEPT_CD	  STRING NOT NULL,
ITEM_KEY 	BIGINT,
ITEM_ID		STRING,
MARKET_CD	  STRING,
VIRTUAL_WAREHOUSE_KEY       BIGINT,
VIRTUAL_WAREHOUSE_ID		STRING,
VIRTUAL_WAREHOUSE_NM		STRING,
SHIP_NODE_KEY     BIGINT,
SHIP_NODE_ID		STRING,
FIRST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
LAST_EFFECTIVE_TS	TIMESTAMP	NOT NULL,
ITEM_SITE_ATTR_NM	STRING,
ITEM_SITE_ATTR_VAL	STRING,
INSERT_TS 	TIMESTAMP 	NOT NULL,
UPDATE_TS 	TIMESTAMP 	NOT NULL)

-- COMMAND ----------

create or replace procedure PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.PROC_INV_FCST_ITEM_SITE_ATTR_CEA_PIVOT()
returns string
language javascript
execute as caller as
$$
  var cols_query = `select distinct ITEM_SITE_ATTR_NM as ITEM_SITE_ATTR_NM from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INV_FCST_ITEM_SITE_ATTR_CEA`;
  var stmt1 = snowflake.createStatement({sqlText: cols_query});
  var results1 = stmt1.execute();
  var col_list = "";
  while(results1.next())
    col_list = col_list +"'"+ results1.getColumnValue(1)+"',";
  col_list= col_list.slice(0, -1);
  pivot_query = `CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.PROC_INV_FCST_ITEM_SITE_ATTR_CEA_PIVOT_V AS (select * from  PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INV_FCST_ITEM_SITE_ATTR_CEA
         pivot(max(ITEM_SITE_ATTR_VAL) for ITEM_SITE_ATTR_NM in (${col_list})))`
  var stmt2 = snowflake.createStatement({sqlText: pivot_query});
  stmt2.execute();
  return pivot_query;
$$
