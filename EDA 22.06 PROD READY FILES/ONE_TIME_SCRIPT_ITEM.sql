-- Databricks notebook source
CREATE TABLE l2_analytics_tables.ITEM_BKP_25APRIL
 DEEP CLONE l2_analytics_tables.ITEM 
LOCATION 'dbfs:/mnt/data/governed/l2/analytics/item/hist_bkp_20220425'

-- COMMAND ----------

select count(*) from l2_analytics_tables.ITEM_BKP_25APRIL;

-- COMMAND ----------

select count(*) from l2_analytics_tables.ITEM ;

-- COMMAND ----------

optimize l2_analytics_tables.ITEM; 

-- COMMAND ----------

merge into l2_analytics_tables.ITEM tgt
using delta.`/mnt/data/teams/hinatuan/l2/stage/item/main_stage_item_hist` stg
on trim(tgt.item_id) = trim(stg.item_idnt)
and tgt.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
AND (tgt.RECIPE_CARD_FLAG ='N' or tgt.RECIPE_CARD_FLAG is NULL)
AND tgt.INSERT_TS >= '2020-04-24T21:22:43.000'
when matched then
update set tgt.RECIPE_CARD_FLAG = STG.RECIPE_CARD_IND,
tgt.update_ts = CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP);

-- COMMAND ----------

-- DBTITLE 1,SNOWFLAKE target table_backup_25_4

CREATE TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ITEM_backup_25_4 CLONE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ITEM;

-- COMMAND ----------

select count(*) from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM_backup_25_4";
select count(*) from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ITEM";

-- COMMAND ----------

-- DBTITLE 1,SNOFWFLAKE Final Update

UPDATE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ITEM TGT
SET TGT.RECIPE_CARD_FLAG = 'Y' , TGT.UPDATE_TS = TO_TIMESTAMP(CURRENT_TIMESTAMP())
FROM DEV_EDAP_STAGE_DB.DEV_EDAP_TDM_STAGE_TABLES.ITEM_MAIN_STAGE_TABLE_PROD STG
WHERE TGT.ITEM_ID = STG.ITEM_IDNT
AND TGT.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000' 
AND (tgt.RECIPE_CARD_FLAG = 'N' or tgt.RECIPE_CARD_FLAG is NULL) 
AND tgt.INSERT_TS >= '2020-04-24 21:22:43.000';
