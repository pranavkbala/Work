-- Databricks notebook source
CREATE TABLE l2_analytics_tables.ORDER_LINE_20220425 
 DEEP CLONE l2_analytics_tables.ORDER_LINE 
LOCATION 'dbfs:/mnt/data/governed/l2/analytics/order/line_20220425'

-- COMMAND ----------

SELECT COUNT(*) FROM l2_analytics_tables.ORDER_LINE_20220425
SELECT COUNT(*) FROM l2_analytics_tables.ORDER_LINE 

-- COMMAND ----------

merge into l2_analytics_tables.ORDER_LINE tgt 
using delta.`/mnt/data/teams/hinatuan/l2/analytics/order/order_line_keys_v1`  stg
on trim(tgt.order_line_id) = trim(stg.order_line_id)
and tgt.source_system = 'STERLING_DTC'
and tgt.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
and tgt.insert_ts< to_timestamp('2022-01-18 00:08:53.927')  
and( tgt.sell_item_key = -1 or tgt.sell_item_key is null)
when matched then
update  set tgt.sell_item_key=stg.sell_item_key,
            tgt.BUNDLE_ORDER_LINE_ITEM_KEY=stg.BUNDLE_ORDER_LINE_ITEM_KEY,
            tgt.update_ts= CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP)

-- COMMAND ----------

optimize l2_analytics_tables.ORDER_LINE 

-- COMMAND ----------

merge into l2_analytics_tables.ORDER_LINE tgt 
using delta.`/mnt/data/teams/hinatuan/l2/analytics/order/order_line_keys_v1`  stg
on trim(tgt.order_line_id) = trim(stg.order_line_id)
and tgt.source_system = 'STERLING_DTC'
and tgt.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
and tgt.insert_ts< to_timestamp('2022-01-18 00:08:53.927')  
and( tgt.ORIGINAL_ITEM_KEY = -1 or tgt.ORIGINAL_ITEM_KEY is null)
when matched then
update  set tgt.ORIGINAL_ITEM_KEY=stg.ORIGINAL_ITEM_KEY,
            tgt.update_ts= CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP)

-- COMMAND ----------

optimize l2_analytics_tables.ORDER_LINE 

-- COMMAND ----------

merge into l2_analytics_tables.ORDER_LINE tgt 
using delta.`/mnt/data/teams/hinatuan/l2/analytics/order/order_line_keys_v1`  stg
on trim(tgt.order_line_id) = trim(stg.order_line_id)
and tgt.source_system = 'STERLING_DTC'
and tgt.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
and tgt.insert_ts< to_timestamp('2022-01-18 00:08:53.927')  
and( tgt.MEDIA_KEY = -1 or tgt.MEDIA_KEY is null)
when matched then
update  set tgt.MEDIA_KEY=stg.MEDIA_KEY,
            tgt.update_ts= CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##snowflake##

-- COMMAND ----------

CREATE TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ORDER_LINE_20220425
CLONE 
PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ORDER_LINE;

-- COMMAND ----------

select count(*) from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ORDER_LINE_20220425;
select count(*) from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ORDER_LINE;

-- COMMAND ----------

update PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ORDER_LINE target
set target.sell_item_key=ol_keys.sell_item_key,
    target.BUNDLE_ORDER_LINE_ITEM_KEY=ol_keys.BUNDLE_ORDER_LINE_ITEM_KEY,
    target.update_ts = TO_TIMESTAMP(CURRENT_TIMESTAMP())
from DEV_EDAP_ANALYTICS_DB.DEV_EDAP_TDM_ANALYTICS_TABLES.OL_KEYS_VIEW ol_keys
where trim(target.order_line_id)=trim(ol_keys.order_line_id)
and target.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
and target.insert_ts< TO_TIMESTAMP('2022-01-18 00:08:53.927')
and target.source_system='STERLING_DTC'
and( target.sell_item_key = -1 or target.sell_item_key is null);

-- COMMAND ----------

update PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ORDER_LINE target
set target.ORIGINAL_ITEM_KEY=ol_keys.ORIGINAL_ITEM_KEY,
    target.update_ts = TO_TIMESTAMP(CURRENT_TIMESTAMP())
from DEV_EDAP_ANALYTICS_DB.DEV_EDAP_TDM_ANALYTICS_TABLES.OL_KEYS_VIEW ol_keys
where trim(target.order_line_id)=trim(ol_keys.order_line_id)
and target.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
and target.insert_ts< TO_TIMESTAMP('2022-01-18 00:08:53.927')
and target.source_system='STERLING_DTC'
and( target.ORIGINAL_ITEM_KEY = -1 or target.ORIGINAL_ITEM_KEY is null);

-- COMMAND ----------

update PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ORDER_LINE target
set target.MEDIA_KEY=ol_keys.MEDIA_KEY,
    target.update_ts = TO_TIMESTAMP(CURRENT_TIMESTAMP())
from DEV_EDAP_ANALYTICS_DB.DEV_EDAP_TDM_ANALYTICS_TABLES.OL_KEYS_VIEW ol_keys
where trim(target.order_line_id)=trim(ol_keys.order_line_id)
and target.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
and target.insert_ts< TO_TIMESTAMP('2022-01-18 00:08:53.927')
and target.source_system='STERLING_DTC'
and( target.MEDIA_KEY = -1 or target.MEDIA_KEY is null);
