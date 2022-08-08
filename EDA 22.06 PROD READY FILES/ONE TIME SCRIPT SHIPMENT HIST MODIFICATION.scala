// Databricks notebook source
create table prod_edap_analytics_db.prod_edap_analytics_tables.SHIPMENT_HISTORY_LOAD_0412_C
clone prod_edap_analytics_db.prod_edap_analytics_tables.SHIPMENT_JDBC;

// COMMAND ----------


update prod_edap_analytics_db.prod_edap_analytics_tables.SHIPMENT_JDBC target
set target.UPS_ACCOUNT_NBR=src.UPS_ACCOUNT_NBR,
    target.PICKUP_DT=src.PICKUP_DT,
    target.UPDATE_TS = TO_TIMESTAMP(CURRENT_TIMESTAMP())
from DEV_EDAP_STAGE_DB.DEV_EDAP_TDM_STAGE_TABLES.SHIPMENT_HISTORY_LOAD_0412 src
where trim(target.source_shipment_id)=trim(src.source_shipment_id)
and target.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
and target.INSERT_TS < '2022-03-15 02:25:20.442';

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into l2_analytics_tables.SHIPMENT tgt 
// MAGIC using delta.`/mnt/data/teams/hinatuan/l2/stage/shipment/shipment_hist`  stg
// MAGIC on trim(tgt.SOURCE_SHIPMENT_ID)=trim(stg.SOURCE_SHIPMENT_ID)
// MAGIC and tgt.source_system = 'STERLING_DTC'
// MAGIC and tgt.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC and tgt.INSERT_TS < '2022-03-15T02:25:20.442+0000'
// MAGIC when matched then
// MAGIC update set tgt.UPS_ACCOUNT_NBR=stg.UPS_ACCOUNT_NBR,
// MAGIC            tgt.PICKUP_DT=stg.PICKUP_DT,
// MAGIC            tgt.UPDATE_TS = TO_TIMESTAMP(CURRENT_TIMESTAMP())  
