-- Databricks notebook source
-- DBTITLE 1,Create backup of target table
CREATE TABLE l2_analytics_tables.INVOICE_DETAIL_20220131
 DEEP CLONE l2_analytics_tables.INVOICE_DETAIL 
LOCATION 'dbfs:/mnt/data/governed/l2/analytics/sales/invoice_detail_bckup20220131'

-- COMMAND ----------

-- DBTITLE 1,Count Checks
select count(*) from l2_analytics_tables.INVOICE_DETAIL;

-- COMMAND ----------

select count(*) from l2_analytics_tables.INVOICE_DETAIL_20220131;

-- COMMAND ----------

-- DBTITLE 1,Alter table cmd to add new field in target table
ALTER table L2_ANALYTICS_TABLES.INVOICE_DETAIL Add COLUMNS 
(GLOBAL_ESTIMATED_COST_AMT DECIMAL(15,4) AFTER UNIT_PRICE_AMT
)

-- COMMAND ----------

refresh table L2_ANALYTICS_TABLES.INVOICE_DETAIL

-- COMMAND ----------

-- DBTITLE 1,Create View
CREATE OR REPLACE VIEW L2_ANALYTICS.INVOICE_DETAIL
AS SELECT * FROM L2_ANALYTICS_TABLES.INVOICE_DETAIL
 WHERE LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
  
  

-- COMMAND ----------

OPTIMIZE l2_analytics_tables.INVOICE_DETAIL

-- COMMAND ----------

-- DBTITLE 1,In SnowFlake
CREATE TABLE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INVOICE_DETAIL_20220131" CLONE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INVOICE_DETAIL";

-- COMMAND ----------

select count(*) from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INVOICE_DETAIL_20220131";
select count(*) from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INVOICE_DETAIL";

-- COMMAND ----------

DROP TABLE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INVOICE_DETAIL"

-- COMMAND ----------

create or replace TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE_DETAIL (
    INVOICE_DETAIL_KEY NUMBER(38,0),
    INVOICE_DETAIL_ID VARCHAR(16777216),
    CONCEPT_CD VARCHAR(16777216),
    SOURCE_SYSTEM VARCHAR(16777216),
    PRIME_LINE_SEQ_NBR NUMBER(38,0),
    INVOICE_KEY NUMBER(38,0),
    INVOICE_ID VARCHAR(16777216),
    INVOICE_DT DATE,
    ITEM_KEY NUMBER(12,0),
    ITEM_ID VARCHAR(16777216),
    ORDER_LINE_KEY NUMBER(38,0),
    ORDER_LINE_ID VARCHAR(16777216),
    LOCATION_KEY NUMBER(38,0),
    LOCATION_ID VARCHAR(16777216),    
    GLOBAL_COST_CENTER_KEY NUMBER(38,0),
    GLOBAL_COST_CENTER_ID VARCHAR(16777216),
    GLOBAL_SUBSIDIARY_KEY NUMBER(38,0),
    GLOBAL_SUBSIDIARY_ID VARCHAR(16777216),
    FIRST_EFFECTIVE_TS TIMESTAMP_NTZ(9),
    LAST_EFFECTIVE_TS TIMESTAMP_NTZ(9),
    RETAIL_TYPE_CD VARCHAR(16777216),
    POS_RING_TYPE_CD VARCHAR(16777216),
    TXN_TYPE_CD VARCHAR(16777216),
    TXN_SUB_TYPE_CD VARCHAR(16777216),
    ENTRY_METHOD VARCHAR(16777216),
    SLSPRSN_ID VARCHAR(16777216),
    INVOICE_QTY NUMBER(38,0),
    INVOICE_AMT NUMBER(15,2),
    UNIT_PRICE_AMT NUMBER(19,6),
    GLOBAL_ESTIMATED_COST_AMT NUMBER(15,4),
    TXN_SIGN VARCHAR(16777216),
    TAXABLE_FLAG VARCHAR(16777216), 
    ORIG_TXN_NBR NUMBER(38,0),
    ORIG_UNIT_PRICE_AMT NUMBER(19,6),
    ORIG_TXN_SIGN_CD VARCHAR(16777216),
    REASON_ID VARCHAR(16777216),
    RETURN_REASON_ID VARCHAR(16777216),
    DROP_SHIP_FLAG VARCHAR(16777216),
    ACTUAL_SHIP_DT DATE,
    WEIGHTED_AVG_COST NUMBER(15,2),
    LABOR_COST NUMBER(15,2),
    LABOR_SKU VARCHAR(16777216),
    VENDOR_COST NUMBER(15,2),
    PO_EXPENSES NUMBER(15,2),
    PO_DUTY NUMBER(15,2),
    FULFILLMENT_STATUS_ID VARCHAR(16777216),
    DISCOUNT_RATE_AMT NUMBER(15,4), 
    ELC_AGENT_COMMISSION_AMT NUMBER(15,4),
    ELC_HTS_AMT NUMBER(15,4),
    ELC_FREIGHT_AMT NUMBER(15,4),
    ELC_MISC_AMT NUMBER(15,4),
    FIRST_COST_AMT NUMBER(15,4),
    PACK_GROUP_NBR NUMBER(38,0),
    COMMITTED_QTY NUMBER(38,0),
    CLOSED_DT DATE,
    INSERT_TS TIMESTAMP_NTZ(9),
    UPDATE_TS TIMESTAMP_NTZ(9)
); 

-- COMMAND ----------

insert into PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE_DETAIL
select 
STG.INVOICE_DETAIL_KEY,
STG.INVOICE_DETAIL_ID,
STG.CONCEPT_CD,
STG.SOURCE_SYSTEM,
STG.PRIME_LINE_SEQ_NBR,
STG.INVOICE_KEY,
STG.INVOICE_ID,
STG.INVOICE_DT,
STG.ITEM_KEY,
STG.ITEM_ID,
STG.ORDER_LINE_KEY,
STG.ORDER_LINE_ID,
STG.LOCATION_KEY,
STG.LOCATION_ID,    
STG.GLOBAL_COST_CENTER_KEY,
STG.GLOBAL_COST_CENTER_ID ,
STG.GLOBAL_SUBSIDIARY_KEY,
STG.GLOBAL_SUBSIDIARY_ID,
STG.FIRST_EFFECTIVE_TS,
STG.LAST_EFFECTIVE_TS,
STG.RETAIL_TYPE_CD,
STG.POS_RING_TYPE_CD,
STG.TXN_TYPE_CD,
STG.TXN_SUB_TYPE_CD,
STG.ENTRY_METHOD,
STG.SLSPRSN_ID,
STG.INVOICE_QTY,
STG.INVOICE_AMT,
STG.UNIT_PRICE_AMT,
null as GLOBAL_ESTIMATED_COST_AMT,
STG.TXN_SIGN,
STG.TAXABLE_FLAG,
STG.ORIG_TXN_NBR,
STG.ORIG_UNIT_PRICE_AMT,
STG.ORIG_TXN_SIGN_CD,
STG.REASON_ID,
STG.RETURN_REASON_ID,
STG.DROP_SHIP_FLAG,
STG.ACTUAL_SHIP_DT,
STG.WEIGHTED_AVG_COST,
STG.LABOR_COST,
STG.LABOR_SKU,
STG.VENDOR_COST,
STG.PO_EXPENSES,
STG.PO_DUTY,
STG.FULFILLMENT_STATUS_ID,
STG.DISCOUNT_RATE_AMT , 
STG.ELC_AGENT_COMMISSION_AMT ,
STG.ELC_HTS_AMT ,
STG.ELC_FREIGHT_AMT ,
STG.ELC_MISC_AMT,
STG.FIRST_COST_AMT,
STG.PACK_GROUP_NBR,
STG.COMMITTED_QTY,
STG.CLOSED_DT,
STG.INSERT_TS,
STG.UPDATE_TS 
from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INVOICE_DETAIL_20220131" STG 

-- COMMAND ----------

CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.INVOICE_DETAIL AS
 SELECT * FROM PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE_DETAIL WHERE LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000' ;

-- COMMAND ----------

-- DBTITLE 1,For DTC Part, remove Snowflake Stage table
DROP TABLE "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."INVOICE_DETAIL_STAGE"

-- COMMAND ----------

-- DBTITLE 1,Drop Netsuite Snowflake Stage table
drop table "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."NETSUITE_INVOICE_DETAIL_STG"

-- COMMAND ----------

-- DBTITLE 1,For RMS Part, no change, because stage tables not altered
--no activity

-- COMMAND ----------

-- DBTITLE 1,Netsuite Historical Part.. Move historical datas from teams to prod folder
-- MAGIC %python 
-- MAGIC dbutils.fs.cp("dbfs:/mnt/data/teams/godavari/governed/l1/global_netsuite/transactions/inbound/historical_data_prod_data_eda2127", "dbfs:/mnt/data/governed/l1/global_netsuite/transactions/inbound/historical_data_eda_2127", recurse = True)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.cp("dbfs:/mnt/data/teams/godavari/governed/l1/global_netsuite/transaction_lines/inbound/historical_data_prod_data_eda2127","dbfs:/mnt/data/governed/l1/global_netsuite/transaction_lines/inbound/historical_data_eda_2127", recurse = True)

-- COMMAND ----------

-- DBTITLE 1,Drop Historical Temp Stg table
drop table L2_STAGE.NETSUITE_INVOICE_DETAIL_HIST_STG_TEMP

-- COMMAND ----------

-- DBTITLE 1,Alter L2 Netsuite Stage table
ALTER table L2_STAGE.NETSUITE_INVOICE_DETAIL_STG Add COLUMNS 
(GLOBAL_ESTIMATED_COST_AMT DECIMAL(15,4) AFTER UNIT_PRICE_AMT
)

-- COMMAND ----------

-- DBTITLE 1,Delete inbound path & alter inbound_archive folder
-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/mnt/data/governed/l1/global_netsuite/sales/table/inbound/invoice_detail/',True)

-- COMMAND ----------

ALTER table delta.`dbfs:/mnt/data/governed/l1/global_netsuite/sales/table/inbound_archive/invoice_detail/` Add COLUMNS 
(ESTIMATED_COST double AFTER LOCATION_ID
)

-- COMMAND ----------

drop table L2_STAGE.NETSUITE_INVOICE_DETAIL_STG_TEMP

-- COMMAND ----------

-- DBTITLE 1,Upload files, backup existing and then move new files
Upload all the required files to the location --> dbfs:/FileStore/tables/JAN_20220124/ --> Run the Below cmds

-- COMMAND ----------

-- DBTITLE 1,NS hist l2 sf config json
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_config_l2_sf.json","dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/backup_20220124/netsuite_invoice_detail_hist_config_l2_sf.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/netsuite_invoice_detail_hist_config_l2_sf.json","dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_config_l2_sf.json")

-- COMMAND ----------

-- DBTITLE 1,NS hist config (mapping) json
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_config.json","dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/backup_20220124/netsuite_invoice_detail_hist_config.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/netsuite_invoice_detail_hist_config.json","dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_config.json")

-- COMMAND ----------

-- DBTITLE 1,NS hist Transformations SQL
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_transformations.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/backup_20220124/netsuite_invoice_detail_hist_transformations.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/netsuite_invoice_detail_hist_transformations.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_transformations.sql")

-- COMMAND ----------

-- DBTITLE 1,NS hist insert into snoflk SQL
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_insert_into_sf.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/backup_20220124/netsuite_invoice_detail_hist_insert_into_sf.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/netsuite_invoice_detail_hist_insert_into_sf.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_insert_into_sf.sql")

-- COMMAND ----------

-- DBTITLE 1,NS daily invoice detail config
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_config.json","dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/backup_20220124/netsuite_invoice_detail_config.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/netsuite_invoice_detail_config.json","dbfs:/FileStore/tables/config/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_config.json")

-- COMMAND ----------

-- DBTITLE 1,NS daily INVOICE_DETAIL transformation SQL
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_transformations.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/backup_20220124/netsuite_invoice_detail_hist_transformations.sql")
-- MAGIC 
-- MAGIC 
-- MAGIC /dbfs/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_transformations.sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/netsuite_invoice_detail_hist_transformations.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_hist_transformations.sql")

-- COMMAND ----------

-- DBTITLE 1,NS daily INVOICE_DETAIL SnoFlk Merge SQL
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_merge_into_sf.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/backup_20220124/netsuite_invoice_detail_merge_into_sf.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/netsuite_invoice_detail_merge_into_sf.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice_detail/netsuite_invoice_detail_merge_into_sf.sql")

-- COMMAND ----------

-- DBTITLE 1,DTC Notebook update
/Users/msurnam@wsgc.com/2020/sales/DTC_SS/e2e_Invoice_InvoiceDetail_InvoiceLineCharge_SS

-- COMMAND ----------

-- DBTITLE 1,DTC snoflk SQL*
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/order/dtc/invoicedetail/merge_invoice_detail_snoflk.sql","dbfs:/FileStore/tables/sql/order/dtc/invoicedetail/backup_20220124/merge_invoice_detail_snoflk.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/merge_invoice_detail_snoflk.sql","dbfs:/FileStore/tables/sql/order/dtc/invoicedetail/merge_invoice_detail_snoflk.sql")

-- COMMAND ----------

-- DBTITLE 1,RMS Transformation SQL*
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/rms/sales/INVOICE_DETAIL_VIEWS_HUBBLE.sql","dbfs:/FileStore/tables/sql/rms/sales/backup_20220124/INVOICE_DETAIL_VIEWS_HUBBLE.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/INVOICE_DETAIL_VIEWS_HUBBLE.sql","dbfs:/FileStore/tables/sql/rms/sales/INVOICE_DETAIL_VIEWS_HUBBLE.sql")

-- COMMAND ----------

-- DBTITLE 1,RMS Snoflk SQL*
-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/rms/sales/rms_merge_invoice_detail_snowflake.sql","dbfs:/FileStore/tables/sql/rms/sales/backup_20220124/rms_merge_invoice_detail_snowflake.sql")
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC dbfs:/FileStore/tables/sql/rms/sales/rms_merge_invoice_detail_snowflake.sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/JAN_20220124/rms_merge_invoice_detail_snowflake.sql","dbfs:/FileStore/tables/sql/rms/sales/rms_merge_invoice_detail_snowflake.sql")
