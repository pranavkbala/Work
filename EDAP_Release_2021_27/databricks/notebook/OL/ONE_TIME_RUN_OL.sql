-- Databricks notebook source
-- MAGIC %md #OL changes

-- COMMAND ----------

--brfore 121 
--after 131 
desc  L2_ANALYTICS_TABLES.ORDER_LINE

-- COMMAND ----------

drop table L2_STAGE.NETSUITE_OL_STG

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/line/netsuite_ol_stg",True)

-- COMMAND ----------

-- CREATE TABLE l2_analytics_tables.ORDER_LINE_20211021 
-- DEEP CLONE l2_analytics_tables.ORDER_LINE 
-- LOCATION 'dbfs:/mnt/data/governed/l2/analytics/order_line/order_line_bkp_20211021' 

-- COMMAND ----------

CREATE TABLE `L2_STAGE`.`NETSUITE_OL_STG` (
`ORDER_LINE_KEY` BIGINT,
  `ORDER_LINE_ID` STRING,
  `ORDER_HEADER_KEY` INT,
  `CHAINED_FROM_ORDER_HEADER_KEY` BIGINT,
  `CHAINED_FROM_ORDER_HEADER_ID` STRING,
  `CHAINED_FROM_ORDER_LINE_KEY` BIGINT,
  `CHAINED_FROM_ORDER_LINE_ID` STRING,
  `SOURCE_SYSTEM` STRING,
  `ORDER_DT` DATE,
  `ORDER_LINE_TYPE` STRING,
  `PRIME_LINE_SEQ_NBR` INT,
  `SUB_LINE_SEQ_NBR` INT,
  `LINKED_ORDER_HEADER_KEY` INT,
  `LINKED_ORDER_HEADER_ID` STRING,
  `LINKED_ORDER_LINE_KEY` BIGINT,
  `LINKED_ORDER_LINE_ID` STRING,
  `ITEM_KEY` INT,
  `ORDER_ITEM_ID` STRING,
  `ORDER_ITEM_TYPE_CD` STRING,
  `ORDER_ITEM_NAME` STRING,
  `SELL_ITEM_KEY` STRING,
  `SELL_ITEM_ID` STRING,
  `ORIGINAL_ITEM_KEY` BIGINT,
  `ORIGINAL_ITEM_ID` STRING,
  `BUNDLE_ORDER_LINE_ITEM_KEY` STRING,
  `BUNDLE_ORDER_LINE_ITEM_ID` STRING,
  `GIFT_REGISTRY_KEY` INT,
  `GIFT_REGISTRY_ID` STRING,
  `GLOBAL_SUBSIDIARY_KEY` BIGINT,
  `GLOBAL_SUBSIDIARY_ID` STRING,
  `GLOBAL_COST_CENTER_KEY` BIGINT,
  `GLOBAL_COST_CENTER_ID` STRING,
  `GLOBAL_COST_CENTER_DESC` STRING,
  `GLOBAL_FULFILLMENT_STATUS_KEY` BIGINT,
  `GLOBAL_FULFILLMENT_STATUS_CD` STRING,
  `IMPORT_DESC` STRING,
  `PACK_ITEM_KEY` BIGINT,
  `PACK_ITEM_ID` STRING,
  `MEDIA_KEY` STRING,
  `MEDIA_ID` STRING,
  `FIRST_EFFECTIVE_TS` TIMESTAMP,
  `LAST_EFFECTIVE_TS` TIMESTAMP,
  `UPC_CD` STRING,
  `KIT_CD` STRING,
  `KIT_QTY` INT,
  `PRODUCT_LINE` STRING,
  `BUNDLE_PARENT_ORDER_LINE_ID` STRING,
  `ORDER_QTY` INT,
  `ORIG_ORDER_QTY` INT,
  `ACT_SALE_UNIT_PRICE_AMT` DECIMAL(15,2),
  `REG_SALE_UNIT_PRICE_AMT` DECIMAL(15,2),
  `EXTENDED_AMT` DECIMAL(15,2),
  `EXTENDED_DISCOUNT_AMT` DECIMAL(15,2),
  `TAX_AMT` DECIMAL(15,2),
  `TAXABLE_AMT` DECIMAL(15,2),
  `GIFT_CARD_AMT` DECIMAL(15,2),
  `GIFTWRAP_CHARGE_AMT` DECIMAL(15,2),
  `LINE_TOTAL_AMT` DECIMAL(15,2),
  `MERCHANDISE_CHARGE_AMT` DECIMAL(15,2),
  `MONO_PZ_CHARGE_AMT` DECIMAL(15,2),
  `MISC_CHARGE_AMT` DECIMAL(15,2),
  `SHIPPING_HANDLING_CHARGE_AMT` DECIMAL(15,2),
  `SHIPPING_SURCHARGE_AMT` DECIMAL(15,2),
  `DONATION_AMT` DECIMAL(15,2),
  `LINE_MERCH_DISC_AMT` DECIMAL(15,4),
  `CNCLD_DEST_SURCHARGE_TAX_AMT` DECIMAL(15,4),
  `CNCLD_GIFT_WRAP_TAX_AMT` DECIMAL(15,4),
  `CNCLD_LINE_DEST_SURCHARGE_EFF_AMT` DECIMAL(15,4),
  `CNCLD_LINE_GIFT_WRAP_EFF_AMT` DECIMAL(15,4),
  `CNCLD_LINE_MERCH_EFF_AMT` DECIMAL(15,4),
  `CNCLD_LINE_MONO_PZ_EFF_AMT` DECIMAL(15,4),
  `CNCLD_LINE_SHIPPING_EFF_AMT` DECIMAL(15,4),
  `CNCLD_LINE_SURCHARGE_EFF_AMT` DECIMAL(15,4),
  `CNCLD_LINE_MERCH_TAX_AMT` DECIMAL(15,4),
  `CNCLD_LINE_MONO_PZ_TAX_AMT` DECIMAL(15,4),
  `CNCLD_SHIPPING_TAX_AMT` DECIMAL(15,4),
  `CNCLD_SURCHARGE_TAX_AMT` DECIMAL(15,4),
  `DEST_SURCHARGE_TAX_AMT` DECIMAL(15,4),
  `GIFT_WRAP_TAX_AMT` DECIMAL(15,4),
  `LINE_DEST_SURCHARGE_EFF_AMT` DECIMAL(15,4),
  `LINE_SURCHARGE_EFF_AMT` DECIMAL(15,4),
  `MERCH_TAX_AMT` DECIMAL(15,4),
  `MONO_PZ_TAX_AMT` DECIMAL(15,4),
  `SHIPPING_TAX_AMT` DECIMAL(15,4),
  `SURCHARGE_TAX_AMT` DECIMAL(15,4),
  `ASSOCIATE_ID` STRING,
  `ENTRY_METHOD` STRING,
  `GIFT_MESSAGE` STRING,
  `VOID_FLAG` STRING,
  `REPO_FLAG` STRING,
  `TAXABLE_FLAG` STRING,
  `PICKABLE_FLAG` STRING,
  `GIFT_FLAG` STRING,
  `FREE_SWATCH_FLAG` STRING,
  `HOLD_FLAG` STRING,
  `HOLD_REASON` STRING,
  `ORIG_BACKORDER_FLAG` STRING,
  `SUBORDER_COMPLEXITY_GROUP_ID` STRING,
  `DELIVERY_CHOICE` STRING,
  `MODIFICATION_REASON_CD` STRING,
  `MODIFICATION_REASON_CD_DESC` STRING,
  `RETURN_ACTION_CD` STRING,
  `RETURN_ACTION` STRING,
  `RETURN_REASON_CD` STRING,
  `RETURN_REASON_DESC` STRING,
  `RETURN_SUB_REASON_CD` STRING,
  `SHIP_TO_FIRST_NAME` STRING,
  `SHIP_TO_MIDDLE_NAME` STRING,
  `SHIP_TO_LAST_NAME` STRING,
  `SHIP_TO_ADDRESS_LINE_1` STRING,
  `SHIP_TO_ADDRESS_LINE_2` STRING,
  `SHIP_TO_CITY` STRING,
  `SHIP_TO_STATE_OR_PROV` STRING,
  `SHIP_TO_POSTAL_CD` STRING,
  `SHIP_TO_COUNTRY` STRING,
  `SHIP_TO_EMAIL_ADDRESS` STRING,
  `SHIP_TO_PHONE_NBR` STRING,
  `CUSTOMER_ORDER_NBR` STRING,
  `DELIVERY_METHOD` STRING,
  `FULFILLMENT_TYPE` STRING,
  `DTC_SUBORDER_NBR` INT,
  `ADDITIONAL_LINE_TYPE_CD` STRING,
  `ORIG_CONFIRMED_QTY` INT,
  `RESKU_FLAG` STRING,
  `CONSOLIDATOR_ADDRESS_CD` STRING,
  `MERGE_NODE_CD` STRING,
  `SHIP_NODE_CD` STRING,
  `RECEIVING_NODE_CD` STRING,
  `LEVEL_OF_SERVICE` STRING,
  `CARRIER_SERVICE_CD` STRING,
  `CARRIER_CD` STRING,
  `VENDOR_PACK_SIZE` STRING,
  `HTS_CD` STRING,
  `INNER_PACK_SIZE` STRING,
  `ACCESS_POINT_CD` STRING,
  `ACCESS_POINT_ID` STRING,
  `ACCESS_POINT_NM` STRING,
  `MINIMUM_SHIP_DT` DATE,
  `REQUESTED_SHIP_DT` DATE,
  `REQUESTED_DELIVERY_DT` DATE,
  `EARLIEST_SCHEDULE_DT` DATE,
  `EARLIEST_DELIVERY_DT` DATE,
  `PROMISED_APPT_END_DT` DATE,
  `GLOBAL_PO_CLOSED_DT` DATE,
  `CHANGED_DAYS_CNT` INT,
  `SPLIT_QTY` INT,
  `SHIPPED_QTY` INT,
  `FILL_QTY` INT,
  `WEIGHTED_AVG_COST` DECIMAL(15,2),
  `DIRECT_SHIP_FLAG` STRING,
  `UNIT_COST` DECIMAL(15,2),
  `LABOR_COST` DECIMAL(15,2),
  `LABOR_SKU` STRING,
  `CUSTOMER_LEVEL_OF_SERVICE` STRING,
  `RETURN_POLICY` STRING,
  `RETURN_POLICY_CHECK_OVERRIDE_FLAG` STRING,
  `RETURN_POLICY_ERROR` STRING,
  `PRODUCT_AVAILABILITY_DT` DATE,
  `ECDD_OVERRIDDEN_FLAG` STRING,
  `ECDD_INVOKED_FLAG` STRING,
  `VAS_GIFT_WRAP_CD` STRING,
  `VAS_MONO_FLAG` STRING,
  `VAS_PZ_FLAG` STRING,
  `VAS_RECIPE_FLAG` STRING,
  `VAS_CARE_CARD_FLAG` STRING,
  `BO_NOTIFICATION_NBR` INT,
  `BO_NOTIFICATION_TYPE_CD` STRING,
  `SOURCE_CREATE_TS` TIMESTAMP,
  `INSERT_TS` TIMESTAMP,
  `UPDATE_TS` TIMESTAMP)
USING delta
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/stage/order/line/netsuite_ol_stg'
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/mnt/data/governed/l1/global_netsuite/order/table/inbound_archive/line/","dbfs:/mnt/data/governed/l1/global_netsuite/order/table/inbound_archive/line_bkp_2022_02_02/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/config/global_netsuite/order/line/netsuite_transaction_lines_ol_config.json","dbfs:/FileStore/tables/config/global_netsuite/order/line/bkp_2022_02_02/netsuite_transaction_lines_ol_config.json")
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/global_netsuite/order/line/netsuite_transaction_lines_ol_transformation.sql","dbfs:/FileStore/tables/sql/global_netsuite/order/line/bkp_2022_02_02/netsuite_transaction_lines_ol_transformation.sql")
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/global_netsuite/order/line/netsuite_transaction_lines_ol_merge_into_sf.sql","dbfs:/FileStore/tables/sql/global_netsuite/order/line/bkp_2022_02_02/netsuite_transaction_lines_ol_merge_into_sf.sql")
-- MAGIC # dbutils.fs.mv("","")

-- COMMAND ----------

-- DBTITLE 1,OL_FILES - Netsuite_OL
-- config
dbfs:/FileStore/tables/config/global_netsuite/order/line/netsuite_transaction_lines_ol_config.json

-- sql
/dbfs/FileStore/tables/sql/global_netsuite/order/line/netsuite_transaction_lines_ol_transformation.sql
/dbfs/FileStore/tables/sql/global_netsuite/order/line/netsuite_transaction_lines_ol_merge_into_sf.sql

-- COMMAND ----------

-- DBTITLE 1,History load
--notebook
/Users/msurnam@wsgc.com/2020/order/header/NETSUITE_OL_HIST

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC source_trs = spark.read.parquet("dbfs:/mnt/prod-read/data/teams/godavari/governed/l1/global_netsuite/transactions/inbound/historical_data")
-- MAGIC source_trs.count()

-- COMMAND ----------

-- MAGIC %python dbutils.fs.rm("dbfs:/mnt/data/governed/l1/global_netsuite/transactions/inbound/historical_data",True)

-- COMMAND ----------

-- MAGIC %python dbutils.fs.cp("dbfs:/mnt/prod-read/data/teams/godavari/governed/l1/global_netsuite/transactions/inbound/historical_data", 
-- MAGIC               "dbfs:/mnt/data/governed/l1/global_netsuite/transactions/inbound/historical_data", recurse = True)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC target_trs = spark.read.parquet("dbfs:/mnt/data/governed/l1/global_netsuite/transactions/inbound/historical_data")
-- MAGIC target_trs.count()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC source_trs = spark.read.parquet("dbfs:/mnt/prod-read/data/teams/godavari/governed/l1/global_netsuite/transaction_lines/inbound/historical_data")
-- MAGIC source_trs.count()

-- COMMAND ----------

-- MAGIC %python dbutils.fs.rm("dbfs:/mnt/data/governed/l1/global_netsuite/transaction_lines/inbound/historical_data",True)

-- COMMAND ----------

-- MAGIC %python dbutils.fs.cp("dbfs:/mnt/prod-read/data/teams/godavari/governed/l1/global_netsuite/transaction_lines/inbound/historical_data", 
-- MAGIC               "dbfs:/mnt/data/governed/l1/global_netsuite/transaction_lines/inbound/historical_data", recurse = True)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC target_trs = spark.read.parquet("dbfs:/mnt/data/governed/l1/global_netsuite/transaction_lines/inbound/historical_data")
-- MAGIC target_trs.count()

-- COMMAND ----------


