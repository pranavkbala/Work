-- Databricks notebook source
--RJ Stage Table Backup
CREATE TABLE L2_STAGE.RJ_ORDER_LINE_STG_BKP_20210810  
USING DELTA
LOCATION 'dbfs:/mnt/data/governed/l2/backup/20210810/rj_order_line_stg_bkp'
AS
SELECT * FROM L2_STAGE.RJ_ORDER_LINE_STG
;

-- COMMAND ----------

--RJ Bkup Table counts
SELECT COUNT(*) FROM L2_STAGE.RJ_ORDER_LINE_STG_BKP_20210727;

-- COMMAND ----------

--RJ Stage Table counts
SELECT COUNT(*) FROM L2_STAGE.RJ_ORDER_LINE_STG;

-- COMMAND ----------

--RJ Stage Table - Delete
DROP TABLE L2_STAGE.RJ_ORDER_LINE_STG;

-- COMMAND ----------

--SSP Stage Table Backup
CREATE TABLE L2_STAGE.ORDER_RTL_LINE_STG_BKP_20210810
USING DELTA
LOCATION 'dbfs:/mnt/data/governed/l2/backup/20210810/order_rtl_stg_bkp'
AS
SELECT * FROM L2_STAGE.ORDER_RTL_STG
;

-- COMMAND ----------

--SSP Backup Table counts
SELECT COUNT(*) FROM L2_STAGE.ORDER_RTL_LINE_STG_BKP_20210727;

-- COMMAND ----------

--SSP Stage Table counts
SELECT COUNT(*) FROM L2_STAGE.ORDER_RTL_LINE_STG;

-- COMMAND ----------

--SSP Stage Table Delete
DROP TABLE L2_STAGE.ORDER_RTL_LINE_STG;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/rj_order_line_stg", true)
-- MAGIC dbutils.fs.mkdirs("dbfs:/mnt/data/governed/l2/stage/order/rj_order_line_stg")
-- MAGIC spark.sql("drop table IF EXISTS L2_STAGE.RJ_ORDER_LINE_STG")

-- COMMAND ----------

CREATE TABLE `L2_STAGE`.`RJ_ORDER_LINE_STG` (
`ORDER_LINE_KEY` BIGINT
,`ORDER_LINE_ID` STRING
,`CHAINED_FROM_ORDER_HEADER_KEY` BIGINT      /* Add new Column - SEPIK */
,`CHAINED_FROM_ORDER_HEADER_ID` STRING       /* Add new Column - SEPIK */
,`CHAINED_FROM_ORDER_LINE_KEY` BIGINT        /* Add new Column - SEPIK */
,`CHAINED_FROM_ORDER_LINE_ID` STRING         /* Add new Column - SEPIK */
,`SOURCE_SYSTEM` STRING
,`PRIME_LINE_SEQ_NBR` INT
,`SUB_LINE_SEQ_NBR` INT
,`ORDER_HEADER_KEY` INT
,`ORDER_DT` DATE
,`LINKED_ORDER_HEADER_KEY` INT
,`LINKED_ORDER_HEADER_ID` STRING
,`LINKED_ORDER_LINE_KEY` BIGINT
,`LINKED_ORDER_LINE_ID` STRING
,`ITEM_KEY` INT
,`ORDER_ITEM_ID` STRING
,`ORDER_ITEM_TYPE_CD` STRING
,`ORDER_ITEM_NAME` STRING
,`GIFT_REGISTRY_KEY` INT
,`GIFT_REGISTRY_ID` STRING
,`FIRST_EFFECTIVE_TS` TIMESTAMP
,`LAST_EFFECTIVE_TS` TIMESTAMP
,`UPC_CD` STRING
,`KIT_CD` STRING
,`KIT_QTY` INT
,`PRODUCT_LINE` STRING
,`BUNDLE_PARENT_ORDER_LINE_ID` STRING
,`ORDER_LINE_TYPE` STRING
,`ORDER_QTY` INT
,`ORIG_ORDER_QTY` INT
,`ACT_SALE_UNIT_PRICE_AMT` DECIMAL(15,2)
,`REG_SALE_UNIT_PRICE_AMT` DECIMAL(15,2)
,`EXTENDED_AMT` DECIMAL(15,2)
,`EXTENDED_DISCOUNT_AMT` DECIMAL(15,2)
,`TAX_AMT` DECIMAL(15,2)
,`TAXABLE_AMT` DECIMAL(15,2)
,`GIFT_CARD_AMT` DECIMAL(15,2)
,`GIFTWRAP_CHARGE_AMT` DECIMAL(15,2)
,`LINE_TOTAL_AMT` DECIMAL(15,2)
,`MERCHANDISE_CHARGE_AMT` DECIMAL(15,2)
,`MONO_PZ_CHARGE_AMT` DECIMAL(15,2)
,`MISC_CHARGE_AMT` DECIMAL(15,2)
,`SHIPPING_HANDLING_CHARGE_AMT` DECIMAL(15,2)
,`SHIPPING_SURCHARGE_AMT` DECIMAL(15,2)
,`DONATION_AMT` DECIMAL(15,2)
,`ASSOCIATE_ID` STRING
,`ENTRY_METHOD` STRING
,`GIFT_MESSAGE` STRING
,`VOID_FLAG` STRING
,`REPO_FLAG` STRING
,`TAXABLE_FLAG` STRING
,`PICKABLE_FLAG` STRING
,`GIFT_FLAG` STRING
,`HOLD_FLAG` STRING
,`HOLD_REASON` STRING
,`ORIG_BACKORDER_FLAG` STRING
,`SUBORDER_COMPLEXITY_GROUP_ID` STRING   /* SUTLEJ CHANGE */
,`DELIVERY_CHOICE` STRING
,`MODIFICATION_REASON_CD` STRING
,`MODIFICATION_REASON_CD_DESC` STRING
,`RETURN_ACTION_CD` STRING
,`RETURN_ACTION` STRING
,`RETURN_REASON_CD` STRING
,`RETURN_REASON_DESC` STRING
,`RETURN_SUB_REASON_CD` STRING
,`SHIP_TO_FIRST_NAME` STRING
,`SHIP_TO_MIDDLE_NAME` STRING
,`SHIP_TO_LAST_NAME` STRING
,`SHIP_TO_ADDRESS_LINE_1` STRING
,`SHIP_TO_ADDRESS_LINE_2` STRING
,`SHIP_TO_CITY` STRING
,`SHIP_TO_STATE_OR_PROV` STRING
,`SHIP_TO_POSTAL_CD` STRING
,`SHIP_TO_COUNTRY` STRING
,`SHIP_TO_EMAIL_ADDRESS` STRING
,`SHIP_TO_PHONE_NBR` STRING
,`DELIVERY_METHOD` STRING                    /* Add new Column - SEPIK */
,`FULFILLMENT_TYPE` STRING                   /* Add new Column - SEPIK */
,`DTC_SUBORDER_NBR` INT                      /* Add new Column - SEPIK */
,`ADDITIONAL_LINE_TYPE_CD` STRING            /* Add new Column - SEPIK */
,`ORIG_CONFIRMED_QTY` INT                    /* Add new Column - SEPIK */
,`RESKU_FLAG` STRING                         /* Add new Column - SEPIK */
,`CONSOLIDATOR_ADDRESS_CD` STRING
,`MERGE_NODE_CD` STRING
,`SHIP_NODE_CD` STRING
,`RECEIVING_NODE_CD` STRING
,`LEVEL_OF_SERVICE` STRING
,`CARRIER_SERVICE_CD` STRING
,`CARRIER_CD` STRING
,`ACCESS_POINT_CD` STRING
,`ACCESS_POINT_ID` STRING
,`ACCESS_POINT_NM` STRING
,`MINIMUM_SHIP_DT` DATE
,`REQUESTED_SHIP_DT` DATE
,`REQUESTED_DELIVERY_DT` DATE
,`EARLIEST_SCHEDULE_DT` DATE
,`EARLIEST_DELIVERY_DT` DATE
,`PROMISED_APPT_END_DT` DATE
,`SPLIT_QTY` INT
,`SHIPPED_QTY` INT
,`FILL_QTY` INT
,`WEIGHTED_AVG_COST` DECIMAL(15,2)
,`DIRECT_SHIP_FLAG` STRING
,`UNIT_COST` DECIMAL(15,2)
,`LABOR_COST` DECIMAL(15,2)
,`LABOR_SKU` STRING
,`CUSTOMER_LEVEL_OF_SERVICE` STRING
,`RETURN_POLICY` STRING	                       /* Add new Column - SEPIK */
,`RETURN_POLICY_CHECK_OVERRIDE_FLAG` STRING    /* Add new Column - SEPIK */
,`PRODUCT_AVAILABILITY_DT` DATE                /* Add new Column - SEPIK */
,`ECDD_OVERRIDDEN_FLAG` STRING                 /* Add new Column - SEPIK */
,`ECDD_INVOKED_FLAG` STRING                    /* Add new Column - SEPIK */
,`VAS_GIFT_WRAP_CD` STRING				       /* Add new Column - SEPIK */
,`VAS_MONO_FLAG` STRING                        /* Add new Column - SEPIK */
,`VAS_PZ_FLAG`	STRING					       /* Add new Column - SEPIK */
,`BO_NOTIFICATION_NBR` INT                     /* New Column-NILE-CYODD */
,`INSERT_TS` TIMESTAMP
,`UPDATE_TS` TIMESTAMP)
USING delta
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/stage/order/rj_order_line_stg'
)
PARTITIONED BY (ORDER_DT, SOURCE_SYSTEM)


-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/retail_line", true)
-- MAGIC dbutils.fs.mkdirs("dbfs:/mnt/data/governed/l2/stage/order/retail_line")
-- MAGIC spark.sql("drop table IF EXISTS L2_STAGE.ORDER_RTL_LINE_STG")

-- COMMAND ----------

CREATE TABLE `L2_STAGE`.`ORDER_RTL_LINE_STG` (
`ORDER_LINE_KEY` BIGINT
,`ORDER_LINE_ID` STRING
,`ORDER_DT` DATE
,`CHAINED_FROM_ORDER_HEADER_KEY` BIGINT      /* Add new Column - SEPIK */
,`CHAINED_FROM_ORDER_HEADER_ID` STRING       /* Add new Column - SEPIK */
,`CHAINED_FROM_ORDER_LINE_KEY` BIGINT        /* Add new Column - SEPIK */
,`CHAINED_FROM_ORDER_LINE_ID` STRING         /* Add new Column - SEPIK */
,`SOURCE_SYSTEM` STRING
,`PRIME_LINE_SEQ_NBR` INT
,`SUB_LINE_SEQ_NBR` STRING
,`ORDER_HEADER_KEY` INT
,`LINKED_ORDER_HEADER_KEY` INT
,`LINKED_ORDER_HEADER_ID` STRING
,`LINKED_ORDER_LINE_KEY` BIGINT
,`LINKED_ORDER_LINE_ID` STRING
,`ITEM_KEY` INT
,`ORDER_ITEM_ID` STRING
,`ORDER_ITEM_TYPE_CD` STRING
,`ORDER_ITEM_NAME` STRING
,`GIFT_REGISTRY_KEY` STRING
,`GIFT_REGISTRY_ID` STRING
,`FIRST_EFFECTIVE_TS` TIMESTAMP
,`LAST_EFFECTIVE_TS` TIMESTAMP
,`UPC_CD` STRING
,`KIT_CD` STRING
,`KIT_QTY` STRING
,`PRODUCT_LINE` STRING
,`BUNDLE_PARENT_ORDER_LINE_ID` STRING
,`ORDER_LINE_TYPE` STRING
,`ORDER_QTY` INT
,`ORIG_ORDER_QTY` INT
,`ACT_SALE_UNIT_PRICE_AMT` DECIMAL(15,2)
,`REG_SALE_UNIT_PRICE_AMT` DECIMAL(15,2)
,`EXTENDED_AMT` DECIMAL(15,2)
,`EXTENDED_DISCOUNT_AMT` DECIMAL(15,2)
,`TAX_AMT` DECIMAL(15,2)
,`TAXABLE_AMT` DECIMAL(15,2)
,`GIFT_CARD_AMT` DECIMAL(15,2)
,`GIFTWRAP_CHARGE_AMT` STRING
,`LINE_TOTAL_AMT` DECIMAL(15,2)
,`MERCHANDISE_CHARGE_AMT` STRING
,`MONO_PZ_CHARGE_AMT` STRING
,`MISC_CHARGE_AMT` STRING
,`SHIPPING_HANDLING_CHARGE_AMT` STRING
,`SHIPPING_SURCHARGE_AMT` STRING
,`DONATION_AMT` DECIMAL(15,2)
,`ASSOCIATE_ID` STRING
,`ENTRY_METHOD` STRING
,`GIFT_MESSAGE` STRING
,`VOID_FLAG` STRING
,`REPO_FLAG` STRING
,`TAXABLE_FLAG` STRING
,`PICKABLE_FLAG` STRING
,`GIFT_FLAG` STRING
,`HOLD_FLAG` STRING
,`HOLD_REASON` STRING
,`ORIG_BACKORDER_FLAG` STRING
,`SUBORDER_COMPLEXITY_GROUP_ID` STRING   /* SUTLEJ CHANGE */
,`DELIVERY_CHOICE` STRING
,`MODIFICATION_REASON_CD` STRING
,`MODIFICATION_REASON_CD_DESC` STRING
,`RETURN_ACTION_CD` STRING
,`RETURN_ACTION` STRING
,`RETURN_REASON_CD` STRING
,`RETURN_REASON_DESC` STRING
,`RETURN_SUB_REASON_CD` STRING
,`SHIP_TO_FIRST_NAME` STRING
,`SHIP_TO_MIDDLE_NAME` STRING
,`SHIP_TO_LAST_NAME` STRING
,`SHIP_TO_ADDRESS_LINE_1` STRING
,`SHIP_TO_ADDRESS_LINE_2` STRING
,`SHIP_TO_CITY` STRING
,`SHIP_TO_STATE_OR_PROV` STRING
,`SHIP_TO_POSTAL_CD` STRING
,`SHIP_TO_COUNTRY` STRING
,`SHIP_TO_EMAIL_ADDRESS` STRING
,`SHIP_TO_PHONE_NBR` STRING
,`DELIVERY_METHOD` STRING                    /* Add new Column - SEPIK */
,`FULFILLMENT_TYPE` STRING                   /* Add new Column - SEPIK */
,`DTC_SUBORDER_NBR` INT                      /* Add new Column - SEPIK */
,`ADDITIONAL_LINE_TYPE_CD` STRING            /* Add new Column - SEPIK */
,`ORIG_CONFIRMED_QTY` INT                    /* Add new Column - SEPIK */
,`RESKU_FLAG` STRING                         /* Add new Column - SEPIK */
,`CONSOLIDATOR_ADDRESS_CD` STRING
,`MERGE_NODE_CD` STRING
,`SHIP_NODE_CD` STRING
,`RECEIVING_NODE_CD` STRING
,`LEVEL_OF_SERVICE` STRING
,`CARRIER_SERVICE_CD` STRING
,`CARRIER_CD` STRING
,`ACCESS_POINT_CD` STRING
,`ACCESS_POINT_ID` STRING
,`ACCESS_POINT_NM` STRING
,`MINIMUM_SHIP_DT` STRING
,`REQUESTED_SHIP_DT` STRING
,`REQUESTED_DELIVERY_DT` STRING
,`EARLIEST_SCHEDULE_DT` STRING
,`EARLIEST_DELIVERY_DT` STRING
,`PROMISED_APPT_END_DT` STRING
,`SPLIT_QTY` STRING
,`SHIPPED_QTY` STRING
,`FILL_QTY` STRING
,`WEIGHTED_AVG_COST` STRING
,`DIRECT_SHIP_FLAG` STRING
,`UNIT_COST` STRING
,`LABOR_COST` STRING
,`LABOR_SKU` STRING
,`CUSTOMER_LEVEL_OF_SERVICE` STRING
,`RETURN_POLICY` STRING	                       /* Add new Column - SEPIK */
,`RETURN_POLICY_CHECK_OVERRIDE_FLAG` STRING    /* Add new Column - SEPIK */
,`PRODUCT_AVAILABILITY_DT` DATE                /* Add new Column - SEPIK */
,`ECDD_OVERRIDDEN_FLAG` STRING                 /* Add new Column - SEPIK */
,`ECDD_INVOKED_FLAG` STRING                    /* Add new Column - SEPIK */
,`VAS_GIFT_WRAP_CD` STRING				       /* Add new Column - SEPIK */
,`VAS_MONO_FLAG` STRING                        /* Add new Column - SEPIK */
,`VAS_PZ_FLAG`	STRING					       /* Add new Column - SEPIK */
,`BO_NOTIFICATION_NBR` INT
,`INSERT_TS` TIMESTAMP
,`UPDATE_TS` TIMESTAMP)
USING DELTA
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/stage/order/retail_line'
)

