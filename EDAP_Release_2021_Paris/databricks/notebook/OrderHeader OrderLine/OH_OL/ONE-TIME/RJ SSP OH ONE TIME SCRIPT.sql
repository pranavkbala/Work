-- Databricks notebook source
CREATE TABLE L2_STAGE.CUSTOMER_ORDER_STG_BKP_20210810
USING DELTA
LOCATION 'dbfs:/mnt/data/governed/l2/backup/20210810/customer_order_stg_bkp'
AS
SELECT * FROM L2_STAGE.CUSTOMER_ORDER_STG
;

-- COMMAND ----------

CREATE TABLE L2_STAGE.RJ_ORDER_HEADER_STG_BKP_20210810
USING DELTA
LOCATION 'dbfs:/mnt/data/governed/l2/backup/20210810/rj_order_header_stg_bkp'
AS
SELECT * FROM L2_STAGE.RJ_ORDER_HEADER_STG
;

-- COMMAND ----------

SELECT COUNT(*) FROM L2_STAGE.RJ_ORDER_HEADER_STG_BKP_20210810;

-- COMMAND ----------

SELECT COUNT(*) FROM L2_STAGE.RJ_ORDER_HEADER_STG

-- COMMAND ----------

DROP TABLE L2_STAGE.RJ_ORDER_HEADER_STG

-- COMMAND ----------

CREATE TABLE L2_STAGE.ORDER_RTL_STG_BKP_20210810
USING DELTA
LOCATION 'dbfs:/mnt/data/governed/l2/backup/20210810/order_rtl_stg_bkp'
AS
SELECT * FROM L2_STAGE.ORDER_RTL_STG
;

-- COMMAND ----------

SELECT COUNT(*) FROM L2_STAGE.ORDER_RTL_STG_BKP_20210810;

-- COMMAND ----------

SELECT COUNT(*) FROM L2_STAGE.ORDER_RTL_STG

-- COMMAND ----------

DROP TABLE L2_STAGE.ORDER_RTL_STG;

-- COMMAND ----------

--Alter TABLE L2_STAGE.ORDER_RTL_STG ADD COLUMNS (MEMBERSHIP_LEVEL_CD String AFTER CUSTOMER_TYPE_CD);



-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/rj_order_header_stg/", true)
-- MAGIC dbutils.fs.mkdirs("dbfs:/mnt/data/governed/l2/stage/order/rj_order_header_stg/")
-- MAGIC spark.sql("drop table IF EXISTS L2_STAGE.RJ_ORDER_HEADER_STG")

-- COMMAND ----------

CREATE TABLE `L2_STAGE`.`RJ_ORDER_HEADER_STG`
 (
	`ORDER_HEADER_KEY` INT,	
	`ORDER_HEADER_ID` STRING,
	`SOURCE_SYSTEM` STRING, 
	`ORDER_ID` STRING, 
	`ORIG_ORDER_ID` STRING,
	`EXCHANGE_ORDER_ID` STRING,  /* Added as part SEPIK changes */
	`LOCATION_KEY` INT, 
	`LOCATION_ID` STRING,
	`HOUSEHOLD_KEY` INT,   /* Added as part SEPIK changes */
    `HOUSEHOLD_ID` STRING, /* Added as part SEPIK changes */
	`DTC_ENTRY_TYPE_CD` STRING,
	`TXN_TYPE_CD` STRING,
	`PAYMENT_STATUS_CD` STRING,  /* Added as part SEPIK changes */
	`CONCEPT_CD` STRING,
	`CURRENCY_CD` STRING,
	`LINKED_ORDER_HEADER_KEY` INT,
	`LINKED_ORDER_HEADER_ID` STRING,
	`POS_WORKSTATION_ID` STRING,
	`POS_WORKSTATION_SEQ_NBR` INT,
	`RTC_LOCATION_KEY` INT,
	`RTC_LOCATION_ID` STRING,
	`ORDER_DT` DATE,
	`LOYALTY_ACCOUNT_KEY` INT,
	`LOYALTY_ACCOUNT_ID` STRING,
	`CUSTOMER_KEY` BIGINT, 
	`CUSTOMER_ID` STRING,
	`EMPLOYEE_ID` STRING,             /* Added as part SEPIK changes */
	`MATCH_METHOD_CD` STRING,
	`FIRST_EFFECTIVE_TS` TIMESTAMP,
	`LAST_EFFECTIVE_TS` TIMESTAMP,
	`BILL_TO_FIRST_NAME` STRING,
	`BILL_TO_MIDDLE_NAME` STRING,
	`BILL_TO_LAST_NAME` STRING,
	`BILL_TO_ADDRESS_LINE_1` STRING,
	`BILL_TO_ADDRESS_LINE_2` STRING,
	`BILL_TO_CITY` STRING,
	`BILL_TO_STATE_OR_PROV_CD` STRING,
	`BILL_TO_POSTAL_CD` STRING,
	`BILL_TO_COUNTRY` STRING,
	`BILL_TO_EMAIL_ADDRESS` STRING,
	`BILL_TO_PHONE_NBR` STRING, 
	`TRADE_ID` STRING,
	`ORDER_TYPE_CD` STRING,
	`CUSTOMER_TYPE_CD` STRING,
	`MEMBERSHIP_LEVEL_CD` STRING,
    `SUBORDERS_CNT` INT,   /* SUTLEJ CHANGE */
	`MARKET_CD` STRING,    /* Added as part SEPIK changes */
	`STORE_ORDER_SOURCE` STRING,
	`OPERATOR_ID` STRING,
	`CANCEL_FLAG` STRING,
	`TRAINING_MODE_FLAG` STRING,
	`REGISTRY_ORDER_FLAG` CHAR(1),        /* Added as part SEPIK changes */
    `DRAFT_ORDER_FLAG` CHAR(1),           /* Added as part SEPIK changes */
    `ORDER_PURPOSE` STRING,              /* Added as part SEPIK changes */
    `SOURCE_CODE_DISCOUNT_AMT` DECIMAL(15,2),      /* SUTLEJ CHANGE */										   
    `GIFTWRAP_WAIVED_FLAG` STRING,              /* SUTLEJ CHANGE */		   
    `SHIPPING_WAIVED_FLAG` STRING,              /* SUTLEJ CHANGE */										   
    `CATALOG_NM` STRING,                                  /* SUTLEJ CHANGE */							  
    `CATALOG_YEAR` STRING,                             /* SUTLEJ CHANGE */							   
    `REGISTRY_ID` STRING,                                /* SUTLEJ CHANGE */							  
    `ORDER_SOURCE_TYPE_CD` STRING,               /* SUTLEJ CHANGE */								   
    `GIFT_FLAG` STRING,                                    /* SUTLEJ CHANGE */							  
    `WAREHOUSE_SITE_CD` STRING,                    /* SUTLEJ CHANGE */								   
    `PAPER_FLAG` STRING,                                  /* SUTLEJ CHANGE */							  
    `STORE_ASSOCIATE_NM` STRING,                  /* SUTLEJ CHANGE */								   
    `TENTATIVE_REFUND_AMT` STRING,              /* SUTLEJ CHANGE */									   
    `CONTACT_FLAG` STRING,                              /* SUTLEJ CHANGE */								
    `RETURN_CARRIER` STRING,                          /* SUTLEJ CHANGE */							   
    `REGISTRY_TYPE_CD` STRING,                      /* SUTLEJ CHANGE */
	`TXN_BEGIN_TS` TIMESTAMP,
	`TXN_END_TS` TIMESTAMP,
	`RETURN_TYPE_CD` STRING,  /* Added as part SEPIK changes */
    `RETURN_DELIVERY_HUB` STRING,    /* Added as part SEPIK changes */
    `RETURN_MANAGING_HUB` STRING,  /* Added as part SEPIK changes */
    `RETURN_CARRIER_CD` STRING,  /* Added as part SEPIK changes */
    `RETURN_METHOD_CD` STRING,   /* Added as part SEPIK changes */
	`RECEIPT_PREFERENCE` STRING,
	`GROSS_AMT` DECIMAL(15,2),
	`NET_AMT` DECIMAL(15,2),
	`TAX_AMT` DECIMAL(15,2),
	`DOCUMENT_TYPE_CD` STRING,  /* Added as part SEPIK changes */
	`REFUND_POLICY` STRING,    /* Added as part SEPIK changes */
	`INSERT_TS` TIMESTAMP, 
	`UPDATE_TS` TIMESTAMP
)
USING delta
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/stage/order/rj_order_header_stg'
)
PARTITIONED BY (ORDER_DT, SOURCE_SYSTEM)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/retail_header/", true)
-- MAGIC dbutils.fs.mkdirs("dbfs:/mnt/data/governed/l2/stage/order/retail_header/")
-- MAGIC spark.sql("drop table IF EXISTS L2_STAGE.ORDER_RTL_HEADER_STG")

-- COMMAND ----------

CREATE TABLE `L2_STAGE`.`ORDER_RTL_HEADER_STG` (
  `ORDER_HEADER_KEY` INT, 
  `ORDER_HEADER_ID` STRING, 
  `SOURCE_SYSTEM` STRING, 
  `ORDER_ID` STRING, 
  `ORIG_ORDER_ID` STRING,
  `EXCHANGE_ORDER_ID` STRING,       /* Added as part SEPIK changes */
  `LOCATION_KEY` STRING, 
  `LOCATION_ID` STRING,
  `HOUSEHOLD_KEY` INT,              /* Added as part SEPIK changes */
  `HOUSEHOLD_ID` STRING,               /* Added as part SEPIK changes */
  `CONCEPT_CD` STRING, 
  `DTC_ENTRY_TYPE_CD` STRING, 
  `TXN_TYPE_CD` STRING,
  `PAYMENT_STATUS_CD` STRING,           /* Added as part SEPIK changes */
  `CURRENCY_CD` STRING, 
  `LINKED_ORDER_HEADER_ID` STRING, 
  `LINKED_ORDER_HEADER_KEY` INT, 
  `POS_WORKSTATION_ID` STRING, 
  `POS_WORKSTATION_SEQ_NBR` INT, 
  `RTC_LOCATION_KEY` INT, 
  `RTC_LOCATION_ID` STRING, 
  `ORDER_DT` DATE, 
  `LINKED_ORDER_DT` DATE, 
  `LOYALTY_ACCOUNT_KEY` INT, 
  `LOYALTY_ACCOUNT_ID` STRING, 
  `BILL_TO_EMAIL_ADDRESS` STRING, 
  `CUSTOMER_KEY` BIGINT, 
  `CUSTOMER_ID` STRING,
  `EMPLOYEE_ID` STRING,             /* Added as part SEPIK changes */
  `MATCH_METHOD_CD` STRING, 
  `FIRST_EFFECTIVE_TS` TIMESTAMP, 
  `LAST_EFFECTIVE_TS` TIMESTAMP, 
  `BILL_TO_FIRST_NAME` STRING, 
  `BILL_TO_MIDDLE_NAME` STRING, 
  `BILL_TO_LAST_NAME` STRING, 
  `BILL_TO_ADDRESS_LINE_1` STRING, 
  `BILL_TO_ADDRESS_LINE_2` STRING, 
  `BILL_TO_CITY` STRING, 
  `BILL_TO_STATE_OR_PROV_CD` STRING, 
  `BILL_TO_POSTAL_CD` STRING, 
  `BILL_TO_COUNTRY` STRING, 
  `BILL_TO_PHONE_NBR` STRING, 
  `TRADE_ID` STRING, 
  `ORDER_TYPE_CD` STRING, 
  `CUSTOMER_TYPE_CD` STRING, 
  `MEMBERSHIP_LEVEL_CD` STRING,
  `SUBORDERS_CNT` INT,   /* SUTLEJ CHANGE */
  `MARKET_CD` STRING,    /* Added as part SEPIK changes */
  `STORE_ORDER_SOURCE` STRING, 
  `OPERATOR_ID` STRING, 
  `CANCEL_FLAG` STRING, 
  `TRAINING_MODE_FLAG` STRING,
  `REGISTRY_ORDER_FLAG` CHAR(1),        /* Added as part SEPIK changes */
  `DRAFT_ORDER_FLAG` CHAR(1),           /* Added as part SEPIK changes */
  `ORDER_PURPOSE` STRING,              /* Added as part SEPIK changes */
  `SOURCE_CODE_DISCOUNT_AMT` DECIMAL(15,2),      /* SUTLEJ CHANGE */										   
  `GIFTWRAP_WAIVED_FLAG` STRING,              /* SUTLEJ CHANGE */		   
  `SHIPPING_WAIVED_FLAG` STRING,              /* SUTLEJ CHANGE */										   
  `CATALOG_NM` STRING,                                  /* SUTLEJ CHANGE */							  
  `CATALOG_YEAR` STRING,                             /* SUTLEJ CHANGE */							   
  `REGISTRY_ID` STRING,                                /* SUTLEJ CHANGE */							  
  `ORDER_SOURCE_TYPE_CD` STRING,               /* SUTLEJ CHANGE */								   
  `GIFT_FLAG` STRING,                                    /* SUTLEJ CHANGE */							  
  `WAREHOUSE_SITE_CD` STRING,                    /* SUTLEJ CHANGE */								   
  `PAPER_FLAG` STRING,                                  /* SUTLEJ CHANGE */							  
  `STORE_ASSOCIATE_NM` STRING,                  /* SUTLEJ CHANGE */								   
  `TENTATIVE_REFUND_AMT` STRING,              /* SUTLEJ CHANGE */									   
  `CONTACT_FLAG` STRING,                              /* SUTLEJ CHANGE */								
  `RETURN_CARRIER` STRING,                          /* SUTLEJ CHANGE */							   
  `REGISTRY_TYPE_CD` STRING,                      /* SUTLEJ CHANGE */
  `TXN_BEGIN_TS` TIMESTAMP, 
  `TXN_END_TS` TIMESTAMP,
  `RETURN_TYPE_CD` STRING,  /* Added as part SEPIK changes */
  `RETURN_DELIVERY_HUB` STRING,    /* Added as part SEPIK changes */
  `RETURN_MANAGING_HUB` STRING,  /* Added as part SEPIK changes */
  `RETURN_CARRIER_CD` STRING,  /* Added as part SEPIK changes */
  `RETURN_METHOD_CD` STRING,   /* Added as part SEPIK changes */
  `RECEIPT_PREFERENCE` STRING, 
  `GROSS_AMT` DECIMAL(15,2), 
  `NET_AMT` DECIMAL(15,2), 
  `TAX_AMT` DECIMAL(15,2),
  `DOCUMENT_TYPE_CD` STRING,  /* Added as part SEPIK changes */
  `REFUND_POLICY` STRING,    /* Added as part SEPIK changes */
  `INSERT_TS` TIMESTAMP, 
  `UPDATE_TS` TIMESTAMP)
USING DELTA
OPTIONS (
  path 'dbfs:/mnt/data/governed/l2/stage/order/retail_header'
)


-- COMMAND ----------

-- DBTITLE 1,Create and populate SSP ORDER_RTL_STG with backup values
create table L2_STAGE.ORDER_RTL_STG 
using delta
LOCATION "dbfs:/mnt/data/governed/l2/stage/order/retail"  
AS select * from L2_STAGE.ORDER_RTL_STG_BKP_20210810
