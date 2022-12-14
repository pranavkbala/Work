-- Databricks notebook source
-- DBTITLE 1,Artifacts & Paths
--/Users/msurnam@wsgc.com/2020/order/bosts/BOSTS_ETL_NOTEBOOK

-- COMMAND ----------

-- DBTITLE 1,MOVE SQL & Backup(change sql script. Wrong here)
--python
--dbutils.fs.mv("dbfs:/FileStore/tables/sql/order/dtc/bosts/insert_open_orders_agg_snoflk.sql","dbfs:/FileStore/tables/sql/order/dtc/bosts/insert_open_orders_agg_snoflk_eda2201_20220214bkp.sql")

-- COMMAND ----------

-- DBTITLE 1,Run in Snowflake
drop table "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."OPEN_ORDERS_AGG_STAGE"

-- COMMAND ----------

-- DBTITLE 1,PROD Snoflk Bkp
CREATE TABLE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."OPEN_ORDERS_AGG_20220228" CLONE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."OPEN_ORDERS_AGG";

select count(*) from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."OPEN_ORDERS_AGG_20220228"

select count(*) from "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."OPEN_ORDERS_AGG"

-- COMMAND ----------

-- DBTITLE 1,Drop  PROD Snoflk table
DROP TABLE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."OPEN_ORDERS_AGG"

-- COMMAND ----------

-- DBTITLE 1,Recreate PROD Snoflk Table
create or replace TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.OPEN_ORDERS_AGG (
ORDER_HEADER_KEY	NUMBER(38,0) NOT NULL,
ORDER_ID	VARCHAR(16777216) NOT NULL,
ORDER_LINE_KEY	NUMBER(38,0) NOT NULL,
ORDER_LINE_ID	VARCHAR(16777216) NOT NULL,
LOCATION_KEY	NUMBER(38,0),
LOCATION_ID	VARCHAR(16777216),
LOCATION_DESC	VARCHAR(16777216),
DISTRICT_KEY	NUMBER(38,0),
DISTRICT_ID	VARCHAR(16777216),
DISTRICT_DESC	VARCHAR(16777216),
SHIP_NODE_KEY	NUMBER(38,0),
SHIP_NODE_ID	VARCHAR(16777216),
SHIP_NODE_DESC	VARCHAR(16777216),
ITEM_KEY	NUMBER(38,0),
ITEM_ID	VARCHAR(16777216),
ITEM_NAME	VARCHAR(16777216),
ITEM_DEPARTMENT_KEY	NUMBER(38,0),
ITEM_DEPARTMENT_ID	VARCHAR(16777216),
ITEM_DEPARTMENT_DESC	VARCHAR(16777216),
ITEM_CLASS_KEY	NUMBER(38,0),
ITEM_CLASS_ID	VARCHAR(16777216),
ITEM_CLASS_DESC	VARCHAR(16777216),
SCAC_KEY	NUMBER(38,0),
SCAC_ID	VARCHAR(16777216),
CONCEPT_CD	VARCHAR(16777216),
ORDER_DT	DATE,
ORDER_QTY	NUMBER(38,0),
ECDD_DELIVERY_END_DT	TIMESTAMP_NTZ(9),
CURRENT_PROMISE_DT	TIMESTAMP_NTZ(9),
ORDER_SHIPPED_DT	DATE,
STORE_RECEIVED_DT	TIMESTAMP_NTZ(9),
ORDER_AGE_DAYS_NBR	NUMBER(38,0),
TOTAL_DAYS_IN_STOCKROOM_CNT	NUMBER(38,0),
STORE_ORDER_SOURCE	VARCHAR(16777216),
PRODUCT_LINE	VARCHAR(16777216),
ADDITIONAL_LINE_TYPE_CD	VARCHAR(16777216),
DISPOSITION_CD	VARCHAR(16777216),
RECEIVING_NODE_CD	VARCHAR(16777216),
CHARGE_NAME	VARCHAR(16777216),
CHARGE_PER_LINE_AMT	NUMBER(15,4),
LINE_TOTAL_AMT	NUMBER(15,4),
UNIT_LENGTH_NBR	NUMBER(14,4),
UNIT_LENGTH_UOM_CD	VARCHAR(16777216),
UNIT_WIDTH_NBR	NUMBER(14,4),
UNIT_WIDTH_UOM_CD	VARCHAR(16777216),
UNIT_HEIGHT_NBR	NUMBER(14,4),
UNIT_HEIGHT_UOM_CD	VARCHAR(16777216),
UNIT_WEIGHT_NBR	NUMBER(14,4),
UNIT_WEIGHT_UOM_CD	VARCHAR(16777216),
CARRIER_CD	VARCHAR(16777216),
STATUS_ID	VARCHAR(16777216),
STATUS_DESCRIPTION	VARCHAR(16777216),
STATUS_TS	TIMESTAMP_NTZ(9),
ORDER_DROP_TS	TIMESTAMP_NTZ(9),
CUSTOMER_PICKUP_TS	TIMESTAMP_NTZ(9),
CUSTOMER_TIME_TO_PICKUP	NUMBER(38,0),
STATUS_QTY	NUMBER(14,4),
BO_STATUS_QTY	NUMBER(15,4),
RETURN_ACTION_CD	VARCHAR(16777216),
RETURN_ACTION	VARCHAR(16777216),
RETURN_REASON_CD	VARCHAR(16777216),
RETURN_REASON_DESC	VARCHAR(16777216),
RETURN_SUB_REASON_CD	VARCHAR(16777216),
BILL_TO_FIRST_NAME	VARCHAR(16777216),
BILL_TO_LAST_NAME	VARCHAR(16777216),
BILL_TO_PHONE_NBR	VARCHAR(16777216),
BILL_TO_EMAIL_ADDRESS	VARCHAR(16777216),
INSERT_TS	TIMESTAMP_NTZ(9)  NOT NULL,
UPDATE_TS	TIMESTAMP_NTZ(9) NOT NULL
);



-- COMMAND ----------

-- DBTITLE 1,Create Temp view in Snoflk
  
  create or replace view "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS"."OPEN_ORDERS_AGG"(
    ORDER_HEADER_KEY,
    ORDER_ID,
    ORDER_LINE_KEY,
    ORDER_LINE_ID,
    LOCATION_KEY,
    LOCATION_ID,
    LOCATION_DESC,
    DISTRICT_KEY,
    DISTRICT_ID,
    DISTRICT_DESC,
    SHIP_NODE_KEY,
    SHIP_NODE_ID,
    SHIP_NODE_DESC,
    ITEM_KEY,
    ITEM_ID,
    ITEM_NAME,
    ITEM_DEPARTMENT_KEY,
    ITEM_DEPARTMENT_ID,
    ITEM_DEPARTMENT_DESC,
    ITEM_CLASS_KEY,
    ITEM_CLASS_ID,
    ITEM_CLASS_DESC,
    SCAC_KEY,
    SCAC_ID,
    CONCEPT_CD,
    ORDER_DT,
    ORDER_QTY,
    ECDD_DELIVERY_END_DT,
    CURRENT_PROMISE_DT,
    ORDER_SHIPPED_DT,
    STORE_RECEIVED_DT,
    ORDER_AGE_DAYS_NBR,
    TOTAL_DAYS_IN_STOCKROOM_CNT,
    STORE_ORDER_SOURCE,
    PRODUCT_LINE,
    ADDITIONAL_LINE_TYPE_CD ,      
    DISPOSITION_CD ,               
    RECEIVING_NODE_CD ,            
    CHARGE_NAME ,                 
    CHARGE_PER_LINE_AMT,          
    LINE_TOTAL_AMT,    
    UNIT_LENGTH_NBR,
    UNIT_LENGTH_UOM_CD,
    UNIT_WIDTH_NBR,
    UNIT_WIDTH_UOM_CD,
    UNIT_HEIGHT_NBR,
    UNIT_HEIGHT_UOM_CD,
    UNIT_WEIGHT_NBR,
    UNIT_WEIGHT_UOM_CD,
    CARRIER_CD,
    STATUS_ID,
    STATUS_DESCRIPTION,
    STATUS_TS,                     
    ORDER_DROP_TS ,                  
    CUSTOMER_PICKUP_TS ,             
    CUSTOMER_TIME_TO_PICKUP , 
    STATUS_QTY,
    BO_STATUS_QTY,
    RETURN_ACTION_CD ,                  
    RETURN_ACTION ,                     
    RETURN_REASON_CD ,                  
    RETURN_REASON_DESC ,                
    RETURN_SUB_REASON_CD , 
    BILL_TO_FIRST_NAME,
    BILL_TO_LAST_NAME,
    BILL_TO_PHONE_NBR,
    BILL_TO_EMAIL_ADDRESS,
    INSERT_TS,
    UPDATE_TS,
    MAX_INSERT_TS
) as ( SELECT * FROM "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."OPEN_ORDERS_AGG" l
INNER JOIN
(SELECT max(INSERT_TS) MAX_INSERT_TS FROM "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."OPEN_ORDERS_AGG") as r
on l.INSERT_TS = r.MAX_INSERT_TS ); 
