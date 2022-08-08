-- Databricks notebook source
-- DBTITLE 1,CREATING BACKUP TABLE
CREATE TABLE l2_analytics_tables.INVOICE_20220425
 DEEP CLONE l2_analytics_tables.INVOICE 
LOCATION 'dbfs:/mnt/data/governed/l2/analytics/sales/invoice_bckup20220425'

-- COMMAND ----------

-- DBTITLE 1,COUNT CHECK
select count(*) from l2_analytics_tables.INVOICE;

-- COMMAND ----------

select count(*) from l2_analytics_tables.INVOICE_20220425;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##ALTER TABLES##

-- COMMAND ----------

ALTER table L2_ANALYTICS_TABLES.INVOICE add columns(ORDER_CHARGE_TRANSACTION_KEY BIGINT after SOURCE_SYSTEM  , ORDER_CHARGE_TRANSACTION_ID STRING AFTER ORDER_CHARGE_TRANSACTION_KEY )

-- COMMAND ----------

refresh table L2_ANALYTICS_TABLES.INVOICE

-- COMMAND ----------

CREATE OR REPLACE VIEW L2_ANALYTICS.INVOICE
AS SELECT * FROM L2_ANALYTICS_TABLES.INVOICE
WHERE LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'

-- COMMAND ----------

alter table L2_STAGE.INVOICE_STAGE_ODBC add columns(ORDER_CHARGE_TRANSACTION_KEY BIGINT after SOURCE_SYSTEM  , ORDER_CHARGE_TRANSACTION_ID STRING AFTER ORDER_CHARGE_TRANSACTION_KEY )

-- COMMAND ----------

refresh table L2_ANALYTICS_TABLES.INVOICE_STAGE_ODBC

-- COMMAND ----------

alter table L2_STAGE.INVOICE_PARKING add columns(ORDER_CHARGE_TRANSACTION_KEY BIGINT after SOURCE_SYSTEM  , ORDER_CHARGE_TRANSACTION_ID STRING AFTER ORDER_CHARGE_TRANSACTION_KEY )

-- COMMAND ----------

refresh table L2_ANALYTICS_TABLES.INVOICE_PARKING

-- COMMAND ----------

-- DBTITLE 1,OPTIMIZE
OPTIMIZE l2_analytics_tables.INVOICE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##snowflake##

-- COMMAND ----------

-- DBTITLE 1,Creating clone of invoice table
CREATE TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE_20220425 CLONE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE;

-- COMMAND ----------

-- DBTITLE 1,Count check
select count(*) from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE_20220425 ;
select count(*) from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE;

-- COMMAND ----------

-- DBTITLE 1,Drop target table
DROP TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE

-- COMMAND ----------

-- DBTITLE 1,Creating target table with new fields
create TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE (
	INVOICE_KEY NUMBER(38,0) NOT NULL,
	INVOICE_ID VARCHAR(16777216) NOT NULL,
	SOURCE_SYSTEM VARCHAR(16777216) NOT NULL,
    ORDER_CHARGE_TRANSACTION_KEY NUMBER(38,0),    /* HINATUAN CHANGE */
    ORDER_CHARGE_TRANSACTION_ID VARCHAR(16777216), /* HINATUAN CHANGE */
	SOURCE_INVOICE_ID VARCHAR(16777216),
	SUB_CHANNEL_CD VARCHAR(16777216),
	CONCEPT_CD VARCHAR(16777216),
	LOCATION_KEY NUMBER(38,0) NOT NULL,
	LOCATION_ID VARCHAR(16777216) NOT NULL,
	DAY_KEY NUMBER(38,0) NOT NULL,
	INVOICE_DT DATE NOT NULL,
	INVOICE_CREATE_TS TIMESTAMP_NTZ(9),
	ORDER_HEADER_KEY NUMBER(38,0) NOT NULL,
	ORDER_HEADER_ID VARCHAR(16777216),
	ORDER_ID VARCHAR(16777216) NOT NULL,
	GLOBAL_TXN_ID VARCHAR(16777216),
	FIRST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
	LAST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
	POS_WORKSTATION_ID VARCHAR(16777216),
	POS_WORKSTATION_SEQ_NBR NUMBER(38,0),
	WSSPL_PURCHASE_ORDER_ID VARCHAR(16777216),
	NOSALE_OVERRIDE_DT DATE,
	OVERRIDE_REASON_ID VARCHAR(16777216),
	CASHIER_ID VARCHAR(16777216),
	ASSOCIATE_ID VARCHAR(16777216),
	IS_ASSOCIATE_FLAG VARCHAR(16777216),
	EMPLOYEE_ID VARCHAR(16777216),
	REPLACEMENT_ORDER_FLAG VARCHAR(16777216),
	INTERNAL_FLAG VARCHAR(16777216),
	DTC_INVOICE_TYPE_CD VARCHAR(16777216),
	DTC_INVOICE_FULFILLMENT_TYPE_CD VARCHAR(16777216),
	DTC_INVOICE_REVISED_TYPE_CD VARCHAR(16777216),
	DTC_TXN_TYPE_CD VARCHAR(16777216),
	DTC_STATUS_CD VARCHAR(16777216),
	DTC_REFERENCE_1 VARCHAR(16777216),
	DTC_COLLECTED_AMT NUMBER(15,2),
	TOTAL_QTY NUMBER(38,0),
	TOTAL_AMT NUMBER(15,2) NOT NULL,
	CREATE_USER_ID VARCHAR(16777216),
	CURRENCY_CD VARCHAR(16777216),
	INSERT_TS TIMESTAMP_NTZ(9) NOT NULL,
	UPDATE_TS TIMESTAMP_NTZ(9) NOT NULL
);

-- COMMAND ----------

-- DBTITLE 1,Inserting the data into target from clone table
INSERT INTO PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE
SELECT     
    INVOICE_KEY  ,
	INVOICE_ID  ,
	SOURCE_SYSTEM  ,
    NULL AS ORDER_CHARGE_TRANSACTION_KEY ,    /* HINATUAN CHANGE */
    NULL AS ORDER_CHARGE_TRANSACTION_ID , /* HINATUAN CHANGE */
	SOURCE_INVOICE_ID ,
	SUB_CHANNEL_CD ,
	CONCEPT_CD ,
	LOCATION_KEY  ,
	LOCATION_ID  ,
	DAY_KEY ,
	INVOICE_DT  ,
	INVOICE_CREATE_TS ,
	ORDER_HEADER_KEY  ,
	ORDER_HEADER_ID ,
	ORDER_ID  ,
	GLOBAL_TXN_ID ,
	FIRST_EFFECTIVE_TS  ,
	LAST_EFFECTIVE_TS  ,
	POS_WORKSTATION_ID ,
	POS_WORKSTATION_SEQ_NBR ,
	WSSPL_PURCHASE_ORDER_ID ,
	NOSALE_OVERRIDE_DT ,
	OVERRIDE_REASON_ID ,
	CASHIER_ID ,
	ASSOCIATE_ID ,
	IS_ASSOCIATE_FLAG ,
	EMPLOYEE_ID ,
	REPLACEMENT_ORDER_FLAG ,
	INTERNAL_FLAG ,
	DTC_INVOICE_TYPE_CD ,
	DTC_INVOICE_FULFILLMENT_TYPE_CD ,
	DTC_INVOICE_REVISED_TYPE_CD ,
	DTC_TXN_TYPE_CD ,
	DTC_STATUS_CD ,
	DTC_REFERENCE_1 ,
	DTC_COLLECTED_AMT ,
	TOTAL_QTY ,
	TOTAL_AMT  ,
	CREATE_USER_ID ,
	CURRENCY_CD ,
	INSERT_TS  ,
	UPDATE_TS  
    FROM PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE_20220425 ;

-- COMMAND ----------

-- DBTITLE 1,Refresh view
CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.INVOICE
AS SELECT * FROM PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INVOICE
WHERE LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Files to be uploaded

-- COMMAND ----------

/dbfs/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_union_parking.sql                                       /dbfs/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_column_mapping.sql                                       /dbfs/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_CDC.sql                                       /dbfs/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_surrogateKey.sql
/dbfs/FileStore/tables/sql/order/dtc/sterling_Invoice/L2_invoice_merge.sql                               /dbfs/FileStore/tables/sql/order/dtc/sterling_Invoice/L2_invoice_merge_sf.sql 
/dbfs/FileStore/tables/sql/rms/sales/INVOICE_VIEWS_HUBBLE.sql
/dbfs/FileStore/tables/sql/rms/sales/rms_merge_invoice_snowflake.sql
/dbfs/FileStore/tables/sql/global_netsuite/sales/invoice/netsuite_invoice_transformations.sql
/dbfs/FileStore/tables/sql/global_netsuite/sales/invoice/netsuite_invoice_merge_into_sf.sql

-- COMMAND ----------

Upload all the files to - dbfs:/FileStore/tables/APRIL0422/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_union_parking.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/bckup0422/Invoice_union_parking.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/Invoice_union_parking.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_union_parking.sql")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_column_mapping.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/bckup0422/Invoice_column_mapping.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/Invoice_column_mapping.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_column_mapping.sql")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_CDC.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/bckup0422/Invoice_CDC.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/Invoice_CDC.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_CDC.sql")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_surrogateKey.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/bckup0422/Invoice_surrogateKey.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/Invoice_surrogateKey.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/Invoice_surrogateKey.sql")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/L2_invoice_merge.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/bckup0422/L2_invoice_merge.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/L2_invoice_merge.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/L2_invoice_merge.sql")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/L2_invoice_merge_sf.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/bckup0422/L2_invoice_merge_sf.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/L2_invoice_merge_sf.sql","dbfs:/FileStore/tables/sql/order/dtc/sterling_Invoice/L2_invoice_merge_sf.sql")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##rms##

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/rms/sales/INVOICE_VIEWS_HUBBLE.sql","dbfs:/FileStore/tables/sql/rms/sales/bckup0422/INVOICE_VIEWS_HUBBLE.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/INVOICE_VIEWS_HUBBLE.sql","dbfs:/FileStore/tables/sql/rms/sales/INVOICE_VIEWS_HUBBLE.sql")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/rms/sales/rms_merge_invoice_snowflake.sql","dbfs:/FileStore/tables/sql/rms/sales/bckup0422/rms_merge_invoice_snowflake.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/rms_merge_invoice_snowflake.sql","dbfs:/FileStore/tables/sql/rms/sales/rms_merge_invoice_snowflake.sql")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##netsuite##

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice/netsuite_invoice_transformations.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice/bckup0422/netsuite_invoice_transformations.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/netsuite_invoice_transformations.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice/netsuite_invoice_transformations.sql")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice/netsuite_invoice_merge_into_sf.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice/bckup0422/netsuite_invoice_merge_into_sf.sql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("dbfs:/FileStore/tables/APRIL0422/netsuite_invoice_merge_into_sf.sql","dbfs:/FileStore/tables/sql/global_netsuite/sales/invoice/netsuite_invoice_merge_into_sf.sql")
