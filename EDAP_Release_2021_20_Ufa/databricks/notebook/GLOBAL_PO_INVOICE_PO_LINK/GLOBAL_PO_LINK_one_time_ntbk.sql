-- Databricks notebook source
INSERT INTO L1_audit.checkpoint_log VALUES ('GLOBAL_PO_LINK','GLOBAL_PO_LINK',CURRENT_TIMESTAMP(),'NETSUITE.TRANSACTION_LINKS','DATE_LAST_MODIFIED','31-12-2013 23:59:59',NULL,NULL,NULL,NULL,NULL,NULL);

INSERT INTO L1_audit.checkpoint_log VALUES ('GLOBAL_PO_INVOICE_LINK','GLOBAL_PO_INVOICE_LINK',CURRENT_TIMESTAMP(),'NETSUITE.TRANSACTION_LINKS','DATE_LAST_MODIFIED','17-09-2014 20:17:25',NULL,NULL,NULL,NULL,NULL,NULL)

-- COMMAND ----------

-- DBTITLE 1,JSON files
-- GLOBAL_PO_LINK
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_invoice/global_po_link_config_l1.json'
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_invoice/global_po_link_config_l2.json'
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_invoice/global_po_link_config_l1_l2_transform.json'
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_invoice/global_po_link_config_sf.json'
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_invoice/netsuite_global_po_link_dq_check.json'

-- GLOBAL_PO_INVOICE_LINK
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_links_invoice/global_po_invoice_link_config_l1.json'
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_links_invoice/global_po_invoice_link_config_l2.json'
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_links_invoice/global_po_invoice_link_config_l2_transform.json'
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_links_invoice/global_po_invoice_link_config_sf.json'
'/dbfs/FileStore/tables/config/netsuite_administrator/transactions_links_invoice/netsuite_global_po_invoice_link_dq_check_prod.json'

-- COMMAND ----------

-- DBTITLE 1,sql files
-- GLOBAL_PO_LINK
'/dbfs/FileStore/tables/sql/netsuite_administrator/transactions_invoice/global_po_link_cdc_lookup.sql'
'/dbfs/FileStore/tables/sql/netsuite_administrator/transactions_invoice/global_po_link_surrogate_key_gen.sql'
'/dbfs/FileStore/tables/sql/netsuite_administrator/transactions_invoice/global_po_link_merge_into_edap.sql'
'/dbfs/FileStore/tables/sql/netsuite_administrator/transactions_invoice/global_po_link_merge_into_sf.sql'

-- GLOBAL_PO_INVOICE_LINK
'/dbfs/FileStore/tables/sql/netsuite_administrator/transactions_links_invoice/global_po_invoice_link_cdc_lookup.sql'
'/dbfs/FileStore/tables/sql/netsuite_administrator/transactions_links_invoice/global_po_invoice_link_surrogate_key_gen.sql'
'/dbfs/FileStore/tables/sql/netsuite_administrator/transactions_links_invoice/global_po_invoice_link_merge_into_edap.sql'
'/dbfs/FileStore/tables/sql/netsuite_administrator/transactions_links_invoice/global_po_invoice_link_merge_into_sf.sql'

-- COMMAND ----------

-- DBTITLE 1,Stage table DDL
DROP TABLE IF EXISTS `L2_STAGE`.`GLOBAL_PO_LINK_STG`;
CREATE TABLE `L2_STAGE`.`GLOBAL_PO_LINK_STG` 
(
GLOBAL_PO_LINK_KEY BIGINT NOT NULL,
ORIGINAL_PO_HEADER_KEY BIGINT,
ORIGINAL_PO_HEADER_ID STRING,
ORIGINAL_PO_LINE_KEY BIGINT,
ORIGINAL_PO_LINE_ID STRING,
APPLIED_PO_HEADER_KEY BIGINT,
APPLIED_PO_ID STRING,
APPLIED_PO_LINE_KEY BIGINT,
APPLIED_PO_LINE_ID STRING,
FIRST_EFFECTIVE_TS TIMESTAMP,
LAST_EFFECTIVE_TS TIMESTAMP,
FOREIGN_LINKED_AMT DECIMAL(15,4),
LINKED_AMT DECIMAL(15,4),
LINKED_QTY INT,
DISCOUNT_FLAG CHAR(1),
LINK_TYPE_CD STRING,
LINK_TYPE_DESC STRING,
INVENTORY_NBR INT,
ORIGINAL_POSTED_DT DATE,
APPLIED_POSTED_DT DATE,
INSERT_TS TIMESTAMP NOT NULL,
UPDATE_TS TIMESTAMP NOT NULL
)
USING delta
LOCATION "dbfs:/mnt/data/governed/l2/stage/transactions/global_po_link_stg/"
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- COMMAND ----------

CREATE TABLE L2_STAGE.GLOBAL_PO_INVOICE_LINK_STG(
GLOBAL_PO_INVOICE_LINK_KEY INT NOT NULL,
ORIGINAL_PO_INVOICE_HEADER_KEY INT,
ORIGINAL_PO_INVOICE_HEADER_ID STRING,
ORIGINAL_PO_INVOICE_LINE_KEY INT,
ORIGINAL_PO_INVOICE_LINE_ID STRING,
APPLIED_PO_INVOICE_HEADER_KEY INT,
APPLIED_PO_INVOICE_ID STRING,
APPLIED_PO_INVOICE_LINE_KEY INT,
APPLIED_PO_INVOICE_LINE_ID STRING,
FIRST_EFFECTIVE_TS TIMESTAMP,
LAST_EFFECTIVE_TS TIMESTAMP,
LINKED_AMT DOUBLE,
FOREIGN_LINKED_AMT DOUBLE,
LINKED_QTY INT,
DISCOUNT_FLAG CHAR(1),
LINK_TYPE_CD STRING,
LINK_TYPE_DESC STRING,
INVENTORY_NBR INT,
ORIGINAL_POSTED_DT DATE,
APPLIED_POSTED_DT DATE,
INSERT_TS TIMESTAMP NOT NULL,
UPDATE_TS TIMESTAMP NOT NULL
) using delta location "dbfs:/mnt/data/governed/l2/stage/transaction_linkes_invoice/global_po_invoice_link_stg"

-- COMMAND ----------

-- DBTITLE 1,GLOBAL_PO_LINK DDL
CREATE TABLE L2_ANALYTICS_TABLES.GLOBAL_PO_LINK
(
GLOBAL_PO_LINK_KEY BIGINT NOT NULL,
ORIGINAL_PO_HEADER_KEY BIGINT,
ORIGINAL_PO_HEADER_ID STRING,
ORIGINAL_PO_LINE_KEY BIGINT,
ORIGINAL_PO_LINE_ID STRING,
APPLIED_PO_HEADER_KEY BIGINT,
APPLIED_PO_ID STRING,
APPLIED_PO_LINE_KEY BIGINT,
APPLIED_PO_LINE_ID STRING,
FIRST_EFFECTIVE_TS TIMESTAMP,
LAST_EFFECTIVE_TS TIMESTAMP,
FOREIGN_LINKED_AMT DECIMAL(15,4),
LINKED_AMT DECIMAL(15,4),
LINKED_QTY INT,
DISCOUNT_FLAG CHAR(1),
LINK_TYPE_CD STRING,
LINK_TYPE_DESC STRING,
INVENTORY_NBR INT,
ORIGINAL_POSTED_DT DATE,
APPLIED_POSTED_DT DATE,
INSERT_TS TIMESTAMP NOT NULL,
UPDATE_TS TIMESTAMP NOT NULL
)
USING DELTA
LOCATION "dbfs:/mnt/data/governed/l2/analytics/transactions/global_po_link/"
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);;

-- COMMAND ----------

drop table if exists L2_ANALYTICS_TABLES.GLOBAL_PO_INVOICE_LINK;

CREATE TABLE L2_ANALYTICS_TABLES.GLOBAL_PO_INVOICE_LINK(
GLOBAL_PO_INVOICE_LINK_KEY BIGINT NOT NULL,
ORIGINAL_PO_INVOICE_HEADER_KEY BIGINT,
ORIGINAL_PO_INVOICE_HEADER_ID STRING,
ORIGINAL_PO_INVOICE_LINE_KEY BIGINT,
ORIGINAL_PO_INVOICE_LINE_ID STRING,
APPLIED_PO_INVOICE_HEADER_KEY BIGINT,
APPLIED_PO_INVOICE_ID STRING,
APPLIED_PO_INVOICE_LINE_KEY BIGINT,
APPLIED_PO_INVOICE_LINE_ID STRING,
FIRST_EFFECTIVE_TS TIMESTAMP,
LAST_EFFECTIVE_TS TIMESTAMP,
LINKED_AMT DOUBLE,
FOREIGN_LINKED_AMT DOUBLE,
LINKED_QTY INT,
DISCOUNT_FLAG CHAR(1),
LINK_TYPE_CD STRING,
LINK_TYPE_DESC STRING,
INVENTORY_NBR INT,
ORIGINAL_POSTED_DT DATE,
APPLIED_POSTED_DT DATE,
INSERT_TS TIMESTAMP NOT NULL,
UPDATE_TS TIMESTAMP NOT NULL
) using delta 
location "dbfs:/mnt/data/governed/l2/analytics/transactions/global_po_invoice_link"
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- COMMAND ----------

-- DBTITLE 1,GLOBAL_PO_LINK view
CREATE OR REPLACE VIEW L2_ANALYTICS.GLOBAL_PO_LINK AS SELECT * FROM  L2_ANALYTICS_TABLES.GLOBAL_PO_LINK WHERE  LAST_EFFECTIVE_TS='9999-12-31T23:59:59.000+0000';

CREATE OR REPLACE VIEW L2_ANALYTICS.GLOBAL_PO_INVOICE_LINK AS SELECT * FROM  L2_ANALYTICS_TABLES.GLOBAL_PO_INVOICE_LINK;

-- COMMAND ----------

-- DBTITLE 1,Snowflake table DDL
CREATE TABLE "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."GLOBAL_PO_LINK"
(GLOBAL_PO_LINK_KEY NUMBER(38,0),
ORIGINAL_PO_HEADER_KEY NUMBER(38,0),
ORIGINAL_PO_HEADER_ID VARCHAR(16777216),
ORIGINAL_PO_LINE_KEY NUMBER(38,0),
ORIGINAL_PO_LINE_ID VARCHAR(16777216),
APPLIED_PO_HEADER_KEY NUMBER(38,0),
APPLIED_PO_ID VARCHAR(16777216),
APPLIED_PO_LINE_KEY NUMBER(38,0),
APPLIED_PO_LINE_ID VARCHAR(16777216),
FIRST_EFFECTIVE_TS TIMESTAMP_NTZ(9),
LAST_EFFECTIVE_TS TIMESTAMP_NTZ(9),
FOREIGN_LINKED_AMT NUMBER(15,4),
LINKED_AMT NUMBER(15,4),
LINKED_QTY NUMBER(38,0),
DISCOUNT_FLAG CHAR(1),
LINK_TYPE_CD VARCHAR(16777216),
LINK_TYPE_DESC VARCHAR(16777216),
INVENTORY_NBR NUMBER(38,0),
ORIGINAL_POSTED_DT DATE,
APPLIED_POSTED_DT DATE,
INSERT_TS TIMESTAMP_NTZ(9) ,
UPDATE_TS TIMESTAMP_NTZ(9));


-- COMMAND ----------

create or replace TABLE PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.GLOBAL_PO_INVOICE_LINK (
	GLOBAL_PO_INVOICE_LINK_KEY NUMBER(38,0) NOT NULL,
	ORIGINAL_PO_INVOICE_HEADER_KEY NUMBER(38,0),
	ORIGINAL_PO_INVOICE_HEADER_ID VARCHAR(16777216),
	ORIGINAL_PO_INVOICE_LINE_KEY NUMBER(38,0),
	ORIGINAL_PO_INVOICE_LINE_ID VARCHAR(16777216),
	APPLIED_PO_INVOICE_HEADER_KEY NUMBER(38,0),
	APPLIED_PO_INVOICE_ID VARCHAR(16777216),
	APPLIED_PO_INVOICE_LINE_KEY NUMBER(38,0),
	APPLIED_PO_INVOICE_LINE_ID VARCHAR(16777216),
	FIRST_EFFECTIVE_TS TIMESTAMP_NTZ(9),
	LAST_EFFECTIVE_TS TIMESTAMP_NTZ(9),
	LINKED_AMT FLOAT,
	FOREIGN_LINKED_AMT FLOAT,
	LINKED_QTY NUMBER(38,0),
	DISCOUNT_FLAG VARCHAR(1),
	LINK_TYPE_CD VARCHAR(16777216),
	LINK_TYPE_DESC VARCHAR(16777216),
	INVENTORY_NBR NUMBER(38,0),
	ORIGINAL_POSTED_DT DATE,
	APPLIED_POSTED_DT DATE,
	INSERT_TS TIMESTAMP_NTZ(9) NOT NULL,
	UPDATE_TS TIMESTAMP_NTZ(9) NOT NULL
);

-- COMMAND ----------

-- DBTITLE 1,Snowflake view
CREATE  OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.GLOBAL_PO_LINK AS SELECT * FROM "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."GLOBAL_PO_LINK" 
WHERE LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000';

CREATE  OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.GLOBAL_PO_INVOICE_LINK AS SELECT * FROM PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.GLOBAL_PO_INVOICE_LINK 
