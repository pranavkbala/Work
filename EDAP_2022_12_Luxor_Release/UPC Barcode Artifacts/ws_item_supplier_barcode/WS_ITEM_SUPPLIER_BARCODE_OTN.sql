--Create Target & Stage Table


/* 

Load config json files to this path:

dbfs:/FileStore/tables/config/rms_upc_barcode/ws_item_supplier_barcode/


create or replace table PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ITEM_VENDOR_BARCODE
  ( ITEM_VENDOR_BARCODE_KEY NUMBER(38,0) NOT NULL,
  VENDOR_KEY NUMBER(38,0),
  VENDOR_ID VARCHAR(16777216),
  CONCEPT_CD VARCHAR(16777216) NOT NULL,
  ITEM_KEY NUMBER(38,0),
  ITEM_ID VARCHAR(16777216),
  MARKET_CD VARCHAR(16777216),
  FIRST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
  LAST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
  BARCODE_NBR NUMBER(38,0),
  BARCODE_TYPE_CD VARCHAR(16777216),
  TRAVERSAL_FLAG VARCHAR(16777216),
  LAST_UPDATE_ID VARCHAR(16777216),
  LAST_UPDATE_DATETIME TIMESTAMP_NTZ(9),
  INSERT_TS TIMESTAMP_NTZ(9) NOT NULL,
  UPDATE_TS TIMESTAMP_NTZ(9) NOT NULL
   );
   
   
      
create or replace table PROD_EDAP_STAGE_DB.PROD_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_STG2
  (ITEM_VENDOR_BARCODE_KEY NUMBER(38,0) NOT NULL,
  VENDOR_KEY NUMBER(38,0),
  VENDOR_ID VARCHAR(16777216),
  CONCEPT_CD VARCHAR(16777216) NOT NULL,
  ITEM_KEY NUMBER(38,0),
  ITEM_ID VARCHAR(16777216),
  MARKET_CD VARCHAR(16777216),
  FIRST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
  LAST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
  BARCODE_NBR NUMBER(38,0),
  BARCODE_TYPE_CD VARCHAR(16777216),
  TRAVERSAL_FLAG VARCHAR(16777216),
  LAST_UPDATE_ID VARCHAR(16777216),
  LAST_UPDATE_DATETIME TIMESTAMP_NTZ(9),
  INSERT_TS TIMESTAMP_NTZ(9) NOT NULL,
  UPDATE_TS TIMESTAMP_NTZ(9) NOT NULL);


   */



insert into PROD_EDAP_L1_DB.PROD_EDAP_L1_TABLES.DATA_PIPELINE_CONFIG  
values (
4,
'UPC_BARCODE',
11,
'UPC_BARCODE_WS_ITEM_SUPPLIER_BARCODE',
1,
'RMS_WS_ITEM_SUPPLIER_BARCODE_INGEST',
1,
1,
' CREATE OR REPLACE VIEW #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_L1_ASSIGNS
  AS (
  SELECT
      cast(-1 as bigint) as ITEM_VENDOR_BARCODE_KEY,
      cast(-1 as bigint) as VENDOR_KEY,
      TRIM(to_varchar(SUPPLIER)) as VENDOR_ID,
      TRIM(to_varchar(\'NA\')) as CONCEPT_CD,
      cast(-1 as bigint) as ITEM_KEY,
      TRIM(to_varchar(ITEM)) as ITEM_ID,
      TRIM(to_varchar(\'NA\')) as MARKET_CD,
      TO_TIMESTAMP_NTZ(LAST_UPDATE_DATETIME) as FIRST_EFFECTIVE_TS,
      TO_TIMESTAMP_NTZ(\'9999-12-31 23:59:59.000\') AS LAST_EFFECTIVE_TS,
      TRIM(CAST(BARCODE as NUMBER(38,0))) as BARCODE_NBR,
      TRIM(to_varchar(BARCODE_TYPE)) as BARCODE_TYPE_CD,
      TRIM(to_varchar(TRAVERSAL_FLAG)) as TRAVERSAL_FLAG,
      TRIM(to_varchar(LAST_UPDATE_ID)) as LAST_UPDATE_ID,
      TO_TIMESTAMP_NTZ(LAST_UPDATE_DATETIME) as LAST_UPDATE_DATETIME,
      Current_timestamp() as INSERT_TS,
      Current_timestamp() as UPDATE_TS
  FROM #ENV#_EDAP_L1_DB.#ENV#_EDAP_L1_TABLES.L1_RMS_WS_ITEM_SUPPLIER_BARCODE_LANDING
);
  
CREATE OR REPLACE VIEW #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_CDC AS
 (
  SELECT
    COALESCE(TGT.ITEM_VENDOR_BARCODE_KEY, -1) AS ITEM_VENDOR_BARCODE_KEY,
      ivb.VENDOR_KEY,
      ivb.VENDOR_ID,
      ivb.CONCEPT_CD,
      ivb.ITEM_KEY,
      ivb.ITEM_ID,
      ivb.MARKET_CD,
      ivb.FIRST_EFFECTIVE_TS,
      ivb.LAST_EFFECTIVE_TS,
      ivb.BARCODE_NBR,
      ivb.BARCODE_TYPE_CD,
      ivb.TRAVERSAL_FLAG,
      ivb.LAST_UPDATE_ID,
      ivb.LAST_UPDATE_DATETIME,
      ivb.INSERT_TS,
      ivb.UPDATE_TS
  FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_L1_ASSIGNS ivb
  LEFT OUTER JOIN #ENV#_EDAP_ANALYTICS_DB.#ENV#_EDAP_ANALYTICS_TABLES.ITEM_VENDOR_BARCODE TGT
         ON TRIM(COALESCE(ivb.VENDOR_ID,\'-1\')) = TRIM(COALESCE(TGT.VENDOR_ID,\'-1\'))
         AND TRIM(COALESCE(ivb.ITEM_ID,\'-1\')) = TRIM(COALESCE(TGT.ITEM_ID,\'-1\'))
         AND TGT.LAST_EFFECTIVE_TS = \'9999-12-31 23:59:59.000\'
  WHERE COALESCE(ivb.VENDOR_ID,\'\') <> COALESCE(TGT.VENDOR_ID,\'\')
        OR COALESCE(ivb.LAST_UPDATE_DATETIME,Current_timestamp()) <> COALESCE(TGT.LAST_UPDATE_DATETIME,Current_timestamp())
        OR COALESCE(ivb.ITEM_ID,\'0\') <> COALESCE(TGT.ITEM_ID,\'0\')
        OR COALESCE(ivb.BARCODE_NBR,\'0\') <> COALESCE(TGT.BARCODE_NBR,\'0\')
        OR COALESCE(ivb.BARCODE_TYPE_CD,\'0\') <> COALESCE(TGT.BARCODE_TYPE_CD,\'0\')
        OR COALESCE(ivb.TRAVERSAL_FLAG,\'NA\') <> COALESCE(TGT.TRAVERSAL_FLAG,\'NA\')
        OR COALESCE(ivb.LAST_UPDATE_ID,\'NA\') <> COALESCE(TGT.LAST_UPDATE_ID,\'NA\')
   );

create or replace table #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_GEN_SK as (
SELECT
  CASE WHEN ITEM_VENDOR_BARCODE_KEY = - 1 THEN
             ROW_NUMBER() OVER (ORDER BY VENDOR_ID) + COALESCE(MAX_ITEM_VENDOR_BARCODE_KEY, 0)
           ELSE ITEM_VENDOR_BARCODE_KEY
    END AS ITEM_VENDOR_BARCODE_KEY,
      VENDOR_KEY,
      VENDOR_ID,
      CONCEPT_CD,
      ITEM_KEY,
      ITEM_ID,
      MARKET_CD,
      FIRST_EFFECTIVE_TS,
      LAST_EFFECTIVE_TS,
      BARCODE_NBR,
      BARCODE_TYPE_CD,
      TRAVERSAL_FLAG,
      LAST_UPDATE_ID,
      LAST_UPDATE_DATETIME,
      INSERT_TS,
      UPDATE_TS
 FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_CDC 
 CROSS JOIN (SELECT MAX(ITEM_VENDOR_BARCODE_KEY) AS MAX_ITEM_VENDOR_BARCODE_KEY FROM #ENV#_EDAP_ANALYTICS_DB.#ENV#_EDAP_ANALYTICS_TABLES.ITEM_VENDOR_BARCODE
   WHERE ITEM_VENDOR_BARCODE_KEY <> - 1) TGT_MAX);

create or replace table #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_JOINS_GEN as
(
select ivb.ITEM_VENDOR_BARCODE_KEY as ITEM_VENDOR_BARCODE_KEY,
      vdr.VENDOR_KEY as VENDOR_KEY,
      ivb.VENDOR_ID as VENDOR_ID,
      itm.CONCEPT_CD as CONCEPT_CD,
      itm.ITEM_KEY as ITEM_KEY,
      ivb.ITEM_ID as ITEM_ID,
      itm.MARKET_CD as MARKET_CD,
      ivb.FIRST_EFFECTIVE_TS as FIRST_EFFECTIVE_TS,
      ivb.LAST_EFFECTIVE_TS as LAST_EFFECTIVE_TS,
      ivb.BARCODE_NBR as BARCODE_NBR,
      ivb.BARCODE_TYPE_CD as BARCODE_TYPE_CD,
      ivb.TRAVERSAL_FLAG as TRAVERSAL_FLAG,
      ivb.LAST_UPDATE_ID as LAST_UPDATE_ID,
      ivb.LAST_UPDATE_DATETIME as LAST_UPDATE_DATETIME,
      ivb.INSERT_TS as INSERT_TS,
      ivb.UPDATE_TS as UPDATE_TS      
from #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_GEN_SK ivb 
 LEFT JOIN #ENV#_EDAP_ANALYTICS_DB.#ENV#_EDAP_ANALYTICS_TABLES.VENDOR vdr on vdr.VENDOR_ID = ivb.VENDOR_ID and vdr.LAST_EFFECTIVE_TS = \'9999-12-31 23:59:59.000\'
 LEFT JOIN #ENV#_EDAP_ANALYTICS_DB.#ENV#_EDAP_ANALYTICS_TABLES.ITEM itm on itm.ITEM_ID = ivb.ITEM_ID and itm.LAST_EFFECTIVE_TS = \'9999-12-31 23:59:59.000\' and itm.MARKET_CD= \'USA\'
);

 INSERT OVERWRITE INTO #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_STG2
  SELECT 
      ITEM_VENDOR_BARCODE_KEY,
      VENDOR_KEY,
      VENDOR_ID,
      CONCEPT_CD,
      ITEM_KEY,
      ITEM_ID,
      MARKET_CD,
      FIRST_EFFECTIVE_TS,
      LAST_EFFECTIVE_TS,
      BARCODE_NBR,
      BARCODE_TYPE_CD,
      TRAVERSAL_FLAG,
      LAST_UPDATE_ID,
      LAST_UPDATE_DATETIME,
      INSERT_TS,
      UPDATE_TS
  FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_JOINS_GEN;
  
  MERGE INTO #ENV#_EDAP_ANALYTICS_DB.#ENV#_EDAP_ANALYTICS_TABLES.ITEM_VENDOR_BARCODE TARGET 
 USING ( 
     SELECT STG.ITEM_VENDOR_BARCODE_KEY AS MERGE_KEY, STG.*
     FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_STG2 STG
     UNION 
     SELECT NULL AS MERGE_KEY, STG1.*
     FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.ITEM_VENDOR_BARCODE_STG2 STG1
    ) STG2 ON TARGET.ITEM_VENDOR_BARCODE_KEY = STG2.MERGE_KEY AND TARGET.VENDOR_ID = STG2.VENDOR_ID AND TARGET.VENDOR_KEY = STG2.VENDOR_KEY AND TARGET.ITEM_ID = STG2.ITEM_ID AND TARGET.ITEM_KEY = STG2.ITEM_KEY
 WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = \'9999-12-31 23:59:59.000\' THEN
   UPDATE SET TARGET.LAST_EFFECTIVE_TS = dateadd(SECOND, - 1, STG2.FIRST_EFFECTIVE_TS), TARGET.UPDATE_TS = STG2.UPDATE_TS
 WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
 INSERT (
      ITEM_VENDOR_BARCODE_KEY,
      VENDOR_KEY,
      VENDOR_ID,
      CONCEPT_CD,
      ITEM_KEY,
      ITEM_ID,
      MARKET_CD,
      FIRST_EFFECTIVE_TS,
      LAST_EFFECTIVE_TS,
      BARCODE_NBR,
      BARCODE_TYPE_CD,
      TRAVERSAL_FLAG,
      LAST_UPDATE_ID,
      LAST_UPDATE_DATETIME,
      INSERT_TS,
      UPDATE_TS)
 VALUES 
 (    STG2.ITEM_VENDOR_BARCODE_KEY,
      STG2.VENDOR_KEY,
      STG2.VENDOR_ID,
      STG2.CONCEPT_CD,
      STG2.ITEM_KEY,
      STG2.ITEM_ID,
      STG2.MARKET_CD,
      STG2.FIRST_EFFECTIVE_TS,
      STG2.LAST_EFFECTIVE_TS,
      STG2.BARCODE_NBR,
      STG2.BARCODE_TYPE_CD,
      STG2.TRAVERSAL_FLAG,
      STG2.LAST_UPDATE_ID,
      STG2.LAST_UPDATE_DATETIME,
      STG2.INSERT_TS,
      STG2.UPDATE_TS );',
'Y',
'R') ;




CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.ITEM_VENDOR_BARCODE_HIST AS 
 select * from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ITEM_VENDOR_BARCODE ;


CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.ITEM_VENDOR_BARCODE AS 
 select * from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.ITEM_VENDOR_BARCODE where LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000';