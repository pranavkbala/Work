--Create Target & Stage Table


/* 

Load config json files to this path:

dbfs:/FileStore/tables/config/rms_upc_barcode/cost_susp_sup_head/


create or replace table PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.VENDOR_COST_CHANGE
  ( VENDOR_COST_CHANGE_KEY NUMBER(38,0) NOT NULL,
  VENDOR_COST_CHANGE_ID VARCHAR(16777216),
  ACTIVE_DT DATE,
  FIRST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
  LAST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
  VENDOR_COST_CHANGE_DESC VARCHAR(16777216),
  REASON_DESC VARCHAR(16777216),
  STATUS_CD VARCHAR(16777216),
  ORIGIN VARCHAR(16777216),
  APPROVAL_DT DATE, 
  APPROVAL_USER_ID VARCHAR(16777216),
  CREATE_USER_ID VARCHAR(16777216),
  MODIFY_USER_ID VARCHAR(16777216) ,
  INSERT_TS TIMESTAMP_NTZ(9) NOT NULL,
  UPDATE_TS TIMESTAMP_NTZ(9) NOT NULL
   );
   
   
      
create or replace table PROD_EDAP_STAGE_DB.PROD_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_STG2
  (VENDOR_COST_CHANGE_KEY NUMBER(38,0) NOT NULL,
  VENDOR_COST_CHANGE_ID VARCHAR(16777216),
  ACTIVE_DT DATE,
  FIRST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
  LAST_EFFECTIVE_TS TIMESTAMP_NTZ(9) NOT NULL,
  VENDOR_COST_CHANGE_DESC VARCHAR(16777216),
  REASON_DESC VARCHAR(16777216),
  STATUS_CD VARCHAR(16777216),
  ORIGIN VARCHAR(16777216),
  APPROVAL_DT DATE, 
  APPROVAL_USER_ID VARCHAR(16777216),
  CREATE_USER_ID VARCHAR(16777216),
  INSERT_TS TIMESTAMP_NTZ(9) NOT NULL,
  UPDATE_TS TIMESTAMP_NTZ(9) NOT NULL);

   */


 insert into PROD_EDAP_L1_DB.PROD_EDAP_L1_TABLES.DATA_PIPELINE_CONFIG  
values (
4,
'UPC_BARCODE',
8,
'UPC_BARCODE_COST_SUSP_SUSP_HEAD',
1,
'RMS_COST_SUSP_SUP_HEAD_INGEST',
1,
1,
' CREATE OR REPLACE VIEW #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_L1_DEDUP
  AS (
  SELECT
      cast(-1 as bigint) as VENDOR_COST_CHANGE_KEY,
      TRIM(to_varchar(COST_CHANGE)) as VENDOR_COST_CHANGE_ID,
      to_date(ACTIVE_DATE) as ACTIVE_DT,
      TO_TIMESTAMP_NTZ(Current_timestamp()) as FIRST_EFFECTIVE_TS,
      TO_TIMESTAMP_NTZ(\'9999-12-31 23:59:59.000\') AS LAST_EFFECTIVE_TS,
      TRIM(to_varchar(COST_CHANGE_DESC)) as VENDOR_COST_CHANGE_DESC,
      TRIM(to_varchar(REASON)) as REASON_DESC,
      TRIM(to_varchar(STATUS)) as STATUS_CD,
      TRIM(to_varchar(COST_CHANGE_ORIGIN)) as ORIGIN,
      to_date(APPROVAL_DATE) as APPROVAL_DT,
      TRIM(to_varchar(APPROVAL_ID)) as APPROVAL_USER_ID,
      TRIM(to_varchar(CREATE_ID)) as CREATE_USER_ID,
      Current_timestamp() as INSERT_TS,
      Current_timestamp() as UPDATE_TS
  FROM
      (
        SELECT
        ROW_NUMBER() OVER (PARTITION BY COST_CHANGE ORDER BY COST_CHANGE) AS R, *
        FROM #ENV#_EDAP_L1_DB.#ENV#_EDAP_L1_TABLES.L1_RMS_COST_SUSP_SUP_HEAD_LANDING
      )
    WHERE R = 1
);
  
  
CREATE OR REPLACE VIEW #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_CDC AS
 (
  SELECT
        COALESCE(TGT.VENDOR_COST_CHANGE_KEY, -1) AS VENDOR_COST_CHANGE_KEY,
            vcc.VENDOR_COST_CHANGE_ID,
            vcc.ACTIVE_DT,
            vcc.FIRST_EFFECTIVE_TS,
            vcc.LAST_EFFECTIVE_TS,
            vcc.VENDOR_COST_CHANGE_DESC,
            vcc.REASON_DESC,
            vcc.STATUS_CD,
            vcc.ORIGIN,
            vcc.APPROVAL_DT,
            vcc.APPROVAL_USER_ID,
            vcc.CREATE_USER_ID,
            vcc.INSERT_TS,
            vcc.UPDATE_TS
  FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_L1_DEDUP vcc
  LEFT OUTER JOIN #ENV#_EDAP_ANALYTICS_DB.#ENV#_EDAP_ANALYTICS_TABLES.VENDOR_COST_CHANGE TGT
    ON TRIM(COALESCE(vcc.VENDOR_COST_CHANGE_ID,\'-1\')) = TRIM(COALESCE(TGT.VENDOR_COST_CHANGE_ID,\'-1\'))
         AND TGT.LAST_EFFECTIVE_TS = \'9999-12-31 23:59:59.000\'
  WHERE COALESCE(vcc.VENDOR_COST_CHANGE_ID,\'\') <> COALESCE(TGT.VENDOR_COST_CHANGE_ID,\'\')
        OR COALESCE(vcc.ACTIVE_DT,current_date()) <> COALESCE(TGT.ACTIVE_DT,current_date())
        OR COALESCE(vcc.VENDOR_COST_CHANGE_DESC,\'0\') <> COALESCE(TGT.VENDOR_COST_CHANGE_DESC,\'0\')
        OR COALESCE(vcc.REASON_DESC,\'0\') <> COALESCE(TGT.REASON_DESC,\'0\')
        OR COALESCE(vcc.STATUS_CD,\'\') <> COALESCE(TGT.STATUS_CD,\'\')
        OR COALESCE(vcc.ORIGIN,\'\') <> COALESCE(TGT.ORIGIN,\'\')
        OR COALESCE(vcc.APPROVAL_DT,current_date()) <> COALESCE(TGT.APPROVAL_DT,current_date())
        OR COALESCE(vcc.APPROVAL_USER_ID,\'\') <> COALESCE(TGT.APPROVAL_USER_ID,\'\')
        OR COALESCE(vcc.CREATE_USER_ID,\'\') <> COALESCE(TGT.CREATE_USER_ID,\'\')
   );
  
create or replace table #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_GEN_SK as (
SELECT
  CASE WHEN VENDOR_COST_CHANGE_KEY = - 1 THEN
             ROW_NUMBER() OVER (ORDER BY VENDOR_COST_CHANGE_ID) + COALESCE(MAX_VENDOR_COST_CHANGE_KEY, 0)
           ELSE VENDOR_COST_CHANGE_KEY
    END AS VENDOR_COST_CHANGE_KEY,
  VENDOR_COST_CHANGE_ID,
  ACTIVE_DT,
  FIRST_EFFECTIVE_TS,
  LAST_EFFECTIVE_TS,
  VENDOR_COST_CHANGE_DESC,
  REASON_DESC,
  STATUS_CD,
  ORIGIN,
  APPROVAL_DT,
  APPROVAL_USER_ID,
  CREATE_USER_ID,
  INSERT_TS,
  UPDATE_TS
 FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_CDC 
 CROSS JOIN (SELECT MAX(VENDOR_COST_CHANGE_KEY) AS MAX_VENDOR_COST_CHANGE_KEY FROM #ENV#_EDAP_ANALYTICS_DB.#ENV#_EDAP_ANALYTICS_TABLES.VENDOR_COST_CHANGE
   WHERE VENDOR_COST_CHANGE_KEY <> - 1) TGT_MAX);
   
 INSERT OVERWRITE INTO #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_STG2
  SELECT 
  VENDOR_COST_CHANGE_KEY,
  VENDOR_COST_CHANGE_ID,
  ACTIVE_DT,
  FIRST_EFFECTIVE_TS,
  LAST_EFFECTIVE_TS,
  VENDOR_COST_CHANGE_DESC,
  REASON_DESC,
  STATUS_CD,
  ORIGIN,
  APPROVAL_DT,
  APPROVAL_USER_ID,
  CREATE_USER_ID,
  INSERT_TS,
  UPDATE_TS
  FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_GEN_SK;
  
  
  MERGE INTO #ENV#_EDAP_ANALYTICS_DB.#ENV#_EDAP_ANALYTICS_TABLES.VENDOR_COST_CHANGE TARGET 
 USING ( 
     SELECT STG.VENDOR_COST_CHANGE_KEY AS MERGE_KEY, STG.*
     FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_STG2 STG
     UNION 
     SELECT NULL AS MERGE_KEY, STG1.*
     FROM #ENV#_EDAP_STAGE_DB.#ENV#_EDAP_STAGE_TABLES.VENDOR_COST_CHANGE_STG2 STG1
    ) STG2 ON TARGET.VENDOR_COST_CHANGE_KEY = STG2.MERGE_KEY AND TARGET.VENDOR_COST_CHANGE_ID = STG2.VENDOR_COST_CHANGE_ID
 WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = \'9999-12-31 23:59:59.000\' THEN
   UPDATE SET TARGET.LAST_EFFECTIVE_TS = dateadd(SECOND, - 1, STG2.FIRST_EFFECTIVE_TS), TARGET.UPDATE_TS = STG2.UPDATE_TS
 WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
 INSERT (
  VENDOR_COST_CHANGE_KEY,
  VENDOR_COST_CHANGE_ID,
  ACTIVE_DT,
  FIRST_EFFECTIVE_TS,
  LAST_EFFECTIVE_TS,
  VENDOR_COST_CHANGE_DESC,
  REASON_DESC,
  STATUS_CD,
  ORIGIN,
  APPROVAL_DT,
  APPROVAL_USER_ID,
  CREATE_USER_ID,
  INSERT_TS,
  UPDATE_TS)
 VALUES 
 (
  STG2.VENDOR_COST_CHANGE_KEY,
  STG2.VENDOR_COST_CHANGE_ID,
  STG2.ACTIVE_DT,
  STG2.FIRST_EFFECTIVE_TS,
  STG2.LAST_EFFECTIVE_TS,
  STG2.VENDOR_COST_CHANGE_DESC,
  STG2.REASON_DESC,
  STG2.STATUS_CD,
  STG2.ORIGIN,
  STG2.APPROVAL_DT,
  STG2.APPROVAL_USER_ID,
  STG2.CREATE_USER_ID,
  STG2.INSERT_TS,
  STG2.UPDATE_TS);',
'Y',
'R') ;



CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.VENDOR_COST_CHANGE_HIST AS 
 select * from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.VENDOR_COST_CHANGE ;


CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.VENDOR_COST_CHANGE AS 
 select * from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.VENDOR_COST_CHANGE where LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000';