MERGE INTO "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INV_FCST_ITEM_SITE_ATTR_CEA" TARGET 
 USING ( 
     SELECT STG.INV_FCST_ITEM_SITE_ATTR_CEA_KEY AS MERGE_KEY, STG.*
     FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."INV_FCST_ITEM_SITE_ATTR_CEA_STAGE" STG
     UNION 
     SELECT NULL AS MERGE_KEY, STG1.*
     FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."INV_FCST_ITEM_SITE_ATTR_CEA_STAGE" STG1
    ) STG2 ON TARGET.INV_FCST_ITEM_SITE_ATTR_CEA_KEY = STG2.MERGE_KEY
 WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000' THEN
	UPDATE SET TARGET.LAST_EFFECTIVE_TS = dateadd(SECOND, - 1, STG2.FIRST_EFFECTIVE_TS), TARGET.UPDATE_TS = STG2.UPDATE_TS
 WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
        INSERT (INV_FCST_ITEM_SITE_ATTR_CEA_KEY,
        LOCATION_KEY,
        LOCATION_ID,
        LOCATION_TYPE_CD,
        CONCEPT_CD,
        ITEM_KEY,
        ITEM_ID,
        MARKET_CD,
        VIRTUAL_WAREHOUSE_KEY,
        VIRTUAL_WAREHOUSE_ID,
        VIRTUAL_WAREHOUSE_NM,
        SHIP_NODE_KEY,
        SHIP_NODE_ID,
        FIRST_EFFECTIVE_TS,
        LAST_EFFECTIVE_TS,
        ITEM_SITE_ATTR_NM,
        ITEM_SITE_ATTR_VAL,
        INSERT_TS,
        UPDATE_TS)
        VALUES (STG2.INV_FCST_ITEM_SITE_ATTR_CEA_KEY,
        STG2.LOCATION_KEY,
        STG2.LOCATION_ID,
        STG2.LOCATION_TYPE_CD,
        STG2.CONCEPT_CD,
        STG2.ITEM_KEY,
        STG2.ITEM_ID,
        STG2.MARKET_CD,
        STG2.VIRTUAL_WAREHOUSE_KEY,
        STG2.VIRTUAL_WAREHOUSE_ID,
        STG2.VIRTUAL_WAREHOUSE_NM,
        STG2.SHIP_NODE_KEY,
        STG2.SHIP_NODE_ID,
        STG2.FIRST_EFFECTIVE_TS,
        STG2.LAST_EFFECTIVE_TS,
        STG2.ITEM_SITE_ATTR_NM,
        STG2.ITEM_SITE_ATTR_VAL,
        STG2.INSERT_TS,
        STG2.UPDATE_TS);


/*
Pivot table Store Procedure to be executed in SnowFlake (Manually execution)

create or replace procedure PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.PROC_INV_FCST_ITEM_SITE_ATTR_CEA_PIVOT()
returns string
language javascript
execute as caller as
$$
  var cols_query = `select distinct ITEM_SITE_ATTR_NM as ITEM_SITE_ATTR_NM from PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INV_FCST_ITEM_SITE_ATTR_CEA`;
  var stmt1 = snowflake.createStatement({sqlText: cols_query});
  var results1 = stmt1.execute();
  var col_list = "";
  while(results1.next())
    col_list = col_list +"'"+ results1.getColumnValue(1)+"',";
  col_list= col_list.slice(0, -1);
  pivot_query = `CREATE OR REPLACE VIEW PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS.PROC_INV_FCST_ITEM_SITE_ATTR_CEA_PIVOT_V AS (select * from  PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.INV_FCST_ITEM_SITE_ATTR_CEA
         pivot(max(ITEM_SITE_ATTR_VAL) for ITEM_SITE_ATTR_NM in (${col_list})))`
  var stmt2 = snowflake.createStatement({sqlText: pivot_query});
  stmt2.execute();
  return pivot_query;
$$

*/

call PROD_EDAP_ANALYTICS_DB.PROD_EDAP_ANALYTICS_TABLES.PROC_INV_FCST_ITEM_SITE_ATTR_CEA_PIVOT();