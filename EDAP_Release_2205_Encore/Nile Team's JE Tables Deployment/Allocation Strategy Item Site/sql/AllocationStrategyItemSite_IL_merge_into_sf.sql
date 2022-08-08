MERGE INTO "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INV_FCST_LOCATION_ITEM_ALLOC_STRATEGY" TARGET 
 USING ( 
     SELECT STG.INV_FCST_LOCATION_ITEM_ALLOC_STRATEGY_KEY AS MERGE_KEY, STG.*
     FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."INV_FCST_LOCATION_ITEM_ALLOC_STRATEGY_STAGE" STG
     UNION 
     SELECT NULL AS MERGE_KEY, STG1.*
     FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."INV_FCST_LOCATION_ITEM_ALLOC_STRATEGY_STAGE" STG1
    ) STG2 ON TARGET.INV_FCST_LOCATION_ITEM_ALLOC_STRATEGY_KEY = STG2.MERGE_KEY
 WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000' THEN
    UPDATE SET TARGET.LAST_EFFECTIVE_TS = dateadd(SECOND, - 1, STG2.FIRST_EFFECTIVE_TS), TARGET.UPDATE_TS = STG2.UPDATE_TS
 WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
        INSERT (INV_FCST_LOCATION_ITEM_ALLOC_STRATEGY_KEY,
    LOCATION_KEY,
    LOCATION_ID,
    LOCATION_DESC,
    LOCATION_TYPE_CD,
    CONCEPT_CD,
    ITEM_KEY,
    ITEM_ID,
    MARKET_CD,
    INV_FCST_ITEM_PLACEHOLDER_KEY,
    INV_FCST_ITEM_PLACEHOLDER_ID,
    VIRTUAL_WAREHOUSE_KEY,
    VIRTUAL_WAREHOUSE_ID,
    VIRTUAL_WAREHOUSE_NM,
    SHIP_NODE_KEY,
    SHIP_NODE_ID,
    FIRST_EFFECTIVE_TS,
    LAST_EFFECTIVE_TS,
    PARENT_ITEM_PLACEHOLDER_CD,
    ITEM_PLACEHOLDER_DESC,
    ITEM_PLACEHOLDER_DEPT_DESC,
    ALLOCATION_STRATEGY_CD,
    ALLOCATION_STRATEGY_TYPE_CD,
    MATERIAL_EXCEPTIONS_DESC,
    PRESENTATION_PRIORITY_DESC,
    DEMAND_TYPE_CD,
    CURVE_ASSIGNMENT_SIZE_DESC,
    PRODUCT_MODEL_DESC,
    RETREND_METHOD_CD,
    USE_RETREND_FLAG,
    LOCATION_CLUSTER_SET,
    LOCATION_CLUSTER,
    LOCATION_STRATEGY_DESC,
    PRESENTATION_MAX_NBR,
    MIN_STOCK_PER_SIZE_NBR,
    SITE_ELIGIBLE_FLAG,
    INVENTORY_SOURCE_SELECTION_DESC,
    PRIORITY_SEQUENCE_DESC,
    SELL_THROUGH_TARGET_PCT,  
    SALES_CURVE_SELECTION_DESC,
    HOLD_BACK_PCT, 
    ALLOCATION_STATUS_DESC, 
    SETTING_SOURCE_DESC,
    ALLOW_RESERVATION_FLAG,
    SALES_PLAN_BASIS_DESC,
    TEMPORAL_PLAN_DESC,
    GROUP_STRATEGY_FLAG,
    STORE_EXPECTED_QTY,
    INSERT_TS,
    UPDATE_TS)
        VALUES (STG2.INV_FCST_LOCATION_ITEM_ALLOC_STRATEGY_KEY,
    STG2.LOCATION_KEY,
    STG2.LOCATION_ID,
    STG2.LOCATION_DESC,
    STG2.LOCATION_TYPE_CD,
    STG2.CONCEPT_CD,
    STG2.ITEM_KEY,
    STG2.ITEM_ID,
    STG2.MARKET_CD,
    STG2.INV_FCST_ITEM_PLACEHOLDER_KEY,
    STG2.INV_FCST_ITEM_PLACEHOLDER_ID,
    STG2.VIRTUAL_WAREHOUSE_KEY,
    STG2.VIRTUAL_WAREHOUSE_ID,
    STG2.VIRTUAL_WAREHOUSE_NM,
    STG2.SHIP_NODE_KEY,
    STG2.SHIP_NODE_ID,
    STG2.FIRST_EFFECTIVE_TS,
    STG2.LAST_EFFECTIVE_TS,
    STG2.PARENT_ITEM_PLACEHOLDER_CD,
    STG2.ITEM_PLACEHOLDER_DESC,
    STG2.ITEM_PLACEHOLDER_DEPT_DESC,
    STG2.ALLOCATION_STRATEGY_CD,
    STG2.ALLOCATION_STRATEGY_TYPE_CD,
    STG2.MATERIAL_EXCEPTIONS_DESC,
    STG2.PRESENTATION_PRIORITY_DESC,
    STG2.DEMAND_TYPE_CD,
    STG2.CURVE_ASSIGNMENT_SIZE_DESC,
    STG2.PRODUCT_MODEL_DESC,
    STG2.RETREND_METHOD_CD,
    STG2.USE_RETREND_FLAG,
    STG2.LOCATION_CLUSTER_SET,
    STG2.LOCATION_CLUSTER,
    STG2.LOCATION_STRATEGY_DESC,
    STG2.PRESENTATION_MAX_NBR,
    STG2.MIN_STOCK_PER_SIZE_NBR,
    STG2.SITE_ELIGIBLE_FLAG,
    STG2.INVENTORY_SOURCE_SELECTION_DESC,
    STG2.PRIORITY_SEQUENCE_DESC,
    STG2.SELL_THROUGH_TARGET_PCT,  
    STG2.SALES_CURVE_SELECTION_DESC,
    STG2.HOLD_BACK_PCT, 
    STG2.ALLOCATION_STATUS_DESC, 
    STG2.SETTING_SOURCE_DESC,
    STG2.ALLOW_RESERVATION_FLAG,
    STG2.SALES_PLAN_BASIS_DESC,
    STG2.TEMPORAL_PLAN_DESC,
    STG2.GROUP_STRATEGY_FLAG,
    STG2.STORE_EXPECTED_QTY,
    STG2.INSERT_TS,
    STG2.UPDATE_TS)