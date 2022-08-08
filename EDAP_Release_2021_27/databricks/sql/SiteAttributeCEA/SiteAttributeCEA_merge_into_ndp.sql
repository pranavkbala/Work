MERGE INTO L2_ANALYTICS_TABLES.INV_FCST_SITE_ATTR_CEA TARGET
 USING (
    SELECT STG.INV_FCST_SITE_ATTR_CEA_KEY as MERGE_KEY, STG.*
    FROM L2_STAGE.INV_FCST_SITE_ATTR_CEA_STG STG
    UNION
    SELECT NULL as MERGE_KEY, STG1.*
    FROM L2_STAGE.INV_FCST_SITE_ATTR_CEA_STG STG1
    ) STG2 
    ON TARGET.INV_FCST_SITE_ATTR_CEA_KEY = STG2.MERGE_KEY
    WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000' 
    THEN
        UPDATE SET 
        TARGET.LAST_EFFECTIVE_TS = FROM_UNIXTIME(CAST(STG2.FIRST_EFFECTIVE_TS AS LONG) - 1),
        TARGET.UPDATE_TS = STG2.UPDATE_TS
    WHEN NOT MATCHED AND MERGE_KEY IS NULL 
    THEN
        INSERT (INV_FCST_SITE_ATTR_CEA_KEY,
        LOCATION_KEY,
        LOCATION_ID,
        LOCATION_TYPE_CD,
        CONCEPT_CD,
        VIRTUAL_WAREHOUSE_KEY,
        VIRTUAL_WAREHOUSE_ID,
        VIRTUAL_WAREHOUSE_NM,
        SHIP_NODE_KEY,
        SHIP_NODE_ID,
        FIRST_EFFECTIVE_TS,
        LAST_EFFECTIVE_TS,
        SITE_ATTR_NM,
        SITE_ATTR_VAL,
        INSERT_TS,
        UPDATE_TS)
        VALUES (STG2.INV_FCST_SITE_ATTR_CEA_KEY,
        STG2.LOCATION_KEY,
        STG2.LOCATION_ID,
        STG2.LOCATION_TYPE_CD,
        STG2.CONCEPT_CD,
        STG2.VIRTUAL_WAREHOUSE_KEY,
        STG2.VIRTUAL_WAREHOUSE_ID,
        STG2.VIRTUAL_WAREHOUSE_NM,
        STG2.SHIP_NODE_KEY,
        STG2.SHIP_NODE_ID,
        STG2.FIRST_EFFECTIVE_TS,
        STG2.LAST_EFFECTIVE_TS,
        STG2.SITE_ATTR_NM,
        STG2.SITE_ATTR_VAL,
        STG2.INSERT_TS,
        STG2.UPDATE_TS)