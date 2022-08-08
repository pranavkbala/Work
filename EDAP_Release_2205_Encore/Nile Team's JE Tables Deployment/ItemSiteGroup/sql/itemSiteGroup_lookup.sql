SELECT 
                COALESCE(CAST(IFLIG.INV_FCST_LOCATION_ITEM_GROUP_KEY as BIGINT),-1) AS INV_FCST_LOCATION_ITEM_GROUP_KEY,
                TRIM(STG.CONCEPT_CD) AS CONCEPT_CD,
                COALESCE(CAST(L2_LOCATION.LOCATION_KEY as BIGINT),-1) AS LOCATION_KEY,
                LPAD(TRIM(CAST(STG.LOCATION_ID AS STRING)),GREATEST(LENGTH(STG.LOCATION_ID),4),0) AS LOCATION_ID,
                COALESCE(CAST(L2_ITEM.ITEM_KEY as BIGINT),-1) AS ITEM_KEY,
                TRIM(CAST(STG.ITEM_ID AS STRING)) AS ITEM_ID,
                TRIM(L2_ITEM.MARKET_CD) AS MARKET_CD,
                COALESCE(CAST(L2_PH_ITEM.INV_FCST_ITEM_PLACEHOLDER_KEY as BIGINT),-1) AS INV_FCST_ITEM_PLACEHOLDER_KEY,
                TRIM(CAST(L2_PH_ITEM.INV_FCST_ITEM_PLACEHOLDER_ID AS STRING)) AS INV_FCST_ITEM_PLACEHOLDER_ID,
                COALESCE(CAST(L2_VIRTUAL_WH.VIRTUAL_WAREHOUSE_KEY as BIGINT),-1) AS VIRTUAL_WAREHOUSE_KEY,
                TRIM(CAST(L2_VIRTUAL_WH.VIRTUAL_WAREHOUSE_ID AS STRING)) as VIRTUAL_WAREHOUSE_ID,
                CAST(L2_VIRTUAL_WH.WAREHOUSE_NM as STRING) AS VIRTUAL_WAREHOUSE_NM,
                COALESCE(CAST(L2_VIRTUAL_WH.SHIP_NODE_KEY as BIGINT),-1) AS SHIP_NODE_KEY,
                TRIM(CAST(L2_VIRTUAL_WH.SHIP_NODE_ID AS STRING)) as SHIP_NODE_ID,
                TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(cast(STG.LastUpdatedDateTime as STRING),'yyyyMMddHHmmss'))) AS LastUpdatedDateTime,
                TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(cast(STG.LastUpdatedDateTime as STRING),'yyyyMMddHHmmss'))) AS FIRST_EFFECTIVE_TS,
                TIMESTAMP(STG.LAST_EFFECTIVE_TS) AS LAST_EFFECTIVE_TS,
                TRIM(L2_PH_ITEM.PARENT_ITEM_PLACEHOLDER_CD) AS PARENT_ITEM_PLACEHOLDER_CD,
                TRIM(L2_PH_ITEM.ITEM_PLACEHOLDER_DESC) AS ITEM_PLACEHOLDER_DESC,
                TRIM(L2_PH_ITEM.ITEM_PLACEHOLDER_DEPT_DESC) AS ITEM_PLACEHOLDER_DEPT_DESC,
                TRIM(STG.INV_GROUP_NM) AS INV_GROUP_NM,
                TRIM(STG.INV_GROUP_VAL) AS INV_GROUP_VAL,
                TIMESTAMP(STG.INSERT_TS) AS INSERT_TS,
                TIMESTAMP(STG.UPDATE_TS) AS UPDATE_TS 
                FROM 
                STG_TEMP_LOOKUP_INV_FCST_LOCATION_ITEM_GROUP STG  
                LEFT OUTER JOIN  
                 (SELECT * FROM L2_ANALYTICS_TABLES.INV_FCST_LOCATION_ITEM_GROUP where last_effective_ts like '%9999%') IFLIG 
                 ON IFLIG.ITEM_ID=TRIM(CAST(STG.ITEM_ID AS STRING))  AND   
                 IFLIG.LOCATION_ID=LPAD(TRIM(CAST(STG.LOCATION_ID AS STRING)),GREATEST(LENGTH(STG.LOCATION_ID),4),0)  AND 
                 IFLIG.CONCEPT_CD=TRIM(STG.CONCEPT_CD)  AND 
                 IFLIG.INV_GROUP_NM=TRIM(STG.INV_GROUP_NM)  
                  LEFT OUTER JOIN  
  (select LOCATION_ID,LOCATION_KEY,country_cd from delta.`dbfs:/mnt/data/governed/l2/analytics/location/location` where last_effective_ts like '%9999%' )L2_LOCATION 
on LPAD(TRIM(CAST(STG.LOCATION_ID AS STRING)),GREATEST(LENGTH(STG.LOCATION_ID),4),0) =L2_LOCATION.LOCATION_ID 
left outer join 
(select VIRTUAL_WAREHOUSE_KEY,VIRTUAL_WAREHOUSE_ID,WAREHOUSE_NM,SHIP_NODE_KEY,SHIP_NODE_ID from delta.`dbfs:/mnt/data/governed/l2/analytics/wh/virtual_warehouse` where last_effective_ts like '%9999%' )L2_VIRTUAL_WH 
on TRIM(CAST(STG.LOCATION_ID AS STRING))=L2_VIRTUAL_WH.VIRTUAL_WAREHOUSE_ID 
LEFT OUTER JOIN
(select INV_FCST_ITEM_PLACEHOLDER_KEY,INV_FCST_ITEM_PLACEHOLDER_ID,PARENT_ITEM_PLACEHOLDER_CD,ITEM_PLACEHOLDER_DESC,ITEM_PLACEHOLDER_DEPT_DESC from  delta.`dbfs:/mnt/data/governed/l2/analytics/je/placeholderitem` where last_effective_ts='9999-12-31T23:59:59.000+0000')L2_PH_ITEM 
on TRIM(CAST(STG.ITEM_ID AS STRING))=trim(L2_PH_ITEM.INV_FCST_ITEM_PLACEHOLDER_ID)  
left outer join 
(select ship_node_id,ship_node_key, COUNTRY_CD from delta.`dbfs:/mnt/data/governed/l2/analytics/ship_node` where last_effective_ts like '%9999%' )L2_SHIP_NODE  
on L2_SHIP_NODE.ship_node_id=L2_VIRTUAL_WH.SHIP_NODE_ID  
left outer join  
(select item_id,item_key,market_cd from  delta.`dbfs:/mnt/data/governed/l2/analytics/item/item`   where last_effective_ts like '%9999%')L2_ITEM 
on TRIM(CAST(STG.ITEM_ID AS STRING))=L2_ITEM.item_id and L2_ITEM.MARKET_CD = 'USA' 