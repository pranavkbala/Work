SELECT
    CASE WHEN LOCATION_ITEM_INV_FCST_WEEK_AGG_KEY = - 1 THEN
        ROW_NUMBER() OVER (ORDER BY LOCATION_ITEM_INV_FCST_WEEK_AGG_KEY, ITEM_ID, CONCEPT_CD, LOCATION_ID, FISCAL_WEEK_START_DT) + COALESCE(MAX_LOCATION_ITEM_INV_FCST_WEEK_AGG_KEY, 0)
        ELSE LOCATION_ITEM_INV_FCST_WEEK_AGG_KEY
    END AS LOCATION_ITEM_INV_FCST_WEEK_AGG_KEY,
    LOCATION_ITEM_INV_FCST_WEEK_AGG_ID,
    CONCEPT_CD	,			 				
    LOCATION_KEY	,
    LOCATION_ID	,
    ITEM_KEY,		
    ITEM_ID,			
    ITEM_NAME,
    MARKET_CD,
    INV_FCST_ITEM_PLACEHOLDER_KEY,
    INV_FCST_ITEM_PLACEHOLDER_ID,
    FISCAL_WEEK_KEY, 
    FISCAL_WEEK_NBR,
    FISCAL_WEEK_START_DT,
    FISCAL_WEEK_END_DT,	
    COALESCE(FIRST_EFFECTIVE_TS,current_timestamp()) AS FIRST_EFFECTIVE_TS,
    LAST_EFFECTIVE_TS    ,
    PARENT_ITEM_PLACEHOLDER_CD,
    ITEM_PLACEHOLDER_DESC,
    ITEM_PLACEHOLDER_DEPT_DESC,
    PROJECTED_STOCK_ON_HAND_QTY	,		
    CONSTRAINED_DEMAND_UNITS_CNT	,
    EFFECTIVE_SAFETY_STOCK_UNITS_CNT	,
    LOST_SALES_UNITS_CNT	,
    MIN_SAFETY_STOCK_UNITS_CNT,
    MODEL_STOCK_UNITS_CNT,
    PRE_AUTHORIZED_ORDER_CNT,
    RECOMMENDED_ORDER_UNITS_CNT,
    EXPEDITE_UNITS_CNT,
    MAX_STOCK_DAYS_CNT,
    MAX_STOCK_UNITS_CNT,
    SHORT_FALL_UNITS_CNT,
    STOCK_OUT_UNITS_CNT,
    SURPLUS_UNITS_CNT,
    RESERVED_STOCK_UNITS_CNT,
    RECOMMENDED_SAFETY_STOCK_UNITS_CNT, 
    OPTIMAL_ALLOCATION_UNIT_CNT,
    DC_RESERVED_UNIT_CNT,
    ALLOCATED_IN_TRANSIT_UNIT_CNT,
    ALLOCATED_UNIT_CNT,
    PRE_ALLOCATED_UNIT_CNT,	 	
    INSERT_TS,			
    UPDATE_TS
 FROM STG_TEMP_LOCATION_ITEM_INV_FCST_WEEK_AGG
 CROSS JOIN (
   SELECT
       MAX(LOCATION_ITEM_INV_FCST_WEEK_AGG_KEY) AS MAX_LOCATION_ITEM_INV_FCST_WEEK_AGG_KEY
   FROM
      L2_ANALYTICS_TABLES.LOCATION_ITEM_INV_FCST_WEEK_AGG
   WHERE
      LOCATION_ITEM_INV_FCST_WEEK_AGG_KEY <> - 1
  ) TGT_MAX