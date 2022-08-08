/*
******************************************************************
******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      01/25/2021  DAN-4647  SQL file created
**  02		06/08/2021 SEPIK      Added COLUMNS as part of adding to OL from Sterling OH
**  03      07/23/2021  SUT-160   Added Columns as part of Adding to OL

******************************************************************

** Author			    : Danube Team DAN-4647
** Create Date		: JAN,2021
** Purpose      	: Look up on ORDER_LINE


Source : L1 RJ Inbound file Dataframe
Target : L2_STAGE.RJ_ORDER_LINE_STG

******************************************************************
******************************************************************
*/
SELECT
         COALESCE(OL.ORDER_LINE_KEY, 0 ) AS ORDER_LINE_KEY,
         T1.INVENTTRANSID AS ORDER_LINE_ID,
         CAST(NULL  AS BIGINT) AS CHAINED_FROM_ORDER_HEADER_KEY,         /* Add new Column - SEPIK */
         NULL AS CHAINED_FROM_ORDER_HEADER_ID,         /* Add new Column - SEPIK */
         CAST(NULL  AS BIGINT) AS CHAINED_FROM_ORDER_LINE_KEY,          /* Add new Column - SEPIK */
         NULL AS CHAINED_FROM_ORDER_LINE_ID,          /* Add new Column - SEPIK */
	      "RJ" AS SOURCE_SYSTEM,
        null AS PRIME_LINE_SEQ_NBR,
        CAST((T1.linenum)*10 AS INT) AS SUB_LINE_SEQ_NBR,
	      COALESCE(OH.ORDER_HEADER_KEY, -1 ) AS ORDER_HEADER_KEY,
	      TO_DATE(T1.createddatetime) AS ORDER_DT,
        null AS LINKED_ORDER_HEADER_KEY,
        null AS LINKED_ORDER_HEADER_ID,
        null AS LINKED_ORDER_LINE_KEY,
        null AS LINKED_ORDER_LINE_ID,
        COALESCE(IT.ITEM_KEY, -1 ) AS ITEM_KEY,
        T1.itemid AS ORDER_ITEM_ID,
        null AS ORDER_ITEM_TYPE_CD,
	      null AS ORDER_ITEM_NAME,
        null AS GIFT_REGISTRY_KEY,
        null AS GIFT_REGISTRY_ID,
        T1.createddatetime AS FIRST_EFFECTIVE_TS,
        CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000" , 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
        null AS UPC_CD,
        null AS KIT_CD,
        null AS KIT_QTY,
        T1.jsproductline AS PRODUCT_LINE,
        null AS BUNDLE_PARENT_ORDER_LINE_ID,
        null AS ORDER_LINE_TYPE,
        CAST(T1.qtyordered AS INT) AS ORDER_QTY,
        null AS ORIG_ORDER_QTY,
        CAST(T1.salesprice AS DECIMAL(15,2)) AS ACT_SALE_UNIT_PRICE_AMT,
        null AS REG_SALE_UNIT_PRICE_AMT,
        null AS EXTENDED_AMT,
        null AS EXTENDED_DISCOUNT_AMT,
        null AS TAX_AMT,
        null AS TAXABLE_AMT,
        null AS GIFT_CARD_AMT,
        null AS GIFTWRAP_CHARGE_AMT,
        null AS LINE_TOTAL_AMT,
        null AS MERCHANDISE_CHARGE_AMT,
        null AS MONO_PZ_CHARGE_AMT,
        null AS MISC_CHARGE_AMT,
        null AS SHIPPING_HANDLING_CHARGE_AMT,
        null AS SHIPPING_SURCHARGE_AMT,
        null AS DONATION_AMT,
        null AS ASSOCIATE_ID,
        null AS ENTRY_METHOD,
        null AS GIFT_MESSAGE,
        null AS VOID_FLAG,
        null AS REPO_FLAG,
        null AS TAXABLE_FLAG,
        null AS PICKABLE_FLAG,
        null AS GIFT_FLAG,
        null AS HOLD_FLAG,
        null AS HOLD_REASON,
        null AS ORIG_BACKORDER_FLAG,
        null AS SUBORDER_COMPLEXITY_GROUP_ID,  /* SUTLEJ CHANGE */
        null AS DELIVERY_CHOICE,
        null AS MODIFICATION_REASON_CD,
        null AS MODIFICATION_REASON_CD_DESC,
        null AS RETURN_ACTION_CD,
        null AS RETURN_ACTION,
        null AS RETURN_REASON_CD,
        null AS RETURN_REASON_DESC,
        null AS RETURN_SUB_REASON_CD,
        null AS SHIP_TO_FIRST_NAME,
        null AS SHIP_TO_MIDDLE_NAME,
        null AS SHIP_TO_LAST_NAME,
        null AS SHIP_TO_ADDRESS_LINE_1,
        null AS SHIP_TO_ADDRESS_LINE_2,
        null AS SHIP_TO_CITY,
        null AS SHIP_TO_STATE_OR_PROV,
        null AS SHIP_TO_POSTAL_CD,
        null AS SHIP_TO_COUNTRY,
        null AS SHIP_TO_EMAIL_ADDRESS,
        null AS SHIP_TO_PHONE_NBR,
        NULL AS DELIVERY_METHOD,                     /* Add new Column - SEPIK */
        NULL AS FULFILLMENT_TYPE,                             /* Add new Column - SEPIK */
        NULL AS DTC_SUBORDER_NBR,                     /* Add new Column - SEPIK */
        NULL AS ADDITIONAL_LINE_TYPE_CD,             /* Add new Column - SEPIK */
        NULL AS ORIG_CONFIRMED_QTY,                /* Add new Column - SEPIK */
        NULL AS RESKU_FLAG,                        /* Add new Column - SEPIK */
        null AS CONSOLIDATOR_ADDRESS_CD,
        null AS MERGE_NODE_CD,
        null AS SHIP_NODE_CD,
        null AS RECEIVING_NODE_CD,
        null AS LEVEL_OF_SERVICE,
        null AS CARRIER_SERVICE_CD,
        null AS CARRIER_CD,
        null AS ACCESS_POINT_CD,
        null AS ACCESS_POINT_ID,
        null AS ACCESS_POINT_NM,
        null AS MINIMUM_SHIP_DT,
        null AS REQUESTED_SHIP_DT,
        null AS REQUESTED_DELIVERY_DT,
        null AS EARLIEST_SCHEDULE_DT,
        null AS EARLIEST_DELIVERY_DT,
        null AS PROMISED_APPT_END_DT,
        null AS SPLIT_QTY,
        null AS SHIPPED_QTY,
        null AS FILL_QTY,
        null AS WEIGHTED_AVG_COST,
        null AS DIRECT_SHIP_FLAG,
        null AS UNIT_COST,
        null AS LABOR_COST,
        null AS LABOR_SKU,
        null AS CUSTOMER_LEVEL_OF_SERVICE,
        NULL AS RETURN_POLICY,	                        /* Add new Column - SEPIK */
        NULL AS RETURN_POLICY_CHECK_OVERRIDE_FLAG,     /* Add new Column - SEPIK */
        NULL AS PRODUCT_AVAILABILITY_DT,               /* Add new Column - SEPIK */
        NULL AS ECDD_OVERRIDDEN_FLAG,                  /* Add new Column - SEPIK */
        NULL AS ECDD_INVOKED_FLAG,                     /* Add new Column - SEPIK */
        NULL AS VAS_GIFT_WRAP_CD,				       /* Add new Column - SEPIK */
        NULL AS VAS_MONO_FLAG,                        /* Add new Column - SEPIK */
        NULL AS VAS_PZ_FLAG,						/* Add new Column - SEPIK */
        null AS BO_NOTIFICATION_NBR,         /* Added by NILE team for CYODD */
        CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
	    CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
	      FROM RJ_OL_VIEW1  T1
        LEFT OUTER JOIN L2_ANALYTICS_TABLES.ORDER_HEADER OH
        ON T1.salesid = OH.ORDER_HEADER_ID
        AND OH.SOURCE_SYSTEM = 'RJ'
        AND OH.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
        LEFT OUTER JOIN L2_ANALYTICS_TABLES.ITEM IT
        ON T1.itemid = IT.ITEM_ID
        AND IT.MARKET_CD = 'USA'
        AND IT.SOURCE_SYSTEM = 'RJDAX'
        AND IT.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
        LEFT OUTER JOIN L2_ANALYTICS_TABLES.ORDER_LINE OL
        ON T1.INVENTTRANSID = OL.ORDER_LINE_ID
        AND OL.SOURCE_SYSTEM = 'RJ'
        AND OL.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
