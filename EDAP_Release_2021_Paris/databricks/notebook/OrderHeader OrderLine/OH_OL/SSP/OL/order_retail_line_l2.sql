/*
******************************************************************
** Author         : Columbia - COL-3872
** Create Date    : March, 2020
** Purpose        : Load ORDER_LINE from POSlog
******************************************************************
** Source Table   : ORDER_STG1_VIEW2
** Target Table   : L2_ANALYTICS_TABLES.ORDER_LINE
** Lookup         :
** Transformations  : Load data from POSLog-Stage to Order-Line
******************************************************************
************************* Change History *************************
******************************************************************
** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      03/10/2020  COL-3872  SQL file created
**  02		06/10/2020  COL-4170  Enhancement by hard coding source_system
**  03		08/13/2020  THAM-205  Cloumns are added for UPSAP
**  04      09/26/2020  VLG-583   RETURN_SUB_REASON_CD for VPS
**  05      10/13/2020  COL-4482  Model Factory- Gift Receipt Flag in POSLog
**  06      11/16/2020  COL-4529  Model Factory- Gift messages Historical in Order_Line 
**  07      01/08/2021  COL-4480  Retail demand data in Snowflake
**  08      06/08/2021  NLE-5606  CYODD- BO_Notification_Nbr in Order_Line
**  09      06/08/2021  SEPIK     Addition of columns
**  10      07/23/2021  SUT-160   Addition of columns
******************************************************************
*/

/*
** Source Table   : L2_ANALYTICS.ORDER_HEADER
** Target View    : ORDER_HEADER_LINE_LINK_STG1_VIEW1
** Identify subset of ORDER_HEADER for deriving LINKED_ORDER_HEADER_KEY
**
*/

CREATE OR REPLACE TEMPORARY VIEW ORDER_HEADER_LINE_LINK_STG1_VIEW1 AS
 (
	SELECT * FROM  L2_ANALYTICS.ORDER_HEADER
	WHERE ORDER_DT IN (SELECT RETURN_LINKED_ORDER_DT FROM  L2_STAGE.ORDER_RTL_STG WHERE RETURN_LINKED_ORDER_DT IS NOT NULL  GROUP BY RETURN_LINKED_ORDER_DT)
	AND SOURCE_SYSTEM IN ( 'REDIRON_POSLOG', 'NCR_POSLOG' )
 );


/*
** Source View    : ORDER_STG1_VIEW2
** Target View    : ORDER_LINE_STG1_VIEW1
** Fetch only Line-CDC records from ORDER_STG1_VIEW2
** Lookup on ORDER_HEADER_STG1_VIEW2 for ORDER_HEADER_KEY
** Derive LINKED_ORDER_HEADER_KEY from L2_ANALYTICS_TABLES.ORDER_HEADER
**
*/

CREATE OR REPLACE TEMPORARY VIEW ORDER_LINE_STG1_VIEW1 AS
 (
	SELECT
		CASE
			WHEN STG_HEADER.ORDER_HEADER_ID IS NULL AND STG.ORDER_HEADER_KEY = 0 THEN -1
			WHEN STG_HEADER.ORDER_HEADER_ID IS NOT NULL THEN STG_HEADER.ORDER_HEADER_KEY
			ELSE STG.ORDER_HEADER_KEY
		END AS ORDER_HEADER_KEY,
		STG.ORDER_LINE_KEY,
		STG.ORDER_LINE_ID,
		STG.ORDER_DT,
		STG.SOURCE_SYSTEM,
		STG.PRIME_LINE_SEQ_NBR,
		STG.ITEM_KEY,
		STG.ORDER_ITEM_ID,
		STG.ORDER_ITEM_TYPE_CD,
		STG.ORDER_ITEM_NAME,
		STG.GIFT_REGISTRY_ID,
		STG.FIRST_EFFECTIVE_TS,
		STG.LAST_EFFECTIVE_TS,
		STG.ORDER_LINE_TYPE,
		STG.ORDER_QTY,
		STG.ORIG_ORDER_QTY,
		STG.ACT_SALE_UNIT_PRICE_AMT,
		STG.REG_SALE_UNIT_PRICE_AMT,
		STG.EXTENDED_AMT,
		STG.EXTENDED_DISCOUNT_AMT,
		STG.LINE_TAX_AMT AS TAX_AMT,
		STG.TAXABLE_AMT,
		STG.GIFT_CARD_AMT,
		STG.LINE_TOTAL_AMT,
		STG.DONATION_AMT,
		STG.ASSOCIATE_ID,
		STG.ENTRY_METHOD,
		STG.VOID_FLAG, 
		STG.GIFT_FLAG, 
		CASE
			WHEN STG.RETURN_LINKED_ORDER_HEADER_ID IS NULL THEN NULL
			ELSE COALESCE(HEADER.ORDER_HEADER_KEY, STG_HEADER_1.ORDER_HEADER_KEY, -1 )
		END AS LINKED_ORDER_HEADER_KEY, 
		STG.RETURN_LINKED_ORDER_HEADER_ID AS LINKED_ORDER_HEADER_ID,
		STG.RETURN_ACTION_CD,
		STG.RETURN_ACTION,
		STG.RETURN_REASON_CD,
		STG.RETURN_REASON_DESC,
		STG.SHIP_TO_FIRST_NAME,
		STG.SHIP_TO_MIDDLE_NAME,
		STG.SHIP_TO_LAST_NAME,
		STG.SHIP_TO_ADDRESS_LINE_1,
		STG.SHIP_TO_ADDRESS_LINE_2,
		STG.SHIP_TO_CITY,
		STG.SHIP_TO_STATE_OR_PROV,
		STG.SHIP_TO_POSTAL_CD,
		STG.SHIP_TO_COUNTRY,
		STG.SHIP_TO_EMAIL_ADDRESS,
		STG.SHIP_TO_PHONE_NBR,
		STG.INSERT_TS,
		STG.UPDATE_TS
	FROM ORDER_STG1_VIEW2 STG
	LEFT OUTER JOIN ORDER_HEADER_STG1_VIEW2 STG_HEADER
		ON STG.ORDER_HEADER_ID = STG_HEADER.ORDER_HEADER_ID
	LEFT OUTER JOIN ORDER_HEADER_STG1_VIEW2 STG_HEADER_1
		ON STG.RETURN_LINKED_ORDER_HEADER_ID = STG_HEADER_1.ORDER_HEADER_ID
	LEFT OUTER JOIN ORDER_HEADER_LINE_LINK_STG1_VIEW1  HEADER
		ON  STG.RETURN_LINKED_ORDER_DT = HEADER.ORDER_DT
		AND STG.RETURN_LINKED_ORDER_HEADER_ID = HEADER.ORDER_HEADER_ID
	WHERE LINE_INC_IND = 'I'
 );


/*
** Source View    : ORDER_LINE_STG1_VIEW1
** Target View    : ORDER_LINE_STG1_VIEW2
** GENERATE SURROGATE KEYS AND POPULATE NULL COLUMNS
**
*/


CREATE OR REPLACE TEMPORARY VIEW ORDER_LINE_STG1_VIEW2 AS
 (
	SELECT
		CASE
			WHEN STG1_VIEW1.ORDER_LINE_KEY = 0 THEN
				CAST((ROW_NUMBER() OVER ( ORDER BY STG1_VIEW1.ORDER_LINE_KEY, STG1_VIEW1.ORDER_LINE_ID ) + MAX_ORDER_LINE_KEY ) AS BIGINT)
			ELSE CAST(STG1_VIEW1.ORDER_LINE_KEY AS BIGINT)
		END AS ORDER_LINE_KEY,
		STG1_VIEW1.ORDER_LINE_ID,
		STG1_VIEW1.ORDER_DT,
		CAST (NULL AS BIGINT) AS CHAINED_FROM_ORDER_HEADER_KEY,         /* Add new Column - SEPIK */
        NULL AS CHAINED_FROM_ORDER_HEADER_ID,         /* Add new Column - SEPIK */
        CAST (NULL AS BIGINT) AS CHAINED_FROM_ORDER_LINE_KEY,          /* Add new Column - SEPIK */
        NULL AS CHAINED_FROM_ORDER_LINE_ID,            /* Add new Column - SEPIK */
		STG1_VIEW1.SOURCE_SYSTEM,
		STG1_VIEW1.PRIME_LINE_SEQ_NBR,
		NULL AS SUB_LINE_SEQ_NBR,
		/*NULL AS DERIVED_FROM_ORDER_LINE_KEY,*/
		/*NULL AS BUNDLE_PARENT_ORDER_LINE_KEY,*/
		STG1_VIEW1.ORDER_HEADER_KEY,
		STG1_VIEW1.LINKED_ORDER_HEADER_KEY,
		STG1_VIEW1.LINKED_ORDER_HEADER_ID,
		CAST(NULL  AS BIGINT)  AS LINKED_ORDER_LINE_KEY,
		NULL AS LINKED_ORDER_LINE_ID,
		STG1_VIEW1.ITEM_KEY,
		STG1_VIEW1.ORDER_ITEM_ID,
		STG1_VIEW1.ORDER_ITEM_TYPE_CD,
		STG1_VIEW1.ORDER_ITEM_NAME,
		NULL AS GIFT_REGISTRY_KEY,
		STG1_VIEW1.GIFT_REGISTRY_ID,
		STG1_VIEW1.FIRST_EFFECTIVE_TS,
		STG1_VIEW1.LAST_EFFECTIVE_TS,
		NULL AS UPC_CD,
		NULL AS KIT_CD,
		NULL AS KIT_QTY,
		NULL AS PRODUCT_LINE,
		NULL AS BUNDLE_PARENT_ORDER_LINE_ID,
		STG1_VIEW1.ORDER_LINE_TYPE,
		STG1_VIEW1.ORDER_QTY,
		STG1_VIEW1.ORIG_ORDER_QTY,
		STG1_VIEW1.ACT_SALE_UNIT_PRICE_AMT,
		STG1_VIEW1.REG_SALE_UNIT_PRICE_AMT,
		STG1_VIEW1.EXTENDED_AMT,
		STG1_VIEW1.EXTENDED_DISCOUNT_AMT,
		STG1_VIEW1.TAX_AMT,
		STG1_VIEW1.TAXABLE_AMT,
		STG1_VIEW1.GIFT_CARD_AMT,
		NULL AS GIFTWRAP_CHARGE_AMT,
		STG1_VIEW1.LINE_TOTAL_AMT,
		NULL AS MERCHANDISE_CHARGE_AMT,
		NULL AS MONO_PZ_CHARGE_AMT,
		NULL AS MISC_CHARGE_AMT,
		NULL AS SHIPPING_HANDLING_CHARGE_AMT,
		NULL AS SHIPPING_SURCHARGE_AMT,
		STG1_VIEW1.DONATION_AMT,
		STG1_VIEW1.ASSOCIATE_ID,
		STG1_VIEW1.ENTRY_METHOD,
		NULL AS GIFT_MESSAGE,
		STG1_VIEW1.VOID_FLAG,
		NULL AS REPO_FLAG,
		NULL AS TAXABLE_FLAG,
		NULL AS PICKABLE_FLAG,
		STG1_VIEW1.GIFT_FLAG,
		NULL AS HOLD_FLAG,
		NULL AS HOLD_REASON,
		NULL AS ORIG_BACKORDER_FLAG,
        NULL AS SUBORDER_COMPLEXITY_GROUP_ID,  /* SUTLEJ CHANGE */
		NULL AS DELIVERY_CHOICE,             /* Add new Column - INDUS */
		NULL AS MODIFICATION_REASON_CD, 	 /* Add new Column - INDUS */
		NULL AS MODIFICATION_REASON_CD_DESC, /* Add new Column - INDUS */
		STG1_VIEW1.RETURN_ACTION_CD,
		STG1_VIEW1.RETURN_ACTION,
		STG1_VIEW1.RETURN_REASON_CD,
		STG1_VIEW1.RETURN_REASON_DESC,
		NULL AS RETURN_SUB_REASON_CD,
		STG1_VIEW1.SHIP_TO_FIRST_NAME,
		STG1_VIEW1.SHIP_TO_MIDDLE_NAME,
		STG1_VIEW1.SHIP_TO_LAST_NAME,
		STG1_VIEW1.SHIP_TO_ADDRESS_LINE_1,
		STG1_VIEW1.SHIP_TO_ADDRESS_LINE_2,
		STG1_VIEW1.SHIP_TO_CITY,
		STG1_VIEW1.SHIP_TO_STATE_OR_PROV,
		STG1_VIEW1.SHIP_TO_POSTAL_CD,
		STG1_VIEW1.SHIP_TO_COUNTRY,
		STG1_VIEW1.SHIP_TO_EMAIL_ADDRESS,
		STG1_VIEW1.SHIP_TO_PHONE_NBR,
		NULL AS DELIVERY_METHOD,                     /* Add new Column - SEPIK */
        NULL AS FULFILLMENT_TYPE,                             /* Add new Column - SEPIK */
        NULL AS DTC_SUBORDER_NBR,                     /* Add new Column - SEPIK */
        NULL AS ADDITIONAL_LINE_TYPE_CD,             /* Add new Column - SEPIK */
        NULL AS ORIG_CONFIRMED_QTY,                /* Add new Column - SEPIK */
        NULL AS RESKU_FLAG,                        /* Add new Column - SEPIK */
		NULL AS CONSOLIDATOR_ADDRESS_CD,
		NULL AS MERGE_NODE_CD,
		NULL AS SHIP_NODE_CD,
		NULL AS RECEIVING_NODE_CD,
		NULL AS LEVEL_OF_SERVICE,
		NULL AS CARRIER_SERVICE_CD,
		NULL AS CARRIER_CD,
        NULL AS ACCESS_POINT_CD,
        NULL AS ACCESS_POINT_ID,
        NULL AS ACCESS_POINT_NM,
		NULL AS MINIMUM_SHIP_DT,
		NULL AS REQUESTED_SHIP_DT,
		NULL AS REQUESTED_DELIVERY_DT,
		NULL AS EARLIEST_SCHEDULE_DT,
		NULL AS EARLIEST_DELIVERY_DT,
		NULL AS PROMISED_APPT_END_DT,
		/*NULL AS SALE_QTY,*/
		NULL AS SPLIT_QTY,
		NULL AS SHIPPED_QTY,
		NULL AS FILL_QTY,
		NULL AS WEIGHTED_AVG_COST,
		NULL AS DIRECT_SHIP_FLAG,
		NULL AS UNIT_COST,
		NULL AS LABOR_COST,
		NULL AS LABOR_SKU,
		NULL AS CUSTOMER_LEVEL_OF_SERVICE,        /* Add new Column - INDUS */
		NULL AS RETURN_POLICY,	                        /* Add new Column - SEPIK */
		NULL AS RETURN_POLICY_CHECK_OVERRIDE_FLAG,     /* Add new Column - SEPIK */
		NULL AS PRODUCT_AVAILABILITY_DT,               /* Add new Column - SEPIK */
		NULL AS ECDD_OVERRIDDEN_FLAG,                  /* Add new Column - SEPIK */
		NULL AS ECDD_INVOKED_FLAG,                     /* Add new Column - SEPIK */
		NULL AS VAS_GIFT_WRAP_CD,				       /* Add new Column - SEPIK */
		NULL AS VAS_MONO_FLAG,                        /* Add new Column - SEPIK */
		NULL AS VAS_PZ_FLAG,						/* Add new Column - SEPIK */
		NULL AS BO_NOTIFICATION_NBR,              /* Added by NILE for CYODD */
		STG1_VIEW1.INSERT_TS,
		STG1_VIEW1.UPDATE_TS
	FROM ORDER_LINE_STG1_VIEW1 STG1_VIEW1
	CROSS JOIN
		(
			SELECT
				COALESCE(MAX(ORDER_LINE_KEY), 0) AS MAX_ORDER_LINE_KEY
			FROM L2_ANALYTICS_TABLES.ORDER_LINE
		) LINE_MAX
 );


/* Insert overwrite to Stage table  */

INSERT OVERWRITE TABLE L2_STAGE.ORDER_RTL_LINE_STG (select * from ORDER_LINE_STG1_VIEW2);

/*
** Source View   : ORDER_LINE_STG1_VIEW2
** Target View   : L2_ANALYTICS_TABLES.ORDER_LINE
** Merge to  L2_ANALYTICS_TABLES.ORDER_LINE
**
*/


MERGE INTO L2_ANALYTICS_TABLES.ORDER_LINE  LINE USING
 (
	SELECT STG1.ORDER_LINE_KEY AS MERGE_KEY, STG1.* FROM L2_STAGE.ORDER_RTL_LINE_STG STG1
	UNION
	SELECT NULL AS MERGE_KEY, STG2.* FROM L2_STAGE.ORDER_RTL_LINE_STG STG2
 ) STG
	ON STG.ORDER_DT = LINE.ORDER_DT
	/* AND STG.SOURCE_SYSTEM = LINE.SOURCE_SYSTEM */
	AND LINE.SOURCE_SYSTEM IN ( 'REDIRON_POSLOG', 'NCR_POSLOG' )
	AND LINE.ORDER_LINE_KEY = STG.MERGE_KEY
 WHEN MATCHED AND LINE.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000' THEN
	UPDATE SET
		LAST_EFFECTIVE_TS = FROM_UNIXTIME(CAST(STG.FIRST_EFFECTIVE_TS AS LONG) - 1),
		UPDATE_TS = STG.UPDATE_TS
 WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
	INSERT (
		ORDER_LINE_KEY,
		ORDER_LINE_ID,
		ORDER_DT,
		CHAINED_FROM_ORDER_HEADER_KEY,         /* Add new Column - SEPIK */
		CHAINED_FROM_ORDER_HEADER_ID,         /* Add new Column - SEPIK */
		CHAINED_FROM_ORDER_LINE_KEY,          /* Add new Column - SEPIK */
		CHAINED_FROM_ORDER_LINE_ID,          /* Add new Column - SEPIK */
		SOURCE_SYSTEM,
		PRIME_LINE_SEQ_NBR,
		SUB_LINE_SEQ_NBR,
		/*DERIVED_FROM_ORDER_LINE_KEY,*/
		ORDER_HEADER_KEY,
		LINKED_ORDER_HEADER_KEY,
		LINKED_ORDER_HEADER_ID,
		LINKED_ORDER_LINE_KEY,
		LINKED_ORDER_LINE_ID,
		ITEM_KEY,
		ORDER_ITEM_ID,
		ORDER_ITEM_TYPE_CD,
		ORDER_ITEM_NAME,
		GIFT_REGISTRY_KEY,
		GIFT_REGISTRY_ID,
		FIRST_EFFECTIVE_TS,
		LAST_EFFECTIVE_TS,
		UPC_CD,
		KIT_CD,
		KIT_QTY,
		PRODUCT_LINE,
		BUNDLE_PARENT_ORDER_LINE_ID,
		/*BUNDLE_PARENT_ORDER_LINE_KEY,*/
		ORDER_LINE_TYPE,
		ORDER_QTY,
		ORIG_ORDER_QTY,
		ACT_SALE_UNIT_PRICE_AMT,
		REG_SALE_UNIT_PRICE_AMT,
		EXTENDED_AMT,
		EXTENDED_DISCOUNT_AMT,
		TAX_AMT,
		TAXABLE_AMT,
		GIFT_CARD_AMT,
		GIFTWRAP_CHARGE_AMT,
		LINE_TOTAL_AMT,
		MERCHANDISE_CHARGE_AMT,
		MONO_PZ_CHARGE_AMT,
		MISC_CHARGE_AMT,
		SHIPPING_HANDLING_CHARGE_AMT,
		SHIPPING_SURCHARGE_AMT,
		DONATION_AMT,
		ASSOCIATE_ID,
		ENTRY_METHOD,
		GIFT_MESSAGE,
		VOID_FLAG,
		REPO_FLAG,
		TAXABLE_FLAG,
		PICKABLE_FLAG,
		GIFT_FLAG,
		HOLD_FLAG,
		HOLD_REASON,
		ORIG_BACKORDER_FLAG,
        SUBORDER_COMPLEXITY_GROUP_ID,  /* SUTLEJ CHANGE */
		DELIVERY_CHOICE, 		     /* Add new Column - INDUS */
		MODIFICATION_REASON_CD, 	 /* Add new Column - INDUS */
		MODIFICATION_REASON_CD_DESC, /* Add new Column - INDUS */
		RETURN_ACTION_CD,
		RETURN_ACTION,
		RETURN_REASON_CD,
		RETURN_REASON_DESC,
		RETURN_SUB_REASON_CD,
		SHIP_TO_FIRST_NAME,
		SHIP_TO_MIDDLE_NAME,
		SHIP_TO_LAST_NAME,
		SHIP_TO_ADDRESS_LINE_1,
		SHIP_TO_ADDRESS_LINE_2,
		SHIP_TO_CITY,
		SHIP_TO_STATE_OR_PROV,
		SHIP_TO_POSTAL_CD,
		SHIP_TO_COUNTRY,
		SHIP_TO_EMAIL_ADDRESS,
		SHIP_TO_PHONE_NBR,
		DELIVERY_METHOD,                     /* Add new Column - SEPIK */	
		FULFILLMENT_TYPE,                             /* Add new Column - SEPIK */	
		DTC_SUBORDER_NBR,                     /* Add new Column - SEPIK */	
		ADDITIONAL_LINE_TYPE_CD,             /* Add new Column - SEPIK */	
		ORIG_CONFIRMED_QTY,                /* Add new Column - SEPIK */	
		RESKU_FLAG,                        /* Add new Column - SEPIK */
		CONSOLIDATOR_ADDRESS_CD,
		MERGE_NODE_CD,
		SHIP_NODE_CD,
		RECEIVING_NODE_CD,
		LEVEL_OF_SERVICE,
		CARRIER_SERVICE_CD,
		CARRIER_CD,
        ACCESS_POINT_CD,
        ACCESS_POINT_ID,
        ACCESS_POINT_NM,
		MINIMUM_SHIP_DT,
		REQUESTED_SHIP_DT,
		EARLIEST_SCHEDULE_DT,
		REQUESTED_DELIVERY_DT,
		EARLIEST_DELIVERY_DT,
		PROMISED_APPT_END_DT,
		/*SALE_QTY,*/
		SPLIT_QTY,
		SHIPPED_QTY,
		FILL_QTY,
		WEIGHTED_AVG_COST,
		DIRECT_SHIP_FLAG,
		UNIT_COST,
		LABOR_COST,
		LABOR_SKU,
		CUSTOMER_LEVEL_OF_SERVICE,      /* Add new Column - INDUS */
		RETURN_POLICY,	                        /* Add new Column - SEPIK */
		RETURN_POLICY_CHECK_OVERRIDE_FLAG,     /* Add new Column - SEPIK */
		PRODUCT_AVAILABILITY_DT,               /* Add new Column - SEPIK */
		ECDD_OVERRIDDEN_FLAG,                  /* Add new Column - SEPIK */
		ECDD_INVOKED_FLAG,                     /* Add new Column - SEPIK */
		VAS_GIFT_WRAP_CD,				       /* Add new Column - SEPIK */
		VAS_MONO_FLAG,                        /* Add new Column - SEPIK */
		VAS_PZ_FLAG,						/* Add new Column - SEPIK */
		BO_NOTIFICATION_NBR,            /* Added by NILE for CYODD */
		INSERT_TS,
		UPDATE_TS
	) VALUES (
		STG.ORDER_LINE_KEY,
		STG.ORDER_LINE_ID,
		STG.ORDER_DT,
		STG.CHAINED_FROM_ORDER_HEADER_KEY,         /* Add new Column - SEPIK */
		STG.CHAINED_FROM_ORDER_HEADER_ID,         /* Add new Column - SEPIK */
		STG.CHAINED_FROM_ORDER_LINE_KEY,          /* Add new Column - SEPIK */
		STG.CHAINED_FROM_ORDER_LINE_ID,          /* Add new Column - SEPIK */
		STG.SOURCE_SYSTEM,
		STG.PRIME_LINE_SEQ_NBR,
		STG.SUB_LINE_SEQ_NBR,
		/*STG.DERIVED_FROM_ORDER_LINE_KEY,*/
		STG.ORDER_HEADER_KEY,
		STG.LINKED_ORDER_HEADER_KEY,
		STG.LINKED_ORDER_HEADER_ID,
		STG.LINKED_ORDER_LINE_KEY,
		STG.LINKED_ORDER_LINE_ID,
		STG.ITEM_KEY,
		STG.ORDER_ITEM_ID,
		STG.ORDER_ITEM_TYPE_CD,
		STG.ORDER_ITEM_NAME,
		STG.GIFT_REGISTRY_KEY,
		STG.GIFT_REGISTRY_ID,
		STG.FIRST_EFFECTIVE_TS,
		STG.LAST_EFFECTIVE_TS,
		STG.UPC_CD,
		STG.KIT_CD,
		STG.KIT_QTY,
		STG.PRODUCT_LINE,
		STG.BUNDLE_PARENT_ORDER_LINE_ID,
		/*STG.BUNDLE_PARENT_ORDER_LINE_KEY,*/
		STG.ORDER_LINE_TYPE,
		STG.ORDER_QTY,
		STG.ORIG_ORDER_QTY,
		STG.ACT_SALE_UNIT_PRICE_AMT,
		STG.REG_SALE_UNIT_PRICE_AMT,
		STG.EXTENDED_AMT,
		STG.EXTENDED_DISCOUNT_AMT,
		STG.TAX_AMT,
		STG.TAXABLE_AMT,
		STG.GIFT_CARD_AMT,
		STG.GIFTWRAP_CHARGE_AMT,
		STG.LINE_TOTAL_AMT,
		STG.MERCHANDISE_CHARGE_AMT,
		STG.MONO_PZ_CHARGE_AMT,
		STG.MISC_CHARGE_AMT,
		STG.SHIPPING_HANDLING_CHARGE_AMT,
		STG.SHIPPING_SURCHARGE_AMT,
		STG.DONATION_AMT,
		STG.ASSOCIATE_ID,
		STG.ENTRY_METHOD,
		STG.GIFT_MESSAGE,
		STG.VOID_FLAG,
		STG.REPO_FLAG,
		STG.TAXABLE_FLAG,
		STG.PICKABLE_FLAG,
		STG.GIFT_FLAG,
		STG.HOLD_FLAG,
		STG.HOLD_REASON,
		STG.ORIG_BACKORDER_FLAG,
        STG.SUBORDER_COMPLEXITY_GROUP_ID,  /* SUTLEJ CHANGE */
		STG.DELIVERY_CHOICE,  		     /* Add new Column - INDUS */
		STG.MODIFICATION_REASON_CD, 	 /* Add new Column - INDUS */
		STG.MODIFICATION_REASON_CD_DESC, /* Add new Column - INDUS */
		STG.RETURN_ACTION_CD,
		STG.RETURN_ACTION,
		STG.RETURN_REASON_CD,
		STG.RETURN_REASON_DESC,
		STG.RETURN_SUB_REASON_CD,
		STG.SHIP_TO_FIRST_NAME,
		STG.SHIP_TO_MIDDLE_NAME,
		STG.SHIP_TO_LAST_NAME,
		STG.SHIP_TO_ADDRESS_LINE_1,
		STG.SHIP_TO_ADDRESS_LINE_2,
		STG.SHIP_TO_CITY,
		STG.SHIP_TO_STATE_OR_PROV,
		STG.SHIP_TO_POSTAL_CD,
		STG.SHIP_TO_COUNTRY,
		STG.SHIP_TO_EMAIL_ADDRESS,
		STG.SHIP_TO_PHONE_NBR,
		STG.DELIVERY_METHOD,                     /* Add new Column - SEPIK */
		STG.FULFILLMENT_TYPE,                             /* Add new Column - SEPIK */
		STG.DTC_SUBORDER_NBR,                     /* Add new Column - SEPIK */
		STG.ADDITIONAL_LINE_TYPE_CD,             /* Add new Column - SEPIK */
		STG.ORIG_CONFIRMED_QTY,                /* Add new Column - SEPIK */
		STG.RESKU_FLAG,                        /* Add new Column - SEPIK */
		STG.CONSOLIDATOR_ADDRESS_CD,
		STG.MERGE_NODE_CD,
		STG.SHIP_NODE_CD,
		STG.RECEIVING_NODE_CD,
		STG.LEVEL_OF_SERVICE,
		STG.CARRIER_SERVICE_CD,
		STG.CARRIER_CD,
        STG.ACCESS_POINT_CD,
        STG.ACCESS_POINT_ID,
        STG.ACCESS_POINT_NM,
		STG.MINIMUM_SHIP_DT,
		STG.REQUESTED_SHIP_DT,
		STG.EARLIEST_SCHEDULE_DT,
		STG.REQUESTED_DELIVERY_DT,
		STG.EARLIEST_DELIVERY_DT,
		STG.PROMISED_APPT_END_DT,
		/*STG.SALE_QTY,*/
		STG.SPLIT_QTY,
		STG.SHIPPED_QTY,
		STG.FILL_QTY,
		STG.WEIGHTED_AVG_COST,
		STG.DIRECT_SHIP_FLAG,
		STG.UNIT_COST,
		STG.LABOR_COST,
		STG.LABOR_SKU,
		STG.CUSTOMER_LEVEL_OF_SERVICE,       /* Add new Column - INDUS */
		STG.RETURN_POLICY,	                        /* Add new Column - SEPIK */
		STG.RETURN_POLICY_CHECK_OVERRIDE_FLAG,     /* Add new Column - SEPIK */
		STG.PRODUCT_AVAILABILITY_DT,               /* Add new Column - SEPIK */
		STG.ECDD_OVERRIDDEN_FLAG,                  /* Add new Column - SEPIK */
		STG.ECDD_INVOKED_FLAG,                     /* Add new Column - SEPIK */
		STG.VAS_GIFT_WRAP_CD,				       /* Add new Column - SEPIK */
		STG.VAS_MONO_FLAG,                        /* Add new Column - SEPIK */
		STG.VAS_PZ_FLAG,						/* Add new Column - SEPIK */
		STG.BO_NOTIFICATION_NBR,             /* Added by NILE for CYODD */
		STG.INSERT_TS,
		STG.UPDATE_TS
	);

/*
** Source View   : ORDER_HEADER_STG1_VIEW2
** Target View   :
** UNCACHE table ORDER_HEADER_STG1_VIEW2
**
*/

UNCACHE TABLE ORDER_HEADER_STG1_VIEW2;

/*
** Source View   : ORDER_STG1_VIEW2
** Target View   :
** UNCACHE table ORDER_STG1_VIEW2
**
*/

UNCACHE TABLE ORDER_STG1_VIEW2

/*
** Source Table   : L2_ANALYTICS_TABLES.ORDER_HEADER
** Target View    :
** Optimize delta data for current_date and retail records
**
*/

/* OPTIMIZE L2_ANALYTICS_TABLES.ORDER_HEADER WHERE ORDER_DT = CURRENT_DATE() AND SOURCE_SYSTEM IN ('NCR_POSLOG', 'REDIRON_POSLOG') ZORDER BY (LAST_EFFECTIVE_TS, TXN_TYPE_CD)   */

/*
** Source Table   : L2_ANALYTICS_TABLES.ORDER_LINE
** Target View    :
** Optimize delta data for current_date and retail records
**
*/

/* OPTIMIZE L2_ANALYTICS_TABLES.ORDER_LINE   WHERE ORDER_DT = CURRENT_DATE() AND SOURCE_SYSTEM IN ('NCR_POSLOG', 'REDIRON_POSLOG') ZORDER BY (LAST_EFFECTIVE_TS, ORDER_LINE_TYPE) */
