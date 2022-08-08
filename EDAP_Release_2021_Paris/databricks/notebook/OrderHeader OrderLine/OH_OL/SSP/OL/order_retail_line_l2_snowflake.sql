/*
******************************************************************
** Author         : Columbia - COL-4480
** Create Date    : January, 2021
** Purpose        : Ingest POSlog data into SnowFlake
******************************************************************
** Source Table   : "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."ORDER_RTL_LINE_STG"
** Target Table   : "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ORDER_LINE"
** Lookup         :
** Transformations  : Merge data from ORDER_RTL_LINE_STG to Order-Line in SnowFlake
******************************************************************
************************* Change History *************************
******************************************************************
** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      01/08/2021  COL-4480  Retail demand data in Snowflake
**  02      06/08/2021  NLE-5606  CYODD- BO_Notification_Nbr in Order_Line
**  03      06/08/2021  SEPIK     Addition of columns
******************************************************************
*/


MERGE INTO "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ORDER_LINE"  LINE USING 
 ( 
	SELECT STG1.ORDER_LINE_KEY AS MERGE_KEY, STG1.* FROM  "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."ORDER_RTL_LINE_STG" STG1 
	UNION 
	SELECT NULL AS MERGE_KEY, STG2.* FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."ORDER_RTL_LINE_STG" STG2 
 ) STG 
	ON STG.ORDER_DT = LINE.ORDER_DT 
	/* AND STG.SOURCE_SYSTEM = LINE.SOURCE_SYSTEM */
	AND LINE.SOURCE_SYSTEM IN ( 'REDIRON_POSLOG', 'NCR_POSLOG' ) 
	AND LINE.ORDER_LINE_KEY = STG.MERGE_KEY 
 WHEN MATCHED AND LINE.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000+0000' THEN 
	UPDATE SET 
		LAST_EFFECTIVE_TS = dateadd(SECOND, - 1, STG.FIRST_EFFECTIVE_TS)   , 
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
    	)
