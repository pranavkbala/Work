/*
******************************************************************
******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      01/25/2021  DAN-4647  SQL file created
**  02		06/08/2021 SEPIK      Added COLUMNS as part of adding to OL from Sterling OH
**  03      07/23/2021  SUT-160   Added columns to OL

******************************************************************

** Author			    : Danube Team DAN-4901
** Create Date		: Jan, 2021
** Purpose      	: generate surrogate key for L2_ANALYTICS_TABLES.ORDER_LINE_TEMP

Source : L1 RJ Inbound file Dataframe
Target : L2_STAGE.RJ_ORDER_LINE_STG

******************************************************************
******************************************************************
*/

     SELECT
	CASE
		WHEN STG.ORDER_LINE_KEY = 0 THEN
            ROW_NUMBER() OVER (ORDER BY STG.ORDER_LINE_ID, STG.SUB_LINE_SEQ_NBR) + COALESCE(MAX_ORDER_LINE_KEY, 0)
		ELSE STG.ORDER_LINE_KEY
	END AS ORDER_LINE_KEY,
  STG.ORDER_LINE_ID,
  STG.CHAINED_FROM_ORDER_HEADER_KEY,         /* Add new Column - SEPIK */
  STG.CHAINED_FROM_ORDER_HEADER_ID,         /* Add new Column - SEPIK */
  STG.CHAINED_FROM_ORDER_LINE_KEY,          /* Add new Column - SEPIK */
  STG.CHAINED_FROM_ORDER_LINE_ID,          /* Add new Column - SEPIK */
  STG.SOURCE_SYSTEM,
  STG.PRIME_LINE_SEQ_NBR,
  STG.SUB_LINE_SEQ_NBR,
  STG.ORDER_HEADER_KEY,
  STG.ORDER_DT,
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
  STG.DELIVERY_CHOICE,
  STG.MODIFICATION_REASON_CD,
  STG.MODIFICATION_REASON_CD_DESC,
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
  STG.REQUESTED_DELIVERY_DT,
  STG.EARLIEST_SCHEDULE_DT,
  STG.EARLIEST_DELIVERY_DT,
  STG.PROMISED_APPT_END_DT,
  STG.SPLIT_QTY,
  STG.SHIPPED_QTY,
  STG.FILL_QTY,
  STG.WEIGHTED_AVG_COST,
  STG.DIRECT_SHIP_FLAG,
  STG.UNIT_COST,
  STG.LABOR_COST,
  STG.LABOR_SKU,
  STG.CUSTOMER_LEVEL_OF_SERVICE,
  STG.RETURN_POLICY,	                        /* Add new Column - SEPIK */
  STG.RETURN_POLICY_CHECK_OVERRIDE_FLAG,     /* Add new Column - SEPIK */
  STG.PRODUCT_AVAILABILITY_DT,               /* Add new Column - SEPIK */
  STG.ECDD_OVERRIDDEN_FLAG,                  /* Add new Column - SEPIK */
  STG.ECDD_INVOKED_FLAG,                     /* Add new Column - SEPIK */
  STG.VAS_GIFT_WRAP_CD,				       /* Add new Column - SEPIK */
  STG.VAS_MONO_FLAG,                        /* Add new Column - SEPIK */
  STG.VAS_PZ_FLAG,						/* Add new Column - SEPIK */
  STG.BO_NOTIFICATION_NBR,              /* Add new Column - NILE */
  STG.INSERT_TS,
  STG.UPDATE_TS
   FROM RJ_OL_VIEW2 STG
   CROSS JOIN
  	(
  		SELECT MAX(ORDER_LINE_KEY) AS MAX_ORDER_LINE_KEY FROM L2_ANALYTICS_TABLES.ORDER_LINE WHERE ORDER_LINE_KEY <> -1
  	) TARGET_MAX
