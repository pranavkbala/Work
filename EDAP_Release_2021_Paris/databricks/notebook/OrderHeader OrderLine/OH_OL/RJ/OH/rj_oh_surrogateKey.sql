/*
******************************************************************
******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      01/27/2021  DAN-5288  SQL file created
**  02    	05/21/2021  NLE-5664  Added Membership_level_cd column as part of 
								  adding to OH from Sterling OH	
**  03      07/23/2021  SUT-160   Added Membership_level_cd column as part of 
								  adding to OH from Sterling OH	

******************************************************************

** Author			    : Danube Team DAN-5288
** Create Date		: Jan, 2021
** Purpose      	: generate surrogate key for L2_ANALYTICS_TABLES.ORDER_HEADER

Source : L1 RJ Inbound file Dataframe
Target : L2_STAGE.RJ_ORDER_HEADER_STG

******************************************************************
******************************************************************
*/
SELECT
	CASE
		WHEN STG.ORDER_HEADER_KEY = 0 THEN
            DENSE_RANK() OVER (ORDER BY STG.ORDER_HEADER_ID) + COALESCE(MAX_ORDER_HEADER_KEY, 0)
		ELSE STG.ORDER_HEADER_KEY
	END AS ORDER_HEADER_KEY,
         STG.ORDER_HEADER_ID,
		 STG.SOURCE_SYSTEM,
         STG.ORDER_ID,
         STG.ORIG_ORDER_ID,
         STG.EXCHANGE_ORDER_ID,                      /* Added as part SEPIK changes */
		 STG.LOCATION_KEY,
		 STG.LOCATION_ID,
		 STG.HOUSEHOLD_KEY,                 /* Added as part SEPIK changes */
         STG.HOUSEHOLD_ID,                   /* Added as part SEPIK changes */
         STG.DTC_ENTRY_TYPE_CD,
         STG.TXN_TYPE_CD,
         STG.PAYMENT_STATUS_CD,           /* Added as part SEPIK changes */
         STG.CONCEPT_CD,
         STG.CURRENCY_CD,
         STG.LINKED_ORDER_HEADER_KEY,
         STG.LINKED_ORDER_HEADER_ID,
         STG.POS_WORKSTATION_ID,
		 STG.POS_WORKSTATION_SEQ_NBR,
         STG.RTC_LOCATION_KEY,
         STG.RTC_LOCATION_ID,
         STG.ORDER_DT,
         STG.LOYALTY_ACCOUNT_KEY,
         STG.LOYALTY_ACCOUNT_ID,
         STG.CUSTOMER_KEY,
         STG.CUSTOMER_ID,
         STG.EMPLOYEE_ID,             /* Added as part SEPIK changes */
         STG.MATCH_METHOD_CD,
         STG.FIRST_EFFECTIVE_TS,
	     STG.LAST_EFFECTIVE_TS,
         STG.BILL_TO_FIRST_NAME,
         STG.BILL_TO_MIDDLE_NAME,
         STG.BILL_TO_LAST_NAME,
         STG.BILL_TO_ADDRESS_LINE_1,
         STG.BILL_TO_ADDRESS_LINE_2,
         STG.BILL_TO_CITY,
         STG.BILL_TO_STATE_OR_PROV_CD,
         STG.BILL_TO_POSTAL_CD,
         STG.BILL_TO_COUNTRY,
         STG.BILL_TO_EMAIL_ADDRESS,
         STG.BILL_TO_PHONE_NBR,
         STG.TRADE_ID,
         STG.ORDER_TYPE_CD,
         STG.CUSTOMER_TYPE_CD,
	     STG.MEMBERSHIP_LEVEL_CD,  /* Added as part Nile changes.*/
         STG.SUBORDERS_CNT,  /* SUTLEJ CHANGE */
	     STG.MARKET_CD,    /* Added as part SEPIK changes */
         STG.STORE_ORDER_SOURCE,
         STG.OPERATOR_ID,
         STG.CANCEL_FLAG,
         STG.TRAINING_MODE_FLAG,
         STG.REGISTRY_ORDER_FLAG,        /* Added as part SEPIK changes */
         STG.DRAFT_ORDER_FLAG,           /* Added as part SEPIK changes */
         STG.ORDER_PURPOSE,              /* Added as part SEPIK changes */
         STG.SOURCE_CODE_DISCOUNT_AMT,      /* SUTLEJ CHANGE */										   
         STG.GIFTWRAP_WAIVED_FLAG,              /* SUTLEJ CHANGE */		   
         STG.SHIPPING_WAIVED_FLAG,              /* SUTLEJ CHANGE */										   
         STG.CATALOG_NM,                                  /* SUTLEJ CHANGE */							  
         STG.CATALOG_YEAR,                             /* SUTLEJ CHANGE */							   
         STG.REGISTRY_ID,                                /* SUTLEJ CHANGE */							  
         STG.ORDER_SOURCE_TYPE_CD,               /* SUTLEJ CHANGE */								   
         STG.GIFT_FLAG,                                    /* SUTLEJ CHANGE */							  
         STG.WAREHOUSE_SITE_CD,                    /* SUTLEJ CHANGE */								   
         STG.PAPER_FLAG,                                  /* SUTLEJ CHANGE */							  
         STG.STORE_ASSOCIATE_NM,                  /* SUTLEJ CHANGE */								   
         STG.TENTATIVE_REFUND_AMT,              /* SUTLEJ CHANGE */									   
         STG.CONTACT_FLAG,                              /* SUTLEJ CHANGE */								
         STG.RETURN_CARRIER,                          /* SUTLEJ CHANGE */							   
         STG.REGISTRY_TYPE_CD,                      /* SUTLEJ CHANGE */
         STG.TXN_BEGIN_TS,
         STG.TXN_END_TS,
         STG.RETURN_TYPE_CD,  /* Added as part SEPIK changes */
         STG.RETURN_DELIVERY_HUB,    /* Added as part SEPIK changes */
         STG.RETURN_MANAGING_HUB,  /* Added as part SEPIK changes */
         STG.RETURN_CARRIER_CD,  /* Added as part SEPIK changes */
         STG.RETURN_METHOD_CD,   /* Added as part SEPIK changes */
		 STG.MODIFIED_TS,
         STG.RECEIPT_PREFERENCE,
         STG.GROSS_AMT,
         STG.NET_AMT,
         STG.TAX_AMT,
         STG.DOCUMENT_TYPE_CD,  /* Added as part SEPIK changes */
         STG.REFUND_POLICY,    /* Added as part SEPIK changes */
         STG.INSERT_TS,
	     STG.UPDATE_TS
  FROM RJ_OH_VIEW3 STG
  CROSS JOIN
	 (
		 SELECT MAX(ORDER_HEADER_KEY) AS MAX_ORDER_HEADER_KEY FROM L2_ANALYTICS_TABLES.ORDER_HEADER WHERE ORDER_HEADER_KEY <> -1
	 ) TARGET_MAX
