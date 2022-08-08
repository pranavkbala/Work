/*
******************************************************************
******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      01/27/2020  DAN-5288  SQL file created
**  02    	05/21/2021  NLE-5664  Added Membership_level_cd column as part of 
								  adding to OH from Sterling OH	
**  03      07/23/2021  SUT-160  Added Membership_level_cd column as part of 
								  adding to OH from Sterling OH

******************************************************************

** Author			    : Danube Team DAN-5288
** Create Date		: Jan, 2020
** Purpose      	: Ingest data from  stage to snowflake table

Source : "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."RJ_ORDER_HEADER_STG"
Target : "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ORDER_HEADER"

******************************************************************
******************************************************************
*/

    MERGE INTO "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ORDER_HEADER" TARGET USING
   (
      SELECT STG_1.ORDER_HEADER_KEY AS MERGE_KEY, STG_1.* FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."RJ_ORDER_HEADER_STG" STG_1
      UNION
      SELECT NULL AS MERGE_KEY, STG_2.* FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."RJ_ORDER_HEADER_STG" STG_2
   ) STG ON STG.ORDER_DT = TARGET.ORDER_DT
         AND TARGET.SOURCE_SYSTEM = 'RJ'
         AND TARGET.ORDER_HEADER_KEY = STG.MERGE_KEY
         AND STG.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
   WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000' THEN
      UPDATE SET
           LAST_EFFECTIVE_TS = dateadd(SECOND, - 1, STG.FIRST_EFFECTIVE_TS),
           UPDATE_TS = STG.UPDATE_TS
   WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
           	INSERT (
           		ORDER_HEADER_KEY,
           		ORDER_HEADER_ID,
           		SOURCE_SYSTEM,
           		ORDER_ID,
           		ORIG_ORDER_ID,
           		EXCHANGE_ORDER_ID,            /* Added as part SEPIK changes */
           		LOCATION_KEY,
           		LOCATION_ID,
           		HOUSEHOLD_KEY,                 /* Added as part SEPIK changes */
           		HOUSEHOLD_ID,                   /* Added as part SEPIK changes */
           		CONCEPT_CD,
           		DTC_ENTRY_TYPE_CD,
           		TXN_TYPE_CD,
           		PAYMENT_STATUS_CD,           /* Added as part SEPIK changes */
           		CURRENCY_CD,
           		LINKED_ORDER_HEADER_KEY,
           		LINKED_ORDER_HEADER_ID,
           		POS_WORKSTATION_ID,
           		POS_WORKSTATION_SEQ_NBR,
           		RTC_LOCATION_KEY,
           		RTC_LOCATION_ID,
           		ORDER_DT,
           		LOYALTY_ACCOUNT_KEY,
           		LOYALTY_ACCOUNT_ID,
           		CUSTOMER_KEY,
           		CUSTOMER_ID,
           		EMPLOYEE_ID,             /* Added as part SEPIK changes */
           		MATCH_METHOD_CD,
           		FIRST_EFFECTIVE_TS,
           		LAST_EFFECTIVE_TS,
           		BILL_TO_FIRST_NAME,
           		BILL_TO_MIDDLE_NAME,
           		BILL_TO_LAST_NAME,
           		BILL_TO_ADDRESS_LINE_1,
           		BILL_TO_ADDRESS_LINE_2,
           		BILL_TO_CITY,
           		BILL_TO_STATE_OR_PROV_CD,
           		BILL_TO_POSTAL_CD,
           		BILL_TO_COUNTRY,
           		BILL_TO_EMAIL_ADDRESS,
           		BILL_TO_PHONE_NBR,
           		TRADE_ID,
           		ORDER_TYPE_CD,
           		CUSTOMER_TYPE_CD,
                MEMBERSHIP_LEVEL_CD,  /* Added as part Nile changes  */
                SUBORDERS_CNT,    /* SUTLEJ CHANGE */
                MARKET_CD,    /* Added as part SEPIK changes */
           		STORE_ORDER_SOURCE,
           		OPERATOR_ID,
           		CANCEL_FLAG,
           		TRAINING_MODE_FLAG,
           		REGISTRY_ORDER_FLAG,        /* Added as part SEPIK changes */
           		DRAFT_ORDER_FLAG,           /* Added as part SEPIK changes */
           		ORDER_PURPOSE,              /* Added as part SEPIK changes */
                SOURCE_CODE_DISCOUNT_AMT,      /* SUTLEJ CHANGE */										   
                GIFTWRAP_WAIVED_FLAG,              /* SUTLEJ CHANGE */		   
                SHIPPING_WAIVED_FLAG,              /* SUTLEJ CHANGE */										   
                CATALOG_NM,                                  /* SUTLEJ CHANGE */							  
                CATALOG_YEAR,                             /* SUTLEJ CHANGE */							   
                REGISTRY_ID,                                /* SUTLEJ CHANGE */							  
                ORDER_SOURCE_TYPE_CD,               /* SUTLEJ CHANGE */								   
                GIFT_FLAG,                                    /* SUTLEJ CHANGE */							  
                WAREHOUSE_SITE_CD,                    /* SUTLEJ CHANGE */								   
                PAPER_FLAG,                                  /* SUTLEJ CHANGE */							  
                STORE_ASSOCIATE_NM,                  /* SUTLEJ CHANGE */								   
                TENTATIVE_REFUND_AMT,              /* SUTLEJ CHANGE */									   
                CONTACT_FLAG,                              /* SUTLEJ CHANGE */								
                RETURN_CARRIER,                          /* SUTLEJ CHANGE */							   
                REGISTRY_TYPE_CD,                      /* SUTLEJ CHANGE */
           		TXN_BEGIN_TS,
           		TXN_END_TS,
           		RETURN_TYPE_CD,  /* Added as part SEPIK changes */
           		RETURN_DELIVERY_HUB,    /* Added as part SEPIK changes */
           		RETURN_MANAGING_HUB,  /* Added as part SEPIK changes */
           		RETURN_CARRIER_CD,  /* Added as part SEPIK changes */
           		RETURN_METHOD_CD,   /* Added as part SEPIK changes */
           		RECEIPT_PREFERENCE,
           		GROSS_AMT,
           		NET_AMT,
           		TAX_AMT,
           		DOCUMENT_TYPE_CD,  /* Added as part SEPIK changes */
           		REFUND_POLICY,    /* Added as part SEPIK changes */
           		INSERT_TS,
           		UPDATE_TS
           	) VALUES (
           		STG.ORDER_HEADER_KEY,
           		STG.ORDER_HEADER_ID,
           		STG.SOURCE_SYSTEM,
           		STG.ORDER_ID,
           		STG.ORIG_ORDER_ID,
           		STG.EXCHANGE_ORDER_ID,            /* Added as part SEPIK changes */
           		STG.LOCATION_KEY,
           		STG.LOCATION_ID,
           		STG.HOUSEHOLD_KEY,                 /* Added as part SEPIK changes */
           		STG.HOUSEHOLD_ID,                   /* Added as part SEPIK changes */
           		STG.CONCEPT_CD,
           		STG.DTC_ENTRY_TYPE_CD,
           		STG.TXN_TYPE_CD,
           		STG.PAYMENT_STATUS_CD,           /* Added as part SEPIK changes */
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
           		STG.MEMBERSHIP_LEVEL_CD,  /* Added as part Nile changes  */
                STG.SUBORDERS_CNT,  /* SUTLEJ CHANGE */
           		STG.MARKET_CD,        /* Added as part SEPIK changes */
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
           		STG.RECEIPT_PREFERENCE,
           		STG.GROSS_AMT,
           		STG.NET_AMT,
           		STG.TAX_AMT,
           		STG.DOCUMENT_TYPE_CD,  /* Added as part SEPIK changes */
           		STG.REFUND_POLICY,    /* Added as part SEPIK changes */
           		STG.INSERT_TS,
           		STG.UPDATE_TS
           	)
