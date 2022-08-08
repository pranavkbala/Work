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
**  03    	06/08/2021  SEPIK  Added  columns as part of
 								  adding to OH from Sterling OH
**  04      07/23/2021  SUT-160 Added  columns as part of
 								  adding to OH from Sterling OH
******************************************************************

** Author			    : Danube Team DAN-5288
** Create Date		: Jan, 2021
** Purpose      	: Look up on ORDER_HEADER

Note : For now we have hard coded CUSTOMER_ID & CUSTOMER_KEY as -1, which will be changed later, when we get data from amperity

Source : L1 RJ Inbound file Dataframe
Target : L2_STAGE.RJ_ORDER_HEADER_STG

******************************************************************
******************************************************************
*/
SELECT
		    COALESCE(OH.ORDER_HEADER_KEY, 0 ) AS ORDER_HEADER_KEY,
		    STG.salesid AS ORDER_HEADER_ID,
		    "RJ" AS SOURCE_SYSTEM,
        null AS ORDER_ID,
				STG.originalsalesid AS ORIG_ORDER_ID,
				NULL AS EXCHANGE_ORDER_ID,                      /* Added as part SEPIK changes */

		    -1 AS LOCATION_KEY,
				COALESCE(XREF.LOCATION_ID, '-1') AS LOCATION_ID,
				NULL AS HOUSEHOLD_KEY,                 /* Added as part SEPIK changes */
                NULL AS HOUSEHOLD_ID,                   /* Added as part SEPIK changes */
        null AS DTC_ENTRY_TYPE_CD,
				CASE
            WHEN STG.salesid LIKE 'RET%' THEN 'RETURN'
            ELSE 'SALE'
        END AS TXN_TYPE_CD,
        NULL AS PAYMENT_STATUS_CD,           /* Added as part SEPIK changes */
        'RJ' AS CONCEPT_CD,
        'USD' AS CURRENCY_CD,
        -1 AS LINKED_ORDER_HEADER_KEY,
        '-1' AS LINKED_ORDER_HEADER_ID,
        null AS POS_WORKSTATION_ID,
		    null AS POS_WORKSTATION_SEQ_NBR,
        -1 AS RTC_LOCATION_KEY,
        '-1' as RTC_LOCATION_ID,
        TO_DATE(STG.createddatetime) AS ORDER_DT,
        -1 AS LOYALTY_ACCOUNT_KEY,
        '-1' AS LOYALTY_ACCOUNT_ID,
				COALESCE(OH.CUSTOMER_KEY, -1 ) AS CUSTOMER_KEY,
        COALESCE(OH.CUSTOMER_ID, '-1' ) AS CUSTOMER_ID,
        NULL AS EMPLOYEE_ID,             /* Added as part SEPIK changes */
        null AS MATCH_METHOD_CD,
				STG.createddatetime AS CREATEDDATETIME,
        STG.modifieddatetime AS MODIFIEDDATETIME,
	      CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000" , 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
        STG.name AS BILL_TO_FIRST_NAME,
        null AS BILL_TO_MIDDLE_NAME,
        null AS BILL_TO_LAST_NAME,
        STG.jsstreet1 AS BILL_TO_ADDRESS_LINE_1,
        CONCAT(STG.jsstreet2," ",STG.jsstreet3) AS BILL_TO_ADDRESS_LINE_2,
        STG.city AS BILL_TO_CITY,
        STG.state AS BILL_TO_STATE_OR_PROV_CD,
        STG.zipcode AS BILL_TO_POSTAL_CD,
        STG.countryregionid AS BILL_TO_COUNTRY,
        STG.email AS BILL_TO_EMAIL_ADDRESS,
        null AS BILL_TO_PHONE_NBR,
        null AS TRADE_ID,
        null AS ORDER_TYPE_CD,
        STG.custgroup AS CUSTOMER_TYPE_CD,
	    null AS MEMBERSHIP_LEVEL_CD,  /* Added as part Nile changes.*/
        null as SUBORDERS_CNT, /* SUTLEJ CHANGE */
	    'USA' AS MARKET_CD,    /* Added as part SEPIK changes */
        null AS STORE_ORDER_SOURCE,
        null AS OPERATOR_ID,
        null AS CANCEL_FLAG,
        null AS TRAINING_MODE_FLAG,
        NULL AS REGISTRY_ORDER_FLAG,        /* Added as part SEPIK changes */
        NULL AS DRAFT_ORDER_FLAG,           /* Added as part SEPIK changes */
        NULL AS ORDER_PURPOSE,              /* Added as part SEPIK changes */
        NULL AS SOURCE_CODE_DISCOUNT_AMT,      /* SUTLEJ CHANGE */										   
        NULL AS GIFTWRAP_WAIVED_FLAG,              /* SUTLEJ CHANGE */		   
        NULL AS SHIPPING_WAIVED_FLAG,              /* SUTLEJ CHANGE */										   
        NULL AS CATALOG_NM,                                  /* SUTLEJ CHANGE */							  
        NULL AS CATALOG_YEAR,                             /* SUTLEJ CHANGE */							   
        NULL AS REGISTRY_ID,                                /* SUTLEJ CHANGE */							  
        NULL AS ORDER_SOURCE_TYPE_CD,               /* SUTLEJ CHANGE */								   
        NULL AS GIFT_FLAG,                                    /* SUTLEJ CHANGE */							  
        NULL AS WAREHOUSE_SITE_CD,                    /* SUTLEJ CHANGE */								   
        NULL AS PAPER_FLAG,                                  /* SUTLEJ CHANGE */							  
        NULL AS STORE_ASSOCIATE_NM,                  /* SUTLEJ CHANGE */								   
        NULL AS TENTATIVE_REFUND_AMT,              /* SUTLEJ CHANGE */									   
        NULL AS CONTACT_FLAG,                              /* SUTLEJ CHANGE */								
        NULL AS RETURN_CARRIER,                          /* SUTLEJ CHANGE */							   
        NULL AS REGISTRY_TYPE_CD,                      /* SUTLEJ CHANGE */
        STG.createddatetime AS TXN_BEGIN_TS,
				STG.createddatetime AS TXN_END_TS,
		NULL AS RETURN_TYPE_CD,  /* Added as part SEPIK changes */
		NULL AS RETURN_DELIVERY_HUB,    /* Added as part SEPIK changes */
		NULL AS RETURN_MANAGING_HUB,  /* Added as part SEPIK changes */
		NULL AS RETURN_CARRIER_CD,  /* Added as part SEPIK changes */
		NULL AS RETURN_METHOD_CD,   /* Added as part SEPIK changes */
        STG.modifieddatetime AS MODIFIED_TS,
        null AS RECEIPT_PREFERENCE,
        CAST(STG.total_sale AS DECIMAL(15,2)) AS GROSS_AMT,
        CAST(STG.net_sale AS DECIMAL(15,2)) AS NET_AMT,
        null AS TAX_AMT,
        NULL AS DOCUMENT_TYPE_CD,  /* Added as part SEPIK changes */
        NULL AS REFUND_POLICY,    /* Added as part SEPIK changes */
        CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
	      CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
	      FROM RJ_OH_VIEW1  STG
				LEFT OUTER JOIN L2_ANALYTICS_TABLES.BRAND_SPECIFIC_CHANNEL_XREF XREF
        ON STG.channel = XREF.CHANNEL_ID
        LEFT OUTER JOIN L2_ANALYTICS_TABLES.ORDER_HEADER OH
        ON STG.salesid = OH.ORDER_HEADER_ID
        AND OH.ORDER_DT = TO_DATE(STG.createddatetime)
        AND OH.SOURCE_SYSTEM = 'RJ'
        AND OH.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
