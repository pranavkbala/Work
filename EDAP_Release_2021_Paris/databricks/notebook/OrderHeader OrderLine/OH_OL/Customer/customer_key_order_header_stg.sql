/*
********************************************************************************************

** Author         : Mekong - MEK-7126
** Create Date    : FEB, 2021
** Purpose        : Load the insert and update records into the ORDER_HEADER Table

********************************************************************************************

** Source Table   : L2_STAGE.CUSTOMER_ORDER_STG
                  
** Target Table   : L2_ANALYTICS_TABLES.ORDER_HEADER
** Lookup         :

*********************************************************************************************
************************* Change History ****************************************************
*********************************************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  -----------------------------------------------------------
**  01      04/20/2021  MEK-7126  SQL file created
**  02    	05/21/2021  NLE-5664  Added Membership_level_cd column as part of 
								  adding to OH from Sterling OH
**  03      07/23/2021  SUT-160   Adding new fields
**

*********************************************************************************************
*/
 CREATE OR REPLACE VIEW CUST_ORDER_HEADER_UPDATE_STG AS
 (SELECT A.ORDER_HEADER_KEY AS ORDER_HEADER_KEY,
 A.ORDER_HEADER_ID AS ORDER_HEADER_ID,
 A.SOURCE_SYSTEM AS SOURCE_SYSTEM,
 A.ORDER_ID AS ORDER_ID,
 A.ORIG_ORDER_ID AS ORIG_ORDER_ID,
 A.EXCHANGE_ORDER_ID AS EXCHANGE_ORDER_ID,            /* Added as part SEPIK changes */
 A.ORDER_TYPE_CD AS ORDER_TYPE_CD,              /* COLUMN ORDER CHANGE Added as part SEPIK changes */
 A.ORDER_DT AS ORDER_DT,                       /* COLUMN ORDER CHANGE Added as part SEPIK changes */
 A.LOCATION_KEY AS LOCATION_KEY,
 A.LOCATION_ID AS LOCATION_ID,
 A.HOUSEHOLD_KEY AS HOUSEHOLD_KEY,                 /* Added as part SEPIK changes */
 A.HOUSEHOLD_ID AS HOUSEHOLD_ID,                   /* Added as part SEPIK changes */
 A.DTC_ENTRY_TYPE_CD AS DTC_ENTRY_TYPE_CD,
 A.TXN_TYPE_CD AS TXN_TYPE_CD,
 A.PAYMENT_STATUS_CD AS PAYMENT_STATUS_CD,           /* Added as part SEPIK changes */
 A.CONCEPT_CD AS CONCEPT_CD,
 A.CURRENCY_CD AS CURRENCY_CD,
 A.LINKED_ORDER_HEADER_KEY AS LINKED_ORDER_HEADER_KEY,
 A.LINKED_ORDER_HEADER_ID AS LINKED_ORDER_HEADER_ID,
 A.POS_WORKSTATION_ID AS POS_WORKSTATION_ID,
 A.POS_WORKSTATION_SEQ_NBR AS POS_WORKSTATION_SEQ_NBR,
 A.RTC_LOCATION_KEY AS RTC_LOCATION_KEY,
 A.RTC_LOCATION_ID AS RTC_LOCATION_ID,
 A.LOYALTY_ACCOUNT_KEY AS LOYALTY_ACCOUNT_KEY,
 A.LOYALTY_ACCOUNT_ID AS LOYALTY_ACCOUNT_ID,
 B.CUSTOMER_KEY AS CUSTOMER_KEY,
 B.CUSTOMER_ID AS CUSTOMER_ID,
 A.EMPLOYEE_ID AS EMPLOYEE_ID,        /* Added as part SEPIK changes */
 A.MATCH_METHOD_CD AS MATCH_METHOD_CD,
 CURRENT_TIMESTAMP AS FIRST_EFFECTIVE_TS,
 '9999-12-31T23:59:59.000+0000' AS LAST_EFFECTIVE_TS,
 A.BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
 A.BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
 A.BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
 A.BILL_TO_ADDRESS_LINE_1 AS BILL_TO_ADDRESS_LINE_1,
 A.BILL_TO_ADDRESS_LINE_2 AS BILL_TO_ADDRESS_LINE_2,
 A.BILL_TO_CITY AS BILL_TO_CITY,
 A.BILL_TO_STATE_OR_PROV_CD AS BILL_TO_STATE_OR_PROV_CD,
 A.BILL_TO_POSTAL_CD AS BILL_TO_POSTAL_CD,
 A.BILL_TO_COUNTRY AS BILL_TO_COUNTRY,
 A.BILL_TO_EMAIL_ADDRESS AS BILL_TO_EMAIL_ADDRESS,
 A.BILL_TO_PHONE_NBR AS BILL_TO_PHONE_NBR,
 A.TRADE_ID AS TRADE_ID,
 A.CUSTOMER_TYPE_CD AS CUSTOMER_TYPE_CD,
 A.MEMBERSHIP_LEVEL_CD AS MEMBERSHIP_LEVEL_CD,  /* Added as part Nile changes.*/ 
 A.SUBORDERS_CNT AS SUBORDERS_CNT,     /* SUTLEJ CHANGE */
 A.MARKET_CD AS MARKET_CD,    /* Added as part SEPIK changes */
 A.STORE_ORDER_SOURCE AS STORE_ORDER_SOURCE,
 A.OPERATOR_ID AS OPERATOR_ID,
 A.CANCEL_FLAG AS CANCEL_FLAG,
 A.TRAINING_MODE_FLAG AS TRAINING_MODE_FLAG,
 A.REGISTRY_ORDER_FLAG AS REGISTRY_ORDER_FLAG,        /* Added as part SEPIK changes */
 A.DRAFT_ORDER_FLAG AS DRAFT_ORDER_FLAG,           /* Added as part SEPIK changes */
 A.ORDER_PURPOSE AS ORDER_PURPOSE,              /* Added as part SEPIK changes */
 A.SOURCE_CODE_DISCOUNT_AMT AS SOURCE_CODE_DISCOUNT_AMT,     /* SUTLEJ CHANGE */										   
 A.GIFTWRAP_WAIVED_FLAG AS GIFTWRAP_WAIVED_FLAG,             /* SUTLEJ CHANGE */		   
 A.SHIPPING_WAIVED_FLAG AS SHIPPING_WAIVED_FLAG,             /* SUTLEJ CHANGE */										   
 A.CATALOG_NM AS CATALOG_NM,                                 /* SUTLEJ CHANGE */							  
 A.CATALOG_YEAR AS CATALOG_YEAR,                             /* SUTLEJ CHANGE */							   
 A.REGISTRY_ID AS REGISTRY_ID,                               /* SUTLEJ CHANGE */							  
 A.ORDER_SOURCE_TYPE_CD AS ORDER_SOURCE_TYPE_CD,              /* SUTLEJ CHANGE */								   
 A.GIFT_FLAG AS GIFT_FLAG,                                   /* SUTLEJ CHANGE */							  
 A.WAREHOUSE_SITE_CD AS WAREHOUSE_SITE_CD,                   /* SUTLEJ CHANGE */								   
 A.PAPER_FLAG AS PAPER_FLAG,                                 /* SUTLEJ CHANGE */							  
 A.STORE_ASSOCIATE_NM AS STORE_ASSOCIATE_NM,                 /* SUTLEJ CHANGE */								   
 A.TENTATIVE_REFUND_AMT AS TENTATIVE_REFUND_AMT,             /* SUTLEJ CHANGE */									   
 A.CONTACT_FLAG AS CONTACT_FLAG,                             /* SUTLEJ CHANGE */								
 A.RETURN_CARRIER AS RETURN_CARRIER,                         /* SUTLEJ CHANGE */							   
 A.REGISTRY_TYPE_CD AS REGISTRY_TYPE_CD,                     /* SUTLEJ CHANGE */
 A.TXN_BEGIN_TS AS TXN_BEGIN_TS,
 A.TXN_END_TS AS TXN_END_TS,
 A.RETURN_TYPE_CD AS RETURN_TYPE_CD,             /* Added as part SEPIK changes */
 A.RETURN_DELIVERY_HUB AS RETURN_DELIVERY_HUB,    /* Added as part SEPIK changes */
 A.RETURN_MANAGING_HUB AS RETURN_MANAGING_HUB,  /* Added as part SEPIK changes */
 A.RETURN_CARRIER_CD AS RETURN_CARRIER_CD,  /* Added as part SEPIK changes */
 A.RETURN_METHOD_CD AS  RETURN_METHOD_CD,   /* Added as part SEPIK changes */
 A.RECEIPT_PREFERENCE AS RECEIPT_PREFERENCE,
 A.GROSS_AMT AS GROSS_AMT,
 A.NET_AMT AS NET_AMT,
 A.TAX_AMT AS TAX_AMT,
 A.DOCUMENT_TYPE_CD AS DOCUMENT_TYPE_CD,  /* Added as part SEPIK changes */
 A.REFUND_POLICY  AS REFUND_POLICY,    /* Added as part SEPIK changes */
 CURRENT_TIMESTAMP AS INSERT_TS,
 CURRENT_TIMESTAMP AS UPDATE_TS
 FROM L2_STAGE.CUSTOMER_ORDER_STG B
 INNER JOIN L2_ANALYTICS.ORDER_HEADER A
 ON A.ORDER_HEADER_ID = B.ORDER_HEADER_ID
 AND A.ORDER_HEADER_KEY = B.ORDER_HEADER_KEY
  WHERE B.INDICATOR IN ('U','I'))
