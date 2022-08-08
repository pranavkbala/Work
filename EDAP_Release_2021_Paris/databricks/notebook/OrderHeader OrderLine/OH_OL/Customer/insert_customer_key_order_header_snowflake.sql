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
**  02      07/23/2021  SUT-160   Added new fields

*********************************************************************************************
*/
 INSERT INTO "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ORDER_HEADER"
  (SELECT
  ORDER_HEADER_KEY
  , ORDER_HEADER_ID
  , SOURCE_SYSTEM
  , ORDER_ID
  , ORIG_ORDER_ID
  , EXCHANGE_ORDER_ID                              /* Added as part SEPIK changes */
  , ORDER_TYPE_CD                            /* COLUMN ORDER CHANGE Added as part SEPIK changes */
  , ORDER_DT                                /* COLUMN ORDER CHANGE Added as part SEPIK changes */
  , LOCATION_KEY
  , LOCATION_ID
  , HOUSEHOLD_KEY                        /* Added as part SEPIK changes */
  , HOUSEHOLD_ID                         /* Added as part SEPIK changes */
  , DTC_ENTRY_TYPE_CD
  , TXN_TYPE_CD
  , PAYMENT_STATUS_CD                       /* Added as part SEPIK changes */
  , CONCEPT_CD
  , CURRENCY_CD
  , LINKED_ORDER_HEADER_KEY
  , LINKED_ORDER_HEADER_ID
  , POS_WORKSTATION_ID
  , POS_WORKSTATION_SEQ_NBR
  , RTC_LOCATION_KEY
  , RTC_LOCATION_ID
  , LOYALTY_ACCOUNT_KEY
  , LOYALTY_ACCOUNT_ID
  , CUSTOMER_KEY
  , CUSTOMER_ID
  , EMPLOYEE_ID                                   /* Added as part SEPIK changes */
  , MATCH_METHOD_CD
  , FIRST_EFFECTIVE_TS
  , to_timestamp(concat(substr(LAST_EFFECTIVE_TS,1,10),' ',substr(LAST_EFFECTIVE_TS,12,12)))
  , BILL_TO_FIRST_NAME
  , BILL_TO_MIDDLE_NAME
  , BILL_TO_LAST_NAME
  , BILL_TO_ADDRESS_LINE_1
  , BILL_TO_ADDRESS_LINE_2
  , BILL_TO_CITY
  , BILL_TO_STATE_OR_PROV_CD
  , BILL_TO_POSTAL_CD
  , BILL_TO_COUNTRY
  , BILL_TO_EMAIL_ADDRESS
  , BILL_TO_PHONE_NBR
  , TRADE_ID
  , CUSTOMER_TYPE_CD
  , MEMBERSHIP_LEVEL_CD
  , SUBORDERS_CNT           /* SUTLEJ CHANGE */
  , MARKET_CD                          /* Added as part SEPIK changes */
  , STORE_ORDER_SOURCE
  , OPERATOR_ID
  , CANCEL_FLAG
  , TRAINING_MODE_FLAG
  , REGISTRY_ORDER_FLAG                                  /* Added as part SEPIK changes */
  , DRAFT_ORDER_FLAG                                  /* Added as part SEPIK changes */
  , ORDER_PURPOSE                                  /* Added as part SEPIK changes */
  , SOURCE_CODE_DISCOUNT_AMT      /* SUTLEJ CHANGE */										   
  , GIFTWRAP_WAIVED_FLAG              /* SUTLEJ CHANGE */		   
  , SHIPPING_WAIVED_FLAG              /* SUTLEJ CHANGE */										   
  , CATALOG_NM                                  /* SUTLEJ CHANGE */							  
  , CATALOG_YEAR                             /* SUTLEJ CHANGE */							   
  , REGISTRY_ID                                /* SUTLEJ CHANGE */							  
  , ORDER_SOURCE_TYPE_CD               /* SUTLEJ CHANGE */								   
  , GIFT_FLAG                                    /* SUTLEJ CHANGE */							  
  , WAREHOUSE_SITE_CD                    /* SUTLEJ CHANGE */								   
  , PAPER_FLAG                                  /* SUTLEJ CHANGE */							  
  , STORE_ASSOCIATE_NM                  /* SUTLEJ CHANGE */								   
  , TENTATIVE_REFUND_AMT              /* SUTLEJ CHANGE */									   
  , CONTACT_FLAG                              /* SUTLEJ CHANGE */								
  , RETURN_CARRIER                          /* SUTLEJ CHANGE */							   
  , REGISTRY_TYPE_CD                      /* SUTLEJ CHANGE */
  , TXN_BEGIN_TS
  , TXN_END_TS
  , RETURN_TYPE_CD                                    /* Added as part SEPIK changes */
  , RETURN_DELIVERY_HUB                                /* Added as part SEPIK changes */
  , RETURN_MANAGING_HUB                              /* Added as part SEPIK changes */
  , RETURN_CARRIER_CD                              /*   Added as part SEPIK changes */
  , RETURN_METHOD_CD                                /*  Added as part SEPIK changes */
  , RECEIPT_PREFERENCE
  , GROSS_AMT
  , NET_AMT
  , TAX_AMT
  , DOCUMENT_TYPE_CD                                   /* Added as part SEPIK changes */
  , REFUND_POLICY                                       /* Added as part SEPIK changes */
  , INSERT_TS
  , UPDATE_TS
 FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."CUSTOMER_ORDER_HEADER_STG")
