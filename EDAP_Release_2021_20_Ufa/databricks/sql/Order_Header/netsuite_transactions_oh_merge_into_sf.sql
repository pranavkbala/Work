  /*
******************************************************************

** Author					: JUNAID
** Team                     : Godavari team (Supplychain_Godavari@wsgc.com)
** Create Date		: JULY, 2021
** Purpose      	: Merging NETSUITE ITEM LOCATION MAP data from stage table into SnowFlake Global Inventory table
******************************************************************
******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author                 Description
**  --      ----------  --------             --------------------------------
**  01      27/07/2021  JUNAID       SQL file created
**

******************************************************************
*/

MERGE INTO "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."ORDER_HEADER" TARGET
 USING (
    SELECT STG.ORDER_HEADER_KEY as MERGE_KEY, STG.*
        FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."NETSUITE_OH_SF_STG" STG
    UNION
    SELECT NULL as MERGE_KEY, STG1.*
        FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."NETSUITE_OH_SF_STG" STG1) STG2 ON TARGET.ORDER_HEADER_KEY = STG2.MERGE_KEY
    WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000' THEN
        UPDATE SET TARGET.LAST_EFFECTIVE_TS = dateadd(second,-1,STG2.FIRST_EFFECTIVE_TS), TARGET.UPDATE_TS = STG2.UPDATE_TS
    WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
    INSERT
    ( ORDER_HEADER_KEY 
    , ORDER_HEADER_ID 
    , SOURCE_SYSTEM 
    , ORDER_ID 
    , ORIG_ORDER_ID 
    , EXCHANGE_ORDER_ID  
    , ORDER_TYPE_CD  
    , ORDER_DT          
    , LOCATION_KEY 
    , LOCATION_ID 
    , HOUSEHOLD_KEY      
    , HOUSEHOLD_ID   
    , GLOBAL_TXN_ID  
    , DTC_ENTRY_TYPE_CD 
    , TXN_TYPE_CD 
    , PAYMENT_STATUS_CD     
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
    , EMPLOYEE_ID 
    , WS_RESERVE_MEMBERSHIP_KEY 
    , WS_RESERVE_MEMBERSHIP_ID       
    , MATCH_METHOD_CD 
    , SUB_CHANNEL_KEY 
    , SUB_CHANNEL_CD 
    , SUB_CHANNEL_TYPE_CD 
    , FIRST_EFFECTIVE_TS 
    , LAST_EFFECTIVE_TS 
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
    , SUBORDERS_CNT 
    , MARKET_CD              
    , STORE_ORDER_SOURCE 
    , OPERATOR_ID 
    , CANCEL_FLAG 
    , TRAINING_MODE_FLAG 
    , REGISTRY_ORDER_FLAG         
    , DRAFT_ORDER_FLAG           
    , ORDER_PURPOSE
    , SOURCE_CODE_DISCOUNT_AMT  		   
    , GIFTWRAP_WAIVED_FLAG        
    , SHIPPING_WAIVED_FLAG      
    , CATALOG_NM                  
    , CATALOG_YEAR              
    , REGISTRY_ID                 
    , ORDER_SOURCE_TYPE_CD        
    , GIFT_FLAG                   
    , WAREHOUSE_SITE_CD           
    , PAPER_FLAG                  
    , STORE_ASSOCIATE_NM          
    , TENTATIVE_REFUND_AMT        
    , CONTACT_FLAG             
    , RETURN_CARRIER              
    , REGISTRY_TYPE_CD  
    , TXN_BEGIN_TS 
    , TXN_END_TS 
    , RETURN_TYPE_CD               
    , RETURN_DELIVERY_HUB          
    , RETURN_MANAGING_HUB          
    , RETURN_CARRIER_CD            
    , RETURN_METHOD_CD             
    , RECEIPT_PREFERENCE 
    , GROSS_AMT 
    , NET_AMT 
    , TAX_AMT 
    , DOCUMENT_TYPE_CD         
    , REFUND_POLICY  
    , GLOBAL_REPLACEMENT_ORDER_FLAG           
    , INSERT_TS 
    , UPDATE_TS )

    VALUES
    ( STG2.ORDER_HEADER_KEY  
    , STG2.ORDER_HEADER_ID 
    , STG2.SOURCE_SYSTEM 
    , STG2.ORDER_ID 
    , STG2.ORIG_ORDER_ID 
    , STG2.EXCHANGE_ORDER_ID  
    , STG2.ORDER_TYPE_CD  
    , STG2.ORDER_DT          
    , STG2.LOCATION_KEY 
    , STG2.LOCATION_ID 
    , STG2.HOUSEHOLD_KEY      
    , STG2.HOUSEHOLD_ID   
    , STG2.GLOBAL_TXN_ID  
    , STG2.DTC_ENTRY_TYPE_CD 
    , STG2.TXN_TYPE_CD 
    , STG2.PAYMENT_STATUS_CD     
    , STG2.CONCEPT_CD 
    , STG2.CURRENCY_CD 
    , STG2.LINKED_ORDER_HEADER_KEY 
    , STG2.LINKED_ORDER_HEADER_ID 
    , STG2.POS_WORKSTATION_ID 
    , STG2.POS_WORKSTATION_SEQ_NBR 
    , STG2.RTC_LOCATION_KEY 
    , STG2.RTC_LOCATION_ID 
    , STG2.LOYALTY_ACCOUNT_KEY 
    , STG2.LOYALTY_ACCOUNT_ID 
    , STG2.CUSTOMER_KEY 
    , STG2.CUSTOMER_ID 
    , STG2.EMPLOYEE_ID 
    , STG2.WS_RESERVE_MEMBERSHIP_KEY 
    , STG2.WS_RESERVE_MEMBERSHIP_ID       
    , STG2.MATCH_METHOD_CD 
    , STG2.SUB_CHANNEL_KEY 
    , STG2.SUB_CHANNEL_CD 
    , STG2.SUB_CHANNEL_TYPE_CD 
    , STG2.FIRST_EFFECTIVE_TS 
    , STG2.LAST_EFFECTIVE_TS 
    , STG2.BILL_TO_FIRST_NAME 
    , STG2.BILL_TO_MIDDLE_NAME 
    , STG2.BILL_TO_LAST_NAME 
    , STG2.BILL_TO_ADDRESS_LINE_1 
    , STG2.BILL_TO_ADDRESS_LINE_2 
    , STG2.BILL_TO_CITY 
    , STG2.BILL_TO_STATE_OR_PROV_CD 
    , STG2.BILL_TO_POSTAL_CD 
    , STG2.BILL_TO_COUNTRY 
    , STG2.BILL_TO_EMAIL_ADDRESS 
    , STG2.BILL_TO_PHONE_NBR 
    , STG2.TRADE_ID 
    , STG2.CUSTOMER_TYPE_CD 
    , STG2.MEMBERSHIP_LEVEL_CD 
    , STG2.SUBORDERS_CNT 
    , STG2.MARKET_CD              
    , STG2.STORE_ORDER_SOURCE 
    , STG2.OPERATOR_ID 
    , STG2.CANCEL_FLAG 
    , STG2.TRAINING_MODE_FLAG 
    , STG2.REGISTRY_ORDER_FLAG         
    , STG2.DRAFT_ORDER_FLAG           
    , STG2.ORDER_PURPOSE
    , STG2.SOURCE_CODE_DISCOUNT_AMT  		   
    , STG2.GIFTWRAP_WAIVED_FLAG        
    , STG2.SHIPPING_WAIVED_FLAG      
    , STG2.CATALOG_NM                  
    , STG2.CATALOG_YEAR              
    , STG2.REGISTRY_ID                 
    , STG2.ORDER_SOURCE_TYPE_CD        
    , STG2.GIFT_FLAG                   
    , STG2.WAREHOUSE_SITE_CD           
    , STG2.PAPER_FLAG                  
    , STG2.STORE_ASSOCIATE_NM          
    , STG2.TENTATIVE_REFUND_AMT        
    , STG2.CONTACT_FLAG             
    , STG2.RETURN_CARRIER              
    , STG2.REGISTRY_TYPE_CD  
    , STG2.TXN_BEGIN_TS 
    , STG2.TXN_END_TS 
    , STG2.RETURN_TYPE_CD               
    , STG2.RETURN_DELIVERY_HUB          
    , STG2.RETURN_MANAGING_HUB          
    , STG2.RETURN_CARRIER_CD            
    , STG2.RETURN_METHOD_CD             
    , STG2.RECEIPT_PREFERENCE 
    , STG2.GROSS_AMT 
    , STG2.NET_AMT 
    , STG2.TAX_AMT 
    , STG2.DOCUMENT_TYPE_CD         
    , STG2.REFUND_POLICY  
    , STG2.GLOBAL_REPLACEMENT_ORDER_FLAG           
    , STG2.INSERT_TS 
    , STG2.UPDATE_TS );

    TRUNCATE TABLE "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."NETSUITE_OH_SF_STG";