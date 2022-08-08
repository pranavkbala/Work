/*
******************************************************************

******************************************************************
************************* Look Up *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      04/10/2020  JAHMED1@WSGC.COM    SQL file created

******************************************************************

** Author			: JUNAID
** Team             : Godavari team (Supplychain_Godavari@wsgc.com)
** Create Date		: AUGUST, 2021
** Purpose      	: Apply mapping rules and generate necessary
					          columns for L2_ANALYTICS_TABLES.ORDER_HEADER table

******************************************************************

** Source Table 	: L2_STAGE.ORDER_HEADER_STG_TEMP
** Target View  	: OH_BACKFILL
** Lookup       	: L2_ANALYTICS_TABLES.LOCATIONS, SHIP_NODE, SUB_CHANNEL, CURRENCY

** Transformations	:
					: column concatenation and date conversion
					: static columns value insertions

******************************************************************
*/

SET spark.sql.broadcastTimeout = 2400;

CREATE OR REPLACE TEMPORARY VIEW OH_STG_VIEW1 AS ( 
SELECT 
-1 AS ORDER_HEADER_KEY,
STRING(INT(STG.TRANSACTION_ID)) AS ORDER_HEADER_ID, 
'GLOBAL_NETSUITE' AS SOURCE_SYSTEM,
STRING(INT(STG.TRANSACTION_ID)) AS ORDER_ID,
DATE_FORMAT(CAST(UNIX_TIMESTAMP(  STG.TRANDATE,"yyyyMMdd") AS TIMESTAMP),"yyyy-MM-dd") AS ORDER_DT,
COALESCE(L.LOCATION_KEY,-1) AS LOCATION_KEY,
TRIM(CAST(CAST(COALESCE(STG.LOCATION_ID, '-1') AS INT)AS STRING)) AS LOCATION_ID,
STRING(INT(STG.TRANID)) AS GLOBAL_TXN_ID,
TRIM(STRING(COALESCE(STG.TRANSACTION_TYPE, '-1'))) AS TXN_TYPE_CD,
TRIM(STRING(STG.STATUS)) AS PAYMENT_STATUS_CD,
TRIM(STRING(COALESCE(STG.COST_CENTER_BRAND, '-1'))) AS CONCEPT_CD,
TRIM(STRING(COALESCE(C.CURRENCY_CD, '-1'))) AS CURRENCY_CD,
TRIM(STRING(STG.REGISTER_NO_)) AS POS_WORKSTATION_ID,
COALESCE(S.SUB_CHANNEL_KEY, -1) as SUB_CHANNEL_KEY,
TRIM(STRING(S.SUB_CHANNEL_DESC)) AS SUB_CHANNEL_CD,
TRIM(STRING(S.SUB_CHANNEL_TYPE_CD)) AS SUB_CHANNEL_TYPE_CD,
CAST(STG.DATE_LAST_MODIFIED AS TIMESTAMP) AS FIRST_EFFECTIVE_TS,
CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000+0000" , "yyyy-MM-dd HH:mm:ss.SSS") AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
CAST(STG.TRANDATE AS TIMESTAMP) AS TXN_BEGIN_TS,
CAST(STG.TRANDATE AS TIMESTAMP) AS TXN_END_TS,
STG.IS_REPLACEMENT_ORDER AS GLOBAL_REPLACEMENT_ORDER_FLAG,
CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS 
  FROM L2_STAGE.ORDER_HEADER_STG_TEMP STG
  
    LEFT OUTER JOIN (SELECT DISTINCT MAX(SUB_CHANNEL_KEY) AS SUB_CHANNEL_KEY, SUB_CHANNEL_TYPE_ID, SUB_CHANNEL_TYPE_CD, SUB_CHANNEL_DESC FROM L2_ANALYTICS_TABLES.SUB_CHANNEL GROUP BY SUB_CHANNEL_TYPE_ID, SUB_CHANNEL_TYPE_CD, SUB_CHANNEL_DESC)S 
        ON S.SUB_CHANNEL_TYPE_ID=CAST(STG.CHANNEL_ID AS INT) 
      
    LEFT OUTER JOIN (select source_location_id as LOCATION_ID,LOCATION_KEY from l2_analytics_tables.location 
        where source_system='GLOBAL_NETSUITE' and last_effective_ts='9999-12-31T23:59:59.000+0000'  
            UNION 
                     select trim(regexp_replace(SHIP_NODE_ID,'GLOBAL_NETSUITE','')) as LOCATION_ID,SHIP_NODE_KEY as LOCATION_KEY from l2_analytics_tables.ship_node 
        where source_system='GLOBAL_NETSUITE' and last_effective_ts='9999-12-31T23:59:59.000+0000') L  
        ON CAST(CAST(STG.LOCATION_ID AS INT)AS STRING)= L.LOCATION_ID 
        
    LEFT OUTER JOIN L2_ANALYTICS_TABLES.CURRENCY C  
        ON CAST(STG.CURRENCY_ID AS INT) = C.GLOBAL_CURRENCY_ID
);


/*
******************************************************************
** Author					: JAHMED1@WSGC.COM
** Create Date		: JULY, 2021
** Purpose      	: CDC for NETSUITE'S TRANSACTIONS data for ORDER_HEADER
******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author                 Description
**  --      ----------  --------             --------------------------------
**  01      24/07/2021  JAHMED1@WSGC.COM       SQL file created
**
******************************************************************
*/


CREATE OR REPLACE TEMPORARY VIEW OH_STG1_VIEW2 AS ( 
select COALESCE(T.ORDER_HEADER_KEY,-1) as ORDER_HEADER_KEY,
    STG1.ORDER_HEADER_ID,
    STG1.SOURCE_SYSTEM,
    STG1.ORDER_ID,
    STG1.ORDER_DT,
    STG1.LOCATION_KEY,
    STG1.LOCATION_ID,
    STG1.GLOBAL_TXN_ID,
    STG1.TXN_TYPE_CD,
    STG1.PAYMENT_STATUS_CD,
    STG1.CONCEPT_CD,
    STG1.CURRENCY_CD,
    STG1.POS_WORKSTATION_ID,
    STG1.SUB_CHANNEL_KEY,
    STG1.SUB_CHANNEL_CD,
    STG1.SUB_CHANNEL_TYPE_CD,
    STG1.FIRST_EFFECTIVE_TS,
    STG1.LAST_EFFECTIVE_TS,
    STG1.TXN_BEGIN_TS,
    STG1.TXN_END_TS,
    STG1.GLOBAL_REPLACEMENT_ORDER_FLAG,
    STG1.INSERT_TS,
    STG1.UPDATE_TS
    FROM  OH_STG_VIEW1 STG1 
 LEFT OUTER JOIN L2_ANALYTICS_TABLES.ORDER_HEADER T
	ON STG1.ORDER_HEADER_ID = T.ORDER_HEADER_ID AND T.SOURCE_SYSTEM='GLOBAL_NETSUITE' AND T.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000' 
WHERE
	  T.ORDER_HEADER_KEY is null
      OR COALESCE(STG1.ORDER_DT,'') <> COALESCE(T.ORDER_DT,'')
      OR COALESCE(STG1.LOCATION_ID,'-1') <> COALESCE(T.LOCATION_ID,'-1')  
      OR COALESCE(STG1.GLOBAL_TXN_ID,'') <> COALESCE(T.GLOBAL_TXN_ID,'')
      OR COALESCE(STG1.TXN_TYPE_CD,'-1') <> COALESCE(T.TXN_TYPE_CD,'-1')
      OR COALESCE(STG1.PAYMENT_STATUS_CD,'') <> COALESCE(T.PAYMENT_STATUS_CD,'')  
      OR COALESCE(STG1.CONCEPT_CD,'-1') <> COALESCE(T.CONCEPT_CD,'-1')
      OR COALESCE(STG1.CURRENCY_CD,'-1') <> COALESCE(T.CURRENCY_CD,'-1')
      OR COALESCE(STG1.POS_WORKSTATION_ID,'') <> COALESCE(T.POS_WORKSTATION_ID,'')
      OR COALESCE(STG1.SUB_CHANNEL_CD,'') <> COALESCE(T.SUB_CHANNEL_CD,'')
      OR COALESCE(STG1.GLOBAL_REPLACEMENT_ORDER_FLAG,'') <> COALESCE(T.GLOBAL_REPLACEMENT_ORDER_FLAG,'')
);


/*
******************************************************************
** Author					: JAHMED1@WSGC.COM
** Create Date		: JULY, 2021
** Purpose      	: Generate surrogate keys for GLOBAL INVENTORY data 
******************************************************************
** Transformations	: Genarating surrogate keys on ORDER_HEADER_KEY, ORDER_HEADER_ID
******************************************************************
************************* Change History *************************
******************************************************************
** Change   Date        Author                 Description
**  --      ----------  --------             --------------------------------
**  01      24/07/2021  JAHMED1@WSGC.COM       SQL file created
******************************************************************
*/

CREATE OR REPLACE TEMPORARY VIEW OH_STG1_VIEW3 AS (
  SELECT
    CASE WHEN ORDER_HEADER_KEY = -1 THEN
             ROW_NUMBER() OVER (ORDER BY ORDER_HEADER_KEY, ORDER_HEADER_ID) + COALESCE(MAX_ORDER_HEADER_KEY, 0) 
        ELSE ORDER_HEADER_KEY
    END AS ORDER_HEADER_KEY,
    ORDER_HEADER_ID,
    SOURCE_SYSTEM,
    ORDER_ID,
    ORDER_DT,
    LOCATION_KEY,
    LOCATION_ID,
    GLOBAL_TXN_ID,
    TXN_TYPE_CD,
    PAYMENT_STATUS_CD,
    CONCEPT_CD,
    CURRENCY_CD,
    POS_WORKSTATION_ID,
    SUB_CHANNEL_KEY,
    SUB_CHANNEL_CD,
    SUB_CHANNEL_TYPE_CD,
    FIRST_EFFECTIVE_TS,
    LAST_EFFECTIVE_TS,
    TXN_BEGIN_TS,
    TXN_END_TS,
    GLOBAL_REPLACEMENT_ORDER_FLAG,
    INSERT_TS,
    UPDATE_TS
  FROM OH_STG1_VIEW2
  CROSS JOIN
  (SELECT MAX(ORDER_HEADER_KEY) AS MAX_ORDER_HEADER_KEY FROM L2_ANALYTICS_TABLES.ORDER_HEADER WHERE ORDER_HEADER_KEY <> -1) TGT_MAX
);


/*
******************************************************************
** Author					: JAHMED1@WSGC.COM
** Create Date		: JULY, 2021
** Purpose      	: Creating the backfill view
******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author                 Description
**  --      ----------  --------             --------------------------------
**  01      27/07/2021  JAHMED1@WSGC.COM       SQL file created
**
******************************************************************
*/


CREATE OR REPLACE TEMPORARY VIEW OH_BACKFILL AS (
SELECT 
  ORDER_HEADER_KEY
, ORDER_HEADER_ID
, SOURCE_SYSTEM
, ORDER_ID
, NULL  AS ORIG_ORDER_ID
, NULL  AS EXCHANGE_ORDER_ID
, NULL  AS ORDER_TYPE_CD
, ORDER_DT
, COALESCE(LOCATION_KEY, -1) AS LOCATION_KEY
, COALESCE(LOCATION_ID, '-1') AS LOCATION_ID
, -1 AS HOUSEHOLD_KEY
, NULL  AS HOUSEHOLD_ID
, GLOBAL_TXN_ID
, NULL  AS DTC_ENTRY_TYPE_CD
, COALESCE(TXN_TYPE_CD, '-1') AS TXN_TYPE_CD
, PAYMENT_STATUS_CD
, COALESCE(CONCEPT_CD, '-1') AS CONCEPT_CD
, COALESCE(CURRENCY_CD, '-1') AS CURRENCY_CD
, -1 AS  LINKED_ORDER_HEADER_KEY
, NULL  AS LINKED_ORDER_HEADER_ID
, POS_WORKSTATION_ID
, NULL  AS POS_WORKSTATION_SEQ_NBR
, -1 AS RTC_LOCATION_KEY
, NULL  AS RTC_LOCATION_ID
, -1 AS LOYALTY_ACCOUNT_KEY
, NULL  AS LOYALTY_ACCOUNT_ID
, -1 AS CUSTOMER_KEY
, NULL  AS CUSTOMER_ID
, NULL  AS EMPLOYEE_ID
, -1 as WS_RESERVE_MEMBERSHIP_KEY
, NULL as WS_RESERVE_MEMBERSHIP_ID       
, NULL  AS MATCH_METHOD_CD
, SUB_CHANNEL_KEY
, SUB_CHANNEL_CD
, SUB_CHANNEL_TYPE_CD
, FIRST_EFFECTIVE_TS
, LAST_EFFECTIVE_TS
, NULL  AS BILL_TO_FIRST_NAME
, NULL  AS BILL_TO_MIDDLE_NAME
, NULL AS BILL_TO_LAST_NAME
, NULL AS BILL_TO_ADDRESS_LINE_1
, NULL AS BILL_TO_ADDRESS_LINE_2
, NULL AS BILL_TO_CITY
, NULL AS BILL_TO_STATE_OR_PROV_CD
, NULL AS BILL_TO_POSTAL_CD
, NULL AS BILL_TO_COUNTRY
, NULL AS BILL_TO_EMAIL_ADDRESS
, NULL AS BILL_TO_PHONE_NBR
, NULL AS TRADE_ID
, NULL AS CUSTOMER_TYPE_CD
, NULL AS MEMBERSHIP_LEVEL_CD
, NULL AS SUBORDERS_CNT
, NULL AS MARKET_CD
, NULL AS STORE_ORDER_SOURCE
, NULL AS OPERATOR_ID
, NULL AS CANCEL_FLAG
, NULL AS TRAINING_MODE_FLAG
, NULL AS REGISTRY_ORDER_FLAG
, NULL AS DRAFT_ORDER_FLAG
, NULL AS ORDER_PURPOSE
, NULL AS SOURCE_CODE_DISCOUNT_AMT
, NULL AS GIFTWRAP_WAIVED_FLAG
, NULL AS SHIPPING_WAIVED_FLAG
, NULL AS CATALOG_NM
, NULL AS CATALOG_YEAR
, NULL AS REGISTRY_ID
, NULL AS ORDER_SOURCE_TYPE_CD
, NULL AS GIFT_FLAG
, NULL AS WAREHOUSE_SITE_CD
, NULL AS PAPER_FLAG
, NULL AS STORE_ASSOCIATE_NM
, NULL AS TENTATIVE_REFUND_AMT
, NULL AS CONTACT_FLAG
, NULL AS RETURN_CARRIER
, NULL AS REGISTRY_TYPE_CD
, TXN_BEGIN_TS
, TXN_END_TS
, NULL AS RETURN_TYPE_CD
, NULL AS RETURN_DELIVERY_HUB
, NULL AS RETURN_MANAGING_HUB
, NULL AS RETURN_CARRIER_CD
, NULL AS RETURN_METHOD_CD
, NULL AS RECEIPT_PREFERENCE
, NULL AS GROSS_AMT
, NULL AS NET_AMT
, NULL AS TAX_AMT
, NULL AS DOCUMENT_TYPE_CD
, NULL AS REFUND_POLICY
, GLOBAL_REPLACEMENT_ORDER_FLAG        
, INSERT_TS 
, UPDATE_TS 
FROM OH_STG1_VIEW3
); 


         /* TRUNCATING THE STAGE TABLE AND REINSERTING THE DATA FROM OH_BACKFILL  */


         TRUNCATE TABLE L2_STAGE.NETSUITE_OH_STG; 
         INSERT INTO L2_STAGE.NETSUITE_OH_STG SELECT * FROM OH_BACKFILL; 
       /*
       ONE_TIME_INSERT ACTIVITY
       */

/*
******************************************************************
** Author					: JAHMED1@WSGC.COM
** Create Date		: JULY, 2021
** Purpose      	: Merging GLOBAL INVENTORY data from stage table into L2 Global Inventory table
******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author                 Description
**  --      ----------  --------             --------------------------------
**  01      27/07/2021  JAHMED1@WSGC.COM       SQL file created
**
******************************************************************
*/

  MERGE INTO L2_ANALYTICS_TABLES.ORDER_HEADER TARGET
 USING (
    SELECT STG.ORDER_HEADER_KEY as MERGE_KEY, STG.*
        FROM L2_STAGE.NETSUITE_OH_STG STG
    UNION
    SELECT NULL as MERGE_KEY, STG1.*
        FROM  L2_STAGE.NETSUITE_OH_STG STG1) STG2 ON TARGET.ORDER_HEADER_KEY = STG2.MERGE_KEY
    WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000' THEN
        UPDATE SET TARGET.LAST_EFFECTIVE_TS = FROM_UNIXTIME (CAST(STG2.FIRST_EFFECTIVE_TS AS LONG) - 1), TARGET.UPDATE_TS = STG2.UPDATE_TS
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



    /*
    ** Source Table   : L2_ANALYTICS_TABLES.ORDER_HEADER
    ** Target Table    : L2_ANALYTICS_TABLES.ORDER_HEADER
    ** Optimize delta data for  LAST_EFFECTIVE_TS
    **
    */
    set spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled =false;

        OPTIMIZE L2_ANALYTICS_TABLES.ORDER_HEADER ZORDER BY LAST_EFFECTIVE_TS  ;