/*
******************************************************************

******************************************************************
************************* Look Up *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      04/10/2020  KIRAN  SQL file created

******************************************************************


******************************************************************
*/

SET spark.sql.broadcastTimeout = 2400;

CREATE OR REPLACE TEMPORARY VIEW INVOICE_STG_VIEW1 AS (
 SELECT
	  0 AS INVOICE_KEY,
     CAST(CAST(STG.TRANSACTION_ID AS INT)AS STRING)  AS INVOICE_ID,
	  'GLOBAL_NETSUITE' AS SOURCE_SYSTEM,
	  CAST(CAST(STG.TRANSACTION_ID AS INT)AS STRING) AS SOURCE_INVOICE_ID,
    S.SUB_CHANNEL_DESC AS SUB_CHANNEL_CD,
    STG.COST_CENTER_BRAND AS CONCEPT_CD,
    COALESCE(L.LOCATION_KEY,-1) as LOCATION_KEY ,
    CAST(CAST(STG.LOCATION_ID AS INT)AS STRING) AS LOCATION_ID,
	  COALESCE(D.DAY_KEY, -1) AS DAY_KEY,
	  DATE_FORMAT(CAST(UNIX_TIMESTAMP(STG.TRANDATE,"yyyyMMdd") AS TIMESTAMP),"yyyy-MM-dd") AS INVOICE_DT,
	  DATE_FORMAT(STG.TRANDATE,'yyyy-MM-dd HH:mm') as INVOICE_CREATE_TS,
    -1 AS ORDER_HEADER_KEY,
    '-1' AS ORDER_HEADER_ID,
    '-1' AS ORDER_ID,
    TRIM(TRANID) AS GLOBAL_TXN_ID,
	  CAST(DATE_LAST_MODIFIED AS TIMESTAMP) AS FIRST_EFFECTIVE_TS,
	  CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000+0000" , "yyyy-MM-dd HH:mm:ss.SSS") AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
	 TRIM(STG.REGISTER_NO_) AS POS_WORKSTATION_ID,
     CAST(NULL AS INT) AS POS_WORKSTATION_SEQ_NBR,
     CAST(CAST(STG.WSSPL_PURCHASE_ORDER_ID AS INT) AS STRING) AS WSSPL_PURCHASE_ORDER_ID,
     CAST(NULL AS TIMESTAMP) AS NOSALE_OVERRIDE_DT,
	  NULL AS  OVERRIDE_REASON_ID,
    NULL AS   CASHIER_ID,
    NULL AS   ASSOCIATE_ID,
    NULL AS  IS_ASSOCIATE_FLAG,
    NULL AS  EMPLOYEE_ID,
    STG.IS_REPLACEMENT_ORDER AS REPLACEMENT_ORDER_FLAG,
    STG.INTERNAL AS INTERNAL_FLAG,
    STG.TRANSACTION_TYPE AS DTC_INVOICE_TYPE_CD,
    NULL AS DTC_INVOICE_FULFILLMENT_TYPE_CD,
    NULL AS DTC_INVOICE_REVISED_TYPE_CD,
    NULL AS DTC_TXN_TYPE_CD,
    STG.STATUS AS DTC_STATUS_CD,
    NULL AS DTC_REFERENCE_1,
    CAST(NULL AS DECIMAL(15,2)) AS DTC_COLLECTED_AMT,
    CAST (0 AS INT )AS TOTAL_QTY,
    CAST (0 AS DECIMAL(15,2)) AS TOTAL_AMT,
    C.CURRENCY_CD,
    CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
	  CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS  
    FROM L2_STAGE.NETSUITE_INVOICE_STG_TEMP STG 
    LEFT OUTER JOIN (SELECT DISTINCT SUB_CHANNEL_TYPE_ID,SUB_CHANNEL_DESC FROM L2_ANALYTICS_TABLES.SUB_CHANNEL)S 
    ON S.SUB_CHANNEL_TYPE_ID=CAST(STG.CHANNEL_ID AS INT) 
    LEFT OUTER JOIN L2_ANALYTICS_TABLES.DAY D 
    ON D.DAY_DT = DATE_FORMAT(CAST(UNIX_TIMESTAMP(STG.TRANDATE,"yyyyMMdd") AS TIMESTAMP),"yyyy-MM-dd")  
    LEFT OUTER JOIN (select source_location_id as LOCATION_ID,LOCATION_KEY from l2_analytics_tables.location 
        where source_system='GLOBAL_NETSUITE' and last_effective_ts='9999-12-31T23:59:59.000+0000'  
            UNION 
                      select trim(regexp_replace(SHIP_NODE_ID,'GLOBAL_NETSUITE','')) as LOCATION_ID,SHIP_NODE_KEY as LOCATION_KEY from l2_analytics_tables.ship_node 
        where source_system='GLOBAL_NETSUITE' and last_effective_ts='9999-12-31T23:59:59.000+0000') L  
    ON CAST(CAST(STG.LOCATION_ID AS INT)AS STRING)= L.LOCATION_ID 
     LEFT OUTER JOIN L2_ANALYTICS_TABLES.CURRENCY C  
     ON C.GLOBAL_CURRENCY_ID=CAST(STG.CURRENCY_ID AS INT) 
  );
  /*
  ******************************************************************

  ******************************************************************
  ************************* Change History *************************
  ******************************************************************

  **
  ** Purpose      	: Change Data capture

  ******************************************************************

  ** Source Table 	: INVOICE_STG_VIEW1
  ** Target View  	: INVOICE_STG_VIEW2
  ** CHANGE DATA CAPTURE   
******************************************************************
  */
  CREATE OR REPLACE TEMPORARY VIEW INVOICE_STG_VIEW2 AS (
       SELECT COALESCE(T.INVOICE_KEY,0) AS INVOICE_KEY,
        STG1.INVOICE_ID,
     	  STG1.SOURCE_SYSTEM,
     	  STG1.SOURCE_INVOICE_ID,
        STG1.SUB_CHANNEL_CD,
        STG1.CONCEPT_CD,
     	  STG1.LOCATION_KEY,
     	  COALESCE(STG1.LOCATION_ID,'-1') AS LOCATION_ID,
     	  STG1.DAY_KEY,
     	  COALESCE(STG1.INVOICE_DT, CAST('9999-12-31' AS DATE)) AS INVOICE_DT,
     	  STG1.INVOICE_CREATE_TS,
        STG1.ORDER_HEADER_KEY,
        STG1.ORDER_HEADER_ID,
        STG1.ORDER_ID,
        STG1.GLOBAL_TXN_ID,
     	  STG1.FIRST_EFFECTIVE_TS,
     	  STG1.LAST_EFFECTIVE_TS,
     	  STG1.POS_WORKSTATION_ID,
     	  STG1.POS_WORKSTATION_SEQ_NBR,
        STG1.WSSPL_PURCHASE_ORDER_ID,
     	  STG1.NOSALE_OVERRIDE_DT,
     	  STG1.OVERRIDE_REASON_ID,
     	  STG1.CASHIER_ID,
     	  STG1.ASSOCIATE_ID,
     	  STG1.IS_ASSOCIATE_FLAG,
     	  STG1.EMPLOYEE_ID,
        STG1.REPLACEMENT_ORDER_FLAG,
        STG1.INTERNAL_FLAG,
        STG1.DTC_INVOICE_TYPE_CD,
        STG1.DTC_INVOICE_FULFILLMENT_TYPE_CD,
        STG1.DTC_INVOICE_REVISED_TYPE_CD,
        STG1.DTC_TXN_TYPE_CD,
        STG1.DTC_STATUS_CD,
        STG1.DTC_REFERENCE_1,
        STG1.DTC_COLLECTED_AMT,
     	  STG1.TOTAL_QTY,
     	  STG1.TOTAL_AMT,
        STG1.CURRENCY_CD,
     	  STG1.INSERT_TS,
     	  STG1.UPDATE_TS
         FROM INVOICE_STG_VIEW1 STG1  
LEFT OUTER JOIN L2_ANALYTICS_TABLES.INVOICE T  
ON STG1.INVOICE_ID = T.INVOICE_ID AND T.SOURCE_SYSTEM='GLOBAL_NETSUITE'  
AND T.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'    
WHERE T.INVOICE_KEY IS NULL OR 
COALESCE(STG1.SOURCE_INVOICE_ID,'')<>COALESCE(T.SOURCE_INVOICE_ID,'') OR 
COALESCE(STG1.SUB_CHANNEL_CD,'')<>COALESCE(T.SUB_CHANNEL_CD,'') OR 
COALESCE(STG1.CONCEPT_CD,'')<>COALESCE(T.CONCEPT_CD,'') OR 
COALESCE(STG1.LOCATION_KEY,-1)<>COALESCE(T.LOCATION_KEY,-1) OR 
COALESCE(STG1.LOCATION_ID,'')<>COALESCE(T.LOCATION_ID,'') OR 
COALESCE(STG1.DAY_KEY,-1)<>COALESCE(T.DAY_KEY,-1) OR 
COALESCE(STG1.INVOICE_DT,'')<>COALESCE(T.INVOICE_DT,'') OR 
COALESCE(STG1.INVOICE_CREATE_TS,cast('1900-01-01 12:01:00.001' as timestamp))<>COALESCE(T.INVOICE_CREATE_TS,cast('1900-01-01 12:01:00.001' as timestamp)) OR 
COALESCE(STG1.ORDER_HEADER_KEY,-1)<>COALESCE(T.ORDER_HEADER_KEY,-1) OR 
COALESCE(STG1.ORDER_HEADER_ID,'')<>COALESCE(T.ORDER_HEADER_ID,'') OR 
COALESCE(STG1.ORDER_ID,'')<>COALESCE(T.ORDER_ID,'') OR 
COALESCE(STG1.GLOBAL_TXN_ID,'')<>COALESCE(T.GLOBAL_TXN_ID,'') OR 
COALESCE(STG1.POS_WORKSTATION_ID,'')<>COALESCE(T.POS_WORKSTATION_ID,'') OR 
COALESCE(STG1.POS_WORKSTATION_SEQ_NBR,0)<>COALESCE(T.POS_WORKSTATION_SEQ_NBR,0) OR 
COALESCE(STG1.WSSPL_PURCHASE_ORDER_ID,'')<>COALESCE(T.WSSPL_PURCHASE_ORDER_ID,'') OR 
COALESCE(STG1.NOSALE_OVERRIDE_DT,'')<>COALESCE(T.NOSALE_OVERRIDE_DT,'') OR 
COALESCE(STG1.OVERRIDE_REASON_ID,'')<>COALESCE(T.OVERRIDE_REASON_ID,'') OR 
COALESCE(STG1.CASHIER_ID,'')<>COALESCE(T.CASHIER_ID,'') OR 
COALESCE(STG1.ASSOCIATE_ID,'')<>COALESCE(T.ASSOCIATE_ID,'') OR 
COALESCE(STG1.IS_ASSOCIATE_FLAG,'')<>COALESCE(T.IS_ASSOCIATE_FLAG,'') OR 
COALESCE(STG1.EMPLOYEE_ID,'')<>COALESCE(T.EMPLOYEE_ID,'') OR 
COALESCE(STG1.REPLACEMENT_ORDER_FLAG,'')<>COALESCE(T.REPLACEMENT_ORDER_FLAG,'') OR 
COALESCE(STG1.INTERNAL_FLAG,'')<>COALESCE(T.INTERNAL_FLAG,'') OR 
COALESCE(STG1.DTC_INVOICE_TYPE_CD,'')<>COALESCE(T.DTC_INVOICE_TYPE_CD,'') OR 
COALESCE(STG1.DTC_INVOICE_FULFILLMENT_TYPE_CD,'')<>COALESCE(T.DTC_INVOICE_FULFILLMENT_TYPE_CD,'') OR 
COALESCE(STG1.DTC_INVOICE_REVISED_TYPE_CD,'')<>COALESCE(T.DTC_INVOICE_REVISED_TYPE_CD,'') OR 
COALESCE(STG1.DTC_TXN_TYPE_CD,'')<>COALESCE(T.DTC_TXN_TYPE_CD,'') OR 
COALESCE(STG1.DTC_STATUS_CD,'')<>COALESCE(T.DTC_STATUS_CD,'') OR 
COALESCE(STG1.DTC_REFERENCE_1,'')<>COALESCE(T.DTC_REFERENCE_1,'') OR 
COALESCE(STG1.DTC_COLLECTED_AMT,0.00)<>COALESCE(T.DTC_COLLECTED_AMT,0.00) OR 
COALESCE(STG1.TOTAL_QTY,0)<>COALESCE(T.TOTAL_QTY,0) OR 
COALESCE(STG1.TOTAL_AMT,0.00)<>COALESCE(T.TOTAL_AMT,0.00) OR 
COALESCE(STG1.CURRENCY_CD,'')<>COALESCE(T.CURRENCY_CD,'') 
  )  ;      
    
     /*
     ******************************************************************

     ******************************************************************
     ************************* Change History *************************
     ******************************************************************

     ** Change   Date        Author    Description
     **  --      ----------  --------  --------------------------------
     **  01      10/21/2021   KIRAN
     ******************************************************************

     ** Source Table 	: INVOICE_STG_VIEW3
     ** Target View  	: INVOICE_STG_VIEW4

     ** Transformations	:  Generate surrogate keys on INVOICE_KEY


     ******************************************************************
     */

     CREATE OR REPLACE TEMPORARY VIEW INVOICE_STG_VIEW3 AS (
       SELECT
     	  CASE
     		  WHEN STG1.INVOICE_KEY = 0 THEN
     		  	ROW_NUMBER() OVER (ORDER BY STG1.INVOICE_KEY, STG1.INVOICE_ID) + COALESCE(MAX_INVOICE_KEY, 0)
     		  ELSE STG1.INVOICE_KEY
     	  END AS INVOICE_KEY,
     	  STG1.INVOICE_ID,
     	  STG1.SOURCE_SYSTEM,
     	  STG1.SOURCE_INVOICE_ID,
        STG1.SUB_CHANNEL_CD,
        STG1.CONCEPT_CD,
     	  STG1.LOCATION_KEY,
     	  STG1.LOCATION_ID,
     	  STG1.DAY_KEY,
     	  STG1.INVOICE_DT,
     	  STG1.INVOICE_CREATE_TS,
        STG1.ORDER_HEADER_KEY,
        STG1.ORDER_HEADER_ID,
        STG1.ORDER_ID,
        STG1.GLOBAL_TXN_ID,
     	  STG1.FIRST_EFFECTIVE_TS,
     	  STG1.LAST_EFFECTIVE_TS,
     	  STG1.POS_WORKSTATION_ID,
     	  STG1.POS_WORKSTATION_SEQ_NBR,
        STG1.WSSPL_PURCHASE_ORDER_ID,
     	  STG1.NOSALE_OVERRIDE_DT,
     	  STG1.OVERRIDE_REASON_ID,
     	  STG1.CASHIER_ID,
     	  STG1.ASSOCIATE_ID,
     	  STG1.IS_ASSOCIATE_FLAG,
     	  STG1.EMPLOYEE_ID,
        STG1.REPLACEMENT_ORDER_FLAG,
        STG1.INTERNAL_FLAG,
        STG1.DTC_INVOICE_TYPE_CD,
        STG1.DTC_INVOICE_FULFILLMENT_TYPE_CD,
        STG1.DTC_INVOICE_REVISED_TYPE_CD,
        STG1.DTC_TXN_TYPE_CD,
        STG1.DTC_STATUS_CD,
        STG1.DTC_REFERENCE_1,
        STG1.DTC_COLLECTED_AMT,
     	  STG1.TOTAL_QTY,
     	  STG1.TOTAL_AMT,
        STG1.CURRENCY_CD,
     	  STG1.INSERT_TS,
     	  STG1.UPDATE_TS
         FROM INVOICE_STG_VIEW2 STG1
         CROSS JOIN 
     	  (
     		   SELECT MAX(INVOICE_KEY) AS MAX_INVOICE_KEY FROM L2_ANALYTICS_TABLES.INVOICE WHERE INVOICE_KEY <> -1
     	  ) TARGET_MAX
      );



         /* TRUNCATING THE STAGE TABLE AND REINSERTING THE DATA FROM INVOICE_STG_VIEW3  */


         TRUNCATE TABLE L2_STAGE.NETSUITE_INVOICE_STG; 
         INSERT INTO L2_STAGE.NETSUITE_INVOICE_STG SELECT * FROM INVOICE_STG_VIEW3; 
       /*
       ******************************************************************

       ******************************************************************
       ************************* Change History *************************
       ******************************************************************

       ** Change   Date        Author    Description
       **  --      ----------  --------  --------------------------------
       **  01      10/21/2021  KIRAN
       ******************************************************************

       ******************************************************************
       */

       MERGE INTO L2_ANALYTICS_TABLES.INVOICE TARGET USING 
        (
       	 SELECT STG1_1.INVOICE_KEY AS MERGE_KEY, STG1_1.* FROM L2_STAGE.NETSUITE_INVOICE_STG STG1_1 
       	 UNION
       	 SELECT NULL AS MERGE_KEY, STG1_2.* FROM L2_STAGE.NETSUITE_INVOICE_STG STG1_2 
        ) STG1 ON TARGET.INVOICE_KEY = STG1.MERGE_KEY 
          WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000' THEN 
       	 UPDATE SET 
       		 LAST_EFFECTIVE_TS = STG1.FIRST_EFFECTIVE_TS,
       		 UPDATE_TS = STG1.UPDATE_TS  
         WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN 
       	   INSERT (
       		   INVOICE_KEY,
       	     INVOICE_ID,
       	     SOURCE_SYSTEM,
			 ORDER_CHARGE_TRANSACTION_KEY,  /* HINATUAN CHANGE */
			 ORDER_CHARGE_TRANSACTION_ID ,  /* HINATUAN CHANGE */
       	     SOURCE_INVOICE_ID,
             SUB_CHANNEL_CD,
             CONCEPT_CD,
       	     LOCATION_KEY,
       	     LOCATION_ID,
       	     DAY_KEY,
       	     INVOICE_DT,
       	     INVOICE_CREATE_TS,
             ORDER_HEADER_KEY,
             ORDER_HEADER_ID,
             ORDER_ID,
             GLOBAL_TXN_ID,
       	     FIRST_EFFECTIVE_TS,
       	     LAST_EFFECTIVE_TS,
       	     POS_WORKSTATION_ID,
       	     POS_WORKSTATION_SEQ_NBR,
             WSSPL_PURCHASE_ORDER_ID,
       	     NOSALE_OVERRIDE_DT,
       	     OVERRIDE_REASON_ID,
       	     CASHIER_ID,
       	     ASSOCIATE_ID,
       	     IS_ASSOCIATE_FLAG,
       	     EMPLOYEE_ID,
            REPLACEMENT_ORDER_FLAG,
            INTERNAL_FLAG,
             DTC_INVOICE_TYPE_CD,
             DTC_INVOICE_FULFILLMENT_TYPE_CD,
             DTC_INVOICE_REVISED_TYPE_CD,
             DTC_TXN_TYPE_CD,
             DTC_STATUS_CD,
             DTC_REFERENCE_1,
             DTC_COLLECTED_AMT,
       	     TOTAL_QTY,
       	     TOTAL_AMT,
			 CREATE_USER_ID,                      /* HINATUAN CHANGES */
             CURRENCY_CD,
       	     INSERT_TS,
       	     UPDATE_TS
       	  ) VALUES (
       		  STG1.INVOICE_KEY,
       	    STG1.INVOICE_ID,
       	    STG1.SOURCE_SYSTEM,
			NULL,  /* HINATUAN CHANGE */
			NULL ,  /* HINATUAN CHANGE */
       	    STG1.SOURCE_INVOICE_ID,
            STG1.SUB_CHANNEL_CD,
            STG1.CONCEPT_CD,
       	    STG1.LOCATION_KEY,
       	    STG1.LOCATION_ID,
       	    STG1.DAY_KEY,
       	    STG1.INVOICE_DT,
       	    STG1.INVOICE_CREATE_TS,
            STG1.ORDER_HEADER_KEY,
            STG1.ORDER_ID,
            STG1.ORDER_ID,
            STG1.GLOBAL_TXN_ID,
       	    STG1.FIRST_EFFECTIVE_TS,
       	    STG1.LAST_EFFECTIVE_TS,
       	    STG1.POS_WORKSTATION_ID,
       	    STG1.POS_WORKSTATION_SEQ_NBR,
            STG1.WSSPL_PURCHASE_ORDER_ID,
       	    STG1.NOSALE_OVERRIDE_DT,
       	    STG1.OVERRIDE_REASON_ID,
       	    STG1.CASHIER_ID,
       	    STG1.ASSOCIATE_ID,
       	    STG1.IS_ASSOCIATE_FLAG,
       	    STG1.EMPLOYEE_ID,
            STG1.REPLACEMENT_ORDER_FLAG,
            STG1.INTERNAL_FLAG,
            STG1.DTC_INVOICE_TYPE_CD,
            STG1.DTC_INVOICE_FULFILLMENT_TYPE_CD,
            STG1.DTC_INVOICE_REVISED_TYPE_CD,
            STG1.DTC_TXN_TYPE_CD,
            STG1.DTC_STATUS_CD,
            STG1.DTC_REFERENCE_1,
            STG1.DTC_COLLECTED_AMT,
       	    STG1.TOTAL_QTY,
       	    STG1.TOTAL_AMT,
			NULL,                                             /* HINATUAN CHANGES */
            STG1.CURRENCY_CD,  
       	    STG1.INSERT_TS,
       	    STG1.UPDATE_TS
       	);

        /*
        ** Source Table   : L2_ANALYTICS_TABLES.INVOICE
        ** Target Table    : L2_ANALYTICS_TABLES.INVOICE
        ** Optimize delta data for  LAST_EFFECTIVE_TS
        **
        */

        OPTIMIZE L2_ANALYTICS_TABLES.INVOICE ZORDER BY LAST_EFFECTIVE_TS  ;
