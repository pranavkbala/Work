/*
******************************************************************

******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      04/15/2020  YAM-05  SQL file created
**  02      10/04/2020   THAM-761 added Snow steps
******************************************************************


** Author			: YAMUNA Team YAM-5
** Create Date		: April, 2020
** Purpose      	: Apply mapping rules and generate necessary
					          columns for L2_ANALYTICS_TABLES.INVOICE_DETAIL table

******************************************************************

** Source Table 	: L2_STAGE.INVOICE_DETAIL_STG
** Target View  	: INVOICE_DETAIL_STG_VIEW1
** Lookup       	: L2_ANALYTICS_TABLES.ITEM

** Transformations	:
					: column concatenation and date conversion
					: static columns value insertions


******************************************************************
*/

SET spark.sql.broadcastTimeout = 2400;

CREATE OR REPLACE TEMPORARY VIEW INVOICE_DETAIL_STG_VIEW1 AS (
 SELECT
	0 AS INVOICE_DETAIL_KEY,
    CONCAT(STG.INVOICE_DT,STG.LOCATION_ID,STG.POS_WORKSTATION_ID,STG.POS_WORKSTATION_SEQ_NBR) AS INVOICE_ID,
	  STG.SOURCE_SYSTEM AS SOURCE_SYSTEM,
    CAST(STG.PRIME_LINE_SEQ_NBR AS DECIMAL(5,0)) AS PRIME_LINE_SEQ_NBR,
    DATE_FORMAT(CAST(UNIX_TIMESTAMP(STG.INVOICE_DT,"yyyyMMdd") AS TIMESTAMP),"yyyy-MM-dd") AS INVOICE_DT,
    -1 AS ITEM_KEY ,
    STG.ITEM_ID AS ITEM_ID,
    -1 AS ORDER_LINE_KEY,
    CONCAT(STG.INVOICE_DT,"-",LPAD(LTRIM("0",STG.LOCATION_ID),4,0),"-",LPAD(STG.POS_WORKSTATION_ID,2,0),"-",LPAD(LTRIM("0",STG.POS_WORKSTATION_SEQ_NBR),4,0),"-",LPAD(STG.PRIME_LINE_SEQ_NBR,4,0)) AS ORDER_LINE_ID,
    STG.ENTRY_METHOD AS ENTRY_METHOD,
	  CAST(UNIX_TIMESTAMP(CONCAT(STG.INVOICE_DT,STG.MIN_IDNT), "yyyyMMddHHmm") AS TIMESTAMP) AS FIRST_EFFECTIVE_TS,
	  CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000+0000" , "yyyy-MM-dd HH:mm:ss.SSS") AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
	  STG.RETAIL_TYPE_CD AS RETAIL_TYPE_CD,
	  STG.POS_RING_TYPE_CD AS POS_RING_TYPE_CD,
    STG.TXN_TYPE_CD AS TXN_TYPE_CD ,
	  STG.TXN_SUB_TYPE_ID AS TXN_SUB_TYPE_CD,
    STG.SLSPRSN_ID AS SLSPRSN_ID,
    CASE WHEN STG.INVOICE_QTY = '' OR STG.INVOICE_QTY IS NULL THEN '0' ELSE CAST(STG.INVOICE_QTY AS INT) END AS INVOICE_QTY,
    CASE WHEN STG.INVOICE_AMT = '' OR STG.INVOICE_AMT IS NULL THEN '0' ELSE CAST(STG.INVOICE_AMT AS DECIMAL(15,2)) END AS INVOICE_AMT,
    STG.TXN_SIGN AS TXN_SIGN,
    STG.TAXABLE_FLAG AS TAXABLE_FLAG,
    CAST(STG.ORIG_TXN_NBR AS INT) as ORIG_TXN_NBR,
    CASE WHEN STG.DISC_AMT = '' OR STG.DISC_AMT IS NULL THEN '0' ELSE CAST(STG.DISC_AMT AS DECIMAL(19,6)) END AS DISC_AMT,
    CASE WHEN STG.ORIG_UNIT_PRICE_AMT = '' OR STG.ORIG_UNIT_PRICE_AMT IS NULL THEN '0' ELSE CAST(STG.ORIG_UNIT_PRICE_AMT AS DECIMAL(19,6)) END AS ORIG_UNIT_PRICE_AMT,
    STG.ORIG_TXN_SIGN_CD AS ORIG_TXN_SIGN_CD,
    STG.REASON_ID AS REASON_ID,
    STG.RETURN_REASON_ID AS RETURN_REASON_ID,
    STG.DROP_SHIP_FLAG AS DROP_SHIP_FLAG,
	CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
	CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
  FROM L2_STAGE.INVOICE_DETAIL_STG STG

);

/*
******************************************************************

******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      04/15/2020  YAM-05  SQL file created
******************************************************************


** Author			: YAMUNA Team YAM-5
** Create Date		: April, 2020
** Purpose      	: handling "coalesce" operations when loading

******************************************************************

** Source Table 	: INVOICE_DETAIL_STG_VIEW1
** Target View  	: INVOICE_DETAIL_STG_VIEW2
** Lookup       	: L2_ANALYTICS_TABLES.INVOICE & L2_ANALYTICS_TABLES.ORDER_LINE
** Transformations	: INVOICE_KEY look up and column type casting


******************************************************************
*/

CREATE OR REPLACE TEMPORARY VIEW INVOICE_DETAIL_STG_VIEW2 AS (
 SELECT
	  STG.INVOICE_DETAIL_KEY AS INVOICE_DETAIL_KEY,
    STG.INVOICE_ID AS INVOICE_ID,
	  STG.SOURCE_SYSTEM AS SOURCE_SYSTEM,
	  STG.PRIME_LINE_SEQ_NBR,
    I.INVOICE_KEY AS INVOICE_KEY,
    CAST(STG.INVOICE_DT AS DATE) AS INVOICE_DT,
    COALESCE(IT.ITEM_KEY,-1) as ITEM_KEY ,
    STG.ITEM_ID AS ITEM_ID,
    COALESCE(OL.ORDER_LINE_KEY,-1) AS ORDER_LINE_KEY,
    STG.ORDER_LINE_ID ,
    STG.ENTRY_METHOD AS ENTRY_METHOD,
	  STG.FIRST_EFFECTIVE_TS AS FIRST_EFFECTIVE_TS,
	  STG.LAST_EFFECTIVE_TS AS LAST_EFFECTIVE_TS,
	  STG.RETAIL_TYPE_CD AS RETAIL_TYPE_CD,
	  STG.POS_RING_TYPE_CD AS POS_RING_TYPE_CD,
    STG.TXN_TYPE_CD AS TXN_TYPE_CD,
	  STG.TXN_SUB_TYPE_CD as TXN_SUB_TYPE_CD,
    STG.SLSPRSN_ID as SLSPRSN_ID,
    CASE WHEN STG.TXN_SIGN = 'P' THEN STG.INVOICE_QTY/10000 ELSE INVOICE_QTY/-10000 END AS INVOICE_QTY,
    CASE WHEN STG.TXN_SIGN = 'P' THEN STG.INVOICE_AMT/10000 ELSE INVOICE_AMT/-10000 END AS INVOICE_AMT,
    CASE WHEN STG.TXN_SIGN = 'P' THEN (STG.ORIG_UNIT_PRICE_AMT - STG.DISC_AMT)/10000 ELSE (STG.ORIG_UNIT_PRICE_AMT - STG.DISC_AMT)/-10000 END AS UNIT_PRICE_AMT,
    STG.TXN_SIGN as TXN_SIGN,
    STG.TAXABLE_FLAG as TAXABLE_FLAG,
    STG.ORIG_TXN_NBR,
    CASE WHEN STG.TXN_SIGN = 'P' THEN STG.ORIG_UNIT_PRICE_AMT/10000 ELSE ORIG_UNIT_PRICE_AMT/-10000 END AS ORIG_UNIT_PRICE_AMT,
    STG.ORIG_TXN_SIGN_CD as ORIG_TXN_SIGN_CD,
    STG.REASON_ID as REASON_ID,
    STG.RETURN_REASON_ID as RETURN_REASON_ID,
    STG.DROP_SHIP_FLAG as DROP_SHIP_FLAG,
	  STG.INSERT_TS AS INSERT_TS,
	  STG.UPDATE_TS AS UPDATE_TS
    FROM INVOICE_DETAIL_STG_VIEW1 STG
    LEFT OUTER JOIN L2_ANALYTICS_TABLES.INVOICE I
    ON I.INVOICE_ID = STG.INVOICE_ID
    AND I.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'

    LEFT OUTER JOIN L2_ANALYTICS.ORDER_LINE OL
    ON  STG.INVOICE_DT = OL.ORDER_DT
    AND OL.SOURCE_SYSTEM IN ( 'CDW','REDIRON_POSLOG', 'NCR_POSLOG' )
    AND STG.ORDER_LINE_ID = OL.ORDER_LINE_ID
    AND OL.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'

    LEFT OUTER JOIN L2_ANALYTICS.ORDER_HEADER OH
      ON  OH.ORDER_HEADER_KEY = OL.ORDER_HEADER_KEY
      AND OH.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'

    LEFT OUTER JOIN L2_ANALYTICS.ITEM IT
      ON STG.ITEM_ID = IT.ITEM_ID
      AND IT.MARKET_CD = COALESCE(OH.MARKET_CD,'USA')
      AND IT.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'



   );



   /*
   ******************************************************************

   ******************************************************************
   ************************* Change History *************************
   ******************************************************************

   ** Change   Date        Author    Description
   **  --      ----------  --------  --------------------------------
   **  01      06/10/2020  YAM-05  SQL file created
   ******************************************************************

   ** Author			: YAMUNA Team YAM-5
   ** Create Date		: June, 2020
   ** Purpose      	: Get INVOICE_DETAIL data in a Temp View with Prime_line_seq_nbr appended with Row_Number

   ******************************************************************

   ** Source Table 	: L2_ANALYTICS_TABLES.INVOICE_DETAIL
   ** Target View  	: INVOICE_DETAIL_TARGET


   ******************************************************************
   */

   CREATE OR REPLACE TEMPORARY VIEW INVOICE_DETAIL_TARGET AS (
     SELECT  INVOICE_DETAIL_KEY,
     	     SOURCE_SYSTEM,
     	     PRIME_LINE_SEQ_NBR || '-' || ROW_NUMBER() OVER (PARTITION BY SOURCE_SYSTEM, INVOICE_KEY, PRIME_LINE_SEQ_NBR order by SOURCE_SYSTEM, INVOICE_KEY, PRIME_LINE_SEQ_NBR) AS      PRIME_LINE_SEQ_NBR,
     	     INVOICE_KEY,
			 INVOICE_ID,
           INVOICE_DT,
     	     ITEM_KEY,
     	     ITEM_ID,
           ORDER_LINE_KEY,
           ORDER_LINE_ID,
           ENTRY_METHOD,
     	     FIRST_EFFECTIVE_TS,
     	     LAST_EFFECTIVE_TS,
     	     RETAIL_TYPE_CD,
     	     POS_RING_TYPE_CD ,
           TXN_TYPE_CD ,
           TXN_SUB_TYPE_CD ,
     	     SLSPRSN_ID,
     	     INVOICE_QTY,
     	     INVOICE_AMT,
           UNIT_PRICE_AMT,
     	     TXN_SIGN,
     	     TAXABLE_FLAG,
     	     ORIG_TXN_NBR,
     	     ORIG_UNIT_PRICE_AMT,
           ORIG_TXN_SIGN_CD,
     	     REASON_ID,
     	     RETURN_REASON_ID,
     	     DROP_SHIP_FLAG,
     	     INSERT_TS,
     	     UPDATE_TS
   FROM L2_ANALYTICS_TABLES.INVOICE_DETAIL
   WHERE SOURCE_SYSTEM = 'RMS' );



     /*
     ******************************************************************

     ******************************************************************
     ************************* Change History *************************
     ******************************************************************

     ** Change   Date        Author    Description
     **  --      ----------  --------  --------------------------------
     **  01      04/15/2020  YAM-05  SQL file created
     ******************************************************************

     ** Author			: YAMUNA Team YAM-5
     ** Create Date		: April, 2020
     ** Purpose      	: operate COALESCE on necessary columns when joining and checking indifference in TARGET table and view table to correctly ingest the data

     ******************************************************************

     ** Source Table 	: INVOICE_DETAIL_STG_VIEW2
     ** Target View  	: INVOICE_DETAIL_STG_VIEW3

     ** Transformations	: operate COALESCE on necessary columns when joining and checking indifference in TARGET table and view table to correctly ingest the data



     ******************************************************************
     */

     CREATE OR REPLACE TEMPORARY VIEW INVOICE_DETAIL_STG_VIEW3 AS (
        SELECT
            COALESCE(TARGET.INVOICE_DETAIL_KEY, STG_RESULT.INVOICE_DETAIL_KEY) AS INVOICE_DETAIL_KEY,
            STG_RESULT.SOURCE_SYSTEM,
     	      STG_RESULT.PRIME_LINE_SEQ_NBR,
     	      STG_RESULT.INVOICE_KEY,
			  STG_RESULT.INVOICE_ID,
            STG_RESULT.INVOICE_DT,
     	      STG_RESULT.ITEM_KEY,
     	      STG_RESULT.ITEM_ID,
            STG_RESULT.ORDER_LINE_KEY,
            STG_RESULT.ORDER_LINE_ID,
            STG_RESULT.ENTRY_METHOD,
     	      STG_RESULT.FIRST_EFFECTIVE_TS,
     	      STG_RESULT.LAST_EFFECTIVE_TS,
     	      STG_RESULT.RETAIL_TYPE_CD,
     	      STG_RESULT.POS_RING_TYPE_CD ,
            STG_RESULT.TXN_TYPE_CD ,
            STG_RESULT.TXN_SUB_TYPE_CD ,
     	      STG_RESULT.SLSPRSN_ID,
     	      STG_RESULT.INVOICE_QTY,
     	      STG_RESULT.INVOICE_AMT,
            STG_RESULT.UNIT_PRICE_AMT,
     	      STG_RESULT.TXN_SIGN,
     	      STG_RESULT.TAXABLE_FLAG,
     	      STG_RESULT.ORIG_TXN_NBR,
     	      STG_RESULT.ORIG_UNIT_PRICE_AMT,
            STG_RESULT.ORIG_TXN_SIGN_CD,
     	      STG_RESULT.REASON_ID,
     	      STG_RESULT.RETURN_REASON_ID,
     	      STG_RESULT.DROP_SHIP_FLAG,
     	      STG_RESULT.INSERT_TS,
     	      STG_RESULT.UPDATE_TS
         FROM (
         SELECT
               STG1.INVOICE_DETAIL_KEY,
     	         STG1.SOURCE_SYSTEM,
     	         STG1.PRIME_LINE_SEQ_NBR || '-' || ROW_NUMBER() OVER (PARTITION BY STG1.SOURCE_SYSTEM, STG1.INVOICE_KEY, STG1.PRIME_LINE_SEQ_NBR order by STG1.SOURCE_SYSTEM, STG1.INVOICE_KEY, STG1.PRIME_LINE_SEQ_NBR) AS PRIME_LINE_SEQ_NBR,
     	         STG1.INVOICE_KEY,
				 STG1.INVOICE_ID,
               STG1.INVOICE_DT,
     	         STG1.ITEM_KEY,
     	         STG1.ITEM_ID,
               STG1.ORDER_LINE_KEY,
               STG1.ORDER_LINE_ID,
               STG1.ENTRY_METHOD,
     	         STG1.FIRST_EFFECTIVE_TS,
     	         STG1.LAST_EFFECTIVE_TS,
     	         STG1.RETAIL_TYPE_CD,
     	         STG1.POS_RING_TYPE_CD ,
               STG1.TXN_TYPE_CD ,
               STG1.TXN_SUB_TYPE_CD ,
     	         STG1.SLSPRSN_ID,
     	         STG1.INVOICE_QTY,
     	         STG1.INVOICE_AMT,
               STG1.UNIT_PRICE_AMT,
     	         STG1.TXN_SIGN,
     	         STG1.TAXABLE_FLAG,
     	         STG1.ORIG_TXN_NBR,
     	         STG1.ORIG_UNIT_PRICE_AMT,
               STG1.ORIG_TXN_SIGN_CD,
     	         STG1.REASON_ID,
     	         STG1.RETURN_REASON_ID,
     	         STG1.DROP_SHIP_FLAG,
     	         STG1.INSERT_TS,
     	         STG1.UPDATE_TS
         FROM INVOICE_DETAIL_STG_VIEW2 STG1
         ) STG_RESULT
         LEFT OUTER JOIN INVOICE_DETAIL_TARGET TARGET
     	   ON STG_RESULT.INVOICE_KEY = TARGET.INVOICE_KEY
         AND STG_RESULT.PRIME_LINE_SEQ_NBR = TARGET.PRIME_LINE_SEQ_NBR
     	   AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
     	   AND STG_RESULT.SOURCE_SYSTEM = TARGET.SOURCE_SYSTEM
       );

       /*
       ******************************************************************

       ******************************************************************
       ************************* Change History *************************
       ******************************************************************

       ** Change   Date        Author    Description
       **  --      ----------  --------  --------------------------------
       **  01      04/15/2020  YAM-05  SQL file created
       ******************************************************************

       ** Author			: YAMUNA Team YAM-5
       ** Create Date		: April, 2020
       ** Purpose      	: Generate surrogate keys

       ******************************************************************

       ** Source Table 	: INVOICE_DETAIL_STG_VIEW3
       ** Target View  	: INVOICE_DETAIL_STG_VIEW4

       ** Transformations	:  Generate surrogate keys on INVOICE_DETAIL_KEY


       ******************************************************************
       */

       CREATE OR REPLACE TEMPORARY VIEW INVOICE_DETAIL_STG_VIEW4 AS (
         SELECT
       	   CASE
       		   WHEN STG1.INVOICE_DETAIL_KEY = 0 THEN
       			    ROW_NUMBER() OVER (ORDER BY STG1.INVOICE_DETAIL_KEY, STG1.SOURCE_SYSTEM, STG1.INVOICE_KEY, STG1.PRIME_LINE_SEQ_NBR) + COALESCE(MAX_INVOICE_DETAIL_KEY, 0)
       		   ELSE STG1.INVOICE_DETAIL_KEY
       	   END AS INVOICE_DETAIL_KEY,
       	      STG1.SOURCE_SYSTEM,
       	      STG1.PRIME_LINE_SEQ_NBR,
       	      STG1.INVOICE_KEY,
			  STG1.INVOICE_ID,
              STG1.INVOICE_DT,
       	      STG1.ITEM_KEY,
       	      STG1.ITEM_ID,
              STG1.ORDER_LINE_KEY,
              STG1.ORDER_LINE_ID,
              STG1.ENTRY_METHOD,
       	      STG1.FIRST_EFFECTIVE_TS,
       	      STG1.LAST_EFFECTIVE_TS,
       	      STG1.RETAIL_TYPE_CD,
       	      STG1.POS_RING_TYPE_CD ,
              STG1.TXN_TYPE_CD ,
              STG1.TXN_SUB_TYPE_CD ,
       	      STG1.SLSPRSN_ID,
       	      STG1.INVOICE_QTY,
       	      STG1.INVOICE_AMT,
              STG1.UNIT_PRICE_AMT,
       	      STG1.TXN_SIGN,
       	      STG1.TAXABLE_FLAG,
       	      STG1.ORIG_TXN_NBR,
       	      STG1.ORIG_UNIT_PRICE_AMT,
              STG1.ORIG_TXN_SIGN_CD,
       	      STG1.REASON_ID,
       	      STG1.RETURN_REASON_ID,
       	      STG1.DROP_SHIP_FLAG,
       	      STG1.INSERT_TS,
       	      STG1.UPDATE_TS
         FROM INVOICE_DETAIL_STG_VIEW3 STG1
         CROSS JOIN
       	 (
       		 SELECT MAX(INVOICE_DETAIL_KEY) AS MAX_INVOICE_DETAIL_KEY FROM L2_ANALYTICS_TABLES.INVOICE_DETAIL WHERE INVOICE_DETAIL_KEY <> -1
       	 ) TARGET_MAX
       );


       /*
       ******************************************************************

       ******************************************************************
       ************************* Change History *************************
       ******************************************************************

       ** Change   Date        Author    Description
       **  --      ----------  --------  --------------------------------
       **  01      04/24/2020  YAM-05  SQL file created
       *****************************************************************


       ** Author			: YAMUNA Team YAM-5
       ** Create Date		: April, 2020
       ** Purpose      	: Generate surrogate keys

       ******************************************************************

       ** Source Table 	: INVOICE_DETAIL_STG_VIEW4
       ** Target View  	: INVOICE_DETAIL_STG_VIEW5

       ** Transformations	:  Historical Data Ingestion using Lead function on FIRST_EFFECTIVE_TS

       ******************************************************************
       */

       CREATE OR REPLACE TEMPORARY VIEW V_INVOICE_DETAIL_STG_VIEW5 AS (
         SELECT
              INVOICE_DETAIL_KEY,  
              CONCAT(INVOICE_KEY,'-',COALESCE(PRIME_LINE_SEQ_NBR,0)) AS INVOICE_DETAIL_ID,
              NULL AS CONCEPT_CD,
              SOURCE_SYSTEM,
              SUBSTRING_INDEX(PRIME_LINE_SEQ_NBR,'-',1) AS PRIME_LINE_SEQ_NBR,
              INVOICE_KEY,
              INVOICE_ID,
              INVOICE_DT,
              ITEM_KEY,
              ITEM_ID,
              ORDER_LINE_KEY,
              ORDER_LINE_ID,
              NULL AS LOCATION_KEY, 
              NULL AS LOCATION_ID, 
              NULL AS GLOBAL_COST_CENTER_KEY,
              NULL AS GLOBAL_COST_CENTER_ID,
              NULL AS GLOBAL_SUBSIDIARY_KEY,
              NULL AS GLOBAL_SUBSIDIARY_ID,
              FIRST_EFFECTIVE_TS,
              COALESCE(LEAD(FIRST_EFFECTIVE_TS) OVER (PARTITION BY INVOICE_DETAIL_KEY ORDER BY FIRST_EFFECTIVE_TS),LAST_EFFECTIVE_TS) AS LAST_EFFECTIVE_TS,
              RETAIL_TYPE_CD,
              POS_RING_TYPE_CD,
              TXN_TYPE_CD,
              TXN_SUB_TYPE_CD,
              ENTRY_METHOD,
              SLSPRSN_ID,
              INVOICE_QTY,
              INVOICE_AMT,
              UNIT_PRICE_AMT,
              TXN_SIGN,
              TAXABLE_FLAG,                
              ORIG_TXN_NBR,
              ORIG_UNIT_PRICE_AMT,
              ORIG_TXN_SIGN_CD,
              REASON_ID,
              RETURN_REASON_ID,
              DROP_SHIP_FLAG,
              NULL AS ACTUAL_SHIP_DT,
              NULL AS WEIGHTED_AVG_COST,
              NULL AS LABOR_COST,
              NULL AS LABOR_SKU,
              NULL AS VENDOR_COST,
              NULL AS PO_EXPENSES,
              NULL AS PO_DUTY,
              NULL AS FULFILLMENT_STATUS_ID,
              NULL AS DISCOUNT_RATE_AMT,
              NULL AS ELC_AGENT_COMMISSION_AMT,
              NULL AS ELC_HTS_AMT,
              NULL AS ELC_FREIGHT_AMT,
              NULL AS ELC_MISC_AMT,
              NULL AS FIRST_COST_AMT,
              NULL AS PACK_GROUP_NBR,
              NULL AS COMMITTED_QTY,
              NULL AS CLOSED_DT,
              INSERT_TS,
              UPDATE_TS 
         FROM INVOICE_DETAIL_STG_VIEW4
       );
	   
	   /*
       ** Change   Date        Author    Description
       **  --      ----------  --------  --------------------------------
       **  01      10/04/2021  THAM-761  Materialize invoice for Snowflake transmission
       */
	   

	   
	   INSERT OVERWRITE table  L2_STAGE.INVOICE_DETAIL_STG_VIEW5 SELECT * FROM V_INVOICE_DETAIL_STG_VIEW5;


       /*
       ******************************************************************

       ******************************************************************
       ************************* Change History *************************
       ******************************************************************

       ** Change   Date        Author    Description
       **  --      ----------  --------  --------------------------------
       **  01      04/24/2020  YAM-05  SQL file created
	   **  02      10/04/2021  THAM-761 Started using materialized table for final merge
       ******************************************************************

       ** Author			: YAMUNA Team YAM-5
       ** Create Date		: April, 2020
       ** Purpose      	: Merge into L2_ANALYTICS_TABLES.INVOICE_DETAIL

       ******************************************************************

       ** Source View 	: INVOICE_DETAIL_STG_VIEW5
       ** Target Table  	: L2_ANALYTICS_TABLES.INVOICE_DETAIL
       ** Transformations	:  Merge into L2_ANALYTICS_TABLES.INVOICE_DETAIL

       ******************************************************************
       */




       MERGE INTO L2_ANALYTICS_TABLES.INVOICE_DETAIL TARGET USING
        (
         SELECT STG1_1.INVOICE_DETAIL_KEY AS MERGE_KEY, STG1_1.* FROM L2_STAGE.INVOICE_DETAIL_STG_VIEW5 STG1_1
         UNION
         SELECT NULL AS MERGE_KEY, STG1_2.* FROM L2_STAGE.INVOICE_DETAIL_STG_VIEW5 STG1_2
        ) STG1 ON TARGET.INVOICE_DETAIL_KEY = STG1.MERGE_KEY
         WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000' THEN
          UPDATE SET
            LAST_EFFECTIVE_TS = STG1.FIRST_EFFECTIVE_TS,
            UPDATE_TS = STG1.UPDATE_TS
         WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
          INSERT (
              INVOICE_DETAIL_KEY,  
                INVOICE_DETAIL_ID,
                CONCEPT_CD,
                SOURCE_SYSTEM,
                PRIME_LINE_SEQ_NBR,
                INVOICE_KEY,
                INVOICE_ID,
                INVOICE_DT,
                ITEM_KEY,
                ITEM_ID,
                ORDER_LINE_KEY,
                ORDER_LINE_ID,
                LOCATION_KEY, 
                LOCATION_ID, 
                GLOBAL_COST_CENTER_KEY,
                GLOBAL_COST_CENTER_ID,
                GLOBAL_SUBSIDIARY_KEY,
                GLOBAL_SUBSIDIARY_ID,
                FIRST_EFFECTIVE_TS,
                LAST_EFFECTIVE_TS,
                RETAIL_TYPE_CD,
                POS_RING_TYPE_CD,
                TXN_TYPE_CD,
                TXN_SUB_TYPE_CD,
                ENTRY_METHOD,
                SLSPRSN_ID,
                INVOICE_QTY,
                INVOICE_AMT,
                UNIT_PRICE_AMT,
                GLOBAL_ESTIMATED_COST_AMT,
                TXN_SIGN,
                TAXABLE_FLAG,                
                ORIG_TXN_NBR,
                ORIG_UNIT_PRICE_AMT,
                ORIG_TXN_SIGN_CD,
                REASON_ID,
                RETURN_REASON_ID,
                DROP_SHIP_FLAG,
                ACTUAL_SHIP_DT,
                WEIGHTED_AVG_COST,
                LABOR_COST,
                LABOR_SKU,
                VENDOR_COST,
                PO_EXPENSES,
                PO_DUTY,
                FULFILLMENT_STATUS_ID,
                DISCOUNT_RATE_AMT,
                ELC_AGENT_COMMISSION_AMT,
                ELC_HTS_AMT,
                ELC_FREIGHT_AMT,
                ELC_MISC_AMT,
                FIRST_COST_AMT,
                PACK_GROUP_NBR,
                COMMITTED_QTY,
                CLOSED_DT,
                INSERT_TS,
                UPDATE_TS 
                ) VALUES (
                 STG1.INVOICE_DETAIL_KEY,
                 STG1.INVOICE_DETAIL_ID,
                 STG1.CONCEPT_CD,
                 STG1.SOURCE_SYSTEM,
                 STG1.PRIME_LINE_SEQ_NBR,
                 STG1.INVOICE_KEY,
                 STG1.INVOICE_ID,
                 STG1.INVOICE_DT,
                 STG1.ITEM_KEY,
                 STG1.ITEM_ID,
                 STG1.ORDER_LINE_KEY,
                 STG1.ORDER_LINE_ID,
                 STG1.LOCATION_KEY, 
                 STG1.LOCATION_ID, 
                 STG1.GLOBAL_COST_CENTER_KEY,
                 STG1.GLOBAL_COST_CENTER_ID,
                 STG1.GLOBAL_SUBSIDIARY_KEY,
                 STG1.GLOBAL_SUBSIDIARY_ID,
                 STG1.FIRST_EFFECTIVE_TS,
                 STG1.LAST_EFFECTIVE_TS,
                 STG1.RETAIL_TYPE_CD,
                 STG1.POS_RING_TYPE_CD ,
                 STG1.TXN_TYPE_CD ,
                 STG1.TXN_SUB_TYPE_CD ,
                 STG1.ENTRY_METHOD,
                 STG1.SLSPRSN_ID,
                 STG1.INVOICE_QTY,
                 STG1.INVOICE_AMT,
                 STG1.UNIT_PRICE_AMT,
                 NULL,
                 STG1.TXN_SIGN,
                 STG1.TAXABLE_FLAG,                 
                 STG1.ORIG_TXN_NBR,
                 STG1.ORIG_UNIT_PRICE_AMT,
                 STG1.ORIG_TXN_SIGN_CD,
                 STG1.REASON_ID,
                 STG1.RETURN_REASON_ID,
                 STG1.DROP_SHIP_FLAG,
                 STG1.ACTUAL_SHIP_DT,
                 STG1.WEIGHTED_AVG_COST,
                 STG1.LABOR_COST,
                 STG1.LABOR_SKU,
                 STG1.VENDOR_COST,
                 STG1.PO_EXPENSES,
                 STG1.PO_DUTY,
                 STG1.FULFILLMENT_STATUS_ID,
                 STG1.DISCOUNT_RATE_AMT,
                 STG1.ELC_AGENT_COMMISSION_AMT,
                 STG1.ELC_HTS_AMT,
                 STG1.ELC_FREIGHT_AMT,
                 STG1.ELC_MISC_AMT,
                 STG1.FIRST_COST_AMT,
                 STG1.PACK_GROUP_NBR,
                 STG1.COMMITTED_QTY,
                 STG1.CLOSED_DT,
                 STG1.INSERT_TS,
                 STG1.UPDATE_TS
              );




/*
** Source Table   : L2_ANALYTICS_TABLES.INVOICE_DETAIL
** Target Table    : L2_ANALYTICS_TABLES.INVOICE_DETAIL
** Optimize delta data for  LAST_EFFECTIVE_TS
**
*/

OPTIMIZE L2_ANALYTICS_TABLES.INVOICE_DETAIL  ;
