/*
******************************************************************

******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      04/10/2020  YAM-05  SQL file created
**  02      10/04/2020   THAM-761 added Snow steps
******************************************************************


** Author			: YAMUNA Team YAM-5
** Create Date		: April, 2020
** Purpose      	: Apply mapping rules and generate necessary
					          columns for L2_ANALYTICS_TABLES.INVOICE table

******************************************************************

** Source Table 	: L2_STAGE.INVOICE_STG
** Target View  	: INVOICE_STG_VIEW1
** Lookup       	: L2_ANALYTICS.DAY, L2_ANALYTICS_TABLES.LOCATION

** Transformations	:
					: DAY_KEY calculation
					: column concatenation and date conversion
					: static columns value insertions



******************************************************************
*/

SET spark.sql.broadcastTimeout = 2400;

CREATE OR REPLACE TEMPORARY VIEW INVOICE_STG_VIEW1 AS (
 SELECT
	  0 AS INVOICE_KEY,
    CONCAT(STG.INVOICE_DT,STG.LOCATION_ID,STG.POS_WORKSTATION_ID,STG.POS_WORKSTATION_SEQ_NBR) AS INVOICE_ID,
	  STG.SOURCE_SYSTEM AS SOURCE_SYSTEM,
	  STG.SOURCE_INVOICE_ID as SOURCE_INVOICE_ID,
    COALESCE(L.LOCATION_KEY,-1) as LOCATION_KEY ,
    STG.LOCATION_ID AS LOCATION_ID,
	  COALESCE(D.DAY_KEY, -1) AS DAY_KEY,
	  DATE_FORMAT(CAST(UNIX_TIMESTAMP(STG.INVOICE_DT,"yyyyMMdd") AS TIMESTAMP),"yyyy-MM-dd") AS INVOICE_DT,
	  DATE_FORMAT(CAST(UNIX_TIMESTAMP(CONCAT(STG.INVOICE_DT,STG.MIN_IDNT), "yyyyMMddHHmm") AS TIMESTAMP),'yyyy-MM-dd HH:mm') as INVOICE_CREATE_TS,
    -1 AS ORDER_HEADER_KEY,
    CONCAT(STG.INVOICE_DT,"-",LPAD(LTRIM("0",STG.LOCATION_ID),4,0),"-",LPAD(STG.POS_WORKSTATION_ID,2,0),"-",LPAD(LTRIM("0",STG.POS_WORKSTATION_SEQ_NBR),4,0)) AS ORDER_ID,
	  CAST(UNIX_TIMESTAMP(CONCAT(STG.INVOICE_DT,STG.MIN_IDNT), "yyyyMMddHHmm") AS TIMESTAMP) AS FIRST_EFFECTIVE_TS,
	  CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000+0000" , "yyyy-MM-dd HH:mm:ss.SSS") AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
	  STG.POS_WORKSTATION_ID AS POS_WORKSTATION_ID,
    STG.PRIME_LINE_SEQ_NBR AS PRIME_LINE_SEQ_NBR,
	  STG.POS_WORKSTATION_SEQ_NBR AS POS_WORKSTATION_SEQ_NBR,
    DATE_FORMAT(CAST(UNIX_TIMESTAMP(STG.NOSALE_OVERRIDE_DT,"yyyyMMdd") AS TIMESTAMP),"yyyy-MM-dd") AS NOSALE_OVERRIDE_DT,
	  STG.OVERRIDE_REASON_ID as OVERRIDE_REASON_ID,
    STG.CASHIER_ID as CASHIER_ID,
    STG.ASSOCIATE_ID as ASSOCIATE_ID,
    STG.IS_ASSOCIATE_FLAG as IS_ASSOCIATE_FLAG,
    STG.EMPLOYEE_ID as EMPLOYEE_ID,
    "null" AS DTC_INVOICE_TYPE_CD,
    "null" AS DTC_TXN_TYPE_CD,
    "null" AS DTC_STATUS_CD,
    "null" AS DTC_REFERENCE_1,
    "null" AS DTC_COLLECTED_AMT,
    STG.TXN_SIGN as TXN_SIGN,
    CASE WHEN STG.TOTAL_QTY = '' OR STG.TOTAL_QTY IS NULL THEN '0' ELSE CAST(STG.TOTAL_QTY AS INT) END AS TOTAL_QTY,
    CASE WHEN STG.TOTAL_AMT = '' OR STG.TOTAL_AMT IS NULL THEN '0' ELSE CAST(STG.TOTAL_AMT AS DECIMAL(15,2)) END AS TOTAL_AMT,
	  CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
	  CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
    FROM L2_STAGE.INVOICE_STG STG
    LEFT OUTER JOIN L2_ANALYTICS.DAY D
    ON D.DAY_DT = DATE_FORMAT(CAST(UNIX_TIMESTAMP(STG.INVOICE_DT,"yyyyMMdd") AS TIMESTAMP),"yyyy-MM-dd")
    LEFT OUTER JOIN L2_ANALYTICS_TABLES.LOCATION L
    ON LPAD(LTRIM("0",STG.LOCATION_ID),4,0) = L.LOCATION_ID
    AND L.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
  );

  /*
  ******************************************************************

  ******************************************************************
  ************************* Change History *************************
  ******************************************************************

  ** Change   Date        Author    Description
  **  --      ----------  --------  --------------------------------
  **  01      04/10/2020  YAM-05  SQL file created

  ******************************************************************

  ** Author			: YAMUNA Team YAM-5
  ** Create Date		: April, 2020
  ** Purpose      	: Apply mapping rules and generate necessary
  					          columns for L2_ANALYTICS_TABLES.INVOICE table

  ******************************************************************

  ** Source Table 	: INVOICE_STG_VIEW1
  ** Target View  	: INVOICE_STG_VIEW2
  ** Lookup       	: L2_ANALYTICS_TABLES.ORDER_HEADER

  ** Transformations	: Division on Columns

  ******************************************************************
  */

  CREATE OR REPLACE TEMPORARY VIEW INVOICE_STG_VIEW2 AS (
   SELECT
  	  STG.INVOICE_KEY,
      STG.INVOICE_ID,
  	  STG.SOURCE_SYSTEM,
  	  STG.SOURCE_INVOICE_ID,
      STG.LOCATION_KEY ,
      STG.LOCATION_ID,
  	  STG.DAY_KEY,
  	  STG.INVOICE_DT,
  	  STG.INVOICE_CREATE_TS,
      COALESCE(OH.ORDER_HEADER_KEY,-1) AS ORDER_HEADER_KEY,
      STG.ORDER_ID,
  	  STG.FIRST_EFFECTIVE_TS,
  	  STG.LAST_EFFECTIVE_TS,
  	  STG.POS_WORKSTATION_ID,
      STG.PRIME_LINE_SEQ_NBR,
  	  STG.POS_WORKSTATION_SEQ_NBR,
      STG.NOSALE_OVERRIDE_DT,
  	  STG.OVERRIDE_REASON_ID,
      STG.CASHIER_ID,
      STG.ASSOCIATE_ID,
      STG.IS_ASSOCIATE_FLAG,
      STG.EMPLOYEE_ID,
      STG.DTC_INVOICE_TYPE_CD,
      STG.DTC_TXN_TYPE_CD,
      STG.DTC_STATUS_CD,
      STG.DTC_REFERENCE_1,
      STG.DTC_COLLECTED_AMT,
      STG.TXN_SIGN,
      CASE WHEN STG.TXN_SIGN = 'P' THEN STG.TOTAL_QTY/10000 ELSE STG.TOTAL_QTY/-10000 END AS TOTAL_QTY,
      CASE WHEN STG.TXN_SIGN = 'P' THEN STG.TOTAL_AMT/10000 ELSE STG.TOTAL_AMT/-10000 END AS TOTAL_AMT,
  	  STG.INSERT_TS,
  	  STG.UPDATE_TS
      FROM INVOICE_STG_VIEW1 STG
      LEFT OUTER JOIN L2_ANALYTICS.ORDER_HEADER OH
      ON  STG.INVOICE_DT = OH.ORDER_DT
      AND OH.SOURCE_SYSTEM IN ( 'CDW','REDIRON_POSLOG', 'NCR_POSLOG' )
      AND STG.ORDER_ID = OH.ORDER_HEADER_ID
      AND OH.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
    );

    /*
    ******************************************************************

    ******************************************************************
    ************************* Change History *************************
    ******************************************************************

    ** Change   Date        Author    Description
    **  --      ----------  --------  --------------------------------
    **  01      04/12/2020  YAM-05  SQL file created
    ******************************************************************

    ** Author			: YAMUNA Team YAM-5
    ** Create Date		: April, 2020
    ** Purpose      	: Apply grouping to remove duplicate records and aggregate the sum


    ******************************************************************

    ** Source View 	: INVOICE_STG_VIEW2
    ** Target View  	: GROUPED_INVOICE_STG_VIEW2


    ** Transformations	:
    					: Aggregation on tran_amt tran_qty
    					: grouping on INVOICE_ID,item_seq_no



    ******************************************************************
    */

    CREATE OR REPLACE TEMPORARY VIEW GROUPED_INVOICE_STG_VIEW2 AS (
    SELECT
        FIRST(STG1.INVOICE_KEY) AS INVOICE_KEY,
    	  STG1.INVOICE_ID AS INVOICE_ID,
    	  FIRST(STG1.SOURCE_SYSTEM) AS SOURCE_SYSTEM,
    	  FIRST(STG1.SOURCE_INVOICE_ID) AS SOURCE_INVOICE_ID,
    	  FIRST(STG1.LOCATION_KEY) AS LOCATION_KEY,
    	  FIRST(STG1.LOCATION_ID) AS LOCATION_ID,
    	  FIRST(STG1.DAY_KEY) AS DAY_KEY,
    	  FIRST(STG1.INVOICE_DT) AS INVOICE_DT,
    	  FIRST(STG1.INVOICE_CREATE_TS) AS INVOICE_CREATE_TS,
        FIRST(STG1.ORDER_HEADER_KEY) AS ORDER_HEADER_KEY,
        FIRST(STG1.ORDER_ID) AS ORDER_ID,
    	  FIRST(STG1.FIRST_EFFECTIVE_TS) AS FIRST_EFFECTIVE_TS,
    	  FIRST(STG1.LAST_EFFECTIVE_TS) AS LAST_EFFECTIVE_TS,
    	  FIRST(STG1.POS_WORKSTATION_ID) AS POS_WORKSTATION_ID,
    	  FIRST(STG1.POS_WORKSTATION_SEQ_NBR) AS POS_WORKSTATION_SEQ_NBR ,
    	  FIRST(STG1.NOSALE_OVERRIDE_DT) AS NOSALE_OVERRIDE_DT,
    	  FIRST(STG1.OVERRIDE_REASON_ID) AS OVERRIDE_REASON_ID,
    	  FIRST(STG1.CASHIER_ID) AS CASHIER_ID,
    	  FIRST(STG1.ASSOCIATE_ID) AS ASSOCIATE_ID,
    	  FIRST(STG1.IS_ASSOCIATE_FLAG) AS IS_ASSOCIATE_FLAG,
    	  FIRST(STG1.EMPLOYEE_ID) AS EMPLOYEE_ID,
        FIRST(STG1.DTC_INVOICE_TYPE_CD) AS DTC_INVOICE_TYPE_CD,
        FIRST(STG1.DTC_TXN_TYPE_CD) AS DTC_TXN_TYPE_CD,
        FIRST(STG1.DTC_STATUS_CD) AS DTC_STATUS_CD,
        FIRST(STG1.DTC_REFERENCE_1) AS DTC_REFERENCE_1,
        FIRST(STG1.DTC_COLLECTED_AMT) AS DTC_COLLECTED_AMT,
    	  SUM(STG1.TOTAL_QTY) AS TOTAL_QTY,
    	  SUM(STG1.TOTAL_AMT) AS TOTAL_AMT,
    	  FIRST(STG1.INSERT_TS) AS INSERT_TS,
    	  FIRST(STG1.UPDATE_TS) AS UPDATE_TS
        FROM
        (
        SELECT
          FIRST(INVOICE_KEY) AS INVOICE_KEY,
    	    INVOICE_ID AS INVOICE_ID,
    	    FIRST(SOURCE_SYSTEM) AS SOURCE_SYSTEM,
    	    FIRST(SOURCE_INVOICE_ID) AS SOURCE_INVOICE_ID,
    	    FIRST(LOCATION_KEY) AS LOCATION_KEY,
    	    FIRST(LOCATION_ID) AS LOCATION_ID,
    	    FIRST(DAY_KEY) AS DAY_KEY,
    	    FIRST(INVOICE_DT) AS INVOICE_DT,
    	    FIRST(INVOICE_CREATE_TS) AS INVOICE_CREATE_TS,
          FIRST(ORDER_HEADER_KEY) AS ORDER_HEADER_KEY,
          FIRST(ORDER_ID) AS ORDER_ID,
    	    FIRST(FIRST_EFFECTIVE_TS) AS FIRST_EFFECTIVE_TS,
    	    FIRST(LAST_EFFECTIVE_TS) AS LAST_EFFECTIVE_TS,
    	    FIRST(POS_WORKSTATION_ID) AS POS_WORKSTATION_ID,
    	    FIRST(POS_WORKSTATION_SEQ_NBR) AS POS_WORKSTATION_SEQ_NBR ,
    	    FIRST(NOSALE_OVERRIDE_DT) AS NOSALE_OVERRIDE_DT,
    	    FIRST(OVERRIDE_REASON_ID) AS OVERRIDE_REASON_ID,
    	    FIRST(CASHIER_ID) AS CASHIER_ID,
    	    FIRST(ASSOCIATE_ID) AS ASSOCIATE_ID,
    	    FIRST(IS_ASSOCIATE_FLAG) AS IS_ASSOCIATE_FLAG,
    	    FIRST(EMPLOYEE_ID) AS EMPLOYEE_ID,
          FIRST(DTC_INVOICE_TYPE_CD) AS DTC_INVOICE_TYPE_CD,
          FIRST(DTC_TXN_TYPE_CD) AS DTC_TXN_TYPE_CD,
          FIRST(DTC_STATUS_CD) AS DTC_STATUS_CD,
          FIRST(DTC_REFERENCE_1) AS DTC_REFERENCE_1,
          FIRST(DTC_COLLECTED_AMT) AS DTC_COLLECTED_AMT,
    	    FIRST(TOTAL_QTY) AS TOTAL_QTY,
    	    FIRST(TOTAL_AMT) AS TOTAL_AMT,
    	    FIRST(INSERT_TS) AS INSERT_TS,
    	    FIRST(UPDATE_TS) AS UPDATE_TS
         FROM INVOICE_STG_VIEW2
         GROUP BY INVOICE_ID, PRIME_LINE_SEQ_NBR
       ) STG1
       GROUP BY STG1.INVOICE_ID
     );


    /*
    ******************************************************************

    ******************************************************************
    ************************* Change History *************************
    ******************************************************************

    ** Change   Date        Author    Description
    **  --      ----------  --------  --------------------------------
    **  01      04/13/2020  YAM-05  SQL file created
    ******************************************************************


    ** Author			: YAMUNA Team YAM-5
    ** Create Date		: April, 2020
    ** Purpose      	: handling "coalesce" operations when loading and checking indifference in TARGET table and view table to correctly ingest the data

    ******************************************************************

    ** Source Table 	: GROUPED_INVOICE_STG_VIEW2
    ** Target View  	: INVOICE_STG_VIEW3
    ** Transformations	: handling "coalesce" operations when loading and checking indifference in TARGET table and view table to correctly ingest the data


    ******************************************************************
    */

    CREATE OR REPLACE TEMPORARY VIEW INVOICE_STG_VIEW3 AS (
      SELECT
    	  COALESCE(TARGET.INVOICE_KEY, STG1.INVOICE_KEY) AS INVOICE_KEY,
    	  STG1.INVOICE_ID,
    	  STG1.SOURCE_SYSTEM,
    	  STG1.SOURCE_INVOICE_ID,
    	  STG1.LOCATION_KEY,
    	  STG1.LOCATION_ID,
    	  STG1.DAY_KEY,
    	  STG1.INVOICE_DT,
    	  STG1.INVOICE_CREATE_TS,
        STG1.ORDER_HEADER_KEY,
        STG1.ORDER_ID,
    	  STG1.FIRST_EFFECTIVE_TS,
    	  STG1.LAST_EFFECTIVE_TS,
    	  STG1.POS_WORKSTATION_ID,
    	  STG1.POS_WORKSTATION_SEQ_NBR,
    	  STG1.NOSALE_OVERRIDE_DT,
    	  STG1.OVERRIDE_REASON_ID,
    	  STG1.CASHIER_ID,
    	  STG1.ASSOCIATE_ID,
    	  STG1.IS_ASSOCIATE_FLAG,
    	  STG1.EMPLOYEE_ID,
        STG1.DTC_INVOICE_TYPE_CD,
        STG1.DTC_TXN_TYPE_CD,
        STG1.DTC_STATUS_CD,
        STG1.DTC_REFERENCE_1,
        STG1.DTC_COLLECTED_AMT,
    	  STG1.TOTAL_QTY,
    	  STG1.TOTAL_AMT,
    	  STG1.INSERT_TS,
    	  STG1.UPDATE_TS
        FROM GROUPED_INVOICE_STG_VIEW2 STG1
        LEFT OUTER JOIN L2_ANALYTICS_TABLES.INVOICE TARGET
      	ON STG1.INVOICE_ID = TARGET.INVOICE_ID
    	  AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
    	  AND STG1.SOURCE_SYSTEM = TARGET.SOURCE_SYSTEM
     );

     /*
     ******************************************************************

     ******************************************************************
     ************************* Change History *************************
     ******************************************************************

     ** Change   Date        Author    Description
     **  --      ----------  --------  --------------------------------
     **  01      04/14/2020  YAM-05  SQL file created
     ******************************************************************


     ** Author			: YAMUNA Team YAM-5
     ** Create Date		: April, 2020
     ** Purpose      	: Generate surrogate keys

     ******************************************************************

     ** Source Table 	: INVOICE_STG_VIEW3
     ** Target View  	: INVOICE_STG_VIEW4

     ** Transformations	:  Generate surrogate keys on INVOICE_KEY


     ******************************************************************
     */

     CREATE OR REPLACE TEMPORARY VIEW INVOICE_STG_VIEW4 AS (
       SELECT
     	  CASE
     		  WHEN STG1.INVOICE_KEY = 0 THEN
     		  	ROW_NUMBER() OVER (ORDER BY STG1.INVOICE_KEY, STG1.INVOICE_ID) + COALESCE(MAX_INVOICE_KEY, 0)
     		  ELSE STG1.INVOICE_KEY
     	  END AS INVOICE_KEY,
     	  STG1.INVOICE_ID,
     	  STG1.SOURCE_SYSTEM,
     	  STG1.SOURCE_INVOICE_ID,
     	  STG1.LOCATION_KEY,
     	  STG1.LOCATION_ID,
     	  STG1.DAY_KEY,
     	  STG1.INVOICE_DT,
     	  STG1.INVOICE_CREATE_TS,
        STG1.ORDER_HEADER_KEY,
        STG1.ORDER_ID,
     	  STG1.FIRST_EFFECTIVE_TS,
     	  STG1.LAST_EFFECTIVE_TS,
     	  STG1.POS_WORKSTATION_ID,
     	  STG1.POS_WORKSTATION_SEQ_NBR,
     	  STG1.NOSALE_OVERRIDE_DT,
     	  STG1.OVERRIDE_REASON_ID,
     	  STG1.CASHIER_ID,
     	  STG1.ASSOCIATE_ID,
     	  STG1.IS_ASSOCIATE_FLAG,
     	  STG1.EMPLOYEE_ID,
        STG1.DTC_INVOICE_TYPE_CD,
        STG1.DTC_TXN_TYPE_CD,
        STG1.DTC_STATUS_CD,
        STG1.DTC_REFERENCE_1,
        STG1.DTC_COLLECTED_AMT,
     	  STG1.TOTAL_QTY,
     	  STG1.TOTAL_AMT,
     	  STG1.INSERT_TS,
     	  STG1.UPDATE_TS
         FROM INVOICE_STG_VIEW3 STG1
         CROSS JOIN
     	  (
     		   SELECT MAX(INVOICE_KEY) AS MAX_INVOICE_KEY FROM L2_ANALYTICS_TABLES.INVOICE WHERE INVOICE_KEY <> -1
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
      ******************************************************************


      ** Author			: YAMUNA Team YAM-5
      ** Create Date		: April, 2020
      ** Purpose      	: Generate surrogate keys

      ******************************************************************

      ** Source Table 	: INVOICE_STG_VIEW4
      ** Target View  	: INVOICE_STG_VIEW5

      ** Transformations	:  Historical Data Ingestion using Lead function on FIRST_EFFECTIVE_TS

      ******************************************************************
      */

      CREATE OR REPLACE TEMPORARY VIEW V_INVOICE_STG_VIEW5 AS (
        SELECT
      	  INVOICE_KEY,
      	  INVOICE_ID,
      	  SOURCE_SYSTEM,
      	  SOURCE_INVOICE_ID,
          NULL AS SUB_CHANNEL_CD,
          NULL AS CONCEPT_CD,
      	  LOCATION_KEY,
      	  LOCATION_ID,
      	  DAY_KEY,
      	  INVOICE_DT,
      	  INVOICE_CREATE_TS,
          ORDER_HEADER_KEY,
          ORDER_ID AS ORDER_HEADER_ID,
          ORDER_ID,
          NULL AS GLOBAL_TXN_ID,
      	  FIRST_EFFECTIVE_TS,
      	  COALESCE(LEAD(FIRST_EFFECTIVE_TS) OVER (PARTITION BY INVOICE_KEY ORDER BY FIRST_EFFECTIVE_TS),LAST_EFFECTIVE_TS) AS LAST_EFFECTIVE_TS,
      	  POS_WORKSTATION_ID,
      	  POS_WORKSTATION_SEQ_NBR,
          NULL AS WSSPL_PURCHASE_ORDER_ID,
      	  NOSALE_OVERRIDE_DT,
      	  OVERRIDE_REASON_ID,
      	  CASHIER_ID,
      	  ASSOCIATE_ID,
      	  IS_ASSOCIATE_FLAG,
      	  EMPLOYEE_ID,
          NULL AS REPLACEMENT_ORDER_FLAG,
          NULL AS INTERNAL_FLAG,
          DTC_INVOICE_TYPE_CD,
          NULL AS DTC_INVOICE_FULFILLMENT_TYPE_CD,
          NULL AS DTC_INVOICE_REVISED_TYPE_CD,
          DTC_TXN_TYPE_CD,
          DTC_STATUS_CD,
          DTC_REFERENCE_1,
          DTC_COLLECTED_AMT,
      	  TOTAL_QTY,
      	  TOTAL_AMT,
          NULL AS CURRENCY_CD,
      	  INSERT_TS,
      	  UPDATE_TS
          FROM INVOICE_STG_VIEW4
       );

	   
	   /*
       ** Change   Date        Author    Description
       **  --      ----------  --------  --------------------------------
       **  01      10/04/2021  THAM-761  Materialize invoice for Snowflake transmission
       */
	   
	   INSERT OVERWRITE table  L2_STAGE.INVOICE_STG_VIEW5 SELECT * FROM V_INVOICE_STG_VIEW5;


       /*
       ******************************************************************

       ******************************************************************
       ************************* Change History *************************
       ******************************************************************

       ** Change   Date        Author    Description
       **  --      ----------  --------  --------------------------------
       **  01      04/24/2020  YAM-05  SQL file created
       ******************************************************************


       ** Author			: YAMUNA Team YAM-5
       ** Create Date		: April, 2020
       ** Purpose      	: Merge into L2_ANALYTICS_TABLES.INVOICE

       ******************************************************************

       ** Source View 	: INVOICE_STG_VIEW4
       ** Target Table  	: L2_ANALYTICS_TABLES.INVOICE


       ** Transformations	:  Merge into L2_ANALYTICS_TABLES.INVOICE


       ******************************************************************
       */

       MERGE INTO L2_ANALYTICS_TABLES.INVOICE TARGET USING
        (
       	 SELECT STG1_1.INVOICE_KEY AS MERGE_KEY, STG1_1.* FROM L2_STAGE.INVOICE_STG_VIEW5 STG1_1
       	 UNION
       	 SELECT NULL AS MERGE_KEY, STG1_2.* FROM L2_STAGE.INVOICE_STG_VIEW5 STG1_2
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
			 CREATE_USER_ID,                   /* HINATUAN */
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
			NULL,                               /* HINATUAN */
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
