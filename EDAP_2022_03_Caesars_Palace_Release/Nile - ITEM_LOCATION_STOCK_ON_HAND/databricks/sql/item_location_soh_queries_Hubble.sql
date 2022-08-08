/*
******************************************************************

******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      04/25/2020  YAM-37    SQL file created
******************************************************************


** Author		     	: YAMUNA Team YAM-37
** Create Date		: April, 2020
** Purpose      	: Apply mapping rules and generate necessary
					          columns for L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND table

******************************************************************

** Source Table 	: L2_STAGE.ITEM_LOCATION_STOCK_ON_HAND_STAGE
** Target View  	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW1
** Lookup       	: L2_ANALYTICS_TABLES.ITEM
******************************************************************
 */

 SET spark.sql.broadcastTimeout = 8000;

    CREATE OR REPLACE TEMPORARY VIEW ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW1 AS (
  SELECT
  STG.CHANNEL_CD,
  STG.CHANNEL_TYPE_CD,
  STG.CHANNEL_DESC,
  COALESCE(I.ITEM_KEY , -1) as ITEM_KEY,
  STG.ITEM_ID  AS ITEM_ID,
  STG.LOCATION_ID  AS LOCATION_ID,
  STG.LOCATION_TYPE_CD  AS LOCATION_TYPE_CD,
  STG.FIRST_EFFECTIVE_TS AS FIRST_EFFECTIVE_TS,
  STG.STOCK_ON_HAND_QTY  AS STOCK_ON_HAND_QTY,
  STG.UNIT_COST_AMT as UNIT_COST_AMT,
  STG.AVG_COST_AMT   as AVG_COST_AMT,
  STG.SOH_UPDATE_DATETIME as SOH_UPDATE_DT,
  STG.TRANSFERRED_EXPECTED_QTY as TRANSFERRED_EXPECTED_QTY,
  STG.WSI_RESERVED_QTY as WSI_RESERVED_QTY,
  STG.CUSTOMER_RESERVE_QTY as CUSTOMER_RESERVE_QTY,
  STG.IN_TRANSIT_QTY as IN_TRANSIT_QTY,
  STG.PACK_COMPONENT_IN_TRANSIT_QTY as PACK_COMPONENT_IN_TRANSIT_QTY,
  STG.PACK_COMPONENT_SOH_QTY as PACK_COMPONENT_SOH_QTY
   FROM L2_STAGE.ITEM_LOCATION_STOCK_ON_HAND_STAGE STG
     Left outer join
  (select * from L2_ANALYTICS.ITEM  t where t.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000' and t.MARKET_CD = 'USA') I
     ON STG.ITEM_ID =I.ITEM_ID
 );

/*
******************************************************************

******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      04/25/2020  YAM-37  SQL file created
******************************************************************


** Author			    : YAMUNA Team YAM-37
** Create Date		: April, 2020
** Purpose      	: handling "coalesce" operations when loading,
                   * date conversion

******************************************************************

** Source Table 	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW1
** Target View  	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW2
** Lookup       	:
** Transformations	:
       *column concatenation and date conversion
       *LOCATION_KEY look up and column type casting
******************************************************************
*/


    CREATE OR REPLACE TEMPORARY VIEW ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW2 AS (
  SELECT
   0 as ITEM_LOCATION_STOCK_ON_HAND_KEY,
   STG.CHANNEL_CD,
   STG.CHANNEL_TYPE_CD,
   STG.CHANNEL_DESC,
   STG.ITEM_KEY,
   STG.ITEM_ID,
   STG.LOCATION_ID,
   STG.LOCATION_TYPE_CD,
   STG.STOCK_ON_HAND_QTY,
   STG.UNIT_COST_AMT,
   STG.AVG_COST_AMT,
   STG.SOH_UPDATE_DT,
   STG.TRANSFERRED_EXPECTED_QTY,
   STG.WSI_RESERVED_QTY,
   STG.CUSTOMER_RESERVE_QTY,
   STG.IN_TRANSIT_QTY,
   STG.PACK_COMPONENT_IN_TRANSIT_QTY,
   STG.PACK_COMPONENT_SOH_QTY,
   COALESCE(t.LOCATION_KEY , -1) as LOCATION_KEY,
   COALESCE(CAST(UNIX_TIMESTAMP(STG.FIRST_EFFECTIVE_TS, "yyyy-MM-dd HH:mm:ss.SSS") AS TIMESTAMP), (CAST(UNIX_TIMESTAMP(SOH_UPDATE_DT, 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP))) AS FIRST_EFFECTIVE_TS,
   CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000+0000" , "yyyy-MM-dd HH:mm:ss.SSS") AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
   CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
   CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
    FROM ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW1 STG
    left outer join (select CAST(IF(LOCATION_ID RLIKE '^[0-9]*$', CAST(LOCATION_ID AS INT), LOCATION_ID ) AS STRING) AS LOCATION_ID
                            ,LOCATION_KEY,LOCATION_TYPE_CD from L2_ANALYTICS.LOCATION loc where loc.LAST_EFFECTIVE_TS='9999-12-31T23:59:59.000+0000') t
      on LPAD(t.LOCATION_id,4,0)  = STG.LOCATION_ID and t.location_type_cd = STG.LOCATION_TYPE_CD
  );


/*
******************************************************************

******************************************************************
************************* Change History *************************
******************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  --------------------------------
**  01      04/20/2020  YAM-37  SQL file created
******************************************************************


** Author			    : YAMUNA Team YAM-38
** Create Date		: April, 2020
** Purpose      	: handling "coalesce" operations when loading

******************************************************************

** Source Table 	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW2
** Target View  	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW4
** Transformations	: handling "coalesce" operations when loading and checking indifference in TARGET table and view table to correctly ingest the data


******************************************************************
*/


  CREATE OR REPLACE TEMPORARY VIEW ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW3 AS (
    SELECT
    	COALESCE(TARGET.ITEM_LOCATION_STOCK_ON_HAND_KEY, STG1.ITEM_LOCATION_STOCK_ON_HAND_KEY) AS ITEM_LOCATION_STOCK_ON_HAND_KEY,
 	 STG1.CHANNEL_CD,
     STG1.CHANNEL_TYPE_CD,
     STG1.CHANNEL_DESC,
	 STG1.ITEM_KEY,
     STG1.ITEM_ID,
     STG1.LOCATION_ID,
     STG1.LOCATION_KEY,
     STG1.LOCATION_TYPE_CD,
     STG1.STOCK_ON_HAND_QTY,
     STG1.UNIT_COST_AMT,
     STG1.AVG_COST_AMT,
     STG1.FIRST_EFFECTIVE_TS,
     STG1.LAST_EFFECTIVE_TS,
     STG1.INSERT_TS,
     STG1.UPDATE_TS,
     STG1.SOH_UPDATE_DT,
     STG1.TRANSFERRED_EXPECTED_QTY,
     STG1.WSI_RESERVED_QTY,
     STG1.CUSTOMER_RESERVE_QTY,
     STG1.IN_TRANSIT_QTY,
     STG1.PACK_COMPONENT_IN_TRANSIT_QTY,
     STG1.PACK_COMPONENT_SOH_QTY
        FROM ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW2 STG1
         LEFT OUTER JOIN L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND TARGET
        ON
          STG1.LOCATION_ID =TARGET.LOCATION_ID
       AND STG1.ITEM_ID = TARGET.ITEM_ID
       AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
     WHERE
      TARGET.ITEM_LOCATION_STOCK_ON_HAND_KEY is null
      OR COALESCE(STG1.CHANNEL_CD, '0') <> COALESCE(TARGET.CHANNEL_CD, '0')
	  OR COALESCE(STG1.CHANNEL_TYPE_CD, '0') <> COALESCE(TARGET.CHANNEL_TYPE_CD, '0')
	  OR COALESCE(STG1.CHANNEL_DESC, '0') <> COALESCE(TARGET.CHANNEL_DESC, '0')
	  OR COALESCE(STG1.ITEM_KEY, '0') <> COALESCE(TARGET.ITEM_KEY, '0')
      OR COALESCE(STG1.LOCATION_TYPE_CD, '0') <> COALESCE(TARGET.LOCATION_TYPE_CD, '0')
      OR COALESCE(STG1.LOCATION_KEY, '0') <> COALESCE(TARGET.LOCATION_KEY, '0')
      OR COALESCE(STG1.STOCK_ON_HAND_QTY, '0') <> COALESCE(TARGET.STOCK_ON_HAND_QTY, '0')
      OR COALESCE(STG1.UNIT_COST_AMT, '0') <> COALESCE(TARGET.UNIT_COST_AMT, '0')
      OR COALESCE(STG1.AVG_COST_AMT, '0') <> COALESCE(TARGET.AVG_COST_AMT, '0')
      OR COALESCE(STG1.TRANSFERRED_EXPECTED_QTY, '0') <> COALESCE(TARGET.TRANSFERRED_EXPECTED_QTY, '0')
      OR COALESCE(STG1.WSI_RESERVED_QTY, '0') <> COALESCE(TARGET.WSI_RESERVED_QTY, '0')
      OR COALESCE(STG1.CUSTOMER_RESERVE_QTY, '0') <> COALESCE(TARGET.CUSTOMER_RESERVE_QTY, '0')
      OR COALESCE(STG1.IN_TRANSIT_QTY, '0') <> COALESCE(TARGET.IN_TRANSIT_QTY, '0')
      OR COALESCE(STG1.PACK_COMPONENT_IN_TRANSIT_QTY, '0') <> COALESCE(TARGET.PACK_COMPONENT_IN_TRANSIT_QTY, '0')
      OR COALESCE(STG1.PACK_COMPONENT_SOH_QTY, '0') <> COALESCE(TARGET.PACK_COMPONENT_SOH_QTY, '0')

 );


 /*
 ******************************************************************

 ******************************************************************
 ************************* Change History *************************
 ******************************************************************

 ** Change   Date        Author    Description
 **  --      ----------  --------  --------------------------------
 **  01      04/25/2020  YAM-37     SQL file created
 ******************************************************************


 ** Author			  : YAMUNA Team YAM-37
 ** Create Date		: April, 2020
 ** Purpose      	: Generate surrogate keys

 ******************************************************************

 ** Source Table 	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW3
 ** Target View  	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW4

 ** Transformations	:  Generate surrogate keys on ITEM_LOCATION_STOCK_ON_HAND_KEY


 ******************************************************************
 */


 CREATE OR REPLACE TEMPORARY VIEW ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW4 AS (
   SELECT
 	  CASE WHEN STG1.ITEM_LOCATION_STOCK_ON_HAND_KEY = 0 THEN
 	   ROW_NUMBER() OVER (ORDER BY STG1.ITEM_LOCATION_STOCK_ON_HAND_KEY, STG1.ITEM_ID, STG1.LOCATION_ID) + COALESCE(MAX_ITEM_LOCATION_STOCK_ON_HAND_KEY, 0)
 	  ELSE STG1.ITEM_LOCATION_STOCK_ON_HAND_KEY
 	   END AS ITEM_LOCATION_STOCK_ON_HAND_KEY,
 	 STG1.CHANNEL_CD,
     STG1.CHANNEL_TYPE_CD,
     STG1.CHANNEL_DESC,
	STG1.ITEM_KEY,
    STG1.ITEM_ID,
    STG1.LOCATION_ID,
    STG1.LOCATION_KEY,
    STG1.LOCATION_TYPE_CD,
    STG1.STOCK_ON_HAND_QTY,
    STG1.UNIT_COST_AMT,
    STG1.AVG_COST_AMT,
    STG1.SOH_UPDATE_DT,
    STG1.TRANSFERRED_EXPECTED_QTY,
    STG1.WSI_RESERVED_QTY,
    STG1.CUSTOMER_RESERVE_QTY,
    STG1.IN_TRANSIT_QTY,
    STG1.PACK_COMPONENT_IN_TRANSIT_QTY,
    STG1.PACK_COMPONENT_SOH_QTY,
    STG1.FIRST_EFFECTIVE_TS,
    STG1.LAST_EFFECTIVE_TS,
    STG1.INSERT_TS,
    STG1.UPDATE_TS
      FROM ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW3 STG1
      CROSS JOIN
 	  (
 		    SELECT MAX(ITEM_LOCATION_STOCK_ON_HAND_KEY) AS MAX_ITEM_LOCATION_STOCK_ON_HAND_KEY
         FROM L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND WHERE ITEM_LOCATION_STOCK_ON_HAND_KEY <> -1
 	  )  TARGET_MAX
  );



  /*
  ******************************************************************

  ******************************************************************
  ************************* Change History *************************
  ******************************************************************

  ** Change   Date        Author    Description
  **  --      ----------  --------  --------------------------------
  **  01      04/24/2020  YAM-37  SQL file created
  ******************************************************************


  ** Author		    	: YAMUNA Team YAM-37
  ** Create Date		: April, 2020
  ** Purpose      	: lead function generation

  ******************************************************************

  ** Source Table 	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW4
  ** Target View  	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW5

  ** Transformations	:  Historical Data Ingestion using Lead function on FIRST_EFFECTIVE_TS

  ******************************************************************
  */



  CREATE OR REPLACE TEMPORARY VIEW ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW5 AS (
  SELECT
  ITEM_LOCATION_STOCK_ON_HAND_KEY,
  CHANNEL_CD,
  CHANNEL_TYPE_CD,
  CHANNEL_DESC,
  ITEM_KEY,
  ITEM_ID,
  LOCATION_ID,
  LOCATION_KEY,
  LOCATION_TYPE_CD,
  STOCK_ON_HAND_QTY,
  UNIT_COST_AMT,
  AVG_COST_AMT,
  SOH_UPDATE_DT,
  TRANSFERRED_EXPECTED_QTY,
  WSI_RESERVED_QTY,
  CUSTOMER_RESERVE_QTY,
  IN_TRANSIT_QTY,
  PACK_COMPONENT_IN_TRANSIT_QTY,
  PACK_COMPONENT_SOH_QTY,
  FIRST_EFFECTIVE_TS,
   COALESCE(LEAD(FIRST_EFFECTIVE_TS) OVER (PARTITION BY ITEM_LOCATION_STOCK_ON_HAND_KEY ORDER BY FIRST_EFFECTIVE_TS),LAST_EFFECTIVE_TS) AS LAST_EFFECTIVE_TS,
  INSERT_TS,
  UPDATE_TS
    FROM ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW4
    );


    /*
    ******************************************************************

    ******************************************************************
    ************************* Change History *************************
    ******************************************************************

    ** Change   Date        Author    Description
    **  --      ----------  --------  --------------------------------
    **  01      04/24/2020  YAM-37   SQL file created
    ******************************************************************


    ** Author		     	: YAMUNA Team YAM-37
    ** Create Date		: April, 2020
    ** Purpose      	: Merge into L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND

    ******************************************************************

    ** Source View 	: ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW5
    ** Target Table  	: L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND


    ** Transformations	:  Merge into L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND


    ******************************************************************
    */


    MERGE INTO L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND TARGET USING
     (
    	 SELECT STG1_1.ITEM_LOCATION_STOCK_ON_HAND_KEY AS MERGE_KEY, STG1_1.* FROM ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW5 STG1_1
    	   UNION
    	 SELECT NULL AS MERGE_KEY, STG1_2.* FROM ITEM_LOCATION_STOCK_ON_HAND_STAGE_VIEW5 STG1_2
     ) STG1 ON TARGET.ITEM_LOCATION_STOCK_ON_HAND_KEY = STG1.MERGE_KEY
         WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000' THEN
    	  UPDATE SET
    		  LAST_EFFECTIVE_TS = FROM_UNIXTIME(CAST(STG1.FIRST_EFFECTIVE_TS AS LONG) - 1),
    		  UPDATE_TS = STG1.UPDATE_TS
       WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
    	    INSERT (
           ITEM_LOCATION_STOCK_ON_HAND_KEY,
    	   CHANNEL_CD,
           CHANNEL_TYPE_CD,
           CHANNEL_DESC,
		   ITEM_KEY,
           ITEM_ID,
           LOCATION_ID,
           LOCATION_KEY,
           LOCATION_TYPE_CD,
           STOCK_ON_HAND_QTY,
           UNIT_COST_AMT,
           AVG_COST_AMT,
           SOH_UPDATE_DT,
           TRANSFERRED_EXPECTED_QTY,
           WSI_RESERVED_QTY,
           CUSTOMER_RESERVE_QTY,
           IN_TRANSIT_QTY,
           PACK_COMPONENT_IN_TRANSIT_QTY,
           PACK_COMPONENT_SOH_QTY,
           FIRST_EFFECTIVE_TS,
           LAST_EFFECTIVE_TS,
           INSERT_TS,
           UPDATE_TS )  VALUES (
            STG1.ITEM_LOCATION_STOCK_ON_HAND_KEY,
 	        STG1.CHANNEL_CD,
            STG1.CHANNEL_TYPE_CD,
            STG1.CHANNEL_DESC,
            STG1.ITEM_KEY,
            STG1.ITEM_ID,
            STG1.LOCATION_ID,
            STG1.LOCATION_KEY,
            STG1.LOCATION_TYPE_CD,
            STG1.STOCK_ON_HAND_QTY,
            STG1.UNIT_COST_AMT,
            STG1.AVG_COST_AMT,
            STG1.SOH_UPDATE_DT,
            STG1.TRANSFERRED_EXPECTED_QTY,
            STG1.WSI_RESERVED_QTY,
            STG1.CUSTOMER_RESERVE_QTY,
            STG1.IN_TRANSIT_QTY,
            STG1.PACK_COMPONENT_IN_TRANSIT_QTY,
            STG1.PACK_COMPONENT_SOH_QTY,
            STG1.FIRST_EFFECTIVE_TS,
            STG1.LAST_EFFECTIVE_TS,
            STG1.INSERT_TS,
            STG1.UPDATE_TS
    	);
  OPTIMIZE L2_ANALYTICS_TABLES.ITEM_LOCATION_STOCK_ON_HAND ZORDER BY LAST_EFFECTIVE_TS;