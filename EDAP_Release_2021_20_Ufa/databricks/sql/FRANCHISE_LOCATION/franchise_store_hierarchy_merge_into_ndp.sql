		set spark.sql.crossJoin.enabled=true;

		CREATE OR REPLACE TEMP VIEW FRANCHISE_STORE_HIERARCHY_VIEW1
        AS
		SELECT
            COALESCE(L.LOCATION_KEY, 0) as LOCATION_KEY,
            TRIM(CONCAT('FLP', CAST(CAST(LP.location_id AS INTEGER) AS STRING))) AS LOCATION_ID,
            null as SOURCE_LOCATION_ID,
            'GLOBAL_FRANCHISE_LIVERPOOL' as SOURCE_SYSTEM,
            null as CONCEPT_CD,
            null as CONCEPT_DESC,  
            -1 as DISTRICT_KEY,
            null as DISTRICT_ID,
            null as DISTRICT_DESC, 
            -1 as REGION_KEY,
            null as REGION_ID,
            null as REGION_DESC,
            trim(LP.channel) AS CHANNEL_CD,
            null AS CHANNEL_DESC,
            null as COMPANY_CD,
            null as COMPANY_DESC,
            trim(LP.location_type) as LOCATION_TYPE_CD,
            trim(LP.COUNTRY) as COUNTRY_CD,
            null as COUNTRY_SUBDIVISION_CD,
            trim(LP.state) as STATE_CD, 
            null as CURRENCY_CD,
            null as VIRTUAL_WAREHOUSE_CD,
            null as PHYSICAL_WAREHOUSE_CD,
            CAST(trim(LP.store_open_date) AS TIMESTAMP) AS FIRST_EFFECTIVE_TS, 
            CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000", 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
            trim(LP.location_name) as LOCATION_DESC,
            trim(LP.location_name) as DISPLAY_NAME,
            null as MALL_NAME,
            null as SHORT_NAME,
            null as MANAGER_NAME,
            null as ADDRESS_LINE_1,
            null as ADDRESS_LINE_2,
            null as ADDRESS_LINE_3,
            null as CITY,
            null as COUNTY,
            null as POSTAL_CD,
            null as ADDRESS_VERIFICATION_STATUS,
            null as CLASSIFICATION,
            null as LATTITUDE_NBR,
            null as LONGITUDE_NBR,
            null as TIME_ZONE,
            null as PHONE_COUNTRY_CODE,
            null as PHONE_NBR,
            null as PHONE_EXTENSION,
            null as PHONE_TYPE,
            null as EMAIL_ADDRESS,
            null as FAX_COUNTRY_CODE,
            null as FAX_NBR,
            null as FAX_EXTENSION,
            null as PICKUP_LOCATION_CD,
            null as PICKUP_ENABLED_FLAG,
            null as OUTLET_FLAG,
            null as POPUP_FLAG,
            null as STORE_IN_STORE_FLAG,
            null as SHIP_TO_STORE_ENABLED_FLAG,
            null as STOCKHOLD_FLAG,
            CAST(trim(LP.store_open_date) AS DATE) AS OPEN_DT,
            null as CLOSE_DT,
            null as TOTAL_AREA,
            null as SELLING_AREA,
            null as LINEAR_DISTANCE,
            null as PROMOTION_ZONE_CD,
            null as PROMOTION_ZONE_DESC,
            null as DEFAULT_WH,
            null as BREAK_PAC_FLAG,
            null as FRANCHISE_CD,
            null as MARKETING_CONCEPT_CD,
            CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
            CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
        FROM L2_STAGE.FRANCHISE_LIVERPOOL_STORE_HIERARCHY_STAGE LP
            LEFT OUTER JOIN l2_analytics_tables.LOCATION L
            ON trim(lower(LP.location_id)) = trim(lower(L.LOCATION_ID))
            AND L.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
            AND L.SOURCE_SYSTEM = 'GLOBAL_FRANCHISE'
            WHERE COALESCE(LP.location_id, '0') <> COALESCE(L.LOCATION_ID, '0') 
            OR COALESCE(LP.channel, '0') <> COALESCE(L.CHANNEL_CD, '0')
            OR COALESCE(LP.location_type, '0') <> COALESCE(L.LOCATION_TYPE_CD, '0')
            OR COALESCE(LP.COUNTRY, '0') <> COALESCE(L.COUNTRY_CD, '0')
            OR COALESCE(LP.location_name, '0') <> COALESCE(L.LOCATION_DESC, '0')
            OR COALESCE(LP.store_open_date, '0') <> COALESCE(L.OPEN_DT, '0')

        UNION
	    SELECT
            COALESCE(L.LOCATION_KEY, 0) as LOCATION_KEY,
            TRIM(CONCAT('FAL', CAST(CAST(AL.location_id AS INTEGER) AS STRING))) AS LOCATION_ID,
            null as SOURCE_LOCATION_ID,
            'GLOBAL_FRANCHISE_ALSHAYA' as SOURCE_SYSTEM,
            null as CONCEPT_CD,
            null as CONCEPT_DESC,  
            -1 as DISTRICT_KEY,
            null as DISTRICT_ID,
            null as DISTRICT_DESC, 
            -1 as REGION_KEY,
            null as REGION_ID,
            null as REGION_DESC,
            trim(AL.channel) AS CHANNEL_CD,
            null AS CHANNEL_DESC,
            null as COMPANY_CD,
            null as COMPANY_DESC,
            trim(AL.location_type) as LOCATION_TYPE_CD,
            trim(AL.COUNTRY) as COUNTRY_CD,
            null as COUNTRY_SUBDIVISION_CD,
            trim(AL.STATE) as STATE_CD, 
            trim(AL.CURRENCY) as CURRENCY_CD,
            null as VIRTUAL_WAREHOUSE_CD,
            null as PHYSICAL_WAREHOUSE_CD,
            CAST(trim(AL.store_open_date) AS TIMESTAMP) AS FIRST_EFFECTIVE_TS, 
            CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000", 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
            trim(AL.location_name) as LOCATION_DESC,
            trim(AL.location_name) as DISPLAY_NAME,
            null as MALL_NAME,
            null as SHORT_NAME,
            null as MANAGER_NAME,
            null as ADDRESS_LINE_1,
            null as ADDRESS_LINE_2,
            null as ADDRESS_LINE_3,
            null as CITY,
            null as COUNTY,
            null as POSTAL_CD,
            null as ADDRESS_VERIFICATION_STATUS,
            null as CLASSIFICATION,
            null as LATTITUDE_NBR,
            null as LONGITUDE_NBR,
            null as TIME_ZONE,
            null as PHONE_COUNTRY_CODE,
            null as PHONE_NBR,
            null as PHONE_EXTENSION,
            null as PHONE_TYPE,
            null as EMAIL_ADDRESS,
            null as FAX_COUNTRY_CODE,
            null as FAX_NBR,
            null as FAX_EXTENSION,
            null as PICKUP_LOCATION_CD,
            null as PICKUP_ENABLED_FLAG,
            null as OUTLET_FLAG,
            null as POPUP_FLAG,
            null as STORE_IN_STORE_FLAG,
            null as SHIP_TO_STORE_ENABLED_FLAG,
            null as STOCKHOLD_FLAG,
            CAST(trim(AL.store_open_date) AS DATE) AS OPEN_DT,
            null as CLOSE_DT,
            null as TOTAL_AREA,
            null as SELLING_AREA,
            null as LINEAR_DISTANCE,
            null as PROMOTION_ZONE_CD,
            null as PROMOTION_ZONE_DESC,
            null as DEFAULT_WH,
            null as BREAK_PAC_FLAG,
            null as FRANCHISE_CD,
            null as MARKETING_CONCEPT_CD,
            CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
            CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
        FROM L2_STAGE.FRANCHISE_ALSHAYA_STORE_HIERARCHY_STAGE AL
            LEFT OUTER JOIN l2_analytics_tables.LOCATION L
            ON trim(lower(AL.location_id)) = trim(lower(L.LOCATION_ID))
            AND L.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
            AND L.SOURCE_SYSTEM = 'GLOBAL_FRANCHISE'
            WHERE COALESCE(AL.location_id, '0') <> COALESCE(L.LOCATION_ID, '0') 
            OR COALESCE(AL.channel, '0') <> COALESCE(L.CHANNEL_CD, '0')
            OR COALESCE(AL.location_type, '0') <> COALESCE(L.LOCATION_TYPE_CD, '0')
            OR COALESCE(AL.COUNTRY, '0') <> COALESCE(L.COUNTRY_CD, '0')
            OR COALESCE(AL.CURRENCY, '0') <> COALESCE(L.CURRENCY_CD, '0')
            OR COALESCE(AL.location_name, '0') <> COALESCE(L.LOCATION_DESC, '0')
            OR COALESCE(AL.store_open_date, '0') <> COALESCE(L.OPEN_DT, '0')

        UNION
	    SELECT
            COALESCE(L.LOCATION_KEY, 0) as LOCATION_KEY,
            TRIM(CONCAT('FRL', CAST(RL.location_id AS STRING))) AS LOCATION_ID,
            null as SOURCE_LOCATION_ID,
            'GLOBAL_FRANCHISE_RELIANCE' as SOURCE_SYSTEM,
            null as CONCEPT_CD,
            null as CONCEPT_DESC,  
            -1 as DISTRICT_KEY,
            null as DISTRICT_ID,
            null as DISTRICT_DESC, 
            -1 as REGION_KEY,
            null as REGION_ID,
            null as REGION_DESC,
            trim(RL.channel) AS CHANNEL_CD,
            null AS CHANNEL_DESC,
            null as COMPANY_CD,
            null as COMPANY_DESC,
            trim(RL.location_type) as LOCATION_TYPE_CD,
            trim(RL.COUNTRY) as COUNTRY_CD,
            null as COUNTRY_SUBDIVISION_CD,
            trim(RL.state) as STATE_CD, 
            null as CURRENCY_CD,
            null as VIRTUAL_WAREHOUSE_CD,
            null as PHYSICAL_WAREHOUSE_CD,
            CAST(trim(RL.store_open_date) AS TIMESTAMP) AS FIRST_EFFECTIVE_TS, 
            CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000", 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
            trim(RL.location_name) as LOCATION_DESC,
            trim(RL.location_name) as DISPLAY_NAME,
            null as MALL_NAME,
            null as SHORT_NAME,
            null as MANAGER_NAME,
            null as ADDRESS_LINE_1,
            null as ADDRESS_LINE_2,
            null as ADDRESS_LINE_3,
            null as CITY,
            null as COUNTY,
            null as POSTAL_CD,
            null as ADDRESS_VERIFICATION_STATUS,
            null as CLASSIFICATION,
            null as LATTITUDE_NBR,
            null as LONGITUDE_NBR,
            null as TIME_ZONE,
            null as PHONE_COUNTRY_CODE,
            null as PHONE_NBR,
            null as PHONE_EXTENSION,
            null as PHONE_TYPE,
            null as EMAIL_ADDRESS,
            null as FAX_COUNTRY_CODE,
            null as FAX_NBR,
            null as FAX_EXTENSION,
            null as PICKUP_LOCATION_CD,
            null as PICKUP_ENABLED_FLAG,
            null as OUTLET_FLAG,
            null as POPUP_FLAG,
            null as STORE_IN_STORE_FLAG,
            null as SHIP_TO_STORE_ENABLED_FLAG,
            null as STOCKHOLD_FLAG,
            CAST(trim(RL.store_open_date) AS DATE) AS OPEN_DT,
            null as CLOSE_DT,
            null as TOTAL_AREA,
            null as SELLING_AREA,
            null as LINEAR_DISTANCE,
            null as PROMOTION_ZONE_CD,
            null as PROMOTION_ZONE_DESC,
            null as DEFAULT_WH,
            null as BREAK_PAC_FLAG,
            null as FRANCHISE_CD,
            null as MARKETING_CONCEPT_CD,
            CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
            CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
        FROM L2_STAGE.FRANCHISE_RELIANCE_STORE_HIERARCHY_STAGE RL
            LEFT OUTER JOIN l2_analytics_tables.LOCATION L
            ON trim(lower(RL.location_id)) = trim(lower(L.LOCATION_ID))
            AND L.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
            AND L.SOURCE_SYSTEM = 'GLOBAL_FRANCHISE'
            WHERE COALESCE(RL.location_id, '0') <> COALESCE(L.LOCATION_ID, '0') 
            OR COALESCE(RL.channel, '0') <> COALESCE(L.CHANNEL_CD, '0')
            OR COALESCE(RL.location_type, '0') <> COALESCE(L.LOCATION_TYPE_CD, '0')
            OR COALESCE(RL.COUNTRY, '0') <> COALESCE(L.COUNTRY_CD, '0')
            OR COALESCE(RL.location_name, '0') <> COALESCE(L.LOCATION_DESC, '0')
            OR COALESCE(RL.store_open_date, '0') <> COALESCE(L.OPEN_DT, '0')

        UNION
        SELECT
            COALESCE(L.LOCATION_KEY, 0) as LOCATION_KEY,
            TRIM(CONCAT('FLA', CAST(RL.location_id AS STRING))) AS LOCATION_ID,
            null as SOURCE_LOCATION_ID,
            'GLOBAL_FRANCHISE_LIVART' as SOURCE_SYSTEM,
            null as CONCEPT_CD,
            null as CONCEPT_DESC,  
            -1 as DISTRICT_KEY,
            null as DISTRICT_ID,
            null as DISTRICT_DESC, 
            -1 as REGION_KEY,
            null as REGION_ID,
            null as REGION_DESC,
            trim(RL.channel) AS CHANNEL_CD,
            null AS CHANNEL_DESC,
            null as COMPANY_CD,
            null as COMPANY_DESC,
            trim(RL.location_type) as LOCATION_TYPE_CD,
            trim(RL.COUNTRY) as COUNTRY_CD,
            null as COUNTRY_SUBDIVISION_CD,
            trim(RL.state) as STATE_CD, 
            null as CURRENCY_CD,
            null as VIRTUAL_WAREHOUSE_CD,
            null as PHYSICAL_WAREHOUSE_CD,
            CAST(trim(RL.store_open_date) AS TIMESTAMP) AS FIRST_EFFECTIVE_TS, 
            CAST(UNIX_TIMESTAMP("9999-12-31 23:59:59.000", 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS LAST_EFFECTIVE_TS,
            trim(RL.location_name) as LOCATION_DESC,
            trim(RL.location_name) as DISPLAY_NAME,
            null as MALL_NAME,
            null as SHORT_NAME,
            null as MANAGER_NAME,
            null as ADDRESS_LINE_1,
            null as ADDRESS_LINE_2,
            null as ADDRESS_LINE_3,
            null as CITY,
            null as COUNTY,
            null as POSTAL_CD,
            null as ADDRESS_VERIFICATION_STATUS,
            null as CLASSIFICATION,
            null as LATTITUDE_NBR,
            null as LONGITUDE_NBR,
            null as TIME_ZONE,
            null as PHONE_COUNTRY_CODE,
            null as PHONE_NBR,
            null as PHONE_EXTENSION,
            null as PHONE_TYPE,
            null as EMAIL_ADDRESS,
            null as FAX_COUNTRY_CODE,
            null as FAX_NBR,
            null as FAX_EXTENSION,
            null as PICKUP_LOCATION_CD,
            null as PICKUP_ENABLED_FLAG,
            null as OUTLET_FLAG,
            null as POPUP_FLAG,
            null as STORE_IN_STORE_FLAG,
            null as SHIP_TO_STORE_ENABLED_FLAG,
            null as STOCKHOLD_FLAG,
            CAST(trim(RL.store_open_date) AS DATE) AS OPEN_DT,
            null as CLOSE_DT,
            null as TOTAL_AREA,
            null as SELLING_AREA,
            null as LINEAR_DISTANCE,
            null as PROMOTION_ZONE_CD,
            null as PROMOTION_ZONE_DESC,
            null as DEFAULT_WH,
            null as BREAK_PAC_FLAG,
            null as FRANCHISE_CD,
            null as MARKETING_CONCEPT_CD,
            CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS INSERT_TS,
            CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss.000') AS TIMESTAMP) AS UPDATE_TS
        FROM L2_STAGE.FRANCHISE_LIVART_STORE_HIERARCHY_STAGE RL
            LEFT OUTER JOIN l2_analytics_tables.LOCATION L
            ON trim(lower(RL.location_id)) = trim(lower(L.LOCATION_ID))
            AND L.LAST_EFFECTIVE_TS = '9999-12-31 23:59:59.000'
            AND L.SOURCE_SYSTEM = 'GLOBAL_FRANCHISE'
            WHERE COALESCE(RL.location_id, '0') <> COALESCE(L.LOCATION_ID, '0') 
            OR COALESCE(RL.channel, '0') <> COALESCE(L.CHANNEL_CD, '0')
            OR COALESCE(RL.location_type, '0') <> COALESCE(L.LOCATION_TYPE_CD, '0')
            OR COALESCE(RL.COUNTRY, '0') <> COALESCE(L.COUNTRY_CD, '0')
            OR COALESCE(RL.location_name, '0') <> COALESCE(L.LOCATION_DESC, '0')
            OR COALESCE(RL.store_open_date, '0') <> COALESCE(L.OPEN_DT, '0');

        CREATE OR REPLACE TEMP VIEW FRANCHISE_STORE_HIERARCHY_VIEW2
        AS
		SELECT
           CASE
               WHEN STG.LOCATION_KEY = 0 THEN 
                    ROW_NUMBER() OVER (ORDER BY STG.LOCATION_ID, STG.COUNTRY_CD) + coalesce(MAX_LOCATION_KEY, 0)
               ELSE STG.LOCATION_KEY
           END AS LOCATION_KEY
           ,STG.LOCATION_ID
           ,STG.SOURCE_LOCATION_ID
           ,STG.SOURCE_SYSTEM
           ,STG.CONCEPT_CD
           ,STG.CONCEPT_DESC
           ,STG.DISTRICT_KEY
           ,STG.DISTRICT_ID
           ,STG.DISTRICT_DESC
           ,STG.REGION_KEY
           ,STG.REGION_ID
           ,STG.REGION_DESC
           ,STG.CHANNEL_CD
           ,STG.CHANNEL_DESC
           ,STG.COMPANY_CD
           ,STG.COMPANY_DESC
           ,STG.LOCATION_TYPE_CD
           ,STG.COUNTRY_CD
           ,STG.COUNTRY_SUBDIVISION_CD
           ,STG.STATE_CD
           ,STG.CURRENCY_CD
           ,STG.VIRTUAL_WAREHOUSE_CD
           ,STG.PHYSICAL_WAREHOUSE_CD
           ,STG.FIRST_EFFECTIVE_TS
           ,STG.LAST_EFFECTIVE_TS
           ,STG.LOCATION_DESC
           ,STG.DISPLAY_NAME
           ,STG.MALL_NAME
           ,STG.SHORT_NAME
           ,STG.MANAGER_NAME
           ,STG.ADDRESS_LINE_1
           ,STG.ADDRESS_LINE_2
           ,STG.ADDRESS_LINE_3
           ,STG.CITY
           ,STG.COUNTY
           ,STG.POSTAL_CD
           ,STG.ADDRESS_VERIFICATION_STATUS
           ,STG.CLASSIFICATION
           ,STG.LATTITUDE_NBR
           ,STG.LONGITUDE_NBR
           ,STG.TIME_ZONE
           ,STG.PHONE_COUNTRY_CODE
           ,STG.PHONE_NBR
           ,STG.PHONE_EXTENSION
           ,STG.PHONE_TYPE
           ,STG.EMAIL_ADDRESS
           ,STG.FAX_COUNTRY_CODE
           ,STG.FAX_NBR
           ,STG.FAX_EXTENSION
           ,STG.PICKUP_LOCATION_CD
           ,STG.PICKUP_ENABLED_FLAG
           ,STG.OUTLET_FLAG
           ,STG.POPUP_FLAG
           ,STG.STORE_IN_STORE_FLAG
           ,STG.SHIP_TO_STORE_ENABLED_FLAG
           ,STG.STOCKHOLD_FLAG
           ,STG.OPEN_DT
           ,STG.CLOSE_DT
           ,STG.TOTAL_AREA
           ,STG.SELLING_AREA
           ,STG.LINEAR_DISTANCE
           ,STG.PROMOTION_ZONE_CD
           ,STG.PROMOTION_ZONE_DESC
           ,STG.DEFAULT_WH
           ,STG.BREAK_PAC_FLAG
           ,STG.FRANCHISE_CD
           ,STG.MARKETING_CONCEPT_CD
           ,STG.INSERT_TS
           ,STG.UPDATE_TS
         FROM FRANCHISE_STORE_HIERARCHY_VIEW1 STG
           CROSS JOIN
            (
                SELECT MAX(LOCATION_KEY) AS MAX_LOCATION_KEY FROM L2_ANALYTICS_TABLES.LOCATION WHERE LOCATION_KEY <> 0
            ) TARGET_MAX;


        TRUNCATE TABLE L2_STAGE.FRANCHISE_STORE_HIERARCHY_STG;
        
        INSERT INTO L2_STAGE.FRANCHISE_STORE_HIERARCHY_STG
           SELECT * FROM FRANCHISE_STORE_HIERARCHY_VIEW2;


        MERGE INTO L2_ANALYTICS_TABLES.LOCATION TGT USING
           (
            SELECT STG_VIEW31.LOCATION_KEY AS MERGE_KEY, STG_VIEW31.* FROM FRANCHISE_STORE_HIERARCHY_VIEW2 STG_VIEW31
            UNION
            SELECT NULL AS MERGE_KEY, STG_VIEW32.* FROM L2_STAGE.FRANCHISE_STORE_HIERARCHY_STG STG_VIEW32
            )STG  ON TGT.LOCATION_KEY = STG.MERGE_KEY
            WHEN MATCHED AND TGT.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000' THEN
            UPDATE SET
               LAST_EFFECTIVE_TS = FROM_UNIXTIME(CAST(STG.FIRST_EFFECTIVE_TS AS LONG) - 1),
               UPDATE_TS = STG.UPDATE_TS
            WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN
            INSERT *