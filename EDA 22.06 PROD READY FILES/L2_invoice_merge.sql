 
INSERT INTO L2_STAGE.INVOICE_PARKING 
SELECT 
  T1.INVOICE_KEY 
, T1.INVOICE_ID  
, T1.SOURCE_SYSTEM 
, T1.ORDER_CHARGE_TRANSACTION_KEY  /* HINATUAN CHANGE */
, T1.ORDER_CHARGE_TRANSACTION_ID   /* HINATUAN CHANGE */
, T1.SOURCE_INVOICE_ID 
, T1.SUB_CHANNEL_CD  
, T1.CONCEPT_CD  
, T1.DAY_KEY 
, T1.INVOICE_DT  
, T1.INVOICE_CREATE_TS 
, T1.GLOBAL_TXN_ID 
, T1.FIRST_EFFECTIVE_TS  
, T1.LAST_EFFECTIVE_TS 
, T1.POS_WORKSTATION_ID  
, T1.POS_WORKSTATION_SEQ_NBR 
, T1.WSSPL_PURCHASE_ORDER_ID 
, T1.NOSALE_OVERRIDE_DT  
, T1.OVERRIDE_REASON_ID  
, T1.CASHIER_ID  
, T1.ASSOCIATE_ID  
, T1.IS_ASSOCIATE_FLAG 
, T1.EMPLOYEE_ID 
, T1.REPLACEMENT_ORDER_FLAG  
, T1.INTERNAL_FLAG 
, T1.DTC_INVOICE_TYPE_CD 
, T1.DTC_INVOICE_FULFILLMENT_TYPE_CD 
, T1.DTC_INVOICE_REVISED_TYPE_CD 
, T1.DTC_STATUS_CD 
, T1.DTC_REFERENCE_1 
, T1.DTC_COLLECTED_AMT 
, T1.TOTAL_QTY 
, T1.TOTAL_AMT 
, T1.CREATE_USER_ID  
, T1.CURRENCY_CD 
, NULL AS ORDER_LINE_KEY  
, T1.ORDER_HEADER_ID 
, T1.ORDER_ID 
, NULL AS ORDER_INVOICE_DETAIL_KEY  
, NULL AS PRIME_LINE_NO 
, NULL AS ITEM_ID 
, NULL AS FINAL_LINE_CHARGES  
, NULL AS UNIT_PRICE  
, NULL AS EXTN_VENDOR_COST  
, NULL AS EXTN_PO_EXPENSES  
, NULL AS EXTN_PO_DUTY  
, NULL AS EXTN_WAC  
, NULL AS EXTN_LABOR_COST 
, T1.INSERT_TS 
, T1.UPDATE_TS 
, cast(date_format(current_timestamp(), 'yyyyMMdd') as bigint) as datehour
, cast(date_format(current_timestamp(), 'HH') as int) hour  
FROM L2_STAGE.INVOICE_STAGE_ODBC T1 WHERE T1.ORDER_HEADER_KEY IS NULL or T1.ORDER_HEADER_KEY=-1 ;  


MERGE INTO L2_ANALYTICS_TABLES.INVOICE TARGET USING 
    ( SELECT STG1_1.INVOICE_KEY AS MERGE_KEY, STG1_1.* FROM L2_STAGE.INVOICE_STAGE_ODBC STG1_1 WHERE STG1_1.ORDER_HEADER_KEY <> -1 
     UNION 
    SELECT NULL AS MERGE_KEY, STG1_2.* FROM L2_STAGE.INVOICE_STAGE_ODBC STG1_2 WHERE STG1_2.ORDER_HEADER_KEY <> -1 
 ) STG1 ON TARGET.INVOICE_KEY = STG1.MERGE_KEY 
 WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000' THEN 
    UPDATE SET 
        LAST_EFFECTIVE_TS = FROM_UNIXTIME(CAST(STG1.FIRST_EFFECTIVE_TS AS LONG) - 1), 
        UPDATE_TS = STG1.UPDATE_TS 
 WHEN NOT MATCHED AND MERGE_KEY IS NULL THEN 
    INSERT ( 
                INVOICE_KEY , 
                INVOICE_ID , 
                SOURCE_SYSTEM , 
				ORDER_CHARGE_TRANSACTION_KEY,  /* HINATUAN CHANGE */
				ORDER_CHARGE_TRANSACTION_ID ,  /* HINATUAN CHANGE */
                SOURCE_INVOICE_ID , 
                SUB_CHANNEL_CD , 
                CONCEPT_CD , 
                LOCATION_KEY , 
                LOCATION_ID , 
                DAY_KEY , 
                INVOICE_DT , 
                INVOICE_CREATE_TS , 
                ORDER_HEADER_KEY , 
                ORDER_HEADER_ID , 
                ORDER_ID , 
                GLOBAL_TXN_ID , 
                FIRST_EFFECTIVE_TS , 
                LAST_EFFECTIVE_TS , 
                POS_WORKSTATION_ID , 
                POS_WORKSTATION_SEQ_NBR , 
                WSSPL_PURCHASE_ORDER_ID , 
                NOSALE_OVERRIDE_DT , 
                OVERRIDE_REASON_ID , 
                CASHIER_ID , 
                ASSOCIATE_ID , 
                IS_ASSOCIATE_FLAG , 
                EMPLOYEE_ID , 
                REPLACEMENT_ORDER_FLAG , 
                INTERNAL_FLAG , 
                DTC_INVOICE_TYPE_CD , 
                DTC_INVOICE_FULFILLMENT_TYPE_CD , 
                DTC_INVOICE_REVISED_TYPE_CD , 
                DTC_TXN_TYPE_CD , 
                DTC_STATUS_CD , 
                DTC_REFERENCE_1 , 
                DTC_COLLECTED_AMT , 
                TOTAL_QTY , 
                TOTAL_AMT , 
                CREATE_USER_ID , 
                CURRENCY_CD , 
                INSERT_TS , 
                UPDATE_TS                 
            ) 
            VALUES 
            ( 
                STG1.INVOICE_KEY , 
                STG1.INVOICE_ID , 
                STG1.SOURCE_SYSTEM , 
				STG1.ORDER_CHARGE_TRANSACTION_KEY,  /* HINATUAN CHANGE */
				STG1.ORDER_CHARGE_TRANSACTION_ID ,  /* HINATUAN CHANGE */
                STG1.SOURCE_INVOICE_ID , 
                STG1.SUB_CHANNEL_CD , 
                STG1.CONCEPT_CD , 
                STG1.LOCATION_KEY , 
                STG1.LOCATION_ID , 
                STG1.DAY_KEY , 
                STG1.INVOICE_DT , 
                STG1.INVOICE_CREATE_TS , 
                STG1.ORDER_HEADER_KEY , 
                STG1.ORDER_HEADER_ID , 
                STG1.ORDER_ID , 
                STG1.GLOBAL_TXN_ID , 
                STG1.FIRST_EFFECTIVE_TS , 
                STG1.LAST_EFFECTIVE_TS , 
                STG1.POS_WORKSTATION_ID , 
                STG1.POS_WORKSTATION_SEQ_NBR , 
                STG1.WSSPL_PURCHASE_ORDER_ID , 
                STG1.NOSALE_OVERRIDE_DT , 
                STG1.OVERRIDE_REASON_ID , 
                STG1.CASHIER_ID , 
                STG1.ASSOCIATE_ID , 
                STG1.IS_ASSOCIATE_FLAG , 
                STG1.EMPLOYEE_ID , 
                STG1.REPLACEMENT_ORDER_FLAG , 
                STG1.INTERNAL_FLAG , 
                STG1.DTC_INVOICE_TYPE_CD , 
                STG1.DTC_INVOICE_FULFILLMENT_TYPE_CD , 
                STG1.DTC_INVOICE_REVISED_TYPE_CD , 
                STG1.DTC_TXN_TYPE_CD , 
                STG1.DTC_STATUS_CD , 
                STG1.DTC_REFERENCE_1 , 
                STG1.DTC_COLLECTED_AMT , 
                STG1.TOTAL_QTY , 
                STG1.TOTAL_AMT , 
                STG1.CREATE_USER_ID , 
                STG1.CURRENCY_CD , 
                STG1.INSERT_TS , 
                STG1.UPDATE_TS                
            );

  DELETE FROM L2_STAGE.INVOICE_PARKING WHERE CAST(datehour AS BIGINT) < (SELECT MIN(DH) FROM ( SELECT CAST(datehour AS BIGINT) AS DH FROM L2_STAGE.INVOICE_PARKING ORDER BY datehour DESC LIMIT 10 ))