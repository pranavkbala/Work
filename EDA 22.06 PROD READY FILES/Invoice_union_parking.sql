 SELECT * FROM ( 
 SELECT  
 ROW_NUMBER() OVER (PARTITION BY trim(INVOICE_ID) ORDER BY FIRST_EFFECTIVE_TS DESC ) AS RN, 
 * 
 FROM 
 ( 
 SELECT   
    CAST(-1 AS BIGINT)AS INVOICE_KEY ,
    TRIM(str.ORDER_INVOICE_KEY) as INVOICE_ID ,
    'STERLING_DTC' as SOURCE_SYSTEM ,
	TRIM(str.CHARGE_TRANSACTION_KEY)  as ORDER_CHARGE_TRANSACTION_ID , /* HINATUAN CHANGE */
    TRIM(str.INVOICE_NO) as SOURCE_INVOICE_ID , 
    NULL AS SUB_CHANNEL_CD , 
    TRIM(str.ENTERPRISE_CODE) AS CONCEPT_CD , 
    CAST(translate(TO_DATE(TO_DATE(COALESCE(str.EXTN_INV_FIN_DATE,'1900-01-01'))),"-","") AS INT) as DAY_KEY , 
    TO_DATE(str.EXTN_INV_FIN_DATE) as INVOICE_DT , 
    str.CREATETS as INVOICE_CREATE_TS , 
    NULL AS GLOBAL_TXN_ID , 
    to_timestamp(str.MODIFYTS) AS FIRST_EFFECTIVE_TS , 
    to_timestamp('9999-12-31T23:59:59') AS LAST_EFFECTIVE_TS , 
    NULL AS POS_WORKSTATION_ID , 
    NULL AS POS_WORKSTATION_SEQ_NBR , 
    NULL AS WSSPL_PURCHASE_ORDER_ID , 
    NULL AS NOSALE_OVERRIDE_DT , 
    NULL AS OVERRIDE_REASON_ID , 
    NULL AS CASHIER_ID , 
    null as ASSOCIATE_ID  , 
    null as IS_ASSOCIATE_FLAG , 
    null as EMPLOYEE_ID , 
    NULL AS REPLACEMENT_ORDER_FLAG , 
    NULL AS INTERNAL_FLAG , 
    TRIM(str.INVOICE_TYPE) as DTC_INVOICE_TYPE_CD , 
    TRIM(str.EXTN_INVOICE_TYPE) as DTC_INVOICE_FULFILLMENT_TYPE_CD , 
    CASE WHEN UPPER(TRIM(str.INVOICE_TYPE)) = 'ORDER' AND UPPER(TRIM(str.EXTN_INVOICE_TYPE)) = 'DC' THEN 'SHIPMENT' ELSE str.INVOICE_TYPE END as DTC_INVOICE_REVISED_TYPE_CD , 
    TRIM(str.STATUS) as DTC_STATUS_CD , 
    TRIM(str.REFERENCE_1) as DTC_REFERENCE_1 , 
    CAST(TRIM(str.AMOUNT_COLLECTED) as DECIMAL(15,2)) as DTC_COLLECTED_AMT , 
    CAST( 0  AS INT) as TOTAL_QTY , 
    CAST(TRIM(str.TOTAL_AMOUNT) AS DECIMAL(15,2)) as TOTAL_AMT , 
    TRIM(str.CREATEUSERID) as CREATE_USER_ID , 
    NULL AS CURRENCY_CD , 
    TRIM(str.ORDER_HEADER_KEY) AS ORDER_HEADER_ID , 
    TRIM(str.order_no) AS order_id,
    current_timestamp() AS INSERT_TS , 
    current_timestamp() AS UPDATE_TS  

 from INV_VIEW1 str 
 group by 
    '-1' , 
    TRIM(str.ORDER_INVOICE_KEY) , 
    'STERLING_DTC' , 
	TRIM(str.CHARGE_TRANSACTION_KEY),  /* HINATUAN CHANGE */
    TRIM(str.INVOICE_NO) , 
    TRIM(str.ENTERPRISE_CODE) , 
    CAST(translate(TO_DATE(TO_DATE(COALESCE(str.EXTN_INV_FIN_DATE,'1900-01-01'))),"-","") AS INT) , 
    TO_DATE(str.EXTN_INV_FIN_DATE) , 
    str.CREATETS ,  
    to_timestamp(str.MODIFYTS) , 
    to_timestamp('9999-12-31T23:59:59') , 
    TRIM(str.INVOICE_TYPE) , 
    TRIM(str.EXTN_INVOICE_TYPE) , 
    CASE WHEN UPPER(TRIM(str.INVOICE_TYPE)) = 'ORDER' AND UPPER(TRIM(str.EXTN_INVOICE_TYPE)) = 'DC' THEN 'SHIPMENT' ELSE str.INVOICE_TYPE END , 
    TRIM(str.STATUS) , 
    TRIM(str.REFERENCE_1) , 
    CAST(TRIM(str.AMOUNT_COLLECTED) as DECIMAL(15,2)) , 
    CAST( 0  AS INT) , 
    CAST(TRIM(str.TOTAL_AMOUNT) AS DECIMAL(15,2)) , 
    TRIM(str.CREATEUSERID) , 
    TRIM(str.ORDER_HEADER_KEY) , 
    TRIM(str.order_no), 
    current_timestamp() , 
    current_timestamp() 
    
    UNION 
    
    SELECT 
        CAST(-1 AS BIGINT)AS INVOICE_KEY , 
        pk.INVOICE_ID   , 
        pk.SOURCE_SYSTEM    ,
		pk.ORDER_CHARGE_TRANSACTION_ID ,   /* HINATUAN CHANGE */
        pk.SOURCE_INVOICE_ID    , 
        NULL AS SUB_CHANNEL_CD , 
        pk.CONCEPT_CD   , 
        pk.DAY_KEY  , 
        pk.INVOICE_DT   , 
        pk.INVOICE_CREATE_TS    , 
        NULL AS GLOBAL_TXN_ID , 
        pk.FIRST_EFFECTIVE_TS   , 
        pk.LAST_EFFECTIVE_TS    , 
        NULL AS POS_WORKSTATION_ID , 
        NULL AS POS_WORKSTATION_SEQ_NBR , 
        NULL AS WSSPL_PURCHASE_ORDER_ID , 
        NULL AS NOSALE_OVERRIDE_DT , 
        NULL AS OVERRIDE_REASON_ID , 
        NULL AS CASHIER_ID , 
        null as ASSOCIATE_ID  , 
        null as IS_ASSOCIATE_FLAG , 
        null as EMPLOYEE_ID , 
        NULL AS REPLACEMENT_ORDER_FLAG ,
        NULL AS INTERNAL_FLAG , 
        pk.DTC_INVOICE_TYPE_CD  , 
        pk.DTC_INVOICE_FULFILLMENT_TYPE_CD  , 
        pk.DTC_INVOICE_REVISED_TYPE_CD  , 
        pk.DTC_STATUS_CD    , 
        pk.DTC_REFERENCE_1  , 
        pk.DTC_COLLECTED_AMT    , 
        pk.TOTAL_QTY    , 
        pk.TOTAL_AMT    , 
        pk.CREATE_USER_ID   , 
        NULL AS CURRENCY_CD , 
        pk.ORDER_HEADER_ID , 
        pk.order_id ,
        current_timestamp() AS INSERT_TS , 
        current_timestamp() AS UPDATE_TS 
 FROM  
 ( 
 SELECT  
 * 
 FROM L2_STAGE.INVOICE_PARKING WHERE datehour= (  SELECT MAX(datehour) FROM L2_STAGE.INVOICE_PARKING ) 
 ) pk 
 LEFT JOIN INV_VIEW1 str 
 ON pk.INVOICE_ID=str.ORDER_INVOICE_KEY 
 WHERE str.ORDER_INVOICE_KEY IS NULL 
 )T 
 )TB WHERE RN=1 