MERGE INTO "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INVOICE" TARGET USING 
        (
       	 SELECT STG1_1.INVOICE_KEY AS MERGE_KEY, STG1_1.* FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."NETSUITE_INVOICE_STG" STG1_1  
       	 UNION
       	 SELECT NULL AS MERGE_KEY, STG1_2.* FROM "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."NETSUITE_INVOICE_STG" STG1_2  
        ) STG1 ON TARGET.INVOICE_KEY = STG1.MERGE_KEY 
          WHEN MATCHED AND TARGET.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000' THEN 
        UPDATE SET TARGET.LAST_EFFECTIVE_TS = dateadd(second,-1,STG1.FIRST_EFFECTIVE_TS), TARGET.UPDATE_TS = STG1.UPDATE_TS 
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
			 CREATE_USER_ID,              /* HINATUAN */
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
			NULL,           /* HINATUAN */
            STG1.CURRENCY_CD,  
       	    STG1.INSERT_TS,
       	    STG1.UPDATE_TS
       	);
