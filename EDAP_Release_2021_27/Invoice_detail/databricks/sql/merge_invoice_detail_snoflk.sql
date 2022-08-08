 MERGE INTO "PROD_EDAP_ANALYTICS_DB"."PROD_EDAP_ANALYTICS_TABLES"."INVOICE_DETAIL" tgt
  USING "PROD_EDAP_STAGE_DB"."PROD_EDAP_STAGE_TABLES"."INVOICE_DETAIL_STAGE" stg 
 ON tgt.source_system = 'STERLING_DTC'
 and trim(tgt.INVOICE_DETAIL_ID) = trim(stg.INVOICE_DETAIL_ID)
 and tgt.FIRST_EFFECTIVE_TS = stg.FIRST_EFFECTIVE_TS
 WHEN MATCHED AND tgt.LAST_EFFECTIVE_TS != stg.LAST_EFFECTIVE_TS  
  THEN UPDATE SET tgt.LAST_EFFECTIVE_TS = stg.LAST_EFFECTIVE_TS, tgt.UPDATE_TS = stg.UPDATE_TS 
 WHEN NOT MATCHED 
  THEN INSERT 
  ( INVOICE_DETAIL_KEY	,INVOICE_DETAIL_ID	,CONCEPT_CD, SOURCE_SYSTEM	,PRIME_LINE_SEQ_NBR	,INVOICE_KEY 
  	,INVOICE_ID, INVOICE_DT	,ITEM_KEY	,ITEM_ID	,ORDER_LINE_KEY	,ORDER_LINE_ID	, LOCATION_KEY
    ,LOCATION_ID, GLOBAL_COST_CENTER_KEY, GLOBAL_COST_CENTER_ID, GLOBAL_SUBSIDIARY_KEY, GLOBAL_SUBSIDIARY_ID, FIRST_EFFECTIVE_TS	
  	,LAST_EFFECTIVE_TS	,RETAIL_TYPE_CD	,POS_RING_TYPE_CD	,TXN_TYPE_CD	,TXN_SUB_TYPE_CD	
  	,ENTRY_METHOD	,SLSPRSN_ID	,INVOICE_QTY	,INVOICE_AMT	,UNIT_PRICE_AMT	,GLOBAL_ESTIMATED_COST_AMT ,TXN_SIGN	
  	,TAXABLE_FLAG	,ORIG_TXN_NBR	,ORIG_UNIT_PRICE_AMT	,ORIG_TXN_SIGN_CD	,REASON_ID	
  	,RETURN_REASON_ID	,DROP_SHIP_FLAG	, ACTUAL_SHIP_DT, WEIGHTED_AVG_COST	,LABOR_COST	,LABOR_SKU	,VENDOR_COST	
  	,PO_EXPENSES	,PO_DUTY	, FULFILLMENT_STATUS_ID, DISCOUNT_RATE_AMT, ELC_AGENT_COMMISSION_AMT, ELC_HTS_AMT, ELC_FREIGHT_AMT
    ,ELC_MISC_AMT, FIRST_COST_AMT, PACK_GROUP_NBR, COMMITTED_QTY, CLOSED_DT, INSERT_TS, UPDATE_TS
  	)
  VALUES
  ( stg.INVOICE_DETAIL_KEY	,stg.INVOICE_DETAIL_ID	, stg.CONCEPT_CD, stg.SOURCE_SYSTEM	,stg.PRIME_LINE_SEQ_NBR	,stg.INVOICE_KEY 
  	,stg.INVOICE_ID, stg.INVOICE_DT	,stg.ITEM_KEY	,stg.ITEM_ID	,stg.ORDER_LINE_KEY	,stg.ORDER_LINE_ID	, stg.LOCATION_KEY
    ,stg.LOCATION_ID, stg.GLOBAL_COST_CENTER_KEY, stg.GLOBAL_COST_CENTER_ID, stg.GLOBAL_SUBSIDIARY_KEY, stg.GLOBAL_SUBSIDIARY_ID, stg.FIRST_EFFECTIVE_TS	
  	,stg.LAST_EFFECTIVE_TS	,stg.RETAIL_TYPE_CD	,stg.POS_RING_TYPE_CD	,stg.TXN_TYPE_CD	,stg.TXN_SUB_TYPE_CD	
  	,stg.ENTRY_METHOD	,stg.SLSPRSN_ID	,stg.INVOICE_QTY	,stg.INVOICE_AMT	,stg.UNIT_PRICE_AMT	,stg.GLOBAL_ESTIMATED_COST_AMT ,stg.TXN_SIGN	
  	,stg.TAXABLE_FLAG	,stg.ORIG_TXN_NBR	,stg.ORIG_UNIT_PRICE_AMT	,stg.ORIG_TXN_SIGN_CD	,stg.REASON_ID	
  	,stg.RETURN_REASON_ID	,stg.DROP_SHIP_FLAG	, stg.ACTUAL_SHIP_DT, stg.WEIGHTED_AVG_COST	,stg.LABOR_COST	,stg.LABOR_SKU	,stg.VENDOR_COST	
  	,stg.PO_EXPENSES	,stg.PO_DUTY, stg.FULFILLMENT_STATUS_ID, stg.DISCOUNT_RATE_AMT, stg.ELC_AGENT_COMMISSION_AMT, stg.ELC_HTS_AMT, stg.ELC_FREIGHT_AMT
    ,stg.ELC_MISC_AMT, stg.FIRST_COST_AMT, stg.PACK_GROUP_NBR, stg.COMMITTED_QTY, stg.CLOSED_DT, stg.INSERT_TS, stg.UPDATE_TS
  ) 
