select COALESCE(T.GLOBAL_PO_LINK_KEY,0) as GLOBAL_PO_LINK_KEY,
STG1_VIEW1.ORIGINAL_PO_HEADER_KEY,
STG1_VIEW1.ORIGINAL_PO_HEADER_ID,
STG1_VIEW1.ORIGINAL_PO_LINE_KEY,
STG1_VIEW1.ORIGINAL_PO_LINE_ID,
STG1_VIEW1.APPLIED_PO_HEADER_KEY,
STG1_VIEW1.APPLIED_PO_ID,
STG1_VIEW1.APPLIED_PO_LINE_KEY,
STG1_VIEW1.APPLIED_PO_LINE_ID,
STG1_VIEW1.FIRST_EFFECTIVE_TS,
STG1_VIEW1.LAST_EFFECTIVE_TS,
STG1_VIEW1.FOREIGN_LINKED_AMT,
STG1_VIEW1.LINKED_AMT,
STG1_VIEW1.LINKED_QTY,
STG1_VIEW1.DISCOUNT_FLAG,
STG1_VIEW1.LINK_TYPE_CD,
STG1_VIEW1.LINK_TYPE_DESC,
STG1_VIEW1.INVENTORY_NBR,
STG1_VIEW1.ORIGINAL_POSTED_DT,
STG1_VIEW1.APPLIED_POSTED_DT,
STG1_VIEW1.INSERT_TS,
STG1_VIEW1.UPDATE_TS
 FROM  GLOBAL_PO_LINK_STG1_VIEW1 STG1_VIEW1 
 LEFT OUTER JOIN L2_ANALYTICS_TABLES.GLOBAL_PO_LINK T  
	ON STG1_VIEW1.ORIGINAL_PO_HEADER_ID = T.ORIGINAL_PO_HEADER_ID
    AND STG1_VIEW1.ORIGINAL_PO_LINE_ID = T.ORIGINAL_PO_LINE_ID
    AND STG1_VIEW1.APPLIED_PO_ID = T.APPLIED_PO_ID
    AND STG1_VIEW1.APPLIED_PO_LINE_ID = T.APPLIED_PO_LINE_ID
    AND STG1_VIEW1.LINK_TYPE_CD = T.LINK_TYPE_CD
	AND T.LAST_EFFECTIVE_TS = '9999-12-31T23:59:59.000+0000'
WHERE
	T.GLOBAL_PO_LINK_KEY is null
	OR COALESCE(STG1_VIEW1.ORIGINAL_PO_HEADER_KEY,0) <> COALESCE(T.ORIGINAL_PO_HEADER_KEY,0)
    OR COALESCE(STG1_VIEW1.ORIGINAL_PO_LINE_KEY,0) <> COALESCE(T.ORIGINAL_PO_LINE_KEY,0)
    OR COALESCE((STG1_VIEW1.APPLIED_PO_HEADER_KEY),0) <> COALESCE(T.APPLIED_PO_HEADER_KEY,0)
    OR COALESCE(STG1_VIEW1.APPLIED_PO_LINE_KEY,0) <> COALESCE(T.APPLIED_PO_LINE_KEY,0)
    OR COALESCE(STG1_VIEW1.FOREIGN_LINKED_AMT,0.0000) <> COALESCE(T.FOREIGN_LINKED_AMT,0.0000)
    OR COALESCE(STG1_VIEW1.LINKED_AMT,0.0000) <> COALESCE(T.LINKED_AMT,0.0000)
    OR COALESCE(STG1_VIEW1.LINKED_QTY,0) <> COALESCE(T.LINKED_QTY,0)
    OR COALESCE(STG1_VIEW1.DISCOUNT_FLAG,'') <> COALESCE(T.DISCOUNT_FLAG,'')
    OR COALESCE(STG1_VIEW1.LINK_TYPE_DESC,'') <> COALESCE(T.LINK_TYPE_DESC,'')
    OR COALESCE(STG1_VIEW1.INVENTORY_NBR,0) <> COALESCE(T.INVENTORY_NBR,0)
    OR COALESCE(STG1_VIEW1.ORIGINAL_POSTED_DT,'') <> COALESCE(T.ORIGINAL_POSTED_DT,'')
    OR COALESCE(STG1_VIEW1.APPLIED_POSTED_DT,'') <> COALESCE(T.APPLIED_POSTED_DT,'')
    
