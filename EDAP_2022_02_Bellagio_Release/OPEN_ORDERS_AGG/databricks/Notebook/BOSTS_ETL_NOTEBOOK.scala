// Databricks notebook source
// MAGIC %sql
// MAGIC create widget text L2_STAGE default "L2_STAGE";
// MAGIC create widget text L2_ANALYTICS default "L2_ANALYTICS";
// MAGIC create widget text L2_ANALYTICS_TABLES default "L2_ANALYTICS_TABLES";
// MAGIC create widget text MIN_ORD_LINE_CNT DEFAULT "10";
// MAGIC create widget text FROM_DATE DEFAULT "2018-01-01";

// COMMAND ----------

val L2_STAGE = dbutils.widgets.get("L2_STAGE")
val L2_ANALYTICS = dbutils.widgets.get("L2_ANALYTICS")
val L2_ANALYTICS_TABLES = dbutils.widgets.get("L2_ANALYTICS_TABLES")
val MIN_ORD_LINE_CNT = dbutils.widgets.get("MIN_ORD_LINE_CNT").toLong
val FROM_DATE = dbutils.widgets.get("FROM_DATE")

// COMMAND ----------

// DBTITLE 1,Obtain Sales Order Data

import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.broadcastTimeout", 3600)
spark.conf.set("spark.sql.shuffle.partitions", 300)
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead","LEGACY")

val TempSalesOrderData = spark.sql(s""" 

(select
      OL.ORDER_DT ORDER_DT,
      trim(OL.ORDER_LINE_ID) ORDER_LINE_ID,
      OL.ORDER_HEADER_KEY ORDER_HEADER_KEY,
      OL.ORDER_LINE_KEY ORDER_LINE_KEY,
      ORS.ORDER_LINE_SCHEDULE_KEY ORDER_LINE_SCHEDULE_KEY,
      OL.ITEM_KEY ITEM_KEY,
      trim(OL.PRODUCT_LINE) PRODUCT_LINE,
      trim(OL.SHIP_NODE_CD) SHIP_NODE_CD,
      trim(OL.RECEIVING_NODE_CD) RECEIVING_NODE_CD,
      OL.LINE_TOTAL_AMT LINE_TOTAL_AMT,                                              --Nile bpk
      trim(OL.ADDITIONAL_LINE_TYPE_CD) ADDITIONAL_LINE_TYPE_CD,                      --Nile bpk
      trim(OL.RETURN_ACTION_CD) RETURN_ACTION_CD,                                    --Nile bpk
      OL.ORDER_QTY ORDER_QTY,                                                        --Nile bpk
      trim(OL.RETURN_REASON_CD) RETURN_REASON_CD,                                    --Nile bpk
      trim(OL.RETURN_SUB_REASON_CD) RETURN_SUB_REASON_CD,                            --Nile bpk
      trim(OL.RETURN_REASON_DESC) RETURN_REASON_DESC,                                --Nile bpk
      trim(OL.RETURN_ACTION) RETURN_ACTION,                                          --Nile bpk
      trim(ORS.STATUS_ID) STATUS_ID,
      ORS.STATUS_QTY STATUS_QTY,
      ORS.STATUS_TS STATUS_TS,                                                       --Nile bpk
      ORS.INSERT_TS INSERT_TS,                                                       --Nie bpk
      trim(ORS.STATUS_DESC) STATUS_DESC
    from
      (SELECT ORDER_DT ORDER_DT,
                trim(ORDER_LINE_ID) ORDER_LINE_ID,
                ORDER_HEADER_KEY ORDER_HEADER_KEY,
                ORDER_LINE_KEY ORDER_LINE_KEY,
                ITEM_KEY ITEM_KEY,
                trim(PRODUCT_LINE) PRODUCT_LINE,
                trim(SHIP_NODE_CD) SHIP_NODE_CD,
                trim(RECEIVING_NODE_CD) RECEIVING_NODE_CD, --- Duplicate Store
                LINE_TOTAL_AMT LINE_TOTAL_AMT,                                               --Nile bpk
                trim(ADDITIONAL_LINE_TYPE_CD) ADDITIONAL_LINE_TYPE_CD,                       --Nile bpk
                trim(RETURN_ACTION_CD) RETURN_ACTION_CD,                                     --Nile bpk
                ORDER_QTY ORDER_QTY,                                                         --Nile bpk
                trim(RETURN_REASON_CD) RETURN_REASON_CD,                                     --Nile bpk
                trim(RETURN_SUB_REASON_CD) RETURN_SUB_REASON_CD,                             --Nile bpk
                trim(RETURN_REASON_DESC) RETURN_REASON_DESC,                                 --Nile bpk
                trim(RETURN_ACTION) RETURN_ACTION                                            --Nile bpk
             FROM $L2_ANALYTICS.ORDER_LINE
             where
                trim(CARRIER_SERVICE_CD) = 'PICKUP_INTERNAL' and
                ORDER_LINE_TYPE != 'GCARD'
                and ( trim(KIT_CD) is null or trim(KIT_CD) = '' )
                and trim(PRODUCT_LINE) is not null
                and SOURCE_SYSTEM = 'STERLING_DTC'
                and ORDER_DT >= '$FROM_DATE'


      ) OL
      join (


             SELECT trim(STATUS_ID) STATUS_ID,
                    ORDER_LINE_SCHEDULE_KEY,
                    STATUS_QTY STATUS_QTY,
                    STATUS_TS STATUS_TS,          --Nile bpk
                    INSERT_TS INSERT_TS,          --Nile bpk
                    trim(STATUS_DESC) STATUS_DESC,
                    trim(ORDER_LINE_ID) ORDER_LINE_ID,
                    ORDER_DT
              FROM $L2_STAGE.ORDER_RELEASE_STATUS
              WHERE  trim(DOCUMENT_TYPE) in ('0001','0003')
              and trim(STATUS_ID) > '1000'
              and ORDER_DT >= '$FROM_DATE'
              


      ) ORS

         on trim(OL.ORDER_LINE_ID) = trim(ORS.ORDER_LINE_ID)
         and ORS.ORDER_DT = OL.ORDER_DT

      )

""")

TempSalesOrderData.createOrReplaceTempView("TEMP_SALES_ORDER_DATA")

// COMMAND ----------

// DBTITLE 1,Obtain Return Order Data (STATUS_ID 3950.01 & 3900)

import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.broadcastTimeout", 3600)
spark.conf.set("spark.sql.shuffle.partitions", 300)
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead","LEGACY")

val TempRetOrderLineRelease = spark.sql(s"""
select OL.ORDER_DT ORDER_DT,
                trim(OL.ORDER_LINE_ID) ORDER_LINE_ID,
                OL.ORDER_HEADER_KEY ORDER_HEADER_KEY,
                OL.ORDER_LINE_KEY ORDER_LINE_KEY,
                ORS.ORDER_LINE_SCHEDULE_KEY ORDER_LINE_SCHEDULE_KEY,
                OL.ITEM_KEY ITEM_KEY,
                trim(OL.PRODUCT_LINE) PRODUCT_LINE,
                trim(OL.SHIP_NODE_CD) SHIP_NODE_CD,
                trim(OL.RECEIVING_NODE_CD) RECEIVING_NODE_CD, --- Duplicate Store
                OL.LINE_TOTAL_AMT LINE_TOTAL_AMT,                                               
                trim(OL.ADDITIONAL_LINE_TYPE_CD) ADDITIONAL_LINE_TYPE_CD,                       
                trim(ROL.RETURN_ACTION_CD) RETURN_ACTION_CD,                                     
                ROL.ORDER_QTY ORDER_QTY,                                                         
                trim(ROL.RETURN_REASON_CD) RETURN_REASON_CD,                                     
                trim(ROL.RETURN_SUB_REASON_CD) RETURN_SUB_REASON_CD,                             
                trim(ROL.RETURN_REASON_DESC) RETURN_REASON_DESC,                                 
                trim(ROL.RETURN_ACTION) RETURN_ACTION,
                trim(ORS.STATUS_ID) STATUS_ID,
                ORS.STATUS_QTY STATUS_QTY,
                ORS.STATUS_TS STATUS_TS,          
                ORS.INSERT_TS INSERT_TS,          
                trim(ORS.STATUS_DESC) STATUS_DESC
                from $L2_ANALYTICS.ORDER_LINE ROL JOIN $L2_ANALYTICS.ORDER_LINE OL ON ROL.LINKED_ORDER_LINE_ID = OL.ORDER_LINE_ID 
                
JOIN (           

             SELECT trim(STATUS_ID) STATUS_ID,
                   ORDER_LINE_SCHEDULE_KEY,
                    STATUS_QTY STATUS_QTY,
                    STATUS_TS STATUS_TS,          
                    INSERT_TS INSERT_TS, 
                    trim(STATUS_DESC) STATUS_DESC,
                    trim(ORDER_LINE_ID) ORDER_LINE_ID,
                    ORDER_DT
              FROM $L2_STAGE.ORDER_RELEASE_STATUS
              WHERE  trim(DOCUMENT_TYPE) in ('0001','0003')
              and trim(STATUS_ID) in ('3950.01','3900')
              and ORDER_DT >= '$FROM_DATE'
     ) ORS
     ON ORS.ORDER_LINE_ID = ROL.ORDER_LINE_ID
     AND ORS.ORDER_DT = ROL.ORDER_DT
     where trim(OL.CARRIER_SERVICE_CD) = 'PICKUP_INTERNAL' and
                OL.ORDER_LINE_TYPE != 'GCARD'
                and ( trim(OL.KIT_CD) is null or trim(OL.KIT_CD) = '' )
                and trim(OL.PRODUCT_LINE) is not null
                and OL.SOURCE_SYSTEM = 'STERLING_DTC'
                and OL.ORDER_DT >= '$FROM_DATE'
                """)

TempRetOrderLineRelease.createOrReplaceTempView("TEMP_RET_ORDER_DATA")


// COMMAND ----------

// DBTITLE 1,CACHED_ORDER_LINE_RELEASE table from Sales & Returns Order

import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.broadcastTimeout", 3600)
spark.conf.set("spark.sql.shuffle.partitions", 300)
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead","LEGACY")

val CachedOrderLineRelease = spark.sql(s"""

SELECT COLR.ORDER_DT,
  COLR.ORDER_LINE_ID,
  COLR.ORDER_HEADER_KEY,
  COLR.ORDER_LINE_KEY,
  COLR.STATUS_ID AS LINE_STATUS, 
  --Max(CASE WHEN COLR.STATUS_QTY > 0 THEN COLR.STATUS_QTY END) UNIT,

  COLR.STATUS_QTY AS UNIT,

  COLR.STATUS_DESC AS LINE_DESCRIPTION,

  COLR.ITEM_KEY,
  COLR.PRODUCT_LINE,
  COLR.SHIP_NODE_CD,
  COLR.RECEIVING_NODE_CD,
  COLR.LINE_TOTAL_AMT,                                   --Nile bpk
  COLR.ADDITIONAL_LINE_TYPE_CD,                 --Nile bpk
  COLR.RETURN_ACTION_CD,                               --Nile bpk
  COLR.ORDER_QTY,                                             --Nile bpk
  COLR.RETURN_REASON_CD,                               --Nile bpk
  COLR.RETURN_SUB_REASON_CD,                       --Nile bpk
  COLR.RETURN_REASON_DESC,                           --Nile bpk
  COLR.RETURN_ACTION,                                     --Nile bpk
  COLR.STATUS_TS,                                        --Nile bpk

DT.ORDER_SHIPPED_DT,
DT.STORE_RECEIVED_DT,
DT.CUSTOMER_PICKED_UP_DATE,
  (CASE WHEN  (TRIM(STATUS_ID)) in ('9000','3700.01.545','3200.525','9000.300','3200.520','3700.01.540','1100.525') OR (TRIM(STATUS_ID)) >= '9000'  THEN (INSERT_TS) ELSE NULL END) CANCELLED_DATE,
  
  DT.CUSTOMER_PICKUP_TS,            --Nile bpk
  min(DT.ORDER_DROP_TS) over (partition by COLR.ORDER_LINE_ID) AS ORDER_DROP_TS ,                  --Nile bpk
  
  (CASE WHEN  (TRIM(STATUS_ID)) in ('3700.01','3700.01.01','3700.02') THEN (INSERT_TS) ELSE NULL END) RETURN_DATE,

  (CASE WHEN  (TRIM(STATUS_ID)) in ('1100','1100.200','1100.525','1300','1310') AND STATUS_QTY > 0 THEN (STATUS_QTY) ELSE NULL END) PENDING_OMS_QTY,

  (CASE WHEN  (TRIM(STATUS_ID)) in ('1500','1500.100','1500.101','3200','3200.050','3200.100','3200.200','3200.500','3200.520') AND STATUS_QTY > 0 THEN (STATUS_QTY) ELSE NULL END) PENDING_SHIPMENT_QTY,
  (CASE WHEN  (TRIM(STATUS_ID)) in ('3700','3700.00.03','3700.01.03','3700.01.540','3700.500','3700.7777') AND STATUS_QTY > 0 THEN (STATUS_QTY) ELSE NULL END) IN_TRANSIT_QTY,

  (CASE WHEN  (TRIM(STATUS_ID)) ='3700.8000' AND STATUS_QTY > 0 THEN (STATUS_QTY) ELSE NULL END) AWAITING_PICKUP_QTY,
  (CASE WHEN  (TRIM(STATUS_ID)) ='3700.9000' AND STATUS_QTY > 0 THEN (STATUS_QTY) ELSE NULL END) CUSTOMER_PICKED_UP_QTY,
  (CASE WHEN  (TRIM(STATUS_ID)) in ('1300','1310','1500','1500.100') AND STATUS_QTY > 0 THEN (STATUS_QTY) ELSE NULL END) BO_STATUS_QTY
FROM (
        SELECT *
        FROM TEMP_SALES_ORDER_DATA 
        
        UNION
        
        SELECT *
        FROM TEMP_RET_ORDER_DATA 
        
    ) COLR
JOIN 
(
SELECT  
  ORDER_LINE_ID,
  ORDER_DT,
  ORDER_LINE_SCHEDULE_KEY,
  max(CASE WHEN  (TRIM(STATUS_ID)) ='3700' and STATUS_QTY>0 THEN (STATUS_TS) WHEN (TRIM(STATUS_ID)) ='3700' and STATUS_QTY =0 THEN(INSERT_TS) ELSE NULL END)   ORDER_SHIPPED_DT,
 max(CASE WHEN  (TRIM(STATUS_ID)) ='3700.8000' and STATUS_QTY>0 THEN (STATUS_TS) WHEN (TRIM(STATUS_ID)) ='3700.8000' and STATUS_QTY =0 THEN(INSERT_TS)  ELSE NULL END)   STORE_RECEIVED_DT,
  max(CASE WHEN  (TRIM(STATUS_ID)) ='3700.8000' and STATUS_QTY>0 THEN NULL 
   WHEN (TRIM(STATUS_ID)) ='3700.9000' and STATUS_QTY>0 THEN (STATUS_TS) WHEN (TRIM(STATUS_ID)) ='3700.9000' and STATUS_QTY=0 THEN (INSERT_TS)
  ELSE NULL END)  CUSTOMER_PICKED_UP_DATE,
  
  max(CASE WHEN  (TRIM(STATUS_ID)) ='3700.8000' and STATUS_QTY>0 THEN NULL
   WHEN (TRIM(STATUS_ID)) ='3700.9000' and STATUS_QTY=0 THEN (INSERT_TS) WHEN (TRIM(STATUS_ID)) ='3700.9000' and STATUS_QTY>0 THEN (STATUS_TS)  
  ELSE NULL END)    CUSTOMER_PICKUP_TS,
  max(CASE WHEN  (TRIM(STATUS_ID)) ='3200.050' and STATUS_QTY>0 THEN (STATUS_TS) WHEN (TRIM(STATUS_ID)) ='3200.050' and STATUS_QTY=0 THEN (INSERT_TS) ELSE NULL END)  ORDER_DROP_TS                
  FROM 
  (
  SELECT *
        FROM TEMP_SALES_ORDER_DATA 
        
        UNION
        
        SELECT *
        FROM TEMP_RET_ORDER_DATA 
  ) 
  group by ORDER_LINE_ID,
  ORDER_DT,
  ORDER_LINE_SCHEDULE_KEY
)  DT on (COLR.order_line_id=DT.order_line_id and COLR.ORDER_DT=DT.ORDER_DT and COLR.ORDER_LINE_SCHEDULE_KEY = DT.ORDER_LINE_SCHEDULE_KEY)
WHERE STATUS_QTY>0 and COLR.STATUS_ID <= '9000' and COLR.STATUS_ID not in ('3700.01.545','3200.525','9000.300','3200.520','3700.01.540','1100.525') and COLR.STATUS_ID NOT IN ('3700.01.01','3700.02')""")

//orderLineRelease.repartition(300,col("ORDER_DT"),col("ORDER_HEADER_KEY")).createOrReplaceTempView("CACHED_ORDER_LINE_RELEASE")
CachedOrderLineRelease.createOrReplaceTempView("CACHED_ORDER_LINE_RELEASE")

// COMMAND ----------

// DBTITLE 1,Staging - AGG_BOSTS_DF

var AGG_BOSTS_DF_INIT = spark.sql(s"""
select * from (
  select

  ORDER_HEADER_KEY,
  Order_NO ORDER_ID,
  ORDER_LINE_KEY,
  ORDER_LINE_ID,

  LOCATION_KEY,
  LOCATION_ID,
  LOCATION_DESC,

  DISTRICT_KEY,
  DISTRICT_ID,
  DISTRICT_DESC,

  SHIP_NODE_KEY,
  SHIP_NODE_ID,
  SHIP_NODE_DESCRIPTION SHIP_NODE_DESC,

  ITEM_KEY,
  ITEM_ID,
  ITEM_NAME,
  ITEM_DEPARTMENT_KEY,
  ITEM_DEPARTMENT_ID,
  ITEM_DEPARTMENT_DESC,
  ITEM_CLASS_KEY,
  ITEM_CLASS_ID,
  ITEM_CLASS_DESC,

  CONCEPT_CD,
  ORDER_DT,
  MIN( CASE WHEN (TRIM(DATE_TYPE_ID)) = 'ECDDDeliveryDate' THEN (EXPECTED_DT) ELSE NULL END ) ECDD_DELIVERY_END_DT,
  MIN( CASE WHEN (TRIM(DATE_TYPE_ID)) = 'CurrentPromiseDate' THEN (EXPECTED_DT) ELSE NULL END ) CURRENT_PROMISE_DT,

  ORDER_SHIPPED_DATE ORDER_SHIPPED_DT,
  STORE_RECEIVED_DATE STORE_RECEIVED_DT ,
  
  CUSTOMER_PICKUP_TS,                --Nile bpk
  ORDER_DROP_TS,                     --Nile bpk
  CUSTOMER_TIME_TO_PICKUP,           --Nile bpk
  TOTAL_DAYS_IN_STOCKROOM_CNT,       --Nile bpk

  ORDER_AGE_DAYS_NBR,

  STORE_ORDER_SOURCE,

  PRODUCT_LINE,
  RECEIVING_NODE_CD,
  LINE_TOTAL_AMT,                    --Nile bpk
  ADDITIONAL_LINE_TYPE_CD,           --Nile bpk
  RETURN_ACTION_CD,                  --Nile bpk
  ORDER_QTY,                         --Nile bpk
  RETURN_REASON_CD,                  --Nile bpk
  RETURN_SUB_REASON_CD,              --Nile bpk
  RETURN_REASON_DESC,                --Nile bpk
  RETURN_ACTION,                     --Nile bpk
  DISPOSITION_CD,                    --Nile bpk
  STATUS_TS,                         --Nile bpk
  CHARGE_NAME,                       --Nile bpk
  CHARGE_PER_LINE_AMT,               --Nile bpk
  UNIT_LENGTH_NBR,
  UNIT_LENGTH_UOM_CD,
  UNIT_WIDTH_NBR,
  UNIT_WIDTH_UOM_CD,
  UNIT_HEIGHT_NBR,
  UNIT_HEIGHT_UOM_CD,
  UNIT_WEIGHT_NBR,
  UNIT_WEIGHT_UOM_CD,
  1 AS CARRIER_CD,
  LINE_STATUS STATUS_ID,
  LINE_DESCRIPTION STATUS_DESCRIPTION,
  UNIT STATUS_QTY,
  CUSTOMER_FIRST_NAME BILL_TO_FIRST_NAME,
  CUSTOMER_LAST_NAME BILL_TO_LAST_NAME,
  CUSTOMER_PHONE_NUMBER BILL_TO_PHONE_NBR,
  CUSTOMER_EMAIL BILL_TO_EMAIL_ADDRESS,
  Current_TIMESTAMP AS INSERT_TS,
  CURRENT_TIMESTAMP AS UPDATE_TS,
  --- Summary Fields
  PICKUP_ENABLED_FLAG,
  SHIP_TO_STORE_ENABLED_FLAG,
  PENDING_OMS_QTY,
  PENDING_SHIPMENT_QTY,
  IN_TRANSIT_QTY,
  AWAITING_PICKUP_QTY,
  CUSTOMER_PICKED_UP_QTY,
  CUSTOMER_PICKED_UP_DATE,
  BO_STATUS_QTY
from
        (Select
        OLS.ORDER_HEADER_KEY ORDER_HEADER_KEY,
        OH.ORDER_ID Order_NO,
        OLS.ORDER_LINE_KEY ORDER_LINE_KEY,
        OLS.ORDER_LINE_ID ORDER_LINE_ID,

        LOC.LOCATION_ID LOCATION_ID,
        LOC.LOCATION_DESC LOCATION_DESC,
        LOC.LOCATION_KEY LOCATION_KEY,
        LOC.DISTRICT_KEY DISTRICT_KEY,
        LOC.DISTRICT_ID DISTRICT_ID,
        LOC.DISTRICT_DESC DISTRICT_DESC,

        SN.SHIP_NODE_ID SHIP_NODE_ID,
        SN.SHIP_NODE_KEY SHIP_NODE_KEY,
        SN.SHIP_NODE_DESC SHIP_NODE_DESCRIPTION,
        ITM.CONCEPT_CD CONCEPT_CD,

        OLS.ORDER_DT ORDER_DT,
        OLS.LINE_STATUS,
        OLS.LINE_DESCRIPTION,
        OLS.UNIT,
        CAST(ORDER_SHIPPED_DT AS DATE) AS ORDER_SHIPPED_DATE,
        STORE_RECEIVED_DT AS STORE_RECEIVED_DATE,
        CUSTOMER_PICKUP_TS,                                                          --Nile bpk
        ORDER_DROP_TS,                                                               --Nile bpk
        
        datediff(CUSTOMER_PICKUP_TS,STORE_RECEIVED_DT) AS CUSTOMER_TIME_TO_PICKUP,   --Nile bpk 
        
        datediff(CURRENT_DATE(), STORE_RECEIVED_DT) AS TOTAL_DAYS_IN_STOCKROOM_CNT,  --Nile bpk
        
        datediff(CURRENT_DATE(), STORE_RECEIVED_DT) AS ORDER_AGE_DAYS_NBR,

        OD.EXPECTED_DT EXPECTED_DT,
        OD.DATE_TYPE_ID DATE_TYPE_ID,

        (CASE when trim(OH.STORE_ORDER_SOURCE) IS NULL or trim(OH.STORE_ORDER_SOURCE) like ('') then 'DTC' else OH.STORE_ORDER_SOURCE  end) STORE_ORDER_SOURCE,

        ITM.ITEM_KEY ITEM_KEY,
        ITM.ITEM_ID ITEM_ID,
        ITM.ITEM_NAME ITEM_NAME,
        OLS.PRODUCT_LINE PRODUCT_LINE,
        RECEIVING_NODE_CD,
        OLS.LINE_TOTAL_AMT,                     --Nile 
        OLS.ADDITIONAL_LINE_TYPE_CD,            --Nile 
        OLS.RETURN_ACTION_CD,                   --Nile 
        OLS.ORDER_QTY,                          --Nile 
        OLS.RETURN_REASON_CD,                   --Nile 
        OLS.RETURN_SUB_REASON_CD,               --Nile 
        OLS.RETURN_REASON_DESC,                 --Nile 
        OLS.RETURN_ACTION,                      --Nile 
        
        OLCHG.CHARGE_NAME,                      --Nile 
        OLCHG.CHARGE_PER_LINE_AMT,              --Nile 
        
        OLS.STATUS_TS,                          --Nile 
        
        RCPT.DISPOSITION_CD,                    --Nile 

        ITM.UNIT_LENGTH_NBR UNIT_LENGTH_NBR,
        ITM.UNIT_LENGTH_UOM_CD UNIT_LENGTH_UOM_CD,
        ITM.UNIT_WIDTH_NBR UNIT_WIDTH_NBR,
        ITM.UNIT_WIDTH_UOM_CD UNIT_WIDTH_UOM_CD,
        ITM.UNIT_HEIGHT_NBR UNIT_HEIGHT_NBR,
        ITM.UNIT_HEIGHT_UOM_CD UNIT_HEIGHT_UOM_CD,
        ITM.UNIT_WEIGHT_NBR UNIT_WEIGHT_NBR,
        ITM.UNIT_WEIGHT_UOM_CD UNIT_WEIGHT_UOM_CD,
        ITM.ITEM_DEPARTMENT_KEY ITEM_DEPARTMENT_KEY,
        ITM.ITEM_DEPARTMENT_ID ITEM_DEPARTMENT_ID,
        ITM.ITEM_DEPARTMENT_DESC ITEM_DEPARTMENT_DESC,
        ITM.ITEM_CLASS_KEY ITEM_CLASS_KEY,
        ITM.ITEM_CLASS_ID ITEM_CLASS_ID,
        ITM.ITEM_CLASS_DESC ITEM_CLASS_DESC,

        OH.BILL_TO_FIRST_NAME CUSTOMER_FIRST_NAME,
        OH.BILL_TO_LAST_NAME CUSTOMER_LAST_NAME,
        OH.BILL_TO_PHONE_NBR CUSTOMER_PHONE_NUMBER,
        OH.BILL_TO_EMAIL_ADDRESS CUSTOMER_EMAIL,

        --- Summary Fields ---
        LOC.PICKUP_ENABLED_FLAG PICKUP_ENABLED_FLAG,
        LOC.SHIP_TO_STORE_ENABLED_FLAG SHIP_TO_STORE_ENABLED_FLAG,
        OLS.PENDING_OMS_QTY,
        OLS.PENDING_SHIPMENT_QTY,
        OLS.IN_TRANSIT_QTY,
        OLS.AWAITING_PICKUP_QTY,
        OLS.CUSTOMER_PICKED_UP_QTY,
        OLS.CUSTOMER_PICKED_UP_DATE,
        OLS.BO_STATUS_QTY
      from
        CACHED_ORDER_LINE_RELEASE OLS 
        JOIN (SELECT  ORDER_ID,
                      STORE_ORDER_SOURCE,
                      BILL_TO_FIRST_NAME,
                      BILL_TO_LAST_NAME,
                      BILL_TO_PHONE_NBR,
                      BILL_TO_EMAIL_ADDRESS,
                      ORDER_HEADER_KEY,
                      ORDER_DT
                     FROM $L2_ANALYTICS.ORDER_HEADER
                     WHERE ORDER_DT >= '$FROM_DATE') OH

                                  ON OH.ORDER_HEADER_KEY = OLS.ORDER_HEADER_KEY
                                  AND OLS.ORDER_DT = OH.ORDER_DT
        LEFT JOIN
        (SELECT EXPECTED_DT,
                DATE_TYPE_ID,
                trim(ORDER_LINE_ID) ORDER_LINE_ID,
                ORDER_HEADER_KEY,
                ORDER_DT
            FROM $L2_ANALYTICS.ORDER_LINE_DATES
            where DATE_TYPE_ID in ('CurrentPromiseDate', 'ECDDDeliveryDate')
                AND ORDER_DT >= '$FROM_DATE') OD

                                  ON OLS.ORDER_DT = OD.ORDER_DT
                                  --AND OLS.ORDER_HEADER_KEY = OD.ORDER_HEADER_KEY
                                  AND trim(OLS.ORDER_LINE_ID) = trim(OD.ORDER_LINE_ID)
       LEFT JOIN
       (SELECT trim(DISPOSITION_CD) DISPOSITION_CD,
               trim(ORDER_LINE_ID) ORDER_LINE_ID,
               ORDER_HEADER_KEY,
               ORDER_DT
           FROM $L2_ANALYTICS.RECEIPT_LINE
           where ORDER_DT >= '$FROM_DATE') RCPT
                                 ON OLS.ORDER_DT = RCPT.ORDER_DT
                                 AND trim(OLS.ORDER_LINE_ID) = trim(RCPT.ORDER_LINE_ID)
       LEFT JOIN
       (SELECT trim(CHARGE_NAME) CHARGE_NAME,
               CHARGE_PER_LINE_AMT,
               trim(ORDER_LINE_ID) ORDER_LINE_ID,
               ORDER_DT
           FROM $L2_ANALYTICS.ORDER_LINE_CHARGE 
           where CHARGE_NAME = 'LineMerchEffective' and ORDER_DT >= '$FROM_DATE') OLCHG

                                  ON OLS.ORDER_DT = OLCHG.ORDER_DT
                                  AND trim(OLS.ORDER_LINE_ID) = trim(OLCHG.ORDER_LINE_ID)

        LEFT JOIN $L2_ANALYTICS.SHIP_NODE SN ON trim(OLS.SHIP_NODE_CD) = trim(SN.SHIP_NODE_ID)
        LEFT JOIN $L2_ANALYTICS.LOCATION LOC ON trim(OLS.RECEIVING_NODE_CD) = trim(LOC.PICKUP_LOCATION_CD)
        Left JOIN $L2_ANALYTICS.ITEM ITM ON OLS.ITEM_KEY = ITM.ITEM_KEY  

        )final
Group by
  ORDER_HEADER_KEY,
  ORDER_LINE_KEY,
  CONCEPT_CD,
  LOCATION_ID,
  LOCATION_DESC,
  LOCATION_KEY,
  DISTRICT_KEY,
  DISTRICT_ID,
  DISTRICT_DESC,
  Order_NO,
  ORDER_DT,
  ORDER_LINE_ID,
  LINE_STATUS,
  LINE_DESCRIPTION,
  ORDER_SHIPPED_DATE,
  CUSTOMER_PICKUP_TS,            --Nile 
  ORDER_DROP_TS,                 --Nile 
  CUSTOMER_TIME_TO_PICKUP,       --Nile 
  TOTAL_DAYS_IN_STOCKROOM_CNT,   --Nile 
  SHIP_NODE_ID,
  SHIP_NODE_KEY,
  SHIP_NODE_DESCRIPTION,
  UNIT,
  STORE_RECEIVED_DT,
  ORDER_AGE_DAYS_NBR,
  STORE_ORDER_SOURCE,
  ITEM_KEY,
  ITEM_ID,
  ITEM_NAME,
  PRODUCT_LINE,
  RECEIVING_NODE_CD,
  LINE_TOTAL_AMT,                      --Nile 
  ADDITIONAL_LINE_TYPE_CD,             --Nile 
  RETURN_ACTION_CD,                    --Nile 
  ORDER_QTY,                           --Nile 
  RETURN_REASON_CD,                    --Nile 
  RETURN_SUB_REASON_CD,                --Nile 
  RETURN_REASON_DESC,                  --Nile 
  RETURN_ACTION,                       --Nile 
  DISPOSITION_CD,                      --Nile 
  STATUS_TS,                           --Nile   
  CHARGE_NAME,                         --Nile 
  CHARGE_PER_LINE_AMT,                 --Nile 
  UNIT_LENGTH_NBR,
  UNIT_LENGTH_UOM_CD,
  UNIT_WIDTH_NBR,
  UNIT_WIDTH_UOM_CD,
  UNIT_HEIGHT_NBR,
  UNIT_HEIGHT_UOM_CD,
  UNIT_WEIGHT_NBR,
  UNIT_WEIGHT_UOM_CD,
  ITEM_DEPARTMENT_KEY,
  ITEM_DEPARTMENT_ID,
  ITEM_DEPARTMENT_DESC,
  ITEM_CLASS_KEY,
  ITEM_CLASS_ID,
  ITEM_CLASS_DESC,
  CUSTOMER_FIRST_NAME,
  CUSTOMER_LAST_NAME,
  CUSTOMER_PHONE_NUMBER,
  CUSTOMER_EMAIL,
  PICKUP_ENABLED_FLAG,
  SHIP_TO_STORE_ENABLED_FLAG,
  PENDING_OMS_QTY,
  PENDING_SHIPMENT_QTY,
  IN_TRANSIT_QTY,
  AWAITING_PICKUP_QTY,
  CUSTOMER_PICKED_UP_QTY,
  CUSTOMER_PICKED_UP_DATE,
  BO_STATUS_QTY)
""")
//.repartition(300,col("ORDER_LINE_ID"))


// COMMAND ----------

// DBTITLE 1,Bring Shipment Details
val shipmentLine = spark.sql(s"select shipment_key, order_line_id from $L2_ANALYTICS.SHIPMENT_LINE where ORDER_DT >= '$FROM_DATE'").repartition(600,col("shipment_key"))

val shipment = spark.sql(s"""select shipment_key, scac_id, scac_key from $L2_ANALYTICS.SHIPMENT
                              where ACT_SHIPMENT_DT >= '2019-12-01' and DOCUMENT_TYPE_CD = '0001' and ORDER_DT >= '$FROM_DATE'""").repartition(600,col("shipment_key"))

val joinedShipment = shipmentLine.join(shipment, Seq("shipment_key"), "inner").drop("shipment_key").repartition(300,col("ORDER_LINE_ID"))

val AGG_SHIP_DF= AGG_BOSTS_DF_INIT.join(joinedShipment, Seq("ORDER_LINE_ID"), "left")

AGG_SHIP_DF.createOrReplaceTempView("AGG_SHIP_DF")

// COMMAND ----------

// DBTITLE 1,Handle Duplicates due to Shipment

val AGG_BOSTS_STAGE_DF = spark.sql("""
                              select
                                ORDER_HEADER_KEY,
                                ORDER_ID,
                                ORDER_LINE_KEY,
                                ORDER_LINE_ID,
                                LOCATION_KEY,
                                LOCATION_ID,
                                LOCATION_DESC,
                                DISTRICT_KEY,7
                                DISTRICT_ID,
                                DISTRICT_DESC,
                                SHIP_NODE_KEY,
                                SHIP_NODE_ID,
                                SHIP_NODE_DESC,
                                ITEM_KEY,
                                ITEM_ID,
                                ITEM_NAME,
                                ITEM_DEPARTMENT_KEY,
                                ITEM_DEPARTMENT_ID,
                                ITEM_DEPARTMENT_DESC,
                                ITEM_CLASS_KEY,
                                ITEM_CLASS_ID,
                                ITEM_CLASS_DESC,

                                MAX(SCAC_KEY) SCAC_KEY,
                                SUBSTRING(MAX(CONCAT(  LPAD( SCAC_KEY, 11, '0'), SCAC_ID)), 12) AS SCAC_ID,

                                CONCEPT_CD,
                                ORDER_DT,
                                ECDD_DELIVERY_END_DT,
                                CURRENT_PROMISE_DT,
                                ORDER_SHIPPED_DT,
                                STORE_RECEIVED_DT,
                                CUSTOMER_PICKUP_TS,           --Nile bpk
                                ORDER_DROP_TS,                --Nile bpk
                                CUSTOMER_TIME_TO_PICKUP,      --Nile bpk
                                TOTAL_DAYS_IN_STOCKROOM_CNT,  --Nile bpk
                                ORDER_AGE_DAYS_NBR,
                                STORE_ORDER_SOURCE,
                                PRODUCT_LINE,
                                RECEIVING_NODE_CD,
                                LINE_TOTAL_AMT,          --Nile bpk
                                ADDITIONAL_LINE_TYPE_CD, --Nile bpk
                                RETURN_ACTION_CD,        --Nile bpk
                                ORDER_QTY,               --Nile bpk
                                RETURN_REASON_CD,        --Nile bpk
                                RETURN_SUB_REASON_CD,    --Nile bpk
                                RETURN_REASON_DESC,      --Nile bpk
                                RETURN_ACTION,           --Nile bpk
                                DISPOSITION_CD,          --Nile bpk
                                STATUS_TS,               --Nile bpk
                                CHARGE_NAME,             --Nile bpk
                                CHARGE_PER_LINE_AMT,     --Nile bpk
                                UNIT_LENGTH_NBR,
                                UNIT_LENGTH_UOM_CD,
                                UNIT_WIDTH_NBR,
                                UNIT_WIDTH_UOM_CD,
                                UNIT_HEIGHT_NBR,
                                UNIT_HEIGHT_UOM_CD,
                                UNIT_WEIGHT_NBR,
                                UNIT_WEIGHT_UOM_CD,
                                CARRIER_CD,
                                STATUS_ID,
                                STATUS_DESCRIPTION,
                                STATUS_QTY,
                                BO_STATUS_QTY, --Added by Nile team for BO qty
                                BILL_TO_FIRST_NAME,
                                BILL_TO_LAST_NAME,
                                BILL_TO_PHONE_NBR,
                                BILL_TO_EMAIL_ADDRESS,
                                INSERT_TS,
                                UPDATE_TS,
                                ---Store Capacity
                                PICKUP_ENABLED_FLAG,
                                SHIP_TO_STORE_ENABLED_FLAG,
                                PENDING_OMS_QTY,
                                PENDING_SHIPMENT_QTY,
                                IN_TRANSIT_QTY,
                                AWAITING_PICKUP_QTY,
                                CUSTOMER_PICKED_UP_QTY,
                                CUSTOMER_PICKED_UP_DATE

                              from
                                AGG_SHIP_DF
                              group by
                                ORDER_HEADER_KEY,
                                ORDER_ID,
                                ORDER_LINE_KEY,
                                ORDER_LINE_ID,
                                LOCATION_KEY,
                                LOCATION_ID,
                                LOCATION_DESC,
                                DISTRICT_KEY,
                                DISTRICT_ID,
                                DISTRICT_DESC,
                                SHIP_NODE_KEY,
                                SHIP_NODE_ID,
                                SHIP_NODE_DESC,
                                ITEM_KEY,
                                ITEM_ID,
                                ITEM_NAME,
                                ITEM_DEPARTMENT_KEY,
                                ITEM_DEPARTMENT_ID,
                                ITEM_DEPARTMENT_DESC,
                                ITEM_CLASS_KEY,
                                ITEM_CLASS_ID,
                                ITEM_CLASS_DESC,

                                CONCEPT_CD,
                                ORDER_DT,
                                ECDD_DELIVERY_END_DT,
                                CURRENT_PROMISE_DT,
                                ORDER_SHIPPED_DT,
                                STORE_RECEIVED_DT,
                                CUSTOMER_PICKUP_TS,            --Nile bpk
                                ORDER_DROP_TS,                 --Nile bpk
                                CUSTOMER_TIME_TO_PICKUP,       --Nile bpk
                                TOTAL_DAYS_IN_STOCKROOM_CNT,   --Nile bpk
                                ORDER_AGE_DAYS_NBR,
                                STORE_ORDER_SOURCE,
                                PRODUCT_LINE,
                                RECEIVING_NODE_CD,
                                LINE_TOTAL_AMT,          --Nile bpk
                                ADDITIONAL_LINE_TYPE_CD, --Nile bpk
                                RETURN_ACTION_CD,        --Nile bpk
                                ORDER_QTY,               --Nile bpk
                                RETURN_REASON_CD,        --Nile bpk
                                RETURN_SUB_REASON_CD,    --Nile bpk
                                RETURN_REASON_DESC,      --Nile bpk
                                RETURN_ACTION,           --Nile bpk
                                DISPOSITION_CD,          --Nile bpk
                                STATUS_TS,               --Nile bpk
                                CHARGE_NAME,             --Nile bpk
                                CHARGE_PER_LINE_AMT,     --Nile bpk
                                UNIT_LENGTH_NBR,
                                UNIT_LENGTH_UOM_CD,
                                UNIT_WIDTH_NBR,
                                UNIT_WIDTH_UOM_CD,
                                UNIT_HEIGHT_NBR,
                                UNIT_HEIGHT_UOM_CD,
                                UNIT_WEIGHT_NBR,
                                UNIT_WEIGHT_UOM_CD,
                                CARRIER_CD,
                                STATUS_ID,
                                STATUS_DESCRIPTION,
                                STATUS_QTY,
                                BO_STATUS_QTY,
                                BILL_TO_FIRST_NAME,
                                BILL_TO_LAST_NAME,
                                BILL_TO_PHONE_NBR,
                                BILL_TO_EMAIL_ADDRESS,
                                INSERT_TS,
                                UPDATE_TS,
                                PICKUP_ENABLED_FLAG,
                                SHIP_TO_STORE_ENABLED_FLAG,
                                PENDING_OMS_QTY,
                                PENDING_SHIPMENT_QTY,
                                IN_TRANSIT_QTY,
                                AWAITING_PICKUP_QTY,
                                CUSTOMER_PICKED_UP_QTY,
                                CUSTOMER_PICKED_UP_DATE
  """)
spark.conf.set("spark.databricks.io.cache.enabled", "true")
AGG_BOSTS_STAGE_DF.cache.createOrReplaceTempView("AGG_BOSTS_STAGE_DF")

// COMMAND ----------

// DBTITLE 1,Selecting Active Return Invoiced instead of Active Return Drafted

import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.broadcastTimeout", 3600)
spark.conf.set("spark.sql.shuffle.partitions", 300)
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead","LEGACY")

val AggBostsDf = spark.sql(s"""


select RQD_DATA.* from AGG_BOSTS_STAGE_DF  RQD_DATA 
INNER JOIN (
select coalesce(OL_37.ORDER_LINE_ID,OL_39.ORDER_LINE_ID) ORDER_LINE_ID,
coalesce(OL_37.ITEM_ID,OL_39.ITEM_ID) ITEM_ID,
case when ifnull(OL_37.STATUS_TS_37,'1900-01-01') < IFNULL(OL_39.STATUS_TS_39,'1900-01-01') THEN  OL_39.STATUS_ID ELSE OL_37.STATUS_ID END RQD_STATUS_ID  from 
(
select ORDER_LINE_ID,ITEM_ID,STATUS_ID,MAX(STATUS_TS) STATUS_TS_37 from AGG_BOSTS_STAGE_DF
where status_id not in ('3950.01') group by ORDER_LINE_ID,ITEM_ID,STATUS_ID
 ) OL_37
FULL JOIN 
(
select ORDER_LINE_ID,ITEM_ID,STATUS_ID,MAX(STATUS_TS) STATUS_TS_39 from AGG_BOSTS_STAGE_DF 
where status_id  in ('3950.01') 
 group by ORDER_LINE_ID,ITEM_ID,STATUS_ID
  
 
  ) OL_39 on OL_37.ORDER_LINE_ID=OL_39.ORDER_LINE_ID and OL_37.ITEM_ID=OL_39.ITEM_ID
  ) OL ON (RQD_DATA.ORDER_LINE_ID=OL.ORDER_LINE_ID AND RQD_DATA.STATUS_ID=OL.RQD_STATUS_ID);

""")

//orderLineRelease.repartition(300,col("ORDER_DT"),col("ORDER_HEADER_KEY")).createOrReplaceTempView("CACHED_ORDER_LINE_RELEASE")
AggBostsDf.createOrReplaceTempView("AGG_BOSTS_DF")




// COMMAND ----------

// DBTITLE 1,DROP OPEN_ORDERS_AGG

spark.sql(s"""drop table if exists $L2_ANALYTICS_TABLES.OPEN_ORDERS_AGG""")
dbutils.fs.rm("dbfs:/mnt/data/governed/l2/analytics/order/open_orders_agg/", true)
Thread.sleep(10000)

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC CREATE TABLE $L2_ANALYTICS_TABLES.OPEN_ORDERS_AGG(
// MAGIC   ORDER_HEADER_KEY int NOT NULL,
// MAGIC   ORDER_ID string NOT NULL,
// MAGIC   ORDER_LINE_KEY bigint NOT NULL,
// MAGIC   ORDER_LINE_ID string NOT NULL,
// MAGIC   LOCATION_KEY int,
// MAGIC   LOCATION_ID string,
// MAGIC   LOCATION_DESC string,
// MAGIC   DISTRICT_KEY int,
// MAGIC   DISTRICT_ID string,
// MAGIC   DISTRICT_DESC string,
// MAGIC   SHIP_NODE_KEY int,
// MAGIC   SHIP_NODE_ID string,
// MAGIC   SHIP_NODE_DESC string,
// MAGIC   ITEM_KEY int,
// MAGIC   ITEM_ID string,
// MAGIC   ITEM_NAME string,
// MAGIC   ITEM_DEPARTMENT_KEY int,
// MAGIC   ITEM_DEPARTMENT_ID string,
// MAGIC   ITEM_DEPARTMENT_DESC string,
// MAGIC   ITEM_CLASS_KEY int,
// MAGIC   ITEM_CLASS_ID string,
// MAGIC   ITEM_CLASS_DESC string,
// MAGIC   SCAC_KEY int,
// MAGIC   SCAC_ID string,
// MAGIC   CONCEPT_CD string,
// MAGIC   ORDER_DT date,
// MAGIC   ORDER_QTY int,                           --Nile 
// MAGIC   ECDD_DELIVERY_END_DT timestamp,
// MAGIC   CURRENT_PROMISE_DT timestamp,
// MAGIC   ORDER_SHIPPED_DT date,
// MAGIC   STORE_RECEIVED_DT timestamp,
// MAGIC   ORDER_AGE_DAYS_NBR int,
// MAGIC   TOTAL_DAYS_IN_STOCKROOM_CNT int,         --Nile 
// MAGIC   STORE_ORDER_SOURCE string,
// MAGIC   PRODUCT_LINE string,
// MAGIC   ADDITIONAL_LINE_TYPE_CD string,          --Nile  
// MAGIC   DISPOSITION_CD string,                   --Nile 
// MAGIC   RECEIVING_NODE_CD string,                --Nile 
// MAGIC   CHARGE_NAME string,                      --Nile 
// MAGIC   CHARGE_PER_LINE_AMT DECIMAL(15,4),       --Nile 
// MAGIC   LINE_TOTAL_AMT DECIMAL(15,4),            --Nile 
// MAGIC   UNIT_LENGTH_NBR decimal(14, 4),
// MAGIC   UNIT_LENGTH_UOM_CD string,
// MAGIC   UNIT_WIDTH_NBR decimal(14, 4),
// MAGIC   UNIT_WIDTH_UOM_CD string,
// MAGIC   UNIT_HEIGHT_NBR decimal(14, 4),
// MAGIC   UNIT_HEIGHT_UOM_CD string,
// MAGIC   UNIT_WEIGHT_NBR decimal(14, 4),
// MAGIC   UNIT_WEIGHT_UOM_CD string,
// MAGIC   CARRIER_CD string,
// MAGIC   STATUS_ID string,
// MAGIC   STATUS_DESCRIPTION string,
// MAGIC   STATUS_TS timestamp,                     --Nile 
// MAGIC   ORDER_DROP_TS TIMESTAMP,                 --Nile 
// MAGIC   CUSTOMER_PICKUP_TS TIMESTAMP,            --Nile 
// MAGIC   CUSTOMER_TIME_TO_PICKUP INT,             --Nile 
// MAGIC   STATUS_QTY decimal(14, 4),
// MAGIC   BO_STATUS_QTY decimal(15, 4),
// MAGIC   RETURN_ACTION_CD string,                 --Nile 
// MAGIC   RETURN_ACTION string,                    --Nile 
// MAGIC   RETURN_REASON_CD string,                 --Nile 
// MAGIC   RETURN_REASON_DESC string,               --Nile 
// MAGIC   RETURN_SUB_REASON_CD string,             --Nile 
// MAGIC   BILL_TO_FIRST_NAME string,
// MAGIC   BILL_TO_LAST_NAME string,
// MAGIC   BILL_TO_PHONE_NBR string,
// MAGIC   BILL_TO_EMAIL_ADDRESS string,
// MAGIC   INSERT_TS timestamp NOT NULL,
// MAGIC   UPDATE_TS timestamp NOT NULL
// MAGIC ) USING DELTA
// MAGIC PARTITIONED BY (ORDER_DT)
// MAGIC LOCATION "dbfs:/mnt/data/governed/l2/analytics/order/open_orders_agg/";

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO
// MAGIC   $L2_ANALYTICS_TABLES.OPEN_ORDERS_AGG
// MAGIC select
// MAGIC   ORDER_HEADER_KEY,
// MAGIC   ORDER_ID,
// MAGIC   ORDER_LINE_KEY,
// MAGIC   ORDER_LINE_ID,
// MAGIC   LOCATION_KEY,
// MAGIC   LOCATION_ID,
// MAGIC   --Store_NO,
// MAGIC   LOCATION_DESC,
// MAGIC   ---Store_Name,
// MAGIC   DISTRICT_KEY,
// MAGIC   DISTRICT_ID,
// MAGIC   DISTRICT_DESC,
// MAGIC 
// MAGIC   SHIP_NODE_KEY,
// MAGIC   --ORIGIN_DC_Key ,
// MAGIC   SHIP_NODE_ID,
// MAGIC   --ORIGIN_DC ,
// MAGIC   SHIP_NODE_DESC,
// MAGIC   --ORIGIN_DC_NAME, --Add
// MAGIC   ITEM_KEY,
// MAGIC   --SKU_Key,
// MAGIC   ITEM_ID,
// MAGIC   --SKU,
// MAGIC   ITEM_NAME,
// MAGIC   ---SKU_NAME,
// MAGIC   ITEM_DEPARTMENT_KEY,
// MAGIC   ITEM_DEPARTMENT_ID,
// MAGIC   ITEM_DEPARTMENT_DESC,
// MAGIC   ITEM_CLASS_KEY,
// MAGIC   ITEM_CLASS_ID,
// MAGIC   ITEM_CLASS_DESC,
// MAGIC   SCAC_KEY,
// MAGIC   --CARRIER_KEY
// MAGIC   SCAC_ID,
// MAGIC   --CARRIER,
// MAGIC   CONCEPT_CD,
// MAGIC   ORDER_DT,
// MAGIC   ORDER_QTY,                 --Nile 
// MAGIC   ECDD_DELIVERY_END_DT,
// MAGIC   --ORIGINAL_ECDD
// MAGIC   CURRENT_PROMISE_DT,
// MAGIC   ---CURRENT_ECDD,
// MAGIC   ORDER_SHIPPED_DT,
// MAGIC   STORE_RECEIVED_DT,
// MAGIC   ORDER_AGE_DAYS_NBR,
// MAGIC   TOTAL_DAYS_IN_STOCKROOM_CNT,   --Nile 
// MAGIC   STORE_ORDER_SOURCE,
// MAGIC   --CHANNEL,
// MAGIC   PRODUCT_LINE,
// MAGIC   ADDITIONAL_LINE_TYPE_CD,   --Nile 
// MAGIC   DISPOSITION_CD,            --Nile 
// MAGIC   RECEIVING_NODE_CD,         --Nile 
// MAGIC   CHARGE_NAME,               --Nile 
// MAGIC   CHARGE_PER_LINE_AMT,       --Nile 
// MAGIC   --SHIP_MODE,
// MAGIC   LINE_TOTAL_AMT,            --NILE 
// MAGIC   UNIT_LENGTH_NBR,
// MAGIC   UNIT_LENGTH_UOM_CD,
// MAGIC   UNIT_WIDTH_NBR,
// MAGIC   UNIT_WIDTH_UOM_CD,
// MAGIC   UNIT_HEIGHT_NBR,
// MAGIC   UNIT_HEIGHT_UOM_CD,
// MAGIC   UNIT_WEIGHT_NBR,
// MAGIC   UNIT_WEIGHT_UOM_CD,
// MAGIC   CARRIER_CD,
// MAGIC   STATUS_ID,
// MAGIC   --- ADD
// MAGIC   STATUS_DESCRIPTION,
// MAGIC   STATUS_TS,               --Nile 
// MAGIC   ORDER_DROP_TS,           --Nile 
// MAGIC   CUSTOMER_PICKUP_TS,      --Nile 
// MAGIC   CUSTOMER_TIME_TO_PICKUP, --Nile 
// MAGIC   --ADD
// MAGIC   STATUS_QTY,
// MAGIC   BO_STATUS_QTY,
// MAGIC   RETURN_ACTION_CD,        --Nile 
// MAGIC   RETURN_ACTION,           --Nile 
// MAGIC   RETURN_REASON_CD,        --Nile 
// MAGIC   RETURN_REASON_DESC,      --Nile 
// MAGIC   RETURN_SUB_REASON_CD,    --Nile 
// MAGIC   BILL_TO_FIRST_NAME,
// MAGIC   BILL_TO_LAST_NAME,
// MAGIC   BILL_TO_PHONE_NBR,
// MAGIC   BILL_TO_EMAIL_ADDRESS,
// MAGIC   from_utc_timestamp(current_timestamp(), "America/Los_Angeles") AS INSERT_TS,
// MAGIC   from_utc_timestamp(current_timestamp(), "America/Los_Angeles") AS UPDATE_TS
// MAGIC from
// MAGIC   AGG_BOSTS_DF
// MAGIC --Where
// MAGIC   --CUSTOMER_PICKED_UP_DATE IS NULL
// MAGIC   --STATUS_ID not in ('3700.9000')

// COMMAND ----------

// DBTITLE 1,DROP STORE_CAPACITY_AGG
spark.sql(s"""drop table if exists $L2_ANALYTICS_TABLES.STORE_CAPACITY_AGG""")
dbutils.fs.rm("dbfs:/mnt/data/governed/l2/analytics/order/store_capacity_agg/", true)
Thread.sleep(10000)

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC CREATE TABLE $L2_ANALYTICS_TABLES.STORE_CAPACITY_AGG(
// MAGIC   CONCEPT_CD string,
// MAGIC   LOCATION_KEY int,
// MAGIC   LOCATION_ID string,
// MAGIC   LOCATION_DESC string,
// MAGIC   DISTRICT_KEY int,
// MAGIC   DISTRICT_ID string,
// MAGIC   DISTRICT_DESC string,
// MAGIC   BOSTS_AVAIL_DT date,
// MAGIC   ADJ_BOSTS_AVAIL_DT date,
// MAGIC   PICKUP_ENABLED_FLAG string,
// MAGIC   SHIP_TO_STORE_ENABLED_FLAG string,
// MAGIC   CUSTOMER_PICKUP_RATE decimal(14, 2),
// MAGIC   CAPACITY_QTY decimal(14, 2),
// MAGIC   CONSUMED_CAPACITY_QTY decimal(14, 2),
// MAGIC   AWAITING_PICKUP_IN_STORE_QTY decimal(14, 2),
// MAGIC   IN_TRANSIT_QTY decimal(14, 2),
// MAGIC   PENDING_SHIPMENT_QTY decimal(14, 2),
// MAGIC   PENDING_IN_OMS_QTY decimal(14, 2),
// MAGIC   AVG_DAILY_CUSTOMER_PICKUP_QTY decimal(14, 2),
// MAGIC   DAYS_OVER_STORE_CAPACITY_NBR decimal(14, 2),
// MAGIC   STOCKROOM_UTILIZATION_PCT decimal(15, 2),
// MAGIC   AVG_AGE_STOCKROOM_ORDER_DAYS decimal(14, 2),
// MAGIC   MAX_AGE_STOCKROOM_ORDER_DAYS int,
// MAGIC   INSERT_TS timestamp NOT NULL,
// MAGIC   UPDATE_TS timestamp NOT NULL
// MAGIC ) USING DELTA
// MAGIC PARTITIONED BY (LOCATION_ID)
// MAGIC LOCATION "dbfs:/mnt/data/governed/l2/analytics/order/store_capacity_agg/";

// COMMAND ----------

// DBTITLE 1,STORE_CAPACITY_AGG INSERT
// MAGIC %sql
// MAGIC INSERT INTO
// MAGIC   $L2_ANALYTICS_TABLES.STORE_CAPACITY_AGG
// MAGIC select
// MAGIC   CONCEPT_CD,
// MAGIC   LOCATION_KEY,
// MAGIC   LOCATION_ID,
// MAGIC   --Store_NO,
// MAGIC   LOCATION_DESC,
// MAGIC   ---Store_Name,
// MAGIC   DISTRICT_KEY,
// MAGIC   DISTRICT_ID,
// MAGIC   DISTRICT_DESC,
// MAGIC 
// MAGIC   BOSTS_AVAIL_DT,
// MAGIC   ADJ_BOSTS_AVAIL_DT,
// MAGIC   PICKUP_ENABLED_FLAG,
// MAGIC   SHIP_TO_STORE_ENABLED_FLAG,
// MAGIC   STORE_CUSTOMER_PICKUP_RATE CUSTOMER_PICKUP_RATE,
// MAGIC   STORE_CAPACITY_QTY CAPACITY_QTY,
// MAGIC 
// MAGIC   (
// MAGIC     Coalesce(SUM(PENDING_OMS_QTY), 0) + Coalesce(SUM(PENDING_SHIPMENT_QTY), 0) + Coalesce(SUM(IN_TRANSIT_QTY), 0) + Coalesce(SUM(AWAITING_PICKUP_QTY), 0)
// MAGIC   ) AS CONSUMED_CAPACITY_QTY,
// MAGIC   -- PENDING_IN_OMS_QTY + PENDING_SHIPMENT_QTY + IN_TRANSIT_QTY + AWAITING_PICKUP_IN_STORE_QTY
// MAGIC   Coalesce(SUM(AWAITING_PICKUP_QTY), 0) AWAITING_PICKUP_IN_STORE_QTY,
// MAGIC   Coalesce(SUM(IN_TRANSIT_QTY), 0) IN_TRANSIT_QTY,
// MAGIC   Coalesce(SUM(PENDING_SHIPMENT_QTY), 0) PENDING_SHIPMENT_QTY,
// MAGIC   Coalesce(SUM(PENDING_OMS_QTY), 0) PENDING_IN_OMS_QTY,
// MAGIC   --SUM(CUSTOMER_PICKED_UP_QTY) CUSTOMER_PICKED_UP_QTY ,
// MAGIC   (
// MAGIC     Sum(
// MAGIC       CASE
// MAGIC         WHEN CUSTOMER_PICKED_UP_DATE >= date_add(CURRENT_DATE(), -7) THEN (CUSTOMER_PICKED_UP_QTY)
// MAGIC         ELSE 0
// MAGIC       END
// MAGIC     )
// MAGIC   ) / 7 AVG_DAILY_CUSTOMER_PICKUP_QTY,
// MAGIC   --SUM(UNIT)  QTY ,
// MAGIC   ( ( Coalesce(SUM(PENDING_OMS_QTY), 0) + Coalesce(SUM(PENDING_SHIPMENT_QTY), 0) + Coalesce(SUM(IN_TRANSIT_QTY), 0) + Coalesce(SUM(AWAITING_PICKUP_QTY), 0) ) - STORE_CAPACITY_QTY ) / STORE_CUSTOMER_PICKUP_RATE
// MAGIC                                  AS DAYS_OVER_STORE_CAPACITY_NBR,
// MAGIC   Coalesce(SUM(AWAITING_PICKUP_QTY), 0) / STORE_CAPACITY_QTY AS STOCKROOM_UTILIZATION_PCT,
// MAGIC   --(Coalesce(SUM(AWAITING_PICKUP_QTY),0)/Coalesce(SUM(STORE_CAPACITY_QTY),0)) ,
// MAGIC 
// MAGIC   AVG(
// MAGIC       CASE
// MAGIC         WHEN CUSTOMER_PICKED_UP_DATE IS NULL THEN (ORDER_AGE_DAYS_NBR)
// MAGIC         ELSE Null
// MAGIC       END
// MAGIC     ) AVG_AGE_STOCKROOM_ORDER_DAYS,
// MAGIC   ----AVG_AGE_STOCKROOM_ORDER_DAYS
// MAGIC    MAX(
// MAGIC       CASE
// MAGIC         WHEN CUSTOMER_PICKED_UP_DATE IS NULL THEN (ORDER_AGE_DAYS_NBR)
// MAGIC         ELSE 0
// MAGIC       END
// MAGIC     )  MAX_AGE_STOCKROOM_ORDER_DAYS,
// MAGIC   from_utc_timestamp(current_timestamp(), "America/Los_Angeles") AS INSERT_TS,
// MAGIC   from_utc_timestamp(current_timestamp(), "America/Los_Angeles") AS UPDATE_TS
// MAGIC from
// MAGIC   (
// MAGIC     SELECT
// MAGIC       ABD.*,
// MAGIC       LCS.CUSTOMER_PICKUP_RATE STORE_CUSTOMER_PICKUP_RATE,
// MAGIC       LCS.CAPACITY_QTY STORE_CAPACITY_QTY,
// MAGIC       '9999-99-99' AS BOSTS_AVAIL_DT,
// MAGIC       LCA.EARLIEST_AVAILABILIITY_DT ADJ_BOSTS_AVAIL_DT
// MAGIC     from
// MAGIC       AGG_BOSTS_DF ABD
// MAGIC       left join ( Select LCS1.FULFILLMENT_LOCATION_ID FULFILLMENT_LOCATION_ID,
// MAGIC                          LCS1.CAPACITY_START_DT CAPACITY_START_DT,
// MAGIC                          LCS1.CAPACITY_END_DT CAPACITY_END_DT,
// MAGIC                          SUM(LCS1.CUSTOMER_PICKUP_RATE) CUSTOMER_PICKUP_RATE,
// MAGIC                          SUM(LCS1.CAPACITY_QTY) CAPACITY_QTY
// MAGIC                          FROM L2_ANALYTICS.location_capacity_supply LCS1
// MAGIC                          WHERE LCS1.CAPACITY_START_DT <= CURRENT_DATE()
// MAGIC                                AND CURRENT_DATE() <= LCS1.CAPACITY_END_DT
// MAGIC                                AND trim(LCS1.CAPACITY_TYPE_CD) = 'STS'
// MAGIC                           GROUP BY LCS1.FULFILLMENT_LOCATION_ID,
// MAGIC                                    LCS1.CAPACITY_START_DT,
// MAGIC                                    LCS1.CAPACITY_END_DT) LCS
// MAGIC          ON trim(ABD.RECEIVING_NODE_CD) = trim(LCS.FULFILLMENT_LOCATION_ID)
// MAGIC        left join (Select LCA1.FULLFILLMENT_LOCATION_ID FULLFILLMENT_LOCATION_ID,
// MAGIC                          MAX(LCA1.EARLIEST_AVAILABILIITY_DT) EARLIEST_AVAILABILIITY_DT
// MAGIC                   FROM L2_ANALYTICS.location_capacity_availability LCA1
// MAGIC                   WHERE trim(LCA1.AVAILABILITY_STATUS_CD) = 'Available'
// MAGIC                   AND trim(LCA1.CAPACITY_TYPE_CD) = 'STS'
// MAGIC                   GROUP BY LCA1.FULLFILLMENT_LOCATION_ID  ) LCA
// MAGIC         ON trim(ABD.RECEIVING_NODE_CD) = trim(LCA.FULLFILLMENT_LOCATION_ID)
// MAGIC         ---group by
// MAGIC   ) agg
// MAGIC group by
// MAGIC   RECEIVING_NODE_CD,
// MAGIC   CONCEPT_CD,
// MAGIC   LOCATION_ID,
// MAGIC   --Store_NO,
// MAGIC   LOCATION_DESC,
// MAGIC   ---Store_Name,
// MAGIC   LOCATION_KEY,
// MAGIC   DISTRICT_KEY,
// MAGIC   DISTRICT_ID,
// MAGIC   DISTRICT_DESC,
// MAGIC   BOSTS_AVAIL_DT,
// MAGIC   ADJ_BOSTS_AVAIL_DT,
// MAGIC   PICKUP_ENABLED_FLAG,
// MAGIC   SHIP_TO_STORE_ENABLED_FLAG,
// MAGIC   STORE_CUSTOMER_PICKUP_RATE,
// MAGIC   STORE_CAPACITY_QTY

// COMMAND ----------


