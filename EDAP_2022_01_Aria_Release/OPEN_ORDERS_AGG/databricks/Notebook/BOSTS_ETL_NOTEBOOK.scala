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

// DBTITLE 1,CACHED_ORDER_LINE_RELEASE
import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.broadcastTimeout", 3600)
spark.conf.set("spark.sql.shuffle.partitions", 300)
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead","LEGACY")

val orderLineRelease = spark.sql(s"""
Select

  COLR.ORDER_DT ORDER_DT,
  COLR.ORDER_LINE_ID ORDER_LINE_ID,
  COLR.ORDER_HEADER_KEY ORDER_HEADER_KEY,
  COLR.ORDER_LINE_KEY ORDER_LINE_KEY,
  MIN(CASE WHEN COLR.STATUS_QTY > 0 THEN COLR.STATUS_ID END)  LINE_STATUS,
  --Max(CASE WHEN COLR.STATUS_QTY > 0 THEN COLR.STATUS_QTY END) UNIT,

  SUBSTRING(MIN(CONCAT(LPAD( CASE WHEN COLR.STATUS_QTY > 0 THEN COLR.STATUS_ID END , 11, '0'), COLR.STATUS_QTY)), 12) AS UNIT,

  SUBSTRING(MIN(CONCAT(  LPAD( CASE WHEN COLR.STATUS_QTY > 0 THEN COLR.STATUS_ID END , 11, '0'), COLR.STATUS_DESC)), 12) AS LINE_DESCRIPTION,

  COLR.ITEM_KEY ITEM_KEY,
  COLR.PRODUCT_LINE PRODUCT_LINE,
  COLR.SHIP_NODE_CD SHIP_NODE_CD,
  COLR.RECEIVING_NODE_CD RECEIVING_NODE_CD,
  COLR.LINE_TOTAL_AMT LINE_TOTAL_AMT,                                   
  COLR.ADDITIONAL_LINE_TYPE_CD ADDITIONAL_LINE_TYPE_CD,                 
  COLR.RETURN_ACTION_CD RETURN_ACTION_CD,                               
  COLR.ORDER_QTY ORDER_QTY,                                             
  COLR.RETURN_REASON_CD RETURN_REASON_CD,                               
  COLR.RETURN_SUB_REASON_CD RETURN_SUB_REASON_CD,                       
  COLR.RETURN_REASON_DESC RETURN_REASON_DESC,                           
  COLR.RETURN_ACTION RETURN_ACTION,                                     
  MIN(COLR.STATUS_TS) STATUS_TS,                                        

  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) ='3700' THEN (COLR.INSERT_TS) ELSE NULL END) ORDER_SHIPPED_DT,
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) ='3700.8000' THEN (COLR.INSERT_TS) ELSE NULL END) STORE_RECEIVED_DT,
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) ='3700.9000' THEN (COLR.INSERT_TS) ELSE NULL END) CUSTOMER_PICKED_UP_DATE,
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) in ('9000','3700.01.545','3200.525','9000.300','3200.520','3700.01.540','1100.525') OR (TRIM(COLR.STATUS_ID)) >= '9000'  THEN (COLR.INSERT_TS) ELSE NULL END) CANCELLED_DATE,
  
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) ='3700.9000' THEN (COLR.INSERT_TS) ELSE NULL END) CUSTOMER_PICKUP_TS,            
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) ='3200.050' THEN (COLR.INSERT_TS) ELSE NULL END) ORDER_DROP_TS,                  
  
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) in ('3700.01','3700.01.01','3700.02') THEN (COLR.INSERT_TS) ELSE NULL END) RETURN_DATE,

  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) in ('1100','1100.200','1100.525','1300','1310') AND COLR.STATUS_QTY > 0 THEN (COLR.STATUS_QTY) ELSE NULL END) PENDING_OMS_QTY,

  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) in ('1500','1500.100','1500.101','3200','3200.050','3200.100','3200.200','3200.500','3200.520') AND COLR.STATUS_QTY > 0 THEN (COLR.STATUS_QTY) ELSE NULL END) PENDING_SHIPMENT_QTY,
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) in ('3700','3700.00.03','3700.01.03','3700.01.540','3700.500','3700.7777') AND COLR.STATUS_QTY > 0 THEN (COLR.STATUS_QTY) ELSE NULL END) IN_TRANSIT_QTY,

  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) ='3700.8000' AND COLR.STATUS_QTY > 0 THEN (COLR.STATUS_QTY) ELSE NULL END) AWAITING_PICKUP_QTY,
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) ='3700.9000' AND COLR.STATUS_QTY > 0 THEN (COLR.STATUS_QTY) ELSE NULL END) CUSTOMER_PICKED_UP_QTY,
  MIN(CASE WHEN  (TRIM(COLR.STATUS_ID)) in ('1300','1310','1500','1500.100') AND COLR.STATUS_QTY > 0 THEN (COLR.STATUS_QTY) ELSE NULL END) BO_STATUS_QTY --Added for BOSTS story by NILE team

FROM
    (select
      OL.ORDER_DT ORDER_DT,
      trim(OL.ORDER_LINE_ID) ORDER_LINE_ID,
      OL.ORDER_HEADER_KEY ORDER_HEADER_KEY,
      OL.ORDER_LINE_KEY ORDER_LINE_KEY,
      OL.ITEM_KEY ITEM_KEY,
      trim(OL.PRODUCT_LINE) PRODUCT_LINE,
      trim(OL.SHIP_NODE_CD) SHIP_NODE_CD,
      trim(OL.RECEIVING_NODE_CD) RECEIVING_NODE_CD,
      OL.LINE_TOTAL_AMT LINE_TOTAL_AMT,                                              
      trim(OL.ADDITIONAL_LINE_TYPE_CD) ADDITIONAL_LINE_TYPE_CD,                      
      trim(OL.RETURN_ACTION_CD) RETURN_ACTION_CD,                                    
      OL.ORDER_QTY ORDER_QTY,                                                        
      trim(OL.RETURN_REASON_CD) RETURN_REASON_CD,                                    
      trim(OL.RETURN_SUB_REASON_CD) RETURN_SUB_REASON_CD,                            
      trim(OL.RETURN_REASON_DESC) RETURN_REASON_DESC,                                
      trim(OL.RETURN_ACTION) RETURN_ACTION,                                          
      trim(ORS.STATUS_ID) STATUS_ID,
      ORS.STATUS_QTY STATUS_QTY,
      ORS.STATUS_TS STATUS_TS,                                                       
      ORS.INSERT_TS INSERT_TS,                                                       
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
                LINE_TOTAL_AMT LINE_TOTAL_AMT,                                               
                trim(ADDITIONAL_LINE_TYPE_CD) ADDITIONAL_LINE_TYPE_CD,                       
                trim(RETURN_ACTION_CD) RETURN_ACTION_CD,                                     
                ORDER_QTY ORDER_QTY,                                                         
                trim(RETURN_REASON_CD) RETURN_REASON_CD,                                     
                trim(RETURN_SUB_REASON_CD) RETURN_SUB_REASON_CD,                             
                trim(RETURN_REASON_DESC) RETURN_REASON_DESC,                                 
                trim(RETURN_ACTION) RETURN_ACTION                                            
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
                    STATUS_QTY STATUS_QTY,
                    STATUS_TS STATUS_TS,          
                    INSERT_TS INSERT_TS,          
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

      ) COLR

GROUP BY
 COLR.ORDER_DT,
 COLR.ORDER_LINE_ID,
 COLR.ORDER_LINE_KEY,
 COLR.PRODUCT_LINE,
 COLR.ITEM_KEY,
 COLR.ORDER_HEADER_KEY,
 COLR.SHIP_NODE_CD,
 COLR.RECEIVING_NODE_CD,
 COLR.LINE_TOTAL_AMT,                                         
 COLR.ADDITIONAL_LINE_TYPE_CD,                                
 COLR.RETURN_ACTION_CD,                                       
 COLR.ORDER_QTY,                                              
 COLR.RETURN_REASON_CD,                                       
 COLR.RETURN_SUB_REASON_CD,                                   
 COLR.RETURN_REASON_DESC,                                     
 COLR.RETURN_ACTION                                           
 --COLR.STATUS_                                               

HAVING
LINE_STATUS not in ('3700.01.545','3200.525','9000.300','3200.520','3700.01.540','1100.525')  --Cancelled
AND LINE_STATUS <= '9000' -- Cancelled
AND LINE_STATUS NOT IN ('3700.01.01','3700.02') --- Returned
 --AND RETURN_DATE IS NULL
              """)

//orderLineRelease.repartition(300,col("ORDER_DT"),col("ORDER_HEADER_KEY")).createOrReplaceTempView("CACHED_ORDER_LINE_RELEASE")
orderLineRelease.createOrReplaceTempView("CACHED_ORDER_LINE_RELEASE")

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
  
  CUSTOMER_PICKUP_TS,                
  ORDER_DROP_TS,                     
  CUSTOMER_TIME_TO_PICKUP,           
  TOTAL_DAYS_IN_STOCKROOM_CNT,       

  ORDER_AGE_DAYS_NBR,

  STORE_ORDER_SOURCE,

  PRODUCT_LINE,
  RECEIVING_NODE_CD,
  LINE_TOTAL_AMT,                    
  ADDITIONAL_LINE_TYPE_CD,           
  RETURN_ACTION_CD,                  
  ORDER_QTY,                         
  RETURN_REASON_CD,                  
  RETURN_SUB_REASON_CD,              
  RETURN_REASON_DESC,                
  RETURN_ACTION,                     
  DISPOSITION_CD,                    
  STATUS_TS,                         
  CHARGE_NAME,                       
  CHARGE_PER_LINE_AMT,               
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
        OLS.Unit UNIT,
        CAST(ORDER_SHIPPED_DT AS DATE) AS ORDER_SHIPPED_DATE,
        STORE_RECEIVED_DT AS STORE_RECEIVED_DATE,
        CUSTOMER_PICKUP_TS,                                                          
        ORDER_DROP_TS,                                                               
        
        datediff(CUSTOMER_PICKUP_TS,STORE_RECEIVED_DT) AS CUSTOMER_TIME_TO_PICKUP,   
        
        datediff(CURRENT_DATE(), STORE_RECEIVED_DT) AS TOTAL_DAYS_IN_STOCKROOM_CNT,  
        
        datediff(CURRENT_DATE(), STORE_RECEIVED_DT) AS ORDER_AGE_DAYS_NBR,

        OD.EXPECTED_DT EXPECTED_DT,
        OD.DATE_TYPE_ID DATE_TYPE_ID,

        (CASE when trim(OH.STORE_ORDER_SOURCE) IS NULL or trim(OH.STORE_ORDER_SOURCE) like ('') then 'DTC' else OH.STORE_ORDER_SOURCE  end) STORE_ORDER_SOURCE,

        ITM.ITEM_KEY ITEM_KEY,
        ITM.ITEM_ID ITEM_ID,
        ITM.ITEM_NAME ITEM_NAME,
        OLS.PRODUCT_LINE PRODUCT_LINE,
        RECEIVING_NODE_CD,
        OLS.LINE_TOTAL_AMT,                     
        OLS.ADDITIONAL_LINE_TYPE_CD,            
        OLS.RETURN_ACTION_CD,                   
        OLS.ORDER_QTY,                          
        OLS.RETURN_REASON_CD,                   
        OLS.RETURN_SUB_REASON_CD,               
        OLS.RETURN_REASON_DESC,                 
        OLS.RETURN_ACTION,                      
        
        OLCHG.CHARGE_NAME,                      
        OLCHG.CHARGE_PER_LINE_AMT,              
        
        OLS.STATUS_TS,                          
        
        RCPT.DISPOSITION_CD,                    

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
  CUSTOMER_PICKUP_TS,            
  ORDER_DROP_TS,                 
  CUSTOMER_TIME_TO_PICKUP,       
  TOTAL_DAYS_IN_STOCKROOM_CNT,   
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
  LINE_TOTAL_AMT,                      
  ADDITIONAL_LINE_TYPE_CD,             
  RETURN_ACTION_CD,                    
  ORDER_QTY,                           
  RETURN_REASON_CD,                    
  RETURN_SUB_REASON_CD,                
  RETURN_REASON_DESC,                  
  RETURN_ACTION,                       
  DISPOSITION_CD,                      
  STATUS_TS,                              
  CHARGE_NAME,                         
  CHARGE_PER_LINE_AMT,                 
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

// DBTITLE 1,Bring Shipment details
val shipmentLine = spark.sql(s"select shipment_key, order_line_id from $L2_ANALYTICS.SHIPMENT_LINE where ORDER_DT >= '$FROM_DATE' ").repartition(600,col("shipment_key"))

val shipment = spark.sql(s"""select shipment_key, scac_id, scac_key from $L2_ANALYTICS.SHIPMENT
                              where ACT_SHIPMENT_DT >= '2019-12-01' and DOCUMENT_TYPE_CD = '0001' and ORDER_DT >= '$FROM_DATE'""").repartition(600,col("shipment_key"))

val joinedShipment = shipmentLine.join(shipment, Seq("shipment_key"), "inner").drop("shipment_key").repartition(300,col("ORDER_LINE_ID"))

val AGG_SHIP_DF= AGG_BOSTS_DF_INIT.join(joinedShipment, Seq("ORDER_LINE_ID"), "left")

AGG_SHIP_DF.createOrReplaceTempView("AGG_SHIP_DF")

// COMMAND ----------

// DBTITLE 1,Handle Duplicates due to Shipment
val AGG_BOSTS_DF = spark.sql("""
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
                                CUSTOMER_PICKUP_TS,           
                                ORDER_DROP_TS,                
                                CUSTOMER_TIME_TO_PICKUP,      
                                TOTAL_DAYS_IN_STOCKROOM_CNT,  
                                ORDER_AGE_DAYS_NBR,
                                STORE_ORDER_SOURCE,
                                PRODUCT_LINE,
                                RECEIVING_NODE_CD,
                                LINE_TOTAL_AMT,          
                                ADDITIONAL_LINE_TYPE_CD, 
                                RETURN_ACTION_CD,        
                                ORDER_QTY,               
                                RETURN_REASON_CD,        
                                RETURN_SUB_REASON_CD,    
                                RETURN_REASON_DESC,      
                                RETURN_ACTION,           
                                DISPOSITION_CD,          
                                STATUS_TS,               
                                CHARGE_NAME,             
                                CHARGE_PER_LINE_AMT,     
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
                                CUSTOMER_PICKUP_TS,            
                                ORDER_DROP_TS,                 
                                CUSTOMER_TIME_TO_PICKUP,       
                                TOTAL_DAYS_IN_STOCKROOM_CNT,   
                                ORDER_AGE_DAYS_NBR,
                                STORE_ORDER_SOURCE,
                                PRODUCT_LINE,
                                RECEIVING_NODE_CD,
                                LINE_TOTAL_AMT,          
                                ADDITIONAL_LINE_TYPE_CD, 
                                RETURN_ACTION_CD,        
                                ORDER_QTY,               
                                RETURN_REASON_CD,        
                                RETURN_SUB_REASON_CD,    
                                RETURN_REASON_DESC,      
                                RETURN_ACTION,           
                                DISPOSITION_CD,          
                                STATUS_TS,               
                                CHARGE_NAME,             
                                CHARGE_PER_LINE_AMT,     
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
AGG_BOSTS_DF.cache.createOrReplaceTempView("AGG_BOSTS_DF")

// COMMAND ----------

// DBTITLE 1,Validations
val dfCount = AGG_BOSTS_DF.count.toLong
if (dfCount < MIN_ORD_LINE_CNT)
   throw new Exception(s" Exception Generated!! - Order Line count is less than $MIN_ORD_LINE_CNT")

else{
  val duplicate = AGG_BOSTS_DF.groupBy("ORDER_LINE_ID").count.where("count>1").count.toLong
  if(duplicate>10) throw new Exception(s" Exception Generated!! - Duplicate Order Line")
}


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
// MAGIC   ORDER_QTY int,                           
// MAGIC   ECDD_DELIVERY_END_DT timestamp,
// MAGIC   CURRENT_PROMISE_DT timestamp,
// MAGIC   ORDER_SHIPPED_DT date,
// MAGIC   STORE_RECEIVED_DT timestamp,
// MAGIC   ORDER_AGE_DAYS_NBR int,
// MAGIC   TOTAL_DAYS_IN_STOCKROOM_CNT int,         
// MAGIC   STORE_ORDER_SOURCE string,
// MAGIC   PRODUCT_LINE string,
// MAGIC   ADDITIONAL_LINE_TYPE_CD string,           
// MAGIC   DISPOSITION_CD string,                   
// MAGIC   RECEIVING_NODE_CD string,                
// MAGIC   CHARGE_NAME string,                      
// MAGIC   CHARGE_PER_LINE_AMT DECIMAL(15,4),       
// MAGIC   LINE_TOTAL_AMT DECIMAL(15,4),            
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
// MAGIC   STATUS_TS timestamp,                     
// MAGIC   ORDER_DROP_TS TIMESTAMP,                 
// MAGIC   CUSTOMER_PICKUP_TS TIMESTAMP,            
// MAGIC   CUSTOMER_TIME_TO_PICKUP INT,             
// MAGIC   STATUS_QTY decimal(14, 4),
// MAGIC   BO_STATUS_QTY decimal(15, 4),
// MAGIC   RETURN_ACTION_CD string,                 
// MAGIC   RETURN_ACTION string,                    
// MAGIC   RETURN_REASON_CD string,                 
// MAGIC   RETURN_REASON_DESC string,               
// MAGIC   RETURN_SUB_REASON_CD string,             
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

// DBTITLE 1,OPEN_ORDERS_AGG INSERT
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
// MAGIC   ORDER_QTY,                 
// MAGIC   ECDD_DELIVERY_END_DT,
// MAGIC   --ORIGINAL_ECDD
// MAGIC   CURRENT_PROMISE_DT,
// MAGIC   ---CURRENT_ECDD,
// MAGIC   ORDER_SHIPPED_DT,
// MAGIC   STORE_RECEIVED_DT,
// MAGIC   ORDER_AGE_DAYS_NBR,
// MAGIC   TOTAL_DAYS_IN_STOCKROOM_CNT,   
// MAGIC   STORE_ORDER_SOURCE,
// MAGIC   --CHANNEL,
// MAGIC   PRODUCT_LINE,
// MAGIC   ADDITIONAL_LINE_TYPE_CD,   
// MAGIC   DISPOSITION_CD,            
// MAGIC   RECEIVING_NODE_CD,         
// MAGIC   CHARGE_NAME,               
// MAGIC   CHARGE_PER_LINE_AMT,       
// MAGIC   --SHIP_MODE,
// MAGIC   LINE_TOTAL_AMT,           
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
// MAGIC   STATUS_TS,               
// MAGIC   ORDER_DROP_TS,           
// MAGIC   CUSTOMER_PICKUP_TS,      
// MAGIC   CUSTOMER_TIME_TO_PICKUP, 
// MAGIC   --ADD
// MAGIC   STATUS_QTY,
// MAGIC   BO_STATUS_QTY,
// MAGIC   RETURN_ACTION_CD,        
// MAGIC   RETURN_ACTION,           
// MAGIC   RETURN_REASON_CD,        
// MAGIC   RETURN_REASON_DESC,      
// MAGIC   RETURN_SUB_REASON_CD,    
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
