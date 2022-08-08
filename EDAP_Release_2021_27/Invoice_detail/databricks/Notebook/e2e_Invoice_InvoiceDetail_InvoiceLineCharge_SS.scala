// Databricks notebook source
// MAGIC %sql
// MAGIC create widget text L1_INCREMENT_PATH default "/mnt/data/governed/l1/sterling/order/decouple_inbound_long_stage/";
// MAGIC create widget text L2_ANALYTICS_TABLES default "L2_ANALYTICS_TABLES";
// MAGIC create widget text L2_STAGE default "L2_STAGE";
// MAGIC create widget text L2_STAGEPATH default "";

// COMMAND ----------

// DBTITLE 1,OI: Set Variables
/////////////
// SPARK SQL VARIABLES
/////////////

val L1_ADLS_LOC =dbutils.widgets.get("L1_INCREMENT_PATH")
val L2_STAGE = dbutils.widgets.get("L2_STAGE")
val L2_ANALYTICS_TABLES = dbutils.widgets.get("L2_ANALYTICS_TABLES")



spark.conf.set("spark.sql.broadcastTimeout", 3600)


// COMMAND ----------


import com.wsgc.bigdata.config.ConfigDataObjects.{KeyVaultAuth, KeyVaultParams}
import com.wsgc.bigdata.utils.{KeyVaultUtils}

val keyVaultParams = KeyVaultParams(
    clientId = "a775e1b6-796d-49aa-b4ae-c478d7892f38",
    usernameKey =Some("STERLING-ORACLE-UserKey"),
    passwordKey ="STERLING-ORACLE-PassKey",
    clientKey ="6bMa+_SX@XuG?MM3fzxL5o7Q_KW1=*jt",
    vaultBaseUrl="https://prod-edap-key-vault.vault.azure.net"
  )
  val keyVaults = KeyVaultAuth(
    userName = None,
    password = None,
    keyVaultParams = Some(keyVaultParams)
  )
  val clientMap: Map[String, String] = KeyVaultUtils(Some(keyVaults)).clientKeyMap
  val OracleUser = clientMap("user")
  val OraclePass = clientMap("password")
  //val OracleURL = "jdbc:oracle:thin://@yanrep-scan.wsgc.com:1521/yanrep" //using loadbalancer changes on 20210806 //@10.168.33.30:1521/yanrep" //yanrep
  val OracleURL = "jdbc:oracle:thin://@yantradr-scan.wsgc.com:1521/yanprddr"



// COMMAND ----------

// DBTITLE 1,OI: Clear Previous Stage Tables
spark.sql("""drop table if exists """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OI""")
dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/dtc_mergedstage_OI/""", true)

spark.sql("""drop table if exists """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OI_oh""")
dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/dtc_mergedstage_OI_oh/""", true)

spark.sql("""drop table if exists """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OID_PreSterling_ol""")
dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/dtc_OID_PreSterling_ol/""", true)

spark.sql("""drop table if exists """ + L2_STAGE + """.sterling_inv_extract""")
dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/sterling_inv_extract/""", true)


spark.sql("""drop table if exists """ + L2_STAGE + """.sterling_inv_extract_oilc""")
dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/sterling_inv_extract_oilc/""", true)

// spark.sql("""drop table if exists """ + L2_STAGE + """.sterling_inv_detl""")
// dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/SS/sales/sterling_inv_detl/""", true)

// spark.sql("""drop table if exists """ + L2_STAGE + """.sterling_inv""")
// dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/SS/sales/sterling_inv/""", true)

// COMMAND ----------

// DBTITLE 1,OI: Create 1st Stage Table (21 Table Merge)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OI_oh(
// MAGIC     `ORDER_HEADER_ID` STRING, 
// MAGIC     `ORDER_DT` STRING, 
// MAGIC     `ORDER_ID` STRING, 
// MAGIC     `DOC_TYPE` STRING, 
// MAGIC     `wsieventTransactionTimeStamp`  STRING,
// MAGIC     `temp_wsieventName` STRING,
// MAGIC     `LOCATION_DESC` STRING,
// MAGIC      extnCustomerType STRING,
// MAGIC      `CONCEPT_CD`  STRING                              --added by hinatuan
// MAGIC ) USING DELTA 
// MAGIC LOCATION "/mnt/data/governed/l2/stage/sales/dtc_mergedstage_OI_oh/";
// MAGIC 
// MAGIC CREATE TABLE $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OI(
// MAGIC     `INVOICE_ID` STRING, 
// MAGIC     `SOURCE_INVOICE_ID` STRING, 
// MAGIC     `LOCATION_DESC` STRING, 
// MAGIC     `DAY_KEY` INT, 
// MAGIC     `INVOICE_DT` DATE, 
// MAGIC     `INVOICE_CREATE_TS` TIMESTAMP, 
// MAGIC     `ORDER_HEADER_ID` STRING, 
// MAGIC     `ORDER_ID` STRING, 
// MAGIC     `ASSOCIATE_ID` STRING, 
// MAGIC     `IS_ASSOCIATE_FLAG` STRING,
// MAGIC     `EMPLOYEE_ID` STRING, 
// MAGIC     `DTC_INVOICE_TYPE_CD` STRING,
// MAGIC     `DTC_INVOICE_FULFILLMENT_TYPE_CD` STRING,
// MAGIC     `DTC_INVOICE_REVISED_TYPE_CD` STRING,
// MAGIC     `DTC_TXN_TYPE_CD` STRING, 
// MAGIC     `DTC_STATUS_CD` STRING, 
// MAGIC     `DTC_REFERENCE_1` STRING, 
// MAGIC     `DTC_COLLECTED_AMT` DECIMAL(15,2), 
// MAGIC     `TOTAL_QTY` INT, 
// MAGIC     `TOTAL_AMT` DECIMAL(15,2), 
// MAGIC      `CREATE_USER_ID` STRING,                 /*Added by HINATUAN*/
// MAGIC      `CONCEPT_CD`  STRING,                              --added by hinatuan
// MAGIC     temp_value_systemContext_wsieventTransactionTimeStamp timestamp ,
// MAGIC     temp_wsieventName        varchar(50)
// MAGIC ) USING DELTA 
// MAGIC LOCATION "/mnt/data/governed/l2/stage/sales/dtc_mergedstage_OI/";
// MAGIC 
// MAGIC CREATE TABLE $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OID_PreSterling_ol 
// MAGIC (
// MAGIC     orderLineKey STRING, 
// MAGIC     returnReason STRING, 
// MAGIC     extnDirectShipInd STRING
// MAGIC )
// MAGIC USING DELTA
// MAGIC LOCATION "/mnt/data/governed/l2/stage/sales/dtc_OID_PreSterling_ol/";

// COMMAND ----------

import org.apache.spark.sql.functions._
import java.util.Calendar

//RETURNS SECTION - INVOICE
var insertsql_RO = spark.sql("""INSERT INTO  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OI_oh
select 
A.ORDER_HEADER_ID AS ORDER_HEADER_ID, 
substring(A.temp_value_order_orderDate ,1,10) as ORDER_DT, 
A.ORDER_ID as ORDER_ID, 
A.documentType as documentType, 
A.temp_value_systemContext_wsieventTransactionTimeStamp AS wsieventTransactionTimeStamp,
A.temp_wsieventName as temp_wsieventName, 
case when trim(A.entryType) = 'CCA' then A.CONCEPT_CD || ' Care Center' else A.CONCEPT_CD || ' Internet' end as LOCATION_ID,
A.extnCustomerType,
A.CONCEPT_CD                               --hinatuan
FROM 
(SELECT 
exploded.orderHeaderKey as ORDER_HEADER_ID 
,exploded.documentType as documentType 
,exploded.orderNo as ORDER_ID 
,exploded.orderDate as temp_value_order_orderDate 
,exploded.orderDate as temp_value_order_orderDate_ts
,event.value.systemContext.wsieventTransactionTimeStamp as temp_value_systemContext_wsieventTransactionTimeStamp 
,event.value.systemContext.wsieventName as temp_wsieventName 
, event.value.order.entryType AS entryType
,exploded.enterpriseCode as CONCEPT_CD                  
,event.value.order.extn.extnCustomerType as extnCustomerType
FROM delta.`/mnt/data/governed/l1/sterling/order/customer/message/structured_streaming/inbound/` A
LATERAL VIEW explode(event.value.order.returnOrders.returnOrder) event_table as exploded
) A
where A.ORDER_HEADER_ID is not null 

""")

var timenow = Calendar.getInstance().getTime()
// println(L1_ADLS_LOC + schemaType + ", Completed " + timenow) 


 //SALES SECTION - INVOICE
var distinctSchemaDF1 = spark.sql("""INSERT INTO  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OI_oh
 select 
event.value.order.orderHeaderKey AS ORDER_HEADER_ID, 
substring(event.value.order.orderDate ,1,10) as ORDER_DT, 
event.value.order.orderNo as ORDER_ID, 
event.value.order.documentType as documentType, 
event.value.systemContext.wsieventTransactionTimeStamp AS wsieventTransactionTimeStamp,
event.value.systemContext.wsieventName as temp_wsieventName, 
case when trim(event.value.order.entryType) = 'CCA' then event.value.order.enterpriseCode || ' Care Center' else event.value.order.enterpriseCode || ' Internet' end as LOCATION_ID,
event.value.order.extn.extnCustomerType,
event.value.order.enterpriseCode                          -- hinatuan
FROM delta.`/mnt/data/governed/l1/sterling/order/customer/message/structured_streaming/inbound/`
where event.value.order.orderHeaderKey is not null
""")
 
 
 
 timenow = Calendar.getInstance().getTime()
// println(L1_ADLS_LOC + schemaType + ", Completed " + timenow) 
 

//SALES SECTION - INVOICE DETL
 var insertsql_ol = """
INSERT INTO  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OID_PreSterling_ol

   select distinct exploded_ol.orderLineKey, exploded_ol.returnReason, exploded_ol.itemDetails.extn.extnDirectShipInd
FROM delta.`/mnt/data/governed/l1/sterling/order/customer/message/structured_streaming/inbound/`
LATERAL VIEW explode(event.value.order.orderLines.orderLine) event_table_ol as exploded_ol
where exploded_ol.orderLineKey is not null
""" ;
spark.sql(insertsql_ol);
 
 timenow = Calendar.getInstance().getTime()
// println(L1_ADLS_LOC + schemaType + ", Completed " + timenow) 
 
//SALES SECTION - INVOICE DETL
 var insertsql_ol2 = """
INSERT INTO  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OID_PreSterling_ol

   select distinct exploded_ol.orderLineKey, exploded_ol.returnReason, exploded_ol.itemDetails.extn.extnDirectShipInd
FROM delta.`/mnt/data/governed/l1/sterling/order/customer/message/structured_streaming/inbound/`
LATERAL VIEW explode(event.value.order.orderLines.orderLine) event_table_ol as exploded_ol
where exploded_ol.orderLineKey is not null
""" ;
spark.sql(insertsql_ol);
 
 timenow = Calendar.getInstance().getTime()
// println(L1_ADLS_LOC + schemaType + ", Completed " + timenow) 
 

// COMMAND ----------

// min modifyts for tables
var minTS = spark.sql("select min(regexp_replace(wsieventTransactionTimeStamp, 'T',' ') - interval '24' hour) as minTS from  " + L2_STAGE + ".STERLING_INCREMENT_STAGE_DELTA_OI_oh").as[String].first()



// COMMAND ----------

spark.sql("""drop table if exists """ + L2_STAGE + """.sterling_inv_extract""")

dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/sterling_inv_extract/""", true)

var extractInvoiceSql1="""( SELECT TRIM(OI.ORDER_INVOICE_KEY) AS ORDER_INVOICE_KEY,
                         TRIM(OI.INVOICE_NO) AS INVOICE_NO,
                         COALESCE(OI.EXTN_INV_FIN_DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD')) AS STRINVOICE_DT,
                         OI.CREATETS AS CREATETS,
                         TRIM(OI.ORDER_HEADER_KEY) AS ORDER_HEADER_ID,
                         TRIM(OI.INVOICE_TYPE) AS INVOICE_TYPE,
                         TRIM(OI.EXTN_INVOICE_TYPE) AS EXTN_INVOICE_TYPE,
                         CASE
                             WHEN UPPER(TRIM(OI.INVOICE_TYPE)) = 'ORDER'
                                  AND UPPER(TRIM(OI.EXTN_INVOICE_TYPE)) = 'DC' THEN 'SHIPMENT'
                             ELSE OI.INVOICE_TYPE
                         END AS DTC_INVOICE_REVISED_TYPE_CD,
                         TRIM(OI.STATUS) AS STATUS,
                         TRIM(OI.REFERENCE_1) AS REFERENCE_1,
                         TRIM(OI.AMOUNT_COLLECTED) AS AMOUNT_COLLECTED,
                         TRIM(OI.TOTAL_AMOUNT) AS TOTAL_AMOUNT,
                         TRIM(OI.MODIFYTS) AS MODIFYTS,
                         TRIM(OI.CREATEUSERID) AS CREATE_USER_ID,            /* COLUMN ADDED BY HINATUAN */
                         TRIM(OI.ENTERPRISE_CODE) AS CONCEPT_CD,             /* COLUMN ADDED BY HINATUAN */  
                         TRIM(OID.ORDER_LINE_KEY) AS ORDER_LINE_KEY,
                         TRIM(OID.ORDER_INVOICE_DETAIL_KEY) AS ORDER_INVOICE_DETAIL_KEY,
                         TRIM(OID.PRIME_LINE_NO) AS PRIME_LINE_NO,
                         TRIM(OID.ITEM_ID) AS ITEM_ID,
                         TRIM(OID.QUANTITY) AS QUANTITY,
                         TRIM(OID.FINAL_LINE_CHARGES) AS FINAL_LINE_CHARGES,
                         TRIM(OID.UNIT_PRICE) AS UNIT_PRICE,
                         TRIM(OID.EXTN_IS_TAXABLE) AS EXTN_IS_TAXABLE,
                         TRIM(OID.ORIGINAL_UNIT_PRICE) AS ORIGINAL_UNIT_PRICE,
                         TRIM(OID.EXTN_VENDOR_COST) AS EXTN_VENDOR_COST,
                         TRIM(OID.EXTN_PO_EXPENSES) AS EXTN_PO_EXPENSES,
                         TRIM(OID.EXTN_PO_DUTY) AS EXTN_PO_DUTY,
                         TRIM(OID.EXTN_WAC) AS EXTN_WAC,
                         TRIM(OID.EXTN_LABOR_COST) AS EXTN_LABOR_COST
                  FROM YANTRA_OWNER.YFS_ORDER_INVOICE OI
                  INNER JOIN YANTRA_OWNER.YFS_ORDER_INVOICE_DETAIL OID ON OI.ORDER_INVOICE_KEY = OID.ORDER_INVOICE_KEY
                  WHERE OI.MODIFYTS >= TO_TIMESTAMP('""" + minTS + """', 'YYYY-MM-DD HH24:MI:SS.FF3')
                         OR OID.MODIFYTS >= TO_TIMESTAMP('""" + minTS + """', 'YYYY-MM-DD HH24:MI:SS.FF3')
                         )"""
                         
var extractDfInvoice = spark.read
          .format("jdbc")
          .option("driver", "oracle.jdbc.driver.OracleDriver")
          .option("url", OracleURL)
          .option("user", OracleUser)
          .option("password", OraclePass)
          .option("query", extractInvoiceSql1)
          .option("fetchSize", 15000)
          .option("numPartitions", 72)
          .option("pushDownPredicate", true)
          .load()

extractDfInvoice.createOrReplaceTempView("extract_df_view")





// COMMAND ----------

// MAGIC %sql
// MAGIC -- materialize sterling_inv_line_charge 
// MAGIC create table l2_stage.sterling_inv_extract
// MAGIC  using delta location '/mnt/data/governed/l2/stage/sales/sterling_inv_extract/' 
// MAGIC  as
// MAGIC  select 
// MAGIC   ORDER_INVOICE_KEY          ,
// MAGIC   INVOICE_NO                 ,
// MAGIC   STRINVOICE_DT              ,
// MAGIC   CREATETS                   ,
// MAGIC   ORDER_HEADER_ID            ,
// MAGIC   INVOICE_TYPE               ,
// MAGIC   EXTN_INVOICE_TYPE          ,
// MAGIC   DTC_INVOICE_REVISED_TYPE_CD,
// MAGIC   STATUS                     ,
// MAGIC   REFERENCE_1                ,
// MAGIC   AMOUNT_COLLECTED           ,
// MAGIC   TOTAL_AMOUNT               ,
// MAGIC   MODIFYTS                   ,
// MAGIC   CREATE_USER_ID             ,                       /* COLUMN ADDED BY HINATUAN */
// MAGIC   CONCEPT_CD                 ,                        /* COLUMN ADDED BY HINATUAN */
// MAGIC   ORDER_LINE_KEY             ,
// MAGIC   ORDER_INVOICE_DETAIL_KEY   ,
// MAGIC   PRIME_LINE_NO              ,
// MAGIC   ITEM_ID                    ,
// MAGIC   QUANTITY                   ,
// MAGIC   FINAL_LINE_CHARGES         ,
// MAGIC   UNIT_PRICE                 ,
// MAGIC   EXTN_IS_TAXABLE            ,
// MAGIC   ORIGINAL_UNIT_PRICE        ,
// MAGIC   EXTN_VENDOR_COST           ,
// MAGIC   EXTN_PO_EXPENSES           ,
// MAGIC   EXTN_PO_DUTY               ,
// MAGIC   EXTN_WAC                   ,
// MAGIC   EXTN_LABOR_COST            
// MAGIC   from extract_df_view;

// COMMAND ----------

spark.sql("""drop table if exists """ + L2_STAGE + """.sterling_inv_extract_oilc""")

dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/sterling_inv_extract_oilc/""", true)

var extarctlinechargesql=""" SELECT TRIM(OILC.HEADER_KEY) AS TEMP_HEADER_KEY,
                         TRIM(OILC.LINE_KEY) AS INVOICE_DETAIL_ID,
                         TRIM(OILC.LINE_KEY) || '-'|| TRIM(OILC.CHARGE_NAME) || '-' ||  TRIM(OILC.CHARGE_CATEGORY) as INVOICE_LINE_CHARGE_ID,
                          TRIM(OILC.MODIFYTS) AS MODIFYTS,
                         TRIM(OILC.CHARGE_NAME) AS CHARGE_NAME,
                         TRIM(OILC.CHARGE_CATEGORY) AS CHARGE_CATEGORY,
                         TRIM(OILC.CHARGEAMOUNT) AS CHARGE_AMOUNT,
                         TRIM(OILC.CHARGEPERLINE) AS CHARGE_PER_LINE_AMT,
                         TRIM(OILC.CHARGEPERUNIT) AS CHARGE_PER_UNIT,
                         TRIM(OILC.REFERENCE) AS REFERENCE_CHARGE_AMT,
                         TRIM(OILC.RECORD_TYPE) AS OILC_RECORD_TYPE
                  FROM YANTRA_OWNER.YFS_LINE_CHARGES OILC 
                  WHERE OILC.RECORD_TYPE = 'INV' and OILC.MODIFYTS >= TO_TIMESTAMP('""" + minTS + """', 'YYYY-MM-DD HH24:MI:SS.FF3')"""  


var extractDfInvoiceLinecharge = spark.read
          .format("jdbc")
          .option("driver", "oracle.jdbc.driver.OracleDriver")
          .option("url", OracleURL)
          .option("user", OracleUser)
          .option("password", OraclePass)
          .option("query", extarctlinechargesql)
          .option("fetchSize", 15000)
          .option("numPartitions", 72)
          .option("pushDownPredicate", true)
          .load()




extractDfInvoiceLinecharge.createOrReplaceTempView("extract_linecharge_View")





// COMMAND ----------

// MAGIC %sql
// MAGIC create table l2_stage.sterling_inv_extract_oilc
// MAGIC  using delta location '/mnt/data/governed/l2/stage/sales/sterling_inv_extract_oilc/' 
// MAGIC  as
// MAGIC  select * from extract_linecharge_View

// COMMAND ----------

// var inv_line_charge_sql = """(SELECT TRIM(HEADER_KEY) AS TEMP_HEADER_KEY ,
//                                      TRIM(LINE_KEY) AS INVOICE_DETAIL_ID ,
//                                      TRIM(LINE_CHARGES_KEY) AS INVOICE_LINE_CHARGE_ID ,
//                                      TRIM(CHARGE_NAME) AS CHARGE_NAME ,
//                                      TRIM(CHARGE_CATEGORY) AS CHARGE_CATEGORY ,
//                                      TRIM(CHARGEAMOUNT) AS CHARGE_AMOUNT ,
//                                      TRIM(CHARGEPERLINE) AS CHARGE_PER_LINE_AMT ,
//                                      TRIM(CHARGEPERUNIT) AS CHARGE_PER_UNIT ,
//                                      TRIM(REFERENCE) AS REFERENCE_CHARGE_AMT
//                               FROM YANTRA_OWNER.YFS_LINE_CHARGES OILC
//                               WHERE OILC.MODIFYTS >= TO_TIMESTAMP('""" + minTS + """','YYYY-MM-DD HH24:MI:SS.FF3')
//                                 AND TRIM(RECORD_TYPE) = 'INV'
//                             )"""

// //println(sqlstmt)
// //Issue select against Sterling Oracle via JDBC
// var sterling_inv_line_charge_df = spark.read
//           .format("jdbc")
//           .option("driver", "oracle.jdbc.driver.OracleDriver")
//           .option("url", OracleURL)
//           .option("user", OracleUser)
//           .option("password", OraclePass)
//           .option("query", inv_line_charge_sql)
//           .option("fetchSize", 15000)
//           .option("numPartitions", 72)
//           .option("pushDownPredicate", true)
//           .load()

// sterling_inv_line_charge_df.createOrReplaceTempView("sterling_inv_line_charge")

// COMMAND ----------

// %sql
// -- materialize sterling_inv_line_charge 
// create table l2_stage.sterling_inv_line_charge
//  using delta location '/mnt/data/governed/l2/stage/SS/sales/sterling_inv_line_charge/' 
//  as
//  select 
//   TEMP_HEADER_KEY        ,
//   INVOICE_DETAIL_ID      ,
//   INVOICE_LINE_CHARGE_ID ,
//   CHARGE_NAME            ,
//   CHARGE_CATEGORY        ,
//   CHARGE_AMOUNT          ,
//   CHARGE_PER_LINE_AMT    ,
//   CHARGE_PER_UNIT        ,
//   REFERENCE_CHARGE_AMT   
//  from sterling_inv_line_charge;

// COMMAND ----------

// var sterling_inv_detl_sql = """(
//                             SELECT TRIM(OID.ORDER_INVOICE_KEY) AS ORDER_INVOICE_KEY,
//                                    TRIM(OID.MODIFYTS) AS MODIFYTS,
//                                    TRIM(OID.ORDER_LINE_KEY) AS ORDER_LINE_KEY,
//                                    TRIM(OID.ORDER_INVOICE_DETAIL_KEY) AS ORDER_INVOICE_DETAIL_KEY,
//                                    TRIM(OID.PRIME_LINE_NO) AS PRIME_LINE_NO,
//                                    TRIM(OID.ITEM_ID) AS ITEM_ID,
//                                    TRIM(OID.QUANTITY) AS QUANTITY,
//                                    TRIM(OID.FINAL_LINE_CHARGES) AS FINAL_LINE_CHARGES,
//                                    TRIM(OID.UNIT_PRICE) AS UNIT_PRICE,
//                                    TRIM(OID.EXTN_IS_TAXABLE) AS EXTN_IS_TAXABLE,
//                                    TRIM(OID.ORIGINAL_UNIT_PRICE) AS ORIGINAL_UNIT_PRICE,
//                                    TRIM(OID.EXTN_VENDOR_COST) AS EXTN_VENDOR_COST,
//                                    TRIM(OID.EXTN_PO_EXPENSES) AS EXTN_PO_EXPENSES,
//                                    TRIM(OID.EXTN_PO_DUTY) AS EXTN_PO_DUTY,
//                                    TRIM(OID.EXTN_WAC) AS EXTN_WAC,
//                                    TRIM(OID.EXTN_LABOR_COST) AS EXTN_LABOR_COST
//                             FROM YANTRA_OWNER.YFS_ORDER_INVOICE_DETAIL OID
//                             WHERE OID.MODIFYTS >= TO_TIMESTAMP('""" + minTS + """','YYYY-MM-DD HH24:MI:SS.FF3')
//                           )"""

// //println(sqlstmt_INV_DETL)
// //Issue select against Sterling Oracle via JDBC
// var sterling_inv_detl_df = spark.read
//           .format("jdbc")
//           .option("driver", "oracle.jdbc.driver.OracleDriver")
//           .option("url", OracleURL)
//           .option("user", OracleUser)
//           .option("password", OraclePass)
//           .option("dbtable", sterling_inv_detl_sql)
//           .option("fetchSize", 15000)
//           .option("numPartitions", 72)
//           .option("pushDownPredicate", true)
//           .load()

// sterling_inv_detl_df.createOrReplaceTempView("sterling_inv_detl")

// COMMAND ----------

// %sql
// -- materialize sterling_invdetl
// create table l2_stage.sterling_inv_detl
//  using delta location '/mnt/data/governed/l2/stage/SS/sales/sterling_inv_detl/' 
//  as
//  select 
//   ORDER_INVOICE_KEY,       
//   MODIFYTS,                
//   ORDER_LINE_KEY,          
//   ORDER_INVOICE_DETAIL_KEY,
//   PRIME_LINE_NO,           
//   ITEM_ID,                 
//   QUANTITY,                
//   FINAL_LINE_CHARGES,      
//   UNIT_PRICE,              
//   EXTN_IS_TAXABLE,         
//   ORIGINAL_UNIT_PRICE,     
//   EXTN_VENDOR_COST,        
//   EXTN_PO_EXPENSES,        
//   EXTN_PO_DUTY,            
//   EXTN_WAC,                
//   EXTN_LABOR_COST          
//  from sterling_inv_detl;

// COMMAND ----------

// var sterling_inv_sql = """( SELECT TRIM(OI.ORDER_INVOICE_KEY) AS ORDER_INVOICE_KEY,
//                                    TRIM(OI.INVOICE_NO) AS INVOICE_NO,
//                                    COALESCE(OI.EXTN_INV_FIN_DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD')) AS STRINVOICE_DT,
//                                    OI.CREATETS AS CREATETS,
//                                    TRIM(OI.ORDER_HEADER_KEY) AS ORDER_HEADER_ID,
//                                    TRIM(OI.INVOICE_TYPE) AS INVOICE_TYPE,
//                                    TRIM(OI.EXTN_INVOICE_TYPE) AS EXTN_INVOICE_TYPE,
//                                    CASE
//                                        WHEN TO_UPPER(TRIM(OI.INVOICE_TYPE)) = 'ORDER'
//                                             AND TO_UPPER(TRIM(OI.EXTN_INVOICE_TYPE)) = 'DC' THEN 'SHIPMENT'
//                                        ELSE OI.INVOICE_TYPE
//                                    END AS DTC_INVOICE_REVISED_TYPE_CD,
//                                    TRIM(OI.STATUS) AS STATUS,
//                                    TRIM(OI.REFERENCE_1) AS REFERENCE_1,
//                                    TRIM(OI.AMOUNT_COLLECTED) AS AMOUNT_COLLECTED,
//                                    TRIM(OI.TOTAL_AMOUNT) AS TOTAL_AMOUNT,
//                                    TRIM(OI.MODIFYTS) AS MODIFYTS
//                             FROM YANTRA_OWNER.YFS_ORDER_INVOICE OI
//                             WHERE OI.MODIFYTS >= TO_TIMESTAMP('""" + minTS + """', 'YYYY-MM-DD HH24:MI:SS.FF3')
//                           )"""

// //println(sqlstmt_INV)
// //Issue select against Sterling Oracle via JDBC
// var sterling_inv_df = spark.read
//           .format("jdbc")
//           .option("driver", "oracle.jdbc.driver.OracleDriver")
//           .option("url", OracleURL)
//           .option("user", OracleUser)
//           .option("password", OraclePass)
//           .option("dbtable", sterling_inv_sql)
//           .option("fetchSize", 15000)
//           .option("numPartitions", 72)
//           .option("pushDownPredicate", true)
//           .load()

// sterling_inv_df.createOrReplaceTempView("sterling_inv")

// COMMAND ----------

// %sql
// -- materialize sterling_inv
// create table l2_stage.sterling_inv
//  using delta location '/mnt/data/governed/l2/stage/SS/sales/sterling_inv/' 
//  as
//  select
//   ORDER_INVOICE_KEY          ,
//   INVOICE_NO                 ,
//   STRINVOICE_DT              ,
//   CREATETS                   ,
//   ORDER_HEADER_ID            ,
//   INVOICE_TYPE               ,
//   EXTN_INVOICE_TYPE          ,
//   DTC_INVOICE_REVISED_TYPE_CD,
//   STATUS                     ,
//   REFERENCE_1                ,
//   AMOUNT_COLLECTED           ,
//   TOTAL_AMOUNT               ,
//   MODIFYTS                   
//  from sterling_inv

// COMMAND ----------

var sterling_lkp_view_sql = """
                            CREATE OR REPLACE TEMPORARY VIEW STERLING_LKP AS                    
                               SELECT OI.ORDER_INVOICE_KEY AS ORDER_INVOICE_KEY                ,
                                       OI.INVOICE_NO AS INVOICE_NO                              ,
                                       OI.STRINVOICE_DT AS STRINVOICE_DT                        ,
                                       OI.CREATETS AS CREATETS                                  ,
                                       OI.ORDER_HEADER_ID AS ORDER_HEADER_ID                   ,
                                       OI.INVOICE_TYPE AS INVOICE_TYPE                          ,
                                       OI.EXTN_INVOICE_TYPE AS EXTN_INVOICE_TYPE                ,
                                       OI.DTC_INVOICE_REVISED_TYPE_CD                           ,
                                       OI.STATUS AS STATUS                                      ,
                                       OI.REFERENCE_1 AS REFERENCE_1                            ,
                                       OI.AMOUNT_COLLECTED AS AMOUNT_COLLECTED                  ,
                                       OI.TOTAL_AMOUNT AS TOTAL_AMOUNT                          ,
                                       OI.MODIFYTS AS MODIFYTS                                  ,
                                       OI.CREATE_USER_ID  AS CREATE_USER_ID                     ,         /* COLUMN ADDED BY HINATUAN */
                                       OI.CONCEPT_CD  AS CONCEPT_CD                             ,         /* COLUMN ADDED BY HINATUAN */
                                       OI.ORDER_LINE_KEY AS ORDER_LINE_KEY                     ,
                                       OI.ORDER_INVOICE_DETAIL_KEY AS ORDER_INVOICE_DETAIL_KEY ,
                                       OI.PRIME_LINE_NO AS PRIME_LINE_NO                       ,
                                       OI.ITEM_ID AS ITEM_ID                                   ,
                                       OI.QUANTITY AS QUANTITY                                 ,
                                       OI.FINAL_LINE_CHARGES AS FINAL_LINE_CHARGES             ,
                                       OI.UNIT_PRICE AS UNIT_PRICE                             ,
                                       OI.EXTN_IS_TAXABLE AS EXTN_IS_TAXABLE                   ,
                                       OI.ORIGINAL_UNIT_PRICE AS ORIGINAL_UNIT_PRICE           ,
                                       OI.EXTN_VENDOR_COST AS EXTN_VENDOR_COST                 ,
                                       OI.EXTN_PO_EXPENSES AS EXTN_PO_EXPENSES                 ,
                                       OI.EXTN_PO_DUTY AS EXTN_PO_DUTY                         ,
                                       OI.EXTN_WAC AS EXTN_WAC                                 ,
                                       OI.EXTN_LABOR_COST AS EXTN_LABOR_COST
                               FROM L2_STAGE.STERLING_INV_EXTRACT OI
                              
                          """

spark.sql(sterling_lkp_view_sql)

// COMMAND ----------

// DBTITLE 1,JDBC JOINING WITH KAFKA(EXPLODED RECORDS)
var insertsql = """
INSERT INTO 
  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OI
select 
    TRIM(str.ORDER_INVOICE_KEY) as `INVOICE_ID` , 
    str.INVOICE_NO as `SOURCE_INVOICE_ID` , 
    oh.LOCATION_DESC as `LOCATION_DESC` , 
    null as `DAY_KEY` , 
    str.STRINVOICE_DT as `INVOICE_DT` , 
    str.CREATETS as `INVOICE_CREATE_TS` , 
    TRIM(oh.ORDER_HEADER_ID) as `ORDER_HEADER_ID` , 
    TRIM(oh.ORDER_ID) as `ORDER_ID` , 
    null as `ASSOCIATE_ID` , 
    null as `IS_ASSOCIATE_FLAG` ,
    null as `EMPLOYEE_ID` , 
    str.INVOICE_TYPE as `DTC_INVOICE_TYPE_CD` , 
    str.EXTN_INVOICE_TYPE as `DTC_INVOICE_FULFILLMENT_TYPE_CD`,
    str.DTC_INVOICE_REVISED_TYPE_CD as `DTC_INVOICE_REVISED_TYPE_CD`,
    TXN.TXN_TYPE_CD as `DTC_TXN_TYPE_CD` , 
    str.STATUS as `DTC_STATUS_CD` , 
    str.REFERENCE_1 as `DTC_REFERENCE_1` , 
    str.AMOUNT_COLLECTED as `DTC_COLLECTED_AMT` , 
    sum(str.quantity)  as `TOTAL_QTY` , 
    str.TOTAL_AMOUNT as `TOTAL_AMT` , 
    str.CREATE_USER_ID as `CREATE_USER_ID`,       --ADDED BY HINATUAN
    case when oh.CONCEPT_CD = '' OR oh.CONCEPT_CD IS NULL  then str.CONCEPT_CD else oh.CONCEPT_CD end as CONCEPT_CD,    --ADDED BY HINATUAN
    oh.wsieventTransactionTimeStamp as temp_value_systemContext_wsieventTransactionTimeStamp ,
    oh.temp_wsieventName        
from """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OI_oh oh
INNER JOIN sterling_lkp str
on str.ORDER_HEADER_ID = oh.ORDER_HEADER_ID
LEFT JOIN L2_ANALYTICS_TABLES.TXN_TYPE TXN
ON TXN.DTC_DOCUMENT_ID = oh.DOC_TYPE 
group by
    str.ORDER_INVOICE_KEY, 
    str.INVOICE_NO , 
    oh.LOCATION_DESC, 
    str.strINVOICE_DT ,
    substring(str.strINVOICE_DT ,1,10) , 
    str.CREATETS  , 
    oh.ORDER_HEADER_ID , 
    oh.ORDER_ID, 
    str.INVOICE_TYPE , 
    str.EXTN_INVOICE_TYPE ,
    str.DTC_INVOICE_REVISED_TYPE_CD ,
    TXN.TXN_TYPE_CD  , 
    str.STATUS , 
    str.REFERENCE_1 , 
    str.AMOUNT_COLLECTED , 
    str.TOTAL_AMOUNT , 
    str.CREATE_USER_ID ,        --ADDED BY HINATUAN
    oh.CONCEPT_CD,         --ADDED BY HINATUAN
    str.CONCEPT_CD,  --ADDED BY HINATUAN
    oh.wsieventTransactionTimeStamp ,
    oh.temp_wsieventName        
""" ;
spark.sql(insertsql);

// COMMAND ----------

// DBTITLE 1,OI: Load 1st Stage Table (21 Table Merge)
//deactivated until we need to to JDBC workaround
//import org.apache.spark.sql.functions._
//import java.util.Calendar
//
//
//
//
//
//
//val schemaType1 = Array( "DSPOCREATE","DSPOMODIFY","PURCHASEORDERTRACKING","RETURNCLOSERECEIPT", "RETURNORDERCREATE", "RETURNORDERRECEIVE","SALESORDERACKNOWLEDGE", "SALESORDERDROP", "SALESORDERMODIFY", "SALESORDERSCHEDULE","SALESORDERSHIPCREATECONFIRM", "SALESORDERSHIPCONFIRM", "SALESORDERTRACKING", "WORKORDERCREATE", "WORKORDERDELIVER", "WORKORDERMODIFY") //for SO's
//
//
//
//for(schemaType <- schemaType1){
// 
//  
// var distinctSchemaDF1 = spark.sql("""select value.systemContext.wsieventTransactionTimeStamp, substring(value.order.orderDate ,1,10) as ORDER_DT, value.order.orderNo, value.order.documentType, exploded.invoiceCollectionDetails as col ,exploded.status, value.order.orderHeaderKey, value.systemContext.wsieventName as temp_wsieventName, case when trim(value.order.entryType) = 'CCA' then value.order.enterpriseCode || ' Care Center' else value.order.enterpriseCode || ' Internet' end as LOCATION_ID
//FROM delta.`""" + L1_ADLS_LOC + schemaType + """/` 
//LATERAL VIEW explode(value.order.chargeTransactionDetails.chargeTransactionDetail) event_table as exploded
//""")
//
//distinctSchemaDF1.createOrReplaceTempView("COMBINEDSCHEMA_INV_lvl1")
//  
//
//
//  
//
//    
//var insertsql = """
//INSERT INTO 
//  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OI
//select 
//    exploded.orderInvoiceKey as `INVOICE_ID` , 
//    exploded.invoiceNo as `SOURCE_INVOICE_ID` , 
//    LOCATION_ID as `LOCATION_ID` , 
//    null as `DAY_KEY` , 
//    substring(exploded.dateInvoiced ,1,10) as `INVOICE_DT` , 
//    regexp_replace(exploded.dateInvoiced, 'T',' ') as `INVOICE_CREATE_TS` , 
//    orderHeaderKey as `ORDER_HEADER_KEY` , 
//    orderNo as `ORDER_ID` , 
//    null as `ASSOCIATE_ID` , 
//    null as `IS_ASSOCIATE_FLAG` ,
//    null as `EMPLOYEE_ID` , 
//    exploded.invoiceType as `DTC_INVOICE_TYPE_CD` , 
//    TXN.TXN_TYPE_CD as `DTC_TXN_TYPE_CD` , 
//    status as `DTC_STATUS_CD` , 
//    null as `DTC_REFERENCE_1` , 
//    exploded.amountCollected as `DTC_COLLECTED_AMT` , 
//    null as `TOTAL_QTY` , 
//    exploded.totalAmount as `TOTAL_AMT` , 
//    wsieventTransactionTimeStamp as temp_value_systemContext_wsieventTransactionTimeStamp ,
//    temp_wsieventName        
//from COMBINEDSCHEMA_INV_lvl1
//LEFT JOIN """ + L2_ANALYTICS_TABLES + """.TXN_TYPE TXN
//ON TXN.DTC_DOCUMENT_ID = CAST(documentType AS int) 
//LATERAL VIEW explode(col.invoiceCollectionDetail) event_table as exploded
//""" ;
//spark.sql(insertsql);
//  
//  var timenow = Calendar.getInstance().getTime()
// println(L1_ADLS_LOC + schemaType + ", Completed " + timenow) 
//}
//


// COMMAND ----------

// MAGIC %sql
// MAGIC optimize $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OI
// MAGIC --2mins

// COMMAND ----------

// DBTITLE 1,OI Audit: Show Counts for First Stage Table 
// MAGIC %sql
// MAGIC select count(*) from $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OI 

// COMMAND ----------

// DBTITLE 1,OI: Calculate DTC Collected Amount
//DEACTIVATED until sterling provides invoice detail in messages
//%sql
//CREATE OR REPLACE TEMPORARY VIEW AGG_INVCCOLLECTED AS
//( 
//SELECT INVOICE_ID ,
//    SOURCE_INVOICE_ID, 
//    LOCATION_ID,
//    DAY_KEY,
//    INVOICE_DT,
//    INVOICE_CREATE_TS,
//    ORDER_HEADER_KEY,
//    ORDER_ID,
//    ASSOCIATE_ID,
//    IS_ASSOCIATE_FLAG,
//    EMPLOYEE_ID,
//    DTC_INVOICE_TYPE_CD,
//    DTC_TXN_TYPE_CD,
//    DTC_STATUS_CD,
//    DTC_REFERENCE_1, 
//    SUM(DTC_COLLECTED_AMT) AS DTC_COLLECTED_AMT, 
//    TOTAL_QTY,
//    TOTAL_AMT,
//    temp_value_systemContext_wsieventTransactionTimeStamp,
//    temp_wsieventName
//FROM (select distinct * from $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OI) SUBQ
//
//GROUP BY     INVOICE_ID,    SOURCE_INVOICE_ID,     LOCATION_ID,    DAY_KEY,    INVOICE_DT,    INVOICE_CREATE_TS,    ORDER_HEADER_KEY,    ORDER_ID,    ASSOCIATE_ID,    IS_ASSOCIATE_FLAG,    EMPLOYEE_ID,    DTC_INVOICE_TYPE_CD,    DTC_TXN_TYPE_CD,    DTC_STATUS_CD,    DTC_REFERENCE_1,     TOTAL_QTY,    TOTAL_AMT,    temp_value_systemContext_wsieventTransactionTimeStamp,    temp_wsieventName
//);

// COMMAND ----------

// DBTITLE 1,OI: Keep only 1 Composite Primary Key Within the Same Batch
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW PREFLATTEN_OI AS
// MAGIC ( 
// MAGIC SELECT *, 
// MAGIC   /*PATTERN: feed columns here EXCEPT event type and event timestamp*/
// MAGIC   coalesce(INVOICE_ID,'temp_flat')
// MAGIC   || coalesce(SOURCE_INVOICE_ID,'temp_flat')
// MAGIC   || coalesce(LOCATION_DESC,'temp_flat')
// MAGIC   || coalesce(DAY_KEY,'temp_flat')
// MAGIC   || coalesce(INVOICE_DT,'temp_flat')
// MAGIC   || coalesce(INVOICE_CREATE_TS,'temp_flat')
// MAGIC   --|| coalesce(ORDER_HEADER_KEY,'temp_flat')
// MAGIC   || coalesce(ORDER_ID,'temp_flat')
// MAGIC   || coalesce(ASSOCIATE_ID,'temp_flat')
// MAGIC   || coalesce(IS_ASSOCIATE_FLAG,'temp_flat')
// MAGIC   || coalesce(EMPLOYEE_ID,'temp_flat')
// MAGIC   || coalesce(DTC_INVOICE_TYPE_CD,'temp_flat')
// MAGIC   || coalesce(DTC_TXN_TYPE_CD,'temp_flat')
// MAGIC   || coalesce(DTC_STATUS_CD,'temp_flat')
// MAGIC   || coalesce(DTC_REFERENCE_1,'temp_flat')
// MAGIC   || coalesce(DTC_COLLECTED_AMT,'temp_flat')
// MAGIC   || coalesce(TOTAL_QTY,'temp_flat')
// MAGIC   || coalesce(TOTAL_AMT,'temp_flat')
// MAGIC   || coalesce(CREATE_USER_ID,'temp_flat')                              -- HINATUAN
// MAGIC   || coalesce(CONCEPT_CD,'temp_flat')                              -- HINATUAN
// MAGIC   || coalesce(DTC_INVOICE_FULFILLMENT_TYPE_CD,'temp_flat')
// MAGIC   || coalesce(DTC_INVOICE_REVISED_TYPE_CD,'temp_flat')
// MAGIC   as row_summary
// MAGIC   /*END PATTERN*/
// MAGIC FROM (select distinct * from $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OI) SUBQ
// MAGIC );
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FLAT_OI AS 
// MAGIC ( 
// MAGIC select DISTINCT * from (
// MAGIC   select *, CASE WHEN row_summary = LAG(row_summary, 1) OVER (PARTITION BY INVOICE_ID ORDER BY  temp_value_systemContext_wsieventTransactionTimeStamp) 
// MAGIC   THEN 0 else 1 end as change_flag from PREFLATTEN_OI
// MAGIC ) where change_flag > 0
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OI: Perform All Look-ups, Generate OILCK if known OID, Generate FIRST_EFFECTIVE_TS
// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW STAGE_OI AS 
// MAGIC ( 
// MAGIC SELECT  
// MAGIC COALESCE(TARGET.INVOICE_KEY, -1) AS INVOICE_KEY
// MAGIC ,OI.INVOICE_ID
// MAGIC ,'STERLING_DTC' as SOURCE_SYSTEM
// MAGIC ,OI.SOURCE_INVOICE_ID
// MAGIC ,loc.LOCATION_KEY
// MAGIC ,loc.LOCATION_ID
// MAGIC ,translate(OI.INVOICE_DT,"-","") as DAY_KEY
// MAGIC ,OI.INVOICE_DT
// MAGIC ,OI.INVOICE_CREATE_TS
// MAGIC ,coalesce(oh.ORDER_HEADER_KEY,-1) as  ORDER_HEADER_KEY
// MAGIC ,OI.ORDER_HEADER_ID as order_header_id --added for EDA-27
// MAGIC ,OI.ORDER_ID
// MAGIC ,temp_value_systemContext_wsieventTransactionTimeStamp as FIRST_EFFECTIVE_TS 
// MAGIC ,null as LAST_EFFECTIVE_TS           
// MAGIC ,null as POS_WORKSTATION_ID
// MAGIC ,null as POS_WORKSTATION_SEQ_NBR
// MAGIC ,null as NOSALE_OVERRIDE_DT
// MAGIC ,null as OVERRIDE_REASON_ID
// MAGIC ,null as CASHIER_ID
// MAGIC ,null as ASSOCIATE_ID
// MAGIC ,null as IS_ASSOCIATE_FLAG
// MAGIC ,null as EMPLOYEE_ID
// MAGIC ,OI.DTC_INVOICE_TYPE_CD
// MAGIC ,OI.DTC_INVOICE_FULFILLMENT_TYPE_CD
// MAGIC ,OI.DTC_INVOICE_REVISED_TYPE_CD
// MAGIC ,OI.DTC_TXN_TYPE_CD
// MAGIC ,OI.DTC_STATUS_CD
// MAGIC ,OI.DTC_REFERENCE_1
// MAGIC ,OI.DTC_COLLECTED_AMT
// MAGIC ,OI.TOTAL_QTY
// MAGIC ,OI.TOTAL_AMT
// MAGIC ,OI.CREATE_USER_ID      --ADDED BY HINATUAN
// MAGIC ,OI.CONCEPT_CD           --ADDED BY HINATUAN
// MAGIC ,null as INSERT_TS  
// MAGIC ,null as UPDATE_TS 
// MAGIC 
// MAGIC FROM FLAT_OI OI
// MAGIC 
// MAGIC LEFT JOIN $L2_ANALYTICS_TABLES.ORDER_HEADER oh 
// MAGIC ON trim(OI.ORDER_HEADER_ID) = trim(oh.ORDER_HEADER_ID)
// MAGIC and oh.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC and oh.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC 
// MAGIC LEFT JOIN L2_ANALYTICS.LOCATION loc
// MAGIC ON oi.LOCATION_DESC = loc.LOCATION_DESC
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC /*PATTERN: left join to target table here*/
// MAGIC LEFT OUTER JOIN 
// MAGIC     $L2_ANALYTICS_TABLES.INVOICE TARGET
// MAGIC     ON TARGET.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC     AND trim(TARGET.INVOICE_ID) = trim(OI.INVOICE_ID)
// MAGIC     --AND TARGET.INVOICE_DT = OI.INVOICE_DT --invoice date can change from 1900 to inv fin date
// MAGIC     AND TARGET.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC 	/*END PATTERN*/
// MAGIC     
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OI: Keep only 1 Composite Primary Key across batches
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW PREFLATTEN_MULTIBATCH_OI AS ( 
// MAGIC SELECT *, 
// MAGIC  --coalesce(INVOICE_KEY, 'temp_flat')||
// MAGIC  coalesce(INVOICE_ID, 'temp_flat')
// MAGIC || coalesce(SOURCE_SYSTEM, 'temp_flat')
// MAGIC || coalesce(SOURCE_INVOICE_ID, 'temp_flat')
// MAGIC --|| coalesce(LOCATION_KEY, 'temp_flat')
// MAGIC || coalesce(LOCATION_ID, 'temp_flat')
// MAGIC || coalesce(DAY_KEY, 'temp_flat')
// MAGIC || coalesce(INVOICE_DT, 'temp_flat')
// MAGIC || coalesce(INVOICE_CREATE_TS, 'temp_flat')
// MAGIC --|| coalesce(ORDER_HEADER_KEY, 'temp_flat')
// MAGIC || coalesce(ORDER_ID, 'temp_flat')
// MAGIC || coalesce(POS_WORKSTATION_ID, 'temp_flat')
// MAGIC || coalesce(POS_WORKSTATION_SEQ_NBR, 'temp_flat')
// MAGIC || coalesce(NOSALE_OVERRIDE_DT, 'temp_flat')
// MAGIC || coalesce(OVERRIDE_REASON_ID, 'temp_flat')
// MAGIC || coalesce(CASHIER_ID, 'temp_flat')
// MAGIC || coalesce(ASSOCIATE_ID, 'temp_flat')
// MAGIC || coalesce(IS_ASSOCIATE_FLAG, 'temp_flat')
// MAGIC || coalesce(EMPLOYEE_ID, 'temp_flat')
// MAGIC || coalesce(DTC_INVOICE_TYPE_CD, 'temp_flat')
// MAGIC || coalesce(DTC_INVOICE_FULFILLMENT_TYPE_CD, 'temp_flat')
// MAGIC || coalesce(DTC_INVOICE_REVISED_TYPE_CD, 'temp_flat')
// MAGIC || coalesce(DTC_TXN_TYPE_CD, 'temp_flat')
// MAGIC || coalesce(DTC_STATUS_CD, 'temp_flat')
// MAGIC || coalesce(DTC_REFERENCE_1, 'temp_flat')
// MAGIC --|| coalesce(DTC_COLLECTED_AMT, 'temp_flat')
// MAGIC as  row_summary 
// MAGIC FROM ( 
// MAGIC select 
// MAGIC tgt.INVOICE_KEY, 
// MAGIC tgt.INVOICE_ID, 
// MAGIC tgt.SOURCE_SYSTEM, 
// MAGIC tgt.SOURCE_INVOICE_ID, 
// MAGIC tgt.LOCATION_KEY, 
// MAGIC tgt.LOCATION_ID, 
// MAGIC tgt.DAY_KEY, 
// MAGIC tgt.INVOICE_DT, 
// MAGIC tgt.INVOICE_CREATE_TS, 
// MAGIC tgt.ORDER_HEADER_KEY, 
// MAGIC tgt.ORDER_HEADER_ID, 
// MAGIC tgt.ORDER_ID, 
// MAGIC tgt.FIRST_EFFECTIVE_TS, 
// MAGIC tgt.LAST_EFFECTIVE_TS, 
// MAGIC tgt.POS_WORKSTATION_ID, 
// MAGIC tgt.POS_WORKSTATION_SEQ_NBR, 
// MAGIC tgt.NOSALE_OVERRIDE_DT, 
// MAGIC tgt.OVERRIDE_REASON_ID, 
// MAGIC tgt.CASHIER_ID, 
// MAGIC tgt.ASSOCIATE_ID, 
// MAGIC tgt.IS_ASSOCIATE_FLAG, 
// MAGIC tgt.EMPLOYEE_ID, 
// MAGIC tgt.DTC_INVOICE_TYPE_CD, 
// MAGIC tgt.DTC_INVOICE_FULFILLMENT_TYPE_CD, 
// MAGIC tgt.DTC_INVOICE_REVISED_TYPE_CD, 
// MAGIC tgt.DTC_TXN_TYPE_CD, 
// MAGIC tgt.DTC_STATUS_CD, 
// MAGIC tgt.DTC_REFERENCE_1, 
// MAGIC tgt.DTC_COLLECTED_AMT, 
// MAGIC tgt.TOTAL_QTY, 
// MAGIC tgt.TOTAL_AMT, 
// MAGIC tgt.CREATE_USER_ID,             --HINATUAN
// MAGIC tgt.CONCEPT_CD,                --HINATUAN
// MAGIC tgt.INSERT_TS, 
// MAGIC tgt.UPDATE_TS, 
// MAGIC  2 as priority from $L2_ANALYTICS_TABLES.INVOICE tgt inner join STAGE_OI ohstg on 
// MAGIC tgt.source_system = 'STERLING_DTC'  and tgt.INVOICE_ID = ohstg.INVOICE_ID and tgt.last_effective_ts = to_timestamp('9999-12-31T23:59:59')
// MAGIC union all 
// MAGIC select distinct *, 1 as priority from STAGE_OI stg 
// MAGIC ) stgtgt );
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FLATTEN_MULTIBATCH_OI AS 
// MAGIC (
// MAGIC select *
// MAGIC , LEAD(FIRST_EFFECTIVE_TS) OVER (PARTITION BY INVOICE_ID ORDER BY  FIRST_EFFECTIVE_TS)  as new_LAST_EFFECTIVE_TS
// MAGIC ,coalesce(INSERT_TS, current_timestamp) as  new_INSERT_TS --retain timestamp if already previously inserted, otherwise generate value
// MAGIC ,current_timestamp as new_UPDATE_TS --always generate value upon insert
// MAGIC from (
// MAGIC   select *, CASE WHEN row_summary = LAG(row_summary, 1) OVER (PARTITION BY INVOICE_ID ORDER BY  FIRST_EFFECTIVE_TS, priority  ) 
// MAGIC   THEN 0 else 1 end as change_flag from PREFLATTEN_MULTIBATCH_OI
// MAGIC ) where change_flag > 0
// MAGIC );

// COMMAND ----------

// DBTITLE 1,OI: Clear 2nd Stage Table (Pre-insert)
spark.sql("""drop table if exists """ + L2_STAGE + """.invoice_stage""")

dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/dtc_preL2_OI/""", true)

// COMMAND ----------

// DBTITLE 1,OI: Create 2nd Stage Table (21 Table Merge)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE $L2_STAGE.invoice_stage (
// MAGIC `INVOICE_KEY` BIGINT, 
// MAGIC `INVOICE_ID` STRING, 
// MAGIC `SOURCE_SYSTEM` STRING, 
// MAGIC `SOURCE_INVOICE_ID` STRING, 
// MAGIC `SUB_CHANNEL_CD` STRING, 
// MAGIC `CONCEPT_CD` STRING, 
// MAGIC `LOCATION_KEY` INT, 
// MAGIC `LOCATION_ID` STRING, 
// MAGIC `DAY_KEY` INT, 
// MAGIC `INVOICE_DT` DATE, 
// MAGIC `INVOICE_CREATE_TS` TIMESTAMP, 
// MAGIC `ORDER_HEADER_KEY` INT, 
// MAGIC `ORDER_HEADER_ID` STRING, 
// MAGIC `ORDER_ID` STRING, 
// MAGIC `GLOBAL_TXN_ID` STRING, 
// MAGIC `FIRST_EFFECTIVE_TS` TIMESTAMP, 
// MAGIC `LAST_EFFECTIVE_TS` TIMESTAMP, 
// MAGIC `POS_WORKSTATION_ID` STRING, 
// MAGIC `POS_WORKSTATION_SEQ_NBR` INT, 
// MAGIC `WSSPL_PURCHASE_ORDER_ID` STRING, 
// MAGIC `NOSALE_OVERRIDE_DT` DATE, 
// MAGIC `OVERRIDE_REASON_ID` STRING, 
// MAGIC `CASHIER_ID` STRING, 
// MAGIC `ASSOCIATE_ID` STRING, 
// MAGIC `IS_ASSOCIATE_FLAG` STRING, 
// MAGIC `EMPLOYEE_ID` STRING, 
// MAGIC `REPLACEMENT_ORDER_FLAG` STRING, 
// MAGIC `INTERNAL_FLAG` STRING, 
// MAGIC `DTC_INVOICE_TYPE_CD` STRING, 
// MAGIC `DTC_INVOICE_FULFILLMENT_TYPE_CD` STRING, 
// MAGIC `DTC_INVOICE_REVISED_TYPE_CD` STRING, 
// MAGIC `DTC_TXN_TYPE_CD` STRING, 
// MAGIC `DTC_STATUS_CD` STRING, 
// MAGIC `DTC_REFERENCE_1` STRING, 
// MAGIC `DTC_COLLECTED_AMT` DECIMAL(15,2), 
// MAGIC `TOTAL_QTY` INT, 
// MAGIC `TOTAL_AMT` DECIMAL(15,2),
// MAGIC `CREATE_USER_ID` STRING,                          --ADDED BY HINATUAN
// MAGIC `CURRENCY_CD` STRING, 
// MAGIC `INSERT_TS` TIMESTAMP, 
// MAGIC `UPDATE_TS` TIMESTAMP)
// MAGIC USING DELTA
// MAGIC PARTITIONED BY (INVOICE_DT)
// MAGIC LOCATION "/mnt/data/governed/l2/stage/sales/dtc_preL2_OI/";

// COMMAND ----------

// DBTITLE 1,OI: Generate OIK if new record
// MAGIC %sql
// MAGIC INSERT INTO TABLE $L2_STAGE.INVOICE_stage 
// MAGIC select 
// MAGIC  CASE WHEN STG1.INVOICE_KEY = -1 THEN
// MAGIC dense_rank() OVER (ORDER BY STG1.INVOICE_KEY, STG1.INVOICE_ID) + MAX_INVOICE_KEY ELSE
// MAGIC STG1.INVOICE_KEY END AS INVOICE_KEY     
// MAGIC ,INVOICE_ID
// MAGIC ,SOURCE_SYSTEM
// MAGIC ,SOURCE_INVOICE_ID
// MAGIC ,null
// MAGIC ,CONCEPT_CD
// MAGIC ,LOCATION_KEY
// MAGIC ,LOCATION_ID
// MAGIC ,DAY_KEY
// MAGIC ,INVOICE_DT
// MAGIC ,INVOICE_CREATE_TS
// MAGIC ,ORDER_HEADER_KEY
// MAGIC ,ORDER_HEADER_ID     --added for EDA-27
// MAGIC ,ORDER_ID
// MAGIC ,null
// MAGIC ,FIRST_EFFECTIVE_TS
// MAGIC , case when new_LAST_EFFECTIVE_TS is null then to_timestamp('9999-12-31T23:59:59')
// MAGIC   else new_LAST_EFFECTIVE_TS - INTERVAL 1 MILLISECONDS 
// MAGIC   end as LAST_EFFECTIVE_TS
// MAGIC ,POS_WORKSTATION_ID
// MAGIC ,POS_WORKSTATION_SEQ_NBR
// MAGIC ,null
// MAGIC ,NOSALE_OVERRIDE_DT
// MAGIC ,OVERRIDE_REASON_ID
// MAGIC ,CASHIER_ID
// MAGIC ,ASSOCIATE_ID
// MAGIC ,IS_ASSOCIATE_FLAG
// MAGIC ,EMPLOYEE_ID
// MAGIC ,null
// MAGIC ,null
// MAGIC ,DTC_INVOICE_TYPE_CD
// MAGIC ,DTC_INVOICE_FULFILLMENT_TYPE_CD
// MAGIC ,DTC_INVOICE_REVISED_TYPE_CD
// MAGIC ,DTC_TXN_TYPE_CD
// MAGIC ,DTC_STATUS_CD
// MAGIC ,DTC_REFERENCE_1
// MAGIC ,sum(DTC_COLLECTED_AMT) over(partition by INVOICE_ID order by FIRST_EFFECTIVE_TS)
// MAGIC ,TOTAL_QTY
// MAGIC ,TOTAL_AMT
// MAGIC ,CREATE_USER_ID                   --ADDED BY HINATUAN
// MAGIC ,null
// MAGIC ,new_INSERT_TS as INSERT_TS
// MAGIC ,new_UPDATE_TS as UPDATE_TS 
// MAGIC from FLATTEN_MULTIBATCH_OI STG1
// MAGIC CROSS JOIN (
// MAGIC     SELECT COALESCE(MAX(INVOICE_KEY),0) AS MAX_INVOICE_KEY FROM $L2_ANALYTICS_TABLES.INVOICE WHERE INVOICE_KEY <> -1
// MAGIC     ) TARGET_MAX

// COMMAND ----------

// DBTITLE 1,OI: Ensure unique composite primary key + first effective ts
import org.apache.spark.sql.functions._
var df_stg = table(L2_STAGE + """.INVOICE_stage""")
var df_uniq = df_stg.orderBy(col("LAST_EFFECTIVE_TS").desc).coalesce(1).dropDuplicates("first_effective_ts", "INVOICE_ID")
df_uniq.createOrReplaceTempView("INVOICE_stage_dedup")

// COMMAND ----------

// DBTITLE 1,OI Audit: check row counts of final stage
// MAGIC %sql
// MAGIC select count(*) from $L2_STAGE.INVOICE_stage

// COMMAND ----------

// DBTITLE 1,OI Audit: check row counts of target before merge
// MAGIC %sql
// MAGIC select count(*) from $L2_ANALYTICS_TABLES.INVOICE

// COMMAND ----------

// DBTITLE 1,OI: writing stage data to physical storage for snowflake consumption
dbutils.fs.rm("/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_OI_stg/", true)
// writing stage data to physical storage for snowflake consumption
df_uniq.write
       .format("delta")
       .save("/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_OI_stg/")

// COMMAND ----------

// DBTITLE 1,OI: Final Insert
// MAGIC %sql
// MAGIC MERGE INTO $L2_ANALYTICS_TABLES.INVOICE  tgt
// MAGIC USING INVOICE_stage_dedup stg
// MAGIC ON tgt.source_system = 'STERLING_DTC'
// MAGIC and trim(tgt.INVOICE_ID) = trim(stg.INVOICE_ID)
// MAGIC and tgt.FIRST_EFFECTIVE_TS = stg.FIRST_EFFECTIVE_TS
// MAGIC WHEN MATCHED AND tgt.LAST_EFFECTIVE_TS != stg.LAST_EFFECTIVE_TS  --if existing record, and there is a new LAST_EFFECTIVE_TS, perform update. If LAST_EFFECTIVE_TS is same, do nothing (no change)
// MAGIC   THEN UPDATE SET tgt.LAST_EFFECTIVE_TS = stg.LAST_EFFECTIVE_TS, tgt.UPDATE_TS = stg.UPDATE_TS 
// MAGIC WHEN NOT MATCHED --if new record, aka, FIRST_EFFECTIVE_TS not found, do an insert
// MAGIC   THEN INSERT * 

// COMMAND ----------

// DBTITLE 1,OI Audit: check row counts of target after merge
// MAGIC %sql
// MAGIC select count(*) from $L2_ANALYTICS_TABLES.INVOICE

// COMMAND ----------

// DBTITLE 1,OID: Clear 1st Stage Table (21 Table Merge)
spark.sql("""drop table if exists """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OID""")
dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/dtc_mergedstage_OID/""", true)


// COMMAND ----------

// DBTITLE 1,OID: Create 1st Stage Table (21 Table Merge)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OID(
// MAGIC     `INVOICE_DETAIL_ID` STRING, 
// MAGIC     `PRIME_LINE_SEQ_NBR` DECIMAL(5,0), 
// MAGIC     `INVOICE_ID` STRING, 
// MAGIC     `INVOICE_DT` DATE, 
// MAGIC     `ITEM_ID` STRING, 
// MAGIC     `ORDER_LINE_ID` STRING, 
// MAGIC     `TXN_TYPE_CD` STRING, 
// MAGIC     `TXN_SUB_TYPE_CD` STRING, 
// MAGIC     `ENTRY_METHOD` STRING, 
// MAGIC     `SLSPRSN_ID` STRING, 
// MAGIC     `INVOICE_QTY` INT, 
// MAGIC     `INVOICE_AMT` DECIMAL(15,2), 
// MAGIC     `UNIT_PRICE_AMT` DECIMAL(19,6), 
// MAGIC     `TXN_SIGN` STRING, 
// MAGIC     `TAXABLE_FLAG` STRING, 
// MAGIC     `ORIG_TXN_NBR` INT, 
// MAGIC     `ORIG_UNIT_PRICE_AMT` DECIMAL(19,6), 
// MAGIC     `ORIG_TXN_SIGN_CD` STRING, 
// MAGIC     `REASON_ID` STRING, 
// MAGIC     `RETURN_REASON_ID` STRING, 
// MAGIC     `DROP_SHIP_FLAG` STRING,
// MAGIC     VENDOR_COST DECIMAL(15,2),
// MAGIC     PO_EXPENSES DECIMAL(15,2),
// MAGIC     PO_DUTY DECIMAL(15,2),
// MAGIC     WEIGHTED_AVG_COST DECIMAL(15,2),
// MAGIC     LABOR_COST DECIMAL(15,2),
// MAGIC     temp_value_systemContext_wsieventTransactionTimeStamp timestamp ,
// MAGIC     temp_wsieventName        varchar(50)
// MAGIC ) USING DELTA 
// MAGIC LOCATION "/mnt/data/governed/l2/stage/sales/dtc_mergedstage_OID/";

// COMMAND ----------

// DBTITLE 1,OID: Load 1st Stage Table -- uses JDBC IF STERLING XML DOESN'T HAVE INVOICE DETAIL}}
import org.apache.spark.sql.functions._
import java.util.Calendar


var insertsql = """
INSERT INTO  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OID
select 
    str.order_invoice_detail_key as INVOICE_DETAIL_ID, 
    str.prime_line_no as PRIME_LINE_SEQ_NBR, 
    TRIM(str.ORDER_INVOICE_KEY)          as INVOICE_ID, 
    str.strINVOICE_DT          as INVOICE_DT, 
    TRIM(str.item_id)            as ITEM_ID, 
    TRIM(str.order_line_key)     as ORDER_LINE_ID, 
    TXN.TXN_TYPE_CD         as TXN_TYPE_CD, 
    oh.extnCustomerType       as TXN_SUB_TYPE_CD, 
    'Online'               as ENTRY_METHOD, 
    null                   as SLSPRSN_ID, 
    str.quantity           as INVOICE_QTY, 
    str.final_line_charges as INVOICE_AMT, 
    str.unit_price         as UNIT_PRICE_AMT, 
    null                   as TXN_SIGN, 
    str.extn_is_taxable    as TAXABLE_FLAG, 
    null                   as ORIG_TXN_NBR, 
    str.original_unit_price as  ORIG_UNIT_PRICE_AMT, 
    null                   as ORIG_TXN_SIGN_CD, 
    ol.returnReason                   as REASON_ID, 
    null        as RETURN_REASON_ID, 
    ol.extnDirectShipInd   as DROP_SHIP_FLAG,
    str.extn_vendor_cost,
    str.extn_po_expenses,
    str.extn_po_duty,
    str.extn_wac,
    str.extn_labor_cost,
    oh.wsieventTransactionTimeStamp ,
    oh.temp_wsieventName
    from """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OI_oh oh
    INNER JOIN sterling_lkp str
    on str.ORDER_HEADER_ID = oh.ORDER_HEADER_ID
    LEFT JOIN L2_ANALYTICS_TABLES.TXN_TYPE TXN
    ON TXN.DTC_DOCUMENT_ID = oh.DOC_TYPE 
    left join """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OID_PreSterling_ol ol
    on ol.orderLineKey = str.order_line_key
    group by
    str.order_invoice_detail_key,
    str.prime_line_no,
    str.ORDER_INVOICE_KEY,
    str.strINVOICE_DT,
    str.item_id ,
    str.order_line_key ,
    TXN.TXN_TYPE_CD ,
    oh.extnCustomerType,
    str.quantity ,
    str.final_line_charges ,
    str.unit_price ,
    str.extn_is_taxable ,
    str.original_unit_price ,
    ol.returnReason ,
    ol.extnDirectShipInd ,
        str.extn_vendor_cost,
    str.extn_po_expenses,
    str.extn_po_duty,
    str.extn_wac,
    str.extn_labor_cost,
    oh.wsieventTransactionTimeStamp ,
    oh.temp_wsieventName
    
""" ;
spark.sql(insertsql);

// COMMAND ----------

// DBTITLE 1,OID: Audit: Show Counts for First Stage Table 
// MAGIC %sql
// MAGIC select count(*) from $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OID 

// COMMAND ----------

// DBTITLE 1,OID: Merge 21 L1 tables into "L1 Stage Delta Table"
//This cell is left deactivated as of 5/1/2020
import org.apache.spark.sql.functions._
import java.util.Calendar

//TBD for when Sterling provides records in L1




//val schemaType1 = Array( "DSPOCREATE","DSPOMODIFY","PURCHASEORDERTRACKING","RETURNCLOSERECEIPT", "RETURNORDERCREATE", "RETURNORDERRECEIVE","SALESORDERACKNOWLEDGE", "SALESORDERDROP", "SALESORDERMODIFY", "SALESORDERSCHEDULE","SALESORDERSHIPCREATECONFIRM", "SALESORDERSHIPCONFIRM", "SALESORDERTRACKING", "WORKORDERCREATE", "WORKORDERDELIVER", "WORKORDERMODIFY") //for SO's
//val schemaType1 = Array( "DSPOCREATE") //for SO's


//for(schemaType <- schemaType1){
 
  
// var distinctSchemaDF1 = spark.sql("""select value.order.extn.extnCustomerType,value.order.documentType, value.systemContext.wsieventTransactionTimeStamp, exploded.invoiceCollectionDetails as col , value.order.orderHeaderKey, value.systemContext.wsieventName as temp_wsieventNameFROM delta.`""" + L1_ADLS_LOC + schemaType + """/` LATERAL VIEW explode(value.order.chargeTransactionDetails.chargeTransactionDetail) event_table as exploded""")

//distinctSchemaDF1.createOrReplaceTempView("COMBINEDSCHEMA_INV_lvl1")
  
//var insertsql = """
//INSERT INTO  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OID_PreSterling
//select distinct 
//    exploded.orderInvoiceKey as `INVOICE_ID` , 
//    substring(exploded.dateInvoiced ,1,10) as `INVOICE_DT` , 
//    orderHeaderKey as `ORDER_HEADER_KEY` , 
//    extnCustomerType as Customer_Type,
//    wsieventTransactionTimeStamp as temp_value_systemContext_wsieventTransactionTimeStamp ,
//    temp_wsieventName as temp_wsieventName   ,
//    TXN.TNX_TYPE_CD
//from COMBINEDSCHEMA_INV_lvl1
//LEFT JOIN """ + L2_ANALYTICS_TABLES + """.TXN_TYPE TXN
//ON TXN.DTC_DOCUMENT_ID = CAST(documentType AS int) 
//LATERAL VIEW explode(col.invoiceCollectionDetail) event_table as exploded
//""" ;
//spark.sql(insertsql);
//  
//  var insertsql_ol = """
//INSERT INTO  """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OID_PreSterling_ol
//
//    select distinct exploded_ol.orderLineKey, exploded_ol.returnReason, exploded_ol.itemDetails.extn.extnDirectShipInd
//FROM delta.`""" + L1_ADLS_LOC + schemaType + """/` 
//LATERAL VIEW explode(value.order.orderLines.orderLine) event_table_ol as exploded_ol
//""" ;
//spark.sql(insertsql_ol);
//  
//  
//  var timenow = Calendar.getInstance().getTime()
// println(L1_ADLS_LOC + schemaType + ", Completed " + timenow) 
//}

// COMMAND ----------

// DBTITLE 1,OID: Keep only 1 Composite Primary Key Within the Same Batch
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW PREFLATTEN_OID AS
// MAGIC ( 
// MAGIC SELECT *, 
// MAGIC   /*PATTERN: feed columns here EXCEPT event type and event timestamp*/
// MAGIC     coalesce(INVOICE_DETAIL_ID,'temp_flat')
// MAGIC     ||coalesce(PRIME_LINE_SEQ_NBR,'temp_flat')
// MAGIC     ||coalesce(INVOICE_ID,'temp_flat')
// MAGIC     ||coalesce(INVOICE_DT,'temp_flat')
// MAGIC     ||coalesce(ITEM_ID,'temp_flat')
// MAGIC     ||coalesce(ORDER_LINE_ID,'temp_flat')
// MAGIC     ||coalesce(TXN_TYPE_CD,'temp_flat')
// MAGIC     ||coalesce(TXN_SUB_TYPE_CD,'temp_flat')
// MAGIC     ||coalesce(ENTRY_METHOD,'temp_flat')
// MAGIC     ||coalesce(SLSPRSN_ID,'temp_flat')
// MAGIC     ||coalesce(INVOICE_QTY,'temp_flat')
// MAGIC     ||coalesce(INVOICE_AMT,'temp_flat')
// MAGIC     ||coalesce(UNIT_PRICE_AMT,'temp_flat')
// MAGIC     ||coalesce(TXN_SIGN,'temp_flat')
// MAGIC     ||coalesce(TAXABLE_FLAG,'temp_flat')
// MAGIC     ||coalesce(ORIG_TXN_NBR,'temp_flat')
// MAGIC     ||coalesce(ORIG_UNIT_PRICE_AMT,'temp_flat')
// MAGIC     ||coalesce(ORIG_TXN_SIGN_CD,'temp_flat')
// MAGIC     ||coalesce(REASON_ID,'temp_flat')
// MAGIC     ||coalesce(RETURN_REASON_ID,'temp_flat')
// MAGIC     ||coalesce(DROP_SHIP_FLAG,'temp_flat')
// MAGIC      as row_summary
// MAGIC   /*END PATTERN*/
// MAGIC FROM (select distinct * from $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OID) SUBQ
// MAGIC );
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FLAT_OID AS 
// MAGIC ( 
// MAGIC select * from (
// MAGIC   select *, CASE WHEN row_summary = LAG(row_summary, 1) OVER (PARTITION BY INVOICE_DETAIL_ID ORDER BY  temp_value_systemContext_wsieventTransactionTimeStamp) 
// MAGIC   THEN 0 else 1 end as change_flag from PREFLATTEN_OID
// MAGIC ) where change_flag > 0
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OID: Perform All Look-ups. Copy Key if known. Generate FIRST_EFFECTIVE_TS
// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW STAGE_OID AS 
// MAGIC ( 
// MAGIC SELECT  
// MAGIC COALESCE(TARGET.INVOICE_DETAIL_KEY, -1) AS INVOICE_DETAIL_KEY,
// MAGIC OID.INVOICE_DETAIL_ID, 
// MAGIC 'STERLING_DTC' as SOURCE_SYSTEM, 
// MAGIC coalesce(OID.PRIME_LINE_SEQ_NBR,-1) as PRIME_LINE_SEQ_NBR, 
// MAGIC coalesce(oi.INVOICE_KEY,-1) as INVOICE_KEY, 
// MAGIC OID.INVOICE_ID as INVOICE_ID, 
// MAGIC OID.INVOICE_DT,
// MAGIC coalesce(it.ITEM_KEY,-1) as ITEM_KEY,
// MAGIC coalesce(trim(oid.ITEM_ID),-1) as item_id, 
// MAGIC coalesce(ol.ORDER_LINE_KEY,-1) as ORDER_LINE_KEY,
// MAGIC coalesce(oid.ORDER_LINE_ID,-1) as ORDER_LINE_ID, 
// MAGIC     oid.temp_value_systemContext_wsieventTransactionTimeStamp as FIRST_EFFECTIVE_TS          ,
// MAGIC     null as LAST_EFFECTIVE_TS ,
// MAGIC null as RETAIL_TYPE_CD,
// MAGIC null as POS_RING_TYPE_CD,
// MAGIC oid.TXN_TYPE_CD,
// MAGIC oid.TXN_SUB_TYPE_CD,
// MAGIC oid.ENTRY_METHOD, 
// MAGIC oid.SLSPRSN_ID, 
// MAGIC oid.INVOICE_QTY, 
// MAGIC oid.INVOICE_AMT, 
// MAGIC oid.UNIT_PRICE_AMT, 
// MAGIC oid.TXN_SIGN, 
// MAGIC oid.TAXABLE_FLAG,
// MAGIC oid.ORIG_TXN_NBR, 
// MAGIC coalesce(oid.ORIG_UNIT_PRICE_AMT,0),
// MAGIC oid.ORIG_TXN_SIGN_CD,
// MAGIC oid.REASON_ID, 
// MAGIC oid.RETURN_REASON_ID, 
// MAGIC oid.DROP_SHIP_FLAG,
// MAGIC oid.WEIGHTED_AVG_COST,
// MAGIC oid.LABOR_COST,
// MAGIC null as LABOR_SKU,
// MAGIC oid.VENDOR_COST,
// MAGIC oid.PO_EXPENSES,
// MAGIC oid.PO_DUTY,
// MAGIC     null as INSERT_TS ,                  
// MAGIC     null as UPDATE_TS               
// MAGIC 
// MAGIC FROM FLAT_OID OID
// MAGIC 
// MAGIC LEFT JOIN $L2_ANALYTICS_TABLES.INVOICE oi
// MAGIC     ON oi.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC     AND trim(oi.INVOICE_ID) = trim(OID.INVOICE_ID)
// MAGIC     AND oi.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC 
// MAGIC LEFT JOIN $L2_ANALYTICS_TABLES.ORDER_LINE ol 
// MAGIC     on ol.source_system = 'STERLING_DTC'
// MAGIC     and trim(ol.ORDER_LINE_ID) = trim(oid.ORDER_LINE_ID)
// MAGIC     and ol.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59') 
// MAGIC 
// MAGIC LEFT JOIN $L2_ANALYTICS_TABLES.ORDER_HEADER oh 
// MAGIC     ON oh.ORDER_HEADER_KEY = ol.ORDER_HEADER_KEY
// MAGIC     and oh.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59') 
// MAGIC     
// MAGIC     
// MAGIC LEFT JOIN L2_ANALYTICS.ITEM it 
// MAGIC     on trim(it.ITEM_ID) = trim(oid.ITEM_ID)
// MAGIC     and trim(it.MARKET_CD) = COALESCE(oh.MARKET_CD,'USA')
// MAGIC     and it.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC 
// MAGIC /*PATTERN: left join to target table here*/
// MAGIC LEFT OUTER JOIN 
// MAGIC   $L2_ANALYTICS_TABLES.INVOICE_DETAIL TARGET
// MAGIC     ON TARGET.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC     AND trim(TARGET.INVOICE_DETAIL_ID) = trim(OID.INVOICE_DETAIL_ID)
// MAGIC     AND TARGET.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC 	
// MAGIC 
// MAGIC /*END PATTERN*/
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OID: Remove Repeating Values Across Mini-batches
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW PREFLATTEN_MULTIBATCH_OID AS ( 
// MAGIC SELECT *, 
// MAGIC 
// MAGIC --coalesce(INVOICE_DETAIL_KEY,'temp_flat') || 
// MAGIC coalesce(INVOICE_DETAIL_ID,'temp_flat') 
// MAGIC ||    coalesce(SOURCE_SYSTEM,'temp_flat')  
// MAGIC ||    coalesce(PRIME_LINE_SEQ_NBR, 'temp_flat')  
// MAGIC --||    coalesce(INVOICE_KEY, 'temp_flat')  
// MAGIC ||    coalesce(INVOICE_DT ,'temp_flat')  
// MAGIC --||    coalesce(ITEM_KEY, 'temp_flat')  --it.ITEM_KEY
// MAGIC ||    coalesce(ITEM_ID, 'temp_flat')  
// MAGIC --||    coalesce(ORDER_LINE_KEY,'temp_flat')   --ol.ORDER_LINE_KEY, 
// MAGIC ||    coalesce(ORDER_LINE_ID, 'temp_flat')  
// MAGIC ||    coalesce(RETAIL_TYPE_CD,'temp_flat')  
// MAGIC ||    coalesce(POS_RING_TYPE_CD,'temp_flat')  
// MAGIC ||    coalesce(TXN_TYPE_CD,'temp_flat')  
// MAGIC ||    coalesce(TXN_SUB_TYPE_CD,'temp_flat')  
// MAGIC ||    coalesce(ENTRY_METHOD, 'temp_flat')  
// MAGIC ||    coalesce(SLSPRSN_ID, 'temp_flat')  
// MAGIC ||    coalesce(INVOICE_QTY, 'temp_flat')  
// MAGIC ||    coalesce(INVOICE_AMT, 'temp_flat')  
// MAGIC ||    coalesce(UNIT_PRICE_AMT, 'temp_flat')  
// MAGIC ||    coalesce(TXN_SIGN, 'temp_flat')  
// MAGIC ||    coalesce(TAXABLE_FLAG,'temp_flat')  
// MAGIC ||    coalesce(ORIG_TXN_NBR, 'temp_flat')  
// MAGIC ||    coalesce(ORIG_UNIT_PRICE_AMT,'temp_flat')  
// MAGIC ||    coalesce(ORIG_TXN_SIGN_CD,'temp_flat')  
// MAGIC ||    coalesce(REASON_ID, 'temp_flat')  
// MAGIC ||    coalesce(RETURN_REASON_ID, 'temp_flat')  
// MAGIC ||    coalesce(DROP_SHIP_FLAG, 'temp_flat')  as  row_summary 
// MAGIC FROM ( 
// MAGIC select 
// MAGIC tgt.INVOICE_DETAIL_KEY,
// MAGIC tgt.INVOICE_DETAIL_ID,
// MAGIC tgt.SOURCE_SYSTEM,
// MAGIC tgt.PRIME_LINE_SEQ_NBR,
// MAGIC tgt.INVOICE_KEY,
// MAGIC tgt.INVOICE_ID,
// MAGIC tgt.INVOICE_DT,
// MAGIC tgt.ITEM_KEY,
// MAGIC tgt.ITEM_ID,
// MAGIC tgt.ORDER_LINE_KEY,
// MAGIC tgt.ORDER_LINE_ID,
// MAGIC tgt.FIRST_EFFECTIVE_TS,
// MAGIC tgt.LAST_EFFECTIVE_TS,
// MAGIC tgt.RETAIL_TYPE_CD,
// MAGIC tgt.POS_RING_TYPE_CD,
// MAGIC tgt.TXN_TYPE_CD,
// MAGIC tgt.TXN_SUB_TYPE_CD,
// MAGIC tgt.ENTRY_METHOD,
// MAGIC tgt.SLSPRSN_ID,
// MAGIC tgt.INVOICE_QTY,
// MAGIC tgt.INVOICE_AMT,
// MAGIC tgt.UNIT_PRICE_AMT,
// MAGIC tgt.TXN_SIGN,
// MAGIC tgt.TAXABLE_FLAG,
// MAGIC tgt.ORIG_TXN_NBR,
// MAGIC tgt.ORIG_UNIT_PRICE_AMT,
// MAGIC tgt.ORIG_TXN_SIGN_CD,
// MAGIC tgt.REASON_ID,
// MAGIC tgt.RETURN_REASON_ID,
// MAGIC tgt.DROP_SHIP_FLAG,
// MAGIC tgt.WEIGHTED_AVG_COST,
// MAGIC tgt.LABOR_COST,
// MAGIC tgt.LABOR_SKU,
// MAGIC tgt.VENDOR_COST,
// MAGIC tgt.PO_EXPENSES,
// MAGIC tgt.PO_DUTY,
// MAGIC tgt.INSERT_TS,
// MAGIC tgt.UPDATE_TS
// MAGIC from $L2_ANALYTICS_TABLES.INVOICE_DETAIL tgt inner join FLAT_OID oistg on --tgt.INVOICE_dt = oistg.INVOICE_dt and 
// MAGIC tgt.source_system = 'STERLING_DTC'  and tgt.INVOICE_DETAIL_ID = oistg.INVOICE_DETAIL_ID AND tgt.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC union all 
// MAGIC select * from STAGE_OID stg 
// MAGIC ) stgtgt );
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FLATTEN_MULTIBATCH_OID AS 
// MAGIC (
// MAGIC select *
// MAGIC , LEAD(FIRST_EFFECTIVE_TS) OVER (PARTITION BY INVOICE_DETAIL_ID ORDER BY  FIRST_EFFECTIVE_TS)  as new_LAST_EFFECTIVE_TS
// MAGIC ,coalesce(INSERT_TS, current_timestamp) as  new_INSERT_TS --retain timestamp if already previously inserted, otherwise generate value
// MAGIC ,current_timestamp as new_UPDATE_TS --always generate value upon insert
// MAGIC from (
// MAGIC   select *, CASE WHEN row_summary = LAG(row_summary, 1) OVER (PARTITION BY INVOICE_DETAIL_ID ORDER BY  FIRST_EFFECTIVE_TS) 
// MAGIC   THEN 0 else 1 end as change_flag from PREFLATTEN_MULTIBATCH_OID
// MAGIC ) where change_flag > 0
// MAGIC );

// COMMAND ----------

// DBTITLE 1,OID: Clear Previous Stage Tables

spark.sql("""drop table if exists """ + L2_STAGE + """.INVOICE_DETAIL_stage""")
dbutils.fs.rm("""dbfs:/mnt/data/governed/l2/stage/sales/dtc_preL2_OID/""", true)



// COMMAND ----------

// DBTITLE 1,OID: Create 2nd Stage Table (pre-insert)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE $L2_STAGE.invoice_Detail_stage (
// MAGIC `INVOICE_DETAIL_KEY` BIGINT, 
// MAGIC `INVOICE_DETAIL_ID` STRING, 
// MAGIC `CONCEPT_CD` string, 
// MAGIC `SOURCE_SYSTEM` STRING, 
// MAGIC `PRIME_LINE_SEQ_NBR` DECIMAL(5,0), 
// MAGIC `INVOICE_KEY` STRING,
// MAGIC `INVOICE_ID` string,  
// MAGIC `INVOICE_DT` DATE, 
// MAGIC `ITEM_KEY` INT, 
// MAGIC `ITEM_ID` STRING, 
// MAGIC `ORDER_LINE_KEY` BIGINT, 
// MAGIC `ORDER_LINE_ID` STRING,
// MAGIC `LOCATION_KEY` bigint, 
// MAGIC `LOCATION_ID` string, 
// MAGIC `GLOBAL_COST_CENTER_KEY` bigint, 
// MAGIC `GLOBAL_COST_CENTER_ID` string, 
// MAGIC `GLOBAL_SUBSIDIARY_KEY` bigint, 
// MAGIC `GLOBAL_SUBSIDIARY_ID` string, 
// MAGIC `FIRST_EFFECTIVE_TS` TIMESTAMP, 
// MAGIC `LAST_EFFECTIVE_TS` TIMESTAMP, 
// MAGIC `RETAIL_TYPE_CD` STRING, 
// MAGIC `POS_RING_TYPE_CD` STRING, 
// MAGIC `TXN_TYPE_CD` STRING,
// MAGIC `TXN_SUB_TYPE_CD` STRING, 
// MAGIC `ENTRY_METHOD` STRING, 
// MAGIC `SLSPRSN_ID` STRING, 
// MAGIC `INVOICE_QTY` INT, 
// MAGIC `INVOICE_AMT` DECIMAL(15,2), 
// MAGIC `UNIT_PRICE_AMT` DECIMAL(19,6),
// MAGIC `GLOBAL_ESTIMATED_COST_AMT` DECIMAL(15,4),   /* GODAVARI CHANGE  */
// MAGIC `TXN_SIGN` STRING,
// MAGIC `TAXABLE_FLAG` STRING, 
// MAGIC `ORIG_TXN_NBR` INT, 
// MAGIC `ORIG_UNIT_PRICE_AMT` DECIMAL(19,6), 
// MAGIC `ORIG_TXN_SIGN_CD` STRING, 
// MAGIC `REASON_ID` STRING, 
// MAGIC `RETURN_REASON_ID` STRING,
// MAGIC `DROP_SHIP_FLAG` STRING,
// MAGIC `ACTUAL_SHIP_DT` date, 
// MAGIC WEIGHTED_AVG_COST DECIMAL(15,2),
// MAGIC LABOR_COST DECIMAL(15,2),
// MAGIC LABOR_SKU varchar(10),
// MAGIC VENDOR_COST  DECIMAL(15,2),
// MAGIC PO_EXPENSES  DECIMAL(15,2),
// MAGIC PO_DUTY DECIMAL(15,2),
// MAGIC `FULFILLMENT_STATUS_ID` string, 
// MAGIC `DISCOUNT_RATE_AMT` decimal(15,4), 
// MAGIC `ELC_AGENT_COMMISSION_AMT` decimal(15,4), 
// MAGIC `ELC_HTS_AMT` decimal(15,4), 
// MAGIC `ELC_FREIGHT_AMT` decimal(15,4), 
// MAGIC `ELC_MISC_AMT` decimal(15,4), 
// MAGIC `FIRST_COST_AMT` decimal(15,4), 
// MAGIC `PACK_GROUP_NBR` int, 
// MAGIC `COMMITTED_QTY` int, 
// MAGIC `CLOSED_DT` date, 
// MAGIC `INSERT_TS` TIMESTAMP,
// MAGIC `UPDATE_TS` TIMESTAMP)
// MAGIC USING DELTA
// MAGIC OPTIONS (
// MAGIC   path "/mnt/data/governed/l2/stage/sales/dtc_preL2_OID/"
// MAGIC )
// MAGIC PARTITIONED BY (INVOICE_DT);

// COMMAND ----------

// DBTITLE 1,OID: Generate OIDK if new record
// MAGIC %sql
// MAGIC INSERT INTO TABLE $L2_STAGE.INVOICE_DETAIL_stage 
// MAGIC select 
// MAGIC  CASE WHEN STG1.INVOICE_DETAIL_KEY = -1 THEN
// MAGIC dense_rank() OVER (ORDER BY STG1.INVOICE_DETAIL_KEY, STG1.INVOICE_DETAIL_ID) + MAX_INVOICE_DETAIL_KEY ELSE
// MAGIC STG1.INVOICE_DETAIL_KEY END AS INVOICE_DETAIL_KEY, 
// MAGIC STG1.INVOICE_DETAIL_ID, 
// MAGIC null, 
// MAGIC STG1.SOURCE_SYSTEM, 
// MAGIC STG1.PRIME_LINE_SEQ_NBR, 
// MAGIC STG1.INVOICE_KEY, 
// MAGIC STG1.INVOICE_ID,
// MAGIC STG1.INVOICE_DT DATE,
// MAGIC STG1.ITEM_KEY, --it.ITEM_KEY
// MAGIC STG1.ITEM_ID, 
// MAGIC STG1.ORDER_LINE_KEY, --ol.ORDER_LINE_KEY, 
// MAGIC STG1.ORDER_LINE_ID, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC STG1.FIRST_EFFECTIVE_TS          ,
// MAGIC  case when new_LAST_EFFECTIVE_TS is null then to_timestamp('9999-12-31T23:59:59')
// MAGIC   else new_LAST_EFFECTIVE_TS - INTERVAL 1 MILLISECONDS 
// MAGIC   end as LAST_EFFECTIVE_TS,
// MAGIC STG1.RETAIL_TYPE_CD,
// MAGIC STG1.POS_RING_TYPE_CD,
// MAGIC STG1.TXN_TYPE_CD,
// MAGIC STG1.TXN_SUB_TYPE_CD,
// MAGIC STG1.ENTRY_METHOD, 
// MAGIC STG1.SLSPRSN_ID, 
// MAGIC STG1.INVOICE_QTY, 
// MAGIC STG1.INVOICE_AMT, 
// MAGIC STG1.UNIT_PRICE_AMT,
// MAGIC null,                       /*  GODAVARI Change   */
// MAGIC STG1.TXN_SIGN, 
// MAGIC STG1.TAXABLE_FLAG,
// MAGIC STG1.ORIG_TXN_NBR, 
// MAGIC STG1.ORIG_UNIT_PRICE_AMT,
// MAGIC STG1.ORIG_TXN_SIGN_CD,
// MAGIC STG1.REASON_ID, 
// MAGIC STG1.RETURN_REASON_ID, 
// MAGIC STG1.DROP_SHIP_FLAG,
// MAGIC null, 
// MAGIC STG1.WEIGHTED_AVG_COST,
// MAGIC STG1.LABOR_COST, 
// MAGIC STG1.LABOR_SKU, 
// MAGIC STG1.VENDOR_COST,
// MAGIC STG1.PO_EXPENSES,
// MAGIC STG1.PO_DUTY,
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC null, 
// MAGIC new_INSERT_TS as INSERT_TS,
// MAGIC new_UPDATE_TS as UPDATE_TS 
// MAGIC from FLATTEN_MULTIBATCH_OID STG1 
// MAGIC CROSS JOIN (
// MAGIC     SELECT COALESCE(MAX(INVOICE_DETAIL_KEY),0) AS MAX_INVOICE_DETAIL_KEY FROM $L2_ANALYTICS_TABLES.INVOICE_DETAIL WHERE INVOICE_DETAIL_KEY <> -1
// MAGIC     ) TARGET_MAX

// COMMAND ----------

// DBTITLE 1,OID Audit: Row count of final stage
// MAGIC %sql
// MAGIC select count(*) from $L2_STAGE.INVOICE_DETAIL_stage  --740357

// COMMAND ----------

// DBTITLE 1,OID: Ensure unique composite primary key + first effective ts
var df_stg = table(L2_STAGE + """.INVOICE_DETAIL_stage""")
var df_uniq = df_stg.orderBy(col("LAST_EFFECTIVE_TS").desc).coalesce(1).dropDuplicates("first_effective_ts", "INVOICE_DETAIL_ID")
df_uniq.createOrReplaceTempView("INVOICE_DETAIL_stage_dedup")

// COMMAND ----------

// DBTITLE 1,OID Audit: Row count of target table before merge
// MAGIC %sql
// MAGIC select count(*) from INVOICE_DETAIL_stage_dedup 

// COMMAND ----------

// DBTITLE 1,OID: writing stage data to physical storage for snowflake consumption
dbutils.fs.rm("/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_OID_stg/", true)
// writing stage data to physical storage for snowflake consumption
df_uniq.write
       .format("delta")
       .save("/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_OID_stg/")

// COMMAND ----------

// DBTITLE 1,OID: Final Insert
// MAGIC %sql
// MAGIC MERGE INTO $L2_ANALYTICS_TABLES.INVOICE_DETAIL  tgt
// MAGIC USING INVOICE_DETAIL_stage_dedup stg
// MAGIC ON tgt.source_system = 'STERLING_DTC'
// MAGIC and trim(tgt.INVOICE_DETAIL_ID) = trim(stg.INVOICE_DETAIL_ID)
// MAGIC and tgt.FIRST_EFFECTIVE_TS = stg.FIRST_EFFECTIVE_TS
// MAGIC WHEN MATCHED AND tgt.LAST_EFFECTIVE_TS != stg.LAST_EFFECTIVE_TS  --if existing record, and there is a new LAST_EFFECTIVE_TS, perform update. If LAST_EFFECTIVE_TS is same, do nothing (no change)
// MAGIC   THEN UPDATE SET tgt.LAST_EFFECTIVE_TS = stg.LAST_EFFECTIVE_TS, tgt.UPDATE_TS = stg.UPDATE_TS 
// MAGIC WHEN NOT MATCHED --if new record, aka, FIRST_EFFECTIVE_TS not found, do an insert
// MAGIC   THEN INSERT * 

// COMMAND ----------

// DBTITLE 1,OILC: Drop and Recreate 1st stage table
spark.sql("""drop table if exists """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OILC""")

dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/sales/dtc_mergestage_OILC/", true)


// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OILC(
// MAGIC     INVOICE_DETAIL_ID               varchar(50) ,
// MAGIC     INVOICE_LINE_CHARGE_ID        varchar(50)  ,
// MAGIC     INVOICE_DT                    date,
// MAGIC     CHARGE_NAME                 varchar(50) ,
// MAGIC     CHARGE_CATEGORY             varchar(50) ,
// MAGIC     CHARGE_AMOUNT               decimal(15, 2) ,
// MAGIC     CHARGE_PER_LINE_AMT         decimal(15, 2) ,
// MAGIC     CHARGE_PER_UNIT             decimal(15, 2) ,
// MAGIC     REFERENCE_CHARGE_AMT        decimal(15, 2) ,
// MAGIC     temp_value_systemContext_wsieventTransactionTimeStamp timestamp 
// MAGIC ) USING DELTA 
// MAGIC PARTITIONED BY (INVOICE_DT)
// MAGIC LOCATION "/mnt/data/governed/l2/stage/sales/dtc_mergestage_OILC/";

// COMMAND ----------

// DBTITLE 1,OILC: Populate first stage table

var insertsql = """INSERT INTO """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_OILC
                    SELECT 
                        STR.INVOICE_DETAIL_ID,
                        STR.INVOICE_LINE_CHARGE_ID,
                        INV.INVOICE_DT,
                        STR.CHARGE_NAME,
                        STR.CHARGE_CATEGORY,
                        STR.CHARGE_AMOUNT,
                        STR.CHARGE_PER_LINE_AMT,
                        STR.CHARGE_PER_UNIT,
                        STR.REFERENCE_CHARGE_AMT,
                        INV.FIRST_EFFECTIVE_TS
                    FROM """ + L2_STAGE + """.INVOICE_STAGE INV
                    INNER JOIN """ + L2_STAGE + """.STERLING_INV_EXTRACT_OILC STR 
                      ON INV.INVOICE_ID =  STR.TEMP_HEADER_KEY 
                        AND STR.OILC_RECORD_TYPE = 'INV'
                """;
spark.sql(insertsql);

// COMMAND ----------

// DBTITLE 1,OILC: Remove Repeating Values within Minibatch
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW PREFLATTEN_OILC AS
// MAGIC ( 
// MAGIC SELECT *, 
// MAGIC   /*PATTERN: feed columns here EXCEPT event type and event timestamp*/
// MAGIC   coalesce(INVOICE_DETAIL_ID,'temp_flat')
// MAGIC   ||coalesce(INVOICE_LINE_CHARGE_ID,'temp_flat')
// MAGIC   ||coalesce(INVOICE_DT,'temp_flat')
// MAGIC   ||coalesce(CHARGE_NAME,'temp_flat')
// MAGIC   ||coalesce(CHARGE_CATEGORY,'temp_flat')
// MAGIC   ||coalesce(CHARGE_AMOUNT,'temp_flat')
// MAGIC   ||coalesce(CHARGE_PER_LINE_AMT,'temp_flat')
// MAGIC   ||coalesce(CHARGE_PER_UNIT,'temp_flat')
// MAGIC   ||coalesce(REFERENCE_CHARGE_AMT,'temp_flat') as row_summary
// MAGIC   /*END PATTERN*/
// MAGIC FROM $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_OILC SUBQ
// MAGIC );
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FLAT_OILC AS 
// MAGIC ( 
// MAGIC select * from (
// MAGIC   select *, CASE WHEN row_summary = LAG(row_summary, 1) OVER (PARTITION BY INVOICE_LINE_CHARGE_ID ORDER BY  temp_value_systemContext_wsieventTransactionTimeStamp) 
// MAGIC   THEN 0 else 1 end as change_flag from PREFLATTEN_OILC
// MAGIC ) where change_flag > 0
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OILC: Perform All Look-ups, Generate OILCK if known
// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW STAGE_OILC AS 
// MAGIC ( 
// MAGIC SELECT  
// MAGIC COALESCE(TARGET.INVOICE_LINE_CHARGE_KEY, -1) AS INVOICE_LINE_CHARGE_KEY,
// MAGIC     oilc.INVOICE_LINE_CHARGE_ID    ,
// MAGIC     oilc.INVOICE_DT                ,
// MAGIC     coalesce(id.INVOICE_DETAIL_KEY,-1) as INVOICE_DETAIL_KEY,
// MAGIC     oilc.INVOICE_DETAIL_ID           ,
// MAGIC     temp_value_systemContext_wsieventTransactionTimeStamp as FIRST_EFFECTIVE_TS          ,
// MAGIC     null as LAST_EFFECTIVE_TS           ,
// MAGIC     oilc.CHARGE_NAME             ,
// MAGIC     oilc.CHARGE_CATEGORY         ,
// MAGIC     oilc.CHARGE_AMOUNT           ,
// MAGIC     oilc.CHARGE_PER_LINE_AMT     ,
// MAGIC     oilc.CHARGE_PER_UNIT         ,
// MAGIC     oilc.REFERENCE_CHARGE_AMT    ,
// MAGIC     null as INSERT_TS                   ,
// MAGIC     null as UPDATE_TS                   
// MAGIC 
// MAGIC FROM FLAT_OILC oilc
// MAGIC LEFT JOIN $L2_ANALYTICS_TABLES.INVOICE_DETAIL id
// MAGIC ON id.source_system = 'STERLING_DTC'
// MAGIC AND trim(oilc.INVOICE_DETAIL_ID) = trim(id.INVOICE_DETAIL_ID)
// MAGIC AND id.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC /*PATTERN: left join to target table here*/
// MAGIC LEFT OUTER JOIN 
// MAGIC     $L2_ANALYTICS_TABLES.INVOICE_LINE_CHARGE TARGET
// MAGIC     ON trim(TARGET.INVOICE_LINE_CHARGE_ID) = trim(oilc.INVOICE_LINE_CHARGE_ID)
// MAGIC     AND TARGET.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC 	
// MAGIC 
// MAGIC /*END PATTERN*/
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OILC: Remove Repeating Values Across Mini-batches
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW PREFLATTEN_MULTIBATCH_OILC AS ( 
// MAGIC SELECT *, 
// MAGIC   coalesce(INVOICE_LINE_CHARGE_KEY   ,'temp_flat')    
// MAGIC  ||    coalesce(INVOICE_LINE_CHARGE_ID,'temp_flat')  
// MAGIC  ||    coalesce(INVOICE_DT,'temp_flat') 
// MAGIC  ||    coalesce(INVOICE_DETAIL_ID,'temp_flat')            
// MAGIC  ||    coalesce(CHARGE_NAME  ,'temp_flat')            
// MAGIC  ||    coalesce(CHARGE_CATEGORY ,'temp_flat')         
// MAGIC  ||    coalesce(CHARGE_AMOUNT   ,'temp_flat')         
// MAGIC  ||    coalesce(CHARGE_PER_LINE_AMT  ,'temp_flat')    
// MAGIC  ||    coalesce(CHARGE_PER_UNIT     ,'temp_flat')     
// MAGIC  ||    coalesce(REFERENCE_CHARGE_AMT,'temp_flat')   as  row_summary 
// MAGIC FROM ( 
// MAGIC select tgt.* from $L2_ANALYTICS_TABLES.INVOICE_LINE_CHARGE tgt inner join STAGE_OILC ohstg on --tgt.INVOICE_dt = ohstg.INVOICE_dt   and
// MAGIC tgt.INVOICE_LINE_CHARGE_ID = ohstg.INVOICE_LINE_CHARGE_ID and TGT.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC union all 
// MAGIC select * from STAGE_OILC stg 
// MAGIC ) stgtgt );
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW FLATTEN_MULTIBATCH_OILC AS 
// MAGIC (
// MAGIC select *
// MAGIC , LEAD(FIRST_EFFECTIVE_TS) OVER (PARTITION BY INVOICE_LINE_CHARGE_ID ORDER BY  FIRST_EFFECTIVE_TS)  as new_LAST_EFFECTIVE_TS
// MAGIC ,coalesce(INSERT_TS, current_timestamp) as  new_INSERT_TS --retain timestamp if already previously inserted, otherwise generate value
// MAGIC ,current_timestamp as new_UPDATE_TS --always generate value upon insert
// MAGIC from (
// MAGIC   select *, CASE WHEN row_summary = LAG(row_summary, 1) OVER (PARTITION BY INVOICE_LINE_CHARGE_ID ORDER BY  FIRST_EFFECTIVE_TS) 
// MAGIC   THEN 0 else 1 end as change_flag from PREFLATTEN_MULTIBATCH_OILC
// MAGIC ) where change_flag > 0
// MAGIC );

// COMMAND ----------

spark.sql("""drop table if exists """ + L2_STAGE + """.INVOICE_LINE_CHARGE_stage""")
dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/sales/sales/dtc_preL2_OILC", true)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE $L2_STAGE.INVOICE_LINE_CHARGE_stage (
// MAGIC     INVOICE_LINE_CHARGE_KEY               LONG ,
// MAGIC     INVOICE_LINE_CHARGE_ID        varchar(50)  ,
// MAGIC     INVOICE_DT                    date         ,
// MAGIC     INVOICE_DETAIL_KEY                     INT,
// MAGIC     INVOICE_DETAIL_ID               varchar(50) ,
// MAGIC     CHARGE_NAME                 varchar(50) ,
// MAGIC     CHARGE_CATEGORY             varchar(50) ,
// MAGIC     CHARGE_AMOUNT               decimal(15, 2) ,
// MAGIC     CHARGE_PER_LINE_AMT         decimal(15, 2) ,
// MAGIC     CHARGE_PER_UNIT             decimal(15, 2) ,
// MAGIC     REFERENCE_CHARGE_AMT        decimal(15, 2) ,
// MAGIC     FIRST_EFFECTIVE_TS          timestamp,
// MAGIC     LAST_EFFECTIVE_TS          timestamp,
// MAGIC     INSERT_TS                   timestamp ,
// MAGIC     UPDATE_TS                   timestamp 
// MAGIC ) USING DELTA 
// MAGIC PARTITIONED BY (INVOICE_DT)
// MAGIC LOCATION "dbfs:/mnt/data/governed/l2/stage/sales/sales/dtc_preL2_OILC";

// COMMAND ----------

// DBTITLE 1,OILC: Generate OILCK if new record & Load Final Stage Table
// MAGIC %sql
// MAGIC INSERT INTO TABLE $L2_STAGE.INVOICE_LINE_CHARGE_stage 
// MAGIC select 
// MAGIC  CASE WHEN STG1.INVOICE_LINE_CHARGE_KEY = -1 THEN
// MAGIC dense_rank() OVER (ORDER BY STG1.INVOICE_LINE_CHARGE_KEY, STG1.INVOICE_LINE_CHARGE_ID) + MAX_INVOICE_LINE_CHARGE_KEY ELSE
// MAGIC STG1.INVOICE_LINE_CHARGE_KEY END AS INVOICE_LINE_CHARGE_KEY       
// MAGIC , INVOICE_LINE_CHARGE_ID     
// MAGIC , INVOICE_DT
// MAGIC , INVOICE_DETAIL_KEY           
// MAGIC , INVOICE_DETAIL_ID            
// MAGIC , CHARGE_NAME              
// MAGIC , CHARGE_CATEGORY          
// MAGIC , CHARGE_AMOUNT            
// MAGIC , CHARGE_PER_LINE_AMT      
// MAGIC , CHARGE_PER_UNIT          
// MAGIC , REFERENCE_CHARGE_AMT 
// MAGIC , FIRST_EFFECTIVE_TS
// MAGIC , case when new_LAST_EFFECTIVE_TS is null then to_timestamp('9999-12-31T23:59:59')
// MAGIC   else new_LAST_EFFECTIVE_TS - INTERVAL 1 MILLISECONDS
// MAGIC   end as LAST_EFFECTIVE_TS
// MAGIC ,new_INSERT_TS as INSERT_TS
// MAGIC ,new_UPDATE_TS as UPDATE_TS 
// MAGIC from FLATTEN_MULTIBATCH_OILC STG1
// MAGIC CROSS JOIN (
// MAGIC     SELECT COALESCE(MAX(INVOICE_LINE_CHARGE_KEY),0) AS MAX_INVOICE_LINE_CHARGE_KEY FROM $L2_ANALYTICS_TABLES.INVOICE_LINE_CHARGE WHERE INVOICE_LINE_CHARGE_KEY <> -1
// MAGIC 	) TARGET_MAX

// COMMAND ----------

// DBTITLE 1,OILC Audit: Count of final stage table before insert
// MAGIC %sql
// MAGIC select count(*) from $L2_STAGE.INVOICE_LINE_CHARGE_stage 

// COMMAND ----------

// DBTITLE 1,OILC: Maintain only 1 record with the same composite PK
import org.apache.spark.sql.functions._
import java.util.Calendar

var df_stg = table(L2_STAGE + """.INVOICE_LINE_CHARGE_stage""")
var df_uniq = df_stg.orderBy(col("LAST_EFFECTIVE_TS").desc).coalesce(1).dropDuplicates("first_effective_ts", "INVOICE_LINE_CHARGE_ID")
df_uniq.createOrReplaceTempView("INVOICE_LINE_CHARGE_stage_dedup")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from INVOICE_LINE_CHARGE_stage_dedup

// COMMAND ----------

// DBTITLE 1,OILC Audit: Count of target table before insert
// MAGIC %sql
// MAGIC select count(*) from $L2_ANALYTICS_TABLES.INVOICE_LINE_CHARGE

// COMMAND ----------

// DBTITLE 1,OILC: writing stage data to physical storage for snowflake consumption
dbutils.fs.rm("/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_OILC_stg/", true)
// writing stage data to physical storage for snowflake consumption
df_uniq.write
       .format("delta")
       .save("/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_OILC_stg/")


// COMMAND ----------

// DBTITLE 1,OILC: Final Insert
// MAGIC %sql
// MAGIC MERGE INTO $L2_ANALYTICS_TABLES.INVOICE_LINE_CHARGE  tgt
// MAGIC USING INVOICE_LINE_CHARGE_stage_dedup stg
// MAGIC ON trim(tgt.INVOICE_LINE_CHARGE_ID) = trim(stg.INVOICE_LINE_CHARGE_ID)
// MAGIC and tgt.FIRST_EFFECTIVE_TS = stg.FIRST_EFFECTIVE_TS
// MAGIC WHEN MATCHED AND tgt.LAST_EFFECTIVE_TS != stg.LAST_EFFECTIVE_TS  --if existing record, and there is a new LAST_EFFECTIVE_TS, perform update. If LAST_EFFECTIVE_TS is same, do nothing (no change)
// MAGIC   THEN UPDATE SET tgt.LAST_EFFECTIVE_TS = stg.LAST_EFFECTIVE_TS, tgt.UPDATE_TS = stg.UPDATE_TS 
// MAGIC WHEN NOT MATCHED --if new record, aka, FIRST_EFFECTIVE_TS not found, do an insert
// MAGIC   THEN INSERT * 

// COMMAND ----------

// DBTITLE 1,Audit: Count of target table after insert
// MAGIC %sql
// MAGIC select count(*) from $L2_ANALYTICS_TABLES.INVOICE_LINE_CHARGE

// COMMAND ----------

// DBTITLE 1,Setting Notebook Exit Status to ADF pipeline
dbutils.notebook.exit("Success")
