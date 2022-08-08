// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC create widget text L1_INCREMENT_PATH default "/mnt/data/governed/l1/sterling/order/customer/message/structured_streaming/inbound/";
// MAGIC create widget text L2_ANALYTICS_TABLES default "L2_ANALYTICS_TABLES";
// MAGIC create widget text L2_STAGE default "L2_STAGE";
// MAGIC create widget text LOCK_FILE_PATH default "dbfs:/mnt/data/governed/audit/generic_tag/TAG_FILE";
// MAGIC create widget text LOCK_DIR_PATH default "dbfs:/mnt/data/governed/audit/order_upsert/tag/"; 
// MAGIC create widget text LOG_DIR_PATH default "dbfs:/mnt/data/governed/audit/order_upsert/log/"; 
// MAGIC create widget text MAX_RETRY_CNT default "10"; 
// MAGIC create widget text MAX_RETRY_INTERVAL default "60000"; 
// MAGIC create widget text TAG_FILE_PREFIX default "DTC_ORDER_"; 
// MAGIC create widget text L1_INBOUND_ARCHIVE default "/mnt/data/governed/l1/sterling/order/customer/message/structured_streaming/inbound_archive/";

// COMMAND ----------

// DBTITLE 1,Required Imports,  UDF,  Variables
// MAGIC %scala
// MAGIC 
// MAGIC import spark.implicits._
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.types._
// MAGIC import org.apache.spark.sql._
// MAGIC import scala.util.Try
// MAGIC import com.wsgc.bigdata.PipelineLockUtil._
// MAGIC 
// MAGIC def addColumnToDFIfNotExists(df: DataFrame, path: String):DataFrame = {
// MAGIC   val hasColumn = Try(df(path)).isSuccess
// MAGIC   if(hasColumn){
// MAGIC     df.as("tmpdf").select($"tmpdf.*", col(path).alias(path.substring(path.lastIndexOf(".")+1)))
// MAGIC   }else{
// MAGIC     df.as("tmpdf").select($"tmpdf.*").withColumn(path.substring(path.lastIndexOf(".")+1),lit(null))
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC /////////////
// MAGIC // pipline parameters
// MAGIC /////////////
// MAGIC 
// MAGIC val lockFilePath: String = dbutils.widgets.get("LOCK_FILE_PATH")
// MAGIC val lockDirPath: String = dbutils.widgets.get("LOCK_DIR_PATH")
// MAGIC val logDirPath: String = dbutils.widgets.get("LOG_DIR_PATH")
// MAGIC val retries:Int = dbutils.widgets.get("MAX_RETRY_CNT").toInt
// MAGIC val retryInterval:Long = dbutils.widgets.get("MAX_RETRY_INTERVAL").toLong
// MAGIC val tagFilePrefix: String = dbutils.widgets.get("TAG_FILE_PREFIX")
// MAGIC val L1_ADLS_LOC = dbutils.widgets.get("L1_INCREMENT_PATH")
// MAGIC val L2_STAGE = dbutils.widgets.get("L2_STAGE")
// MAGIC val L2_ANALYTICS_TABLES = dbutils.widgets.get("L2_ANALYTICS_TABLES")
// MAGIC 
// MAGIC //To Operator: if encountering autoBroadcastJoinThreshold exceeded error, set this value to -1, then set back to 52428800 after.
// MAGIC //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) //turning off
// MAGIC spark.conf.set("spark.sql.broadcastTimeout", 3600)
// MAGIC //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760) //default

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
lazy  val keyVaults = KeyVaultAuth(
    userName = None,
    password = None,
    keyVaultParams = Some(keyVaultParams)
  )
lazy  val clientMap: Map[String, String] = KeyVaultUtils(Some(keyVaults)).clientKeyMap
lazy  val OracleUser = clientMap("user")
lazy  val Oracle = clientMap("password")
lazy  val OracleURL = "jdbc:oracle:thin://@yanrep-scan.wsgc.com:1521/yanrep" //using loadbalancer changes on 20210629 
//jdbc:oracle:thin://@10.168.33.30:1521/yanrep" //prod
//val OracleURL = "jdbc:oracle:thin://@yantradr-scan.wsgc.com:1521/yanprddr" //prod
//var OracleURL = "jdbc:oracle:thin://@10.168.33.62:1521/yanmact" //dev
//var OracleUser = "Sterling_prod_support" //dev


// COMMAND ----------

// DBTITLE 1,OH + OL: Clear Previous Stage Tables

spark.sql("""drop table if exists """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA""")

dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/dtc_mergedstage/", true)

spark.sql("""drop table if exists """ + L2_STAGE + """.sterling_increment_stage_delta_ol""")

dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/order_line_stg/", true)

spark.sql("""drop table if exists """ + L2_STAGE + """.STERLING_INCREMENT_STAGE_DELTA_str""")

dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/dtc_mergedstage_str/", true)

spark.sql("""drop table if exists """ + L2_STAGE + """.sterling_increment_stage_delta_ol_str""")

dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/order_line_stg_str/", true)


// COMMAND ----------

// DBTITLE 1,OH: Create 1st Stage Table (21 Table Merge)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA(
// MAGIC         ORDER_HEADER_ID											string       ,
// MAGIC         SOURCE_SYSTEM											string       ,
// MAGIC         ORDER_ID												string       ,
// MAGIC         DTC_ENTRY_TYPE_CD										string       ,
// MAGIC         CONCEPT_CD												string       ,
// MAGIC         TXN_TYPE_CD												string       ,
// MAGIC         CURRENCY_CD												string       ,
// MAGIC         LINKED_ORDER_HEADER_KEY									int          ,
// MAGIC         LINKED_ORDER_HEADER_ID									string       ,
// MAGIC         POS_WORKSTATION_ID										string       ,
// MAGIC         POS_WORKSTATION_SEQ_NBR									int          ,
// MAGIC         RTC_LOCATION_ID											string       ,
// MAGIC         temp_value_order_orderDate								date         ,
// MAGIC         temp_value_order_orderDate_ts							string       ,
// MAGIC         temp_value_order_customerRewardsNo						string       ,
// MAGIC         CUSTOMER_KEY											bigint       ,
// MAGIC         CUSTOMER_ID												string       ,
// MAGIC         MATCH_METHOD_CD											string       ,
// MAGIC         temp_value_systemContext_wsieventTransactionTimeStamp	timestamp    ,
// MAGIC         BILL_TO_FIRST_NAME										string       ,
// MAGIC         BILL_TO_MIDDLE_NAME										string       ,
// MAGIC         BILL_TO_LAST_NAME										string       ,
// MAGIC         BILL_TO_ADDRESS_LINE_1									string       ,
// MAGIC         BILL_TO_ADDRESS_LINE_2									string       ,
// MAGIC         BILL_TO_CITY											string       ,
// MAGIC         BILL_TO_STATE_OR_PROV_CD								string       ,
// MAGIC         BILL_TO_POSTAL_CD										string       ,
// MAGIC         BILL_TO_COUNTRY											string       ,
// MAGIC         BILL_TO_EMAIL_ADDRESS									string       ,
// MAGIC         temp_value_order_personInfoBillTo_mobilePhone			string       ,
// MAGIC         temp_value_order_personInfoBillTo_otherPhone			string       ,
// MAGIC         temp_value_order_personInfoBillTo_dayPhone				string       ,
// MAGIC         temp_value_order_personInfoBillTo_eveningPhone			string       ,
// MAGIC         ORDER_TYPE_CD											string       ,
// MAGIC         CUSTOMER_TYPE_CD										string       ,
// MAGIC         STORE_ORDER_SOURCE										string       ,
// MAGIC         OPERATOR_ID												string       ,
// MAGIC         CANCEL_FLAG												string       ,
// MAGIC         TRAINING_MODE_FLAG										string       ,
// MAGIC         RECEIPT_PREFERENCE										string       ,
// MAGIC         GROSS_AMT												decimal(15,2),
// MAGIC         temp_value_order_overallTotals_lineSubTotal				decimal(15,2),
// MAGIC         temp_value_order_overallTotals_grandCharges				decimal(15,2),
// MAGIC         TAX_AMT													decimal(15,2),
// MAGIC         temp_wsieventName										string       ,
// MAGIC         MARKET_CD               STRING ,                        /* COLUMNS ADDED BY SEPIK */ 
// MAGIC        REGISTRY_ORDER_FLAG     CHAR(1),                        /* COLUMNS ADDED BY SEPIK */ 
// MAGIC        EMPLOYEE_ID             STRING ,                         /* COLUMNS ADDED BY SEPIK */
// MAGIC        RETURN_TYPE_CD          STRING ,                            /* COLUMNS ADDED BY SEPIK*/
// MAGIC        EXCHANGE_ORDER_ID       STRING ,                      /* COLUMNS ADDED BY SEPIK */   
// MAGIC        DOCUMENT_TYPE_CD        STRING ,                        /* COLUMNS ADDED BY SEPIK */ 
// MAGIC        DRAFT_ORDER_FLAG        CHAR(1),                       /* COLUMNS ADDED BY SEPIK */  
// MAGIC        ORDER_PURPOSE           STRING ,                        /* COLUMNS ADDED BY SEPIK */
// MAGIC 	    SOURCE_CODE_DISCOUNT_AMT decimal(15,2),--change by Sutlej1
// MAGIC         GIFTWRAP_WAIVED_FLAG     CHAR(1),--change by Sutlej1
// MAGIC         SHIPPING_WAIVED_FLAG                                    string, /* change by Sutlej1 */  
// MAGIC         CATALOG_NM                                              string,--change by Sutlej1
// MAGIC         CATALOG_YEAR                                            string,  --change by Sutlej1 
// MAGIC 	    REGISTRY_ID                                             string,--change by Sutlej1
// MAGIC         ORDER_SOURCE_TYPE_CD                                    string,--change by Sutlej1 
// MAGIC         GIFT_FLAG                                               CHAR(1),--change by Sutlej1
// MAGIC 	    WAREHOUSE_SITE_CD                                       string,--change by Sutlej1
// MAGIC 	    PAPER_FLAG                                              string,--change by Sutlej1
// MAGIC 	    STORE_ASSOCIATE_NM                                      string,--change by Sutlej1
// MAGIC 	    TENTATIVE_REFUND_AMT                                    string,--change by Sutlej1
// MAGIC 	    CONTACT_FLAG                                            CHAR(1),--change by Sutlej1
// MAGIC 	    RETURN_CARRIER                                          string,--change by Sutlej1
// MAGIC        REGISTRY_TYPE_CD                                        string,--change by Sutlej1 
// MAGIC        RETURN_DELIVERY_HUB     STRING ,                    /* COLUMNS ADDED BY SEPIK */     
// MAGIC        RETURN_MANAGING_HUB     STRING ,                     /* COLUMNS ADDED BY SEPIK */    
// MAGIC        RETURN_CARRIER_CD       STRING ,                       /* COLUMNS ADDED BY SEPIK */  
// MAGIC        RETURN_METHOD_CD        STRING ,                       /* COLUMNS ADDED BY SEPIK */  
// MAGIC        HOUSEHOLD_ID            STRING ,                 /* COLUMNS ADDED BY SEPIK */        
// MAGIC        HOUSEHOLD_KEY           BIGINT ,              /* COLUMNS ADDED BY SEPIK */           
// MAGIC        PAYMENT_STATUS_CD       STRING ,                      /* COLUMNS ADDED BY SEPIK */   
// MAGIC        REFUND_POLICY           STRING                        /* COLUMNS ADDED BY SEPIK */ 
// MAGIC ) USING DELTA LOCATION "dbfs:/mnt/data/governed/l2/stage/order/dtc_mergedstage/" ;
// MAGIC  
// MAGIC  
// MAGIC  
// MAGIC CREATE TABLE IF NOT EXISTS $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_str(
// MAGIC         ORDER_HEADER_ID											string        ,
// MAGIC         SOURCE_SYSTEM											string        ,
// MAGIC         ORDER_ID												string        ,
// MAGIC         ORIG_ORDER_ID                                           String        ,
// MAGIC         DTC_ENTRY_TYPE_CD										string        ,
// MAGIC         CONCEPT_CD												string        ,
// MAGIC         TXN_TYPE_CD												string        ,
// MAGIC         CURRENCY_CD												string        ,
// MAGIC         LINKED_ORDER_HEADER_KEY									int           ,
// MAGIC         LINKED_ORDER_HEADER_ID									string        ,
// MAGIC         POS_WORKSTATION_ID										string        ,
// MAGIC         POS_WORKSTATION_SEQ_NBR									int           ,
// MAGIC         RTC_LOCATION_ID											string        ,
// MAGIC         temp_value_order_orderDate								date          ,
// MAGIC         temp_value_order_orderDate_ts							string        ,																									
// MAGIC         temp_value_order_customerRewardsNo						string        ,
// MAGIC         CUSTOMER_KEY											bigint           ,
// MAGIC         CUSTOMER_ID												string        ,
// MAGIC         LOYALTY_ACCOUNT_ID                                      string        , --CCR Change													  
// MAGIC         MATCH_METHOD_CD											string        ,
// MAGIC         temp_value_systemContext_wsieventTransactionTimeStamp	timestamp     ,
// MAGIC         BILL_TO_FIRST_NAME										string        ,
// MAGIC         BILL_TO_MIDDLE_NAME										string        ,
// MAGIC         BILL_TO_LAST_NAME										string        ,
// MAGIC         BILL_TO_ADDRESS_LINE_1									string        ,
// MAGIC         BILL_TO_ADDRESS_LINE_2									string        ,
// MAGIC         BILL_TO_CITY											string        ,
// MAGIC         BILL_TO_STATE_OR_PROV_CD								string        ,
// MAGIC         BILL_TO_POSTAL_CD										string        ,
// MAGIC         BILL_TO_COUNTRY											string        ,
// MAGIC         BILL_TO_EMAIL_ADDRESS									string        ,
// MAGIC         temp_value_order_personInfoBillTo_mobilePhone			string        ,
// MAGIC         temp_value_order_personInfoBillTo_otherPhone			string        ,
// MAGIC         temp_value_order_personInfoBillTo_dayPhone				string        ,
// MAGIC         temp_value_order_personInfoBillTo_eveningPhone			string        ,
// MAGIC         ORDER_TYPE_CD											string        ,
// MAGIC         CUSTOMER_TYPE_CD										string        ,
// MAGIC         MEMBERSHIP_LEVEL_CD                                     string        ,
// MAGIC         SUBORDERS_CNT                                           int           ,--change by Sutlej
// MAGIC         STORE_ORDER_SOURCE										string        ,
// MAGIC         OPERATOR_ID												string        ,
// MAGIC         CANCEL_FLAG												string        ,
// MAGIC         TRAINING_MODE_FLAG										string        ,
// MAGIC         RECEIPT_PREFERENCE										string        ,
// MAGIC         GROSS_AMT												decimal(15,2) ,
// MAGIC         temp_value_order_overallTotals_lineSubTotal				decimal(15,2) ,
// MAGIC         temp_value_order_overallTotals_grandCharges				decimal(15,2) ,
// MAGIC         TAX_AMT													decimal(15,2) ,
// MAGIC         temp_wsieventName										string        ,
// MAGIC         MARKET_CD               STRING ,                        /* COLUMNS ADDED BY SEPIK */ 
// MAGIC        REGISTRY_ORDER_FLAG     CHAR(1),                        /* COLUMNS ADDED BY SEPIK */ 
// MAGIC        EMPLOYEE_ID             STRING ,                         /* COLUMNS ADDED BY SEPIK */
// MAGIC        RETURN_TYPE_CD          STRING ,                            /* COLUMNS ADDED BY SEPIK*/
// MAGIC        EXCHANGE_ORDER_ID       STRING ,                      /* COLUMNS ADDED BY SEPIK */   
// MAGIC        DOCUMENT_TYPE_CD        STRING ,                        /* COLUMNS ADDED BY SEPIK */ 
// MAGIC        DRAFT_ORDER_FLAG        CHAR(1),                       /* COLUMNS ADDED BY SEPIK */  
// MAGIC        ORDER_PURPOSE           STRING ,                        /* COLUMNS ADDED BY SEPIK */
// MAGIC        SOURCE_CODE_DISCOUNT_AMT                                decimal(15,2)  ,--change by Sutlej1       
// MAGIC        GIFTWRAP_WAIVED_FLAG                                    CHAR(1)       ,--change by Sutlej1
// MAGIC        SHIPPING_WAIVED_FLAG                                    string      ,--change by Sutlej1     
// MAGIC        CATALOG_NM                                              string        ,--change by Sutlej1
// MAGIC        CATALOG_YEAR                                            string            ,--change by Sutlej1
// MAGIC        REGISTRY_ID                                             string        ,--change by Sutlej1
// MAGIC        ORDER_SOURCE_TYPE_CD                                    string       ,--change by Sutlej1
// MAGIC        GIFT_FLAG                                               CHAR(1)      ,--change by Sutlej1
// MAGIC        WAREHOUSE_SITE_CD                                       string       ,--change by Sutlej1
// MAGIC        PAPER_FLAG                                              string       ,--change by Sutlej1
// MAGIC 	   STORE_ASSOCIATE_NM                                      string,--change by Sutlej1
// MAGIC        TENTATIVE_REFUND_AMT                                    string       ,--change by Sutlej1
// MAGIC        CONTACT_FLAG                                            CHAR(1)      ,--change by Sutlej1 
// MAGIC        RETURN_CARRIER                                          string       ,--change by Sutlej1
// MAGIC        REGISTRY_TYPE_CD                                        string        ,--change by Sutlej1  					   
// MAGIC        RETURN_DELIVERY_HUB     STRING ,                    /* COLUMNS ADDED BY SEPIK */     
// MAGIC        RETURN_MANAGING_HUB     STRING ,                     /* COLUMNS ADDED BY SEPIK */    
// MAGIC        RETURN_CARRIER_CD       STRING ,                       /* COLUMNS ADDED BY SEPIK */  
// MAGIC        RETURN_METHOD_CD        STRING ,                       /* COLUMNS ADDED BY SEPIK */  
// MAGIC        HOUSEHOLD_ID            STRING ,                 /* COLUMNS ADDED BY SEPIK */        
// MAGIC        HOUSEHOLD_KEY           BIGINT ,              /* COLUMNS ADDED BY SEPIK */           
// MAGIC        PAYMENT_STATUS_CD       STRING ,                      /* COLUMNS ADDED BY SEPIK */   
// MAGIC        REFUND_POLICY           STRING                        /* COLUMNS ADDED BY SEPIK */ 
// MAGIC ) USING DELTA LOCATION "dbfs:/mnt/data/governed/l2/stage/order/dtc_mergedstage_str/" ;
// MAGIC  
// MAGIC  
// MAGIC  

// COMMAND ----------

// DBTITLE 1,OL: Create 1st Stage Table (21 Table Merge)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS $L2_STAGE.sterling_increment_stage_delta_ol (
// MAGIC       event_type                          string       ,
// MAGIC       transaction_id  				      string       ,
// MAGIC       order_header_id                     string       ,
// MAGIC       doc_type  				          string       ,
// MAGIC       order_line_id  			          string       ,
// MAGIC       source_system  				      string       ,
// MAGIC       prime_line_seq_nbr  			      int          ,
// MAGIC       sub_line_seq_nbr  			      int          ,
// MAGIC       order_dt 						      date         ,
// MAGIC       linked_order_header_id  		      string       ,
// MAGIC       linked_order_line_id  		      string       ,
// MAGIC       order_item_id  				      string       ,
// MAGIC       order_item_type_cd			      string       ,		       
// MAGIC       order_item_name  				      string       ,
// MAGIC       gift_registry_key  			      int          ,		       
// MAGIC       gift_registry_id  			      string       ,
// MAGIC       first_effective_ts  			      timestamp    ,
// MAGIC       last_effective_ts  			      timestamp    ,
// MAGIC       upc_cd  						      string       ,
// MAGIC       kit_cd  						      string       ,
// MAGIC       kit_qty                             int          ,
// MAGIC       product_line  				      string       ,
// MAGIC       bundle_parent_order_line_id  	      string       ,
// MAGIC       order_line_type  				      string       ,
// MAGIC       order_qty  					      int          ,
// MAGIC       orig_order_qty  				      int          ,
// MAGIC       act_sale_unit_price_amt  		      decimal(15,2),
// MAGIC       reg_sale_unit_price_amt  		      decimal(15,2),
// MAGIC       extended_amt  				      decimal(15,2),
// MAGIC       extended_discount_amt  		      decimal(15,2),
// MAGIC       tax_amt  						      decimal(15,2),
// MAGIC       taxable_amt  					      decimal(15,2),
// MAGIC       gift_card_amt  				      decimal(15,2),
// MAGIC       giftwrap_charge_amt  			      decimal(15,2),
// MAGIC       line_total_amt  				      decimal(15,2),
// MAGIC       merchandise_charge_amt  		      decimal(15,2),
// MAGIC       mono_pz_charge_amt  			      decimal(15,2),
// MAGIC       misc_charge_amt  				      decimal(15,2),
// MAGIC       shipping_handling_charge_amt        decimal(15,2),
// MAGIC       shipping_surcharge_amt  		      decimal(15,2),
// MAGIC       donation_amt  				      decimal(15,2),
// MAGIC       associate_id  				      string       ,
// MAGIC       entry_method  				      string       ,
// MAGIC       void_flag  					      string       ,
// MAGIC       repo_flag  					      string       ,
// MAGIC       taxable_flag  				      string       ,
// MAGIC       pickable_flag  				      string       ,
// MAGIC       gift_flag  					      string       ,
// MAGIC       hold_flag  					      string       ,
// MAGIC       hold_reason  					      string       ,
// MAGIC       orig_backorder_flag 			      string       ,
// MAGIC       SUBORDER_COMPLEXITY_GROUP_ID                       string       , --Sutlej																						 
// MAGIC       return_action_cd  			      string       ,
// MAGIC       return_action  				      string       ,
// MAGIC       return_reason_cd  			      string       ,
// MAGIC       return_reason_desc  			      string       ,
// MAGIC       ship_to_first_name  			      string       ,
// MAGIC       ship_to_middle_name  			      string       ,
// MAGIC       ship_to_last_name  			      string       ,
// MAGIC       ship_to_address_line_1  		      string       ,
// MAGIC       ship_to_address_line_2  		      string       ,
// MAGIC       ship_to_city  				      string       ,
// MAGIC       ship_to_state_or_prov  		      string       ,
// MAGIC       ship_to_postal_cd  			      string       ,
// MAGIC       ship_to_country  				      string       ,
// MAGIC       ship_to_email_address  		      string       ,
// MAGIC       ship_to_phone_nbr  			      string       ,
// MAGIC       consolidator_address_cd  		      string       ,
// MAGIC       merge_node_cd  				      string       ,
// MAGIC       ship_node_cd  				      string       ,
// MAGIC       receiving_node_cd 			      string       ,
// MAGIC       level_of_service  			      string       ,
// MAGIC       carrier_service_cd  			      string       ,
// MAGIC       carrier_cd  					      string       ,
// MAGIC       access_point_cd                     string       ,
// MAGIC       access_point_id                     string       ,
// MAGIC       access_point_nm                     string       ,
// MAGIC       minimum_ship_dt 				      date         ,
// MAGIC       requested_ship_dt  			      date         ,
// MAGIC       requested_delivery_dt  		      date         ,
// MAGIC       earliest_schedule_dt  		      date         ,
// MAGIC       earliest_delivery_dt 			      date         ,
// MAGIC       promised_appt_end_dt 			      date         ,
// MAGIC       split_qty  					      int          ,
// MAGIC       shipped_qty 					      int          ,
// MAGIC       fill_qty						      int          ,
// MAGIC       weighted_avg_cost  			      decimal(15,2),
// MAGIC       direct_ship_flag  			      string       ,
// MAGIC       unit_cost  					      decimal(15,2),
// MAGIC       labor_cost  					      decimal(15,2),
// MAGIC       labor_sku  					      string       ,
// MAGIC       gift_message                        string       ,   --changes made to add gift message 
// MAGIC       delivery_choice                     string       ,   --Indus code changes for adding delivery_choice column
// MAGIC       modification_reason_cd              string       ,   --Indus code changes for adding modification_reason_cd column
// MAGIC       modification_reason_cd_desc         string       ,   --Indus code changes for adding modification_reason_cd_desc column
// MAGIC       customer_level_of_service           string        ,   --Indus code changes for adding customer_level_of_service column
// MAGIC       chained_from_order_header_id         string      ,         --sepik
// MAGIC       chained_from_order_line_id          string       ,          --sepik
// MAGIC       delivery_method                     string        ,       --sepik 
// MAGIC       fulfillment_type                    string        ,       --sepik 
// MAGIC       dtc_suborder_nbr                    char(10)      ,    --sepik 
// MAGIC       orig_confirmed_qty                  int           ,   --sepik 
// MAGIC       resku_flag                          char(1)       ,  --sepik
// MAGIC       return_policy	                      string         ,      -- sepik
// MAGIC       return_policy_check_override_flag   char(1)         ,      -- sepik
// MAGIC       product_availability_dt             date           ,    -- sepik
// MAGIC       ecdd_overridden_flag                char(1)        ,    -- sepik
// MAGIC       ecdd_invoked_flag                   char(1)        ,    -- sepik
// MAGIC       ADDITIONAL_LINE_TYPE_CD                    string         ,    --sepik
// MAGIC       vas_gift_wrap_cd				      string		,	   -- sepik
// MAGIC       vas_mono_flag          				  char(1)		,	   -- sepik  
// MAGIC       vas_pz_flag                              char(1)     ,     --sepik
// MAGIC       bo_notification_nbr                 int              --New column added by NILE Team for CYODD Project
// MAGIC ) USING DELTA LOCATION "dbfs:/mnt/data/governed/l2/stage/order/order_line_stg/";
// MAGIC                           
// MAGIC 
// MAGIC  
// MAGIC  
// MAGIC CREATE TABLE IF NOT EXISTS $L2_STAGE.sterling_increment_stage_delta_ol_str (
// MAGIC       event_type                      string       ,
// MAGIC       transaction_id  				  string       ,
// MAGIC       order_header_id                 string       ,
// MAGIC       doc_type  					  string       ,
// MAGIC       order_line_id  				  string       ,
// MAGIC       source_system  				  string       ,
// MAGIC       prime_line_seq_nbr  			  int          ,
// MAGIC       sub_line_seq_nbr  			  int          ,
// MAGIC       order_dt 						  date         ,
// MAGIC       linked_order_header_id  		  string       ,
// MAGIC       linked_order_line_id  		  string       ,
// MAGIC       order_item_id  				  string       ,
// MAGIC       order_item_type_cd			  string       ,		         
// MAGIC       order_item_name  				  string       ,
// MAGIC       gift_registry_key  			  int          ,		         
// MAGIC       gift_registry_id  			  string       ,
// MAGIC       first_effective_ts  			  timestamp    ,
// MAGIC       last_effective_ts  			  timestamp    ,
// MAGIC       upc_cd  						  string       ,
// MAGIC       kit_cd  						  string       ,
// MAGIC       kit_qty                         int          ,
// MAGIC       product_line  				  string       ,
// MAGIC       bundle_parent_order_line_id  	  string       ,
// MAGIC       order_line_type  				  string       ,
// MAGIC       order_qty  					  int          ,
// MAGIC       orig_order_qty  				  int          ,
// MAGIC       act_sale_unit_price_amt  		  decimal(15,2),
// MAGIC       reg_sale_unit_price_amt  		  decimal(15,2),
// MAGIC       extended_amt  				  decimal(15,2),
// MAGIC       extended_discount_amt  		  decimal(15,2),
// MAGIC       tax_amt  						  decimal(15,2),
// MAGIC       taxable_amt  					  decimal(15,2),
// MAGIC       gift_card_amt  				  decimal(15,2),
// MAGIC       giftwrap_charge_amt  			  decimal(15,2),
// MAGIC       line_total_amt  				  decimal(15,2),
// MAGIC       merchandise_charge_amt  		  decimal(15,2),
// MAGIC       mono_pz_charge_amt  			  decimal(15,2),
// MAGIC       misc_charge_amt  				  decimal(15,2),
// MAGIC       shipping_handling_charge_amt    decimal(15,2),
// MAGIC       shipping_surcharge_amt  		  decimal(15,2),
// MAGIC       donation_amt  				  decimal(15,2),
// MAGIC       associate_id  				  string       ,
// MAGIC       entry_method  				  string       ,
// MAGIC       void_flag  					  string       ,
// MAGIC       repo_flag  					  string       ,
// MAGIC       taxable_flag  				  string       ,
// MAGIC       pickable_flag  				  string       ,
// MAGIC       gift_flag  					  string       ,
// MAGIC       hold_flag  					  string       ,
// MAGIC       hold_reason  					  string       ,
// MAGIC       orig_backorder_flag 			  string       ,
// MAGIC       SUBORDER_COMPLEXITY_GROUP_ID                       string       , --Sutlej					 
// MAGIC       return_action_cd  			  string       ,
// MAGIC       return_action  				  string       ,
// MAGIC       return_reason_cd  		      string       ,
// MAGIC       return_reason_desc  			  string       ,
// MAGIC       return_sub_reason_cd            string       ,
// MAGIC       ship_to_first_name  			  string       ,
// MAGIC       ship_to_middle_name  			  string       ,
// MAGIC       ship_to_last_name  			  string       ,
// MAGIC       ship_to_address_line_1  		  string       ,
// MAGIC       ship_to_address_line_2  		  string       ,
// MAGIC       ship_to_city  				  string       ,
// MAGIC       ship_to_state_or_prov  		  string       ,
// MAGIC       ship_to_postal_cd  			  string       ,
// MAGIC       ship_to_country  				  string       ,
// MAGIC       ship_to_email_address  		  string       ,
// MAGIC       ship_to_phone_nbr  			  string       ,
// MAGIC       consolidator_address_cd  		  string       ,
// MAGIC       merge_node_cd  				  string       ,
// MAGIC       ship_node_cd  				  string       ,
// MAGIC       receiving_node_cd 			  string       ,
// MAGIC       level_of_service  			  string       ,
// MAGIC       carrier_service_cd  			  string       ,
// MAGIC       carrier_cd  					  string       ,
// MAGIC       access_point_cd                 string       ,
// MAGIC       access_point_id                 string       ,
// MAGIC       access_point_nm                 string       ,
// MAGIC       minimum_ship_dt 				  date         ,
// MAGIC       requested_ship_dt  			  date         ,
// MAGIC       requested_delivery_dt  		  date         ,
// MAGIC       earliest_schedule_dt  		  date         ,
// MAGIC       earliest_delivery_dt 			  date         ,
// MAGIC       promised_appt_end_dt 			  date         ,
// MAGIC       split_qty  					  int          ,
// MAGIC       shipped_qty 					  int          ,
// MAGIC       fill_qty						  int          ,
// MAGIC       weighted_avg_cost  			  decimal(15,2),
// MAGIC       direct_ship_flag  			  string       ,
// MAGIC       unit_cost  					  decimal(15,2),
// MAGIC       labor_cost  					  decimal(15,2),
// MAGIC       labor_sku  					  string       ,
// MAGIC       gift_message                    string       ,   --changes made to add gift message
// MAGIC       delivery_choice                 string       ,   --Indus code changes for adding delivery_choice column
// MAGIC       modification_reason_cd          string       ,   --Indus code changes for adding modification_reason_cd column
// MAGIC       modification_reason_cd_desc     string       ,   --Indus code changes for adding modification_reason_cd_desc column
// MAGIC       customer_level_of_service       string        ,   --Indus code changes for adding customer_level_of_service column
// MAGIC       chained_from_order_header_id         string      ,         --sepik
// MAGIC       chained_from_order_line_id          string       ,          --sepik
// MAGIC       delivery_method                     string        ,       --sepik 
// MAGIC       fulfillment_type                    string        ,       --sepik 
// MAGIC       dtc_suborder_nbr                    char(10)      ,    --sepik 
// MAGIC       orig_confirmed_qty                  int           ,   --sepik 
// MAGIC       resku_flag                          char(1)       ,  --sepik
// MAGIC       return_policy	                      string         ,      -- sepik
// MAGIC       return_policy_check_override_flag   char(1)         ,      -- sepik
// MAGIC       product_availability_dt             date           ,    -- sepik
// MAGIC       ecdd_overridden_flag                char(1)        ,    -- sepik
// MAGIC       ecdd_invoked_flag                   char(1)        ,    -- sepik
// MAGIC       ADDITIONAL_LINE_TYPE_CD                    string         ,    --sepik
// MAGIC       vas_gift_wrap_cd				      string		,	   -- sepik
// MAGIC       vas_mono_flag          				  char(1)		,	   -- sepik  
// MAGIC       vas_pz_flag                              char(1)     ,     --sepik
// MAGIC       bo_notification_nbr                 int              --New column added by NILE Team for CYODD Project
// MAGIC ) USING DELTA LOCATION "dbfs:/mnt/data/governed/l2/stage/order/order_line_stg_str/";
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC       

// COMMAND ----------

import spark.implicits._
import scala.collection.immutable.{HashMap}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
val inbound_stage_loc: String = dbutils.widgets.get("L1_INBOUND_ARCHIVE")

val now = new DateTime().minusMinutes(10); 
// // 1600624968092

// /mnt/data/governed/l1/sterling/order/customer/message/inbound
val checkPointTS = spark.sql("""select max(CHECKPOINT_VALUE_1) from L1_AUDIT.CHECKPOINT_LOG where extract_id = 'COPY_COV_DTC_STERLING_OMSEVENTS_INBOUND' and CHECKPOINT_TABLE_1 = 'OHOL_PIPELINE' """).first().getString(0).toLong;
val checkPointDateHour = new DateTime(checkPointTS).hourOfDay().roundFloorCopy().getMillis

val currentTS = now.getMillis
val currentDateHour = now.hourOfDay().roundFloorCopy().getMillis

 // arai added / 1000 using without milliseconds
val sql = """ SELECT * , to_date(from_unixtime(arriveDateHour)) as date_new FROM delta.`""" + inbound_stage_loc + 
            """` WHERE arriveDateHour >= """ + checkPointDateHour/1000 + """ and arriveDateHour <= """ + currentDateHour/1000 +
            """ and arriveTS > '""" + checkPointTS/1000 + """' and arriveTS <= '""" + currentTS/1000 + """' order by arriveDateHour, arriveTS"""


  val sourceDF = spark.sql(sql)
  val sourceDFCount = sourceDF.count
  sourceDF.write.partitionBy("eventType", "arriveDateHour").format("delta").mode("append").save(L1_ADLS_LOC) 
  val targetDF = spark.read.format("delta").load(L1_ADLS_LOC)
  val targetDFCount = targetDF.count
  if(sourceDFCount > targetDFCount){
    throw new Exception("Copy to inbound from inbound_stage failed counts do not match")
  }
  println(inbound_stage_loc + ", Completed ")

spark.sql(""" insert into L1_AUDIT.CHECKPOINT_LOG values ('COPY_COV_DTC_STERLING_OMSEVENTS_INBOUND', 'COPY_OMS_EVENTS_FROM_INBOUND_STG_TO_INBOUND', """ + current_timestamp() + """, 'OHOL_PIPELINE', 'arriveTS', """ + currentTS + """ ,'','','','','','') """)



// COMMAND ----------

// DBTITLE 1,OH: L1 to 1st Stage Extract - Sales
spark.sql(s"""INSERT INTO TABLE $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA
SELECT 
  A.ORDER_HEADER_ID									
 ,A.SOURCE_SYSTEM										
 ,A.ORDER_ID											
 ,A.DTC_ENTRY_TYPE_CD									
 ,A.CONCEPT_CD											
 ,B.TXN_TYPE_CD	as TXN_TYPE_CD									
 ,A.CURRENCY_CD										
 ,A.LINKED_ORDER_HEADER_KEY							
 ,A.LINKED_ORDER_HEADER_ID								
 ,A.POS_WORKSTATION_ID									
 ,A.POS_WORKSTATION_SEQ_NBR							
 ,A.RTC_LOCATION_ID									
 ,A.temp_value_order_orderDate							
 ,A.temp_value_order_orderDate_ts						
 ,A.temp_value_order_customerRewardsNo					
 ,A.CUSTOMER_KEY										
 ,A.CUSTOMER_ID										
 ,A.MATCH_METHOD_CD									
 ,A.temp_value_systemContext_wsieventTransactionTimeStamp
 ,A.BILL_TO_FIRST_NAME									
 ,A.BILL_TO_MIDDLE_NAME								
 ,A.BILL_TO_LAST_NAME									
 ,A.BILL_TO_ADDRESS_LINE_1								
 ,A.BILL_TO_ADDRESS_LINE_2								
 ,A.BILL_TO_CITY										
 ,A.BILL_TO_STATE_OR_PROV_CD							
 ,A.BILL_TO_POSTAL_CD									
 ,A.BILL_TO_COUNTRY									
 ,A.BILL_TO_EMAIL_ADDRESS								
 ,A.temp_value_order_personInfoBillTo_mobilePhone		
 ,A.temp_value_order_personInfoBillTo_otherPhone		
 ,A.temp_value_order_personInfoBillTo_dayPhone			
 ,A.temp_value_order_personInfoBillTo_eveningPhone		
 ,A.ORDER_TYPE_CD										
 ,A.CUSTOMER_TYPE_CD									
 ,A.STORE_ORDER_SOURCE									
 ,A.OPERATOR_ID										
 ,A.CANCEL_FLAG										
 ,A.TRAINING_MODE_FLAG									
 ,A.RECEIPT_PREFERENCE									
 ,A.GROSS_AMT											
 ,A.temp_value_order_overallTotals_lineSubTotal		
 ,A.temp_value_order_overallTotals_grandCharges		
 ,A.TAX_AMT											
 ,A.temp_wsieventName									     
 ,CASE WHEN A.ORDER_TYPE_CD in ('OTHERS_CA','DES_STUDIO') THEN 'CAN'
  ELSE A.MARKET_CD
  END AS MARKET_CD                         --SEPIK
 ,A.REGISTRY_ORDER_FLAG                   --SEPIK               
 ,A.EMPLOYEE_ID                           --SEPIK               
 ,A.RETURN_TYPE_CD                        --SEPIK               
 ,A.EXCHANGE_ORDER_ID                     --SEPIK               
 ,A.DOCUMENT_TYPE_CD                      --SEPIK               
 ,A.DRAFT_ORDER_FLAG                       --SEPIK              
 ,A.ORDER_PURPOSE                          --SEPIK                     
 ,A.SOURCE_CODE_DISCOUNT_AMT                --Sutlej change from here       
 ,A.GIFTWRAP_WAIVED_FLAG
 ,A.SHIPPING_WAIVED_FLAG                   
 ,A.CATALOG_NM
 ,A.CATALOG_YEAR
 ,A.REGISTRY_ID
 ,A.ORDER_SOURCE_TYPE_CD
 ,A.GIFT_FLAG
 ,A.WAREHOUSE_SITE_CD
 ,A.PAPER_FLAG
 ,A.STORE_ASSOCIATE_NM
 ,A.TENTATIVE_REFUND_AMT               
 ,A.CONTACT_FLAG
 ,A.RETURN_CARRIER                         --Sutlej Change till here
 ,A.REGISTRY_TYPE_CD  																		   
 ,A.RETURN_DELIVERY_HUB                    --SEPIK              
 ,A.RETURN_MANAGING_HUB                    --SEPIK              
 ,A.RETURN_CARRIER_CD                      --SEPIK              
 ,A.RETURN_METHOD_CD                        --SEPIK             
 ,A.HOUSEHOLD_ID                           --SEPIK              
 ,A.HOUSEHOLD_KEY                          --SEPIK              
 ,A.PAYMENT_STATUS_CD                      --SEPIK              
 ,A.REFUND_POLICY                           --SEPIK             
from
(
select
event.value.order.orderHeaderKey as ORDER_HEADER_ID
,'STERLING_DTC' as SOURCE_SYSTEM
,event.value.order.orderNo as ORDER_ID
,event.value.order.entryType as DTC_ENTRY_TYPE_CD
,event.value.order.enterpriseCode as CONCEPT_CD
--,B.TXN_TYPE_CD as TXN_TYPE_CD
,event.value.order.priceInfo.currency as CURRENCY_CD
,null as LINKED_ORDER_HEADER_KEY
,null as LINKED_ORDER_HEADER_ID
,null as POS_WORKSTATION_ID
,null as POS_WORKSTATION_SEQ_NBR
,event.value.order.extn.extnStoreNumber as RTC_LOCATION_ID
,event.value.order.orderDate as temp_value_order_orderDate
,event.value.order.orderDate as temp_value_order_orderDate_ts
,event.value.order.customerRewardsNo as temp_value_order_customerRewardsNo
,null as CUSTOMER_KEY
,null as CUSTOMER_ID
,null as MATCH_METHOD_CD
,event.value.systemContext.wsieventTransactionTimeStamp as temp_value_systemContext_wsieventTransactionTimeStamp
,event.value.order.personInfoBillTo.firstName as BILL_TO_FIRST_NAME
,event.value.order.personInfoBillTo.middleName as BILL_TO_MIDDLE_NAME
,event.value.order.personInfoBillTo.lastName as BILL_TO_LAST_NAME
,event.value.order.personInfoBillTo.addressLine1 as BILL_TO_ADDRESS_LINE_1
,event.value.order.personInfoBillTo.addressLine2 as BILL_TO_ADDRESS_LINE_2
,event.value.order.personInfoBillTo.city as BILL_TO_CITY
,event.value.order.personInfoBillTo.state as BILL_TO_STATE_OR_PROV_CD
,event.value.order.personInfoBillTo.zipCode as BILL_TO_POSTAL_CD
,event.value.order.personInfoBillTo.country as BILL_TO_COUNTRY
,event.value.order.personInfoBillTo.emailID as BILL_TO_EMAIL_ADDRESS
,event.value.order.personInfoBillTo.mobilePhone as temp_value_order_personInfoBillTo_mobilePhone
,event.value.order.personInfoBillTo.otherPhone as temp_value_order_personInfoBillTo_otherPhone
,event.value.order.personInfoBillTo.dayPhone as temp_value_order_personInfoBillTo_dayPhone
,event.value.order.personInfoBillTo.eveningPhone as temp_value_order_personInfoBillTo_eveningPhone
,event.value.order.orderType as ORDER_TYPE_CD
,event.value.order.extn.extnCustomerType as CUSTOMER_TYPE_CD
,event.value.order.extn.extnStoreOrdSource as STORE_ORDER_SOURCE
,event.value.order.enteredBy as OPERATOR_ID
,null as CANCEL_FLAG
,null as TRAINING_MODE_FLAG
,'Email' as RECEIPT_PREFERENCE
,event.value.order.overallTotals.grandTotal as GROSS_AMT
,event.value.order.overallTotals.lineSubTotal as temp_value_order_overallTotals_lineSubTotal
,event.value.order.overallTotals.grandCharges as temp_value_order_overallTotals_grandCharges
,event.value.order.overallTotals.grandTax as TAX_AMT
,event.value.systemContext.wsieventName as temp_wsieventName
,Coalesce(event.value.order.extn.extnMarketCode,'USA') AS MARKET_CD --start
,exploded.extnIsRegistryOrder AS REGISTRY_ORDER_FLAG 
,exploded.extnEmployeeID AS EMPLOYEE_ID
,event.value.order.extn.extnReturnType AS RETURN_TYPE_CD
,event.value.order.extn.extnExchangeOrderNo AS EXCHANGE_ORDER_ID
,event.value.order.documentType AS DOCUMENT_TYPE_CD
,event.value.order.draftOrderFlag AS DRAFT_ORDER_FLAG
,(event.value.order.orderPurpose) AS ORDER_PURPOSE
,exploded.extnSourceCodeDiscount as SOURCE_CODE_DISCOUNT_AMT  --SUTLEJ CHANGE FROM HERE
,exploded.extnIsGiftWrapWaived as GIFTWRAP_WAIVED_FLAG
,exploded.extnIsShippingWaived as SHIPPING_WAIVED_FLAG
,exploded.extnCatalog as CATALOG_NM      
,exploded.extnCatalogYear as CATALOG_YEAR                   
,exploded.extnRegistryID as REGISTRY_ID
,exploded.extnOrderSourceType as ORDER_SOURCE_TYPE_CD
,exploded.extnGiftIndicator as GIFT_FLAG
,exploded.extnWarehouseSiteCode as WAREHOUSE_SITE_CD
,exploded.extnPaperFlag as PAPER_FLAG
,exploded.extnStoreAssociateName as STORE_ASSOCIATE_NM
,exploded.extnTentativeRefundAmount as TENTATIVE_REFUND_AMT          
,exploded.extnContactFlag as CONTACT_FLAG
,exploded.extnReturnCarrier as RETURN_CARRIER                --SUTLEJ CHANGE TILL HERE
,exploded.extnRegistryType as REGISTRY_TYPE_CD																	  
,exploded.extnReturnDeliveryHub AS RETURN_DELIVERY_HUB
,exploded.extnReturnManagingHub AS RETURN_MANAGING_HUB
,exploded.extnReturnCarrier AS RETURN_CARRIER_CD
,event.value.order.extn.extnReturnMethod AS RETURN_METHOD_CD
,NULL AS HOUSEHOLD_ID
,NULL AS HOUSEHOLD_KEY
,NULL AS PAYMENT_STATUS_CD   
,event.value.order.extn.extnRefundPolicy AS REFUND_POLICY --End Sepik changes
FROM delta.`$L1_ADLS_LOC`
LATERAL VIEW explode(event.value.order.extn.wsiaddnlOrderDataList.wsiaddnlOrderData) event_table as exploded
where eventType <> 'DSPOSHIPUPDATE') A
inner join L2_ANALYTICS.TXN_TYPE B
on B.DTC_DOCUMENT_ID = A.DOCUMENT_TYPE_CD
and A.DOCUMENT_TYPE_CD in ('0001','0003') 
and A.ORDER_HEADER_ID is not null """)


// COMMAND ----------

// DBTITLE 1,OH: L1 to 1st Stage Extract - Returns
spark.sql(s"""
INSERT INTO TABLE $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA
select
A.ORDER_HEADER_ID
,'STERLING_DTC' as SOURCE_SYSTEM
,A.ORDER_ID
,null as DTC_ENTRY_TYPE_CD
,A.CONCEPT_CD
,B.TXN_TYPE_CD as TXN_TYPE_CD
,'' as CURRENCY_CD
,null as LINKED_ORDER_HEADER_KEY
,null as LINKED_ORDER_HEADER_ID
,null as POS_WORKSTATION_ID
,null as POS_WORKSTATION_SEQ_NBR
,null as RTC_LOCATION_ID
,A.temp_value_order_orderDate
,A.temp_value_order_orderDate_ts
,null as temp_value_order_customerRewardsNo
,null as CUSTOMER_KEY
,null as CUSTOMER_ID
,null as MATCH_METHOD_CD
,A.temp_value_systemContext_wsieventTransactionTimeStamp
,null as BILL_TO_FIRST_NAME
,null as BILL_TO_MIDDLE_NAME
,null as BILL_TO_LAST_NAME
,null as BILL_TO_ADDRESS_LINE_1
,null as BILL_TO_ADDRESS_LINE_2
,null as BILL_TO_CITY
,null as BILL_TO_STATE_OR_PROV_CD
,null as BILL_TO_POSTAL_CD
,null as BILL_TO_COUNTRY
,null as BILL_TO_EMAIL_ADDRESS
,null as temp_value_order_personInfoBillTo_mobilePhone
,null as temp_value_order_personInfoBillTo_otherPhone
,null as temp_value_order_personInfoBillTo_dayPhone
,null as temp_value_order_personInfoBillTo_eveningPhone
,null as ORDER_TYPE_CD
,null as CUSTOMER_TYPE_CD
,null as STORE_ORDER_SOURCE
,A.OPERATOR_ID
,null as CANCEL_FLAG
,null as TRAINING_MODE_FLAG
,null as RECEIPT_PREFERENCE
,null as GROSS_AMT
,null as temp_value_order_overallTotals_lineSubTotal
,null as temp_value_order_overallTotals_grandCharges
,null as TAX_AMT
,A.temp_wsieventName 
,A.MARKET_CD                                    /* COLUMNS ADDED BY SEPIK */
,NULL AS REGISTRY_ORDER_FLAG                          /* COLUMNS ADDED BY SEPIK */
,NULL AS EMPLOYEE_ID                                  /* COLUMNS ADDED BY SEPIK */
,NULL AS RETURN_TYPE_CD                                   /* COLUMNS ADDED BY SEPIK */
,NULL AS EXCHANGE_ORDER_ID                            /* COLUMNS ADDED BY SEPIK */
,A.DOCUMENT_TYPE_CD                                 /* COLUMNS ADDED BY SEPIK */
,NULL AS DRAFT_ORDER_FLAG                               /* COLUMNS ADDED BY SEPIK */
,NULL AS ORDER_PURPOSE                                      /* COLUMNS ADDED BY SEPIK */
,null as SOURCE_CODE_DISCOUNT_AMT             /* SUTLEJ CHANGE FROM HERE */
,null as GIFTWRAP_WAIVED_FLAG
,null as SHIPPING_WAIVED_FLAG                   
,null as CATALOG_NM
,null as CATALOG_YEAR
,null as REGISTRY_ID
,null as ORDER_SOURCE_TYPE_CD
,null as GIFT_FLAG
,null as WAREHOUSE_SITE_CD
,null as PAPER_FLAG
,null as STORE_ASSOCIATE_NM
,null as TENTATIVE_REFUND_AMT
,null as CONTACT_FLAG
,null as RETURN_CARRIER                    /* SUTLEJ CHANGE TILL HERE */
,null as REGISTRY_TYPE_CD																		
,NULL AS RETURN_DELIVERY_HUB                             /* COLUMNS ADDED BY SEPIK */
,NULL AS RETURN_MANAGING_HUB                              /* COLUMNS ADDED BY SEPIK */
,NULL AS RETURN_CARRIER_CD                                   /* COLUMNS ADDED BY SEPIK */
,NULL AS RETURN_METHOD_CD                                     /* COLUMNS ADDED BY SEPIK */
,NULL AS HOUSEHOLD_ID                                    /* COLUMNS ADDED BY SEPIK */
,NULL AS HOUSEHOLD_KEY                                /* COLUMNS ADDED BY SEPIK */
,NULL AS PAYMENT_STATUS_CD                                    /* COLUMNS ADDED BY SEPIK */
,NULL AS REFUND_POLICY                                 /* COLUMNS ADDED BY SEPIK */
FROM 
(SELECT 
exploded.orderHeaderKey as ORDER_HEADER_ID
,exploded.documentType as DOCUMENT_TYPE_CD
,exploded.orderNo as ORDER_ID
,exploded.enterpriseCode as CONCEPT_CD
,exploded.orderDate as temp_value_order_orderDate
,exploded.orderDate as temp_value_order_orderDate_ts
,event.value.systemContext.wsieventTransactionTimeStamp as temp_value_systemContext_wsieventTransactionTimeStamp
,exploded.enteredBy as OPERATOR_ID
,event.value.systemContext.wsieventName as temp_wsieventName 
,event.value.order.orderHeaderKey as so_orderHeaderKey
,event.value.order.extn.extnMarketCode AS MARKET_CD
FROM delta.`$L1_ADLS_LOC` A 
LATERAL VIEW explode(event.value.order.returnOrders.returnOrder) event_table as exploded
) A
inner join L2_ANALYTICS.TXN_TYPE B
on B.DTC_DOCUMENT_ID = A.DOCUMENT_TYPE_CD
and A.DOCUMENT_TYPE_CD in ('0001','0003') 
and A.ORDER_HEADER_ID is not null  
and A.so_orderHeaderKey != A.ORDER_HEADER_ID
""")

// COMMAND ----------

// DBTITLE 1,OL: L1 to Stage Extract - Sales
// MAGIC %scala
// MAGIC 
// MAGIC  
// MAGIC   val sourceDF = spark.read.format("delta").option("inferSchema", "true").load("""dbfs:""" + L1_ADLS_LOC + """/""").filter($"eventType" =!= "DSPOSHIPUPDATE").withColumn("last_effective_ts", to_timestamp(lit("9999-12-31 23:59:59.000+0000"),"yyyy-MM-dd HH:mm:ss.SSS")).withColumn("source_system",lit("STERLING_DTC")).withColumn("event_type",$"eventType") 
// MAGIC   
// MAGIC   val sourceOLExplDF = sourceDF.select($"event.value.systemContext.wsieventTransactionID".alias("trans_id"), $"event.value.order.orderHeaderKey".alias("ohid"), $"event.value.order.documentType".alias("document_type"),
// MAGIC                                        to_date($"event.value.order.orderDate","yyyy-MM-dd").alias("order_dt"),
// MAGIC                                        to_timestamp($"event.value.systemContext.wsieventTransactionTimeStamp").alias("first_effective_ts"), $"last_effective_ts",
// MAGIC                                        $"source_system", 
// MAGIC                                        $"event_type",
// MAGIC                                        explode_outer($"event.value.order.orderLines.orderLine").alias("ol")
// MAGIC                                       )
// MAGIC   //println("Processing " + schemaType + " with count " + sourceOLExplDF.count)
// MAGIC   
// MAGIC   val sourceAddlOlDataDF = sourceOLExplDF.select($"trans_id", $"ohid", $"ol.orderLineKey".alias("olid"), explode_outer($"ol.extn.wsiaddnlOrderLineDataList.wsiaddnlOrderLineData").alias("ol_ext_data"))
// MAGIC                                            .select($"trans_id", $"ohid", $"olid", $"ol_ext_data.extnOrderLineKey", $"ol_ext_data.extnWAC".alias("extnWAC"), $"ol_ext_data.extnActionCode".alias("extnActionCode"), $"ol_ext_data.extnLaborCost".alias("extnLaborCost"), $"ol_ext_data.extnModificationReasonCode".alias("extnModificationReasonCode"), $"ol_ext_data.extnModificationReasonCodeDesc".alias("extnModificationReasonCodeDesc"))
// MAGIC 
// MAGIC val sourceVasDataDF = sourceOLExplDF.select($"trans_id", $"ohid", $"ol.orderLineKey".alias("olid"), explode_outer($"ol.extn.wsivasdataList.wsivasdata").alias("ol_ext_vas_data"))
// MAGIC                                            .select($"trans_id", $"ohid", $"olid", $"ol_ext_vas_data.extnVASGiftWrap".alias("extnVASGiftWrap"), $"ol_ext_vas_data.extnVASMono".alias("extnVASMono"),$"ol_ext_vas_data.extnVASPZ".alias("extnVASPZ")) //SEPIK
// MAGIC                  
// MAGIC   val sourceLineChargeDF = sourceOLExplDF.select($"trans_id", $"ohid", $"ol.orderLineKey".alias("olid"), explode_outer($"ol.LineCharges.linecharge").alias("ol_lCharge"))
// MAGIC                                   .select($"trans_id", $"ohid", $"olid",
// MAGIC                                           when($"ol_lCharge.chargeName" === lit("GiftWrap")            && $"ol_lCharge.chargeCategory" === lit("LineInfo"),    $"ol_lCharge.chargeAmount".cast(DecimalType(15,2))).otherwise(lit(0.0).cast(DecimalType(15,2))).alias("giftwrap_charge_amt"),
// MAGIC                                           when($"ol_lCharge.chargeName" === lit("LineMerchEffective")  && $"ol_lCharge.chargeCategory" === lit("LineInfo"),    $"ol_lCharge.chargeAmount".cast(DecimalType(15,2))).otherwise(lit(0.0).cast(DecimalType(15,2))).alias("merchandise_charge_amt"),
// MAGIC                                           when($"ol_lCharge.chargeName" === lit("LineMonoPZEffective") && $"ol_lCharge.chargeCategory" === lit("LineCharges"), $"ol_lCharge.chargeAmount".cast(DecimalType(15,2))).otherwise(lit(0.0).cast(DecimalType(15,2))).alias("mono_pz_charge_amt"),
// MAGIC                                           when($"ol_lCharge.chargeName" === lit("Shipping")            && $"ol_lCharge.chargeCategory" === lit("LineInfo"),    $"ol_lCharge.chargeAmount".cast(DecimalType(15,2))).otherwise(lit(0.0).cast(DecimalType(15,2))).alias("shipping_handling_charge_amt"),
// MAGIC                                           when($"ol_lCharge.chargeName" === lit("ShippingSurcharge")   && $"ol_lCharge.chargeCategory" === lit("LineInfo"),    $"ol_lCharge.chargeAmount".cast(DecimalType(15,2))).otherwise(lit(0.0).cast(DecimalType(15,2))).alias("shipping_surcharge_amt")
// MAGIC                                    ).groupBy("trans_id", "ohid","olid")
// MAGIC                                          .agg(max($"giftwrap_charge_amt").alias("giftwrap_charge_amt"),
// MAGIC                                               max($"merchandise_charge_amt").alias("merchandise_charge_amt"),
// MAGIC                                               max($"mono_pz_charge_amt").alias("mono_pz_charge_amt"),
// MAGIC                                               max($"shipping_handling_charge_amt").alias("shipping_handling_charge_amt"),
// MAGIC                                               max($"shipping_surcharge_amt").alias("shipping_surcharge_amt")
// MAGIC                                   )
// MAGIC 
// MAGIC   val sourceScheduleDF = sourceOLExplDF.select($"trans_id", $"ohid", $"ol.orderLineKey".alias("olid"), explode_outer($"ol.schedules.schedule").alias("ol_schedule"))
// MAGIC                               .select($"trans_id", $"ohid", $"olid", $"ol_schedule.orderHeaderKey".alias("ohid_sch"), $"ol_schedule.orderLineKey".alias("olid_sch"), $"ol_schedule.mergeNode".alias("olid_mergeNode"), $"ol_schedule.orderLineScheduleKey")
// MAGIC                               .groupBy("trans_id", "ohid", "olid", "ohid_sch","olid_sch", "olid_mergeNode").agg(max($"orderLineScheduleKey"))
// MAGIC 
// MAGIC   /** Gift Message derivation  COL-4529 **/ 
// MAGIC   val sourceGiftDF = sourceOLExplDF.select(
// MAGIC     $"trans_id",
// MAGIC     $"ohid",
// MAGIC     $"ol.orderLineKey".alias("olid"),
// MAGIC     explode_outer($"ol.instructions.instruction").alias("insructions"))
// MAGIC   .select(
// MAGIC     $"trans_id",
// MAGIC     $"ohid",
// MAGIC     $"olid",
// MAGIC     $"insructions.instructionType".alias("INSTRUCTION_TYPE"),
// MAGIC     $"insructions.instructionText".alias("INSTRUCTION_TEXT"))
// MAGIC   .filter($"INSTRUCTION_TYPE" === "VAS_GMSG")
// MAGIC   .groupBy("trans_id", "ohid", "olid")
// MAGIC   .agg(max($"INSTRUCTION_TEXT").alias("INSTRUCTION_TEXT"))
// MAGIC   /* ------------------------ */
// MAGIC   
// MAGIC   val preStg1OLDF1 = addColumnToDFIfNotExists(sourceOLExplDF, "ol.derivedFromOrderHeaderKey")
// MAGIC   
// MAGIC   val preStg1OLDF2 = addColumnToDFIfNotExists(preStg1OLDF1, "ol.derivedFromOrderLineKey")
// MAGIC   
// MAGIC   val preStg1OLDF3 = addColumnToDFIfNotExists(preStg1OLDF2, "ol.extn.extnIsBackOrderAtCreate")
// MAGIC 
// MAGIC   val preStg1OLDF4 = addColumnToDFIfNotExists(preStg1OLDF3, "ol.extn.extnGroupID") //Sutlej																									
// MAGIC   
// MAGIC   val sourceOLDF = preStg1OLDF4.select(
// MAGIC                                        $"trans_id",
// MAGIC                                        $"ohid",
// MAGIC                                        $"document_type",
// MAGIC                                        $"ol.orderLineKey".alias("order_line_id"),
// MAGIC                                        $"source_system", 
// MAGIC                                        $"ol.primeLineNo".alias("prime_line_seq_nbr"),
// MAGIC                                        $"ol.subLineNo".alias("sub_line_seq_nbr"),
// MAGIC                                        //order_header_key
// MAGIC                                        $"order_dt",
// MAGIC                                        // linked_order_header_key                                               // lookup L2 - Order_header.Order_header_key
// MAGIC                                        $"derivedFromOrderHeaderKey".alias("linked_order_header_id"),       
// MAGIC                                        // linked_order_line_key                                                 // lookup L2 - Order_line.Order_line_key
// MAGIC                                        $"derivedFromOrderLineKey".alias("linked_order_line_id"),          
// MAGIC                                        // item_key                                                              // lookup L2 - item table ol.item.itemid = item_id else -1
// MAGIC                                        $"ol.item.itemid".alias("order_item_id"),
// MAGIC                                        // order_item_type_cd                                                    // Add this column use withColumn lit(null)
// MAGIC                                        $"ol.item.itemDesc".alias("order_item_name"),
// MAGIC                                        // gift_registry_key                                                     // Add this column use withColumn lit(null)
// MAGIC                                        $"ol.extn.extnGiftRegistryID".alias("gift_registry_id"),
// MAGIC                                        $"ol.extn.extnDeliveryChoiceID".alias("delivery_choice"),             // Indus code changes for delivery_choice
// MAGIC                                        $"ol.extn.extnCustLOS".alias("customer_level_of_service"),            // Indus code changes for customer_level_of_service
// MAGIC                                        $"ol.extn.extnBONotificationNo".alias("bo_notification_nbr"),                // Added by Nite Team for CYODD project 
// MAGIC                                        $"first_effective_ts",
// MAGIC                                        $"last_effective_ts",
// MAGIC                                        $"ol.item.UPCCode".alias("upc_cd"),
// MAGIC                                        $"ol.KitCode".alias("kit_cd"),
// MAGIC                                        $"ol.kitQty".alias("kit_qty"),
// MAGIC                                        $"ol.item.ProductLine".alias("product_line"),
// MAGIC                                        $"ol.bundleParentOrderLineKey".alias("bundle_parent_order_line_id"),       
// MAGIC                                        $"ol.lineType".alias("order_line_type"),
// MAGIC                                        $"ol.orderedQty".alias("order_qty"),
// MAGIC                                        $"ol.originalOrderedQty".alias("orig_order_qty"),
// MAGIC                                        $"ol.linePriceInfo.UnitPrice".alias("act_sale_unit_price_amt"),
// MAGIC                                        $"ol.linePriceInfo.RetailPrice".alias("reg_sale_unit_price_amt"),
// MAGIC                                        $"ol.lineOverallTotals.extendedPrice".alias("extended_amt"),
// MAGIC                                        $"ol.lineOverallTotals.discount".alias("extended_discount_amt"),
// MAGIC                                        $"ol.lineOverallTotals.tax".alias("tax_amt"),
// MAGIC                                        $"ol.lineOverallTotals.lineTotalWithoutTax".alias("taxable_amt"),
// MAGIC                                        when(lower($"ol.shipNode") === lit("gcwh") && lower($"ol.itemDetails.extn.extnNonMerchType") === lit("nmerch"), $"ol.LinePriceInfo.UnitPrice").otherwise(0.0).alias("gift_card_amt"),
// MAGIC                                        // giftwrap_charge_amt                                                           // Join with sourceLineChargeDF provide this
// MAGIC                                        $"ol.LineOverallTotals.LineTotal".alias("line_total_amt"),
// MAGIC                                        // merchandise_charge_amt                                                        // Join with sourceLineChargeDF provide this
// MAGIC                                        // mono_pz_charge_amt                                                            // Join with sourceLineChargeDF provide this 
// MAGIC                                        // misc_charge_amt                                                           // Add this column use withColumn lit(null)
// MAGIC                                        // shipping_handling_charge_amt                                                  // Join with sourceLineChargeDF provide this
// MAGIC                                        // shipping_surcharge_amt                                                        // Join with sourceLineChargeDF provide this 
// MAGIC                                        // donation_amt                                                              // Add this column use withColumn lit(null)
// MAGIC                                        // associate_id                                                              // Add this column use withColumn lit(null)
// MAGIC                                        // entry_method                                                              // Add this column use withColumn lit("Online")
// MAGIC                                        // void_flag                                                                 // Add this column use withColumn lit(null)
// MAGIC                                        $"ol.extn.extnRepoFlag".alias("repo_flag"),
// MAGIC                                        $"ol.linePriceInfo.taxableFlag".alias("taxable_flag"),
// MAGIC                                        $"ol.pickableFlag".alias("pickable_flag"),
// MAGIC                                        $"ol.giftFlag".alias("gift_flag"),
// MAGIC                                        $"ol.holdFlag".alias("hold_flag"),
// MAGIC                                        $"ol.holdReasonCode".alias("hold_reason"),
// MAGIC                                        $"extnIsBackOrderAtCreate".alias("orig_backorder_flag"),
// MAGIC                                        $"extnGroupID".alias("SUBORDER_COMPLEXITY_GROUP_ID"),                                    //Sutlej
// MAGIC                                        // return_action_cd                                                           // Join with sourceAddlOlDataDF provide this
// MAGIC                                        // return_action                                                              // Add this column use withColumn lit(null)
// MAGIC                                        $"ol.returnReason".alias("return_reason_cd"),
// MAGIC                                        // return_reason_desc                                                         // Add this column use withColumn lit(null) 
// MAGIC                                        $"ol.personInfoShipTo.firstName".alias("ship_to_first_name"),
// MAGIC                                        $"ol.personInfoShipTo.middleName".alias("ship_to_middle_name"),
// MAGIC                                        $"ol.personInfoShipTo.lastName".alias("ship_to_last_name"),
// MAGIC                                        $"ol.personInfoShipTo.addressLine1".alias("ship_to_address_line_1"),
// MAGIC                                        $"ol.personInfoShipTo.addressLine2".alias("ship_to_address_line_2"),
// MAGIC                                        $"ol.personInfoShipTo.city".alias("ship_to_city"),
// MAGIC                                        $"ol.personInfoShipTo.state".alias("ship_to_state_or_prov"),
// MAGIC                                        $"ol.personInfoShipTo.zipCode".alias("ship_to_postal_cd"),
// MAGIC                                        $"ol.personInfoShipTo.country".alias("ship_to_country"),
// MAGIC                                        $"ol.personInfoShipTo.eMailID".alias("ship_to_email_address"),
// MAGIC                                        //$"ol.personInfoShipTo.mobilePhone".alias("ship_to_phone_nbr"), //code change by NILE Team
// MAGIC                                        when($"ol.personInfoShipTo.mobilePhone" =!= lit(""), $"ol.personInfoShipTo.mobilePhone").
// MAGIC                                        when($"ol.personInfoShipTo.dayPhone" =!= lit(""), $"ol.personInfoShipTo.dayPhone").
// MAGIC                                        when($"ol.personInfoShipTo.eveningPhone" =!= lit(""), $"ol.personInfoShipTo.eveningPhone").
// MAGIC                                        when($"ol.personInfoShipTo.otherPhone" =!= lit(""), $"ol.personInfoShipTo.otherPhone").alias("ship_to_phone_nbr"),
// MAGIC 
// MAGIC                                        $"ol.extn.extnConsolidatorAddressCode".alias("consolidator_address_cd"),
// MAGIC                                        // merge_node_cd                                                                   // Join with sourceLineChargeDF provide this
// MAGIC                                        $"ol.shipNode".alias("ship_node_cd"),
// MAGIC                                        $"ol.receivingNode".alias("receiving_node_cd"),                            // Added as part of SS
// MAGIC                                        $"ol.levelOfService".alias("level_of_service"),
// MAGIC                                        $"ol.carrierServiceCode".alias("carrier_service_cd"),
// MAGIC                                        $"ol.scac".alias("carrier_cd"),
// MAGIC                                        
// MAGIC                                        // upsap change start
// MAGIC                                        $"ol.personInfoShipTo.addressID".alias("access_point_cd"),
// MAGIC                                        when(lower($"ol.personInfoShipTo.addressID") === lit("upsap") || lower($"ol.personInfoShipTo.addressID") === lit("upsap_alt"), $"ol.personInfoShipTo.addressLine6").otherwise(lit(null)).alias("access_point_id"),
// MAGIC                                        //$"ol.personInfoShipTo.addressLine6".alias("access_point_id"),
// MAGIC                                        when(lower($"ol.personInfoShipTo.addressID") === lit("upsap") || lower($"ol.personInfoShipTo.addressID") === lit("upsap_alt"), $"ol.personInfoShipTo.company").otherwise(lit(null)).alias("access_point_nm"),
// MAGIC                                        // $"ol.personInfoShipTo.company".alias("access_point_nm"),
// MAGIC                                        // upsap change end
// MAGIC     
// MAGIC                                        //  $"ol.earliestShipDate".alias("minimum_ship_dt"),                          // Add this column use withColumn lit(null)
// MAGIC                                        // requested_ship_dt                                                          // Add this column use withColumn lit(null)
// MAGIC                                        // requested_delivery_dt                                                      // Add this column use withColumn lit(null)
// MAGIC                                        // earliest_schedule_dt                                                       // Add this column use withColumn lit(null)
// MAGIC                                        // $"ol.EarliestDeliveryDate".alias("earliest_delivery_dt"),                  // Add this column use withColumn lit(null)
// MAGIC                                        // promised_appt_end_dt                                                       // Add this column use withColumn lit(null) 
// MAGIC                                        $"ol.splitQty".alias("split_qty"),
// MAGIC                                          $"ol.shippedQuantity".alias("shipped_qty"),                               //Added as part of SS
// MAGIC                                        $"ol.fillQuantity".alias("fill_qty"),                                     // Added as part of SS
// MAGIC                                        // weighted_avg_cost                                                          // Join with sourceAddlOlDataDF provide this
// MAGIC                                        $"ol.itemDetails.extn.extnDirectShipInd".alias("direct_ship_flag"),
// MAGIC                                        $"ol.item.unitCost".alias("unit_cost"),
// MAGIC                                        // labor_cost                                                                 // Join with sourceAddlOlDataDF provide this
// MAGIC                                        $"ol.extn.extnLaborSKU".alias("labor_sku"),
// MAGIC                                        $"ol.chainedFromOrderHeaderKey".alias("chained_from_order_header_id"), //SEPIK 
// MAGIC                                        $"ol.chainedFromOrderLineKey".alias("chained_from_order_line_id"),     //SEPIK 
// MAGIC                                        $"ol.deliveryMethod".alias("delivery_method"),                          //SEPIK 
// MAGIC                                        $"ol.fulfillmentType".alias("fulfillment_type"),                        //SEPIK 
// MAGIC                                        $"ol.extn.extnDTCSubOrderNo".alias("dtc_suborder_nbr"),                  //SEPIK 
// MAGIC                                        $"ol.extn.EXTNRESKUIND".alias("resku_flag"),                             //SEPIK 
// MAGIC                                        $"ol.extn.extnReturnPolicy".alias("return_policy"),                      //SEPIK 
// MAGIC                                        $"ol.extn.extnIsReturnPolicyCheckOverridden".alias("return_policy_check_override_flag"), //SEPIK 
// MAGIC                                        $"ol.extn.extnProductAvailabilityDate".alias("product_availability_dt"), //SEPIK 
// MAGIC                                        $"ol.extn.extnIsECDDOverridden".alias("ecdd_overridden_flag"), //SEPIK 
// MAGIC                                        $"ol.extn.extnIsECDDInvoked".alias("ecdd_invoked_flag"), //SEPIK
// MAGIC                                        $"ol.extn.extnLineType".alias("additional_line_type_cd"), //SEPIK
// MAGIC                                        $"event_type"
// MAGIC                                      ).withColumn("order_item_type_cd", lit(null).cast(StringType))
// MAGIC                                       .withColumn("gift_registry_key", lit(null).cast(IntegerType))
// MAGIC                                       .withColumn("misc_charge_amt", lit(0.0).cast(DecimalType(15,2)))
// MAGIC                                       .withColumn("donation_amt", lit(0.0).cast(DecimalType(15,2)))
// MAGIC                                       .withColumn("associate_id", lit(null).cast(StringType))
// MAGIC                                       .withColumn("entry_method", lit("Online"))
// MAGIC                                       .withColumn("void_flag", lit(null).cast(StringType))
// MAGIC                                       .withColumn("return_action", lit(null).cast(StringType))
// MAGIC                                       .withColumn("return_reason_desc", lit(null).cast(StringType))
// MAGIC                                      // .withColumn("receiving_node_cd", lit(null).cast(StringType))  //Removed as part of SS
// MAGIC                                       .withColumn("minimum_ship_dt", lit(null).cast(DateType))
// MAGIC                                       .withColumn("requested_ship_dt", lit(null).cast(DateType))
// MAGIC                                       .withColumn("requested_delivery_dt", lit(null).cast(DateType))
// MAGIC                                       .withColumn("earliest_schedule_dt", lit(null).cast(DateType))
// MAGIC                                       .withColumn("earliest_delivery_dt", lit(null).cast(DateType))
// MAGIC                                       .withColumn("promised_appt_end_dt", lit(null).cast(DateType))
// MAGIC                                       .withColumn("orig_confirmed_qty", lit(null).cast(IntegerType)) //SEPIK
// MAGIC                                       //.withColumn("shipped_qty", lit(null).cast(IntegerType)) //Removed as part of SS
// MAGIC                                       //.withColumn("fill_qty", lit(null).cast(IntegerType))  //Removed as part of SS
// MAGIC                                       // additional column for event type
// MAGIC                                     //  .withColumn("event_type", lit(schemaType))
// MAGIC 
// MAGIC   val stgSourceOLDF = sourceOLDF.as("d1")
// MAGIC                                     .join(sourceVasDataDF.as("d6")     , $"d1.trans_id" === $"d6.trans_id" && $"d1.ohid" === $"d6.ohid"        && $"d1.order_line_id" === $"d6.olid", "left")
// MAGIC                                     .join(sourceAddlOlDataDF.as("d4")  , $"d1.trans_id" === $"d4.trans_id" && $"d1.ohid" === $"d4.ohid"        && $"d1.order_line_id" === $"d4.extnOrderLineKey", "left")
// MAGIC                                     .join(sourceLineChargeDF.as("d2")  , $"d1.trans_id" === $"d2.trans_id" && $"d1.ohid" === $"d2.ohid"        && $"d1.order_line_id" === $"d2.olid", "left")
// MAGIC                                     .join(sourceScheduleDF.as("d3")    , $"d1.trans_id" === $"d3.trans_id" && $"d1.ohid" === $"d3.ohid_sch"    && $"d1.order_line_id" === $"d3.olid_sch","left").join(sourceGiftDF.as("d5"),
// MAGIC                                           $"d1.trans_id" === $"d5.trans_id" && $"d1.ohid" === $"d5.ohid" && 
// MAGIC                                           $"d1.order_line_id" === $"d5.olid", "left").select($"d1.*", 
// MAGIC                                             $"d2.giftwrap_charge_amt", $"d2.merchandise_charge_amt", $"d2.mono_pz_charge_amt", $"d2.shipping_handling_charge_amt", $"d2.shipping_surcharge_amt",
// MAGIC                                             $"d3.olid_mergeNode".alias("merge_node_cd"),
// MAGIC                                             $"d4.extnWAC".alias("weighted_avg_cost"), $"d4.extnLaborCost".alias("labor_cost"),$"d4.extnActionCode".alias("return_action_cd"), $"d4.extnModificationReasonCode".alias("modification_reason_cd"), $"d4.extnModificationReasonCodeDesc".alias("modification_reason_cd_desc"),
// MAGIC                                                                                              $"d5.INSTRUCTION_TEXT".alias("gift_message"),$"d6.extnVASGiftWrap".alias("vas_gift_wrap_cd"),$"d6.extnVASMono".alias("vas_mono_flag"),$"d6.extnVASPZ".alias("vas_pz_flag")
// MAGIC                                     )
// MAGIC   stgSourceOLDF.createOrReplaceTempView("sterling_increment_stage_delta_ol")//SUBORDER_COMPLEXITY_GROUP_ID sutlej field
// MAGIC   spark.sql("""insert into table """+ L2_STAGE + """.sterling_increment_stage_delta_ol 
// MAGIC              SELECT event_type, trans_id, ohid, document_type, order_line_id, source_system, prime_line_seq_nbr, sub_line_seq_nbr, order_dt, linked_order_header_id, linked_order_line_id, order_item_id, order_item_type_cd, 
// MAGIC                  order_item_name, gift_registry_key, gift_registry_id, first_effective_ts, last_effective_ts, upc_cd, kit_cd, kit_qty, product_line, bundle_parent_order_line_id, order_line_type, order_qty, orig_order_qty, 
// MAGIC                  act_sale_unit_price_amt, reg_sale_unit_price_amt, extended_amt, extended_discount_amt, tax_amt, taxable_amt, gift_card_amt, giftwrap_charge_amt, line_total_amt, merchandise_charge_amt, mono_pz_charge_amt, 
// MAGIC                  misc_charge_amt, shipping_handling_charge_amt, shipping_surcharge_amt, donation_amt, associate_id, entry_method, void_flag, repo_flag, taxable_flag, pickable_flag, gift_flag, hold_flag, hold_reason, 
// MAGIC                  orig_backorder_flag,SUBORDER_COMPLEXITY_GROUP_ID, return_action_cd, return_action, return_reason_cd, return_reason_desc, ship_to_first_name, ship_to_middle_name, ship_to_last_name, ship_to_address_line_1, ship_to_address_line_2, 
// MAGIC                  ship_to_city, ship_to_state_or_prov, ship_to_postal_cd, ship_to_country, ship_to_email_address, ship_to_phone_nbr, consolidator_address_cd, merge_node_cd, ship_node_cd, receiving_node_cd, level_of_service, 
// MAGIC                  carrier_service_cd, carrier_cd, access_point_cd, access_point_id, access_point_nm, minimum_ship_dt, requested_ship_dt, requested_delivery_dt, earliest_schedule_dt, earliest_delivery_dt, promised_appt_end_dt, 
// MAGIC                  split_qty, shipped_qty, fill_qty, weighted_avg_cost, direct_ship_flag, unit_cost, labor_cost, labor_sku,
// MAGIC     gift_message, delivery_choice, modification_reason_cd, modification_reason_cd_desc, customer_level_of_service, chained_from_order_header_id, chained_from_order_line_id, delivery_method, fulfillment_type, dtc_suborder_nbr, orig_confirmed_qty, resku_flag, return_policy, return_policy_check_override_flag, product_availability_dt, ecdd_overridden_flag, ecdd_invoked_flag,additional_line_type_cd, vas_gift_wrap_cd, vas_mono_flag, vas_pz_flag, bo_notification_nbr                            
// MAGIC             FROM sterling_increment_stage_delta_ol"""
// MAGIC           )
// MAGIC   

// COMMAND ----------

// DBTITLE 1,OL: L1 to Stage Extract - Return Orders
val insertsql_RO = """
INSERT INTO """ + L2_STAGE + """.sterling_increment_stage_delta_ol  
  select 
    A.temp_wsieventName as event_type                      ,
    A.wsieventTransactionID as transaction_id  				,
    a.ORDER_HEADER_ID as order_header_id                 ,
    a.documentType  as doc_type			,
    exploded.orderLineKey as order_line_id  					,
    'STERLING_DTC' as source_system  					,
    exploded.primeLineNo as  prime_line_seq_nbr  			,
    exploded.subLineNo as sub_line_seq_nbr  				,
    a.temp_value_order_orderDate  AS order_dt 						,
    exploded.derivedFromOrderHeaderKey as linked_order_header_id  		,
    exploded.derivedFromOrderLineKey as linked_order_line_id  			,
    exploded.item.itemID as order_item_id  					,
    null as order_item_type_cd				,-- null will be there
    exploded.item.itemDesc as order_item_name  				,
    null as gift_registry_key  				,-- nothing will be there may be defaulted to -1
    null as gift_registry_id  				,
    a.temp_value_systemContext_wsieventTransactionTimeStamp first_effective_ts  			,
    null as last_effective_ts  				,
    null as upc_cd  						,
    null as kit_cd  						,
    null as kit_qty                         ,
    null as product_line  					,
    null as bundle_parent_order_line_id  	,
    exploded.lineType as order_line_type  				,
    exploded.orderedQty as order_qty  						,
     exploded.originalOrderedQty as orig_order_qty  				,
    null as act_sale_unit_price_amt  		,
    null as reg_sale_unit_price_amt  		,
    null as extended_amt  					,
    null as extended_discount_amt  			,
    null as tax_amt  						,
    null as taxable_amt  					,
    null as gift_card_amt  					,
    null as giftwrap_charge_amt  			,
    null as line_total_amt  				,
    null as merchandise_charge_amt  		,
    null as mono_pz_charge_amt  			,
    null as misc_charge_amt  				,
    null as shipping_handling_charge_amt  	,
    null as shipping_surcharge_amt  		,
    null as donation_amt  					,
    null as associate_id  					,
    null as entry_method  					,
    null as void_flag  						,
    null as repo_flag  						,
    null as taxable_flag  					,
    null as pickable_flag  					,
    null as gift_flag  						,
    null as hold_flag  						,
    null as hold_reason  					,
    null as orig_backorder_flag 			,
    null as SUBORDER_COMPLEXITY_GROUP_ID                   , --Sutlej					 
    null as return_action_cd  				,
    null as return_action  					,
    null as return_reason_cd  				,
    null as return_reason_desc  			,
    null as ship_to_first_name  			,
    null as ship_to_middle_name  			,
    null as ship_to_last_name  				,
    null as ship_to_address_line_1  		,
    null as ship_to_address_line_2  		,
    null as ship_to_city  					,
    null as ship_to_state_or_prov  			,
    null as ship_to_postal_cd  				,
    null as ship_to_country  				,
    null as ship_to_email_address  			,
    null as ship_to_phone_nbr  				,
    null as consolidator_address_cd  		,
    null as merge_node_cd  					,
     exploded.shipNode as ship_node_cd  					,
    null as receiving_node_cd 				,
    null as level_of_service  				,
    null as carrier_service_cd  			,
    null as carrier_cd  					,
    null as access_point_cd  				,
    null as access_point_id  				,
    null as access_point_nm  				,
    null as minimum_ship_dt 				,
    null as requested_ship_dt  				,
    null as requested_delivery_dt  			,
    null as earliest_schedule_dt  			,
    null as earliest_delivery_dt 			,
    null as promised_appt_end_dt 			,
    null as split_qty  						,
    null as shipped_qty 					,
    null as fill_qty						,
    null as weighted_avg_cost  				,
    null as direct_ship_flag  				,
    null as unit_cost  						,
    null as labor_cost  					,
    null as labor_sku  						,
    null as gift_message  			        ,
    null as delivery_choice                 , 
    null as modification_reason_cd          , 
    null as modification_reason_cd_desc     ,
    null as customer_level_of_service       ,
    null as chained_from_order_header_id                ,   --sepik
    null as chained_from_order_line_id                   ,  --sepik
    null as delivery_method                             ,  --sepik
    null as fulfillment_type                            ,  --sepik
    null as dtc_suborder_nbr                            ,  --sepik
    null as orig_confirmed_qty                          ,  --sepik
    null as resku_flag                                  ,  --sepik
    null as return_policy	                            ,  --sepik
    null as return_policy_check_override_flag           ,  --sepik
    null as product_availability_dt                     ,  --sepik
    null as ecdd_overridden_flag                        ,  --sepik
    null as ecdd_invoked_flag                           ,  --sepik
    null as additional_line_type_cd                            ,  --sepik
    null as vas_gift_wrap_cd				            ,  --sepik
    null as vas_mono_flag          				            ,  --sepik
    null as vas_pz_flag                                       ,   --sepik
    null as bo_notification_nbr
								 
  
  FROM 
  (
    SELECT 
    exploded.orderHeaderKey as ORDER_HEADER_ID
    ,exploded.documentType as documentType
    ,exploded.orderNo as ORDER_ID
    ,exploded.enterpriseCode as CONCEPT_CD
    ,exploded.orderDate as temp_value_order_orderDate
    ,exploded.orderDate as temp_value_order_orderDate_ts
    ,event.value.systemContext.wsieventTransactionTimeStamp as temp_value_systemContext_wsieventTransactionTimeStamp
    ,exploded.enteredBy as OPERATOR_ID
    ,event.value.systemContext.wsieventName as temp_wsieventName 
    ,event.value.systemContext.wsieventTransactionID as wsieventTransactionID 
    ,event.value.order.orderHeaderKey as so_orderHeaderKey
    ,exploded.orderLines as col
   FROM delta.`""" + L1_ADLS_LOC + """/` A
    LATERAL VIEW explode(event.value.order.returnOrders.returnOrder) event_table as exploded 
    ) A
  LATERAL VIEW EXPLODE(col.orderLine) as exploded
""" ;
spark.sql(insertsql_RO);



// COMMAND ----------

// DBTITLE 1,Retrieve OMS records for gaps (missing records, missing attributes): JDBC Extract for OH
var minTS = spark.sql("select min(regexp_replace(temp_value_systemContext_wsieventTransactionTimeStamp, 'T',' ') - interval '24' hour) as minTS from  " + L2_STAGE + ".STERLING_INCREMENT_STAGE_DELTA").as[String].first()

var sqlstmt_OH = """(
select /*+ parallel(oi 8) parallel (ol 8) parallel (oh  8) parallel (ad 8) ordered */
cast(substr(oh.order_header_key, 0, 8) as int) as order_header_int,
trim(oh.order_header_key) as order_header_id,
trim(oh.entry_type) as dtc_entry_type, 
trim(oh.order_type) as dtc_order_type, 
oh.order_date as temp_value_orderDate, 
trim(oh.document_type) as document_type, 
trim(oh.order_no) as order_id, 
trim(oh.extn_store_number) as RTC_LOCATION_ID, 
trim(oh.EXTN_ORIGINAL_ORDER_NO) as ORIG_ORDER_ID,
trim(oh.extn_store_ord_source) as STORE_ORDER_SOURCE,
trim(oh.enterprise_key) as CONCEPT_CD, 
trim(oh.customer_rewards_no) as customerRewardsNo,
CASE WHEN trim(oh.order_type) in ('OTHERS_CA','DES_STUDIO') THEN 'CAN'
  ELSE oh.extn_market_code
  END AS MARKET_CD,
trim(oh.extn_membership_level) as MEMBERSHIP_LEVEL_CD,
trim(ad_oh.EXTN_NUMBER_OF_SUBORDERS) as SUBORDERS_CNT,
TRIM(ad_oh.EXTN_IS_REGISTRY_ORDER) as  REGISTRY_ORDER_FLAG, /* COLUMNS ADDED BY SEPIK */
TRIM(ad_oh.EXTN_EMPLOYEE_ID)  AS EMPLOYEE_ID,      /* COLUMNS ADDED BY SEPIK */
TRIM(oh.EXTN_RETURN_TYPE)AS RETURN_TYPE_CD,        /* COLUMNS ADDED BY SEPIK */
TRIM(oh.EXTN_EXCHANGE_ORDER_NO) AS EXCHANGE_ORDER_ID,    /* COLUMNS ADDED BY SEPIK */       
TRIM(oh.DRAFT_ORDER_FLAG) AS DRAFT_ORDER_FLAG,        /* COLUMNS ADDED BY SEPIK */
TRIM(oh.ORDER_PURPOSE) AS     ORDER_PURPOSE,      /* COLUMNS ADDED BY SEPIK */
cast(trim(ad_oh.EXTN_SOURCE_CODE_DISCOUNT)as VARCHAR(16)) as SOURCE_CODE_DISCOUNT_AMT,    /* Sutlej1 changes from here */ 
trim(ad_oh.EXTN_IS_GIFTWRAP_WAIVED) as GIFTWRAP_WAIVED_FLAG,
cast(trim(ad_oh.EXTN_IS_SHIPPING_WAIVED) as VARCHAR(1)) as SHIPPING_WAIVED_FLAG,
trim(ad_oh.EXTN_CATALOG) as CATALOG_NM,
trim(ad_oh.EXTN_CATALOG_YEAR) as CATALOG_YEAR,
trim(ad_oh.EXTN_REGISTRY_ID) as REGISTRY_ID,
trim(ad_oh.EXTN_ORDER_SOURCE_TYPE) as ORDER_SOURCE_TYPE_CD,
trim(ad_oh.EXTN_GIFT_INDICATOR) as GIFT_FLAG,
trim(ad_oh.EXTN_WAREHOUSE_SITE_CODE) as WAREHOUSE_SITE_CD,
cast(trim(ad_oh.EXTN_PAPER_FLAG) as VARCHAR(1)) as PAPER_FLAG,
trim(ad_oh.EXTN_STORE_ASSOCIATE_NAME) as STORE_ASSOCIATE_NM,
cast(trim(ad_oh.EXTN_TENTATIVE_REFUND_AMOUNT) as VARCHAR(16)) as TENTATIVE_REFUND_AMT,
trim(ad_oh.EXTN_CONTACT_FLAG) as CONTACT_FLAG,
trim(ad_oh.EXTN_RETURN_CARRIER) as RETURN_CARRIER,
trim(ad_oh.EXTN_REGISTRY_TYPE) as REGISTRY_TYPE_CD,
TRIM(ad_oh.EXTN_RETURN_DELIVERY_HUB) AS RETURN_DELIVERY_HUB, /* COLUMNS ADDED BY SEPIK */
TRIM(ad_oh.EXTN_RETURN_MANAGING_HUB) AS RETURN_MANAGING_HUB,  /* COLUMNS ADDED BY SEPIK */
TRIM(ad_oh.EXTN_RETURN_CARRIER) AS RETURN_CARRIER_CD,   /* COLUMNS ADDED BY SEPIK */
TRIM(oh.EXTN_RETURN_METHOD) AS RETURN_METHOD_CD,    /* COLUMNS ADDED BY SEPIK */                              
TRIM(oh.PAYMENT_STATUS) AS PAYMENT_STATUS_CD,       /* COLUMNS ADDED BY SEPIK */  
TRIM(oh.EXTN_REFUND_POLICY) AS  REFUND_POLICY,    /* COLUMNS ADDED BY SEPIK */
trim(ad_oh.extn_loyalty_id) as LOYALTY_ACCOUNT_ID,                             /* COLUMN ADDED BY CCR */
oh.modifyts as wsieventTransactionTimeStamp,
trim(oh.extn_customer_type) as customer_type_cd,
mod(ORA_HASH(oh.order_header_key),50) + 1 as part
 from yantra_owner.yfs_order_header oh /* oh adoh first condition */
 inner join yantra_owner.wsi_addnl_order_data ad_oh
 on trim(oh.order_header_key) = trim(ad_oh.EXTN_ORDER_HEADER_KEY)
 where oh.modifyts >= to_date('""" + minTS + """','YYYY-MM-DD HH24:MI:SS')
 )"""

println(sqlstmt_OH)


//Issue select against Sterling Oracle via JDBC
lazy val sterling_attributesOH = spark.read
          .format("jdbc")
          .option("driver", "oracle.jdbc.driver.OracleDriver")
          .option("url", OracleURL)
          .option("user", OracleUser)
          .option("password", Oracle) //Delhi changes, now uses keyvault
          .option("dbtable", sqlstmt_OH)
          .option("fetchSize", 15000)
          .option("partitionColumn", "part") //delhi change for optimization
          .option("lowerBound", "1")  //delhi change for optimization
          .option("upperBound", "51")  //delhi change for optimization
          .option("numPartitions", "50")  //delhi change for optimization
          .load()

sterling_attributesOH.createOrReplaceTempView("sterling_attributesOH")

// COMMAND ----------

// DBTITLE 1,Retrieve OMS records for gaps (missing records, missing attributes): JDBC Extract for OL
var minTS2 = spark.sql("select min(regexp_replace(temp_value_systemContext_wsieventTransactionTimeStamp, 'T',' ') - interval '24' hour) as minTS from  " + L2_STAGE + ".STERLING_INCREMENT_STAGE_DELTA").as[String].first()

var sqlstmt_OL = """(
select /*+ parallel(oi 8) parallel (ol 8) parallel (oh  8) parallel (ad 8) ordered */
trim(ol.order_line_key) as order_line_id, 
trim(ol.item_id) as item_id, 
trim(LINE_TYPE) as LINE_TYPE,
trim(ad.extn_action_code) as extn_action_code,
ad.extn_vendor_cost,
ad.extn_wac,
ad.extn_labor_cost,
trim(ol.extn_labor_sku) as extn_labor_sku,
ol.unit_cost,
trim(ol.return_reason) as return_reason_cd,
trim(ol.EXTN_RETURN_SUB_REASON ) as return_sub_reason_cd,
trim(ol.Receiving_Node) as receiving_node_cd,
trim(ol.CHAINED_FROM_ORDER_LINE_KEY) AS  CHAINED_FROM_ORDER_LINE_ID ,     /* COLUMNS ADDED BY SEPIK */         
trim(ol.CHAINED_FROM_ORDER_HEADER_KEY) AS  CHAINED_FROM_ORDER_HEADER_ID,  /* COLUMNS ADDED BY SEPIK */
trim(ol.DELIVERY_METHOD) AS  DELIVERY_METHOD,                              /* COLUMNS ADDED BY SEPIK */    
trim(ol.FULFILLMENT_TYPE) AS  FULFILLMENT_TYPE,                             /* COLUMNS ADDED BY SEPIK */   
trim(ol.EXTN_DTC_SUBORDERNO) AS  DTC_SUBORDER_NBR,                            /* COLUMNS ADDED BY SEPIK */
trim(ol.EXTN_ORG_CONFIRMED_QTY) AS ORIG_CONFIRMED_QTY,                       /* COLUMNS ADDED BY SEPIK */
trim(ol.EXTN_RESKU_IND) AS  RESKU_FLAG,                                       /* COLUMNS ADDED BY SEPIK */ 
trim(ol.EXTN_RETURN_POLICY) AS  RETURN_POLICY,                                 /* COLUMNS ADDED BY SEPIK */
trim(ol.EXTN_RET_POLICY_CHK_OVERIDEN) AS  RETURN_POLICY_CHECK_OVERRIDE,   /* COLUMNS ADDED BY SEPIK */
trim(ol.EXTN_PRODUCT_AVAILABILITY_DATE) AS  PRODUCT_AVAILABILITY_DT,           /* COLUMNS ADDED BY SEPIK */
trim(ol.EXTN_IS_ECDD_OVERRIDDEN) AS  ECDD_OVERRIDDEN_FLAG,                     /* COLUMNS ADDED BY SEPIK */
trim(ol.EXTN_IS_ECDD_INVOKED) AS  ECDD_INVOKED_FLAG,                           /* COLUMNS ADDED BY SEPIK */
trim(ol.EXTN_LINE_TYPE) AS  ADDITIONAL_LINE_TYPE_CD,                            /* COLUMNS ADDED BY SEPIK */
/* trim(vas.EXTN_VAS_GIFT_WRAP) AS VAS_GIFT_WRAP_CD,                                 COLUMNS ADDED BY SEPIK */                   
/* trim(vas.EXTN_VAS_MONO) AS VAS_MONO_FLAG,                                           COLUMNS ADDED BY SEPIK */
/* trim(vas.EXTN_VAS_PZ) AS VAS_PZ_FLAG,                                              COLUMNS ADDED BY SEPIK */
trim(ol.extn_is_backorder_atcreate) as orig_backorder_flag,                /* COLUMNS SUTLEJ */
trim(ol.extn_group_id) as SUBORDER_COMPLEXITY_GROUP_ID,                                  /* COLUMNS  SUTLEJ */
trim(ol.extn_bo_notif_no) AS BO_NOTIFICATION_NBR,
mod(ORA_HASH(ol.order_line_key),50) + 1 as part
 from yantra_owner.yfs_order_line ol
 inner join yantra_owner.wsi_addnl_order_line_data ad
 on trim(ol.order_line_key) = trim(ad.extn_order_line_key)
 where ol.modifyts >= to_date('""" + minTS2 + """','YYYY-MM-DD HH24:MI:SS')
 )"""

println(sqlstmt_OL)


//Issue select against Sterling Oracle via JDBC
lazy val sterling_attributesOL = spark.read
          .format("jdbc")
          .option("driver", "oracle.jdbc.driver.OracleDriver")
          .option("url", OracleURL)
          .option("user", OracleUser)
          .option("password", Oracle) //Delhi changes, now uses keyvault
          .option("dbtable", sqlstmt_OL)
          .option("fetchSize", 15000)
          .option("partitionColumn", "part") //delhi change for optimization
          .option("lowerBound", "1")  //delhi change for optimization
          .option("upperBound", "51")  //delhi change for optimization
          .option("numPartitions", "50")  //delhi change for optimization
          .load()

sterling_attributesOL.createOrReplaceTempView("sterling_attributesOL")

// COMMAND ----------

// DBTITLE 1,OH: Populate null dtc_entry_type_cd and  order_type_cd into 1st OH Stage
// MAGIC %sql
// MAGIC INSERT INTO $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_str
// MAGIC select 
// MAGIC trim(oh.ORDER_HEADER_ID)											,
// MAGIC oh.SOURCE_SYSTEM											,
// MAGIC oh.ORDER_ID                                                 ,
// MAGIC str.ORIG_ORDER_ID                                            ,
// MAGIC case when oh.DTC_ENTRY_TYPE_CD = '' OR oh.DTC_ENTRY_TYPE_CD IS NULL  then str.dtc_entry_type else oh.DTC_ENTRY_TYPE_CD end as DTC_ENTRY_TYPE_CD,
// MAGIC oh.CONCEPT_CD												,
// MAGIC oh.TXN_TYPE_CD												,
// MAGIC oh.CURRENCY_CD												,
// MAGIC oh.LINKED_ORDER_HEADER_KEY									,
// MAGIC oh.LINKED_ORDER_HEADER_ID									,
// MAGIC oh.POS_WORKSTATION_ID										,
// MAGIC oh.POS_WORKSTATION_SEQ_NBR									,
// MAGIC oh.RTC_LOCATION_ID											,
// MAGIC oh.temp_value_order_orderDate								,
// MAGIC oh.temp_value_order_orderDate_ts							,																					
// MAGIC oh.temp_value_order_customerRewardsNo						,
// MAGIC oh.CUSTOMER_KEY											,
// MAGIC oh.CUSTOMER_ID												,
// MAGIC str.LOYALTY_ACCOUNT_ID                                      , -- CCR change
// MAGIC oh.MATCH_METHOD_CD											,
// MAGIC oh.temp_value_systemContext_wsieventTransactionTimeStamp	,
// MAGIC oh.BILL_TO_FIRST_NAME										,
// MAGIC oh.BILL_TO_MIDDLE_NAME										,
// MAGIC oh.BILL_TO_LAST_NAME										,
// MAGIC oh.BILL_TO_ADDRESS_LINE_1									,
// MAGIC oh.BILL_TO_ADDRESS_LINE_2									,
// MAGIC oh.BILL_TO_CITY											,
// MAGIC oh.BILL_TO_STATE_OR_PROV_CD								,
// MAGIC oh.BILL_TO_POSTAL_CD										,
// MAGIC oh.BILL_TO_COUNTRY											,
// MAGIC oh.BILL_TO_EMAIL_ADDRESS									,
// MAGIC oh.temp_value_order_personInfoBillTo_mobilePhone			,
// MAGIC oh.temp_value_order_personInfoBillTo_otherPhone			,
// MAGIC oh.temp_value_order_personInfoBillTo_dayPhone				,
// MAGIC oh.temp_value_order_personInfoBillTo_eveningPhone			,
// MAGIC case when oh.ORDER_TYPE_CD = '' OR oh.ORDER_TYPE_CD is null  then str.dtc_order_type else oh.ORDER_TYPE_CD end as ORDER_TYPE_CD		,
// MAGIC oh.CUSTOMER_TYPE_CD										,
// MAGIC str.MEMBERSHIP_LEVEL_CD                                 ,
// MAGIC str.SUBORDERS_CNT                                       ,         --SUTLEJ CHANGE																					  
// MAGIC case when trim(oh.STORE_ORDER_SOURCE) = '' or oh.STORE_ORDER_SOURCE is null then str.STORE_ORDER_SOURCE else oh.STORE_ORDER_SOURCE end as STORE_ORDER_SOURCE		,
// MAGIC oh.OPERATOR_ID												,
// MAGIC oh.CANCEL_FLAG												,
// MAGIC oh.TRAINING_MODE_FLAG										,
// MAGIC oh.RECEIPT_PREFERENCE										,
// MAGIC oh.GROSS_AMT												,
// MAGIC oh.temp_value_order_overallTotals_lineSubTotal				,
// MAGIC oh.temp_value_order_overallTotals_grandCharges				,
// MAGIC oh.TAX_AMT																						,
// MAGIC 'str' as temp_wsieventName                                  ,
// MAGIC case when oh.MARKET_CD = '' OR oh.MARKET_CD is null then str.MARKET_CD else oh.MARKET_CD end as MARKET_CD,--USE CASE STATEMENT TO SELECT FROM ABOVE VIEW IF NULL --SEPIK CHANGES START
// MAGIC case when oh.REGISTRY_ORDER_FLAG = '' OR oh.REGISTRY_ORDER_FLAG is null then str.REGISTRY_ORDER_FLAG else oh.REGISTRY_ORDER_FLAG end as REGISTRY_ORDER_FLAG  ,
// MAGIC case when oh.EMPLOYEE_ID = '' OR oh.EMPLOYEE_ID is null then str.EMPLOYEE_ID else oh.EMPLOYEE_ID end as EMPLOYEE_ID,
// MAGIC case when oh.RETURN_TYPE_CD = '' OR oh.RETURN_TYPE_CD is null then str.RETURN_TYPE_CD else oh.RETURN_TYPE_CD end as RETURN_TYPE_CD,     
// MAGIC case when oh.EXCHANGE_ORDER_ID = '' OR oh.EXCHANGE_ORDER_ID is null then str.EXCHANGE_ORDER_ID else oh.EXCHANGE_ORDER_ID end as EXCHANGE_ORDER_ID,  
// MAGIC case when oh.DOCUMENT_TYPE_CD = '' OR oh.DOCUMENT_TYPE_CD is null then str.document_type else oh.DOCUMENT_TYPE_CD end as DOCUMENT_TYPE_CD,   
// MAGIC case when oh.DRAFT_ORDER_FLAG = '' OR oh.DRAFT_ORDER_FLAG is null then str.DRAFT_ORDER_FLAG else oh.DRAFT_ORDER_FLAG end as DRAFT_ORDER_FLAG,   
// MAGIC case when oh.ORDER_PURPOSE = '' OR oh.ORDER_PURPOSE is null then str.ORDER_PURPOSE else oh.ORDER_PURPOSE end as ORDER_PURPOSE,
// MAGIC case when oh.SOURCE_CODE_DISCOUNT_AMT = '' OR oh.SOURCE_CODE_DISCOUNT_AMT IS NULL then str.SOURCE_CODE_DISCOUNT_AMT else oh.SOURCE_CODE_DISCOUNT_AMT end as SOURCE_CODE_DISCOUNT_AMT, --SUTLEJ CHANGES FROM HERE
// MAGIC case when oh.GIFTWRAP_WAIVED_FLAG = '' OR oh.GIFTWRAP_WAIVED_FLAG IS NULL then str.GIFTWRAP_WAIVED_FLAG else oh.GIFTWRAP_WAIVED_FLAG end as GIFTWRAP_WAIVED_FLAG,
// MAGIC case when oh.SHIPPING_WAIVED_FLAG = '' OR oh.SHIPPING_WAIVED_FLAG IS NULL then str.SHIPPING_WAIVED_FLAG else oh.SHIPPING_WAIVED_FLAG end as SHIPPING_WAIVED_FLAG, 
// MAGIC case when oh.CATALOG_NM = '' OR oh.CATALOG_NM IS NULL  then str.CATALOG_NM else oh.CATALOG_NM end as CATALOG_NM,
// MAGIC case when oh.CATALOG_YEAR = '' OR oh.CATALOG_YEAR IS NULL then str.CATALOG_YEAR else oh.CATALOG_YEAR end as CATALOG_YEAR, 
// MAGIC case when oh.REGISTRY_ID = '' OR oh.REGISTRY_ID IS NULL  then str.REGISTRY_ID else oh.REGISTRY_ID end as REGISTRY_ID,
// MAGIC case when oh.ORDER_SOURCE_TYPE_CD = '' OR oh.ORDER_SOURCE_TYPE_CD IS NULL  then str.ORDER_SOURCE_TYPE_CD else oh.ORDER_SOURCE_TYPE_CD end as ORDER_SOURCE_TYPE_CD,
// MAGIC case when oh.GIFT_FLAG = '' OR oh.GIFT_FLAG IS NULL  then str.GIFT_FLAG else oh.GIFT_FLAG end as GIFT_FLAG,
// MAGIC case when oh.WAREHOUSE_SITE_CD = '' OR oh.WAREHOUSE_SITE_CD IS NULL  then str.WAREHOUSE_SITE_CD else oh.WAREHOUSE_SITE_CD end as WAREHOUSE_SITE_CD,
// MAGIC case when oh.PAPER_FLAG = '' OR oh.PAPER_FLAG IS NULL  then str.PAPER_FLAG else oh.PAPER_FLAG end as PAPER_FLAG,
// MAGIC case when oh.STORE_ASSOCIATE_NM = '' OR oh.STORE_ASSOCIATE_NM IS NULL  then str.STORE_ASSOCIATE_NM else oh.STORE_ASSOCIATE_NM end as STORE_ASSOCIATE_NM,
// MAGIC case when oh.TENTATIVE_REFUND_AMT = '' OR oh.TENTATIVE_REFUND_AMT IS NULL  then str.TENTATIVE_REFUND_AMT else oh.TENTATIVE_REFUND_AMT end as TENTATIVE_REFUND_AMT, 
// MAGIC case when oh.CONTACT_FLAG = '' OR oh.CONTACT_FLAG IS NULL  then str.CONTACT_FLAG else oh.CONTACT_FLAG end as CONTACT_FLAG,
// MAGIC case when oh.RETURN_CARRIER = '' OR oh.RETURN_CARRIER IS NULL  then str.RETURN_CARRIER else oh.RETURN_CARRIER end as RETURN_CARRIER,
// MAGIC case when oh.REGISTRY_TYPE_CD = '' OR oh.REGISTRY_TYPE_CD IS NULL  then str.REGISTRY_TYPE_CD else oh.REGISTRY_TYPE_CD end as REGISTRY_TYPE_CD, --SUTLEJ CHANGES END													   
// MAGIC case when oh.RETURN_DELIVERY_HUB = '' OR oh.RETURN_DELIVERY_HUB is null then str.RETURN_DELIVERY_HUB else oh.RETURN_DELIVERY_HUB end as RETURN_DELIVERY_HUB,
// MAGIC case when oh.RETURN_MANAGING_HUB = '' OR oh.RETURN_MANAGING_HUB is null then str.RETURN_MANAGING_HUB else oh.RETURN_MANAGING_HUB end as RETURN_MANAGING_HUB,
// MAGIC case when oh.RETURN_CARRIER_CD = '' OR oh.RETURN_CARRIER_CD is null then str.RETURN_CARRIER_CD else oh.RETURN_CARRIER_CD end as RETURN_CARRIER_CD,
// MAGIC case when oh.RETURN_METHOD_CD = '' OR oh.RETURN_METHOD_CD is null then str.RETURN_METHOD_CD else oh.RETURN_METHOD_CD end as RETURN_METHOD_CD,   
// MAGIC oh.HOUSEHOLD_ID,       
// MAGIC oh.HOUSEHOLD_KEY,      
// MAGIC case when oh.PAYMENT_STATUS_CD = '' OR oh.PAYMENT_STATUS_CD is null then str.PAYMENT_STATUS_CD else oh.PAYMENT_STATUS_CD end as PAYMENT_STATUS_CD,  
// MAGIC case when oh.REFUND_POLICY = '' OR oh.REFUND_POLICY is null then str.REFUND_POLICY else oh.REFUND_POLICY end as REFUND_POLICY --SEPIK CHANGES END
// MAGIC from $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA oh
// MAGIC left join sterling_attributesOH str
// MAGIC on trim(coalesce(str.order_header_id,-1)) = trim(coalesce(oh.order_header_id,-2))
// MAGIC 
// MAGIC group by oh.ORDER_HEADER_ID								    ,
// MAGIC oh.SOURCE_SYSTEM											,
// MAGIC oh.ORDER_ID												    ,
// MAGIC str.ORIG_ORDER_ID                                           ,
// MAGIC oh.DTC_ENTRY_TYPE_CD,
// MAGIC str.dtc_entry_type,
// MAGIC oh.CONCEPT_CD												,
// MAGIC oh.TXN_TYPE_CD												,
// MAGIC oh.CURRENCY_CD												,
// MAGIC oh.LINKED_ORDER_HEADER_KEY									,
// MAGIC oh.LINKED_ORDER_HEADER_ID									,
// MAGIC oh.POS_WORKSTATION_ID										,
// MAGIC oh.POS_WORKSTATION_SEQ_NBR									,
// MAGIC oh.RTC_LOCATION_ID											,
// MAGIC oh.temp_value_order_orderDate								,
// MAGIC oh.temp_value_order_orderDate_ts							,			   
// MAGIC oh.temp_value_order_customerRewardsNo						,
// MAGIC oh.CUSTOMER_KEY											    ,
// MAGIC oh.CUSTOMER_ID												,
// MAGIC str.LOYALTY_ACCOUNT_ID                                      , --CCR Change
// MAGIC oh.MATCH_METHOD_CD											,
// MAGIC oh.temp_value_systemContext_wsieventTransactionTimeStamp	,
// MAGIC oh.BILL_TO_FIRST_NAME										,
// MAGIC oh.BILL_TO_MIDDLE_NAME										,
// MAGIC oh.BILL_TO_LAST_NAME										,
// MAGIC oh.BILL_TO_ADDRESS_LINE_1									,
// MAGIC oh.BILL_TO_ADDRESS_LINE_2									,
// MAGIC oh.BILL_TO_CITY											,
// MAGIC oh.BILL_TO_STATE_OR_PROV_CD								,
// MAGIC oh.BILL_TO_POSTAL_CD										,
// MAGIC oh.BILL_TO_COUNTRY											,
// MAGIC oh.BILL_TO_EMAIL_ADDRESS									,
// MAGIC oh.temp_value_order_personInfoBillTo_mobilePhone			,
// MAGIC oh.temp_value_order_personInfoBillTo_otherPhone			,
// MAGIC oh.temp_value_order_personInfoBillTo_dayPhone				,
// MAGIC oh.temp_value_order_personInfoBillTo_eveningPhone			,
// MAGIC oh.ORDER_TYPE_CD,
// MAGIC str.dtc_order_type,
// MAGIC oh.CUSTOMER_TYPE_CD										,
// MAGIC str.MEMBERSHIP_LEVEL_CD	                                ,
// MAGIC str.SUBORDERS_CNT                                       ,         --SUTLEJ CHANGE					  
// MAGIC oh.STORE_ORDER_SOURCE                                   ,
// MAGIC str.STORE_ORDER_SOURCE                                  ,
// MAGIC oh.OPERATOR_ID												,
// MAGIC oh.CANCEL_FLAG												,
// MAGIC oh.TRAINING_MODE_FLAG										,
// MAGIC oh.RECEIPT_PREFERENCE										,
// MAGIC oh.GROSS_AMT												,
// MAGIC oh.temp_value_order_overallTotals_lineSubTotal				,
// MAGIC oh.temp_value_order_overallTotals_grandCharges				,
// MAGIC oh.TAX_AMT													,
// MAGIC oh.MARKET_CD                                            ,
// MAGIC str.MARKET_CD                                               ,
// MAGIC oh.REGISTRY_ORDER_FLAG                                      ,
// MAGIC str.REGISTRY_ORDER_FLAG                                    ,
// MAGIC oh.EMPLOYEE_ID                                              ,
// MAGIC str.EMPLOYEE_ID                                            ,
// MAGIC oh.RETURN_TYPE_CD                                          ,
// MAGIC str.RETURN_TYPE_CD                                         ,
// MAGIC oh.EXCHANGE_ORDER_ID                                        ,
// MAGIC str.EXCHANGE_ORDER_ID                                       ,
// MAGIC oh.DOCUMENT_TYPE_CD                                         , 
// MAGIC str.DOCUMENT_TYPE                                           ,
// MAGIC oh.DRAFT_ORDER_FLAG                                        ,
// MAGIC str.DRAFT_ORDER_FLAG                                        ,
// MAGIC oh.ORDER_PURPOSE                                            ,
// MAGIC str.ORDER_PURPOSE                                            ,
// MAGIC oh.SOURCE_CODE_DISCOUNT_AMT                             ,--change by Sutlej1
// MAGIC str.SOURCE_CODE_DISCOUNT_AMT                             ,--change by Sutlej1																					 
// MAGIC oh.GIFTWRAP_WAIVED_FLAG                                 ,--change by Sutlej1
// MAGIC str.GIFTWRAP_WAIVED_FLAG                                 ,--change by Sutlej1																				  
// MAGIC oh.SHIPPING_WAIVED_FLAG                                 ,--change by Sutlej1 
// MAGIC str.SHIPPING_WAIVED_FLAG                                 ,--change by Sutlej1  																				 
// MAGIC oh.CATALOG_NM                                           ,--change by Sutlej1 
// MAGIC str.CATALOG_NM                                           ,--change by Sutlej1																					  
// MAGIC oh.CATALOG_YEAR                                         ,--change by Sutlej1
// MAGIC str.CATALOG_YEAR                                         ,--change by Sutlej1																					 
// MAGIC oh.REGISTRY_ID                                          ,--change by Sutlej1
// MAGIC str.REGISTRY_ID                                          ,--change by Sutlej1																					  
// MAGIC oh.ORDER_SOURCE_TYPE_CD                                 ,--change by Sutlej1
// MAGIC str.ORDER_SOURCE_TYPE_CD                                 ,--change by Sutlej1																					 
// MAGIC oh.GIFT_FLAG                                            ,--change by Sutlej1
// MAGIC str.GIFT_FLAG                                            ,--change by Sutlej1																					  
// MAGIC oh.WAREHOUSE_SITE_CD                                    ,--change by Sutlej1
// MAGIC str.WAREHOUSE_SITE_CD                                    ,--change by Sutlej1																					 
// MAGIC oh.PAPER_FLAG                                           ,--change by Sutlej1
// MAGIC str.PAPER_FLAG                                           ,--change by Sutlej1																					  
// MAGIC oh.STORE_ASSOCIATE_NM                                   ,--change by Sutlej1
// MAGIC str.STORE_ASSOCIATE_NM                                   ,--change by Sutlej1																					 
// MAGIC oh.TENTATIVE_REFUND_AMT                                 ,--change by Sutlej1
// MAGIC str.TENTATIVE_REFUND_AMT                                 ,--change by Sutlej1																					  
// MAGIC oh.CONTACT_FLAG                                         ,--change by Sutlej1
// MAGIC str.CONTACT_FLAG                                         ,--change by Sutlej1																					 
// MAGIC oh.RETURN_CARRIER                                       ,--change by Sutlej1
// MAGIC str.RETURN_CARRIER                                       ,--change by Sutlej1																					  																				  
// MAGIC oh.REGISTRY_TYPE_CD                                     ,--change by Sutlej1
// MAGIC str.REGISTRY_TYPE_CD                                     ,--change by Sutlej1																					 																		  																				  
// MAGIC oh.RETURN_DELIVERY_HUB                                       ,
// MAGIC str.RETURN_DELIVERY_HUB                                      ,
// MAGIC oh.RETURN_MANAGING_HUB                                       ,
// MAGIC str.RETURN_MANAGING_HUB                                       ,
// MAGIC oh.RETURN_CARRIER_CD                                         ,
// MAGIC str.RETURN_CARRIER_CD                                         ,
// MAGIC oh.RETURN_METHOD_CD                                         ,
// MAGIC str.RETURN_METHOD_CD                                         ,
// MAGIC oh.HOUSEHOLD_ID                                            ,       
// MAGIC oh.HOUSEHOLD_KEY                                            ,      
// MAGIC oh.PAYMENT_STATUS_CD                                         ,  
// MAGIC str.PAYMENT_STATUS_CD                                         ,
// MAGIC oh.REFUND_POLICY                                             ,
// MAGIC str.REFUND_POLICY
// MAGIC 
// MAGIC --7mins

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_str

// COMMAND ----------

// DBTITLE 1,OL: Populate null return action codes into 1st OL Stage
// MAGIC %sql
// MAGIC INSERT INTO $L2_STAGE.sterling_increment_stage_delta_ol_str 
// MAGIC SELECT 
// MAGIC                             ol.event_type                    ,
// MAGIC                             ol.transaction_id  				,
// MAGIC                             trim(ol.order_header_id)               ,
// MAGIC                             ol.doc_type  						,
// MAGIC                             trim(ol.order_line_id)  					,
// MAGIC                             ol.source_system  					,
// MAGIC                             ol.prime_line_seq_nbr  			,
// MAGIC                             ol.sub_line_seq_nbr  				,
// MAGIC                             ol.order_dt 						,
// MAGIC                             ol.linked_order_header_id  		,
// MAGIC                             ol.linked_order_line_id  			,
// MAGIC                             ol.order_item_id  					,
// MAGIC                             ol.order_item_type_cd				,    
// MAGIC                             ol.order_item_name  				,
// MAGIC                             ol.gift_registry_key  				, 
// MAGIC                             ol.gift_registry_id  				,
// MAGIC                             ol.first_effective_ts  			,
// MAGIC                             ol.last_effective_ts  				,
// MAGIC                             ol.upc_cd  						,
// MAGIC                             ol.kit_cd  						,
// MAGIC                             ol.kit_qty                       ,
// MAGIC                             ol.product_line  					,
// MAGIC                             ol.bundle_parent_order_line_id  	,
// MAGIC                             case when trim(ol.order_line_type) = '' or ol.order_line_type is null then str.LINE_TYPE else ol.order_line_type end as order_line_type		,
// MAGIC                             ol.order_qty  						,
// MAGIC                             ol.orig_order_qty  				,
// MAGIC                             ol.act_sale_unit_price_amt  		,
// MAGIC                             ol.reg_sale_unit_price_amt  		,
// MAGIC                             ol.extended_amt  					,
// MAGIC                             ol.extended_discount_amt  			,
// MAGIC                             ol.tax_amt  						,
// MAGIC                             ol.taxable_amt  					,
// MAGIC                             ol.gift_card_amt  					,
// MAGIC                             ol.giftwrap_charge_amt  			,
// MAGIC                             ol.line_total_amt  				,
// MAGIC                             ol.merchandise_charge_amt  		,
// MAGIC                             ol.mono_pz_charge_amt  			,
// MAGIC                             ol.misc_charge_amt  				,
// MAGIC                             ol.shipping_handling_charge_amt  ,
// MAGIC                             ol.shipping_surcharge_amt  		,
// MAGIC                             ol.donation_amt  					,
// MAGIC                             ol.associate_id  					,
// MAGIC                             ol.entry_method  					,
// MAGIC                             ol.void_flag  						,
// MAGIC                             ol.repo_flag  						,
// MAGIC                             ol.taxable_flag  					,
// MAGIC                             ol.pickable_flag  					,
// MAGIC                             ol.gift_flag  						,
// MAGIC                             ol.hold_flag  						,
// MAGIC                             ol.hold_reason  					,
// MAGIC                             case when trim(ol.orig_backorder_flag) = '' or ol.orig_backorder_flag is null then str.orig_backorder_flag else ol.orig_backorder_flag end as orig_backorder_flag,--Sutlej change, added case stmt to avoid null values
// MAGIC                             case when trim(ol.SUBORDER_COMPLEXITY_GROUP_ID) = '' or ol.SUBORDER_COMPLEXITY_GROUP_ID is null then str.SUBORDER_COMPLEXITY_GROUP_ID else ol.SUBORDER_COMPLEXITY_GROUP_ID end as SUBORDER_COMPLEXITY_GROUP_ID,--Sutlej change																																													   
// MAGIC                             case when trim(ol.return_action_cd) = '' or ol.return_action_cd is null then str.EXTN_ACTION_CODE else ol.return_action_cd end as return_action_cd,
// MAGIC                             ol.return_action  					,
// MAGIC                             case when trim(ol.return_reason_cd) = '' or ol.return_reason_cd is null then str.return_reason_cd else ol.return_reason_cd end as return_reason_cd,
// MAGIC                             ol.return_reason_desc  			    ,
// MAGIC                             str.return_sub_reason_cd            ,
// MAGIC                             ol.ship_to_first_name  			    ,
// MAGIC                             ol.ship_to_middle_name  			,
// MAGIC                             ol.ship_to_last_name  				,
// MAGIC                             ol.ship_to_address_line_1  		,
// MAGIC                             ol.ship_to_address_line_2  		,
// MAGIC                             ol.ship_to_city  					,
// MAGIC                             ol.ship_to_state_or_prov  			,
// MAGIC                             ol.ship_to_postal_cd  				,
// MAGIC                             ol.ship_to_country  				,
// MAGIC                             ol.ship_to_email_address  			,
// MAGIC                             ol.ship_to_phone_nbr  				,
// MAGIC                             ol.consolidator_address_cd  		,
// MAGIC                             ol.merge_node_cd  					,
// MAGIC                             ol.ship_node_cd  					,
// MAGIC                             case when trim(ol.receiving_node_cd) = '' or ol.receiving_node_cd is null then str.receiving_node_cd else ol.receiving_node_cd end as receiving_node_cd,
// MAGIC                             ol.level_of_service  				,
// MAGIC                             ol.carrier_service_cd  			,
// MAGIC                             ol.carrier_cd  					,
// MAGIC                             ol.access_point_cd  					,
// MAGIC                             ol.access_point_id  					,
// MAGIC                             ol.access_point_nm  					,
// MAGIC                             ol.minimum_ship_dt 				,
// MAGIC                             ol.requested_ship_dt  				,
// MAGIC                             ol.requested_delivery_dt  			,
// MAGIC                             ol.earliest_schedule_dt  			,
// MAGIC                             ol.earliest_delivery_dt 			,
// MAGIC                             ol.promised_appt_end_dt 			,
// MAGIC                             ol.split_qty  						,
// MAGIC                             ol.shipped_qty 					,
// MAGIC                             ol.fill_qty						,
// MAGIC                             case when ol.weighted_avg_cost = '' or ol.weighted_avg_cost is null then str.extn_wac else ol.weighted_avg_cost end as weighted_avg_cost,
// MAGIC                             ol.direct_ship_flag  				,
// MAGIC                             case when ol.unit_cost = '' or ol.unit_cost is null then str.unit_cost else ol.unit_cost end as unit_cost,
// MAGIC                             case when ol.labor_cost = '' or ol.labor_cost is null then str.extn_labor_cost else ol.labor_cost end as labor_cost,
// MAGIC                             case when ol.labor_sku = '' or ol.labor_sku is null then str.extn_labor_sku else ol.labor_sku end as labor_sku,
// MAGIC                             ol.gift_message,
// MAGIC                             ol.delivery_choice, 
// MAGIC                             ol.modification_reason_cd, 
// MAGIC                             ol.modification_reason_cd_desc,
// MAGIC                             ol.customer_level_of_service,
// MAGIC                             case when ol.chained_from_order_header_id = '' or ol.chained_from_order_header_id is null then str.chained_from_order_header_id else ol.chained_from_order_header_id end as chained_from_order_header_id, --sepik start
// MAGIC                             case when ol.chained_from_order_line_id = '' or ol.chained_from_order_line_id is null then str.chained_from_order_line_id else ol.chained_from_order_line_id end as chained_from_order_line_id,
// MAGIC                             case when ol.delivery_method = '' or ol.delivery_method is null then str.delivery_method else ol.delivery_method end as delivery_method,               
// MAGIC                             case when ol.fulfillment_type = '' or ol.fulfillment_type is null then str.fulfillment_type else ol.fulfillment_type end as fulfillment_type,              
// MAGIC                             case when ol.dtc_suborder_nbr = '' or ol.dtc_suborder_nbr is null then str.dtc_suborder_nbr else ol.dtc_suborder_nbr end as dtc_suborder_nbr,
// MAGIC                             case when ol.orig_confirmed_qty = '' or ol.orig_confirmed_qty is null then str.orig_confirmed_qty else ol.orig_confirmed_qty end as orig_confirmed_qty,
// MAGIC                             case when ol.resku_flag = '' or ol.resku_flag is null then str.resku_flag else ol.resku_flag end as resku_flag,                    
// MAGIC                             case when ol.return_policy = '' or ol.return_policy is null then str.return_policy else ol.return_policy end as return_policy,                 
// MAGIC                             case when ol.return_policy_check_override_flag = '' or ol.return_policy_check_override_flag is null then str.return_policy_check_override else ol.return_policy_check_override_flag end as return_policy_check_override_flag,  
// MAGIC                             case when ol.product_availability_dt = '' or ol.product_availability_dt is null then str.product_availability_dt else ol.product_availability_dt end as product_availability_dt,
// MAGIC                             case when ol.ecdd_overridden_flag = '' or ol.ecdd_overridden_flag is null then str.ecdd_overridden_flag else ol.ecdd_overridden_flag end as ecdd_overridden_flag,      
// MAGIC                             case when ol.ecdd_invoked_flag = '' or ol.ecdd_invoked_flag is null then str.ecdd_invoked_flag else ol.ecdd_invoked_flag end as ecdd_invoked_flag,    
// MAGIC                             case when ol.additional_line_type_cd = '' or ol.additional_line_type_cd is null then str.additional_line_type_cd else ol.additional_line_type_cd end as additional_line_type_cd,
// MAGIC                             ol.vas_gift_wrap_cd,
// MAGIC                             ol.vas_mono_flag,
// MAGIC                             ol.vas_pz_flag,   --sepik end
// MAGIC                             case when ol.bo_notification_nbr = '' or ol.bo_notification_nbr is null then str.bo_notification_nbr else ol.bo_notification_nbr end as bo_notification_nbr
// MAGIC                            
// MAGIC                            
// MAGIC                           from $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_ol ol
// MAGIC                           left join sterling_attributesOL str
// MAGIC                          on trim(str.order_line_id) = trim(ol.order_line_id)
// MAGIC                           group by 
// MAGIC                                  ol.event_type                    ,
// MAGIC                             ol.transaction_id  				,
// MAGIC                             ol.order_header_id               ,
// MAGIC                             ol.doc_type  						,
// MAGIC                             ol.order_line_id  					,
// MAGIC                             ol.source_system  					,
// MAGIC                             ol.prime_line_seq_nbr  			,
// MAGIC                             ol.sub_line_seq_nbr  				,
// MAGIC                             ol.order_dt 						,
// MAGIC                             ol.linked_order_header_id  		,
// MAGIC                             ol.linked_order_line_id  			,
// MAGIC                             ol.order_item_id  					,
// MAGIC                             ol.order_item_type_cd				,    
// MAGIC                             ol.order_item_name  				,
// MAGIC                             ol.gift_registry_key  				,   
// MAGIC                             ol.gift_registry_id  				,
// MAGIC                             ol.first_effective_ts  			,
// MAGIC                             ol.last_effective_ts  				,
// MAGIC                             ol.upc_cd  						,
// MAGIC                             ol.kit_cd  						,
// MAGIC                             ol.kit_qty                       ,
// MAGIC                             ol.product_line  					,
// MAGIC                             ol.bundle_parent_order_line_id  	,
// MAGIC                             ol.order_line_type ,
// MAGIC                             str.LINE_TYPE ,
// MAGIC                             ol.order_qty  						,
// MAGIC                             ol.orig_order_qty  				,
// MAGIC                             ol.act_sale_unit_price_amt  		,
// MAGIC                             ol.reg_sale_unit_price_amt  		,
// MAGIC                             ol.extended_amt  					,
// MAGIC                             ol.extended_discount_amt  			,
// MAGIC                             ol.tax_amt  						,
// MAGIC                             ol.taxable_amt  					,
// MAGIC                             ol.gift_card_amt  					,
// MAGIC                             ol.giftwrap_charge_amt  			,
// MAGIC                             ol.line_total_amt  				,
// MAGIC                             ol.merchandise_charge_amt  		,
// MAGIC                             ol.mono_pz_charge_amt  			,
// MAGIC                             ol.misc_charge_amt  				,
// MAGIC                             ol.shipping_handling_charge_amt  ,
// MAGIC                             ol.shipping_surcharge_amt  		,
// MAGIC                             ol.donation_amt  					,
// MAGIC                             ol.associate_id  					,
// MAGIC                             ol.entry_method  					,
// MAGIC                             ol.void_flag  						,
// MAGIC                             ol.repo_flag  						,
// MAGIC                             ol.taxable_flag  					,
// MAGIC                             ol.pickable_flag  					,
// MAGIC                             ol.gift_flag  						,
// MAGIC                             ol.hold_flag  						,
// MAGIC                             ol.hold_reason  					,
// MAGIC                             case when trim(ol.orig_backorder_flag) = '' or ol.orig_backorder_flag is null then str.orig_backorder_flag else ol.orig_backorder_flag end, 
// MAGIC                             ol.orig_backorder_flag,--Sutlej change, added case stmt to avoid null values
// MAGIC                             case when trim(ol.SUBORDER_COMPLEXITY_GROUP_ID) = '' or ol.SUBORDER_COMPLEXITY_GROUP_ID is null then str.SUBORDER_COMPLEXITY_GROUP_ID else ol.SUBORDER_COMPLEXITY_GROUP_ID end,
// MAGIC                             ol.SUBORDER_COMPLEXITY_GROUP_ID,--Sutlej change				
// MAGIC                             case when trim(ol.return_action_cd) = '' or ol.return_action_cd is null then str.EXTN_ACTION_CODE else ol.return_action_cd end,
// MAGIC                             ol.return_action  					,
// MAGIC                             case when trim(ol.return_reason_cd) = '' or ol.return_reason_cd is null then str.return_reason_cd else ol.return_reason_cd end,
// MAGIC                             ol.return_reason_desc  			,
// MAGIC                             str.return_sub_reason_cd            ,
// MAGIC                             ol.ship_to_first_name  			,
// MAGIC                             ol.ship_to_middle_name  			,
// MAGIC                             ol.ship_to_last_name  				,
// MAGIC                             ol.ship_to_address_line_1  		,
// MAGIC                             ol.ship_to_address_line_2  		,
// MAGIC                             ol.ship_to_city  					,
// MAGIC                             ol.ship_to_state_or_prov  			,
// MAGIC                             ol.ship_to_postal_cd  				,
// MAGIC                             ol.ship_to_country  				,
// MAGIC                             ol.ship_to_email_address  			,
// MAGIC                             ol.ship_to_phone_nbr  				,
// MAGIC                             ol.consolidator_address_cd  		,
// MAGIC                             ol.merge_node_cd  					,
// MAGIC                             ol.ship_node_cd  					,
// MAGIC                             case when trim(ol.receiving_node_cd) = '' or ol.receiving_node_cd is null then str.receiving_node_cd else ol.receiving_node_cd end,
// MAGIC                             ol.level_of_service  				,
// MAGIC                             ol.carrier_service_cd  			,
// MAGIC                             ol.carrier_cd  					,
// MAGIC                             ol.access_point_cd  					,
// MAGIC                             ol.access_point_id  					,
// MAGIC                             ol.access_point_nm  					,
// MAGIC                             ol.minimum_ship_dt 				,
// MAGIC                             ol.requested_ship_dt  				,
// MAGIC                             ol.requested_delivery_dt  			,
// MAGIC                             ol.earliest_schedule_dt  			,
// MAGIC                             ol.earliest_delivery_dt 			,
// MAGIC                             ol.promised_appt_end_dt 			,
// MAGIC                             ol.split_qty  						,
// MAGIC                             ol.shipped_qty 					,
// MAGIC                             ol.fill_qty						,
// MAGIC                             case when ol.weighted_avg_cost = '' or ol.weighted_avg_cost is null then str.extn_wac else ol.weighted_avg_cost end,
// MAGIC                             ol.direct_ship_flag  				,
// MAGIC                             case when ol.unit_cost = '' or ol.unit_cost is null then str.unit_cost else ol.unit_cost end,
// MAGIC                             case when ol.labor_cost = '' or ol.labor_cost is null then str.extn_labor_cost else ol.labor_cost end,
// MAGIC                             case when ol.labor_sku = '' or ol.labor_sku is null then str.extn_labor_sku else ol.labor_sku end  	,
// MAGIC                             ol.gift_message,
// MAGIC                             ol.delivery_choice, 
// MAGIC                             ol.modification_reason_cd, 
// MAGIC                             ol.modification_reason_cd_desc,
// MAGIC                             ol.customer_level_of_service,
// MAGIC                             case when ol.chained_from_order_header_id = '' or ol.chained_from_order_header_id is null then str.chained_from_order_header_id else ol.chained_from_order_header_id end, --sepik start
// MAGIC                             case when ol.chained_from_order_line_id = '' or ol.chained_from_order_line_id is null then str.chained_from_order_line_id else ol.chained_from_order_line_id end,
// MAGIC                             case when ol.delivery_method = '' or ol.delivery_method is null then str.delivery_method else ol.delivery_method end,               
// MAGIC                             case when ol.fulfillment_type = '' or ol.fulfillment_type is null then str.fulfillment_type else ol.fulfillment_type end,              
// MAGIC                             case when ol.dtc_suborder_nbr = '' or ol.dtc_suborder_nbr is null then str.dtc_suborder_nbr else ol.dtc_suborder_nbr end,
// MAGIC                             case when ol.orig_confirmed_qty = '' or ol.orig_confirmed_qty is null then str.orig_confirmed_qty else ol.orig_confirmed_qty end,
// MAGIC                             case when ol.resku_flag = '' or ol.resku_flag is null then str.resku_flag else ol.resku_flag end,                    
// MAGIC                             case when ol.return_policy = '' or ol.return_policy is null then str.return_policy else ol.return_policy end,                 
// MAGIC                             case when ol.return_policy_check_override_flag = '' or ol.return_policy_check_override_flag is null then str.return_policy_check_override else ol.return_policy_check_override_flag end,  
// MAGIC                             case when ol.product_availability_dt = '' or ol.product_availability_dt is null then str.product_availability_dt else ol.product_availability_dt end,
// MAGIC                             case when ol.ecdd_overridden_flag = '' or ol.ecdd_overridden_flag is null then str.ecdd_overridden_flag else ol.ecdd_overridden_flag end,      
// MAGIC                             case when ol.ecdd_invoked_flag = '' or ol.ecdd_invoked_flag is null then str.ecdd_invoked_flag else ol.ecdd_invoked_flag end,    
// MAGIC                             case when ol.additional_line_type_cd = '' or ol.additional_line_type_cd is null then str.additional_line_type_cd else ol.additional_line_type_cd end,
// MAGIC                             ol.vas_gift_wrap_cd,
// MAGIC                             ol.vas_mono_flag,
// MAGIC                             ol.vas_pz_flag,   --sepik end
// MAGIC                             case when ol.bo_notification_nbr = '' or ol.bo_notification_nbr is null then str.bo_notification_nbr else ol.bo_notification_nbr end
// MAGIC                            
// MAGIC                             --20mins

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from $L2_STAGE.sterling_increment_stage_delta_ol_str

// COMMAND ----------

// DBTITLE 1,OH: Keep only 1 Composite Primary Key Within the Same Batch
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMP VIEW COMBINEDSCHEMA_OH AS
// MAGIC (
// MAGIC SELECT
// MAGIC ORDER_HEADER_ID
// MAGIC ,SOURCE_SYSTEM
// MAGIC ,ORDER_ID
// MAGIC ,ORIG_ORDER_ID                
// MAGIC ,EXCHANGE_ORDER_ID            --SEPIK
// MAGIC ,ORDER_TYPE_CD               --column order change
// MAGIC ,temp_value_order_orderDate   --column order change
// MAGIC ,HOUSEHOLD_KEY                --SEPIK
// MAGIC ,HOUSEHOLD_ID                 --SEPIK
// MAGIC ,DTC_ENTRY_TYPE_CD
// MAGIC ,CONCEPT_CD
// MAGIC ,TXN_TYPE_CD
// MAGIC ,PAYMENT_STATUS_CD           --SEPIK
// MAGIC ,CURRENCY_CD
// MAGIC ,LINKED_ORDER_HEADER_KEY
// MAGIC ,LINKED_ORDER_HEADER_ID
// MAGIC ,POS_WORKSTATION_ID
// MAGIC ,POS_WORKSTATION_SEQ_NBR
// MAGIC ,RTC_LOCATION_ID
// MAGIC ,temp_value_order_orderDate_ts												
// MAGIC ,temp_value_order_customerRewardsNo
// MAGIC ,CUSTOMER_KEY
// MAGIC ,CUSTOMER_ID
// MAGIC ,LOYALTY_ACCOUNT_ID        --CCR Change
// MAGIC ,EMPLOYEE_ID               --SEPIK																											   
// MAGIC ,RETURN_TYPE_CD            --SEPIK
// MAGIC ,MATCH_METHOD_CD
// MAGIC ,temp_value_systemContext_wsieventTransactionTimeStamp
// MAGIC ,BILL_TO_FIRST_NAME
// MAGIC ,BILL_TO_MIDDLE_NAME
// MAGIC ,BILL_TO_LAST_NAME
// MAGIC ,BILL_TO_ADDRESS_LINE_1
// MAGIC ,BILL_TO_ADDRESS_LINE_2
// MAGIC ,BILL_TO_CITY
// MAGIC ,BILL_TO_STATE_OR_PROV_CD
// MAGIC ,BILL_TO_POSTAL_CD
// MAGIC ,BILL_TO_COUNTRY
// MAGIC ,BILL_TO_EMAIL_ADDRESS
// MAGIC ,temp_value_order_personInfoBillTo_mobilePhone
// MAGIC ,temp_value_order_personInfoBillTo_otherPhone
// MAGIC ,temp_value_order_personInfoBillTo_dayPhone
// MAGIC ,temp_value_order_personInfoBillTo_eveningPhone
// MAGIC ,CUSTOMER_TYPE_CD
// MAGIC ,MEMBERSHIP_LEVEL_CD
// MAGIC ,SUBORDERS_CNT       --SUTLEJ	  
// MAGIC ,MARKET_CD           --SEPIK
// MAGIC ,STORE_ORDER_SOURCE   
// MAGIC ,OPERATOR_ID
// MAGIC ,CANCEL_FLAG
// MAGIC ,TRAINING_MODE_FLAG
// MAGIC ,REGISTRY_ORDER_FLAG     --SEPIK
// MAGIC ,DRAFT_ORDER_FLAG         --SEPIK
// MAGIC ,ORDER_PURPOSE           --SEPIK
// MAGIC ,SOURCE_CODE_DISCOUNT_AMT     --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,GIFTWRAP_WAIVED_FLAG                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,SHIPPING_WAIVED_FLAG                             --change by Sutlej1, field from wsi_addnl_order_data 																											   
// MAGIC ,CATALOG_NM                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,CATALOG_YEAR                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,REGISTRY_ID                                      --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,ORDER_SOURCE_TYPE_CD                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,GIFT_FLAG                                        --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,WAREHOUSE_SITE_CD                                --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,PAPER_FLAG                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,STORE_ASSOCIATE_NM                               --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,TENTATIVE_REFUND_AMT                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,CONTACT_FLAG                                     --change by Sutlej1, field from wsi_addnl_order_data																												
// MAGIC ,RETURN_CARRIER                                   --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC ,REGISTRY_TYPE_CD                                 --change by Sutlej1, field from wsi_addnl_order_data
// MAGIC ,RETURN_DELIVERY_HUB        --SEPIK
// MAGIC ,RETURN_MANAGING_HUB        --SEPIK
// MAGIC ,RETURN_CARRIER_CD          --SEPIK
// MAGIC ,RETURN_METHOD_CD            --SEPIK
// MAGIC ,RECEIPT_PREFERENCE
// MAGIC ,GROSS_AMT
// MAGIC ,temp_value_order_overallTotals_lineSubTotal
// MAGIC ,temp_value_order_overallTotals_grandCharges
// MAGIC ,TAX_AMT
// MAGIC ,DOCUMENT_TYPE_CD           --SEPIK
// MAGIC ,REFUND_POLICY               --SEPIK
// MAGIC ,temp_wsieventName
// MAGIC FROM
// MAGIC (
// MAGIC SELECT TGT.*,ROW_NUMBER() OVER(PARTITION BY temp_value_systemContext_wsieventTransactionTimeStamp, ORDER_HEADER_ID ORDER BY COALESCE(temp_value_order_customerRewardsNo,-1) DESC) AS RNK
// MAGIC FROM $L2_STAGE.STERLING_INCREMENT_STAGE_DELTA_str TGT
// MAGIC ) INN
// MAGIC WHERE INN.RNK = 1
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OH: Perform All Look-ups, Generate OHK if known OID, Generate LAST_EFFECTIVE_TS
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_HEADER_STG1_VIEW3 AS 
// MAGIC ( 
// MAGIC SELECT  
// MAGIC COALESCE(TARGET.ORDER_HEADER_KEY, -1) AS ORDER_HEADER_KEY
// MAGIC , oh.ORDER_HEADER_ID
// MAGIC , oh.SOURCE_SYSTEM
// MAGIC , oh.ORDER_ID
// MAGIC , oh.ORIG_ORDER_ID
// MAGIC , oh.EXCHANGE_ORDER_ID            --SEPIK
// MAGIC , oh.ORDER_TYPE_CD                 --column order change
// MAGIC , substring(oh.temp_value_order_orderDate,1,10) as ORDER_DT    --column order change
// MAGIC , coalesce(loc.LOCATION_KEY,-1) as LOCATION_KEY
// MAGIC , coalesce(loc.LOCATION_ID ,-1) as LOCATION_ID
// MAGIC , oh.HOUSEHOLD_KEY                --SEPIK
// MAGIC , oh.HOUSEHOLD_ID                 --SEPIK
// MAGIC , oh.DTC_ENTRY_TYPE_CD
// MAGIC , oh.TXN_TYPE_CD
// MAGIC , oh.PAYMENT_STATUS_CD           --SEPIK
// MAGIC , oh.CONCEPT_CD
// MAGIC , coalesce(oh.CURRENCY_CD,'') AS CURRENCY_CD
// MAGIC , oh.LINKED_ORDER_HEADER_KEY
// MAGIC , oh.LINKED_ORDER_HEADER_ID
// MAGIC , oh.POS_WORKSTATION_ID
// MAGIC , oh.POS_WORKSTATION_SEQ_NBR
// MAGIC , coalesce(loc_rtc.LOCATION_KEY,-1) as RTC_LOCATION_KEY
// MAGIC , oh.RTC_LOCATION_ID as RTC_LOCATION_ID
// MAGIC , -1 as LOYALTY_ACCOUNT_KEY
// MAGIC , oh.LOYALTY_ACCOUNT_ID --CCR change
// MAGIC , oh.CUSTOMER_KEY
// MAGIC , oh.CUSTOMER_ID
// MAGIC , oh.EMPLOYEE_ID               --SEPIK
// MAGIC , oh.MATCH_METHOD_CD
// MAGIC , oh.temp_value_systemContext_wsieventTransactionTimeStamp as FIRST_EFFECTIVE_TS
// MAGIC , null as LAST_EFFECTIVE_TS
// MAGIC , oh.BILL_TO_FIRST_NAME as BILL_TO_FIRST_NAME
// MAGIC , oh.BILL_TO_MIDDLE_NAME as BILL_TO_MIDDLE_NAME 
// MAGIC , oh.BILL_TO_LAST_NAME as BILL_TO_LAST_NAME
// MAGIC , oh.BILL_TO_ADDRESS_LINE_1 as BILL_TO_ADDRESS_LINE_1
// MAGIC , oh.BILL_TO_ADDRESS_LINE_2 as BILL_TO_ADDRESS_LINE_2
// MAGIC , oh.BILL_TO_CITY as BILL_TO_CITY
// MAGIC , oh.BILL_TO_STATE_OR_PROV_CD as BILL_TO_STATE_OR_PROV_CD
// MAGIC , oh.BILL_TO_POSTAL_CD as BILL_TO_POSTAL_CD
// MAGIC , oh.BILL_TO_COUNTRY as BILL_TO_COUNTRY
// MAGIC , oh.BILL_TO_EMAIL_ADDRESS as BILL_TO_EMAIL_ADDRESS
// MAGIC , case when trim(temp_value_order_personInfoBillTo_mobilePhone) != '' then temp_value_order_personInfoBillTo_mobilePhone
// MAGIC   when trim(temp_value_order_personInfoBillTo_dayPhone) != '' then temp_value_order_personInfoBillTo_dayPhone
// MAGIC   when trim(temp_value_order_personInfoBillTo_eveningPhone) != '' then temp_value_order_personInfoBillTo_eveningPhone
// MAGIC   when trim(temp_value_order_personInfoBillTo_otherPhone) != '' then temp_value_order_personInfoBillTo_otherPhone
// MAGIC   else null end as BILL_TO_PHONE_NBR
// MAGIC , CASE WHEN trim(oh.CUSTOMER_TYPE_CD) = 'BUSINESS' THEN oh.temp_value_order_customerRewardsNo ELSE null END as TRADE_ID
// MAGIC , oh.CUSTOMER_TYPE_CD
// MAGIC , oh.MEMBERSHIP_LEVEL_CD
// MAGIC , oh.SUBORDERS_CNT       --SUTLEJ CHANGE				 
// MAGIC , oh.MARKET_CD           --SEPIK
// MAGIC , oh.STORE_ORDER_SOURCE
// MAGIC , oh.OPERATOR_ID
// MAGIC , oh.CANCEL_FLAG
// MAGIC , oh.TRAINING_MODE_FLAG
// MAGIC , oh.REGISTRY_ORDER_FLAG     --SEPIK
// MAGIC , oh.DRAFT_ORDER_FLAG         --SEPIK
// MAGIC , oh.ORDER_PURPOSE           --SEPIK
// MAGIC , oh.SOURCE_CODE_DISCOUNT_AMT     --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.GIFTWRAP_WAIVED_FLAG                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.SHIPPING_WAIVED_FLAG                             --change by Sutlej1, field from wsi_addnl_order_data 																											   
// MAGIC , oh.CATALOG_NM                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.CATALOG_YEAR                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.REGISTRY_ID                                      --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.ORDER_SOURCE_TYPE_CD                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.GIFT_FLAG                                        --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.WAREHOUSE_SITE_CD                                --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.PAPER_FLAG                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.STORE_ASSOCIATE_NM                               --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.TENTATIVE_REFUND_AMT                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.CONTACT_FLAG                                     --change by Sutlej1, field from wsi_addnl_order_data																												
// MAGIC , oh.RETURN_CARRIER                                   --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC , oh.REGISTRY_TYPE_CD                                 --change by Sutlej1, field from wsi_addnl_order_data																													   
// MAGIC , to_timestamp(oh.temp_value_order_orderDate_ts) as TXN_BEGIN_TS
// MAGIC , to_timestamp(oh.temp_value_order_orderDate_ts) as TXN_END_TS
// MAGIC , oh.RETURN_TYPE_CD            --SEPIK
// MAGIC , oh.RETURN_DELIVERY_HUB        --SEPIK
// MAGIC , oh.RETURN_MANAGING_HUB        --SEPIK
// MAGIC , oh.RETURN_CARRIER_CD          --SEPIK
// MAGIC , oh.RETURN_METHOD_CD            --SEPIK
// MAGIC , oh.RECEIPT_PREFERENCE
// MAGIC , CASE WHEN oh.TXN_TYPE_CD='RETURN' then oh.GROSS_AMT * -1 --return 
// MAGIC   else  oh.GROSS_AMT --sale
// MAGIC   end as GROSS_AMT
// MAGIC , oh.temp_value_order_overallTotals_lineSubTotal + oh.temp_value_order_overallTotals_grandCharges as NET_AMT
// MAGIC , oh.TAX_AMT as TAX_AMT
// MAGIC , oh.DOCUMENT_TYPE_CD           --SEPIK
// MAGIC , oh.REFUND_POLICY               --SEPIK
// MAGIC , null as INSERT_TS
// MAGIC , null as UPDATE_TS
// MAGIC 
// MAGIC FROM COMBINEDSCHEMA_OH oh
// MAGIC LEFT JOIN L2_ANALYTICS.LOCATION loc
// MAGIC ON (case when trim(oh.DTC_ENTRY_TYPE_CD) = 'CCA' then oh.CONCEPT_CD || ' ' || 'Care Center' else oh.CONCEPT_CD || ' ' || 'Internet' end) = loc.LOCATION_DESC
// MAGIC LEFT JOIN L2_ANALYTICS.LOCATION loc_rtc
// MAGIC ON trim(oh.RTC_LOCATION_ID) = trim(loc_rtc.LOCATION_ID)
// MAGIC 
// MAGIC 
// MAGIC /*PATTERN: left join to target table here*/
// MAGIC LEFT OUTER JOIN 
// MAGIC   $L2_ANALYTICS_TABLES.Order_HEADER TARGET
// MAGIC   ON TARGET.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC   AND trim(TARGET.ORDER_HEADER_ID) = trim(oh.ORDER_HEADER_ID)
// MAGIC   --AND TARGET.ORDER_DT = cast(oh.temp_value_order_orderDate as date)
// MAGIC   AND TARGET.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC   
// MAGIC /*END PATTERN*/
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OH: Retrieve Active Records from Target Table (9999-12-31)
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_HEADER_STG1_VIEW3_merged AS ( 
// MAGIC select tgt.* from $L2_ANALYTICS_TABLES.ORDER_HEADER tgt inner join ORDER_HEADER_STG1_VIEW3 stg on --tgt.order_dt = stg.order_dt and 
// MAGIC tgt.source_system = 'STERLING_DTC'  and trim(tgt.Order_HEADER_ID) = trim(stg.Order_HEADER_ID) and tgt.last_effective_ts =  to_timestamp('9999-12-31T23:59:59')
// MAGIC union all 
// MAGIC select * from ORDER_HEADER_STG1_VIEW3 stg 
// MAGIC );

// COMMAND ----------

// DBTITLE 1,OH: Remove repeating attributes / Keep unique records
			  
														

					 
var df_stg_ORDER_HEADER_STG1_VIEW3_merged = spark.sql("SELECT * FROM ORDER_HEADER_STG1_VIEW3_merged")
var df_uniq1_multibatch = df_stg_ORDER_HEADER_STG1_VIEW3_merged.dropDuplicates("FIRST_EFFECTIVE_TS", "ORDER_HEADER_ID")
var df_uniq2_record_multibatch = df_uniq1_multibatch.orderBy("FIRST_EFFECTIVE_TS").coalesce(1).dropDuplicates(
"ORDER_HEADER_ID"
,"SOURCE_SYSTEM"
,"ORDER_ID"
,"ORIG_ORDER_ID"
,"EXCHANGE_ORDER_ID"            //SEPIK
,"ORDER_TYPE_CD"
,"ORDER_DT"
,"LOCATION_ID"
,"HOUSEHOLD_KEY"           //SEPIK
,"HOUSEHOLD_ID"             //SEPIK
,"DTC_ENTRY_TYPE_CD"
,"TXN_TYPE_CD"
,"PAYMENT_STATUS_CD"         //SEPIK
,"CONCEPT_CD"
,"CURRENCY_CD"
,"LINKED_ORDER_HEADER_ID"
,"POS_WORKSTATION_ID"
,"POS_WORKSTATION_SEQ_NBR"
,"RTC_LOCATION_ID"
,"LOYALTY_ACCOUNT_KEY"
,"LOYALTY_ACCOUNT_ID"
,"CUSTOMER_KEY"
,"CUSTOMER_ID"
,"EMPLOYEE_ID"            //SEPIK
,"MATCH_METHOD_CD"
,"BILL_TO_FIRST_NAME"
,"BILL_TO_MIDDLE_NAME"
,"BILL_TO_LAST_NAME"
,"BILL_TO_ADDRESS_LINE_1"
,"BILL_TO_ADDRESS_LINE_2"
,"BILL_TO_CITY"
,"BILL_TO_STATE_OR_PROV_CD"
,"BILL_TO_POSTAL_CD"
,"BILL_TO_COUNTRY"
,"BILL_TO_EMAIL_ADDRESS"
,"BILL_TO_PHONE_NBR"
,"TRADE_ID"
,"CUSTOMER_TYPE_CD"
,"MEMBERSHIP_LEVEL_CD"
,"SUBORDERS_CNT"        //SUTLEJ								
,"MARKET_CD"            //SEPIK
,"STORE_ORDER_SOURCE"
,"OPERATOR_ID"
,"CANCEL_FLAG"
,"TRAINING_MODE_FLAG"
,"REGISTRY_ORDER_FLAG"    //SEPIK
,"DRAFT_ORDER_FLAG"     //SEPIK
,"ORDER_PURPOSE" //SEPIK
,"SOURCE_CODE_DISCOUNT_AMT"     //change by Sutlej1, field from wsi_addnl_order_data																											   
,"GIFTWRAP_WAIVED_FLAG"                             //change by Sutlej1, field from wsi_addnl_order_data																											   
,"SHIPPING_WAIVED_FLAG"                             //change by Sutlej1, field from wsi_addnl_order_data 																											   
,"CATALOG_NM"                                       //change by Sutlej1, field from wsi_addnl_order_data																											   
,"CATALOG_YEAR"                                       //change by Sutlej1, field from wsi_addnl_order_data																											   
,"REGISTRY_ID"                                      //change by Sutlej1, field from wsi_addnl_order_data																											   
,"ORDER_SOURCE_TYPE_CD"                             //change by Sutlej1, field from wsi_addnl_order_data																											   
,"GIFT_FLAG"                                        //change by Sutlej1, field from wsi_addnl_order_data																											   
,"WAREHOUSE_SITE_CD"                                //change by Sutlej1, field from wsi_addnl_order_data																											   
,"PAPER_FLAG"                                       //change by Sutlej1, field from wsi_addnl_order_data																											   
,"STORE_ASSOCIATE_NM"                               //change by Sutlej1, field from wsi_addnl_order_data																											   
,"TENTATIVE_REFUND_AMT"                             //change by Sutlej1, field from wsi_addnl_order_data																											   
,"CONTACT_FLAG"                                     //change by Sutlej1, field from wsi_addnl_order_data																												
,"RETURN_CARRIER"                                   //change by Sutlej1, field from wsi_addnl_order_data																											   
,"REGISTRY_TYPE_CD"                                 //change by Sutlej1, field from wsi_addnl_order_data											   
,"TXN_BEGIN_TS"
,"TXN_END_TS"
,"RETURN_TYPE_CD"  //SEPIK
,"RETURN_DELIVERY_HUB"  //SEPIK
,"RETURN_MANAGING_HUB"  //SEPIK
,"RETURN_CARRIER_CD"  //SEPIK
,"RETURN_METHOD_CD"  //SEPIK
,"RECEIPT_PREFERENCE"
,"GROSS_AMT"
,"NET_AMT"
,"TAX_AMT"
,"DOCUMENT_TYPE_CD" //SEPIK
,"REFUND_POLICY"  //SEPIK
)

df_uniq2_record_multibatch.createOrReplaceTempView("ORDER_HEADER_STG1_VIEW3_merged_UNIQ")

// COMMAND ----------

// DBTITLE 1,OH: Begin calculating last_effective_ts (get adjacent record's first_effective_ts)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_HEADER_STG1_VIEW4 AS ( 
// MAGIC select *
// MAGIC , LEAD(FIRST_EFFECTIVE_TS) OVER (PARTITION BY ORDER_HEADER_ID ORDER BY  FIRST_EFFECTIVE_TS)  as new_LAST_EFFECTIVE_TS
// MAGIC ,coalesce(INSERT_TS, current_timestamp) as  new_INSERT_TS --retain timestamp if already previously inserted, otherwise generate value
// MAGIC ,current_timestamp as new_UPDATE_TS --always generate value upon insert
// MAGIC from ORDER_HEADER_STG1_VIEW3_merged_UNIQ
// MAGIC );

// COMMAND ----------

// DBTITLE 1,Aquire Lock here
// MAGIC %scala
// MAGIC spark.sql("CACHE TABLE ORDER_HEADER_STG1_VIEW4")
// MAGIC if(!aquireLock(tagFilePrefix, lockFilePath, lockDirPath, logDirPath, retries, retryInterval)){
// MAGIC   throw new Exception("OH :::After retrying for " + retries + " times and waiting for " + (retries*retryInterval) + " minutes, process is unable to aquire lock, skipping further exeution")
// MAGIC }

// COMMAND ----------

// DBTITLE 1,OH: Complete calculating last_effective_ts (applying -1 second difference, or put 9999-12-31)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_HEADER_STG1_final AS ( 
// MAGIC select 
// MAGIC CASE WHEN STG1.ORDER_HEADER_KEY = -1 THEN
// MAGIC dense_rank() OVER (ORDER BY STG1.ORDER_HEADER_KEY ,STG1.ORDER_HEADER_ID) + COALESCE(MAX_ORDER_HEADER_KEY, 0) ELSE
// MAGIC STG1.ORDER_HEADER_KEY END AS ORDER_HEADER_KEY
// MAGIC ,ORDER_HEADER_ID           
// MAGIC ,SOURCE_SYSTEM             
// MAGIC ,ORDER_ID                  
// MAGIC ,ORIG_ORDER_ID             
// MAGIC ,EXCHANGE_ORDER_ID --SEPIK        
// MAGIC ,ORDER_TYPE_CD     --column order change        
// MAGIC ,ORDER_DT            --column order change    
// MAGIC ,LOCATION_KEY              
// MAGIC ,LOCATION_ID               
// MAGIC ,HOUSEHOLD_KEY       --SEPIK       
// MAGIC ,HOUSEHOLD_ID         --SEPIK       
// MAGIC ,DTC_ENTRY_TYPE_CD         
// MAGIC ,TXN_TYPE_CD               
// MAGIC ,PAYMENT_STATUS_CD       --SEPIK    
// MAGIC ,CONCEPT_CD                
// MAGIC ,CURRENCY_CD               
// MAGIC ,LINKED_ORDER_HEADER_KEY   
// MAGIC ,LINKED_ORDER_HEADER_ID    
// MAGIC ,POS_WORKSTATION_ID        
// MAGIC ,POS_WORKSTATION_SEQ_NBR   
// MAGIC ,RTC_LOCATION_KEY          
// MAGIC ,RTC_LOCATION_ID           
// MAGIC ,LOYALTY_ACCOUNT_KEY       
// MAGIC ,LOYALTY_ACCOUNT_ID         
// MAGIC ,CUSTOMER_KEY              
// MAGIC ,CUSTOMER_ID              
// MAGIC ,EMPLOYEE_ID          --SEPIK     
// MAGIC ,MATCH_METHOD_CD           
// MAGIC ,FIRST_EFFECTIVE_TS        
// MAGIC , case when new_LAST_EFFECTIVE_TS is null then to_timestamp('9999-12-31T23:59:59')	
// MAGIC else new_LAST_EFFECTIVE_TS - INTERVAL 1 MILLISECONDS
// MAGIC   end as LAST_EFFECTIVE_TS        
// MAGIC ,BILL_TO_FIRST_NAME        
// MAGIC ,BILL_TO_MIDDLE_NAME       
// MAGIC ,BILL_TO_LAST_NAME         
// MAGIC ,BILL_TO_ADDRESS_LINE_1    
// MAGIC ,BILL_TO_ADDRESS_LINE_2    
// MAGIC ,BILL_TO_CITY              
// MAGIC ,BILL_TO_STATE_OR_PROV_CD  
// MAGIC ,BILL_TO_POSTAL_CD         
// MAGIC ,BILL_TO_COUNTRY           
// MAGIC ,BILL_TO_EMAIL_ADDRESS     
// MAGIC  ,BILL_TO_PHONE_NBR         
// MAGIC  ,TRADE_ID                  
// MAGIC  ,CUSTOMER_TYPE_CD
// MAGIC  ,MEMBERSHIP_LEVEL_CD
// MAGIC  ,SUBORDERS_CNT       --SUTLEJ CHANGE HERE				   
// MAGIC  ,MARKET_CD           --SEPIK      
// MAGIC  ,STORE_ORDER_SOURCE        
// MAGIC  ,OPERATOR_ID               
// MAGIC  ,CANCEL_FLAG               
// MAGIC  ,TRAINING_MODE_FLAG        
// MAGIC  ,REGISTRY_ORDER_FLAG        --SEPIK 
// MAGIC  ,DRAFT_ORDER_FLAG           --SEPIK 
// MAGIC  ,ORDER_PURPOSE              --SEPIK
// MAGIC  ,SOURCE_CODE_DISCOUNT_AMT     --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,GIFTWRAP_WAIVED_FLAG                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,SHIPPING_WAIVED_FLAG                             --change by Sutlej1, field from wsi_addnl_order_data 																											   
// MAGIC  ,CATALOG_NM                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,CATALOG_YEAR                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,REGISTRY_ID                                      --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,ORDER_SOURCE_TYPE_CD                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,GIFT_FLAG                                        --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,WAREHOUSE_SITE_CD                                --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,PAPER_FLAG                                       --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,STORE_ASSOCIATE_NM                               --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,TENTATIVE_REFUND_AMT                             --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,CONTACT_FLAG                                     --change by Sutlej1, field from wsi_addnl_order_data																												
// MAGIC  ,RETURN_CARRIER                                   --change by Sutlej1, field from wsi_addnl_order_data																											   
// MAGIC  ,REGISTRY_TYPE_CD                                 --change by Sutlej1, field from wsi_addnl_order_data														 
// MAGIC  ,TXN_BEGIN_TS              
// MAGIC  ,TXN_END_TS                
// MAGIC  ,RETURN_TYPE_CD             --SEPIK 
// MAGIC  ,RETURN_DELIVERY_HUB        --SEPIK 
// MAGIC  ,RETURN_MANAGING_HUB         --SEPIK 
// MAGIC  ,RETURN_CARRIER_CD          --SEPIK 
// MAGIC  ,RETURN_METHOD_CD           --SEPIK 
// MAGIC  ,RECEIPT_PREFERENCE        
// MAGIC  ,GROSS_AMT                 
// MAGIC  ,NET_AMT                   
// MAGIC  ,TAX_AMT                   
// MAGIC  ,DOCUMENT_TYPE_CD           --SEPIK 
// MAGIC  ,REFUND_POLICY              --SEPIK 
// MAGIC  ,new_INSERT_TS AS INSERT_TS                 
// MAGIC  ,new_UPDATE_TS AS UPDATE_TS                 
// MAGIC from ORDER_HEADER_STG1_VIEW4 STG1
// MAGIC CROSS JOIN (
// MAGIC     SELECT MAX(ORDER_HEADER_KEY) AS MAX_ORDER_HEADER_KEY FROM $L2_ANALYTICS_TABLES.Order_HEADER WHERE ORDER_HEADER_KEY <> -1
// MAGIC 	) TARGET_MAX
// MAGIC     
// MAGIC     )

// COMMAND ----------

// DBTITLE 1,OH: Retrieve previous known values via a backfill in case of inconsistent schema
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_HEADER_STG1_final_BKFIL AS 
// MAGIC ( 
// MAGIC select 
// MAGIC  ORDER_HEADER_KEY
// MAGIC ,ORDER_HEADER_ID
// MAGIC ,SOURCE_SYSTEM
// MAGIC ,ORDER_ID
// MAGIC ,last(ORIG_ORDER_ID, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ORIG_ORDER_ID
// MAGIC ,last(EXCHANGE_ORDER_ID, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as EXCHANGE_ORDER_ID    --SEPIK
// MAGIC ,last(ORDER_TYPE_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ORDER_TYPE_CD     --column order change
// MAGIC ,ORDER_DT        --column order change
// MAGIC ,LOCATION_KEY
// MAGIC ,LOCATION_ID
// MAGIC ,HOUSEHOLD_ID       --SEPIK
// MAGIC ,HOUSEHOLD_KEY      --SEPIK
// MAGIC ,last(DTC_ENTRY_TYPE_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as DTC_ENTRY_TYPE_CD
// MAGIC ,TXN_TYPE_CD
// MAGIC ,last(PAYMENT_STATUS_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as PAYMENT_STATUS_CD  --SEPIK
// MAGIC ,CONCEPT_CD
// MAGIC ,last(CURRENCY_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CURRENCY_CD
// MAGIC ,LINKED_ORDER_HEADER_KEY
// MAGIC ,LINKED_ORDER_HEADER_ID
// MAGIC ,POS_WORKSTATION_ID
// MAGIC ,POS_WORKSTATION_SEQ_NBR
// MAGIC ,RTC_LOCATION_KEY
// MAGIC ,RTC_LOCATION_ID
// MAGIC ,LOYALTY_ACCOUNT_KEY
// MAGIC ,last(LOYALTY_ACCOUNT_ID, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as LOYALTY_ACCOUNT_ID
// MAGIC ,last(CUSTOMER_KEY, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUSTOMER_KEY	
// MAGIC ,last(CUSTOMER_ID, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUSTOMER_ID
// MAGIC ,last(EMPLOYEE_ID, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as EMPLOYEE_ID  --SEPIK
// MAGIC ,MATCH_METHOD_CD
// MAGIC ,FIRST_EFFECTIVE_TS
// MAGIC ,LAST_EFFECTIVE_TS
// MAGIC ,last(BILL_TO_FIRST_NAME, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_FIRST_NAME
// MAGIC ,last(BILL_TO_MIDDLE_NAME, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_MIDDLE_NAME
// MAGIC ,last(BILL_TO_LAST_NAME, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_LAST_NAME
// MAGIC ,last(BILL_TO_ADDRESS_LINE_1, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_ADDRESS_LINE_1
// MAGIC ,last(BILL_TO_ADDRESS_LINE_2, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_ADDRESS_LINE_2
// MAGIC ,last(BILL_TO_CITY, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_CITY
// MAGIC ,last(BILL_TO_STATE_OR_PROV_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_STATE_OR_PROV_CD
// MAGIC ,last(BILL_TO_POSTAL_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_POSTAL_CD
// MAGIC ,last(BILL_TO_COUNTRY, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_COUNTRY
// MAGIC ,last(BILL_TO_EMAIL_ADDRESS, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_EMAIL_ADDRESS
// MAGIC ,last(BILL_TO_PHONE_NBR, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BILL_TO_PHONE_NBR
// MAGIC ,last(TRADE_ID, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as TRADE_ID
// MAGIC ,CUSTOMER_TYPE_CD
// MAGIC ,last(MEMBERSHIP_LEVEL_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MEMBERSHIP_LEVEL_CD
// MAGIC ,last(SUBORDERS_CNT, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SUBORDERS_CNT --Sutlej																																											  
// MAGIC ,last(MARKET_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MARKET_CD  --SEPIK
// MAGIC ,last(STORE_ORDER_SOURCE, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as STORE_ORDER_SOURCE
// MAGIC ,OPERATOR_ID
// MAGIC ,CANCEL_FLAG
// MAGIC ,TRAINING_MODE_FLAG
// MAGIC ,last(REGISTRY_ORDER_FLAG, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as REGISTRY_ORDER_FLAG   --SEPIK
// MAGIC ,last(DRAFT_ORDER_FLAG, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as DRAFT_ORDER_FLAG   --SEPIK
// MAGIC ,last(ORDER_PURPOSE, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ORDER_PURPOSE           --SEPIK
// MAGIC ,last(SOURCE_CODE_DISCOUNT_AMT, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SOURCE_CODE_DISCOUNT_AMT--SUTLEJ FROM HERE
// MAGIC ,last(GIFTWRAP_WAIVED_FLAG, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as GIFTWRAP_WAIVED_FLAG
// MAGIC ,last(SHIPPING_WAIVED_FLAG, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIPPING_WAIVED_FLAG
// MAGIC ,last(CATALOG_NM, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CATALOG_NM
// MAGIC ,last(CATALOG_YEAR, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CATALOG_YEAR
// MAGIC ,last(REGISTRY_ID , true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as REGISTRY_ID 
// MAGIC ,last(ORDER_SOURCE_TYPE_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ORDER_SOURCE_TYPE_CD
// MAGIC ,last(GIFT_FLAG, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as GIFT_FLAG
// MAGIC ,last(WAREHOUSE_SITE_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as WAREHOUSE_SITE_CD
// MAGIC ,last(PAPER_FLAG, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as PAPER_FLAG
// MAGIC ,last(STORE_ASSOCIATE_NM, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as STORE_ASSOCIATE_NM
// MAGIC ,last(TENTATIVE_REFUND_AMT, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as TENTATIVE_REFUND_AMT
// MAGIC ,last(CONTACT_FLAG, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CONTACT_FLAG
// MAGIC ,last(RETURN_CARRIER, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_CARRIER--SUTLEJ TILL HERE
// MAGIC ,last(REGISTRY_TYPE_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as REGISTRY_TYPE_CD				
// MAGIC ,TXN_BEGIN_TS
// MAGIC ,TXN_END_TS
// MAGIC ,last(RETURN_TYPE_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_TYPE_CD    --SEPIK
// MAGIC ,last(RETURN_DELIVERY_HUB, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_DELIVERY_HUB --SEPIK
// MAGIC ,last(RETURN_MANAGING_HUB, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_MANAGING_HUB --SEPIK
// MAGIC ,last(RETURN_CARRIER_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_CARRIER_CD  --SEPIK
// MAGIC ,last(RETURN_METHOD_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_METHOD_CD --SEPIK
// MAGIC ,RECEIPT_PREFERENCE
// MAGIC ,last(GROSS_AMT, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as GROSS_AMT
// MAGIC ,last(NET_AMT, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as NET_AMT
// MAGIC ,last(TAX_AMT, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as TAX_AMT
// MAGIC ,last(DOCUMENT_TYPE_CD, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as DOCUMENT_TYPE_CD --SEPIK
// MAGIC ,last(REFUND_POLICY, true) over (partition by order_header_id order by FIRST_EFFECTIVE_TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as REFUND_POLICY  --SEPIK
// MAGIC ,INSERT_TS
// MAGIC ,UPDATE_TS 
// MAGIC from ORDER_HEADER_STG1_final STG1
// MAGIC 
// MAGIC 
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OH: Copy final stage data to physical storage for snowflake consumption
val dropOhSnoflkStg = s"DROP TABLE IF exists $L2_STAGE.PRE_SNOFLK_OH_STG"
spark.sql(dropOhSnoflkStg);

dbutils.fs.rm("/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_oh_stg/", true);

val createOhSnoflkStg = s"""CREATE TABLE $L2_STAGE.PRE_SNOFLK_OH_STG
USING DELTA
LOCATION '/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_oh_stg/'
AS SELECT * FROM ORDER_HEADER_STG1_final_BKFIL""";
spark.sql(createOhSnoflkStg);

// COMMAND ----------

// DBTITLE 1,OH: Merge into Target
// MAGIC %sql
// MAGIC 
// MAGIC MERGE INTO $L2_ANALYTICS_TABLES.ORDER_HEADER tgt
// MAGIC USING $L2_STAGE.PRE_SNOFLK_OH_STG stg
// MAGIC ON tgt.source_system = 'STERLING_DTC'
// MAGIC and tgt.ORDER_dt = stg.ORDER_dt 
// MAGIC and tgt.ORDER_HEADER_ID = stg.ORDER_HEADER_ID
// MAGIC and to_timestamp(stg.FIRST_EFFECTIVE_TS) = to_timestamp(tgt.FIRST_EFFECTIVE_TS)
// MAGIC WHEN MATCHED AND to_timestamp(tgt.LAST_EFFECTIVE_TS) != to_timestamp(stg.LAST_EFFECTIVE_TS)  --if existing record, and there is a new LAST_EFFECTIVE_TS, perform update. If LAST_EFFECTIVE_TS is same, do nothing (no change)
// MAGIC   THEN UPDATE SET tgt.LAST_EFFECTIVE_TS = stg.LAST_EFFECTIVE_TS, tgt.UPDATE_TS = stg.UPDATE_TS 
// MAGIC WHEN NOT MATCHED --and tgt.FIRST_EFFECTIVE_TS < stg.FIRST_EFFECTIVE_TS
// MAGIC   THEN INSERT * 

// COMMAND ----------

// DBTITLE 1,Release Lock here
// MAGIC %scala
// MAGIC releaseLock(lockDirPath)
// MAGIC spark.sql("CLEAR CACHE")

// COMMAND ----------

// DBTITLE 1,OL: Keep only 1 Composite Primary Key Within the Same Batch
var df_stg = table(L2_STAGE + """.sterling_increment_stage_delta_ol_str""")
var df_uniq1_pk = df_stg.dropDuplicates("first_effective_ts", "ORDER_LINE_ID")
df_uniq1_pk.createOrReplaceTempView("FLAT_OL")

// COMMAND ----------

// MAGIC 
// MAGIC 
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_LINE_STG1_VIEW3 AS 
// MAGIC ( 
// MAGIC --SELECT  COALESCE(TARGET.ORDER_LINE_KEY, -1) = -1 AS ORDER_LINE_KEY
// MAGIC SELECT  COALESCE(TARGET.ORDER_LINE_KEY, -1)  AS ORDER_LINE_KEY 
// MAGIC , ol.ORDER_LINE_ID as ORDER_LINE_ID
// MAGIC , COALESCE(oh.ORDER_HEADER_KEY,  -1) as ORDER_HEADER_KEY --COLUMN ORDER CHANGE
// MAGIC ,-1 as CHAINED_FROM_ORDER_HEADER_KEY                                                   --SEPIK
// MAGIC , case when trim(ol.CHAINED_FROM_ORDER_HEADER_ID) = '' then null else ol.CHAINED_FROM_ORDER_HEADER_ID end as CHAINED_FROM_ORDER_HEADER_ID --SEPIK
// MAGIC ,-1 as CHAINED_FROM_ORDER_LINE_KEY                                                  --SEPIK
// MAGIC , case when trim(ol.CHAINED_FROM_ORDER_LINE_ID) = '' then null else ol.CHAINED_FROM_ORDER_LINE_ID end as CHAINED_FROM_ORDER_LINE_ID      --SEPIK
// MAGIC , ol.SOURCE_SYSTEM as SOURCE_SYSTEM
// MAGIC , ol.ORDER_DT as ORDER_DT --COLUMN ORDER CHANGE
// MAGIC , coalesce(trim(ol.ORDER_LINE_TYPE), '') as ORDER_LINE_TYPE --COLUMN ORDER CHANGE
// MAGIC , coalesce(ol.PRIME_LINE_SEQ_NBR,'') as PRIME_LINE_SEQ_NBR
// MAGIC , case when trim(ol.SUB_LINE_SEQ_NBR)  = '' then null else ol.SUB_LINE_SEQ_NBR end as SUB_LINE_SEQ_NBR
// MAGIC , COALESCE(linkedoh.ORDER_HEADER_KEY,  -1) as LINKED_ORDER_HEADER_KEY
// MAGIC , case when trim(ol.LINKED_ORDER_HEADER_ID) = '' then null else ol.LINKED_ORDER_HEADER_ID end as LINKED_ORDER_HEADER_ID
// MAGIC , COALESCE(linked.ORDER_LINE_KEY,  -1)  as LINKED_ORDER_LINE_KEY
// MAGIC , case when trim(ol.LINKED_ORDER_LINE_ID) = '' then null else ol.LINKED_ORDER_LINE_ID end as LINKED_ORDER_LINE_ID
// MAGIC , COALESCE(itm.ITEM_KEY,  -1)  as ITEM_KEY
// MAGIC , case when trim(ol.ORDER_ITEM_ID)  = '' then null else ol.ORDER_ITEM_ID end as ORDER_ITEM_ID
// MAGIC , case when trim(ol.ORDER_ITEM_TYPE_CD)  = '' then null else ol.ORDER_ITEM_TYPE_CD end  as ORDER_ITEM_TYPE_CD
// MAGIC , case when trim(ol.ORDER_ITEM_NAME)  = '' then null else ol.ORDER_ITEM_NAME end  as ORDER_ITEM_NAME
// MAGIC , case when trim(ol.GIFT_REGISTRY_KEY)  = '' then null else ol.GIFT_REGISTRY_KEY end  as GIFT_REGISTRY_KEY
// MAGIC , case when trim(ol.GIFT_REGISTRY_ID)  = '' then null else ol.GIFT_REGISTRY_ID end  as GIFT_REGISTRY_ID
// MAGIC , ol.FIRST_EFFECTIVE_TS as FIRST_EFFECTIVE_TS
// MAGIC , null as LAST_EFFECTIVE_TS
// MAGIC , case when trim(ol.UPC_CD )  = '' then null else ol.UPC_CD end as UPC_CD
// MAGIC , case when trim(ol.KIT_CD )  = '' then null else ol.KIT_CD end  as KIT_CD
// MAGIC , case when trim(ol.KIT_QTY)  = '' then null else ol.KIT_QTY end  as KIT_QTY
// MAGIC , case when trim(ol.PRODUCT_LINE)  = '' then null else ol.PRODUCT_LINE end  as PRODUCT_LINE
// MAGIC , case when trim(ol.BUNDLE_PARENT_ORDER_LINE_ID)  = '' then null else ol.BUNDLE_PARENT_ORDER_LINE_ID end  as BUNDLE_PARENT_ORDER_LINE_ID
// MAGIC , case when trim(ol.ORDER_QTY)  = '' then null else  ol.ORDER_QTY  end as  ORDER_QTY
// MAGIC , case when trim(ol.ORIG_ORDER_QTY)  = '' then null else  ol.ORIG_ORDER_QTY  end as  ORIG_ORDER_QTY
// MAGIC , case when trim(ol.ACT_SALE_UNIT_PRICE_AMT)  = '' then null else  ol.ACT_SALE_UNIT_PRICE_AMT  end as  ACT_SALE_UNIT_PRICE_AMT
// MAGIC , case when trim(ol.REG_SALE_UNIT_PRICE_AMT)  = '' then null else  ol.REG_SALE_UNIT_PRICE_AMT  end as  REG_SALE_UNIT_PRICE_AMT
// MAGIC , ol.ORDER_QTY   * ol.ACT_SALE_UNIT_PRICE_AMT as EXTENDED_AMT
// MAGIC , case when trim(ol.EXTENDED_DISCOUNT_AMT)  = '' then null else  ol.EXTENDED_DISCOUNT_AMT  end as  EXTENDED_DISCOUNT_AMT
// MAGIC , case when trim(ol.TAX_AMT)  = '' then null else  ol.TAX_AMT  end as  TAX_AMT
// MAGIC , case when trim(ol.TAXABLE_AMT)  = '' then null else  ol.TAXABLE_AMT  end as  TAXABLE_AMT
// MAGIC , case when trim(ol.GIFT_CARD_AMT)  = '' then null else  ol.GIFT_CARD_AMT  end as  GIFT_CARD_AMT
// MAGIC , case when trim(ol.GIFTWRAP_CHARGE_AMT)  = '' then null else  ol.GIFTWRAP_CHARGE_AMT  end as  GIFTWRAP_CHARGE_AMT
// MAGIC , case when trim(ol.LINE_TOTAL_AMT)  = '' then null else  ol.LINE_TOTAL_AMT  end as  LINE_TOTAL_AMT
// MAGIC , case when trim(ol.MERCHANDISE_CHARGE_AMT)  = '' then null else  ol.MERCHANDISE_CHARGE_AMT  end as  MERCHANDISE_CHARGE_AMT
// MAGIC , case when trim(ol.MONO_PZ_CHARGE_AMT)  = '' then null else  ol.MONO_PZ_CHARGE_AMT  end as  MONO_PZ_CHARGE_AMT
// MAGIC , case when trim(ol.MISC_CHARGE_AMT)  = '' then null else  ol.MISC_CHARGE_AMT  end as  MISC_CHARGE_AMT
// MAGIC , case when trim(ol.SHIPPING_HANDLING_CHARGE_AMT)  = '' then null else  ol.SHIPPING_HANDLING_CHARGE_AMT  end as  SHIPPING_HANDLING_CHARGE_AMT
// MAGIC , case when trim(ol.SHIPPING_SURCHARGE_AMT)  = '' then null else  ol.SHIPPING_SURCHARGE_AMT  end as  SHIPPING_SURCHARGE_AMT
// MAGIC , case when trim(ol.DONATION_AMT)  = '' then null else  ol.DONATION_AMT  end as  DONATION_AMT
// MAGIC , case when trim(ol.ASSOCIATE_ID)  = '' then null else  ol.ASSOCIATE_ID  end as  ASSOCIATE_ID
// MAGIC , case when trim(ol.ENTRY_METHOD)  = '' then null else  ol.ENTRY_METHOD  end as  ENTRY_METHOD 
// MAGIC , case when trim(ol.gift_message) = '' then null else ol.gift_message end as GIFT_MESSAGE 
// MAGIC , case when trim(ol.VOID_FLAG)  = '' then null else  ol.VOID_FLAG  end as  VOID_FLAG
// MAGIC , case when trim(ol.REPO_FLAG)  = '' then null else  ol.REPO_FLAG  end as  REPO_FLAG
// MAGIC , case when trim(ol.TAXABLE_FLAG)  = '' then null else  ol.TAXABLE_FLAG  end as  TAXABLE_FLAG
// MAGIC , case when trim(ol.PICKABLE_FLAG)  = '' then null else  ol.PICKABLE_FLAG  end as  PICKABLE_FLAG
// MAGIC , case when trim(ol.GIFT_FLAG)  = '' then null else  ol.GIFT_FLAG  end as  GIFT_FLAG
// MAGIC , case when trim(ol.HOLD_FLAG)  = '' then null else  ol.HOLD_FLAG  end as  HOLD_FLAG
// MAGIC , case when trim(ol.HOLD_REASON)  = '' then null else  ol.HOLD_REASON  end as  HOLD_REASON
// MAGIC , case when trim(ol.ORIG_BACKORDER_FLAG)  = '' then null else  ol.ORIG_BACKORDER_FLAG  end as  ORIG_BACKORDER_FLAG
// MAGIC , case when trim(ol.SUBORDER_COMPLEXITY_GROUP_ID)  = '' then null else  ol.SUBORDER_COMPLEXITY_GROUP_ID  end as  SUBORDER_COMPLEXITY_GROUP_ID --Sutlej																																							   
// MAGIC , case when trim(ol.DELIVERY_CHOICE) = '' then null else ol.DELIVERY_CHOICE end as DELIVERY_CHOICE
// MAGIC , case when trim(ol.MODIFICATION_REASON_CD) = '' then null else ol.MODIFICATION_REASON_CD end as MODIFICATION_REASON_CD
// MAGIC , case when trim(ol.MODIFICATION_REASON_CD_DESC) = '' then null else ol.MODIFICATION_REASON_CD_DESC end as MODIFICATION_REASON_CD_DESC
// MAGIC , case when trim(ol.RETURN_ACTION_CD)  = '' then null else  ol.RETURN_ACTION_CD  end as  RETURN_ACTION_CD
// MAGIC , case when trim(ol.RETURN_ACTION)  = '' then null else  ol.RETURN_ACTION  end as  RETURN_ACTION
// MAGIC , case when trim(ol.RETURN_REASON_CD)  = '' then null else  ol.RETURN_REASON_CD  end as  RETURN_REASON_CD
// MAGIC , case when trim(ol.RETURN_REASON_DESC)  = '' then null else  ol.RETURN_REASON_DESC  end as  RETURN_REASON_DESC
// MAGIC , case when trim(ol.RETURN_SUB_REASON_CD)  = '' then null else  ol.RETURN_SUB_REASON_CD  end as  RETURN_SUB_REASON_CD
// MAGIC , case when trim(ol.SHIP_TO_FIRST_NAME)  = '' then null else  ol.SHIP_TO_FIRST_NAME  end as  SHIP_TO_FIRST_NAME
// MAGIC , case when trim(ol.SHIP_TO_MIDDLE_NAME)  = '' then null else  ol.SHIP_TO_MIDDLE_NAME  end as  SHIP_TO_MIDDLE_NAME
// MAGIC , case when trim(ol.SHIP_TO_LAST_NAME)  = '' then null else  ol.SHIP_TO_LAST_NAME  end as  SHIP_TO_LAST_NAME
// MAGIC , case when trim(ol.SHIP_TO_ADDRESS_LINE_1)  = '' then null else  ol.SHIP_TO_ADDRESS_LINE_1  end as  SHIP_TO_ADDRESS_LINE_1
// MAGIC , case when trim(ol.SHIP_TO_ADDRESS_LINE_2)  = '' then null else  ol.SHIP_TO_ADDRESS_LINE_2  end as  SHIP_TO_ADDRESS_LINE_2
// MAGIC , case when trim(ol.SHIP_TO_CITY)  = '' then null else  ol.SHIP_TO_CITY  end as  SHIP_TO_CITY
// MAGIC , case when trim(ol.SHIP_TO_STATE_OR_PROV)  = '' then null else  ol.SHIP_TO_STATE_OR_PROV  end as  SHIP_TO_STATE_OR_PROV
// MAGIC , case when trim(ol.SHIP_TO_POSTAL_CD)  = '' then null else  ol.SHIP_TO_POSTAL_CD  end as  SHIP_TO_POSTAL_CD
// MAGIC , case when trim(ol.SHIP_TO_COUNTRY)  = '' then null else  ol.SHIP_TO_COUNTRY  end as  SHIP_TO_COUNTRY
// MAGIC , case when trim(ol.SHIP_TO_EMAIL_ADDRESS)  = '' then null else  ol.SHIP_TO_EMAIL_ADDRESS  end as  SHIP_TO_EMAIL_ADDRESS
// MAGIC , case when trim(ol.SHIP_TO_PHONE_NBR)  = '' then null else  ol.SHIP_TO_PHONE_NBR  end as  SHIP_TO_PHONE_NBR
// MAGIC , case when trim(ol.DELIVERY_METHOD)  = '' then null else  ol.DELIVERY_METHOD  end as DELIVERY_METHOD           --SEPIK   
// MAGIC , case when trim(ol.FULFILLMENT_TYPE)  = '' then null else  ol.FULFILLMENT_TYPE  end as FULFILLMENT_TYPE             --SEPIK
// MAGIC , case when trim(ol.DTC_SUBORDER_NBR)  = '' then null else  ol.DTC_SUBORDER_NBR  end as DTC_SUBORDER_NBR             --SEPIK
// MAGIC , case when trim(ol.ADDITIONAL_LINE_TYPE_CD)  = '' then null else  ol.ADDITIONAL_LINE_TYPE_CD  end as ADDITIONAL_LINE_TYPE_CD  --SEPIK    
// MAGIC , case when trim(ol.ORIG_CONFIRMED_QTY)  = '' then null else  ol.ORIG_CONFIRMED_QTY  end as ORIG_CONFIRMED_QTY           --SEPIK
// MAGIC , case when trim(ol.RESKU_FLAG)  = '' then null else  ol.RESKU_FLAG  end as RESKU_FLAG                   --SEPIK
// MAGIC , case when trim(ol.CONSOLIDATOR_ADDRESS_CD)  = '' then null else  ol.CONSOLIDATOR_ADDRESS_CD  end as  CONSOLIDATOR_ADDRESS_CD
// MAGIC , case when trim(ol.MERGE_NODE_CD)  = '' then null else  ol.MERGE_NODE_CD  end as  MERGE_NODE_CD
// MAGIC , case when trim(ol.SHIP_NODE_CD)  = '' then null else  ol.SHIP_NODE_CD  end as  SHIP_NODE_CD
// MAGIC , case when trim(ol.RECEIVING_NODE_CD)  = '' then null else  ol.RECEIVING_NODE_CD  end as  RECEIVING_NODE_CD
// MAGIC , case when trim(ol.LEVEL_OF_SERVICE)  = '' then null else  ol.LEVEL_OF_SERVICE  end as  LEVEL_OF_SERVICE
// MAGIC , case when trim(ol.CARRIER_SERVICE_CD)  = '' then null else  ol.CARRIER_SERVICE_CD  end as  CARRIER_SERVICE_CD
// MAGIC , case when trim(ol.CARRIER_CD)  = '' then null else  ol.CARRIER_CD  end as  CARRIER_CD
// MAGIC , case when trim(ol.ACCESS_POINT_CD)  = '' then null else  ol.ACCESS_POINT_CD  end as  ACCESS_POINT_CD
// MAGIC , case when trim(ol.ACCESS_POINT_ID)  = '' then null else  ol.ACCESS_POINT_ID  end as  ACCESS_POINT_ID
// MAGIC , case when trim(ol.ACCESS_POINT_NM)  = '' then null else  ol.ACCESS_POINT_NM  end as  ACCESS_POINT_NM
// MAGIC , case when trim(ol.MINIMUM_SHIP_DT)  = '' then null else  ol.MINIMUM_SHIP_DT  end as  MINIMUM_SHIP_DT
// MAGIC , case when trim(ol.REQUESTED_SHIP_DT)  = '' then null else  ol.REQUESTED_SHIP_DT  end as  REQUESTED_SHIP_DT
// MAGIC , case when trim(ol.REQUESTED_DELIVERY_DT)  = '' then null else  ol.REQUESTED_DELIVERY_DT  end as  REQUESTED_DELIVERY_DT
// MAGIC , case when trim(ol.EARLIEST_SCHEDULE_DT)  = '' then null else  ol.EARLIEST_SCHEDULE_DT  end as  EARLIEST_SCHEDULE_DT
// MAGIC , case when trim(ol.EARLIEST_DELIVERY_DT)  = '' then null else  ol.EARLIEST_DELIVERY_DT  end as  EARLIEST_DELIVERY_DT
// MAGIC , case when trim(ol.PROMISED_APPT_END_DT)  = '' then null else  ol.PROMISED_APPT_END_DT  end as  PROMISED_APPT_END_DT
// MAGIC , case when trim(ol.SPLIT_QTY)  = '' then null else  ol.SPLIT_QTY  end as  SPLIT_QTY
// MAGIC , case when trim(ol.SHIPPED_QTY)  = '' then null else  ol.SHIPPED_QTY  end as  SHIPPED_QTY
// MAGIC , case when trim(ol.FILL_QTY)  = '' then null else  ol.FILL_QTY  end as  FILL_QTY
// MAGIC , case when trim(ol.WEIGHTED_AVG_COST)  = '' then null else  ol.WEIGHTED_AVG_COST  end as  WEIGHTED_AVG_COST
// MAGIC , case when trim(ol.DIRECT_SHIP_FLAG)  = '' then null else  ol.DIRECT_SHIP_FLAG  end as  DIRECT_SHIP_FLAG
// MAGIC , case when trim(ol.UNIT_COST)  = '' then null else  ol.UNIT_COST  end as  UNIT_COST
// MAGIC , case when trim(ol.LABOR_COST)  = '' then null else  ol.LABOR_COST  end as  LABOR_COST
// MAGIC , case when trim(ol.LABOR_SKU)  = '' then null else  ol.LABOR_SKU  end as  LABOR_SKU
// MAGIC , case when trim(ol.CUSTOMER_LEVEL_OF_SERVICE) = '' then null else ol.CUSTOMER_LEVEL_OF_SERVICE end as CUSTOMER_LEVEL_OF_SERVICE
// MAGIC , case when trim(ol.RETURN_POLICY)  = '' then null else  ol.RETURN_POLICY  end as RETURN_POLICY	   --SEPIK           
// MAGIC , case when trim(ol.RETURN_POLICY_CHECK_OVERRIDE_FLAG)  = '' then null else  ol.RETURN_POLICY_CHECK_OVERRIDE_FLAG  end as RETURN_POLICY_CHECK_OVERRIDE_FLAG --SEPIK
// MAGIC , case when trim(ol.PRODUCT_AVAILABILITY_DT)  = '' then null else  ol.PRODUCT_AVAILABILITY_DT  end as PRODUCT_AVAILABILITY_DT --SEPIK      
// MAGIC , case when trim(ol.ECDD_OVERRIDDEN_FLAG)  = '' then null else  ol.ECDD_OVERRIDDEN_FLAG  end as ECDD_OVERRIDDEN_FLAG          --SEPIK
// MAGIC , case when trim(ol.ECDD_INVOKED_FLAG)  = '' then null else  ol.ECDD_INVOKED_FLAG  end as ECDD_INVOKED_FLAG             --SEPIK
// MAGIC , case when trim(ol.VAS_GIFT_WRAP_CD)  = '' then null else  ol.VAS_GIFT_WRAP_CD  end as VAS_GIFT_WRAP_CD			--SEPIK
// MAGIC , case when trim(ol.VAS_MONO_FLAG)  = '' then null else  ol.VAS_MONO_FLAG  end as VAS_MONO_FLAG                 --SEPIK
// MAGIC , case when trim(ol.VAS_PZ_FLAG)  = '' then null else  ol.VAS_PZ_FLAG  end as VAS_PZ_FLAG					--SEPIK
// MAGIC , case when trim(ol.BO_NOTIFICATION_NBR)  = '' then null else  ol.BO_NOTIFICATION_NBR  end as BO_NOTIFICATION_NBR	--NILE		
// MAGIC , null as INSERT_TS
// MAGIC , null as UPDATE_TS
// MAGIC 
// MAGIC FROM FLAT_OL ol
// MAGIC 
// MAGIC LEFT JOIN 
// MAGIC   $L2_ANALYTICS_TABLES.ORDER_LINE TARGET
// MAGIC   ON TARGET.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC   --AND TARGET.ORDER_DT = ol.ORDER_DT
// MAGIC   AND TARGET.last_effective_ts = to_timestamp('9999-12-31T23:59:59')
// MAGIC   AND trim(TARGET.ORDER_LINE_ID) = trim(ol.ORDER_LINE_ID)
// MAGIC     
// MAGIC     
// MAGIC LEFT JOIN 
// MAGIC   $L2_ANALYTICS_TABLES.ORDER_LINE linked
// MAGIC   ON linked.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC   --AND linked.ORDER_DT = ol.ORDER_DT --bec return OL may have different order dates as per sterling
// MAGIC   AND linked.last_effective_ts = to_timestamp('9999-12-31T23:59:59')
// MAGIC   AND trim(linked.ORDER_LINE_ID) = trim(ol.LINKED_ORDER_LINE_ID)
// MAGIC 
// MAGIC 
// MAGIC LEFT JOIN 
// MAGIC     $L2_ANALYTICS_TABLES.ORDER_header oh
// MAGIC     ON oh.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC     --AND oh.ORDER_DT = ol.ORDER_DT
// MAGIC     AND oh.last_effective_ts = to_timestamp('9999-12-31T23:59:59')
// MAGIC     AND trim(oh.ORDER_header_ID) = trim(ol.ORDER_HEADER_ID)
// MAGIC     
// MAGIC LEFT JOIN L2_ANALYTICS.ITEM itm
// MAGIC ON ol.ORDER_ITEM_ID = itm.ITEM_ID
// MAGIC AND itm.MARKET_CD = Coalesce(Oh.MARKET_CD,'USA') --SEPIK CHANGE
// MAGIC AND itm.DELETE_FLAG = 'N' --SEPIK CHANGE
// MAGIC and itm.LAST_EFFECTIVE_TS = to_timestamp('9999-12-31T23:59:59')
// MAGIC 
// MAGIC LEFT JOIN 
// MAGIC     $L2_ANALYTICS_TABLES.ORDER_header linkedoh
// MAGIC     ON linkedoh.SOURCE_SYSTEM = 'STERLING_DTC'
// MAGIC     --AND linkedoh.ORDER_DT = ol.ORDER_DT --bec return OL may have different order dates as per sterling
// MAGIC     AND linkedoh.last_effective_ts = to_timestamp('9999-12-31T23:59:59')
// MAGIC     AND trim(linkedoh.ORDER_header_ID) = trim(ol.LINKED_ORDER_HEADER_ID)
// MAGIC   
// MAGIC where ol.ORDER_LINE_ID is not null
// MAGIC 
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OL: Retrieve Active Records from Target Table (9999-12-31)
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_LINE_STG1_VIEW3_merged AS ( 
// MAGIC select tgt.* from $L2_ANALYTICS_TABLES.Order_LINE tgt inner join $L2_STAGE.sterling_increment_stage_delta_ol_str stg on --tgt.order_dt = stg.order_dt and 
// MAGIC tgt.source_system = 'STERLING_DTC' and trim(tgt.Order_LINE_ID) = trim(stg.Order_LINE_ID) and tgt.last_effective_ts =  to_timestamp('9999-12-31T23:59:59') and stg.order_line_id is not null
// MAGIC union all 
// MAGIC select * from ORDER_LINE_STG1_VIEW3 stg 
// MAGIC );

// COMMAND ----------

// DBTITLE 1,OL: Remove repeating attributes / Keep unique records
var df_stg_multibatch = spark.sql("SELECT * FROM ORDER_LINE_STG1_VIEW3_merged")
var df_uniq1_pk_multibatch = df_stg_multibatch.dropDuplicates("first_effective_ts", "ORDER_LINE_ID")
var df_uniq2_record_multibatch = df_uniq1_pk_multibatch.orderBy(col("FIRST_EFFECTIVE_TS").asc).coalesce(1).dropDuplicates(
"ORDER_LINE_ID"
,"CHAINED_FROM_ORDER_HEADER_ID" //SEPIK
,"CHAINED_FROM_ORDER_LINE_ID" //SEPIK
,"SOURCE_SYSTEM"
,"ORDER_DT"             //COLUMN ORDER CHANGE
,"ORDER_LINE_TYPE"     //COLUMN ORDER CHANGE
,"PRIME_LINE_SEQ_NBR"
,"SUB_LINE_SEQ_NBR"
,"LINKED_ORDER_HEADER_ID" 
,"LINKED_ORDER_LINE_ID"
,"ORDER_ITEM_ID"
,"ORDER_ITEM_TYPE_CD"
,"ORDER_ITEM_NAME"
,"GIFT_REGISTRY_KEY"
,"GIFT_REGISTRY_ID"
,"UPC_CD"
,"KIT_CD"
,"KIT_QTY"
,"PRODUCT_LINE"
,"BUNDLE_PARENT_ORDER_LINE_ID"
,"ORDER_QTY"
,"ORIG_ORDER_QTY"
,"ACT_SALE_UNIT_PRICE_AMT"
,"REG_SALE_UNIT_PRICE_AMT"
,"EXTENDED_AMT"
,"EXTENDED_DISCOUNT_AMT"
,"TAX_AMT"
,"TAXABLE_AMT"
,"GIFT_CARD_AMT"
,"GIFTWRAP_CHARGE_AMT"
,"LINE_TOTAL_AMT"
,"MERCHANDISE_CHARGE_AMT"
,"MONO_PZ_CHARGE_AMT"
,"MISC_CHARGE_AMT"
,"SHIPPING_HANDLING_CHARGE_AMT"
,"SHIPPING_SURCHARGE_AMT"
,"DONATION_AMT"
,"ASSOCIATE_ID"
,"ENTRY_METHOD"
,"GIFT_MESSAGE" 
,"VOID_FLAG"
,"REPO_FLAG"
,"TAXABLE_FLAG"
,"PICKABLE_FLAG"
,"GIFT_FLAG"
,"HOLD_FLAG"
,"HOLD_REASON"
,"ORIG_BACKORDER_FLAG"
,"SUBORDER_COMPLEXITY_GROUP_ID"    //Sutlej				   
,"DELIVERY_CHOICE"
,"MODIFICATION_REASON_CD"
,"MODIFICATION_REASON_CD_DESC"
,"RETURN_ACTION_CD"
,"RETURN_ACTION"
,"RETURN_REASON_CD"
,"RETURN_REASON_DESC"
,"RETURN_SUB_REASON_CD"
,"SHIP_TO_FIRST_NAME"
,"SHIP_TO_MIDDLE_NAME"
,"SHIP_TO_LAST_NAME"
,"SHIP_TO_ADDRESS_LINE_1"
,"SHIP_TO_ADDRESS_LINE_2"
,"SHIP_TO_CITY"
,"SHIP_TO_STATE_OR_PROV"
,"SHIP_TO_POSTAL_CD"
,"SHIP_TO_COUNTRY"
,"SHIP_TO_EMAIL_ADDRESS"
,"SHIP_TO_PHONE_NBR"
,"DELIVERY_METHOD"          //SEPIK
,"FULFILLMENT_TYPE"         //SEPIK
,"DTC_SUBORDER_NBR"         //SEPIK
,"ADDITIONAL_LINE_TYPE_CD" //SEPIK
,"ORIG_CONFIRMED_QTY"      //SEPIK
,"RESKU_FLAG"              //SEPIK
,"CONSOLIDATOR_ADDRESS_CD"
,"MERGE_NODE_CD"
,"SHIP_NODE_CD"
,"RECEIVING_NODE_CD"
,"LEVEL_OF_SERVICE"
,"CARRIER_SERVICE_CD"
,"CARRIER_CD"
,"ACCESS_POINT_CD"
,"ACCESS_POINT_ID"
,"ACCESS_POINT_NM"
,"MINIMUM_SHIP_DT"
,"REQUESTED_SHIP_DT"
,"REQUESTED_DELIVERY_DT"
,"EARLIEST_SCHEDULE_DT"
,"EARLIEST_DELIVERY_DT"
,"PROMISED_APPT_END_DT"
,"SPLIT_QTY"
,"SHIPPED_QTY"
,"FILL_QTY"
,"WEIGHTED_AVG_COST"
,"DIRECT_SHIP_FLAG"
,"UNIT_COST"
,"LABOR_COST"
,"LABOR_SKU"
,"CUSTOMER_LEVEL_OF_SERVICE" 
,"RETURN_POLICY"	          //SEPIK  
,"RETURN_POLICY_CHECK_OVERRIDE_FLAG" //SEPIK
,"PRODUCT_AVAILABILITY_DT"     //SEPIK
,"ECDD_OVERRIDDEN_FLAG"       //SEPIK
,"ECDD_INVOKED_FLAG"           //SEPIK
,"VAS_GIFT_WRAP_CD"			//SEPIK
,"VAS_MONO_FLAG"            //SEPIK   
,"VAS_PZ_FLAG"				//SEPIK	
,"BO_NOTIFICATION_NBR")
df_uniq2_record_multibatch.createOrReplaceTempView("df_uniq2_record_multibatch_view")

// COMMAND ----------

// DBTITLE 1,OL: Begin calculating last_effective_ts (get adjacent record's first_effective_ts)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW calculate_last_eff AS 
// MAGIC (
// MAGIC select *
// MAGIC , LEAD(FIRST_EFFECTIVE_TS) OVER (PARTITION BY Order_LINE_ID ORDER BY  FIRST_EFFECTIVE_TS)  as new_LAST_EFFECTIVE_TS
// MAGIC ,coalesce(INSERT_TS, current_timestamp) as  new_INSERT_TS --retain timestamp if already previously inserted, otherwise generate value
// MAGIC ,current_timestamp as new_UPDATE_TS --always generate value upon insert
// MAGIC from df_uniq2_record_multibatch_view ol
// MAGIC 
// MAGIC );

// COMMAND ----------

// DBTITLE 1,OL: Complete calculating last_effective_ts (applying -1 second difference, or put 9999-12-31)
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW final_last_eff AS 
// MAGIC (	Select *
// MAGIC , case when new_LAST_EFFECTIVE_TS is null then to_timestamp('9999-12-31T23:59:59')
// MAGIC   else new_LAST_EFFECTIVE_TS - INTERVAL 1 MILLISECONDS 
// MAGIC   end as final_LAST_EFFECTIVE_TS --apply -1 second difference to last effective ts of expired record
// MAGIC from calculate_last_eff ol
// MAGIC 
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OL: Purge 2nd stage table
spark.sql("""drop table if exists """ + L2_STAGE + """.ORDER_LINE_stage""")

dbutils.fs.rm("dbfs:/mnt/data/governed/l2/stage/order/order_line_stage/", true)

// COMMAND ----------

// MAGIC %sql
// MAGIC create table if not exists $L2_STAGE.ORDER_LINE_stage
// MAGIC (
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC     ORDER_LINE_KEY                  bigint           ,
// MAGIC     ORDER_LINE_ID                   varchar(40)   ,
// MAGIC     ORDER_HEADER_KEY                int           ,   --COLUMN ORDER CHANGE
// MAGIC     CHAINED_FROM_ORDER_HEADER_KEY   BIGINT        ,  --SEPIK
// MAGIC     CHAINED_FROM_ORDER_HEADER_ID    varchar(50)   ,                      --SEPIK
// MAGIC     CHAINED_FROM_ORDER_LINE_KEY     BIGINT          ,      --SEPIK
// MAGIC     CHAINED_FROM_ORDER_LINE_ID      varchar(50)   ,                --SEPIK
// MAGIC     SOURCE_SYSTEM                   varchar(20)   ,   
// MAGIC     ORDER_DT                        date          ,   --COLUMN ORDER CHANGE
// MAGIC     ORDER_LINE_TYPE                 varchar(50)   ,   --COLUMN ORDER CHANGE
// MAGIC     PRIME_LINE_SEQ_NBR              int           ,
// MAGIC     SUB_LINE_SEQ_NBR                int           ,
// MAGIC     LINKED_ORDER_HEADER_KEY         int           ,
// MAGIC     LINKED_ORDER_HEADER_ID          varchar(50)   ,
// MAGIC     LINKED_ORDER_LINE_KEY           bigint           ,
// MAGIC     LINKED_ORDER_LINE_ID            varchar(50)   ,
// MAGIC     ITEM_KEY                        int           ,
// MAGIC     ORDER_ITEM_ID                   varchar(50)   ,
// MAGIC     ORDER_ITEM_TYPE_CD              varchar(50)   ,
// MAGIC     ORDER_ITEM_NAME                 varchar(100)  ,
// MAGIC     GIFT_REGISTRY_KEY               int           ,
// MAGIC     GIFT_REGISTRY_ID                varchar(50)   ,
// MAGIC     FIRST_EFFECTIVE_TS              timestamp     ,
// MAGIC     LAST_EFFECTIVE_TS               timestamp     ,
// MAGIC     UPC_CD                          varchar(40)   ,
// MAGIC     KIT_CD                          varchar(20)   ,
// MAGIC     KIT_QTY                         int           ,
// MAGIC     PRODUCT_LINE                    varchar(100)  ,
// MAGIC     BUNDLE_PARENT_ORDER_LINE_ID     varchar(50)   ,
// MAGIC     ORDER_QTY                       int           ,
// MAGIC     ORIG_ORDER_QTY                  int           ,
// MAGIC     ACT_SALE_UNIT_PRICE_AMT         decimal(15, 2),
// MAGIC     REG_SALE_UNIT_PRICE_AMT         decimal(15, 2),
// MAGIC     EXTENDED_AMT                    decimal(15, 2),
// MAGIC     EXTENDED_DISCOUNT_AMT           decimal(15, 2),
// MAGIC     TAX_AMT                         decimal(15, 2),
// MAGIC     TAXABLE_AMT                     decimal(15, 2),
// MAGIC     GIFT_CARD_AMT                   decimal(15, 2),
// MAGIC     GIFTWRAP_CHARGE_AMT             decimal(15, 2),
// MAGIC     LINE_TOTAL_AMT                  decimal(15, 2),
// MAGIC     MERCHANDISE_CHARGE_AMT          decimal(15, 2),
// MAGIC     MONO_PZ_CHARGE_AMT              decimal(15, 2),
// MAGIC     MISC_CHARGE_AMT                 decimal(15, 2),
// MAGIC     SHIPPING_HANDLING_CHARGE_AMT    decimal(15, 2),
// MAGIC     SHIPPING_SURCHARGE_AMT          decimal(15, 2),
// MAGIC     DONATION_AMT                    decimal(15, 2),
// MAGIC     ASSOCIATE_ID                    varchar(50)   ,
// MAGIC     ENTRY_METHOD                    varchar(50)   ,
// MAGIC     GIFT_MESSAGE                    varchar(200)  , 
// MAGIC     VOID_FLAG                       char(1)       ,
// MAGIC     REPO_FLAG                       char(1)       ,
// MAGIC     TAXABLE_FLAG                    char(1)       ,
// MAGIC     PICKABLE_FLAG                   char(1)       ,
// MAGIC     GIFT_FLAG                       char(1)       ,
// MAGIC     HOLD_FLAG                       char(1)       ,
// MAGIC     HOLD_REASON                     varchar(40)   ,
// MAGIC     ORIG_BACKORDER_FLAG             char(1)       ,
// MAGIC     SUBORDER_COMPLEXITY_GROUP_ID                   varchar(50)   , --Sutlej						
// MAGIC     DELIVERY_CHOICE                 varchar(50)   ,
// MAGIC     MODIFICATION_REASON_CD          varchar(10)   ,
// MAGIC     MODIFICATION_REASON_CD_DESC     varchar(200)  ,
// MAGIC     RETURN_ACTION_CD                varchar(10)   ,
// MAGIC     RETURN_ACTION                   varchar(50)   ,
// MAGIC     RETURN_REASON_CD                varchar(10)   ,
// MAGIC     RETURN_REASON_DESC              varchar(200)  ,
// MAGIC     RETURN_SUB_REASON_CD            varchar(50)   ,
// MAGIC     SHIP_TO_FIRST_NAME              varchar(100)  ,
// MAGIC     SHIP_TO_MIDDLE_NAME             varchar(100)  ,
// MAGIC     SHIP_TO_LAST_NAME               varchar(100)  ,
// MAGIC     SHIP_TO_ADDRESS_LINE_1          varchar(100)  ,
// MAGIC     SHIP_TO_ADDRESS_LINE_2          varchar(100)  ,
// MAGIC     SHIP_TO_CITY                    varchar(30)   ,
// MAGIC     SHIP_TO_STATE_OR_PROV           varchar(35)   ,
// MAGIC     SHIP_TO_POSTAL_CD               varchar(30)   ,
// MAGIC     SHIP_TO_COUNTRY                 varchar(120)  ,
// MAGIC     SHIP_TO_EMAIL_ADDRESS           varchar(100)  ,
// MAGIC     SHIP_TO_PHONE_NBR               varchar(15)   ,
// MAGIC     DELIVERY_METHOD                 varchar(100)  ,    --SEPIK           
// MAGIC     FULFILLMENT_TYPE                varchar(100)  ,    --SEPIK          
// MAGIC     DTC_SUBORDER_NBR                INT          ,     --SEPIK
// MAGIC     ADDITIONAL_LINE_TYPE_CD         varchar(100)  ,      --SEPIK    
// MAGIC     ORIG_CONFIRMED_QTY              INT               ,  --SEPIK
// MAGIC     RESKU_FLAG                      CHAR(1)           ,   --SEPIK
// MAGIC     CONSOLIDATOR_ADDRESS_CD         varchar(40)   ,
// MAGIC     MERGE_NODE_CD                   varchar(50)   ,
// MAGIC     SHIP_NODE_CD                    varchar(50)   ,
// MAGIC     RECEIVING_NODE_CD               varchar(50)   ,
// MAGIC     LEVEL_OF_SERVICE                varchar(40)   ,
// MAGIC     CARRIER_SERVICE_CD              varchar(40)   ,
// MAGIC     CARRIER_CD                      varchar(25)   ,
// MAGIC     ACCESS_POINT_CD                 STRING        , 
// MAGIC     ACCESS_POINT_ID                 STRING        , 
// MAGIC     ACCESS_POINT_NM                 STRING        , 
// MAGIC     MINIMUM_SHIP_DT                 date          ,
// MAGIC     REQUESTED_SHIP_DT               date          ,
// MAGIC     REQUESTED_DELIVERY_DT           date          ,
// MAGIC     EARLIEST_SCHEDULE_DT            date          ,
// MAGIC     EARLIEST_DELIVERY_DT            date          ,
// MAGIC     PROMISED_APPT_END_DT            date          ,
// MAGIC     SPLIT_QTY                       int           ,
// MAGIC     SHIPPED_QTY                     int           ,
// MAGIC     FILL_QTY                        int           ,
// MAGIC     WEIGHTED_AVG_COST               decimal(15, 2),
// MAGIC     DIRECT_SHIP_FLAG                char(1)       ,
// MAGIC     UNIT_COST                       decimal(15, 2),
// MAGIC     LABOR_COST                      decimal(15, 2),
// MAGIC     LABOR_SKU                       varchar(10)   ,
// MAGIC     CUSTOMER_LEVEL_OF_SERVICE       STRING       ,
// MAGIC     RETURN_POLICY	                varchar(100) , -- SEPIK
// MAGIC     RETURN_POLICY_CHECK_OVERRIDE_FLAG    CHAR(1) , -- SEPIK
// MAGIC     PRODUCT_AVAILABILITY_DT         DATE      , -- SEPIK
// MAGIC     ECDD_OVERRIDDEN_FLAG            CHAR(1)    , -- SEPIK
// MAGIC     ECDD_INVOKED_FLAG               CHAR(1)     , -- SEPIK
// MAGIC     VAS_GIFT_WRAP_CD				varchar(100), -- SEPIK
// MAGIC     VAS_MONO_FLAG                   CHAR(1),  --SEPIK
// MAGIC     VAS_PZ_FLAG						CHAR(1),  --SEPIK
// MAGIC     BO_NOTIFICATION_NBR				INT,
// MAGIC     INSERT_TS                       timestamp     ,
// MAGIC     UPDATE_TS                       timestamp     
// MAGIC )USING DELTA 
// MAGIC PARTITIONED BY (ORDER_DT,SOURCE_SYSTEM) 
// MAGIC LOCATION "dbfs:/mnt/data/governed/l2/stage/order/order_line_stage/";

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC INSERT INTO $L2_STAGE.ORDER_LINE_stage
// MAGIC SELECT  
// MAGIC  OL.ORDER_LINE_KEY                
// MAGIC , OL.ORDER_LINE_ID                 
// MAGIC , ol.ORDER_HEADER_KEY    --Column order change          
// MAGIC , ol.CHAINED_FROM_ORDER_HEADER_KEY --sepik
// MAGIC , ol.CHAINED_FROM_ORDER_HEADER_ID  --sepik
// MAGIC , ol.CHAINED_FROM_ORDER_LINE_KEY   --sepik
// MAGIC , ol.CHAINED_FROM_ORDER_LINE_ID    --sepik
// MAGIC , ol.SOURCE_SYSTEM                 
// MAGIC , ol.ORDER_DT                      --Column order change
// MAGIC , ol.ORDER_LINE_TYPE               --Column order change
// MAGIC , ol.PRIME_LINE_SEQ_NBR            
// MAGIC , ol.SUB_LINE_SEQ_NBR              
// MAGIC , ol.LINKED_ORDER_HEADER_KEY       
// MAGIC , ol.LINKED_ORDER_HEADER_ID        
// MAGIC , ol.LINKED_ORDER_LINE_KEY         
// MAGIC , ol.LINKED_ORDER_LINE_ID          
// MAGIC , ol.ITEM_KEY                      
// MAGIC , ol.ORDER_ITEM_ID                 
// MAGIC , ol.ORDER_ITEM_TYPE_CD            
// MAGIC , ol.ORDER_ITEM_NAME               
// MAGIC , ol.GIFT_REGISTRY_KEY             
// MAGIC , ol.GIFT_REGISTRY_ID              
// MAGIC , ol.FIRST_EFFECTIVE_TS            
// MAGIC , ol.final_LAST_EFFECTIVE_TS as LAST_EFFECTIVE_TS             
// MAGIC , ol.UPC_CD                        
// MAGIC , ol.KIT_CD                        
// MAGIC , ol.KIT_QTY                       
// MAGIC , ol.PRODUCT_LINE                  
// MAGIC , ol.BUNDLE_PARENT_ORDER_LINE_ID   
// MAGIC , ol.ORDER_QTY                     
// MAGIC , ol.ORIG_ORDER_QTY                
// MAGIC , ol.ACT_SALE_UNIT_PRICE_AMT       
// MAGIC , ol.REG_SALE_UNIT_PRICE_AMT       
// MAGIC , ol.EXTENDED_AMT                  
// MAGIC , ol.EXTENDED_DISCOUNT_AMT         
// MAGIC , ol.TAX_AMT                       
// MAGIC , ol.TAXABLE_AMT                   
// MAGIC , ol.GIFT_CARD_AMT                 
// MAGIC , ol.GIFTWRAP_CHARGE_AMT           
// MAGIC , ol.LINE_TOTAL_AMT                
// MAGIC , ol.MERCHANDISE_CHARGE_AMT        
// MAGIC , ol.MONO_PZ_CHARGE_AMT            
// MAGIC , ol.MISC_CHARGE_AMT               
// MAGIC , ol.SHIPPING_HANDLING_CHARGE_AMT  
// MAGIC , ol.SHIPPING_SURCHARGE_AMT        
// MAGIC , ol.DONATION_AMT                  
// MAGIC , ol.ASSOCIATE_ID                  
// MAGIC , ol.ENTRY_METHOD                  
// MAGIC , ol.GIFT_MESSAGE                  
// MAGIC , ol.VOID_FLAG                     
// MAGIC , ol.REPO_FLAG                     
// MAGIC , ol.TAXABLE_FLAG                  
// MAGIC , ol.PICKABLE_FLAG                 
// MAGIC , ol.GIFT_FLAG                     
// MAGIC , ol.HOLD_FLAG                     
// MAGIC , ol.HOLD_REASON                   
// MAGIC , ol.ORIG_BACKORDER_FLAG 
// MAGIC , ol.SUBORDER_COMPLEXITY_GROUP_ID  --Sutlej			
// MAGIC , ol.DELIVERY_CHOICE               
// MAGIC , ol.MODIFICATION_REASON_CD        
// MAGIC , ol.MODIFICATION_REASON_CD_DESC   
// MAGIC , ol.RETURN_ACTION_CD              
// MAGIC , ol.RETURN_ACTION                 
// MAGIC , ol.RETURN_REASON_CD              
// MAGIC , ol.RETURN_REASON_DESC            
// MAGIC , ol.RETURN_SUB_REASON_CD          
// MAGIC , ol.SHIP_TO_FIRST_NAME            
// MAGIC , ol.SHIP_TO_MIDDLE_NAME           
// MAGIC , ol.SHIP_TO_LAST_NAME             
// MAGIC , ol.SHIP_TO_ADDRESS_LINE_1        
// MAGIC , ol.SHIP_TO_ADDRESS_LINE_2        
// MAGIC , ol.SHIP_TO_CITY                  
// MAGIC , ol.SHIP_TO_STATE_OR_PROV         
// MAGIC , ol.SHIP_TO_POSTAL_CD             
// MAGIC , ol.SHIP_TO_COUNTRY               
// MAGIC , ol.SHIP_TO_EMAIL_ADDRESS         
// MAGIC , ol.SHIP_TO_PHONE_NBR             
// MAGIC , ol.DELIVERY_METHOD            --sepik   
// MAGIC , ol.FULFILLMENT_TYPE           --sepik   
// MAGIC , ol.DTC_SUBORDER_NBR            --sepik  
// MAGIC , ol.ADDITIONAL_LINE_TYPE_CD     --sepik  
// MAGIC , ol.ORIG_CONFIRMED_QTY           --sepik 
// MAGIC , ol.RESKU_FLAG                   --sepik 
// MAGIC , ol.CONSOLIDATOR_ADDRESS_CD       
// MAGIC , ol.MERGE_NODE_CD                 
// MAGIC , ol.SHIP_NODE_CD                  
// MAGIC , ol.RECEIVING_NODE_CD             
// MAGIC , ol.LEVEL_OF_SERVICE              
// MAGIC , ol.CARRIER_SERVICE_CD            
// MAGIC , ol.CARRIER_CD                    
// MAGIC , ol.ACCESS_POINT_CD               
// MAGIC , ol.ACCESS_POINT_ID               
// MAGIC , ol.ACCESS_POINT_NM               
// MAGIC , ol.MINIMUM_SHIP_DT               
// MAGIC , ol.REQUESTED_SHIP_DT             
// MAGIC , ol.REQUESTED_DELIVERY_DT         
// MAGIC , ol.EARLIEST_SCHEDULE_DT          
// MAGIC , ol.EARLIEST_DELIVERY_DT          
// MAGIC , ol.PROMISED_APPT_END_DT          
// MAGIC , ol.SPLIT_QTY                     
// MAGIC , ol.SHIPPED_QTY                   
// MAGIC , ol.FILL_QTY                      
// MAGIC , ol.WEIGHTED_AVG_COST             
// MAGIC , ol.DIRECT_SHIP_FLAG              
// MAGIC , ol.UNIT_COST                     
// MAGIC , ol.LABOR_COST                    
// MAGIC , ol.LABOR_SKU                     
// MAGIC , ol.CUSTOMER_LEVEL_OF_SERVICE     
// MAGIC , ol.RETURN_POLICY	               --sepik
// MAGIC , ol.RETURN_POLICY_CHECK_OVERRIDE_FLAG  --sepik
// MAGIC , ol.PRODUCT_AVAILABILITY_DT       --sepik
// MAGIC , ol.ECDD_OVERRIDDEN_FLAG          --sepik
// MAGIC , ol.ECDD_INVOKED_FLAG             --sepik
// MAGIC , ol.VAS_GIFT_WRAP_CD			--sepik
// MAGIC , ol.VAS_MONO_FLAG                 --sepik
// MAGIC , ol.VAS_PZ_FLAG				--sepik
// MAGIC , ol.BO_NOTIFICATION_NBR		                     		                    
// MAGIC , ol.new_INSERT_TS as INSERT_TS
// MAGIC , ol.new_UPDATE_TS as UPDATE_TS
// MAGIC 
// MAGIC FROM final_last_eff ol
// MAGIC 
// MAGIC 
// MAGIC /*END PATTERN*/
// MAGIC 
// MAGIC --50mins

// COMMAND ----------

// DBTITLE 1, Acquire TAG file before Orderline MERGE
// MAGIC %scala
// MAGIC if(!aquireLock(tagFilePrefix, lockFilePath, lockDirPath, logDirPath, retries, retryInterval)){
// MAGIC   throw new Exception("OL ::: After retrying for " + retries + " times and waiting for " + (retries*retryInterval) + " minutes, process is unable to aquire lock, skipping further exeution")
// MAGIC }

// COMMAND ----------

// DBTITLE 1,Get Max Surrogate Key from L2 table

//val q = "SELECT MAX(ORDER_LINE_KEY) AS MAX_ORDER_LINE_KEY FROM " + "L2_ANALYTICS_TABLES" + ".ORDER_LINE"; 
val q = """SELECT COALESCE(MAX(ORDER_LINE_KEY),0) AS MAX_ORDER_LINE_KEY FROM """ + L2_ANALYTICS_TABLES + """.ORDER_LINE"""

val df_MAX_KEY = spark.sql(q)

val var_MAX_KEY = df_MAX_KEY.head().getLong(0)

//var df_ord_lin_stage = spark.sql("SELECT * FROM $L2_STAGE.ORDER_LINE_stage") 
var df_ord_lin_stage = spark.sql("""SELECT * FROM """ + L2_STAGE + """.ORDER_LINE_stage""") 

import org.apache.spark.sql.functions._
val df_ord_lin_stage_max= df_ord_lin_stage.withColumn("new_Max_Orderline_key",lit(var_MAX_KEY))

df_ord_lin_stage_max.createOrReplaceTempView("df_ord_lin_stage_max")



// COMMAND ----------

// DBTITLE 1,Generate Surrogate keys new Orderlines
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_LINE_linked_stage_2 AS 
// MAGIC (
// MAGIC SELECT *, CASE WHEN COALESCE(ol.ORDER_LINE_KEY, -1) = -1 THEN
// MAGIC dense_rank() OVER (ORDER BY ol.ORDER_LINE_KEY, ol.ORDER_lINE_ID) + COALESCE(ol.new_Max_Orderline_key, 0) ELSE
// MAGIC  ol.ORDER_LINE_KEY END AS new_ORDER_LINE_KEY FROM
// MAGIC df_ord_lin_stage_max ol
// MAGIC )

// COMMAND ----------

spark.sql("CACHE TABLE ORDER_LINE_linked_stage_2")

// COMMAND ----------

// DBTITLE 1,OL: check for linked order line keys within the same batch
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_LINE_linked_stage AS 
// MAGIC (
// MAGIC SELECT  
// MAGIC ol.*
// MAGIC , COALESCE(samebatch.new_ORDER_LINE_KEY,  -1)  as new_LINKED_ORDER_LINE_KEY
// MAGIC FROM ORDER_LINE_linked_stage_2 ol
// MAGIC LEFT JOIN (select distinct ORDER_LINE_ID, new_ORDER_LINE_KEY FROM ORDER_LINE_linked_stage_2   WHERE  LINKED_ORDER_LINE_ID is not null )  samebatch
// MAGIC 	ON trim(ol.LINKED_ORDER_LINE_ID) = trim(samebatch.ORDER_LINE_ID)
// MAGIC 
// MAGIC )
// MAGIC /*END PATTERN*/

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_LINE_BFILLED_stage_pre AS 
// MAGIC ( 
// MAGIC SELECT  
// MAGIC *, to_date(coalesce(ol.ORDER_DT,CONCAT(substr(trim(order_line_id), 0, 4),'-',substr(trim(order_line_id), 5, 2),'-',substr(trim(order_line_id), 7, 2)))) AS new_ORDER_DT
// MAGIC ,CONCAT(substr(trim(order_line_id), 0, 4),'-',substr(trim(order_line_id), 5, 2),'-',substr(trim(order_line_id), 7, 2)) as test_order_dt
// MAGIC 
// MAGIC 
// MAGIC FROM ORDER_LINE_linked_stage ol
// MAGIC 
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OL: Retrieve previous known values via a backfill in case of inconsistent schema
// MAGIC %sql
// MAGIC 
// MAGIC CREATE OR REPLACE TEMPORARY VIEW ORDER_LINE_BFILLED_stage AS 
// MAGIC ( 
// MAGIC SELECT  
// MAGIC  OL.new_ORDER_LINE_KEY as ORDER_LINE_KEY
// MAGIC , OL.ORDER_LINE_ID
// MAGIC , ol.ORDER_HEADER_KEY --COLUMN ORDER CHANGE
// MAGIC , last(ol.CHAINED_FROM_ORDER_HEADER_KEY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CHAINED_FROM_ORDER_HEADER_KEY --SEPIK
// MAGIC , last(ol.CHAINED_FROM_ORDER_HEADER_ID , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CHAINED_FROM_ORDER_HEADER_ID --SEPIK
// MAGIC , last(ol.CHAINED_FROM_ORDER_LINE_KEY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CHAINED_FROM_ORDER_LINE_KEY --SEPIK
// MAGIC , last(ol.CHAINED_FROM_ORDER_LINE_ID , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CHAINED_FROM_ORDER_LINE_ID --SEPIK
// MAGIC , ol.SOURCE_SYSTEM
// MAGIC , ol.new_ORDER_DT AS ORDER_DT  --COLUMN ORDER CHANGE
// MAGIC , coalesce(last(ol.ORDER_LINE_TYPE , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),'') as ORDER_LINE_TYPE  --COLUMN ORDER CHANGE
// MAGIC , coalesce(ol.PRIME_LINE_SEQ_NBR,-1) as PRIME_LINE_SEQ_NBR
// MAGIC , ol.SUB_LINE_SEQ_NBR
// MAGIC , last(ol.LINKED_ORDER_HEADER_KEY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as LINKED_ORDER_HEADER_KEY
// MAGIC , last(ol.LINKED_ORDER_HEADER_ID , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as LINKED_ORDER_HEADER_ID
// MAGIC , last(ol.new_LINKED_ORDER_LINE_KEY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as LINKED_ORDER_LINE_KEY
// MAGIC , last(ol.LINKED_ORDER_LINE_ID , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as LINKED_ORDER_LINE_ID
// MAGIC , ol.ITEM_KEY
// MAGIC , ol.ORDER_ITEM_ID
// MAGIC , ol.ORDER_ITEM_TYPE_CD
// MAGIC , ol.ORDER_ITEM_NAME
// MAGIC , ol.GIFT_REGISTRY_KEY
// MAGIC , last(ol.GIFT_REGISTRY_ID , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as GIFT_REGISTRY_ID
// MAGIC , ol.FIRST_EFFECTIVE_TS
// MAGIC , ol.LAST_EFFECTIVE_TS
// MAGIC , ol.UPC_CD
// MAGIC , ol.KIT_CD
// MAGIC , last(ol.KIT_QTY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as KIT_QTY
// MAGIC , ol.PRODUCT_LINE
// MAGIC , last(ol.BUNDLE_PARENT_ORDER_LINE_ID , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BUNDLE_PARENT_ORDER_LINE_ID
// MAGIC , ol.ORDER_QTY
// MAGIC , ol.ORIG_ORDER_QTY
// MAGIC , last(ol.ACT_SALE_UNIT_PRICE_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ACT_SALE_UNIT_PRICE_AMT
// MAGIC , last(ol.REG_SALE_UNIT_PRICE_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as REG_SALE_UNIT_PRICE_AMT
// MAGIC , last(ol.EXTENDED_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as EXTENDED_AMT
// MAGIC , last(ol.EXTENDED_DISCOUNT_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as EXTENDED_DISCOUNT_AMT
// MAGIC , last(ol.TAX_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as TAX_AMT
// MAGIC , last(ol.TAXABLE_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as TAXABLE_AMT
// MAGIC , last(ol.GIFT_CARD_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as GIFT_CARD_AMT
// MAGIC , last(ol.GIFTWRAP_CHARGE_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as GIFTWRAP_CHARGE_AMT
// MAGIC , last(ol.LINE_TOTAL_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as LINE_TOTAL_AMT
// MAGIC , last(ol.MERCHANDISE_CHARGE_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MERCHANDISE_CHARGE_AMT
// MAGIC , last(ol.MONO_PZ_CHARGE_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MONO_PZ_CHARGE_AMT
// MAGIC , ol.MISC_CHARGE_AMT
// MAGIC , last(ol.SHIPPING_HANDLING_CHARGE_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIPPING_HANDLING_CHARGE_AMT
// MAGIC , last(ol.SHIPPING_SURCHARGE_AMT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIPPING_SURCHARGE_AMT
// MAGIC , ol.DONATION_AMT
// MAGIC , ol.ASSOCIATE_ID
// MAGIC , ol.ENTRY_METHOD
// MAGIC , last(ol.GIFT_MESSAGE , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as GIFT_MESSAGE   
// MAGIC , ol.VOID_FLAG
// MAGIC , ol.REPO_FLAG
// MAGIC , last(ol.TAXABLE_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as TAXABLE_FLAG
// MAGIC , ol.PICKABLE_FLAG
// MAGIC , ol.GIFT_FLAG
// MAGIC , ol.HOLD_FLAG
// MAGIC , ol.HOLD_REASON
// MAGIC , last(ol.ORIG_BACKORDER_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ORIG_BACKORDER_FLAG
// MAGIC , last(ol.SUBORDER_COMPLEXITY_GROUP_ID, true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SUBORDER_COMPLEXITY_GROUP_ID																																						   
// MAGIC , last(ol.DELIVERY_CHOICE, true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as DELIVERY_CHOICE
// MAGIC , last(ol.MODIFICATION_REASON_CD, true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MODIFICATION_REASON_CD
// MAGIC , last(ol.MODIFICATION_REASON_CD_DESC, true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MODIFICATION_REASON_CD_DESC
// MAGIC , last(ol.RETURN_ACTION_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_ACTION_CD
// MAGIC , ol.RETURN_ACTION
// MAGIC , last(ol.RETURN_REASON_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_REASON_CD
// MAGIC , ol.RETURN_REASON_DESC
// MAGIC , last(ol.RETURN_SUB_REASON_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_SUB_REASON_CD
// MAGIC , last(ol.SHIP_TO_FIRST_NAME , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_FIRST_NAME
// MAGIC , last(ol.SHIP_TO_MIDDLE_NAME , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_MIDDLE_NAME
// MAGIC , last(ol.SHIP_TO_LAST_NAME , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_LAST_NAME
// MAGIC , last(ol.SHIP_TO_ADDRESS_LINE_1 , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_ADDRESS_LINE_1
// MAGIC , last(ol.SHIP_TO_ADDRESS_LINE_2 , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_ADDRESS_LINE_2
// MAGIC , last(ol.SHIP_TO_CITY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_CITY
// MAGIC , last(ol.SHIP_TO_STATE_OR_PROV , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_STATE_OR_PROV
// MAGIC , last(ol.SHIP_TO_POSTAL_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_POSTAL_CD
// MAGIC , last(ol.SHIP_TO_COUNTRY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_COUNTRY
// MAGIC , last(ol.SHIP_TO_EMAIL_ADDRESS , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_EMAIL_ADDRESS
// MAGIC , last(ol.SHIP_TO_PHONE_NBR , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIP_TO_PHONE_NBR
// MAGIC , last(ol.DELIVERY_METHOD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as DELIVERY_METHOD    --SEPIK	     
// MAGIC , last(ol.FULFILLMENT_TYPE , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as FULFILLMENT_TYPE   --SEPIK	     
// MAGIC , last(ol.DTC_SUBORDER_NBR , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as DTC_SUBORDER_NBR   --SEPIK	     
// MAGIC , last(ol.ADDITIONAL_LINE_TYPE_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ADDITIONAL_LINE_TYPE_CD --SEPIK	
// MAGIC , last(ol.ORIG_CONFIRMED_QTY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ORIG_CONFIRMED_QTY   --SEPIK	   
// MAGIC , last(ol.RESKU_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RESKU_FLAG            --SEPIK	  
// MAGIC , last(ol.CONSOLIDATOR_ADDRESS_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CONSOLIDATOR_ADDRESS_CD
// MAGIC , last(ol.MERGE_NODE_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MERGE_NODE_CD
// MAGIC , ol.SHIP_NODE_CD
// MAGIC , last(ol.RECEIVING_NODE_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RECEIVING_NODE_CD
// MAGIC , ol.LEVEL_OF_SERVICE
// MAGIC , last(ol.CARRIER_SERVICE_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CARRIER_SERVICE_CD
// MAGIC , last(ol.CARRIER_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CARRIER_CD
// MAGIC , last(ol.ACCESS_POINT_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ACCESS_POINT_CD    
// MAGIC , last(ol.ACCESS_POINT_ID , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ACCESS_POINT_ID     
// MAGIC , last(ol.ACCESS_POINT_NM , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ACCESS_POINT_NM
// MAGIC , last(ol.MINIMUM_SHIP_DT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MINIMUM_SHIP_DT
// MAGIC , last(ol.REQUESTED_SHIP_DT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as REQUESTED_SHIP_DT
// MAGIC , last(ol.REQUESTED_DELIVERY_DT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as REQUESTED_DELIVERY_DT
// MAGIC , last(ol.EARLIEST_SCHEDULE_DT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as EARLIEST_SCHEDULE_DT
// MAGIC , last(ol.EARLIEST_DELIVERY_DT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as EARLIEST_DELIVERY_DT
// MAGIC , ol.PROMISED_APPT_END_DT
// MAGIC , ol.SPLIT_QTY
// MAGIC , last(ol.SHIPPED_QTY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SHIPPED_QTY
// MAGIC , last(ol.FILL_QTY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as FILL_QTY
// MAGIC , last(ol.WEIGHTED_AVG_COST , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as WEIGHTED_AVG_COST
// MAGIC , last(ol.DIRECT_SHIP_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as DIRECT_SHIP_FLAG
// MAGIC , last(ol.UNIT_COST , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as UNIT_COST
// MAGIC , last(ol.LABOR_COST , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as LABOR_COST
// MAGIC , last(ol.LABOR_SKU , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as LABOR_SKU
// MAGIC , last(ol.CUSTOMER_LEVEL_OF_SERVICE , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUSTOMER_LEVEL_OF_SERVICE
// MAGIC , last(ol.RETURN_POLICY , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_POLICY	               --SEPIK	 
// MAGIC , last(ol.RETURN_POLICY_CHECK_OVERRIDE_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as RETURN_POLICY_CHECK_OVERRIDE_FLAG --SEPIK	
// MAGIC , last(ol.PRODUCT_AVAILABILITY_DT , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as PRODUCT_AVAILABILITY_DT   --SEPIK	     
// MAGIC , last(ol.ECDD_OVERRIDDEN_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ECDD_OVERRIDDEN_FLAG  --SEPIK	         
// MAGIC , last(ol.ECDD_INVOKED_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as ECDD_INVOKED_FLAG        --SEPIK	      
// MAGIC , last(ol.VAS_GIFT_WRAP_CD , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as VAS_GIFT_WRAP_CD			--SEPIK		
// MAGIC , last(ol.VAS_MONO_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as VAS_MONO_FLAG              --SEPIK	    
// MAGIC , last(ol.VAS_PZ_FLAG , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as VAS_PZ_FLAG					--SEPIK	
// MAGIC , last(ol.BO_NOTIFICATION_NBR , true) over (partition by ol.order_line_id order by ol.first_effective_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as BO_NOTIFICATION_NBR	--SEPIK	                     
// MAGIC , ol.INSERT_TS
// MAGIC , ol.UPDATE_TS
// MAGIC 
// MAGIC FROM ORDER_LINE_BFILLED_stage_pre ol
// MAGIC 
// MAGIC )

// COMMAND ----------

// DBTITLE 1,OL: Copy final stage data to physical storage for snowflake consumption
val dropOlSnoflkStg = "DROP TABLE IF exists L2_STAGE.PRE_SNOFLK_OL_STG"
spark.sql(dropOlSnoflkStg);

dbutils.fs.rm("/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_ol_stg/", true);

val createOlSnoflkStg = """CREATE TABLE L2_STAGE.PRE_SNOFLK_OL_STG
USING DELTA
LOCATION '/mnt/data/governed/l2/stage/sales/dtc_pre_snoflk_ol_stg/'
AS SELECT * FROM ORDER_LINE_BFILLED_stage""";
spark.sql(createOlSnoflkStg);

// COMMAND ----------

// DBTITLE 1,OL: Merge Final Stage into Target Table
// MAGIC %sql
// MAGIC 
// MAGIC MERGE INTO $L2_ANALYTICS_TABLES.ORDER_LINE tgt
// MAGIC USING L2_STAGE.PRE_SNOFLK_OL_STG stg
// MAGIC ON tgt.source_system = 'STERLING_DTC'
// MAGIC and tgt.ORDER_dt = stg.ORDER_dt 
// MAGIC and trim(tgt.ORDER_LINE_ID) = trim(stg.ORDER_LINE_ID)
// MAGIC and tgt.FIRST_EFFECTIVE_TS = stg.FIRST_EFFECTIVE_TS
// MAGIC WHEN MATCHED AND tgt.LAST_EFFECTIVE_TS != stg.LAST_EFFECTIVE_TS  --if existing record, and there is a new LAST_EFFECTIVE_TS, perform update. If LAST_EFFECTIVE_TS is same, do nothing (no change)
// MAGIC   THEN UPDATE SET tgt.LAST_EFFECTIVE_TS = stg.LAST_EFFECTIVE_TS, tgt.UPDATE_TS = stg.UPDATE_TS 
// MAGIC WHEN NOT MATCHED --if new record, aka, FIRST_EFFECTIVE_TS not found, do an insert
// MAGIC   THEN INSERT * 

// COMMAND ----------

// MAGIC %scala
// MAGIC if (!releaseLock(lockDirPath)){
// MAGIC 
// MAGIC  dbutils.notebook.exit("""OL done ::: Not able to release the lock ::: hence not proceeding further failing the execution""")
// MAGIC }
// MAGIC 
// MAGIC // java.util.NoSuchElementException: next on empty iterator -- this error means it was trying to clear a log directory that is already empty

// COMMAND ----------

// DBTITLE 1,Notebook exit success
 dbutils.notebook.exit("Success")
