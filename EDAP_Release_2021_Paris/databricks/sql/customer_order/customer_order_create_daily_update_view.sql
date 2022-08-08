/*
********************************************************************************************

** Author         : Mekong - MEK-7127
** Create Date    : FEB, 2021
** Purpose        : Create the CUSTOMER_INBOUND_DAILY_UPDATE_VIEW from 
                    CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW, 
                    CUSTOMER and ORDER_HEADER for all the Update Records

********************************************************************************************

** Source Table   : CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW
                  : L2_ANALYTICS_TABLES.CUSTOMER
				  : MKTG_MEKONG.ORDER_HEADER_02092021
                  
** Target Table   : CUSTOMER_INBOUND_DAILY_UPDATE_VIEW
** Lookup         :

*********************************************************************************************
************************* Change History ****************************************************
*********************************************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  -----------------------------------------------------------
**  01      02/23/2021  MEK-7127  SQL file created
**  02      07/13/2021  SPK-299   ADD MARKET_CD
**  03      08/06/2021  MEK-7871  Trim Order Header Id. Also updated the query to use views

*********************************************************************************************
*/
CREATE OR REPLACE VIEW CUSTOMER_INBOUND_DAILY_UPDATE_VIEW as (
   select distinct
   CUST_ORD.CUSTOMER_ORDER_KEY AS CUSTOMER_ORDER_KEY,
   CUST_ORD.SOURCE_SYSTEM as SOURCE_SYSTEM,
   CUST_ORD.CONCEPT_CD as CONCEPT_CD,
   CUST.HOUSEHOLD_KEY as HOUSEHOLD_KEY,
   CUST.HOUSEHOLD_ID as HOUSEHOLD_ID,
   CUST.CUSTOMER_KEY as CUSTOMER_KEY,
   CUST.CUSTOMER_ID as CUSTOMER_ID,
   CUST_ORD.ORDER_HEADER_KEY as ORDER_HEADER_KEY,
   trim(CUST_ORD.ORDER_HEADER_ID) as ORDER_HEADER_ID,  /*   Added by Mekong for trim order header id   */
   CUST_ORD.ORDER_DT as ORDER_DT,
   trim(CUST_ORD.ORDER_ID) as ORDER_ID,                /*   Added by Mekong for trim order header id   */
   CUST_ORD.MARKET_CD as MARKET_CD,                    /*   Added by SEPIK for GMTP   */
   CURRENT_TIMESTAMP as FIRST_EFFECTIVE_TS,
   '9999-12-31T23:59:59.000+0000' as LAST_EFFECTIVE_TS,
   IN_ORDER.INITIAL_CHANNEL_CD AS INITIAL_CHANNEL_CD,
   IN_ORDER.DEMAND_CHANNEL_CD AS DEMAND_CHANNEL_CD,
   CURRENT_TIMESTAMP AS INSERT_TS,
   CURRENT_TIMESTAMP AS UPDATE_TS,
   CASE WHEN (
   upper(trim(coalesce(CUST.HOUSEHOLD_ID,'')))=upper(trim(coalesce(CUST_ORD.HOUSEHOLD_ID,''))) and
   upper(trim(coalesce(CUST.CUSTOMER_ID,'')))=upper(trim(coalesce(CUST_ORD.CUSTOMER_ID,'')))
   ) THEN 'NU' ELSE 'U' END as INDICATOR
   from 
   CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW IN_ORDER
   inner join L2_ANALYTICS.CUSTOMER_ORDER CUST_ORD on trim(IN_ORDER.ACTIVITY_ID) = trim(CUST_ORD.ORDER_HEADER_ID)  /*   Added by Mekong for trim order header id   */
   inner join L2_ANALYTICS_TABLES.CUSTOMER CUST on IN_ORDER.amperity_id = CUST.customer_id
   where IN_ORDER.amperity_id is not null
   )