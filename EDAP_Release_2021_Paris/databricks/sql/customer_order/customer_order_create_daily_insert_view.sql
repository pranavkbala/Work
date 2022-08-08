/*
********************************************************************************************

** Author         : Mekong - MEK-7127
** Create Date    : FEB, 2021
** Purpose        : Create the CUSTOMER_INBOUND_DAILY_INSERT_VIEW from 
                    CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW, 
                    CUSTOMER and ORDER_HEADER for all the Insert Records

********************************************************************************************

** Source Table   : CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW
                  : L2_ANALYTICS_TABLES.CUSTOMER
				  : MKTG_MEKONG.ORDER_HEADER_02092021
                  
** Target Table   : CUSTOMER_INBOUND_DAILY_INSERT_VIEW
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
CREATE OR REPLACE VIEW CUSTOMER_INBOUND_DAILY_INSERT_VIEW as (
   select distinct
   cast(dense_rank() OVER(ORDER BY OH.ORDER_HEADER_KEY,trim(OH.ORDER_HEADER_ID),CUST.CUSTOMER_ID ASC)+(select max(customer_order_key) from L2_ANALYTICS_TABLES.CUSTOMER_ORDER) as bigint) AS CUSTOMER_ORDER_KEY,
   OH.SOURCE_SYSTEM as SOURCE_SYSTEM,
   OH.CONCEPT_CD as CONCEPT_CD,
   CUST.HOUSEHOLD_KEY as HOUSEHOLD_KEY,
   CUST.HOUSEHOLD_ID as HOUSEHOLD_ID,
   CUST.CUSTOMER_KEY as CUSTOMER_KEY,
   CUST.CUSTOMER_ID as CUSTOMER_ID,
   cast(OH.ORDER_HEADER_KEY as bigint) as ORDER_HEADER_KEY,
   trim(OH.ORDER_HEADER_ID) as ORDER_HEADER_ID,           /*   Added by Mekong for trim order header id   */
   OH.ORDER_DT as ORDER_DT,
   trim(OH.ORDER_ID) as ORDER_ID,                         /*   Added by Mekong for trim order header id   */
   OH.MARKET_CD as MARKET_CD,                             /*   Added by SEPIK for GMTP   */
   CURRENT_TIMESTAMP as FIRST_EFFECTIVE_TS,
   '9999-12-31T23:59:59.000+0000' as LAST_EFFECTIVE_TS,
   IN_ORDER.INITIAL_CHANNEL_CD AS INITIAL_CHANNEL_CD,
   IN_ORDER.DEMAND_CHANNEL_CD AS DEMAND_CHANNEL_CD,
   CURRENT_TIMESTAMP AS INSERT_TS,
   CURRENT_TIMESTAMP AS UPDATE_TS,
   'I' as indicator
   from 
   CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW IN_ORDER
   left anti join L2_ANALYTICS.CUSTOMER_ORDER CUST_ORD on trim(IN_ORDER.ACTIVITY_ID) = trim(CUST_ORD.ORDER_HEADER_ID)   /*   Added by Mekong for trim order header id   */
   inner join L2_ANALYTICS_TABLES.CUSTOMER CUST on IN_ORDER.amperity_id = CUST.customer_id
   inner join (select distinct SOURCE_SYSTEM, CONCEPT_CD, ORDER_HEADER_KEY, trim(ORDER_HEADER_ID) AS ORDER_HEADER_ID, ORDER_DT, trim(ORDER_ID) AS ORDER_ID, MARKET_CD, MAX(FIRST_EFFECTIVE_TS) AS FIRST_EFFECTIVE_TS FROM L2_ANALYTICS.ORDER_HEADER GROUP BY 1,2,3,4,5,6,7) OH on trim(IN_ORDER.ACTIVITY_ID) = trim(OH.ORDER_HEADER_ID) /*   Added by Mekong for trim order header id   */
   where IN_ORDER.amperity_id is not null
   )