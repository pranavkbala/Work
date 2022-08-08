/*
********************************************************************************************

** Author         : Mekong - MEK-7127
** Create Date    : FEB, 2021
** Purpose        : Create the CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW view removing 
                    duplicates from INBOUND_UNIFIED_ORDERS

********************************************************************************************

** Source Table   : L2_STAGE.INBOUND_UNIFIED_ORDERS
                  
** Target Table   : CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW
** Lookup         :

*********************************************************************************************
************************* Change History ****************************************************
*********************************************************************************************

** Change   Date        Author    Description
**  --      ----------  --------  -----------------------------------------------------------
**  01      02/23/2021  MEK-7127  SQL file created
**  02      08/06/2021  MEK-7871  Trim Order Header Id.

*********************************************************************************************
*/
create or replace view CUSTOMER_ORDER_INBOUND_WITHOUT_DUP_VIEW as
    (
    select * from
    (
    select CUSTOMER_ACTIVITY_ID,
    source_system,
    trim(ACTIVITY_ID) AS ACTIVITY_ID,    /*   Added by Mekong for trim order header id   */
    ACTIVITY_DT,
    HOUSEHOLD_ID,
    AMPERITY_ID,
    CONCEPT_CD,
    STORE_NBR,
    TRAN_TYPE_CD,
    TRAN_STATUS_CD,
    DEMAND_CHANNEL_CD,
    INITIAL_CHANNEL_CD,
    INBOUND_CONTACT_TYPE_CD,
    rank() over(PARTITION BY trim(ACTIVITY_ID) ORDER BY CONCEPT_CD)R from L2_STAGE.INBOUND_UNIFIED_ORDERS)
    where R=1
)