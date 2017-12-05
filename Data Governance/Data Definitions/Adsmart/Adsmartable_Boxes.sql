/*###############################################################################
# Created on:   10/01/2014
# Created by:   Tony Kinnaird (TKD)
# Description:  Rule to derive for any accounts you want the number of Boxes 
#		capable of running Adsmart Adverts on them
#		Code will need to be amended to VIQ Scaling tables instead of current
#		scaling tables
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/01/2014  TKD   v01 - initial version
#
###############################################################################*/


declare @adsmart_date date

set @adsmart_date = '2014-01-01'

SELECT base.cb_key_household
      ,base.account_number
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
                                                                    ELSE 0
       END AS Adsmartable
      ,SUM(Adsmartable) AS T_AdSm_box
INTO #SetTop
FROM
(
-- Extracting Active Boxes per account (one line per box per account)
           select  *            from    (
-- Ranking STB based on service instance id to dedupe the table
                        Select  account_number
                        ,x_pvr_type
                        ,x_personal_storage_capacity
                        ,currency_code
                        ,x_manufacturer
                        ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                        from    sk_prod.CUST_SET_TOP_BOX
where box_installed_dt < @adsmart_date
           )       as box
           where   active_flag = 1

)       as active_boxes

inner join 
--join to accounts that you want to get the Set Top Box value for
--LW_TEMP_BASE_CB_KEY as Base
on active_boxes.account_number = Base.account_number
GROUP BY
base.cb_key_household
,base.account_number
,x_pvr_type
,x_manufacturer;
