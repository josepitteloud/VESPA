/*
This code was taken from vespa_toolbox_02_adsmartUniverse and edited to find the account_numbers of those boxes that
are adsmartable and also to find those boxes that are adsmart_capabale.

Whilst it is probably inefficient to run the SQL code twice (with the viewing consent line blocked out in the second block)
we would have had to edit later code (data_set_up.sql) if we incorporated everything into one table. A quick and easy
solution was required and this was the easiest to implement.
*/


-- create or replace procedure vespa_toolbox_02_AdsmartUniverse -- execute vespa_toolbox_02_adsmartUniverse
-- as
begin

-----------------------
-- ADSMARTABLE UNIVERSE
-----------------------
IF object_id('adsmart_account_numbers') IS NOT NULL DROP TABLE adsmart_account_numbers
select  sav.account_number as account_number, adsmart.flag
into    adsmart_account_numbers
from    (
            select      distinct account_number
                 from   sk_prod.CUST_SINGLE_ACCOUNT_VIEW
                where   CUST_ACTIVE_DTV = 1                     -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
                  and   pty_country_code = 'GBR'
                  and   cust_viewing_data_capture_allowed = 'Y' -- [ ENABLE/DISABLE this criteria to consider viewing consent ]
        )as sav
                left join       (
                        ----------------------------------------------------------
                        -- B03: Flag Adsmartable boxes based on Adsmart definition
                        ----------------------------------------------------------
                            select  account_number
                                            ,max(   CASE    WHEN x_pvr_type ='PVR6'                                 THEN 1
                                                            WHEN x_pvr_type ='PVR5'                                 THEN 1
                                                            WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
                                                            WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
                                                            ELSE 0
                                                            END) AS flag
                            from    (
                                    --------------------------------------------------------------------------
                                    -- B02: Extracting Active Boxes per account (one line per box per account)
                                    --------------------------------------------------------------------------
                                    select  *
                                    from    (
                                            --------------------------------------------------------------------
                                            -- B01: Ranking STB based on service instance id to dedupe the table
                                            --------------------------------------------------------------------
                                            Select  account_number
                                                            ,x_pvr_type
                                                            ,x_personal_storage_capacity
                                                            ,currency_code
                                                            ,x_manufacturer
                                                            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                                            from    sk_prod.CUST_SET_TOP_BOX

                                    )       as base
                                    where   active_flag = 1

                            )       as active_boxes
                    where   currency_code = 'GBP'
                    group   by      account_number

                )       as adsmart
                on      sav.account_number = adsmart.account_number
                left join       (
                        -----------------------------------------------------------------------------------------
                        --C01: Listing DP active Accounts that have reported at least 1 day amongst last 30 days
                        -----------------------------------------------------------------------------------------
                        select  distinct account_number
                             from   vespa_analysts.vespa_single_box_view
                            where   panel = 'VESPA'
                              and   status_vespa = 'Enabled'

                )       as sbv
                on      sav.account_number = sbv.account_number
commit

IF object_id('adsmart_capable_account_numbers') IS NOT NULL DROP TABLE adsmart_capable_account_numbers
select  sav.account_number as account_number, adsmart.flag
into    adsmart_capable_account_numbers
from    (
            select      distinct account_number
                 from   sk_prod.CUST_SINGLE_ACCOUNT_VIEW
                where   CUST_ACTIVE_DTV = 1                     -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
                  and   pty_country_code = 'GBR'
        )as sav
                left join       (
                        ----------------------------------------------------------
                        -- B03: Flag Adsmartable boxes based on Adsmart definition
                        ----------------------------------------------------------
                            select  account_number
                                            ,max(   CASE    WHEN x_pvr_type ='PVR6'                                 THEN 1
                                                            WHEN x_pvr_type ='PVR5'                                 THEN 1
                                                            WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1
                                                            WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1
                                                            ELSE 0
                                                            END) AS flag
                            from    (
                                    --------------------------------------------------------------------------
                                    -- B02: Extracting Active Boxes per account (one line per box per account)
                                    --------------------------------------------------------------------------
                                    select  *
                                    from    (
                                            --------------------------------------------------------------------
                                            -- B01: Ranking STB based on service instance id to dedupe the table
                                            --------------------------------------------------------------------
                                            Select  account_number
                                                            ,x_pvr_type
                                                            ,x_personal_storage_capacity
                                                            ,currency_code
                                                            ,x_manufacturer
                                                            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                                            from    sk_prod.CUST_SET_TOP_BOX

                                    )       as base
                                    where   active_flag = 1

                            )       as active_boxes
                    where   currency_code = 'GBP'
                    group   by      account_number

                )       as adsmart
                on      sav.account_number = adsmart.account_number
                left join       (
                        -----------------------------------------------------------------------------------------
                        --C01: Listing DP active Accounts that have reported at least 1 day amongst last 30 days
                        -----------------------------------------------------------------------------------------
                        select  distinct account_number
                             from   vespa_analysts.vespa_single_box_view
                            where   panel = 'VESPA'
                              and   status_vespa = 'Enabled'

                )       as sbv
                on      sav.account_number = sbv.account_number
commit

end

-- select      top 20 account_number, cust_viewing_capture_allwd_start_dt --distinct account_number
--      from   sk_prod.CUST_SINGLE_ACCOUNT_VIEW
-- --     where   CUST_ACTIVE_DTV = 1                     -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
-- --       and   pty_country_code = 'GBR'
--      where   account_number in (
--                 select account_number
--                     from V154_accounts_proxy_consent
--                     where sky_base_universe like '%but no%'
--                     and vespa_universe like 'Vespa%')
--                     order by cust_viewing_capture_allwd_start_dt
--
-- select      top 20 cust_viewing_capture_allwd_start_dt --distinct account_number
--      from   sk_prod.CUST_SINGLE_ACCOUNT_VIEW
-- --     where   CUST_ACTIVE_DTV = 1                     -- this field implies -> prod_latest_dtv_status_code IN ('AC','AB','PC')
-- --       and   pty_country_code = 'GBR'
--      where   account_number in (
--                 select account_number
--                     from V154_accounts_proxy_consent
--                     where sky_base_universe like '%but no%'
--                     and vespa_universe like 'Vespa%')
--                     order by cust_viewing_capture_allwd_start_dt desc
--
