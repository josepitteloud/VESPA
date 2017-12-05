/* Identifying Adsmartable Boxes for the present period
   For Luca Valer
   Author: Sourav Das
*/

select  account_number
            ,case
                when flag = 1 and cust_viewing_data_capture_allowed = 'Y' then 'Adsmartable with consent'
                when flag = 1 and cust_viewing_data_capture_allowed <> 'Y' then 'Adsmartable but no consent'
                else 'Not adsmartable'
                end as sky_base_universe
				CASE WHEN sky_base_universe = 'Adsmartable with consent' THen 1 ELSE 0 END ads
        into  adsmartables_UK_201609
        from (
                 select  sav.account_number as account_number, adsmart.flag, cust_viewing_data_capture_allowed
                    from    (
                                select      distinct account_number, cust_viewing_data_capture_allowed
                                     from   CUST_SINGLE_ACCOUNT_VIEW
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
																				WHEN x_pvr_type ='PVR7'                                 THEN 1
																				WHEN x_pvr_type ='PVR8'                                 THEN 1
                                                                            --    WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung'  THEN 1	these boxes have been disabled Apr 2015
                                                                            --    WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'     THEN 1	these boxes have been disabled Apr 2015
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
                                                                                ,currency_code
                                                                                ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                                                                from    CUST_SET_TOP_BOX

                                                        )       as base
                                                        where   active_flag = 1

                                                )       as active_boxes
                                        where   currency_code = 'GBP'
                                        group   by      account_number

                                    )       as adsmart
                                    on      sav.account_number = adsmart.account_number
        ) as sub1
     commit

    

