/*
 
 
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

-----------------------------------------------------------------------------------

**Project Name:                         Vespa over Broadband
**Analysts:                             Jon Green   (Jonathan.Green@skyiq.co.uk)
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@skyiq.co.uk)
**Stakeholder:                          Vespa
**Due Date:                             n/a
**Project Code (Insight Collation):     V107
**SharePoint Folder:                    TBA
                                                                        
**Business Brief:

Vespa over Broadband enables viewing data from STBs to be returned via broadband.
This complements the current PSTN set up, and migration will effect Opex reductions
as well as increased flexibility in the callback time window.

This script will form the basis of ongoing panel management activities in which STBs 
eligible for enablement onto a Broadband panel are identified.


**Modules:

    1. Get multiroom accounts
    2. Get active PSTN panel accounts and boxes from vespa_subscriber_status, filtering out excluded accounts
    3. Get box hardware details and account number for each active STB (unique service_instance_id)
    4. Add the last PSTN callback date per subscriber_id
    5. Calculate account-wise last content download datetime
    6. Calculate RQ from panel_data
    7. Calculate CA callback rate in absence of RQ
    8. Combine and link to STB hardware details and multiroom flag
    9. Generate migration groups
    10. Summarise results and export for enablement campaigns




**Stats:

    Running time: ~100s
    
-----------------------------------------------------------------------------------

*/


----------------------------
-- 1. Get multiroom accounts
----------------------------
drop table #temp_mr;

  select account_number
    into #temp_mr
    from sk_prod.cust_subs_hist
   where status_code in ('AC','AB','PC')
     and effective_to_dt = '9999-09-09'
     and subscription_sub_type = 'DTV Extra Subscription'
group by account_number
;
create unique hg index temp_mr_u_idx_1 on #temp_mr(account_number);




------------------------------------------------------------------------------------------------------------
-- 2. Get active PSTN panel accounts and boxes from vespa_subscriber_status, filtering out excluded accounts
------------------------------------------------------------------------------------------------------------
drop table #temp_panel_accounts;

  select VSS.account_number
        ,VSS.card_subscriber_id
        ,VSS.panel_no
        ,cast(null as date) as min_dt --needed later
        ,cast(null as varchar(30)) as service_instance_id
        ,cast(0 as bit) as primary_flag
        ,cast(0 as float) as rq
    into #temp_panel_accounts
    from sk_prod.vespa_subscriber_status as VSS
         left join tanghoi.cust_panel_exclusions as CPE on CPE.account_number = VSS.account_number
   where VSS.result = 'Enabled'
     and VSS.panel_no in (6, 7, 12)
     and CPE.account_number is null
;

create          hg index temp_panel_accounts_idx_1      on #temp_panel_accounts(account_number);
create unique   hg index temp_panel_u_accounts_idx_2    on #temp_panel_accounts(card_subscriber_id); --should be unique
create          hg index temp_panel_accounts_idx_3      on #temp_panel_accounts(service_instance_id);


-- Add service_instance_id to each card_subscriber_id
  update #temp_panel_accounts as bas
     set bas.service_instance_id = ccl.service_instance_id
    from sk_prod.cust_card_subscriber_link as ccl
   where ccl.card_subscriber_id = bas.card_subscriber_id
     and current_flag = 'Y'
;


-- Set primary box flag
  update #temp_panel_accounts as bas
     set primary_flag = 1
    from sk_prod.cust_service_instance as csi
   where bas.service_instance_id = csi.service_instance_id
     and si_service_instance_type = 'Primary DTV'
;





--------------------------------------------------------------------------------------------------
-- 3. Get box hardware details and account number for each active STB (unique service_instance_id)
--------------------------------------------------------------------------------------------------
drop table #temp_stb_active;

  select STB.account_number
        ,STB.service_instance_id
        ,STB.x_pvr_type
        ,STB.x_manufacturer
        ,STB.x_model_number
    into #temp_stb_active
    from sk_prod.cust_set_top_box as stb
         inner join #temp_panel_accounts as bas on stb.account_number = bas.account_number
   where x_active_box_flag_new = 'Y'
group by STB.account_number
        ,STB.service_instance_id
        ,STB.x_pvr_type
        ,STB.x_manufacturer
        ,STB.x_model_number
;

  insert into #temp_stb_active
  select bas.account_number
        ,'Unknown'
        ,'Unknown'
        ,'Unknown'
        ,'Unknown'
    into #temp_stb_active
    from #temp_panel_accounts as bas
         left join sk_prod.cust_set_top_box as stb on stb.account_number = bas.account_number
   where stb.account_number is null
;

create hg index temp_stb_active_idx_1 on #temp_stb_active(account_number);
create hg index temp_stb_active_idx_2 on #temp_stb_active(service_instance_id);



-------------------------------------------------------
-- 4. Add the last PSTN callback date per subscriber_id
-------------------------------------------------------
drop table #temp_vespa_cb;

  select PD.subscriber_id
        ,max(dt) as last_dt
    into #temp_vespa_cb
    from vespa_analysts.panel_data       as PD
         inner join #temp_panel_accounts  as PA on convert(int,PA.card_subscriber_id) = PD.subscriber_id
                                              and PD.panel in (6, 7, 12)
                                              and PD.data_received = 1
group by PD.subscriber_id
;

create unique hg index temp_vespa_cb_u_idx_1 on #temp_vespa_cb(subscriber_id);


-- alter table #temp_VoBB_pstn_panel
-- add last_PSTN_cb date default null
-- ;
--
-- update VoBB_pstn_panel as VPP
-- set VPP.last_PSTN_cb = CB.last_dt
-- from #temp_pstn_cb as CB
-- where VPP.card_subscriber_id = right('00000000' + convert(varchar(8),CB.subscriber_id),8)
-- ;




-------------------------------------------------------
--  5. Calculate account-wise last content d/l datetime
-------------------------------------------------------
drop table #temp_last_dl;

  select APD.account_number
        ,max(APD.last_modified_dt)            as last_dl_dt
    into #temp_last_dl
    from sk_prod.cust_anytime_plus_downloads   as APD
         inner join #temp_panel_accounts        as PAn       on PAn.account_number = APD.account_number
group by APD.account_number
;

create hg index temp_last_dl_idx_1 on #temp_last_dl(account_number);






----------------------------------
-- 6. Calculate RQ from panel_data
----------------------------------

-- First, extract the most recent date available
create variable @max_dt datetime;
select @max_dt = max(dt) from vespa_analysts.panel_data;


-- Get the first ever date of data received per subID
drop table #temp_min_dt;

  select subscriber_id
        ,min(dt) as min_dt
    into #temp_min_dt
    from vespa_analysts.panel_data
   where data_received = 1
group by subscriber_id
;


-- Insert the first ever date of data received into panel_accounts
  update #temp_panel_accounts as bas
     set min_dt = mnd.min_dt
    from #temp_min_dt as mnd
   where cast(bas.card_subscriber_id as int) = mnd.subscriber_id
;


-- Now, calculate the RQ, taking into account recent panel additions
drop variable @rq_window;
create variable @rq_window int;
set @rq_window = 15;


drop table #temp_rq;

  select subscriber_id
        ,sum(1.0 * data_received / (case when (@max_dt - min_dt + 1) < @rq_window then (@max_dt - min_dt + 1) else @rq_window end)) as rq
    into #temp_rq
    from vespa_analysts.panel_data       as PD
         inner join #temp_panel_accounts  as PA on convert(int, PA.card_subscriber_id) = PD.subscriber_id
                                              and dt between (@max_dt - @rq_window + 1) and @max_dt
                                              and PD.data_received = 1
   where @max_dt > min_dt
group by subscriber_id
;

create hg index temp_rq_idx_1 on #temp_rq(subscriber_id);


-- Finally, insert RQ metric into main table
  update #temp_panel_accounts as bas
     set rq= trq.rq
    from #temp_rq as trq
   where cast(bas.card_subscriber_id as int) = trq.subscriber_id
;




-------------------------------------------------
-- 7. Calculate CA callback rate in absence of RQ
-------------------------------------------------
drop table #temp_cbck_rate;

  select CBK.account_number
        ,sum(1.0 * CBK.expected_cbcks - CBK.missing_cbcks) / sum(CBK.expected_cbcks) as cbck_rate
    into #temp_cbck_rate
    from vespa_analysts.Waterfall_scms_callback_data     as CBK
         inner join #temp_panel_accounts                  as PA  on PA.account_number = CBK.account_number
                                                               and CBK.expected_cbcks > 0
group by CBK.account_number
;

create hg index temp_cbck_rate_idx_1 on #temp_cbck_rate(account_number);




------------------------------------------------------------------
-- 8. Combine and link to STB hardware details and multiroom flag
------------------------------------------------------------------
drop table #temp_VoBB_pstn_panel;

  select bas.*
        ,case when mrx.account_number is not null then 1 else 0 end as multiroom
        ,case when (stb.x_model_number like 'DRX 89%'
                or  stb.x_manufacturer = 'Samsung'
                or (stb.x_manufacturer = 'Pace' and stb.x_pvr_type = 'PVR4')) then 1  -- Broadband-capable STB
              when stb.x_model_number like 'DRX 595'                          then 2  -- Inelegant but simple addition to flag DRX595 boxes (can be commented/uncommented without affecting the other filters)
              else 0  end as BB_capable
        ,vcb.last_dt as last_vespa_cb
        ,cbk.cbck_rate
        ,ldl.last_dl_dt
    into #temp_VoBB_pstn_panel
    from #temp_panel_accounts                             as bas
         left join #temp_stb_active                       as stb  on bas.service_instance_id             = stb.service_instance_id
         left join #temp_mr                               as mrx  on bas.account_number                  = mrx.account_number
         left join #temp_last_dl                          as ldl  on bas.account_number                  = ldl.account_number
         left join #temp_cbck_rate                        as cbk  on bas.account_number                  = cbk.account_number
         left join #temp_vespa_cb                         as vcb  on convert(int,bas.card_subscriber_id) = vcb.subscriber_id
;

create hg index temp_VoBB_pstn_panel_idx_1 on #temp_VoBB_pstn_panel(account_number);
create hg index temp_VoBB_pstn_panel_idx_2 on #temp_VoBB_pstn_panel(card_subscriber_id);




-------------------------------
-- 9. Generate migration groups
-------------------------------

-- Set conditions to determine migration group label per STB
drop table #temp_VoBB_migration_groups;

  select account_number
        ,card_subscriber_id
        ,panel_no
        ,multiroom
        ,BB_capable
        ,Primary_Flag
        ,cbck_rate
        ,last_dl_dt
        ,case when panel_no in (6,7) then rq * 2 else rq end as rq
        ,case when last_vespa_cb >= (@max_dt - 180) then 'CB within last 180d'
              when last_vespa_cb <  (@max_dt - 180) then 'CB before last 180d'
              when last_vespa_cb is null then 'No CB' end as PSTN_cb_status
        ,case when last_dl_dt >= (@max_dt - 180)   then 'DL within last 180d'
              when last_dl_dt < (@max_dt - 180)    then 'DL before last 180d'
              when last_dl_dt is null then 'No DL' end as OD_dl_status
----------------------------------------- Single Room -----------------------------------------
        ,case when BB_capable = 0                                   then '_no_BB'
              when BB_capable = 2                                   then '_DRX_595'
              when panel_no in (6,7)
               and multiroom = 0
               and last_vespa_cb  is not null
               and primary_flag = 1
               and last_dl_dt >= (@max_dt - 30)  then '1a'                               -- P6/7, BB-capable, PSTN cb, DL within last 30 days "very high probability returners"
              when panel_no = 12
               and multiroom = 0
               and last_vespa_cb is not null
               and primary_flag = 1
               and last_dl_dt >= (@max_dt - 30)  then '1d'                               -- P12, BB-capable, PSTN cb, DL within last 30 days "very high probability returners"
              when panel_no in (6,7)
               and multiroom = 0
               and last_vespa_cb  is not null
               and primary_flag = 1
               and last_dl_dt >= (@max_dt - 90)  then '2a'                               -- P6/7, BB-capable, PSTN cb, DL within last 90 days "high probability returners"
              when panel_no = 12
               and multiroom = 0
               and last_vespa_cb is not null
               and primary_flag = 1
               and last_dl_dt >= (@max_dt - 90)  then '2d'                               -- P12, BB-capable, PSTN cb, DL within last 90 days "high probability returners"
              when panel_no in (6,7)
               and multiroom = 0
               and last_vespa_cb  is not null
               and primary_flag = 1
               and last_dl_dt >= (@max_dt - 180) then '3a'                               -- P6/7, BB-capable, PSTN cb, DL within last 180 days "medium probability returners"
              when panel_no = 12
               and multiroom = 0
               and last_vespa_cb is not null
               and primary_flag = 1
               and last_dl_dt >= (@max_dt - 180) then '3d'                               -- P12, BB-capable, PSTN cb, DL within last 180 days "medium probability returners"
              when panel_no in (6,7)
               and multiroom = 0
               and last_vespa_cb is not null
               and primary_flag = 1
               and last_dl_dt is not null        then '4a'                               -- P6/7, BB-capable, PSTN cb, DL over 180 days ago "low probability returners"
              when panel_no in (12)
               and multiroom = 0
               and last_vespa_cb is not null
               and primary_flag = 1
               and last_dl_dt is not null        then '4d'                               -- P12, BB-capable, PSTN cb, DL over 180 days ago "low probability returners"
              when panel_no in (6,7)
               and multiroom = 0
               and last_vespa_cb is not null
               and primary_flag = 1              then '5a'                               -- P6/7, BB-capable, PSTN cb, No DL "low probability returners"
              when panel_no in (12)
               and multiroom = 0
               and last_vespa_cb is not null
               and primary_flag = 1              then '5d'                               -- P12, BB-capable, PSTN cb, no DL "low probability returners"
              when panel_no in (6,7)
               and multiroom = 0
               and primary_flag = 1              then '6a'                               -- P6/7, BB-capable, no PSTN cb "very low probability returners"
              when panel_no in (12)
               and multiroom = 0
               and primary_flag = 1              then '6d'                               -- P12, BB-capable, no PSTN cb "very low probability returners"
            ----------------------------------------- Multi Room -----------------------------------------
              when panel_no in (6,7)
               and multiroom = 1
               and last_vespa_cb is not null     then 'M5'                               -- P6/7, BB-capable, PSTN cb "low probably returners"
              when panel_no = 12
               and multiroom = 1
               and last_vespa_cb is not null     then 'M6'                               -- P12, BB-capable, PSTN cb "low probably returners"
              when panel_no in (6,7)
               and multiroom = 1
               and last_vespa_cb is null         then 'M7'                               -- P6/7, BB-capable, no PSTN cb ever "very low probability returners, but BB capable"
              when panel_no = 12
               and multiroom = 1
               and last_vespa_cb is null         then 'M7d'                              -- P12, BB-capable, no PSTN cb ever "very low probability returners, but BB capable"
            ----------------------------------------- Everything else -----------------------------------------
              else                                                       '_no_group' end as migration_group
    into #temp_VoBB_migration_groups
    from #temp_VoBB_pstn_panel
;


-- ~100s to run everything up to here in Sybase 16!


------------------------------------------------------------
-- 10. Summarise results and export for enablement campaigns
------------------------------------------------------------

-- Summarise migration groups
select
    migration_group
    , count(distinct account_number)        as accounts
    , count(distinct card_subscriber_id)    as boxes
    , round(avg(rq),3)                      as ARQ
    , round(sum(rq),0)                      as returners
from #temp_VoBB_migration_groups
group by migration_group
;




/*
-- Export for enablement campaign creation

create variable @now datetime;
set @now = now();

--insert into vespa_analysts.panel_movements_log(
         account_number
        ,card_subscriber_id
        ,source
        ,destination
        ,ca_callback_day
        ,request_created_dt
        ,requested_enablement_dt
        ,requested_enablement_route
        ,rq
        ,ca_callback_rate
        ,last_ca_callback_route
        ,multiroom
        ,last_ondemand_download_dt
        ,last_CA_callback_dt
        ,requested_movement_type)
  select vob.account_number
        ,card_subscriber_id
        ,panel_no
        ,case when migration_group in ('1a', '2a', '3a', '4a', '5a', '6a', 'M5', 'M7') then 5 else 11 end
        ,cbk_day
        ,@now
        ,cast(case when cbk_day is null then '2014-04-15' else '2014-0' || case when cast(cbk_day as int) >= 15 then '4-' else '5-' end || right('0' || cbk_day, 2) end as date)
        ,'KQ'
        ,rq
        ,cbck_rate
        ,'Unknown' as last_ca_callback_route
        ,multiroom
        ,last_dl_dt
        ,date(substr(date_time_received,7,4) || '-' || substr(date_time_received,4,2) || '-' || left(date_time_received,2))
        ,'BB Mig. Group ' || migration_group
    from #temp_VoBB_migration_groups as vob
         left join vespa_analysts.Waterfall_scms_callback_data as cbk on cast(vob.card_subscriber_id as int) = cbk.subscriber_id
   where migration_group in ('1a','1d','2a','2d','3a','3d')
*/




