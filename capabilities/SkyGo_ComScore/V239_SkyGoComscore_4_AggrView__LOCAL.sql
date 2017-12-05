/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$     ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$      ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$=      ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$       ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$        ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=        ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$         ODD   ZDDDDDDDN
                                      $$$           .      $DDZ
                                     $$$                  ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        V239 - Oneview Comscore (SkyGo) - Aggregated Event View


###############################################################################
# Created between:   06/08/2014 - 30/10/2014
# Created by:        Alan Barber (ABA)
# Description:
#
# List of steps:
# --------------
#
# code_location_A01  First prepare the information about this load and identify if ok to load
# code_location_A02  Prepare Universe information
# ----------------------------------------------------------
# code_location_B01  Halt the build process and raise error if the raw data looks wrong
# ----------------------------------------------------------
# code_location_C01  Start the conversion of raw data into event aggregate.
#                    First - extract the local day's period keys from VESPA_CALENDAR
# code_location_C02  Extract the required fields from the day's data (from COMSCORE_UNION_VIEW)
#                    handling DQ error codes with conversions
#                    Add a unique ID to each row, which will also order events
# code_location_C03  Form the context of each event by creating the event transitions
#                    using functions lag() and lead()
#                    Identify certain events (including 'orphaned events'), content starts,
#                    and event series start with flags
# code_location_C04  Identify the context of the Orphaned events, and events that signify
#                    the end of content or event series. Also apply fixes to the assetID and
#                    accumulated playing time (ns_st_pt) if required
# code_location_C05  Apply midnight cross-over rules to extend viewing to/from midnight
#                    add local-time fields. Remove orphaned-end events
# code_location_D01  Build an aggregate table using new_event_series = 1, end_event_series = 1,
#                    continuing_content_from = 1, content_continuing_to = 1
#                    Incorporate additional lead/lag context/transistions for requiring fields
# code_location_D02  Compress the prepared start/end event rows into a single 'aggregated' row
#                    Apply rules for allocating the event's play duration
# code_location_E01  Insert the aggregated events into the monthly table. If the start
#                    of a new month, create a new table.
#                    1. Prepare executable/dynamic SQL statement
# code_location_E02  2. Execute the dynamic SQL statement
# code_location_F01  Update basic stats... links back with file/raw data stats
# code_location_G01  Create Channel Mapping Information
# code_location_G02  Make manual adjustments to channel mapping info
# code_location_H01  Split Linear Events into Programme Instances
# code_location_H02  Record stats around splitting linear events into Programme Instances
# code_location_I01  Scan the day's records and make corrections to missing or incorrect data
# code_location_J01  Refresh union view  -- VESPA_Comscore_SkyGo_Union_View
# code_location_K01  Log completion of build procs
# code_location_L01  drop tables no longer requiered
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - sk_prod.VESPA_CALENDAR
#     - VESPA_Comscore_SAV_summary_tbl
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 30/10/2014  ABA   Initial version
# 07/11/2014  ABA   Added @run_date parameter to 'vespa_comscore_skygo_event_view_prep' procedure call
# 13/11/2014  ABA   Added code in union_view build to re-rank available months so the view builds correctly
# 27/11/2014  ABA   Added row_id to the universe_tbl in preparation for splitting the extraction of viewing data
#                   for large quantities... but then commented out, as temp table memory issues
# 09/12/2014  ABA   Code section B01 now referencing 'data_date' field rather than it's previous name
# 09/12/2014  ABA   Added 'drop table #Comscore_skygo_day_stats_tmp' command to F01 & H02: following update statement
# 11/12/2014  ABA   now resets basic_view_created = 0 after failed run
# 11/12/2014  ABA   Section K01 to drop temporary tables at the end of the proc
# 22/12/2014  ABA   Added ability to run when data volume is low -  IF @status not in ('ok','low') then raise error
# 22/12/2014  ABA   Wrapped the remained of script inside BEGIN/END statement for "IF @status not in ('ok','low')" so not run if failure
# 07/01/2015  ABA   Added execution of a new proc to build universe information (replaces previous code)
# 07/01/2015  ABA   Added some indexes to monthly tables when they are first built
# 08/01/2015  ABA   Added indexes to VESPA_CALENDAR_section_tmp on utc columns
#                   Changed use of VESPA_CALENDAR to use VESPA_CALENDAR_section_tmp instead
#                   Call to EXEC 'vespa_comscore_skygo_channelmap_refresh' now includes: @data_run_date
# 13/01/2015  ABA   Set erroneous_data_flag = 1  when  view_continuing_flag = 1  but first event starts way after midnight
# 15/08/2016  ABA   Midnight x-over calculation: added rule to deal with occurences of client before, but server timestamps after midnight cut-off. No rounds to the correct hour (rather than an hour too early!)
#
###############################################################################*/



CREATE or REPLACE procedure barbera.vespa_comscore_skygo_event_view_create(
                IN @data_run_date                 DATE) AS
BEGIN



            /* code_location_C01 *************************************************************************
             *****                                                                                      **
             *****      Start the conversion of raw data into event aggregate.                          **
             *****      First - extract the local day's period keys from VESPA_CALENDAR                 **
             *****                                                                                      **
             *********************************************************************************************/

DECLARE @xsql               varchar(10000)
DECLARE @monthly_table_name varchar(300)


/*
 create or replace VIEW barbera.comscore_union_view as
        select * from comscore_201607
          union all
        select * from comscore_201608
          union all
        select * from sk_prod.comscore_201609  --X
          union all
        select * from comscore_201610
          union all
        select * from comscore_201611
          union all
        select * from comscore_201612
commit
*/

--declare @data_run_date       DATE
--set @data_run_date = '2016-06-01'

            IF object_id('barbera.VESPA_CALENDAR_section_tmp') IS NOT NULL  BEGIN drop table barbera.VESPA_CALENDAR_section_tmp END

            select cast(c.utc_day_date as date) utc_day_date,
                   cast(c.utc_time_hours as integer) utc_time_hours,
                   c.local_day_date, c.local_time_hours,
                   c.daylight_savings_flag
              into barbera.VESPA_CALENDAR_section_tmp -- create a tempory table just with the hours we are interested in for this load, so that the inner joins on these only
              FROM VESPA_CALENDAR c
             where local_day_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
             order by c.utc_day_date, c.utc_time_hours

            --add indexes to VESPA_CALENDAR_section_tmp on utc columns
            create DATE index VESPA_CALENDAR_section_tmp_dayutc_idx on barbera.VESPA_CALENDAR_section_tmp(utc_day_date)
            create LF index VESPA_CALENDAR_section_tmp_timeutc_idx on barbera.VESPA_CALENDAR_section_tmp(utc_time_hours)
            commit



            /* code_location_C02 *************************************************************************
             *****                                                                                      **
             *****      extract the required fields from the day's data (from COMSCORE_UNION_VIEW)      **
             *****      handling DQ error codes with conversions                                        **
             *****      Add a unique ID to each row, which will also order events                       **
             *****                                                                                      **
             *********************************************************************************************/

            --Create table for the day. Extract by server-side local-time. Cut-down dataset to required fields
             --drop tmp table before we start if exists
            IF object_id('barbera.V239_comscore_event_tmp5') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.V239_comscore_event_tmp5
                END

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Extracting raw Comscore data'

            select dense_rank() over(order by coalesce(cast(t.sam_profileid as varchar), t.ns_st_id), ns_ap_device, ns_ts_raw, ns_st_ec asc, ns_utc_raw, cb_row_id asc) uniqid,
                   t.cb_row_id,
                   ns_ap_ec,
                   case when ns_ap_pfm = 'unknown' then sg_vs_dt else ns_ap_pfm end as ns_ap_pfm,   --correction especially for playstaion where ns_ap_pfm is unknown
                   ns_ap_pfv,
                   ns_radio,
                   agent,
                   case when ns_st_ev like 'ad_%' then 1 else ns_st_ad end          as ns_st_ad,
                   ns_st_bp, ns_st_bt,
                   ns_st_ca,
                   case when ns_st_cl >= 0 then ns_st_cl else null end              as ns_st_cl,  --handling DQ error codes
                   ns_st_cn, ns_st_ec,
                   case when ns_st_el >= 0 then ns_st_el else null end              as ns_st_el,  --handling DQ error codes
                   ns_st_ep,
                   ns_st_ge, ns_st_hc, ns_st_id,
                   ns_vid, --added 2016-07-26 for reconciliation work with activity capture
                   ns_st_it, ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe,
                   ns_st_pl, ns_st_pn,
                   ns_st_po,
                   lead(ns_st_po) over(partition by sam_profileid, ns_ap_device, ns_st_id, ns_st_ad order by ns_ts_raw, ns_st_ec)   as next_ns_st_po,
                   ns_st_pp,
                   case when ns_st_pr in ('preroll','midroll') then null else ns_st_pr end       as ns_st_pr, --cleansing programme name of preroll entry
                   ns_st_sp, ns_st_sq, ns_st_st, ns_st_tp, ns_st_ty,
                   case when ns_st_ci = 'unknown' then null else ns_st_ci end       as ns_st_ci, --cleanse unknowns from assetID
                   case when sg_vs_sc in ('vod','lin','dvod') then sg_vs_sc else 'unknown' end sg_vs_sc,
                   t.sam_profileid,
                   ns_ap_device,
                   ns_utc,
                   ns_utc_raw,
                   -- dateadd(hh, c.daylight_savings_flag, ns_utc) ns_server_local_time,
                   time_of_day, --no longer required as calculating datetime version above
                   ns_ts_raw,
                   ns_ts,
                   ns_st_ev,
                   case when ns_st_pt_raw >= 0 then ns_st_pt_raw else null end ns_st_pt_raw, --handling DQ error codes
                   case when ns_st_pt >= 0     then ns_st_pt     else null end ns_st_pt,  --handling DQ error codes
                   case when ns_st_ev like '%play' and ns_st_ad = 1 then (next_ns_st_po - ns_st_po) else null end               as ad_play_duration_raw --for correcting ns_st_pt during ad events
              into barbera.V239_comscore_event_tmp5
              from barbera.COMSCORE_UNION_VIEW t
/*                     INNER JOIN barbera.VESPA_CALENDAR_section_tmp c -- using just with the hours we are interested in for this load, so that the inner joins is efficient
                    on cast(dateformat(t.ns_utc,'YYYY-MM-DD') as date) = c.utc_day_date
                   and cast(dateformat(t.ns_utc,'hh') as Integer)      = c.utc_time_hours
*/             --where t.sam_profileid is not null --later we need to include these, but the data is not good enough yet
                 where t.cb_source_file = 'Comscore_'||dateformat(cast(@data_run_date as date), 'YYYYMMDD')||'.gz'
            commit



            /*******************************************************************
             * update station name (ns_st_st) filling in 'unknown' station names
             * using identified (sk) code and Channel Mapping data
             *******************************************************************/

            create unique index sk_list_tmp_row_idx on barbera.V239_comscore_event_tmp5(cb_row_id)

            IF object_id('barbera.sk_list_tmp') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.sk_list_tmp commit
                END

            select cb_row_id, cast(ns_st_ci as INTEGER) as service_key, ns_st_st, ns_st_pl
              into sk_list_tmp
              from barbera.COMSCORE_UNION_VIEW c
             where c.cb_source_file = 'Comscore_'||dateformat(cast(@data_run_date as date), 'YYYYMMDD')||'.gz'
               and ns_st_st = 'unknown'
               and ns_st_cu like '%/'||coalesce(ns_st_ci,'____')||'/%'   --confirm if the service key is in the CDN resource descriptor
            commit

            create unique index sk_list_tmp_row_idx on sk_list_tmp(cb_row_id)
            create HG index sk_list_tmp_sk_idx on sk_list_tmp(service_key)    


            IF object_id('barbera.sk_list_vespa_name_tmp') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.sk_list_vespa_name_tmp commit
                END 
            
            select sk.*, cm.vespa_name
              into sk_list_vespa_name_tmp
              from sk_list_tmp sk
                     left join
                   VESPA_ANALYSTS.channel_map_prod_service_key_attributes cm
                     on sk.service_key = cm.service_key
             where cast(@data_run_date as date) between effective_from and effective_to
            commit

            create unique index sk_list_vespa_name_tmp_row_idx on sk_list_vespa_name_tmp(cb_row_id)


            --make updates
            UPDATE barbera.V239_comscore_event_tmp5 x
               SET ns_st_st = coalesce(sk.vespa_name, 'unknown')
              from sk_list_vespa_name_tmp sk
             where x.cb_row_id = sk.cb_row_id
            commit

            -- try an additional update where the playlist title used for a linear stream is the channel name
            UPDATE barbera.V239_comscore_event_tmp5 x
               SET x.ns_st_st = sk.ns_st_pl
              from sk_list_vespa_name_tmp sk
             where x.cb_row_id = sk.cb_row_id
               and lower(sk.ns_st_pl) = lower(sk.vespa_name)
               and coalesce(sk.ns_st_st, 'unknown') = 'unknown'
               and x.ns_st_st = 'unknown'
            commit

            drop table sk_list_tmp              commit
            drop table sk_list_vespa_name_tmp   commit



            /*******************************************************************
             * Continue preparing data
             *
             *******************************************************************/
            IF object_id('barbera.V239_comscore_event_tmp4') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.V239_comscore_event_tmp4
                END

            --correct ns_st_pt for ad play events
            --add the server local-time
            select uniqid, cb_row_id,
                   ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
                   ns_radio, agent, ns_st_ad, ns_st_bp, ns_st_bt, ns_st_ca,
                   ns_st_cl, ns_st_cn, ns_st_ec, ns_st_el, ns_st_ep,
                   ns_st_ge, ns_st_hc, ns_st_id,
                   ns_vid, --added 2016-07-26 for reconciliation work with activity capture
                   ns_st_it, ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe,
                   ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr,
                   ns_st_sp, ns_st_sq, ns_st_st, ns_st_tp, ns_st_ty,
                   ns_st_ci, sg_vs_sc,
                   sam_profileid,
                   ns_ap_device, ns_utc, ns_utc_raw, time_of_day,
                   ns_ts_raw, ns_ts,
                   case when ns_st_ev like 'ad_%' then substring(ns_st_ev, 4) else ns_st_ev end as ns_st_ev,  -- transformation for stripping out ad_
                   ns_st_pt_raw, 
                   --ns_st_pt,
                   case when ns_st_ad = 1 then
                                sum(ad_play_duration_raw/1000) over(partition by sam_profileid, ns_ap_device, ns_st_id, ns_st_ad order by ns_ts_raw, ns_st_ec 
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                        else ns_st_pt
                    end as ns_st_pt,
                   dateadd(hh, c.daylight_savings_flag, a.ns_utc) ns_server_local_time
              into barbera.V239_comscore_event_tmp4
              from barbera.V239_comscore_event_tmp5 a
        INNER JOIN barbera.VESPA_CALENDAR_section_tmp c -- using just with the hours we are interested in for this load, so that the inner joins is efficient
                on cast(dateformat(a.ns_utc,'YYYY-MM-DD') as date) = c.utc_day_date
               and cast(dateformat(a.ns_utc,'hh') as Integer)      = c.utc_time_hours
            commit



            /* code_location_C03 *************************************************************************
             *****                                                                                      **
             *****      Form the context of each event by creating the event transitions                **
             *****      using functions lag() and lead()                                                **
             *****      Identify certain events (including 'orphaned events'), content starts,          **
             *****      and event series start with flags                                               **
             *****                                                                                      **
             *********************************************************************************************/

            --drop tmp table before we start if exists
            IF object_id('barbera.V239_comscore_event_tmp3') IS NOT NULL
                BEGIN
                    DROP TABLE barbera.V239_comscore_event_tmp3
                END

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Applying universe restriction, and preparing view'

            -- create the low-level event view [this had a temp table memory issue so added an index to the universe, may cause problems in the future if greater volume of data is received]
            select uniqid,
                   t.cb_row_id,
                   s.aggregate_flag,
                   ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
                   lag(ns_radio) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_radio,
                   ns_radio,
                   agent,
                   lag(ns_st_ad) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_st_ad, --account, device, ip (for location if more than one person using the same account)?
                   ns_st_ad,
                   lead(ns_st_ad) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) next_ns_st_ad,
                   ns_st_bp, 

ns_st_bt, --add in lead/lag on clip accum buffering time here

                   ns_st_ca, ns_st_cl, ns_st_cn, ns_st_ec,
                   ns_st_el, ns_st_ep,
                   ns_st_ge, ns_st_hc, ns_st_id,
                   ns_vid, --added 2016-07-26 for reconciliation work with activity capture
                   ns_st_it, ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe,
                   ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr,
                   ns_st_sp, ns_st_sq,
                   lag(ns_st_st) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_st_st, --added 2015-11-04
                   ns_st_st,
                   ns_st_tp, ns_st_ty,
                   lag(ns_st_ci) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_st_ci, --account, device, ip (for location if more than one person using the same account)?
                   ns_st_ci,
                   lead(ns_st_ci) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) next_ns_st_ci,
                   time_of_day, -- don't need this as using the derived 'ns_server_local_time' below
                   sg_vs_sc,
                   --not using the aggregate flag
                   --case when s.aggregate_flag = 1 then null else s.account_number end as account_number,
                   --case when s.aggregate_flag = 1 then null else s.cb_key_household end as cb_key_household,
                   s.account_number,
                   s.cb_key_household,
                   t.sam_profileid,
                   ns_ap_device,
                   ns_utc,
                   ns_utc_raw,
                   ns_server_local_time,
                   lag(ns_ts_raw) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_ts_raw,
                   ns_ts_raw,
                   lag(ns_ts) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_ts,
                   ns_ts,
                   lead(ns_ts) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) next_ns_ts,
                   lag(ns_st_ev) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) prev_ns_st_ev,
                   ns_st_ev,
                   lead(ns_st_ev) over(partition by t.sam_profileid, ns_ap_device  order by uniqid) next_ns_st_ev,
                   ns_st_pt_raw,
                   lag(ns_st_pt) over(partition by t.sam_profileid, ns_ap_device order by uniqid) prev_ns_st_pt,
                   case when t.ns_st_pt = 0 AND t.ns_st_pt_raw between 501 and 999 then 1               --bring back short events > half-second, < second [round to a second]
                        else t.ns_st_pt
                    end ns_st_pt,
                   lead(ns_st_pt) over(partition by t.sam_profileid, ns_ap_device order by uniqid) next_ns_st_pt,
                   floor((ns_ts_raw - prev_ns_ts_raw)/1000) event_secs,

                    --new content/channel
                   case when prev_ns_st_ev is null and ns_st_ev = 'keep-alive' and datediff(ss,'00:00:00', dateformat(ns_server_local_time, 'HH:MM:SS')) /*20mins*/ < 1200 then 0
                        when prev_ns_st_ev is null and ns_st_ev = 'end'  then 0
                        when prev_ns_st_ev = 'play' AND ns_st_ev = 'end' then 0
                        when coalesce(prev_ns_st_ad, -1) != coalesce(ns_st_ad, -1) then 1 --moved this to before linear condition (2015-11-04)

                        when sg_vs_sc = 'lin' AND (ns_st_st = 'unknown' OR prev_ns_st_st = 'unknown') then 0 --treat as we dont know whats happening, continue blindly  <--added 2015-11-06

                        when sg_vs_sc = 'lin' AND prev_ns_st_st != ns_st_st then 1 --basically a new channel is being viewed  <-- added 2015-11-04

                        when sg_vs_sc != 'lin' AND (ns_st_ci = 'unknown' OR prev_ns_st_ci = 'unknown') then 0 --treat as we dont know whats happening, continue blindly  <--added 2015-11-06

                        when sg_vs_sc != 'lin' AND prev_ns_st_ci != ns_st_ci then 1 --general condition for VOD/DVOD content changing
                        else 0 end new_content, -- this doesn't work too well when we want to take the max(accumulated_playtime) later in the code

                    --continuing content/channel
                   case when prev_ns_st_ev is null and ns_st_ev = 'keep-alive' and datediff(ss,'00:00:00', dateformat(ns_server_local_time, 'HH:MM:SS')) /*20mins*/ < 1200 then 1
                        when prev_ns_st_ev is null and ns_st_ev = 'keep-alive' and prev_ns_st_ci = ns_st_ci then 1
                        when prev_ns_st_ev is null and ns_st_ev = 'end' then 1
                        else 0 end continuing_content_from,

                    --content_continuing_to
                   case when ns_st_ev in ('play','keep-alive') and next_ns_st_ev is null and datediff(ss,'00:00:00', dateformat(ns_server_local_time, 'HH:MM:SS')) /*days_secs*/ >= 85200 then 1
                        else 0 end content_continuing_to,

                    --new event series
                   case when coalesce(prev_ns_st_ev, 'end') = 'end' AND ns_st_ev = 'play' then 1
                        when sg_vs_sc != 'lin' AND prev_ns_st_ev = 'play' AND ns_st_ev = 'play' AND datediff(ss, prev_ns_ts, ns_ts) > 20*60  then 1 -- and diff>20mins
                        when new_content = 1 then 1
                        --when prev_ns_radio not in ('none','unknown') AND ns_radio not in ('none','unknown') AND prev_ns_radio != ns_radio then 1   --connection_type has changed, so we need to split the event - NOT POSSIBLE AT THE MOMENT using ns_st_pt                      
                        --additonal rules for linear                            
                        when sg_vs_sc = 'lin' AND prev_ns_st_ev = 'pause'                   AND ns_st_ev = 'play'         then 1
                        when sg_vs_sc = 'lin' AND coalesce(prev_ns_st_ev, 'end') = 'end'    AND ns_st_ev = 'keep-alive'   then 1  
                        when sg_vs_sc = 'lin' AND coalesce(prev_ns_st_ev, 'play') in ('play', 'keep-alive') AND ns_st_ev = 'keep-alive' 
                                              AND datediff(ss, prev_ns_ts, ns_ts) > 20*60+1                               then 1

   --[don't think this is right, not setting up to catch "play -> pause -> end", but "pause -> play -> end"]
   --removing rule [2015-11-06]          when sg_vs_sc = 'lin' AND prev_ns_st_ev = 'pause' AND ns_st_ev in ('play','end') then 1 --having 'end' in this list forces 'pause','end','null' sequences to be a single row event starting and ending with the one entry, these are then removed (as end is a none event)

                        else 0
                    end new_event_series, --this is better at recognising events

                   --mimick the rule for new_event_series that looks at the secs difference between events. Flag if greater than 20mins+1sec
                   case when sg_vs_sc = 'lin' AND coalesce(prev_ns_st_ev, 'play') in ('play', 'keep-alive') AND ns_st_ev = 'keep-alive' 
                                              AND datediff(ss, prev_ns_ts, ns_ts) > 20*60+1                               then 1
                        else 0 
                    end as keep_alive_cap_flag, -- [added 2017-03-24] 
                    --play event
                   case when ns_st_ev = 'play'  then 1
                        when ns_st_ev = 'keep-alive'  then 1
                        else 0 end play_event,
                    --keep-alive event
                   case when ns_st_ev = 'keep-alive'  then 1
                        else 0 end keep_alive_event,
                    --play duration
                   case when prev_ns_st_ev = 'play' and event_secs > 0 then event_secs
                        when prev_ns_st_ev = 'keep-alive' and event_secs > 0 then event_secs
                        else 0 end play_duration,
                    --pause event
                   case when ns_st_ev = 'pause'  then 1
                        else 0 end pause_event,
                    --pause duration
                   case when prev_ns_st_ev = 'pause' and ns_st_ev = 'end' then 1
                        when prev_ns_st_ev = 'pause' and ns_st_ev = 'play' then 1
                        else 0
                    end pause_duration,
                    --end event
                   case when prev_ns_st_ev = 'end' and ns_st_ev != 'play' then 1
                        when prev_ns_st_ev = 'pause' and ns_st_ev = 'end' then 1
                        when ns_st_ev = 'end' then 1
                        --when pause_ev = 0 and play_ev = 0 and prev_ns_st_ev is not null then 1
                        else 0
                    end end_ev,
                   case when prev_ns_st_ev = 'pause' AND ns_st_ev = 'end' AND content_continuing_to != 1 then 1
                        when prev_ns_st_ev = 'end' AND ns_st_ev = 'pause' AND content_continuing_to != 1 then 1
                        else 0
                    end orphaned_end,     --  'pause' -> 'end' events are considered unnecessary, so marked as 'orphaned'
                     --lag_new_content_group
                    row_number() over(order by t.sam_profileid, ns_ap_device, ns_st_ci, ns_ts, ns_st_ec) row,                    --not required
                    row_number() over(partition by t.sam_profileid, ns_ap_device, ns_st_ci order by ns_ts, ns_st_ec) group_row,  --not required
                    count(1) over(partition by t.sam_profileid, ns_ap_device, ns_st_ci) event_count   --replaced later in preference for the difference in unique IDs
               into barbera.V239_comscore_event_tmp3
               from barbera.V239_comscore_event_tmp4 t
                     INNER JOIN barbera.VESPA_Comscore_SkyGo_universe_tbl s -- the universe table (limit using row_id in batches for large volumes of data)
                     --LEFT JOIN barbera.VESPA_Comscore_SkyGo_universe_tbl s
                     on t.sam_profileid = s.sam_profileid
              where s.exclude_flag != 1
                    --coalesce(s.exclude_flag, 0) != 1   --this makes the query way too slow, so we will only use known accounts
              order by t.sam_profileid, ns_ap_device, ns_ts,/* ns_st_ci,*/ ns_st_ec asc

             commit



        /* code_location_C04 *************************************************************************
         *****      Identify the context of the Orphaned events, and events that signify            **
         *****      the end of content or event series. Also apply fixes to the assetID and         **
         *****      accumulated playing time (ns_st_pt) if required                                 **
         *********************************************************************************************/

        IF object_id('barbera.V239_comscore_event_tmp2') IS NOT NULL
            BEGIN
                drop table barbera.V239_comscore_event_tmp2
            END

        SELECT uniqid, cb_row_id,
               aggregate_flag,
               ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
               ns_radio, agent,
               prev_ns_st_ad,
               case when prev_ns_st_ci = ns_st_ci and prev_ns_st_ad != ns_st_ad then prev_ns_st_ad else ns_st_ad end ns_st_ad, --corrects for cases where the ad_flag is not set
               next_ns_st_ad,
               ns_st_bp, --playlist buffering

ns_st_bt,  --clip buffering
--if prev_bt < bt AND prev_ev = 'pause' then bt-prev_bt else 0 end buffering  --this buffering should be added to a live offset duration

ns_st_ca, ns_st_cl, ns_st_cn, ns_st_ec,
               ns_st_el, ns_st_ep, ns_st_ge, ns_st_hc, ns_st_id,
               ns_vid, --added 2016-07-26 for reconciliation work with activity capture
               ns_st_it,
               ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe, ns_st_pl,
               ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr, ns_st_sp, ns_st_sq,
               ns_st_st, ns_st_tp, ns_st_ty, prev_ns_st_ci,
               --ns_st_ci,
               next_ns_st_ci,
               time_of_day, --   <-- not required anymore
               ns_server_local_time,
               sg_vs_sc, account_number, cb_key_household, sam_profileid,
               ns_ap_device,
               ns_utc, ns_utc_raw,
               prev_ns_ts, ns_ts_raw, ns_ts, next_ns_ts,
               prev_ns_st_ev, ns_st_ev, next_ns_st_ev,
               ns_st_pt_raw, prev_ns_st_pt, ns_st_pt,
               next_ns_st_pt,
               event_secs,
               --lag(orphaned_end) over(partition by sam_profileid, ns_ap_device  order by ns_ts, ns_st_ec) prev_orphaned_end,
               lag(orphaned_end) over(partition by sam_profileid, ns_ap_device  order by uniqid) prev_orphaned_end,
               orphaned_end,
               --lead(orphaned_end) over(partition by sam_profileid, ns_ap_device  order by ns_ts, ns_st_ec) next_orphaned_end,
               lead(orphaned_end) over(partition by sam_profileid, ns_ap_device  order by uniqid) next_orphaned_end,
               new_content, continuing_content_from, content_continuing_to,
               case when coalesce(prev_ns_st_ev, 'end') = 'end' 
                         and ns_st_ev = 'keep-alive'   -- [added 2017-02-24] (basically if the event starts with a keep-alive, start a new event series)
                         and datediff(ss,'00:00:00', dateformat(ns_server_local_time, 'HH:MM:SS')) /*20mins*/ > 1201 -- [added 2017-03-01] (trigger new series if NOT continuing from previous day)
                         then 1
                    when prev_orphaned_end = 1 then 1 --this is so when an orphaned end event is removed, we treat the following row as a new event series
                    else e.new_event_series
                end new_event_series,
               new_event_series as old_new_event_series,
               keep_alive_cap_flag,
               play_event, keep_alive_event, play_duration, pause_event, pause_duration,
               end_ev,
               row, group_row, event_count,
               --e.new_content,
               case --when ns_st_pt = 0 AND prev_ns_st_ev = 'play' AND ns_st_ev = 'play' then
                    when ns_st_pt = 0 then prev_ns_st_pt
                    else ns_st_pt
                end fake_ns_st_pt,--used in cases where it's the last event for the account on the day (pulls forward the accumulated play time)
               --replace asset_id when it's just changed but is an end event
               case when e.prev_ns_st_ci != e.ns_st_ci AND e.ns_st_ev = 'end' /*and e.next_ns_st_ev is null*/ then e.prev_ns_st_ci
                    else e.ns_st_ci
                end ns_st_ci,
               case when ns_st_ev = 'end' and next_ns_st_ev is null then 1
                    when ns_st_ev = 'pause' AND next_ns_st_ev is null then 1
                    when ns_st_ev = 'play' AND next_ns_st_ev = 'end' AND content_continuing_to = 0 AND coalesce(ns_st_ci, 'un') = coalesce(next_ns_st_ci, 'un') then 0    --as this event is not the end event
                    when content_continuing_to = 1 then 0
                    else lead(e.new_content) over(partition by sam_profileid, ns_ap_device order by uniqid)
                end end_content,
               case when prev_ns_st_ev is null AND ns_st_ev = 'end' AND continuing_content_from = 1 then 1
                    when ns_st_ev = 'end' AND next_ns_st_ev is null AND content_continuing_to != 1 then 1
                    when ns_st_ev = 'pause' AND coalesce(next_ns_st_ev, 'end') = 'end' AND content_continuing_to != 1 then 1
                    when ns_st_ev = 'play' AND next_ns_st_ev is null AND content_continuing_to != 1 then 1
                    when ns_st_ev = 'play' AND next_ns_st_ev = 'end' AND content_continuing_to != 1 then 0    --as next event is the end event, and not this one
                    when content_continuing_to = 1 then 0
                    --#####
                    --need to expand this for linear viewing... to split up events...
                    when sg_vs_sc='lin' AND prev_ns_st_ev = 'play' AND ns_st_ev in ('pause','end') then 1 --[added 2016-12-21]  pause added [2017-03-07]
                    when sg_vs_sc='lin' AND prev_ns_st_ev = 'keep-alive' AND ns_st_ev in ('pause','end') then 1 --[added 2017-03-02]    pause added [2017-03-07] 
                    --when sg_vs_sc='lin' AND ns_st_ev  = 'keep-alive' AND coalesce(next_ns_st_ev, 'end') = 'end' then 1 --[added 2017-03-10] <-- NOT CORRECT
                    -- sequence: end  -->  keep-alive  -->  play   (is not being captured - but not valid anyway)  if keepalive play transition is greater than 20mins then we could 
                    --                                              make it an orphan and strip out, just incase it makes it through
                    when next_orphaned_end = 1 then 1 --- required as the next step removes orphaned events leaving end_event_series as 0 (no ending for the event)
                    when keep_alive_cap_flag = 1 then 1 -- the event is being capped so should act as the end of an event series
                    else lead(e.new_event_series) over(partition by sam_profileid, ns_ap_device order by uniqid) --this doesnt always get picked up for some reason

                end end_event_series
               into barbera.V239_comscore_event_tmp2
           from barbera.V239_comscore_event_tmp3 e
          order by uniqid
        --drop table V239_comscore_event_tmp2
        commit



        /* code_location_C05 *************************************************************************
         *****    Apply midnight cross-over rules to extend viewing to/from midnight                **
         *****    add local-time fields. Remove orphaned-end events                                 **
         *****                                                                                      **
         *********************************************************************************************/

        IF object_id('barbera.V239_comscore_event_tmp') IS NOT NULL
            BEGIN
                drop table barbera.V239_comscore_event_tmp
            END

        SELECT e.uniqid, e.cb_row_id, aggregate_flag,
             ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
             case when viewing_start_client_utc is null then null
                  else ns_radio
              end ns_radio_start,
             case when viewing_end_client_utc is null then null
                  else ns_radio
              end ns_radio_end,
             agent, prev_ns_st_ad, ns_st_ad, next_ns_st_ad, ns_st_bp, ns_st_bt,
             ns_st_ca, ns_st_cl, ns_st_cn,
             ns_st_ec,
             ns_st_el, ns_st_ep, ns_st_ge, ns_st_hc, ns_st_id, 
             ns_vid, --added 2016-07-26 for reconciliation work with activity capture
             ns_st_it, ns_st_li,
             ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe, ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr, ns_st_sp, ns_st_sq,
             ns_st_st, ns_st_tp, ns_st_ty,
             prev_ns_st_ci, next_ns_st_ci,
             sg_vs_sc,
             account_number, cb_key_household, sam_profileid, ns_ap_device,            
             --ns_ts is based on utc at the client. This means that we need to adjust the cut-off according to daylight savings (summer is -1hr from midnight, winter is midnight)
             case when new_event_series = 1 AND end_event_series = 1 then prev_ns_ts  --if there is only one row of data explaining the event then we need to use the previous event timestamp, otherwise event will have 0 duration
                  when new_event_series = 1 then ns_ts
                  when continuing_content_from = 1 AND DATEFLOOR(hh, ns_ts) = DATEFLOOR(hh, ns_utc)
                            then DATEFLOOR(hh, ns_ts) -- <---- test this, not sure it works  [added line below 2016-08-15 to deal with client ts before, but server after cut-off occurences]
                  when continuing_content_from = 1 AND dateadd(hh, 1, DATEFLOOR(hh, ns_ts)) = DATEFLOOR(hh, ns_utc)
                            then dateadd(hh, 1, DATEFLOOR(hh, ns_ts))   -- <--- this could lead to negative durations which is what we want as we should subtract from previous day
                  else null
              end viewing_start_client_utc,
             case when content_continuing_to = 1 then DATECEILING(hh, ns_ts)
                  --next rule used for capping  
                  when end_event_series = 1 AND keep_alive_cap_flag = 1 then /*cap*/ dateadd(ss, 20*60, prev_ns_ts) --used to cap keep-alive events to 20mins
                  when end_event_series = 1 then ns_ts
                  else null
              end viewing_end_client_utc,
             dateadd(hh, c_ts.daylight_savings_flag, viewing_start_client_utc) viewing_start_client_local,
             dateadd(hh, c_ts.daylight_savings_flag, viewing_end_client_utc)  viewing_end_client_local,
             case when viewing_start_client_utc is null then null
                  else c_ts.daylight_savings_flag
              end daylight_savings_flag_start,
             case when viewing_end_client_utc is null then null
                  else c_ts.daylight_savings_flag
              end daylight_savings_flag_end,
             prev_ns_ts,
             ns_ts,
             next_ns_ts,
             ns_ts_raw,
             ns_ts_raw - truncnum(ns_ts_raw,-3) client_millis,                  --may not need this
             ns_utc_raw - truncnum(ns_utc_raw,-3) server_millis,                --may not need this
             datediff(ms, viewing_start_client_utc, ns_ts) start_day_ts_diff,   --may not need this
             datediff(ms, ns_ts, viewing_end_client_utc) end_day_ts_diff,       --may not need this
             datediff(ms, viewing_start_client_utc, ns_utc) start_day_utc_diff, --may not need this
             datediff(ms, ns_utc, viewing_end_client_utc) end_day_utc_diff,     --may not need this
             case when viewing_start_client_utc is null then null
                  --when start_day_ts_diff > 0 then ns_ts_raw - start_day_ts_diff - client_millis
                  when continuing_content_from = 1 OR keep_alive_cap_flag = 1 then datediff(ms, '1970-01-01 00:00:00.000000', viewing_start_client_utc) --ok to not use milliseconds as rounding to hour
                  else ns_ts_raw
              end viewing_start_client_utc_raw,
             case when viewing_end_client_utc is null then null
                  --when end_day_ts_diff > 0 then ns_ts_raw + end_day_ts_diff - client_millis
                  when content_continuing_to = 1 OR keep_alive_cap_flag = 1 then datediff(ms, '1970-01-01 00:00:00.000000', viewing_end_client_utc) --ok to not use milliseconds as rounding to hour
                  else ns_ts_raw
              end viewing_end_client_utc_raw,
             -- UTC at Comscore server
             case when viewing_start_client_utc is null then null
             --adjust for continuing from
                  when continuing_content_from = 1 then DATEFLOOR(hh, ns_utc)
                  else ns_utc
              end server_start_utc,
             case when viewing_end_client_utc is null then null
                  when content_continuing_to = 1 then DATECEILING(hh, ns_utc)
                  else ns_utc
              end server_end_utc,
             --raw server_utc milliseconds
             case when viewing_start_client_utc is null then null
                  --when start_day_utc_diff > 0 then ns_utc_raw - start_day_utc_diff - server_millis                    -- [removed 2016-08-16]
                  when continuing_content_from = 1 then datediff(ms, '1970-01-01 00:00:00.000000', server_start_utc)    -- [added 2016-08-16:  reset to midnight/11pm for continuing events]
                  else ns_utc_raw
              end server_start_utc_raw,
             case when viewing_end_client_utc is null then null
                  --when end_day_utc_diff > 0 then ns_utc_raw + end_day_utc_diff - server_millis                    -- [removed 2016-08-16]
                  when content_continuing_to = 1 then datediff(ms, '1970-01-01 00:00:00.000000', server_end_utc)    -- [added 2016-08-16:  reset to midnight/11pm for continuing events]
                  else ns_utc_raw
              end server_end_utc_raw,
             --Comscore local-time [ based on time_of_day] (used to determine file cut-off point)
             case when viewing_start_client_utc is null then null
                  when continuing_content_from = 1 then
                            dateadd(hh, c_utc.daylight_savings_flag, server_start_utc)
                  else ns_server_local_time   -- better than time_of_day provided by Comscore
              end server_start_local_time,
             case when viewing_end_client_utc is null then null
                  when content_continuing_to = 1 then
                            dateadd(hh, c_utc.daylight_savings_flag, server_end_utc)
                  else ns_server_local_time   -- better than time_of_day provided by Comscore
              end server_end_local_time,
             prev_ns_st_ev, ns_st_ev, next_ns_st_ev,

             --adjust all the accumulated play times to midnight if continuing or continued event
             --might be best to do this in one of the next sections??
              prev_ns_st_pt,
              ns_st_pt, ns_st_pt_raw, --if continuing: pt+millis, if continued: remove time before midnight
              next_ns_st_pt,
             case when viewing_start_client_utc is not null then uniqid --used to use ns_st_ec but is very unreliable so better to use self-generated uniqid
                  else null
              end start_uniqid,
             case when viewing_end_client_utc is not null then uniqid
                  else null
              end end_uniqid,
             event_secs, --can be worked out more accurately using the raw milliseconds now....  done in next query
             new_content, continuing_content_from, content_continuing_to,
             -- need a column to flag erroneous data suspected
             case when lag(e.end_event_series) over(partition by sam_profileid, ns_ap_device order by uniqid) = 1 AND old_new_event_series = 0 then 1                  
                  -- we could also flag: case when prev_ns_st_ev is null and ns_st_ev = 'keep-alive' then 1    --when an event starts with keep-alive... seems ok for most cases though
                  -- this was added as a rule into identifying an event_start on 2017-02-24
                  else 0
              end erroneous_data_suspected_flag,             
             --fix any missing start events if the end has been recognised but not the next start
             --new_event_series,
             case when lag(e.end_event_series) over(partition by sam_profileid, ns_ap_device order by uniqid) = 1 AND new_event_series = 0 then 1
                  else new_event_series
              end new_event_series,
             keep_alive_cap_flag, 
             play_event, play_duration, pause_event, pause_duration, end_ev,
             row, group_row,
             event_count,
             fake_ns_st_pt,
             ns_st_ci,
             end_content,
             end_event_series             
        into barbera.V239_comscore_event_tmp
        from barbera.V239_comscore_event_tmp2 e,
             --barbera.VESPA_CALENDAR_section_tmp c_ts,   <-- commented out as restricted to local server day records and client-side timestamps can fall outside this range
             VESPA_CALENDAR c_ts,                         --  <-- [change applied 2015-11-09, now includes client-side events that fall outside local_data_date range]
             barbera.VESPA_CALENDAR_section_tmp c_utc
       where cast(ns_ts as date) = c_ts.utc_day_date
         and cast(dateformat(ns_ts,'hh') as Integer) = c_ts.utc_time_hours
         and cast(ns_utc as date) = c_utc.utc_day_date
         and cast(dateformat(ns_utc,'hh') as Integer) = c_utc.utc_time_hours
         and orphaned_end != 1
        commit



        /* code_location_C06 *************************************************************************
         *****                                                                                      **
         *****    1. Removal of events that are non-events                                          **
         *****                                                                                      **         
         *********************************************************************************************/

      --1st remove any event that is an end event (ns_st_ev = 'end') as these events are handled in context of the remaining events
        DELETE barbera.V239_comscore_event_tmp t
         where t.new_event_series = 1
           and t.end_event_series = 1
           and t.sg_vs_sc = 'lin'
           and coalesce(t.prev_ns_st_ev, 'end') = 'end'
        commit


--Q: What happens to longer events that end with a keep-alive that needs capping - this would not occure on one line?

   --make amendment here to single row events: make sure start and end are correct? capped etc.??  
    --note that we have not changed server times here. We need to swap from using server time to connect linear viewing to use CORRECTED client time






        /* code_location_D01 *************************************************************************
         *****                                                                                      **
         *****      Build an aggregate table using new_event_series = 1, end_event_series = 1,      **
         *****      continuing_content_from = 1, content_continuing_to = 1                          **
         *****      Incorporate additional lead/lag context/transistions for requiring fields       **
         *********************************************************************************************/

        IF object_id('barbera.V239_comscore_view2') IS NOT NULL
            BEGIN
                drop table barbera.V239_comscore_view2
            END

        select uniqid,
               account_number,
               cb_key_household,
               sam_profileid,
               aggregate_flag,
               ns_ap_device,
               ns_ap_pfm platform_name,
               ns_ap_pfv platform_version,
               ns_st_id, --added this 2016-01-07 so we can split linear channel events, as viewing of a new channel usually uses a different streamID
               ns_vid, --added 2016-07-26 for reconciliation work with activity capture
               sg_vs_sc stream_context,
               ns_st_st station_name,
               case when sg_vs_sc = 'lin' then ns_st_ci else null end channel_id,
               case when sg_vs_sc like '%vod' AND ns_st_ad = 0 then ns_st_ci else null end vod_asset_id,
               case when ns_st_ad = 1 then ns_st_ci else null end ad_asset_id,
               e.prev_ns_st_ev, e.ns_st_ev,
               e.continuing_content_from, e.content_continuing_to, e.end_content,
               erroneous_data_suspected_flag,
               new_event_series, end_event_series,
               keep_alive_cap_flag,
               e.next_ns_st_pt,
               e.fake_ns_st_pt,
               case when e.content_continuing_to = 1 and e.ns_st_pt = 0 then fake_ns_st_pt      --don't really need this as we work out the time to midnight anyway
                    else e.ns_st_pt
                end ns_st_pt,
               ns_ts_raw,
               ns_ts,
               daylight_savings_flag_start,
               daylight_savings_flag_end,
               viewing_start_client_utc_raw,
               viewing_end_client_utc_raw,
               viewing_start_client_utc,
               viewing_end_client_utc,
               viewing_start_client_local,
               viewing_end_client_local,
               server_start_utc,
               server_end_utc,
               server_start_utc_raw,
               server_end_utc_raw,
               server_start_local_time,
               server_end_local_time,
               ns_radio_start,
               ns_radio_end,
               --altered to use uniqid to order
               lag(daylight_savings_flag_start) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_daylight_savings_flag_start,
               lead(daylight_savings_flag_end) over(partition by sam_profileid, ns_ap_device order by uniqid) next_daylight_savings_flag_end,
               lag(viewing_start_client_utc) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_viewing_start_client_utc,
               lead(viewing_end_client_utc) over(partition by sam_profileid, ns_ap_device order by uniqid) next_viewing_end_client_utc,
               lag(viewing_start_client_utc_raw) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_viewing_start_client_utc_raw,
               lead(viewing_end_client_utc_raw) over(partition by sam_profileid, ns_ap_device order by uniqid) next_viewing_end_client_utc_raw,
               lag(viewing_start_client_local) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_viewing_start_client_local,
               lead(viewing_end_client_local) over(partition by sam_profileid, ns_ap_device order by uniqid) next_viewing_end_client_local,
               lag(server_start_utc) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_server_start_utc,
               lead(server_end_utc) over(partition by sam_profileid, ns_ap_device order by uniqid) next_server_end_utc,
               lag(server_start_utc_raw) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_server_start_utc_raw,
               lead(server_end_utc_raw) over(partition by sam_profileid, ns_ap_device order by uniqid) next_server_end_utc_raw,
               lag(server_start_local_time) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_server_start_local_time,
               lead(server_end_local_time) over(partition by sam_profileid, ns_ap_device order by uniqid) next_server_end_local_time,
               lag(ns_radio_start) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_ns_radio_start,
               lead(ns_radio_end) over(partition by sam_profileid, ns_ap_device order by uniqid) next_ns_radio_end,
               ns_st_ge genre,
               ns_st_ad ad_flag,
               null unique_duration,
               ns_st_ec, --event counter for playlist
               start_uniqid,
               end_uniqid,
               lag(start_uniqid) over(partition by sam_profileid, ns_ap_device order by uniqid) prev_uniqid_start,
               lead(end_uniqid) over(partition by sam_profileid, ns_ap_device order by uniqid) next_uniqid_end,
               case when sg_vs_sc = 'lin' then null              -- we don't know the linear content length yet
                    when coalesce(ns_st_el, 0) > 0  AND  e.ns_st_ev != 'end'  then ns_st_el -- episode length
                    when ns_st_cl > 0  AND  e.ns_st_ev != 'end'  then ns_st_cl              -- clip clength (there are often multiple clips in an episode, but it's the best 2nd choice)
                    else null
                end content_duration,                            -- episode more reliable for length of total clips (sometimes end events dont hold correct info
               --play_events,
               case when new_event_series = 1 or continuing_content_from = 1 and ns_st_pr is not null then ns_st_pr
                    when end_event_series = 1 or content_continuing_to = 1 and sg_vs_sc like '%vod' then ns_st_pr   --  <--- careful with this, just incase it introduces mis-reads of the data  2016-07-25
                    else null
                end programme_name, --this is required for linear where the start and end programme could be different
               case when new_event_series = 1 or content_continuing_to = 1 then 0
                    else ns_st_bt
                end buffering_duration
          into barbera.V239_comscore_view2
          from barbera.V239_comscore_event_tmp e
         where new_event_series = 1
            or end_event_series = 1
            or continuing_content_from = 1
            or content_continuing_to = 1
        order by sam_profileid,
                ns_ap_device,
                ns_ts,
                ns_st_ec
        commit



        /* code_location_D02 *************************************************************************
         *****                                                                                      **
         *****    Compress the prepared start/end event rows into a single 'aggregated' row         **
         *****    Apply rules for allocating the event's play duration                              **
         *********************************************************************************************/

        IF object_id('barbera.V239_comscore_view') IS NOT NULL
            BEGIN
                drop table barbera.V239_comscore_view
            END

        --create 'final' table view
        select e.uniqid,
               e.account_number,
               e.cb_key_household,
               e.sam_profileid,
               e.aggregate_flag,
               e.ns_ap_device,
               e.platform_name,
               e.platform_version,
               e.stream_context,

               --e.station_name, [fix 'unknown' station name  <-- applied 2015-11-06]  further adjustment made partitioning by ns_st_id applied 2016-01-07
               lag(e.station_name) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) first_station_name,
               lead(e.station_name) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) second_station_name,
               case when e.station_name = 'unknown' AND e.station_name != first_station_name then first_station_name
                    when e.station_name = 'unknown' AND e.station_name != second_station_name then second_station_name
                    else e.station_name
                end station_name,

               --e.channel_id, [fix 'unknown' channel_id   <-- applied 2015-11-06]  further adjustment made partitioning by ns_st_id applied 2016-01-07
               lag(e.channel_id) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) first_channel_id,
               lead(e.channel_id) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) second_channel_id,  -- not being split properly - reading lines from entries we shouldn't be
               case when e.channel_id = 'unknown' AND e.channel_id != first_channel_id then first_channel_id
                    when e.channel_id = 'unknown' AND e.channel_id != second_channel_id then second_channel_id
                    else e.channel_id
                end channel_id,

               e.vod_asset_id, e.ad_asset_id,
               e.ns_st_id,
               e.ns_vid,
               e.prev_ns_st_ev, e.ns_st_ev,
               e.continuing_content_from, e.content_continuing_to,
               e.erroneous_data_suspected_flag,
               e.keep_alive_cap_flag,
               e.new_event_series, e.end_content, e.next_ns_st_pt, e.ns_st_pt,
               coalesce(e.viewing_start_client_utc, e.prev_viewing_start_client_utc) viewing_event_start_utc,
               coalesce(e.viewing_end_client_utc, e.next_viewing_end_client_utc) viewing_event_end_utc,
               coalesce(e.viewing_start_client_utc_raw, e.prev_viewing_start_client_utc_raw) viewing_event_start_client_utc_raw,
               coalesce(e.viewing_end_client_utc_raw, e.next_viewing_end_client_utc_raw) viewing_event_end_client_utc_raw,
               coalesce(e.viewing_start_client_local, e.prev_viewing_start_client_local) viewing_event_start_client_local,
               coalesce(e.viewing_end_client_local, e.next_viewing_end_client_local) viewing_event_end_client_local,
               coalesce(e.daylight_savings_flag_start, e.prev_daylight_savings_flag_start) daylight_savings_flag_start,
               coalesce(e.daylight_savings_flag_end, e.next_daylight_savings_flag_end) daylight_savings_flag_end,
               coalesce(e.server_start_utc, e.prev_server_start_utc) server_start_utc,                          --  Comscore utc
               coalesce(e.server_end_utc, e.next_server_end_utc) server_end_utc,                                --  Comscore utc
               coalesce(e.server_start_utc_raw, e.prev_server_start_utc_raw) server_start_utc_raw,              --  Comscore utc RAW (milliseconds)
               coalesce(e.server_end_utc_raw, e.next_server_end_utc_raw) server_end_utc_raw,                    --  Comscore utc RAW (milliseconds)
               coalesce(e.server_start_local_time, e.prev_server_start_local_time) server_start_local_time,     --  Comscore local-time
               coalesce(e.server_end_local_time, e.next_server_end_local_time) server_end_local_time,           --  Comscore local-time
               coalesce(e.ns_radio_start, e.prev_ns_radio_start) connection_type_start,                         --  connection_type
               coalesce(e.ns_radio_end, e.next_ns_radio_end) connection_type_end,                               --  connection_type
               coalesce(e.start_uniqid, e.prev_uniqid_start) uniqid_start,                                --  event counter using uniqid
               coalesce(e.end_uniqid, e.next_uniqid_end) uniqid_end,                                      --  event counter using uniqid

               --e.genre, [fix 'unknown' channel_id   <-- applied 2015-11-06]  further adjustment made partitioning by ns_st_id applied 2016-01-07
               lag(e.genre) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) first_genre,
               lead(e.genre) over(partition by sam_profileid, ns_ap_device, ns_st_id order by uniqid) second_genre,
               case when e.genre = 'unknown' AND e.genre != first_genre then first_genre
                    when e.genre = 'unknown' AND e.genre != second_genre then second_genre
                    else e.genre
                end genre,

               ad_flag,
               unique_duration,
               datediff(ss, viewing_event_start_utc, viewing_event_end_utc) event_duration,                           --  better to convert this to use the raw start/end
               floor((viewing_event_end_client_utc_raw - viewing_event_start_client_utc_raw)/1000) event_sec_precise, --  <-- I think use this for event_secs
               content_duration,
               programme_name, buffering_duration,
                --need to work out the duration of viewing before midnight if event_continuing
               case when continuing_content_from = 1 AND end_event_series = 1 then datediff(ss, viewing_event_start_utc, ns_ts)  --  period of content after midnight (to subtract from accumulated clip playing time)
                    when continuing_content_from = 1 then -(ns_st_pt - datediff(ss, viewing_event_start_utc, ns_ts))  --  period of content *before* midnight (to subtract from accumulated clip playing time)
                    when content_continuing_to = 1 then  datediff(ss, viewing_event_start_utc, viewing_event_end_utc) --  period *upto* midnight to use as accumulated clip playing time
                    
                    --for linear stream (where ns_st_pt doesnt really work)
                    when stream_context = 'lin' AND end_event_series = 1 then datediff(ss, viewing_event_start_utc, viewing_event_end_utc)

                    --for VOD (where we can use ns_st_pt)
                    when new_event_series = 1 then 0                                                                  --  don't use any surplus accumulated play-times at the start of the series
                    when end_event_series = 1 AND e.ns_st_pt = 0 then fake_ns_st_pt
                    when end_content = 1 AND e.ns_st_pt = 0 AND prev_ns_st_ev = 'pause' AND ns_st_ev = 'play' then fake_ns_st_pt
                    when end_content = 1 AND e.ns_st_pt = 0 then next_ns_st_pt                                        --  use fix if playtime is attributed to the next event incorrectly
                    else ns_st_pt
                end play_duration
          into barbera.V239_comscore_view
          from barbera.V239_comscore_view2 e
         --where not(coalesce(prev_ns_st_ev, 'play') = 'end' and ns_st_ev = 'end') --fix for taking out a whole load of continuous end events...
         order by sam_profileid,
                  ns_ap_device,
                  ns_ts,
                  ns_st_ec
        commit



        /* code_location_E01 *************************************************************************
         *****                                                                                      **
         *****     Insert the aggregated events into the monthly table. If the start                **
         *****     of a new month, create a new table.                                              **
         *****                                                                                      **
         *****     1. Prepare executable/dynamic SQL statement                                      **
         *****                                                                                      **
         *********************************************************************************************/
/*commit
declare @monthly_table_name varchar(300)
declare @xsql varchar(10000)
declare @data_run_date       DATE
SET @data_run_date = '2015-08-05'
*/

        SET @monthly_table_name = 'barbera.VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')

        EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Using ['||@monthly_table_name||']'


        SET @xsql = ' ###insert_into_table###
                   select 
                   account_number               as account_number,
                   cb_key_household             as cb_key_household,
                   sam_profileid                as sam_profileid,
                   ns_ap_device                 as ns_ap_device,
                   platform_name                as platform_name,
                   platform_version             as platform_version,
                   max(stream_context)          as stream_context,
                   max(station_name)            as station_name,
                   max(channel_id)              as channel_id,
                   null                         as service_key,
                   max(vod_asset_id)            as vod_asset_id,
                   max(ad_asset_id)             as ad_asset_id,
                   cast(min(ns_st_id)  as varchar(24))  as stream_id,                           --added 2016-07-26 for reconciliation work with Activity Capture
                   cast(min(ns_vid) as varchar(48))     as session_id,                          --added 2016-07-26 for reconciliation work with Activity Capture
                   cast(null as bigint)         as dk_programme_instance_dim,
                   cast(null as datetime)       as broadcast_start_date_time_utc,
                   cast(null as datetime)       as broadcast_end_date_time_utc,
                   cast(null as datetime)       as broadcast_start_date_time_local,
                   cast(null as datetime)       as broadcast_end_date_time_local,

                   -- the client side viewing event times require adjusting if the
                   -- duration spans more than one day to use the server-side timestamps
                   max(viewing_event_start_utc)             as programme_instance_start_utc,
                   max(viewing_event_end_utc)               as programme_instance_end_utc,
                   max(viewing_event_start_client_local)    as programme_instance_start_local,
                   max(viewing_event_end_client_local)      as programme_instance_end_local,
                   viewing_event_start_utc,
                   viewing_event_end_utc,
                   viewing_event_start_client_utc_raw   as viewing_event_start_utc_raw,
                   viewing_event_end_client_utc_raw     as viewing_event_end_utc_raw,
                   viewing_event_start_client_local     as viewing_event_start_local,
                   viewing_event_end_client_local       as viewing_event_end_local,
                   max(daylight_savings_flag_start)     as daylight_savings_start_flag,
                   max(daylight_savings_flag_end)       as daylight_savings_end_flag,
                   max(server_start_utc)                as server_event_start_utc,
                   max(server_end_utc)                  as server_event_end_utc,
                   max(server_start_utc_raw)            as server_event_start_utc_raw,
                   max(server_end_utc_raw)              as server_event_end_utc_raw,
                   max(server_start_local_time)         as server_start_local_time,
                   max(server_end_local_time)           as server_end_local_time,
                   max(connection_type_start)           as connection_type_start,
                   max(connection_type_end)             as connection_type_end,
                   max(genre)                           as genre_description,
                   cast(null as varchar(20))            as sub_genre_description,
                   max(ad_flag)                         as ad_flag,
                   min(cast((dateformat(cast(''###data_run_date###'' as date), ''YYYYMMDD'')||uniqid) as bigint)) aggr_event_id, --unique identifier for the event
                   (max(uniqid_end) - min(uniqid_start))+1 as event_count, -- +1 as inclusive
                   max(erroneous_data_suspected_flag)   as erroneous_data_suspected_flag,
                   max(continuing_content_from)         as view_continuing_flag,
                   max(content_continuing_to)           as view_continues_next_day_flag,
                   0                                    as linear_instance_flag, --as not split by programme yet
                   max(content_duration)                as content_duration, -- <--- picks up wrong duration sometimes
                   max(programme_name)                  as programme_name,    --  <--- preroll descriptors should be stripped from programme name before this point
                   case
                        --require fix for durations that have been affected by client-side clock change making the duration really long..
                        when sum(play_duration) > content_duration AND event_count <= 2 then content_duration
                        else sum(play_duration)
                    end                                 as duration_viewed, -- alternative with capping applied if only two events are defining aggregate, ie play/end
                   case when coalesce(content_duration, 0) = 0 then cast(null as decimal(5,3))
                        else cast(round((duration_viewed*1.0/content_duration*1.0),2) as decimal(5,3))
                    end                                 as percentage_viewed,
                   cast(''###data_run_date###'' as date) data_date_local,
                   today() load_date
            ###into_table###             --into VESPA_Comscore_SkyGo_YYYYMM       
             from barbera.V239_comscore_view            
            group by account_number, cb_key_household, sam_profileid, ns_ap_device, platform_name, platform_version,
                     viewing_event_start_utc, viewing_event_end_utc, viewing_event_start_utc_raw, viewing_event_end_utc_raw,
                     viewing_event_start_local, viewing_event_end_local
            order by account_number, cb_key_household, sam_profileid, ns_ap_device, platform_name, platform_version, viewing_event_start_utc
            commit


          --update the audit_run table with success...
            UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
               SET basic_view_created = 1
             where data_date = dateformat(cast(''###data_run_date###'' as date), ''YYYY-MM-DD'')
            commit
            '



        /* code_location_E02 *************************************************************************
         *****                                                                                      **
         *****     2. Execute the dynamic SQL statement                                             **
         *****                                                                                      **
         *********************************************************************************************/

        IF object_id(@monthly_table_name) IS NOT NULL
            BEGIN
                --delete any previous runs of the same load if they exist..
                execute('delete '||@monthly_table_name||' where data_date_local = cast('''||@data_run_date||''' as date)')

                execute (
                    replace(
                      replace(
                        replace(@xsql, '###insert_into_table###', 'INSERT into '||@monthly_table_name),
                                       '###into_table###', ''),
                                       '###data_run_date###', @data_run_date)
                )
                commit
                EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Inserting into existing view<'||@monthly_table_name||'>'
                commit
            END
         ELSE
            BEGIN
                execute (
                  replace(
                    replace(
                        replace(@xsql, '###insert_into_table###', ''),
                                       '###into_table###', 'into '||@monthly_table_name),
                                       '###data_run_date###', @data_run_date)
                )
                commit

                --add indexes to new table
                execute('
                    create index '||@monthly_table_name||'_aggr_event_id_idx on '||@monthly_table_name||'(aggr_event_id)
                    --more indexes added
                    create index '||@monthly_table_name||'_data_date_local_idx on '||@monthly_table_name||'(data_date_local)
                    create index '||@monthly_table_name||'_sam_profileid_idx on '||@monthly_table_name||'(sam_profileid)
                    create index '||@monthly_table_name||'_station_name_idx on '||@monthly_table_name||'(station_name)
                    create DTTM index '||@monthly_table_name||'_server_startutc_idx on '||@monthly_table_name||'(server_event_start_utc)
                    create DTTM index '||@monthly_table_name||'_server_endutc_idx on '||@monthly_table_name||'(server_event_end_utc)
                    ')

                EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Creating new view<'||@monthly_table_name||'>'
                commit
            END

        execute('grant select on '||@monthly_table_name||' to vespa_group_low_security')

        execute('grant select on '||@monthly_table_name||' to public')  --temporary

        EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed build of basic view'



        /* code_location_F01 *************************************************************************
         *****                                                                                      **
         *****       Update basic stats... links back with file/raw data stats                      **
         *****                                                                                      **
         *********************************************************************************************/

            SET @xsql = '
                        SELECT sum(case when stream_context = ''vod'' then 1 else 0 end)  aggr_vod_events,
                               sum(case when stream_context = ''dvod'' then 1 else 0 end) aggr_dvod_events,
                               sum(case when stream_context = ''lin'' then 1 else 0 end)  aggr_lin_events
                          into barbera.Comscore_skygo_day_stats_tmp
                          from '||@monthly_table_name||' a
                         where a.data_date_local = '''||@data_run_date||'''
                         group by a.data_date_local

                        UPDATE barbera.VESPA_Comscore_SkyGo_audit_stats_tbl s
                           SET s.aggr_vod_events  = a.aggr_vod_events,
                               s.aggr_dvod_events = a.aggr_dvod_events,
                               s.aggr_lin_events  = a.aggr_lin_events
                          from barbera.Comscore_skygo_day_stats_tmp a
                         WHERE s.data_date = '''||@data_run_date||'''

                          drop table barbera.Comscore_skygo_day_stats_tmp

                         commit
                        '
            EXECUTE(@xsql)


END;




--------------------------------------------------------------------------------
--#####################     END     ############################################
--##############################################################################
--##############################################################################
--##############################################################################




