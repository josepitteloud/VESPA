/*
###############################################################################
# Created on:   06/08/2014
# Created from COMSCORE:   Alan Barber (ABA)
# Created in Olive : Mohammed Rafi
# Description:  Construct table required for the final output
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - none
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2014  ABA   Initial version
# 11/11/2014  ABA   Added run_datetime to ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
#                   so that proc can be run and logged multiple times on the same data date
# 22/12/2014  ABA   Added ability to run when data volume is low -  IF @status not in ('ok','low') then raise error
# 22/12/2014  ABA   Wrapped the remained of script inside BEGIN/END statement for "IF @status not in ('ok','low')" so not run if failure

###############################################################################*/

/* code_location_A01 *************************************************************************
 *****                                                                                      **
 *****            Create account information table                                             **
 *********************************************************************************************/


CREATE or REPLACE procedure ${CBAF_DB_LIVE_SCHEMA}.vespa_comscore_skygo_event_view_create(
                IN @data_run_date                 DATE,
                IN @suppress_universe_build       BIT,
                IN @lower_limit                   INTEGER,
                IN @stddev_over_rows              INTEGER,
                IN @weighting_rows                INTEGER) AS
BEGIN

    commit
    DECLARE @status             varchar(24)
    DECLARE @xsql               varchar(10000)
    DECLARE @monthly_table_name varchar(300)



    /* code_location_A01 ***********************************************************
     *****                                                                        **
     *****     First prepare the information about this load and identify         **
     *****     if ok to load                                                      **
     *****                                                                        **
     *******************************************************************************/

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log 'Build Start <'||@data_run_date||'>'


    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Preparing stats'


----------------------
/*
commit
    DECLARE @data_run_date date
    DECLARE @suppress_stats_universe_build bit
    DECLARE @lower_limit integer
    DECLARE @stddev_over_rows integer
    DECLARE @weighting_rows integer

    --set the date we are running this for
    SET @data_run_date = '2014-11-07'
    SET @suppress_stats_universe_build = 0

    --set variables for the monitoring stats (calculating expected data volumes from the underlaying data)
    SET @lower_limit = 200000 -- floor at which an alert is triggered regardless of other stats (number of expected accounts)
    SET @stddev_over_rows = 7 -- how many days the stddev of accounts is calculated over
    SET @weighting_rows = 4   -- how many days are used to exponentially weight the moving average

*/

    EXEC ${CBAF_DB_LIVE_SCHEMA}.vespa_comscore_skygo_event_view_prep @data_run_date, @lower_limit, @stddev_over_rows, @weighting_rows

    if @@error = 18001 BEGIN return raiserror 18001 'No File Supplied' END


    /* code_location_A02 *************************************************************************
     *****                                                                                      **
     *****      Prepare Universe information                                                    **
     *****                                                                                      **
     *********************************************************************************************/
    IF @suppress_universe_build != 1
        BEGIN
            EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Creating universe information'

            IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_universe_tbl') IS NOT NULL
                BEGIN
                    drop table ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_universe_tbl
                END
            select --dense_rank() over(order by c.sam_profileid asc) row_id,
                   c.sam_profileid,
                   sav.account_number,
                   sav.cb_key_household,
                   coalesce(sav.exclude_flag, 0) exclude_flag,       --default to 0 (don't exclude)
                   coalesce(sav.aggregate_flag, 0) aggregate_flag,     --default to 0 (don't aggregate)
                   count(distinct ns_ap_device) device_types_in_yr,
                   count(distinct case when sg_vs_sc = 'lin' then ns_st_ci else null end)   unique_lin_channels_in_yr,
                   count(distinct case when sg_vs_sc = 'vod' then ns_st_ci else null end)   unique_vod_assets_in_yr,
                   count(distinct case when sg_vs_sc = 'dvod' then ns_st_ci else null end)  unique_dvod_assets_in_yr,
                   max(cast(ns_utc as date)) last_received_date,
                   dateformat(min(ns_ts), 'YYYY-MM-DD HH:MM:SS') first_event_utc,  --local client time-stamp
                   dateformat(max(ns_ts), 'YYYY-MM-DD HH:MM:SS') latest_event_utc, --local client time-stamp
                   dateformat(min(ns_utc), 'YYYY-MM-DD HH:MM:SS') first_data_utc,  --timestamp recived at the Comscore server
                   dateformat(max(ns_utc), 'YYYY-MM-DD HH:MM:SS') latest_data_utc, --timestamp recived at the Comscore server
                   cast(max(case when sg_vs_sc = 'lin' then ns_utc else null end) as date)  latest_lin_data_utc,
                   cast(max(case when sg_vs_sc = 'vod' then ns_utc else null end) as date)  latest_vod_data_utc,
                   cast(max(case when sg_vs_sc = 'dvod' then ns_utc else null end) as date)  latest_dvod_data_utc,
                   max(cb_data_date) last_load_date,
                   cast(max(ns_utc) as date) event_last_load_date,
                   cast(dateadd(dd, -1, today()) as date) last_expected_event_date,
                   datediff(dd, event_last_load_date, last_expected_event_date) days_since_last_data,
                   count( distinct case when cast(ns_utc as date) > dateadd(dd, -7, last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_week,
                   count( distinct case when cast(ns_utc as date) > dateadd(mm, -1, last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_month,
                   count( distinct case when cast(ns_utc as date) > dateadd(qq, -1, last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_quarter,
                   count( distinct case when cast(ns_utc as date) > dateadd(yy, -1, last_expected_event_date) then cast(ns_utc as date) else null end) days_active_rolling_year
              into ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_universe_tbl
              from ${CBAF_DB_LIVE_SCHEMA}.COMSCORE_UNION_VIEW c --using the union view
                   left join ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_summary_tbl sav --combine with SAV
                     on c.sam_profileid = sav.sam_profileid
             group by c.sam_profileid, sav.account_number, sav.cb_key_household, sav.exclude_flag, sav.aggregate_flag

            commit

            create index VESPA_Comscore_universe_sam_idx on ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_universe_tbl(sam_profileid)
            commit
        END
     ELSE
        BEGIN
            EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Suppressed Universe Build'
        END -- end @supress_stats_universe_build if = 1



    /* code_location_B01 *************************************************************************
     *****                                                                                      **
     *****      Halt the build process and raise error if the raw data looks wrong                                                                                **
     *****                                                                                      **
     *********************************************************************************************/

    --check/QA to see if we can run the process for the required day
    SELECT @status = QA_status
      from ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl r
    where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
    
    IF @status not in ('ok','low')
        BEGIN
            -- set processed information to flag issue
            UPDATE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl
               SET basic_view_created = -1
             where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

            EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Process failed['||@status||']'

            RAISERROR 50001 'data volume is '||@status
        END
    ELSE
        BEGIN
            --reset any previous attempts
            UPDATE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl
               SET basic_view_created = 0
             where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')



    /* code_location_C01 *************************************************************************
     *****                                                                                      **
     *****      Start the conversion of raw data into event aggregate.                          **
     *****      First - extract the local day's period keys from ${CBAF_DB_LIVE_SCHEMA}.VESPA_CALENDAR                 **
     *****                                                                                      **
     *********************************************************************************************/

--declare @data_run_date       DATE
--set @data_run_date = '2014-08-08'

    IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_CALENDAR_section_tmp') IS NOT NULL  BEGIN drop table ${CBAF_DB_DATA_SCHEMA}.VESPA_CALENDAR_section_tmp END

    select c.utc_day_date, c.utc_time_hours,
           c.local_day_date, c.local_time_hours,
           c.daylight_savings_flag
      into ${CBAF_DB_DATA_SCHEMA}.VESPA_CALENDAR_section_tmp -- create a tempory table just with the hours we are interested in for this load, so that the inner joins on these only
      FROM ${CBAF_DB_LIVE_SCHEMA}.VESPA_CALENDAR c
     where local_day_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
     order by c.utc_day_date, c.utc_time_hours


    /* code_location_C02 *************************************************************************
     *****                                                                                      **
     *****      extract the required fields from the day's data (from ${CBAF_DB_LIVE_SCHEMA}.COMSCORE_UNION_VIEW)      **
     *****      handling DQ error codes with conversions                                        **
     *****      Add a unique ID to each row, which will also order events                       **
     *****                                                                                      **
     *********************************************************************************************/

    --Create table for the day. Extract by server-side local-time. Cut-down dataset to required fields
     --drop tmp table before we start if exists
    IF object_id('${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp4') IS NOT NULL
        BEGIN
            DROP TABLE ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp4
        END

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Extracting raw Comscore data'

    select dense_rank() over(order by coalesce(cast(t.sam_profileid as varchar), t.ns_st_id), ns_ap_device, ns_ts_raw, ns_st_ec asc, ns_utc_raw, cb_row_id asc) uniqid,
           t.cb_row_id,
           ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
           ns_radio,
           agent,
           ns_st_ad,
           ns_st_bp, ns_st_bt,
           ns_st_ca,
           case when ns_st_cl >= 0 then ns_st_cl else null end ns_st_cl,  --handling DQ error codes
           ns_st_cn, ns_st_ec,
           case when ns_st_el >= 0 then ns_st_el else null end ns_st_el,  --handling DQ error codes
           ns_st_ep,
           ns_st_ge, ns_st_hc, ns_st_id,
           ns_st_it, ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe,
           ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr,
           ns_st_sp, ns_st_sq, ns_st_st, ns_st_tp, ns_st_ty,
           ns_st_ci,
           case when sg_vs_sc in ('vod','lin','dvod') then sg_vs_sc else 'unknown' end sg_vs_sc,
           t.sam_profileid,
           ns_ap_device,
           ns_utc,
           ns_utc_raw,
           dateadd(hh, c.daylight_savings_flag, ns_utc) ns_server_local_time,
           time_of_day, --no longer required as calculating datetime version above
           ns_ts_raw,
           ns_ts,
           ns_st_ev,
           case when ns_st_pt_raw >= 0 then ns_st_pt_raw else null end ns_st_pt_raw, --handling DQ error codes
           case when ns_st_pt >= 0     then ns_st_pt     else null end ns_st_pt  --handling DQ error codes
      into ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp4
      from ${CBAF_DB_LIVE_SCHEMA}.COMSCORE_UNION_VIEW t
             INNER JOIN ${CBAF_DB_DATA_SCHEMA}.VESPA_CALENDAR_section_tmp c -- using just with the hours we are interested in for this load, so that the inner joins is efficient
            on cast(dateformat(t.ns_utc,'YYYY-MM-DD') as date) = c.utc_day_date
           and cast(dateformat(t.ns_utc,'hh') as Integer)      = c.utc_time_hours
     where --dateformat(ns_server_local_time, 'YYYY-MM-DD') = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
           t.sam_profileid is not null --later we need to include these, but the data is not good enough yet

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
    IF object_id('${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp3') IS NOT NULL
        BEGIN
            DROP TABLE ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp3
        END

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Applying universe restriction, and preparing view'

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
           ns_st_bp, ns_st_bt,
           ns_st_ca, ns_st_cl, ns_st_cn, ns_st_ec,
           ns_st_el, ns_st_ep,
           ns_st_ge, ns_st_hc, ns_st_id,
           ns_st_it, ns_st_li, ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe,
           ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr,
           ns_st_sp, ns_st_sq, ns_st_st, ns_st_tp, ns_st_ty,
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
           t.sam_profileid, --leave just for testing - use above line in final code
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
                when prev_ns_st_ci != ns_st_ci then 1
                when coalesce(prev_ns_st_ad, -1) != coalesce(ns_st_ad, -1) then 1
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
                when sg_vs_sc != 'lin' AND prev_ns_st_ev = 'play' AND ns_st_ev = 'play' AND (ns_ts_raw - prev_ns_ts_raw) > 1200000 then 1 -- and diff>20mins
                when new_content = 1 then 1
                --when prev_ns_radio not in ('none','unknown') AND ns_radio not in ('none','unknown') AND prev_ns_radio != ns_radio then 1   --connection_type has changed, so we need to split the event - NOT POSSIBLE AT THE MOMENT using ns_st_pt
                when sg_vs_sc = 'lin' AND prev_ns_st_ev = 'pause' AND ns_st_ev in ('play','end') then 1 --having 'end' in this list forces 'pause','end','null' sequences to be a single row event starting and ending with the one entry, these are then removed (as event is a none event)
                else 0
            end new_event_series, --this is better at recognising events
            --play event
           case when ns_st_ev = 'play'  then 1
                when ns_st_ev = 'keep-alive'  then 1
                else 0 end play_event,
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
       into ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp3
       from ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp4 t
             INNER JOIN ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_universe_tbl s -- the universe table (limit using row_id in batches for large volumes of data)
            on t.sam_profileid = s.sam_profileid
      where s.exclude_flag != 1
      order by t.sam_profileid, ns_ap_device, ns_ts,/* ns_st_ci,*/ ns_st_ec asc

     commit


    /* code_location_C04 *************************************************************************
     *****      Identify the context of the Orphaned events, and events that signify            **
     *****      the end of content or event series. Also apply fixes to the assetID and         **
     *****      accumulated playing time (ns_st_pt) if required                                 **
     *********************************************************************************************/

    IF object_id('${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp2') IS NOT NULL
        BEGIN
            drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp2
        END

    SELECT uniqid, cb_row_id,
           aggregate_flag,
           ns_ap_ec, ns_ap_pfm, ns_ap_pfv,
           ns_radio, agent, prev_ns_st_ad, ns_st_ad, next_ns_st_ad,
           ns_st_bp, ns_st_bt, ns_st_ca, ns_st_cl, ns_st_cn, ns_st_ec,
           ns_st_el, ns_st_ep, ns_st_ge, ns_st_hc, ns_st_id, ns_st_it,
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
           case when prev_orphaned_end = 1 then 1 --this is so when an orphaned end event is removed, we treat the following row as a new event series
                else e.new_event_series
            end new_event_series,
           new_event_series as old_new_event_series,
           play_event, play_duration, pause_event, pause_duration,
           end_ev,
           row, group_row, event_count,
           --e.new_content,
           case --when ns_st_pt = 0 AND prev_ns_st_ev = 'play' AND ns_st_ev = 'play' then
                when ns_st_pt = 0 then prev_ns_st_pt
                else ns_st_pt
            end fake_ns_st_pt,--used in cases where it's the last event for the account on the day (pulls forward the accumulated play time)
           --replace asset_id when it's just changed but is an end event
           case when e.prev_ns_st_ci != e.ns_st_ci AND e.ns_st_ev = 'end' and e.next_ns_st_ev is null then e.prev_ns_st_ci
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
                when next_orphaned_end = 1 then 1 --- required as the next step removes orphaned events leaving end_event_series as 0 (no ending for the event)
                else lead(e.new_event_series) over(partition by sam_profileid, ns_ap_device order by uniqid)
            end end_event_series
       into ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp2
       from ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp3 e
      order by uniqid
    --drop table V239_comscore_event_tmp2
    commit
  


    /* code_location_C05 *************************************************************************
     *****    Apply midnight cross-over rules to extend viewing to/from midnight                **
     *****    add local-time fields. Remove orphaned-end events                                 **
     *****                                                                                      **
     *********************************************************************************************/

    IF object_id('${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp') IS NOT NULL
        BEGIN
            drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp
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
         ns_st_el, ns_st_ep, ns_st_ge, ns_st_hc, ns_st_id, ns_st_it, ns_st_li,
         ns_st_pa, ns_st_pb, ns_st_pc, ns_st_pe, ns_st_pl, ns_st_pn, ns_st_po, ns_st_pp, ns_st_pr, ns_st_sp, ns_st_sq,
         ns_st_st, ns_st_tp, ns_st_ty,
         prev_ns_st_ci, next_ns_st_ci,
         sg_vs_sc,
         account_number, cb_key_household, sam_profileid, ns_ap_device,
         --ns_ts is based on utc at the client. This means that we need to adjust the cut-off according to daylight savings (summer is -1hr from midnight, winter is midnight)
         case when new_event_series = 1 then ns_ts
              when continuing_content_from = 1 then DATEFLOOR(hh, ns_ts)
              else null
          end viewing_start_client_utc,
         case when content_continuing_to = 1 then DATECEILING(hh, ns_ts)
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
         ns_ts_raw - truncnum(ns_ts_raw,-3) client_millis,
         ns_utc_raw - truncnum(ns_utc_raw,-3) server_millis,
         datediff(ms, viewing_start_client_utc, ns_ts) start_day_ts_diff,
         datediff(ms, ns_ts, viewing_end_client_utc) end_day_ts_diff,
         datediff(ms, viewing_start_client_utc, ns_utc) start_day_utc_diff,
         datediff(ms, ns_utc, viewing_end_client_utc) end_day_utc_diff,
         case when viewing_start_client_utc is null then null
              when start_day_ts_diff > 0 then ns_ts_raw - start_day_ts_diff - client_millis
              else ns_ts_raw
          end viewing_start_client_utc_raw,
         case when viewing_end_client_utc is null then null
              when end_day_ts_diff > 0 then ns_ts_raw + end_day_ts_diff - client_millis
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
              when start_day_utc_diff > 0 then ns_utc_raw - start_day_utc_diff - server_millis
              else ns_utc_raw
          end server_start_utc_raw,
         case when viewing_end_client_utc is null then null
              when end_day_utc_diff > 0 then ns_utc_raw + end_day_utc_diff - server_millis
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
              else 0
          end erroneous_data_suspected_flag,
         --fix any missing start events if the end has been recognised but not the next start
         --new_event_series,
         case when lag(e.end_event_series) over(partition by sam_profileid, ns_ap_device order by uniqid) = 1 AND new_event_series = 0 then 1
              else new_event_series
          end new_event_series,
         play_event, play_duration, pause_event, pause_duration, end_ev,
         row, group_row,
         event_count,
         fake_ns_st_pt,
         ns_st_ci,
         end_content,
         end_event_series
    into ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp
    from ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp2 e,
         ${CBAF_DB_LIVE_SCHEMA}.VESPA_CALENDAR c_ts,
         ${CBAF_DB_LIVE_SCHEMA}.VESPA_CALENDAR c_utc,
   where cast(ns_ts as date) = c_ts.utc_day_date
     and cast(dateformat(ns_ts,'hh') as Integer) = c_ts.utc_time_hours
     and cast(ns_utc as date) = c_utc.utc_day_date
     and cast(dateformat(ns_utc,'hh') as Integer) = c_utc.utc_time_hours
     and orphaned_end != 1
    commit


    /* code_location_D01 *************************************************************************
     *****                                                                                      **
     *****      Build an aggregate table using new_event_series = 1, end_event_series = 1,      **
     *****      continuing_content_from = 1, content_continuing_to = 1                          **
     *****      Incorporate additional lead/lag context/transistions for requiring fields       **
     *********************************************************************************************/

    IF object_id('${CBAF_DB_DATA_SCHEMA}.V239_comscore_view2') IS NOT NULL
        BEGIN
            drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_view2
        END

    select uniqid,
           account_number,
           cb_key_household,
           sam_profileid,
           aggregate_flag,
           ns_ap_device,
           ns_ap_pfm platform_name,
           ns_ap_pfv platform_version,
           sg_vs_sc stream_context,
           ns_st_st station_name,
           case when sg_vs_sc = 'lin' then ns_st_ci else null end channel_id,
           case when sg_vs_sc like '%vod' AND ns_st_ad = 0 then ns_st_ci else null end vod_asset_id,
           case when ns_st_ad = 1 then ns_st_ci else null end ad_asset_id,
           e.prev_ns_st_ev, e.ns_st_ev,
           e.continuing_content_from, e.content_continuing_to, e.end_content,
           erroneous_data_suspected_flag,
           new_event_series, end_event_series,
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
                when coalesce(ns_st_el, 0) > 0 then ns_st_el -- episode length
                when ns_st_cl > 0 then ns_st_cl              -- clip clength (there are often multiple clips in an episode, but it's the best 2nd choice)
                else null
            end content_duration,                            -- episode more reliable for length of total clips
           --play_events,
           case when new_event_series = 1 or continuing_content_from = 1 then ns_st_pr
                else null
            end programme_name, --this is required for linear where the start and end programme could be different
           case when new_event_series = 1 or content_continuing_to = 1 then 0
                else ns_st_bt
            end buffering_duration
      into ${CBAF_DB_DATA_SCHEMA}.V239_comscore_view2
      from ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp e
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

    IF object_id('${CBAF_DB_DATA_SCHEMA}.V239_comscore_view') IS NOT NULL
        BEGIN
            drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_view
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
           e.station_name,
           e.channel_id, e.vod_asset_id, e.ad_asset_id,
           e.prev_ns_st_ev, e.ns_st_ev,
           e.continuing_content_from, e.content_continuing_to,
           e.erroneous_data_suspected_flag,
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
           genre, ad_flag,
           unique_duration,
           datediff(ss, viewing_event_start_utc, viewing_event_end_utc) event_duration,                           --  better to convert this to use the raw start/end
           floor((viewing_event_end_client_utc_raw - viewing_event_start_client_utc_raw)/1000) event_sec_precise, --  <-- I think use this for event_secs
           content_duration,
           programme_name, buffering_duration,
            --need to work out the duration of viewing before midnight if event_continuing
           case when continuing_content_from = 1 AND end_event_series = 1 then datediff(ss, viewing_event_start_utc, ns_ts)  --  period of content after midnight (to subtract from accumulated clip playing time)
                when continuing_content_from = 1 then -(ns_st_pt - datediff(ss, viewing_event_start_utc, ns_ts))  --  period of content *before* midnight (to subtract from accumulated clip playing time)
                when content_continuing_to = 1 then  datediff(ss, viewing_event_start_utc, viewing_event_end_utc) --  period *upto* midnight to use as accumulated clip playing time
                when new_event_series = 1 then 0                                                                  --  don't use any surplus accumulated play-times at the start of the series
                when end_event_series = 1 AND e.ns_st_pt = 0 then fake_ns_st_pt
                when end_content = 1 AND e.ns_st_pt = 0 AND prev_ns_st_ev = 'pause' AND ns_st_ev = 'play' then fake_ns_st_pt
                when end_content = 1 AND e.ns_st_pt = 0 then next_ns_st_pt                                        --  use fix if playtime is attributed to the next event incorrectly
                else ns_st_pt
            end play_duration
      into ${CBAF_DB_DATA_SCHEMA}.V239_comscore_view
      from ${CBAF_DB_DATA_SCHEMA}.V239_comscore_view2 e
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
declare @data_run_date       DATE
declare @monthly_table_name varchar(300)
declare @xsql varchar(5000)
SET @data_run_date = '2014-08-08'
*/

    SET @monthly_table_name = '${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_' + dateformat(@data_run_date, 'YYYYMM')
    print 'monthly_table_name = '+@monthly_table_name

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Using ['||@monthly_table_name||']'

    SET @xsql = ' ###insert_into_table###
'
    set @xsql=@xsql+' select account_number, cb_key_household, sam_profileid, ns_ap_device, platform_name, platform_version,    '
    set @xsql=@xsql+'       stream_context, station_name, channel_id, null service_key, '
    set @xsql=@xsql+'       vod_asset_id, ad_asset_id,  '
    set @xsql=@xsql+'       cast(null as bigint) dk_programme_instance_dim,     '
    set @xsql=@xsql+'       cast(null as datetime) broadcast_start_date_time_utc,       '
    set @xsql=@xsql+'       cast(null as datetime) broadcast_end_date_time_utc, '
    set @xsql=@xsql+'       cast(null as datetime) broadcast_start_date_time_local,     '
    set @xsql=@xsql+'       cast(null as datetime) broadcast_end_date_time_local,       '
    set @xsql=@xsql+'       -- the client side viewing event times require adjusting if the     '
    set @xsql=@xsql+'       -- duration spans more than one day to use the server-side timestamps       '
    set @xsql=@xsql+'       viewing_event_start_utc as programme_instance_start_utc,    '
    set @xsql=@xsql+'       viewing_event_end_utc as programme_instance_end_utc,        '
    set @xsql=@xsql+'       viewing_event_start_client_local as programme_instance_start_local, '
    set @xsql=@xsql+'       viewing_event_end_client_local as programme_instance_end_local,     '
    set @xsql=@xsql+'       viewing_event_start_utc,    '
    set @xsql=@xsql+'       viewing_event_end_utc,      '
    set @xsql=@xsql+'       viewing_event_start_client_utc_raw as viewing_event_start_utc_raw,  '
    set @xsql=@xsql+'       viewing_event_end_client_utc_raw as viewing_event_end_utc_raw,      '
    set @xsql=@xsql+'       viewing_event_start_client_local as viewing_event_start_local,      '
    set @xsql=@xsql+'       viewing_event_end_client_local as viewing_event_end_local,  '
    set @xsql=@xsql+'       daylight_savings_flag_start as daylight_savings_start_flag, '
    set @xsql=@xsql+'       daylight_savings_flag_end as daylight_savings_end_flag,     '
    set @xsql=@xsql+'       server_start_utc as server_event_start_utc, '
    set @xsql=@xsql+'       server_end_utc as server_event_end_utc,     '
    set @xsql=@xsql+'       server_start_utc_raw as server_event_start_utc_raw, '
    set @xsql=@xsql+'       server_end_utc_raw as server_event_end_utc_raw,     '
    set @xsql=@xsql+'       server_start_local_time, server_end_local_time,     '
    set @xsql=@xsql+'       connection_type_start, connection_type_end, '
    set @xsql=@xsql+'       genre genre_description,    '
    set @xsql=@xsql+'       cast(null as varchar(20)) sub_genre_description,    '
    set @xsql=@xsql+'       ad_flag,    '
    set @xsql=@xsql+'       max(cast((dateformat(cast(''###data_run_date###'' as date), ''YYYYMMDD'')||uniqid) as bigint)) aggr_event_id, --unique identifier for the event     '
    set @xsql=@xsql+'       (max(uniqid_end) - min(uniqid_start))+1 as event_count, -- +1 as inclusive  '
    set @xsql=@xsql+'       max(erroneous_data_suspected_flag) erroneous_data_suspected_flag,   '
    set @xsql=@xsql+'       max(continuing_content_from) as view_continuing_flag,       '
    set @xsql=@xsql+'       max(content_continuing_to) as view_continues_next_day_flag, '
    set @xsql=@xsql+'       0 linear_instance_flag, --as not split by programme yet     '
    set @xsql=@xsql+'       max(content_duration) content_duration,     '
    set @xsql=@xsql+'       max(programme_name) programme_name, '
    set @xsql=@xsql+'       --sum(play_duration) duration_viewed,  --  <--- question-mark around this   '
    set @xsql=@xsql+'       case        '
    set @xsql=@xsql+'            --require fix for durations that have been affected by client-side clock change making the duration really long..      '
    set @xsql=@xsql+'           '
    set @xsql=@xsql+'            when sum(play_duration) > content_duration AND event_count <= 2 then content_duration  '
    set @xsql=@xsql+'            else sum(play_duration)        '
    set @xsql=@xsql+'        end duration_viewed, -- alternative with capping applied if only two events are defining aggregate, ie play/end    '
    set @xsql=@xsql+'       case when coalesce(content_duration, 0) = 0 then cast(null as decimal(5,3)) '
    set @xsql=@xsql+'            else cast(round((duration_viewed*1.0/content_duration*1.0),2) as decimal(5,3)) '
    set @xsql=@xsql+'       end percentage_viewed,      '
    set @xsql=@xsql+'       cast(''###data_run_date###'' as date) data_date_local,      '
    set @xsql=@xsql+'       today() load_date   '
    set @xsql=@xsql+' ###into_table###             --into VESPA_Comscore_SkyGo_YYYYMM   '
    set @xsql=@xsql+' from ${CBAF_DB_DATA_SCHEMA}.V239_comscore_view    '
    set @xsql=@xsql+'--where (erroneous_data_suspected_flag = 0 AND duration_viewed <= content_duration)        '
    set @xsql=@xsql+' group by account_number, cb_key_household, sam_profileid, ns_ap_device, platform_name, platform_version,  '
    set @xsql=@xsql+'         stream_context, station_name, channel_id, vod_asset_id, ad_asset_id,      '
    set @xsql=@xsql+'         --broadcast details       '
    set @xsql=@xsql+'         viewing_event_start_utc, viewing_event_end_utc,   '
    set @xsql=@xsql+'         viewing_event_start_client_utc_raw, viewing_event_end_client_utc_raw,     '
    set @xsql=@xsql+'         viewing_event_start_client_local, viewing_event_end_client_local, '
    set @xsql=@xsql+'         daylight_savings_flag_start, daylight_savings_flag_end,   '
    set @xsql=@xsql+'         server_start_utc,  server_end_utc,        '
    set @xsql=@xsql+'         server_start_utc_raw, server_end_utc_raw, '
    set @xsql=@xsql+'         server_start_local_time, server_end_local_time,   '
    set @xsql=@xsql+'         connection_type_start, connection_type_end,       '
    set @xsql=@xsql+'         genre, ad_flag--, ns_st_ec_start, ns_st_ec_end    '
    set @xsql=@xsql+' order by account_number, cb_key_household, sam_profileid, ns_ap_device, platform_name, platform_version, viewing_event_start_utc  '
    set @xsql=@xsql+' commit    '
    set @xsql=@xsql+'           '
    set @xsql=@xsql+'    --update the audit_run table with success...   '
    set @xsql=@xsql+' UPDATE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl  '
    set @xsql=@xsql+'   SET basic_view_created = 1      '
    set @xsql=@xsql+' where data_date = dateformat(cast(''###data_run_date###'' as date), ''YYYY-MM-DD'')       '
    set @xsql=@xsql+' commit    '

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
            EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Inserting into existing view<'||@monthly_table_name||'>'
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
                ')

            EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Creating new view<'||@monthly_table_name||'>'
            commit
        END

    execute('grant select on '||@monthly_table_name||' to vespa_group_low_security')

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed build of basic view'



    /* code_location_F01 *************************************************************************
     *****                                                                                      **
     *****       Update basic stats... links back with file/raw data stats                      **
     *****                                                                                      **
     *********************************************************************************************/

    SET @xsql = '
                SELECT sum(case when stream_context = ''vod'' then 1 else 0 end)  aggr_vod_events,
                       sum(case when stream_context = ''dvod'' then 1 else 0 end) aggr_dvod_events,
                       sum(case when stream_context = ''lin'' then 1 else 0 end)  aggr_lin_events
                  into ${CBAF_DB_DATA_SCHEMA}.Comscore_skygo_day_stats_tmp
                  from '||@monthly_table_name||' a
                 where a.data_date_local = '''||@data_run_date||'''
                 group by a.data_date_local

                UPDATE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_stats_tbl s
                   SET s.aggr_vod_events  = a.aggr_vod_events,
                       s.aggr_dvod_events = a.aggr_dvod_events,
                       s.aggr_lin_events  = a.aggr_lin_events
                  from ${CBAF_DB_DATA_SCHEMA}.Comscore_skygo_day_stats_tmp a
                WHERE s.data_date = '''||@data_run_date||'''

                  drop table ${CBAF_DB_DATA_SCHEMA}.Comscore_skygo_day_stats_tmp

                 commit
                '
    EXECUTE(@xsql)



    /* code_location_G01 *************************************************************************
     *****                                                                                      **
     *****                      Create Channel Mapping Information                              **
     *****                                                                                      **
     *********************************************************************************************/

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Refreshing channel mapping information'
    EXEC ${CBAF_DB_LIVE_SCHEMA}.vespa_comscore_skygo_channelmap_refresh



    /* code_location_G02 *************************************************************************
     *****                                                                                      **
     *****                Make manual adjustments to channel mapping info                       **
     *****                                                                                      **
     *********************************************************************************************/

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Making manual channelmap updates'
    EXEC ${CBAF_DB_LIVE_SCHEMA}.vespa_comscore_skygo_channelmap_manual_updates

    --update channels_mapped flag in audit_run_tbl
    execute('UPDATE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl
                SET channels_mapped = 1
              where data_date = '''||@data_run_date||'''
             commit ')



    /* code_location_H01 *************************************************************************
     *****                                                                                      **
     *****             Split Linear Events into Programme Instances                             **
     *****                                                                                      **
     *********************************************************************************************/

/* --params for running code from this point...
DECLARE @data_run_date date
DECLARE @xsql               varchar(5000)
DECLARE @monthly_table_name varchar(32)
SET @data_run_date = '2014-10-25'
SET @monthly_table_name = 'VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')
*/

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Splitting linear viewing events'

    --iterates through the aggregated linear events, splitting each one into programme instances
    EXEC ${CBAF_DB_LIVE_SCHEMA}.vespa_comscore_skygo_linear_split @data_run_date

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed linear instance derivations'

    --update linear_events_split flag in audit_run_tbl
    execute('
      Update ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl
         SET linear_events_split = 1
       WHERE data_date = '''||@data_run_date||'''
      commit
     ')



    /* code_location_H02 *************************************************************************
     *****                                                                                      **
     *****        Record stats around splitting linear events into Programme Instances          **
     *****                                                                                      **
     *********************************************************************************************/


    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Updating linear stats'

    SET @xsql = '
                SELECT sum(case when linear_instance_flag = 1 then 1 else 0 end)  linear_event_instances,
                       sum(case when linear_instance_flag = 0 then 1 else 0 end) aggr_lin_not_split
                  into ${CBAF_DB_DATA_SCHEMA}.Comscore_skygo_day_stats_tmp_b
                  from '||@monthly_table_name||' a
                 where a.data_date_local = '''||@data_run_date||'''
                   and stream_context = ''lin''
                 group by a.data_date_local

                UPDATE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_stats_tbl s
                   SET s.linear_event_instances  = a.linear_event_instances,
                       s.aggr_lin_not_split = a.aggr_lin_not_split
                  from ${CBAF_DB_DATA_SCHEMA}.Comscore_skygo_day_stats_tmp_b a
                WHERE s.data_date = '''||@data_run_date||'''

                  drop table ${CBAF_DB_DATA_SCHEMA}.Comscore_skygo_day_stats_tmp_b

                commit
                '
    EXECUTE(@xsql)


  /* code_location_I01 ***************************************************************************
     *****                                                                                      **
     *****       Scan the day's records and make corrections to missing or incorrect data       **
     *****                                                                                      **
     *********************************************************************************************/


    -- we should log these updates/deletes for data quality purposes


    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Applying adjustments to impossible viewing events'

    --set duration from programme_instance start/end if it is found to be negative - and start/end are available
    SET @xsql = 'UPDATE '||@monthly_table_name||'
                    SET duration_viewed = datediff(ss, programme_instance_start_utc, programme_instance_end_utc)
                  where coalesce(duration_viewed, -1) < 0
                    and programme_instance_start_utc is not null
                    and programme_instance_end_utc is not null
                    and data_date_local = '''||@data_run_date||'''
                 commit'
    EXECUTE(@xsql)

    --make sure that all viewing durations are capped to the calculated viewing period
    SET @xsql = 'UPDATE '||@monthly_table_name||'
                    SET duration_viewed = datediff(ss, programme_instance_start_utc, programme_instance_end_utc)
                  where datediff(ss, programme_instance_start_utc, programme_instance_end_utc) < duration_viewed
                    and programme_instance_start_utc is not null
                    and programme_instance_end_utc is not null
                    and data_date_local = '''||@data_run_date||'''
                 commit'
    EXECUTE(@xsql)

    --remove anything that that doesn't have a start or end time. Later we could try and rescue these but the volumes are low (circa 0.1%)
    SET @xsql = 'DELETE '||@monthly_table_name||'
                  where programme_instance_start_utc is null
                     or programme_instance_end_utc is null
                 commit'
    EXECUTE(@xsql)

    --set erroneous data flag when long duration events are suspected due to client-side clock changes [flag all viewing where duration_viewed > 86400]
    SET @xsql = 'UPDATE '||@monthly_table_name||'
                    SET erroneous_data_suspected_flag = 1
                  where duration_viewed > (24*3600)
                 commit'
    EXECUTE(@xsql)



    /* code_location_J01 *************************************************************************
     *****                                                                                      **
     *****       Refresh union view  -- VESPA_Comscore_SkyGo_Union_View                         **
     *****                                                                                      **
     *********************************************************************************************/

    declare @current_month varchar(6)
    declare @months        integer

    --create months list that should be available
    select dateformat(data_date, 'yyyymm') months, dense_rank() over(order by months) rank, null new_rank
      into ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp
      from ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_summary_tbl
     group by dateformat(data_date, 'yyyymm')


    --verify that the months are available
    select @months = max(rank)
      from ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp
     where rank <= 13    --- restrict the maximum number of months in this view


    --iterate through each month, if the table doesn't exist, delete it from the list
    while(@months>0)
        BEGIN
            select @current_month = months
              from ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp
             where rank = @months

            IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_'||@current_month) IS NULL
                BEGIN
                    delete ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp
                     where months = @current_month
                    commit
                END
            SET @months = @months - 1
        END

    --as some months may have been missing (so deleted), give the order a new_rank
    select months, rank, dense_rank() over(order by months) new_rank
      into ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp_b
      from ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp
     group by months, rank


    --create union view of all the verified months
    select @months = count(1)
      from ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp_b
    IF @months > 0
        BEGIN
            declare @union_view_str varchar(1000)
            set @union_view_str = 'create or replace view ${CBAF_DB_LIVE_SCHEMA}.VESPA_Comscore_SkyGo_Union_View as ('

            --iterate through each month, if the table doesn't exist delete it from the list
            while(@months>0)
                BEGIN
                    select @current_month = months
                      from ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp_b
                     where new_rank = @months

                    SET @union_view_str = @union_view_str||' select * from ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_'||@current_month
                    IF @months > 1
                        BEGIN
                            SET @union_view_str = @union_view_str||' union all '
                        END
                    SET @months = @months - 1
                END
            set @union_view_str = @union_view_str||')     '
            execute(@union_view_str)
            commit
        END




    /* code_location_K01 *************************************************************************
     *****                                                                                      **
     *****                       Log that the build has completed                               **
     *****                                                                                      **
     *********************************************************************************************/

    EXEC ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed Build'



    /* code_location_L01 *************************************************************************
     *****                                                                                      **
     *****                       Drop tables no longer required                                 **
     *****                                                                                      **
     *********************************************************************************************/

    drop table ${CBAF_DB_DATA_SCHEMA}.VESPA_CALENDAR_section_tmp
    drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp4
    drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp3
    drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp2
    drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_event_tmp
    drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_view2
    drop table ${CBAF_DB_DATA_SCHEMA}.V239_comscore_view
    drop table ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp_b
    drop table ${CBAF_DB_DATA_SCHEMA}.Comscore_SkyGo_union_months_tmp


    END --end of wrapped @status not in ('ok','low') check

END
GO


