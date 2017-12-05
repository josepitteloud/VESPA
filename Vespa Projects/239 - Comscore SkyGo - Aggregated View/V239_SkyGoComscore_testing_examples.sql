

    /* code_location_A01 *********************************************************
     *****                                                                      **
     *****            Files to execute SQL                                      **
     *****                                                                      **
     *****************************************************************************/

        V239_SkyGoComscore_0_TableConstruct                 -- Constructs some tables
        V239_SkyGoComscore_1_EnvSetup                       -- Contains procedure for upfront DQ checks and stats for the raw data
        V239_SkyGoComscore_2_ChannelMap_refresh             -- Contains procedure for the refresh of channel mapping info from raw Comscore and Channel Mapping Attributes table
        V239_SkyGoComscore_2-1_ChannelMap_manual_mappings   -- Contains procedure for over-writing NULL entries in the Channel Mapped refreshed data with some predefined defaults
        V239_SkyGoComscore_3_LinearBuild                    -- Contains procedure for building the linear programme instances from the EPG and replacing channel events in the final view
        V239_SkyGoComscore_4_AggrView                       -- Contains procedure that runs the daily build of aggregated events



    /* code_location_A01 *********************************************************
     *****                                                                      **
     *****            Tables created during the build process                   **
     *****                                                                      **
     *****************************************************************************/


        VESPA_Comscore_SkyGo_log_tbl
        VESPA_Comscore_SkyGo_universe_tbl
        VESPA_Comscore_SkyGo_SAV_account_type_tbl
        VESPA_Comscore_SkyGo_SAV_summary_tbl
        VESPA_Comscore_SkyGo_audit_stats_tbl
        VESPA_Comscore_SkyGo_audit_run_tbl
        VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
        VESPA_Comscore_SkyGo_Channel_Mapping_tbl
        VESPA_Comscore_SkyGo_201408   [VESPA_Comscore_SkyGo_YYYYMM]

        --temporary tables:




    /* code_location_A02 *********************************************************
     *****                                                                      **
     *****            Procedures created                                        **
     *****                                                                      **
     *****************************************************************************/

        vespa_comscore_skygo_channelmap_manual_updates  -- builds manual defaults to use where channel mapping info is not generated
        vespa_comscore_skygo_channelmap_refresh         -- refreshes channel mapping info for automated channel mapped
        vespa_comscore_skygo_event_view_create          -- runs the daily build of aggregated events [main procedure]
        vespa_comscore_skygo_event_view_prep            -- does upfront DQ checks and raw data stats
        vespa_comscore_skygo_linear_split               -- builds linear programme instances from the EPG and replaces channel events in the final view
        vespa_comscore_skygo_log                        -- logging proc





    /* code_location_B01 ***********************************************************************
     *****                                                                                    **
     *****       Build Log - entries created during the run of procedures                     **
     *****                                                                                    **
     *******************************************************************************************/

    commit
    select top 3000 *
      from VESPA_Comscore_SkyGo_log_tbl
     order by log_datetime asc

     ------ not officialy a log.. but does summarise what's been built
    commit
    select *
      from VESPA_Comscore_SkyGo_audit_run_tbl r,
           VESPA_Comscore_SkyGo_audit_stats_tbl s
     where r.data_date = s.data_date
     order by r.data_date




    /* code_location_C01 ***********************************************************************
     *****                                                                                    **
     *****      Raw Feed Testing  -- following the ETL process of the raw Comscore file       **
     *****                                                                                    **
     *******************************************************************************************/

    -- Each month has a new table/view created [format Comscore_YYYYMM  in Olive]
    select top 1000 *
      from Comscore_201408

    select top 1000 *
      from Comscore_201409

    --these monthly views are combined (for the last 13months) into a combined view
    select top 1000 *
      from COMSCORE_UNION_VIEW

    -- See data dictionaries for the explanation of column headings.


    --did a file arrive
     select sg_vs_sc, count(1) records
       from COMSCORE_UNION_VIEW
      where cb_source_file = 'Comscore_20141106.gz'
      group by sg_vs_sc



    /* code_location_D01 *****************************************************************
     *****                                                                              **
     *****       Preliminary Staging Tables                                             **
     *****                                                                              **
     *************************************************************************************/

    -- These tables are used:
    --     V239_comscore_event_tmp4
    --     V239_comscore_event_tmp3
    --     V239_comscore_event_tmp2
    --     V239_comscore_event_tmp


    --end of day example
    select top 1000 *
      from V239_comscore_event_tmp
     where sam_profileid = 10026485
       and ns_ts >'2014-08-04 22:50:00.000000'   --  <-- look at the end of day by adjusting this (23:00 is mignight UTC, 00:00 local-time in BST)


    --missing end-time example
    select top 1000 *
      from V239_comscore_event_tmp
     where sam_profileid = 18892265
       and ns_ts >'2014-08-03 23:00:00.000000'
    -- interested by the event at [2014-08-04 00:01:23.000000]
    -- this will only be here if the tmp table was last built to the 3rd August, otherwise change the samprofileid to look for alternative


    --missing duration +other details example    [ns_st_pt  = accumulated clip playing time, when end_event_Series = 1 this is the ns_st_pt that is used]
    select top 1000 *
      from V239_comscore_event_tmp
     where sam_profileid = 17894031
     order by uniqid
      --if ns_st_pt = 0 and end_event_series = 1 then how is this represented in the following views, is there are duration?




    /* code_location_E01 *************************************************************************
     *****                                                                                    **
     *****       Preliminary 'View' Tables                                                              **
     *****                                                                                    **
     *******************************************************************************************/
      -- These tables are used:
      --     V239_comscore_view2
      --     V239_comscore_view


    --start of day example
    select top 1000 *
      from V239_comscore_view2
     where stream_context = 'lin'
       and sam_profileid = 10003466

    --end of day example
    select top 1000 *
      from V239_comscore_view2
     where sam_profileid = 10026485
       --and ns_utc >'2014-08-04 23:50:00.000000'
       and viewing_event_start_utc >= '2014-08-04 23:50:00.000000'



    select top 1000 *
      from V239_comscore_view
     where stream_context = 'lin'
     order by sam_profileid, uniqid --viewing_event_start_utc


    --end of day example
    select top 1000 *
      from V239_comscore_view
     where sam_profileid = 10026485
       and viewing_event_start_utc >= '2014-08-04 23:50:00.000000'
     order by uniqid



    /* code_location_F01 *************************************************************************
     *****                                                                                    **
     *****       Final Event 'View' Table                                                     **
     *****                                                                                    **
     *******************************************************************************************/

    -- a months aggregated view (similar to the raw data)
    select top 1000 *
      from VESPA_Comscore_SkyGo_201411

    -- view that covers 13 months (similar to the raw data)
    select top 1000 *
      from VESPA_Comscore_SkyGo_UNION_VIEW


    --identify linear viewing split into programme instance
    select top 10000 *
      from VESPA_Comscore_SkyGo_201408
     where stream_context = 'lin'
       and instance_flag = 1   --(0 for events that were not channel mapped)


    /* code_location_G01 ***********************************************************************
     *****                                                                                    **
     *****       Linear programme instance identification                                     **
     *****                                                                                    **
     *******************************************************************************************/

        --this doesn't handle historical service_keys
        commit
        select top 1000 *
          from VESPA_Comscore_SkyGo_Channel_Mapping_tbl

        commit
        select top 10000 *
          from VESPA_Comscore_SkyGo_RawChannels_tmp


        select top 1000 *
          from VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
         where events_mapped is null
           and data_date_local = '2014-12-15'


    /* code_location_H01 ***********************************************************************
     *****                                                                                    **
     *****       Audit tables / tests                                                         **
     *****                                                                                    **
     *******************************************************************************************/

    commit
    select *
      from VESPA_Comscore_SkyGo_audit_run_tbl r,
           VESPA_Comscore_SkyGo_audit_stats_tbl s
     where r.data_date = s.data_date
     order by r.data_date


--has a file arrived
select count(1)
from Comscore_union_view
where cb_source_file = 'Comscore_20141124.gz'



------update run/stats  [set basic_view_created=0 where the build didn't create]
UPDATE VESPA_Comscore_SkyGo_audit_run_tbl u
   SET u.basic_view_created = 0
   where u.data_date in ( select r.data_date
      from VESPA_Comscore_SkyGo_audit_run_tbl r,
           VESPA_Comscore_SkyGo_audit_stats_tbl s
     where r.data_date = s.data_date
       and s.aggr_vod_events = -99
       and s.aggr_dvod_events = -99
       and s.aggr_lin_events = -99)
commit
------

    select *
      from VESPA_Comscore_SkyGo_audit_run_tbl r
     order by data_date

      select *
      from VESPA_Comscore_SkyGo_audit_stats_tbl r
     order by data_date

    select top 1000 *
      from VESPA_Comscore_SkyGo_universe_tbl

    select count(1)
      from VESPA_Comscore_SkyGo_universe_tbl


--channel map audit
    select top 1000 a.*
      from VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl a
     where data_date_local = '2014-12-11'

    commit
    select *
      from VESPA_Comscore_SkyGo_audit_stats_tbl s

    --find channel information we need
    select top 100 *
      from VESPA_ANALYSTS.channel_map_prod_service_key_attributes
     where effective_to = '2999-12-31 00:00:00.000000'
       and lower(full_name) like '%crime%'




    /* code_location_I01 ***********************************************************************
     *****                                                                                    **
     *****       Areas to test                                                                **
     *****                                                                                    **
     *******************************************************************************************/

    -- 1. Number of raw accounts  vs.  accounts in the final view
    -- 2. Linear durations calculated correctly (Look at how the events are broken down, where the event starts and ends etc..)
    -- 3. Events missing end durations - are there any, if so - why?
    -- 4. Negative durations - are there any, if so - why?  likely to be focused on vod/dvod content
    -- 5. Midnight cross-over duration calculation, are these correct?





    /* code_location_I01 ***********************************************************************
     *****                                                                                    **
     *****       Following linear viewing through from raw data to aggregated view            **
     *****                                                                                    **
     *******************************************************************************************/

    --raw data at the start of the feed [starts at 2014-07-31 23:03:51 on the client]
    select top 1000 *
      from Comscore_201408
     where sam_profileid = 11068195
       and sg_vs_sc = 'lin'
       and ns_utc between '2014-07-31 23:00:00.000000' and '2014-08-01 22:59:59.999999'
     order by ns_ts_raw, ns_st_ec, ns_utc_raw

    --output in event view - split into programme instances
    select top 100 *
      from VESPA_Comscore_SkyGo_201408
     where sam_profileid = 11068195
     order by aggr_event_id, broadcast_start_date_time_utc

    --what happened in the middle? [this only works if the last day built is the day you are interested in, as tables overwritten]
    select top 1000 *
      from VESPA_Comscore_linear_base_tmp
     where aggr_event_id = 20140805693788





---negative duration
--output in event view - split into programme instances
    select top 100 *
      from VESPA_Comscore_SkyGo_201408
     where duration_viewed < 0
     and viewing_event_start_utc > '2014-08-10 00:00:00'
     order by aggr_event_id, broadcast_start_date_time_utc


--really bad Comscore data example... no wonder we can't transform this :(
   select top 1000 *
      from Comscore_201408
     where sam_profileid = 30661794
      -- and sg_vs_sc = 'lin'
        and ns_utc between '2014-07-31 23:00:00.000000' and '2014-08-01 22:59:59.999999'
     order by ns_ts_raw, ns_st_ec, ns_utc_raw


--we remove these from the final view, so shouldn't be any
select count(1)
from VESPA_Comscore_SkyGo_201408
where programme_instance_start_utc is null

--we convert these (ns_st_pt) to NULL, so we don't use the default -99 in calculations
select *
  from Comscore_201408
 where ns_st_pt = -99
 order by sam_profileid, ns_ts_raw

--an example where substantial information is missing that is required to determine start and end points
select *
from Comscore_201408
where sam_profileid = 27817618
and ns_st_ci = '79e152ebf8177410VgnVCM1000000b43150a____'
order by ns_st_id, ns_st_pt_raw, last_session_ts, ns_ts_raw, ns_utc_raw





select data_date_local, stream_context, count(1) ad_impressions
  from VESPA_Comscore_SkyGo_Union_View
 where ad_flag = 1
 and ns_st_ci is null

 group by data_date_local, stream_context
 order by data_date_local, stream_context

select stream_context, content_duration, duration_viewed
  from VESPA_Comscore_SkyGo_Union_View
 where ad_flag = 1
   and ad_asset_id is null
   group by stream_context,content_duration, duration_viewed
--count(1) 1,118,274

commit
select top 1000 *
  from VESPA_Comscore_SkyGo_Union_View
 where ad_flag = 1
   and ad_asset_id is null
   and content_duration >= 50000


select *
from Comscore_201408
--where ns_ts = '2014-08-01 14:49:03.000000'
where cast(dateformat(ns_ts ,'YYYYMMDD') as date) = cast('2014-08-01' as date)
and sam_profileid = 29736804
order by ns_ts_raw

--same assets with different clip lengths!... what is consistent between these??
select ns_st_cl, t.*
  from Comscore_201408 t
 where ns_st_ci = '376a26d788d47410VgnVCM1000000b43150a____'
order by ns_st_cl


--are there linear cross-over events?
select top 1000 *
  from VESPA_Comscore_SkyGo_Union_View
 where view_continuing_flag = 1
 and stream_context = 'lin'
 and linear_instance_flag = 1

-------



select top 1000 *
  from VESPA_Comscore_SkyGo_Union_View
  where duration_viewed > (content_duration *2)
  and content_duration > 0
  and cast(aggr_event_id as varchar(24)) like '20140808%'
--issue for sam: 15613558
-- aggrid: 20140806 1318278
ad_asset_id: 'cue_point-33318304909'
duration: 163
content: 30

--issues
sam_profileid = 10127975
2014-08-08


--cases where we are calculating a negative ns_st_pt part to sum from the play duration
select top 1000 *
from V239_comscore_view
where coalesce(content_duration,0) < play_duration
--where play_duration < 0
--where continuing_content_from = 1 --negative durations found here
--where content_continuing_to = 1
and stream_context = 'vod'
--and ns_st_ec_end-ns_st_ec_start between 0 and 1
and sam_profileid = 10049052

--where sam_profileid = 10017072 --negative duration adjustment
where sam_profileid = 10091579 -- long vod play [asset:'13ad1b190c197410VgnVCM1000000b43150a____']


--event that continues to the next day, has play duration 5747 calculated, but is long for vod (unless movie).
--play duration for content continuing to next day is simply the event duration (rather than play calculated using ns_st_pt) is it correct to do this?
sam_profileid = 10091579
[sg_vs_sc] stream_context = 'vod'
[ns_st_ci] vod_asset_id = '13ad1b190c197410VgnVCM1000000b43150a____'
[ns_st_pr] programme name = 'Trading Places'


--this will be capped?
sam: 10253082
asset: f4495321c6405410VgnVCM1000000b43150a____

--not capped
sam: 10049052
asset: 6a5d56649b867410VgnVCM1000000b43150a____
content: 3268
play: 71088


select top 100 *
from VESPA_Comscore_SkyGo_201408
where cast('2014-08-08' as date) = data_date_local

--not capped
select top 100 *
from VESPA_Comscore_SkyGo_201408
where --cast('2014-08-08' as date) = data_date_local
    vod_asset_id = '6a5d56649b867410VgnVCM1000000b43150a____'
and sam_profileid = 10049052

--capped?
select top 100 *
from VESPA_Comscore_SkyGo_201408
where cast('2014-08-08' as date) = data_date_local
and vod_asset_id = 'f4495321c6405410VgnVCM1000000b43150a____'
and sam_profileid = 10253082


select top 100 *
from V239_comscore_view
where vod_asset_id = 'f4495321c6405410VgnVCM1000000b43150a____'
and sam_profileid = 10253082


--so for this asset the event should continue on 2014-08-07 into 08 but does not
--what's in the raw data?
select top 1000 *
from Comscore_201408
where ns_st_ci = '6a5d56649b867410VgnVCM1000000b43150a____'
and sam_profileid = 10049052
order by ns_ts_raw
--this has alot of end events that are being included in the viewing duration




--what happens detecting end events
commit
select top 1000 *
from V239_comscore_event_tmp2
where ns_st_ci = '6a5d56649b867410VgnVCM1000000b43150a____'
and sam_profileid = 10049052
order by ns_ts_raw

--removing false end - end
commit
select top 1000 t.*, t.prev_ns_st_ev||ns_st_ev code_str
into #part_tmp
from V239_comscore_event_tmp2 t
where ns_st_ci = '6a5d56649b867410VgnVCM1000000b43150a____'
and sam_profileid = 10049052
order by ns_ts_raw

select *
from #part_tmp

where code_str != 'endend'
 and ( new_event_series = 1
        or end_event_series = 1
        or continuing_content_from = 1
        or content_continuing_to = 1)

--what happens in create view section
select top 1000 *
  from V239_comscore_view2
  where sam_profileid = 10049052
and vod_asset_id = '6a5d56649b867410VgnVCM1000000b43150a____'

--try a fix to exclude end:end events
select top 1000 *
  from V239_comscore_view2
  where sam_profileid = 10049052
and vod_asset_id = '6a5d56649b867410VgnVCM1000000b43150a____'
and not(coalesce(prev_ns_st_ev, 'play') = 'end' and ns_st_ev = 'end')

--what happens when we combine these rows
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
           coalesce(e.start_ns_st_ec, e.prev_ns_st_ec_start) ns_st_ec_start,                                --  event counter
           coalesce(e.end_ns_st_ec, e.next_ns_st_ec_end) ns_st_ec_end,                                      --  event counter
           genre, ad_flag,
           unique_duration,
           datediff(ss, viewing_event_start_utc, viewing_event_end_utc) event_duration,                           --  better to convert this to use the raw start/end
           floor((viewing_event_end_client_utc_raw - viewing_event_start_client_utc_raw)/1000) event_sec_precise, --  <-- I think use this for event_secs
           content_duration,
           programme_name, buffering_duration,
            --need to work out the duration of viewing before midnight if event_continuing
           case when continuing_content_from = 1 then -(ns_st_pt - datediff(ss, viewing_event_start_utc, ns_ts))  --  period of content *before* midnight (to subtract from accumulated clip playing time)
                when content_continuing_to = 1 then  datediff(ss, viewing_event_start_utc, viewing_event_end_utc) --  period *upto* midnight to use as accumulated clip playing time
                when new_event_series = 1 then 0                                                                  --  don't use any surplus accumulated play-times at the start of the series
                when end_event_series = 1 AND e.ns_st_pt = 0 then fake_ns_st_pt
                when end_content = 1 AND e.ns_st_pt = 0 AND prev_ns_st_ev = 'pause' AND ns_st_ev = 'play' then fake_ns_st_pt
                when end_content = 1 AND e.ns_st_pt = 0 then next_ns_st_pt                                        --  use fix if playtime is attributed to the next event incorrectly
                else ns_st_pt
            end play_duration
 from V239_comscore_view2 e
  where sam_profileid = 10049052
and vod_asset_id = '6a5d56649b867410VgnVCM1000000b43150a____'
and not(coalesce(prev_ns_st_ev, 'play') = 'end' and ns_st_ev = 'end')


select top 10 *
from barbera.VESPA_Comscore_SkyGo_Union_View

commit
select top 1000 e.*,
       case when coalesce(content_duration, 0) =0 then null else cast(round((duration_viewed*1.00/content_duration*1.00),2) as decimal(5,3)) end percentage_viewed
  from VESPA_Comscore_SkyGo_Union_View e
  where sam_profileid = 10049052
and vod_asset_id = '6a5d56649b867410VgnVCM1000000b43150a____'

select 71088*1.0/3268*1.0
select cast(71088*1.0/3268*1.0 as decimal(5,3))

--in general
commit
select top 1000 *
from V239_comscore_event_tmp3
where prev_ns_st_ev is null and ns_st_ev = 'end' and ns_Server_local_time between cast('2014-10-08 00:00:00.000000' as datetime) and cast('2014-10-08 00:20:00.000000' as datetime)

ns_st_ci = '6a5d56649b867410VgnVCM1000000b43150a____'
and sam_profileid = 10049052
order by ns_ts_raw


--end of day example
select top 1000 *
  from V239_comscore_event_tmp
 where sam_profileid = 10026485
   and ns_ts >'2014-08-04 22:50:00.000000'

--missing end-time example
select top 1000 *
  from V239_comscore_event_tmp
 where sam_profileid = 18892265
   and ns_ts >'2014-08-03 23:00:00.000000'
-- interested by the event at [2014-08-04 00:01:23.000000]

--missing duration +other details example
select top 1000 *
  from V239_comscore_event_tmp
 where sam_profileid = 17894031
   and ns_ts >'2014-08-04 15:00:00.000000'


select top 1000 *
from V239_comscore_view
where sam_profileid is not null
order by sam_profileid, viewing_event_start_utc

select top 1000 *
from V239_comscore_view
where --stream_context = 'lin'
      sam_profileid = 10003458
order by sam_profileid, viewing_event_start_utc

--end of day example
select top 1000 *
  from V239_comscore_view
 where sam_profileid = 10026485
   --and ns_utc >'2014-08-04 23:50:00.000000'
   and viewing_event_start_utc >= '2014-08-04 23:50:00.000000'

----grouping test
select top 1000 *
  from V239_comscore_view
 where sam_profileid = 10659552
 order by sam_profileid, ns_ap_device, platform_name, platform_version,
          viewing_event_start_utc
          

--------------------------
--------------------------
--SKYGO TESTING


--how many content durations are null
select data_date_local, count(1)
  from VESPA_Comscore_SkyGo_201408
where content_duration is null
group by data_date_local


--how many content durations are null (by stream_context)
select data_date_local,  stream_context, platform_name, count(1)
  from VESPA_Comscore_SkyGo_201408
where content_duration is null
group by data_date_local,  stream_context, platform_name

--if linear durations are null, then are we channel mapping?
select top 10000 *
  from vespa_comscore_skygo_channel_mapping_tbl
where service_key is null

--these are the station names that we are not mapping - and need to be
select top 10000 *
  from barbera.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
where data_date_local = '2014-08-13'
order by events_not_mapped desc

select top 1000 *
from VESPA_Comscore_SkyGo_audit_stats_tbl

commit
select top 1000 *
  from VESPA_Comscore_SkyGo_Union_View
  where sam_profileid = 10049052
and vod_asset_id = '6a5d56649b867410VgnVCM1000000b43150a____'


--find where we have long duration viewing, and could it be erroneous
commit
select data_date_local, erroneous_data_suspected_flag, max(duration_viewed), max(content_duration)
  from VESPA_Comscore_SkyGo_201408
group by data_date_local, erroneous_data_suspected_flag

select top 1000 v.*, datediff(ss, v.viewing_event_start_utc, v.viewing_event_end_utc) period, duration_viewed
  from VESPA_Comscore_SkyGo_201408 v


select top 100 v.*, datediff(ss, v.programme_instance_start_utc, v.programme_instance_end_utc)
--select count(1)
  from VESPA_Comscore_SkyGo_201408 v
 where duration_viewed > 200
 and ad_flag = 1
 and data_date_local = '2014-08-10'



select data_date_local, stream_Context, count(1) sample_count
  from VESPA_Comscore_SkyGo_201408 v
where datediff(ss, v.programme_instance_start_utc, v.programme_instance_end_utc) < duration_viewed
group by data_date_local, stream_Context


select top 1000 v.*, datediff(ss, v.viewing_event_start_utc, v.viewing_event_end_utc) period, duration_viewed
  from VESPA_Comscore_SkyGo_201408 v
where datediff(ss, v.programme_instance_start_utc, v.programme_instance_end_utc) < duration_viewed



--- this one has a viewing duration for an Ad that is far too long
select top 1000 *
  from VESPA_Comscore_SkyGo_201408 v
 where sam_profileid = 39570268
 and data_date_local = '2014-08-10'

--summery of event
start: 2014-08-10 00:00:00.000000
  end: 2014-08-10 22:18:01.000000
     event_count: 20
content_duration: 80
duration_viewed = 80281

--in raw data
select top 1000 *
from Comscore_201408
where ns_utc between '2014-08-08 22:00:00.000000' and '2014-08-10 22:00:00.000000'
and sam_profileid = 39570268
order by ns_ts_raw

--compare
select --top 10000 a.*, b.*,
       --sum(a.duration_viewed)--
       sum(b.duration_viewed)
from VESPA_Comscore_SkyGo_201408_tmp a, VESPA_Comscore_SkyGo_201408 b
where a.aggr_event_id = b.aggr_event_id
and a.duration_viewed != b.duration_viewed
and a.stream_context != 'lin'
and b.stream_context != 'lin'

--prev
sum(a.duration_viewed)
545,574,340

--after
204,752,298
sum(b.duration_viewed)
select (204752298*0.1)/(545574340*0.1)


select 340822042/3600/24


--long duration views  - RESULTING FROM CLIENT CLOCK CHANGE?
> 400000 on '2014-08-12'
select *
from VESPA_comscore_skygo_union_view
where data_date_local = '2014-08-12'
and duration_viewed > 400000
-- 2014-08-07 12:02:21.000000   to   2014-08-12 14:11:36.000000

select *
from Comscore_union_view
where sam_profileid = 25666771
order by ns_ts_raw


commit
select top 1000 *
from V239_comscore_event_tmp2
--where sam_profileid = 35925386
where sam_profileid = 37674787
--where sam_profileid = 32818634

commit
select top 1000 *
from V239_comscore_view
where sam_profileid = 32818634


select top 100 *
  from VESPA_Comscore_SkyGo_201408
where sam_profileid = 38387672
and data_date_local = '2014-08-08'


--there is a pattern where long viewing durations are linked to prev_ns_st_ev is null and ns_st_ev = 'keep-alive'
select top 1000 *
from V239_comscore_event_tmp2
where prev_ns_st_ev is null and ns_st_ev = 'keep-alive'
and ns_st_pt > ns_st_el

select top 1000 *
  from VESPA_Comscore_SkyGo_201408
where sam_profileid in (10003817,
10006804,
10010920,
10011063,
10011462,
10011982,
10012086,
10012505,
10013744,
10015806,
10016490,
10017710,
10018967,
10020212,
10021587,
10026176,
10026664,
10026986,
10027254,
10028077,
10028187,
10028394,
10029340)
and data_date_local = '2014-08-08'
order by sam_profileid



--are there many really long durations?
 select top 1000 *
   from VESPA_Comscore_SkyGo_201408
  where duration_viewed >  content_duration*3
  and coalesce(content_duration, 0) !=0
    and data_date_local = '2014-08-12'


select (66414*0.1)/(2509*0.1), cast((66414*0.1)/(2509*0.1) as decimal(5,3))
alter table VESPA_Comscore_SkyGo_201408 modify percentage_viewed decimal(5,3)
commit




--lets look at events where the event count is null, what do they have in common? are they all with no start event for example??
select top 100 *
  from VESPA_Comscore_SkyGo_201408
where event_Count is null
and data_date_local = '2014-08-12'

--sample

-- which platforms do we have ad data for?
select platform_name, stream_context, sum(case when ad_asset_id is not null then 1 else 0 end) sample_Count
  from VESPA_Comscore_SkyGo_201408
where ad_flag = 1
group by platform_name, stream_context



select *
from V239_comscore_event_tmp2
where sam_profileid = 14284776
dur 2509
view_utc: S 2014-08-08 16:31:06.000000
          E 2014-08-08 17:22:57.000000


select *
from V239_comscore_event_tmp2
where sam_profileid = 38387672

-----------------------------
-----------------------------




commit
select *
from VESPA_Comscore_SkyGo_log_tbl

--truncate table VESPA_Comscore_SkyGo_log_tbl
commit


select top 1000 *
from Comscore_201408
where sam_profileid = 15613558
and ns_st_cl = 30
order by ns_ts_raw, ns_st_ec

--can we recognise events that have play, end (2 events) where the accumulated duration is > content duration... and cap the event at content duration?
--if ns_st_ec is 1 difference then cap


-----

--actual viewing
--jason a/c: 622025642266





/************************
 **  stats
 ************************/


select top 10 *
from VESPA_COMSCORE_SKYGO_UNION_VIEW


select data_date_local, platform_name, stream_context, linear_instance_flag, count(1) viewing_events
from VESPA_COMSCORE_SKYGO_UNION_VIEW
group by data_date_local, platform_name, stream_context, linear_instance_flag



--Audi... ad slot id

--creativeID required from Freewheel
select top 10 *
from COMSCORE_union_view
where ns_st_ci like '0.0.0.587891%' --027'
or ns_st_ci = '0.0.0.1727687828'
or ns_st_ci = '0.0.0.1727687828'

