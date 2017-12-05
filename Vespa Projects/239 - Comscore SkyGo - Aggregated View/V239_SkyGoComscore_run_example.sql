

    /* code_location_A01 *********************************************************
     *****                                                                      **
     *****     Example of running the process for the required date             **
     *****                                                                      **
     *****************************************************************************/

 commit

    DECLARE @data_run_date date
    DECLARE @suppress_stats_universe_build bit
    DECLARE @lower_limit integer
    DECLARE @stddev_over_rows integer
    DECLARE @weighting_rows integer

    --set the date we are running this for
    SET @data_run_date = '2014-11-07'
    SET @suppress_stats_universe_build = 1

    --set variables for the monitoring stats (calculating expected data volumes from the underlaying data)
    SET @lower_limit = 200000 -- floor at which an alert is triggered regardless of other stats (number of expected accounts)
    SET @stddev_over_rows = 7 -- how many days the stddev of accounts is calculated over
    SET @weighting_rows = 4   -- how many days are used to exponentially weight the moving average

    EXEC vespa_comscore_skygo_event_view_create @data_run_date, @suppress_stats_universe_build, @lower_limit, @stddev_over_rows, @weighting_rows





    /* code_location_B01 *********************************************************
     *****                                                                      **
     *****            Tables created during the build process                   **
     *****                                                                      **
     *****************************************************************************/

    --the log
    commit
    select top 1000 *
      from VESPA_Comscore_SkyGo_log_tbl
     order by log_datetime asc

        --VESPA_Comscore_SkyGo_log_tbl

--tables changed name for + logging proc name
  ALTER TABLE  VESPA_Comscore_log_tbl          RENAME  VESPA_Comscore_SkyGo_log_tbl
  ALTER TABLE  VESPA_Comscore_universe_tbl     RENAME  VESPA_Comscore_SkyGo_universe_tbl
  ALTER TABLE  VESPA_SAV_account_type_tbl      RENAME  VESPA_Comscore_SkyGo_SAV_account_type_tbl
  ALTER TABLE  VESPA_Comscore_SAV_summary_tbl  RENAME  VESPA_Comscore_SkyGo_SAV_summary_tbl
  ALTER TABLE  VESPA_Comscore_audit_stats_tbl  RENAME  VESPA_Comscore_SkyGo_audit_stats_tbl
  ALTER TABLE  VESPA_Comscore_audit_run_tbl    RENAME  VESPA_Comscore_SkyGo_audit_run_tbl
  ALTER TABLE  VESPA_SkyGo_Comscore_201408     RENAME  VESPA_Comscore_SkyGo_201408

--logging proc change
VESPA_Comscore_log  --->  VESPA_Comscore_SkyGo_log

commit



    /* code_location_B02 *********************************************************
     *****    RAW DATA                                                          **/

    -- Each month has a new table/view created [format Comscore_YYYYMM]
    select top 1000 *
      from Comscore_201408

    select top 1000 *
      from Comscore_201409

    --these monthly views are combined (for the last 13months) into a combined view
    select count(1)
      from COMSCORE_UNION_VIEW
     where cb_source_file = 'Comscore_20141114.gz'
       and sg_vs_sc = 'lin'




    /* code_location_B03 *********************************************************
     *****    AUDIT TABLES                                                      **/

    select *
      from barbera.VESPA_Comscore_SkyGo_audit_run_tbl
      order by data_date
        -->  VESPA_Comscore_SkyGo_audit_run_tbl


    select *
      from barbera.VESPA_Comscore_SkyGo_audit_stats_tbl
      order by data_date
        -->  VESPA_Comscore_SkyGo_audit_stats_tbl


    select top 1000 *
      from barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl
        -->  VESPA_Comscore_SkyGo_SAV_summary_tbl

    select top 1000 *
      from VESPA_Comscore_SkyGo_SAV_account_type_tbl
        -->  VESPA_Comscore_SkyGo_SAV_account_type_tbl


    select *
      from barbera.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
     order by data_date_local


    select top 100000 *
      from barbera.VESPA_Comscore_SkyGo_universe_tbl
        --> VESPA_Comscore_SkyGo_universe_tbl




    /* code_location_C01 *********************************************************
     *****                                                                      **
     *****                    Examples for testing                              **
     *****                                                                      **
     *****************************************************************************/



    -- how many SkyGo accounts are we not getting IDs for, in the monthly aggregated event table
    select count(1) sample_count
      from VESPA_Comscore_SkyGo_201408
     where sam_profileid is null


    --Last data load details
    select max(latest_data_utc) latest_data_received_utc,
           max(last_load_date) last_data_load_date
      from VESPA_Comscore_universe_tbl




--### good profileIDs to use for testing
-- sam_profileid = 10015451     --trying to remove orphaned events... ones without start or end times...
--                 10186413     --end event missing
--                 10006184     --start/end missing

--viewing_event_end_utc = '2014-08-01 14:55:41.000000'

--raw
select top 100 *
from Comscore_201408
where sam_profileid = 10006184
order by ns_ts, ns_st_ec

commit
select top 100 *
from V239_comscore_event_tmp3
where sam_profileid = 10006184
--and ns_st_ci = '31afbb41112e6410VgnVCM1000000b43150a____'
--and ns_ts = '2014-08-01 14:55:35.000000'
order by ns_ts_raw, next_ns_ts
order by uniqid

--end events set here
commit
select top 100 *
from V239_comscore_event_tmp2
where sam_profileid = 10006184
order by uniqid

--strips out events like end - pause, pause - end
select top 100 *
from V239_comscore_event_tmp
where sam_profileid = 10087341
order by uniqid



--ok so this stage has dropped a valid end event... :(
commit
select top 100 *
from V239_comscore_view2
where sam_profileid = 10006184

commit
select top 100 *
from V239_comscore_view
where sam_profileid = 10006184

commit
select *
from VESPA_SkyGo_Comscore_201408
where sam_profileid = 10029366

select top 1000 *
from VESPA_SkyGo_Comscore_201408

select top 1000 *
from VESPA_SkyGo_Comscore_201408
where account_number is not null



select *
from VESPA_SkyGo_Comscore_201408
where viewing_event_start_utc is null


*/

--overview of durations
select sam_profileid,
       station_name,
       count(distinct coalesce(vod_asset_id, cast(dk_programme_instance_dim as varchar(40)) )) distinct_progs,
       count(coalesce(vod_asset_id, cast(dk_programme_instance_dim as varchar(40)) )) programme_count,
       count(ad_asset_id) ad_impressions,
       sum(case when stream_context = 'vod' then duration_viewed else 0 end) vod_duration,
       sum(case when stream_context = 'dvod' then duration_viewed else 0 end) dvod_duration,
       sum(case when stream_context = 'lin' then duration_viewed else 0 end) lin_duration,
       sum(duration_viewed) total_duration
from VESPA_SkyGo_Comscore_201408
group by sam_profileid, station_name






select distinct ns_st_ci over()
Comscore_201408



-drop table VESPA_SkyGo_Comscore_201408
commit
*/











--- Run LOG
commit
select top 1000 *
  from VESPA_Comscore_SkyGo_log_tbl
 order by log_datetime asc

--log maintenance
delete VESPA_Comscore_SkyGo_log_tbl
where log_datetime < '2014-10-08 09:32:13.828297'
commit



--processing table
select top 1000 *
from V239_comscore_event_tmp4   ... _tmp

select top 100 *
from V239_comscore_view2

select top 100 *
from V239_comscore_view
--- end processing tables


--final table
select top 100 *
from VESPA_SkyGo_Comscore_201408
where account_number is not null
and instance_flag = 1
order by aggr_event_id, broadcast_start_date_Time_local





grant select on VESPA_Comscore_audit_stats_tbl to thompsonja
grant select on VESPA_Comscore_audit_run_tbl to thompsonja

grant select on VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl to thompsonja

grant select on VESPA_Comscore_universe_tbl to thompsonja

grant select on VESPA_SkyGo_Comscore_201408 to thompsonja











