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
# 07/11/2016  ABA   Made the run_build procedure, and viewing create script seperate. 
# 16/11/2016  ABA   Changed round the order of execution code to trigger channel mapping before the raw events are manipulated
#
###############################################################################*/


CREATE or REPLACE procedure barbera.vespa_comscore_skygo_run_build(
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

    DECLARE @build_start_datetime datetime
        SET @build_start_datetime = dateformat(now(),'YYYY-MM-DD HH:mm:ss')


--tempory fix.. as Olive is shocking sometimes
 create or replace VIEW barbera.comscore_union_view as
        select * from sk_prod.comscore_201508
          union all
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


    /* code_location_A01 ***********************************************************
     *****                                                                        **
     *****     First prepare the information about this load and identify         **
     *****     if ok to load                                                      **
     *****                                                                        **
     *******************************************************************************/

    UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_started = 1
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
    commit

    EXEC barbera.VESPA_Comscore_SkyGo_log 'Build Start <'||@data_run_date||'>'

    EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Preparing stats'


----------------------
/*
commit
    DECLARE @data_run_date date
    DECLARE @suppress_stats_universe_build bit
    DECLARE @lower_limit integer
    DECLARE @stddev_over_rows integer
    DECLARE @weighting_rows integer

    --set the date we are running this for
    SET @data_run_date = '2015-01-02'
    SET @suppress_stats_universe_build = 0

    --set variables for the monitoring stats (calculating expected data volumes from the underlaying data)
    SET @lower_limit = 200000 -- floor at which an alert is triggered regardless of other stats (number of expected accounts)
    SET @stddev_over_rows = 7 -- how many days the stddev of accounts is calculated over
    SET @weighting_rows = 4   -- how many days are used to exponentially weight the moving average
------
*/


    EXEC barbera.vespa_comscore_skygo_event_view_prep @data_run_date, @lower_limit, @stddev_over_rows, @weighting_rows

    if @@error = 18001 BEGIN return raiserror 18001 'No File Supplied' END


    /* code_location_A02 *************************************************************************
     *****                                                                                      **
     *****      Prepare Universe information                                                    **
     *****                                                                                      **
     *********************************************************************************************/
    IF @suppress_universe_build != 1
        BEGIN
            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Creating universe information'

            exec barbera.vespa_comscore_skygo_universe_build @data_run_date

/*
            IF object_id('VESPA_Comscore_SkyGo_universe_tbl') IS NOT NULL
                BEGIN
                    drop table VESPA_Comscore_SkyGo_universe_tbl
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
              into VESPA_Comscore_SkyGo_universe_tbl
              from COMSCORE_UNION_VIEW c --using the union view
                   left join VESPA_Comscore_SkyGo_SAV_summary_tbl sav --combine with SAV
                     on c.sam_profileid = sav.sam_profileid
             group by c.sam_profileid, sav.account_number, sav.cb_key_household, sav.exclude_flag, sav.aggregate_flag

            commit

            create index VESPA_Comscore_universe_sam_idx on VESPA_Comscore_SkyGo_universe_tbl(sam_profileid)
            commit
*/

        END
     ELSE
        BEGIN
            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Suppressed Universe Build'
        END -- end @supress_stats_universe_build if = 1



    /* code_location_B01 *************************************************************************
     *****                                                                                      **
     *****      Halt the build process and raise error if the raw data looks wrong              **
     *****                                                                                      **
     *********************************************************************************************/

    --check/QA to see if we can run the process for the required day
    SELECT @status = qa_status
      from barbera.VESPA_Comscore_SkyGo_audit_run_tbl r
    where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    IF @status not in ('ok','low')
        BEGIN
            -- set processed information to flag issue
            UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
               SET basic_view_created = -1
             where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Process failed['||@status||']'

            RAISERROR 50001 'data volume is '||@status
        END
    ELSE
        BEGIN
            --reset any previous attempts
            UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
               SET basic_view_created = 0
             where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')
            commit


            /* code_location_G01 *************************************************************************
             *****                                                                                      **
             *****                      Create Channel Mapping Information                              **
             *****                                                                                      **
             *********************************************************************************************/

            EXEC barbera.vespa_comscore_skygo_channelmap_build  @data_run_date   

            


            /* code_location_C01 *************************************************************************
             *****                                                                                      **
             *****      Start the conversion of raw data into event aggregate.                          **
             *****      First - extract the local day's period keys from VESPA_CALENDAR                 **
             *****                                                                                      **
             *********************************************************************************************/

--DECLARE @xsql               varchar(10000)
--DECLARE @monthly_table_name varchar(300)
--declare @data_run_date       DATE
--set @data_run_date = '2016-06-01'

            EXEC barbera.vespa_comscore_skygo_event_view_create @data_run_date



            --Apply Channel mapping logic to clean up the view thats just been created
            SET @monthly_table_name = 'barbera.VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')
            execute('UPDATE '||@monthly_table_name||' v
                        SET v.station_name = cm.channel_name,
                            v.service_key  = cm.service_key
                       FROM barbera.VESPA_Comscore_SkyGo_Channel_Mapping_tbl cm
                      WHERE v.station_name = cm.station_name
                     commit ')




            /* code_location_H01 *************************************************************************
             *****                                                                                      **
             *****             Split Linear Events into Programme Instances                             **
             *****                                                                                      **
             *********************************************************************************************/

 --params for running code from this point...
--this small section (to the end of the proc) can be run to manually run the conversion of linear instances
/*DECLARE @data_run_date date
DECLARE @xsql               varchar(8000)
DECLARE @monthly_table_name varchar(48)
SET @data_run_date = '2015-01-18'

*/
SET @monthly_table_name = 'VESPA_Comscore_SkyGo_'||dateformat(@data_run_date, 'YYYYMM')

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Splitting linear viewing events'

            --iterates through the aggregated linear events, splitting each one into programme instances
            EXEC barbera.vespa_comscore_skygo_linear_split @data_run_date, 0

            EXEC barbera.VESPA_Comscore_SkyGo_log ' <'||@data_run_date||'> proc complete with error code <'||@@error||'>'

            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed linear instance derivations'

            --update linear_events_split flag in audit_run_tbl
            execute('
              Update barbera.VESPA_Comscore_SkyGo_audit_run_tbl
                 SET linear_events_split = 1
               WHERE data_date = '''||@data_run_date||'''
              commit
             ')



            /* code_location_H02 *************************************************************************
             *****                                                                                      **
             *****        Record stats around splitting linear events into Programme Instances          **
             *****                                                                                      **
             *********************************************************************************************/


            EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Updating linear stats'

            SET @xsql = '
                        SELECT sum(case when linear_instance_flag = 1 then 1 else 0 end)  linear_event_instances,
                               sum(case when linear_instance_flag = 0 then 1 else 0 end) aggr_lin_not_split
                          into barbera.Comscore_skygo_day_stats_tmp_b
                          from '||@monthly_table_name||' a
                         where a.data_date_local = '''||@data_run_date||'''
                           and stream_context = ''lin''
                         group by a.data_date_local

                        UPDATE barbera.VESPA_Comscore_SkyGo_audit_stats_tbl s
                           SET s.linear_event_instances  = a.linear_event_instances,
                               s.aggr_lin_not_split = a.aggr_lin_not_split
                          from barbera.Comscore_skygo_day_stats_tmp_b a
                        WHERE s.data_date = '''||@data_run_date||'''

                          drop table barbera.Comscore_skygo_day_stats_tmp_b

                        commit
                        '
            EXECUTE(@xsql)


          /* code_location_I01 ***************************************************************************
           *****                                                                                      **
           *****       Scan the day's records and make corrections to missing or incorrect data       **
           *****                                                                                      **
           *********************************************************************************************/


          -- we should log these updates/deletes for data quality purposes


          EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Applying adjustments to impossible viewing events'

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


          --set erroneous data flag when view_continuing_flag is 1 (as the first event of the day was an 'end' event) but the end event
          --    happend after the first hour of the day leading to a start hour > 0 (which is not possible for a view_continuing event)
          SET @xsql = 'UPDATE '||@monthly_table_name||'
                          SET erroneous_data_suspected_flag = 1
                        where view_continuing_flag = 1
                          and datepart(hh, viewing_event_start_local) != 0  --client clock could be wrong, but this makes it erroneous anyway..
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
        
        EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Refreshing union view'
 
        declare @current_month varchar(6)
        declare @months        integer
        declare @max_rank      integer
        declare @month_exists  bit

        IF object_id('barbera.Comscore_SkyGo_union_months_tmp')   IS NOT NULL   BEGIN  drop table barbera.Comscore_SkyGo_union_months_tmp    END
        IF object_id('barbera.Comscore_SkyGo_union_months_tmp_b') IS NOT NULL   BEGIN  drop table barbera.Comscore_SkyGo_union_months_tmp_b  END


        --create months list that should be available
        select dateformat(data_date, 'YYYYMM') months, dense_rank() over(order by months desc) rank, null new_rank   -- order by months desc. changed [2016-07-29]
          into barbera.Comscore_SkyGo_union_months_tmp
          from barbera.VESPA_Comscore_SkyGo_audit_run_tbl
         where build_completed = 1                            --only add the months in this list that have built correctly. Changed [2016-07-29] to use build_completed rather than build_success
         group by dateformat(data_date, 'YYYYMM')
        commit 

        --verify that the months are available
        /*
        select @months = max(rank)
          from barbera.Comscore_SkyGo_union_months_tmp
         where rank <= 13    --- restrict the maximum number of months in this view
        */

        

        --iterate through each month, if the table doesn't exist, delete it from the list
        set @months = 1

        select @max_rank = max(rank)
         from barbera.Comscore_SkyGo_union_months_tmp

        while(@months<=@max_rank) -- Method changed to count up (rather than down) to 13 [2016-07-29]
            BEGIN
                select @current_month = months
                  from barbera.Comscore_SkyGo_union_months_tmp
                 where rank = @months

                IF object_id('barbera.VESPA_Comscore_SkyGo_'||@current_month) IS NULL 
                        AND @current_month IS NOT NULL -- this condition deletes the month entry if the @current_month didnt exists - not all ranks exist
                    BEGIN
                        delete barbera.Comscore_SkyGo_union_months_tmp
                         where months = @current_month
                        commit
                    END
                SET @months = @months + 1  --limit to 13 months. Method changed to count up (rather than down) to 13 [2016-07-29]
            END


        

        --see if month exists in the listing
        select @month_exists = max(case when dateformat(@data_run_date, 'YYYYMM') = months then 1 else 0 end)  --modified to use max [2016-07-29]
          from barbera.Comscore_SkyGo_union_months_tmp
 
        --if the month doesn't exist, add it.. This happens when loading records for a new month
        IF @month_exists = 0
          BEGIN
            --set location of last record
            select @max_rank = max(rank)
              from barbera.Comscore_SkyGo_union_months_tmp

            INSERT into barbera.Comscore_SkyGo_union_months_tmp(months, rank, new_rank)
                 values(dateformat(@data_run_date, 'YYYYMM') , @max_rank+1, null )
            commit     
          END
          


        --as some months may have been missing (so deleted), give the order a new_rank
        select months, rank, dense_rank() over(order by months) new_rank
          into barbera.Comscore_SkyGo_union_months_tmp_b
          from barbera.Comscore_SkyGo_union_months_tmp
         group by months, rank
        commit
 
        --create union view of all the verified months
        select @months = count(1)
          from barbera.Comscore_SkyGo_union_months_tmp_b
        
        IF @months > 13 
          BEGIN  
             SET @months = 13 --limit to 13 months. Cap applied here rather than earlier [2017-02-27]
          END
  
        IF @months > 0
            BEGIN                
                declare @union_view_str varchar(1000)
                set @union_view_str = 'create or replace view barbera.VESPA_Comscore_SkyGo_Union_View as ('

                --iterate through each month, if the table doesn't exist delete it from the list
                while(@months>0)
                    BEGIN
                        select @current_month = months
                          from barbera.Comscore_SkyGo_union_months_tmp_b
                         where new_rank = @months

                        IF object_id('barbera.VESPA_Comscore_SkyGo_'||@current_month)   IS NOT NULL
                            BEGIN
                                SET @union_view_str = @union_view_str||' select * from barbera.VESPA_Comscore_SkyGo_'||@current_month
                                SET @months = @months - 1
                             END
                        ELSE
                            BEGIN
                               SET @months = @months - 1
                            END
                        IF @months > 0
                            BEGIN
                               SET @union_view_str = @union_view_str||' union all '
                            END

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

        EXEC barbera.VESPA_Comscore_SkyGo_log '  <'||@data_run_date||'> Completed Build'



        /* code_location_L01 *************************************************************************
         *****                                                                                      **
         *****                       Drop tables no longer required                                 **
         *****                                                                                      **
         *********************************************************************************************/


        IF object_id('barbera.VESPA_CALENDAR_section_tmp')          IS NOT NULL   BEGIN  drop table barbera.VESPA_CALENDAR_section_tmp          commit  END    
        IF object_id('barbera.V239_comscore_event_tmp5')            IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp5            commit  END
        IF object_id('barbera.V239_comscore_event_tmp4')            IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp4            commit  END
        IF object_id('barbera.V239_comscore_event_tmp3')            IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp3            commit  END
        IF object_id('barbera.V239_comscore_event_tmp2')            IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp2            commit  END
        IF object_id('barbera.V239_comscore_event_tmp')             IS NOT NULL   BEGIN  drop table barbera.V239_comscore_event_tmp             commit  END
        IF object_id('barbera.V239_comscore_view2')                 IS NOT NULL   BEGIN  drop table barbera.V239_comscore_view2                 commit  END
        IF object_id('barbera.V239_comscore_view')                  IS NOT NULL   BEGIN  drop table barbera.V239_comscore_view                  commit  END
        IF object_id('barbera.Comscore_SkyGo_union_months_tmp_b')   IS NOT NULL   BEGIN  drop table barbera.Comscore_SkyGo_union_months_tmp_b   commit  END
        IF object_id('barbera.Comscore_SkyGo_union_months_tmp')     IS NOT NULL   BEGIN  drop table barbera.Comscore_SkyGo_union_months_tmp     commit  END
        
    END --end of wrapped @status not in ('ok','low') check




    /* code_location_M01 ********************************************************************
     *****                                                                                 **
     *****                       Record run success                                        **
     *****                                                                                 **
     ****************************************************************************************/

    DECLARE @build_end_datetime datetime
    DECLARE @build_success      BIT
        SET @build_end_datetime = now()

    UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_completed = 1
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

     UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_completed_date = @build_end_datetime
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_duration_secs = datediff(ss, @build_start_datetime, @build_end_datetime)
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    select @build_success = case when build_started       = 1
                                  and basic_view_created  = 1
                                  and channels_mapped     = 1
                                  and linear_events_split = 1
                                  and build_completed     = 1
                                 then 1 else 0
                             end
      from barbera.VESPA_Comscore_SkyGo_audit_run_tbl
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    UPDATE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
       SET build_success = @build_success
     where data_date = dateformat(cast(@data_run_date as date), 'YYYY-MM-DD')

    commit

END; 




--------------------------------------------------------------------------------
--#####################     END     ############################################
--##############################################################################
--##############################################################################
--##############################################################################




