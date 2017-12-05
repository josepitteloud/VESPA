-- Recalibrating the capping builds. Heh, we need to mix the scaling into this
-- as well, awesomes... It wants the minute allocation too, but that's not ready
-- yet. After we run that batch over the historc cache we've t, then we'll be
-- okay. Oh but we'll have to do that multiple times because we still need to
-- rebuild the entire capped cache with the recalibrated capping rules, awesome.

-- So we need to cut this stuff up by event start hour, consumption hour, channel
-- and timeshifting. And we want average number of subscribers on each channel
-- over the hour and also the average time people watching any of this channel
-- spend on it. So it loks like there are ing to be a few intermediate tables
-- before we can line stuff up with the BARB totals, but that's okay.

-- Okay, to split stuff up into consumption hours, we're ing to join (causing
-- duplicates) into this kind of index table thing


-- execute V079_result_calculation_aug '2012-10-03';



if object_id('V079_result_calculation_aug') is not null then drop procedure V079_result_calculation_aug endif;


create procedure V079_result_calculation_aug
    @scanning_day        date = NULL      -- Date of daily table caps to cache
as
begin

    declare @varSql               varchar(15000)


    if object_id('V079_time_periods') is not null drop table V079_time_periods
    create table V079_time_periods (
         viewing_period         varchar(10) default null
        ,period_start_time      datetime default null
        ,period_end_time        datetime default null
    )
    create dttm index idx13 on V079_time_periods(period_start_time)
    create dttm index idx14 on V079_time_periods(period_end_time)

    insert into V079_time_periods values ('00 - 03', @scanning_day || ' 00:00:00.000', @scanning_day || ' 04:00:00.000')
    insert into V079_time_periods values ('04 - 05', @scanning_day || ' 04:00:00.000', @scanning_day || ' 06:00:00.000')
    insert into V079_time_periods values ('06 - 09', @scanning_day || ' 06:00:00.000', @scanning_day || ' 10:00:00.000')
    insert into V079_time_periods values ('10 - 14', @scanning_day || ' 10:00:00.000', @scanning_day || ' 15:00:00.000')
    insert into V079_time_periods values ('15 - 19', @scanning_day || ' 15:00:00.000', @scanning_day || ' 20:00:00.000')
    insert into V079_time_periods values ('20 - 22', @scanning_day || ' 20:00:00.000', @scanning_day || ' 23:00:00.000')
    insert into V079_time_periods values ('23 - 23', @scanning_day || ' 23:00:00.000', @scanning_day + 1 || ' 00:00:00.000')



    if object_id('V079_pull_cache') is not null drop table V079_pull_cache
    create table V079_pull_cache (
        cb_row_id               bigint
        ,subscriber_id          bigint
        ,account_number         varchar(20)
        ,scanning_date          date
        ,viewing_starts_utc     datetime
        ,viewing_stops_utc      datetime
        ,viewing_starts         datetime
        ,viewing_stops          datetime
        ,service_key            int
        ,service_id             int
        ,channel                varchar(25)
        ,timeshifting           varchar(12)

        ,viewing_period         varchar(10) default null
        ,viewing_day            date default null

        ,period_start_time      datetime default null
        ,period_end_time        datetime default null

        ,day_start_time         datetime default null
        ,day_end_time           datetime default null
    )
    create hg index idx01 on V079_pull_cache(cb_row_id)
    create hg index idx02 on V079_pull_cache(subscriber_id)
    create hg index idx03 on V079_pull_cache(account_number)
    create date index idx04 on V079_pull_cache(scanning_date)
    create dttm index idx05 on V079_pull_cache(viewing_starts)
    create dttm index idx06 on V079_pull_cache(viewing_stops)
    create hg index idx07 on V079_pull_cache(service_key)
    create hg index idx08 on V079_pull_cache(service_id)
    create lf index idx09 on V079_pull_cache(channel)
    create lf index idx10 on V079_pull_cache(timeshifting)
    create lf index idx11 on V079_pull_cache(viewing_period)
    create date index idx12 on V079_pull_cache(viewing_day)
    create dttm index idx13 on V079_pull_cache(period_start_time)
    create dttm index idx14 on V079_pull_cache(period_end_time)
    create dttm index idx15 on V079_pull_cache(day_start_time)
    create dttm index idx16 on V079_pull_cache(day_end_time)



    if object_id('V079_aggregate_collection') is not null drop table V079_aggregate_collection
    create table V079_aggregate_collection (
        aggregate_type          varchar(50)
        ,min_viewing_starts     datetime
        ,max_viewing_starts     datetime
        ,total_seconds          bigint
        ,households             bigint
    )

    create lf index idx2 on V079_aggregate_collection (aggregate_type)



      -- ###########################################################################################
      -- ######  Fill cache table with relevant events                                        ######
      -- ###########################################################################################
    set @varSql = '
            delete from V079_pull_cache
            commit

            insert into V079_pull_cache
            select
                da.cb_row_id                                                                -- cb_row_id
                ,da.subscriber_id                                                           -- subscriber_id
                ,da.account_number                                                          -- account_number
                ,''##^!!1!!^##''                                                            -- scanning_date
                ,da.viewing_starts as viewing_starts_utc                                    -- viewing_starts
                ,da.viewing_stops as viewing_stops_utc                                      -- viewing_stops

                ,case
                    when (da.viewing_starts <  ''2012-03-25 01:00:00'') then da.viewing_starts                      -- prior Mar 12 - no change, consider UTC = local
                    when (da.viewing_starts <  ''2012-10-28 02:00:00'') then dateadd(hour, 1, da.viewing_starts)    -- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
                    when (da.viewing_starts <  ''2013-03-31 01:00:00'') then da.viewing_starts                      -- Oct 12-Mar 13 => UTC = Local
                    when (da.viewing_starts <  ''2013-10-27 02:00:00'') then dateadd(hour, 1, da.viewing_starts)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                    when (da.viewing_starts <  ''2014-03-30 01:00:00'') then da.viewing_starts                      -- Oct 13-Mar 14 => UTC = Local
                      else NULL                                                                                     -- the scrippt will have to be updated past Mar 2014
                 end as viewing_starts                                                     -- viewing_starts

                ,case
                    when (da.viewing_stops <  ''2012-03-25 01:00:00'') then da.viewing_stops                        -- prior Mar 12 - no change, consider UTC = local
                    when (da.viewing_stops <  ''2012-10-28 02:00:00'') then dateadd(hour, 1, da.viewing_stops)      -- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
                    when (da.viewing_stops <  ''2013-03-31 01:00:00'') then da.viewing_stops                        -- Oct 12-Mar 13 => UTC = Local
                    when (da.viewing_stops <  ''2013-10-27 02:00:00'') then dateadd(hour, 1, da.viewing_stops)      -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                    when (da.viewing_stops <  ''2014-03-30 01:00:00'') then da.viewing_stops                        -- Oct 13-Mar 14 => UTC = Local
                      else NULL                                                                                     -- the scrippt will have to be updated past Mar 2014
                 end as viewing_stops                                                       -- viewing_stops

                ,epg.service_key                                                            -- service_key
                ,epg.service_id                                                             -- service_id
                ,null                                                                       -- channel - getting channel name instead from the Techedge lookup
                ,da.timeshifting                                                            -- timeshifting
                ,null                                                                       -- viewing_period
                ,null                                                                       -- viewing_day
                ,null                                                                       -- period_start_time
                ,null                                                                       -- period_end_time
                ,null                                                                       -- day_start_time
                ,null                                                                       -- day_end_time

              from vespa_analysts.Vespa_Daily_Augs_##^!!2!!^## as da,
                   sk_prod.vespa_programme_schedule as epg
             where da.programme_trans_sk = epg.dk_programme_instance_dim
               and timeshifting in (''LIVE'', ''VOSDAL'', ''PLAYBACK7'')
    '

    execute(replace(replace(@varSql,
                            '##^!!2!!^##',
                            dateformat(@scanning_day,'yyyymmdd')
                            ),
                    '##^!!1!!^##',
                    dateformat(@scanning_day,'yyyy-mm-dd')
                    )
           )
    commit


      -- ###########################################################################################
      -- ######  Populate missing fields with relevant data                                   ######
      -- ###########################################################################################
      -- All time based fields are based on local time
    update V079_pull_cache
       set viewing_period     = case
                                  when (hour(viewing_starts) between  0 and  3) then '00 - 03'
                                  when (hour(viewing_starts) between  4 and  5) then '04 - 05'
                                  when (hour(viewing_starts) between  6 and  9) then '06 - 09'
                                  when (hour(viewing_starts) between 10 and 14) then '10 - 14'
                                  when (hour(viewing_starts) between 15 and 19) then '15 - 19'
                                  when (hour(viewing_starts) between 20 and 22) then '20 - 22'
                                  when (hour(viewing_starts) between 23 and 23) then '23 - 23'
                                    else '???'
                               end,

           viewing_day        = date(viewing_starts),

           period_start_time  = case
                                  when (hour(viewing_starts) between  0 and  3) then dateformat(viewing_starts,'yyyy-mm-dd 00:00:00')
                                  when (hour(viewing_starts) between  4 and  5) then dateformat(viewing_starts,'yyyy-mm-dd 04:00:00')
                                  when (hour(viewing_starts) between  6 and  9) then dateformat(viewing_starts,'yyyy-mm-dd 06:00:00')
                                  when (hour(viewing_starts) between 10 and 14) then dateformat(viewing_starts,'yyyy-mm-dd 10:00:00')
                                  when (hour(viewing_starts) between 15 and 19) then dateformat(viewing_starts,'yyyy-mm-dd 15:00:00')
                                  when (hour(viewing_starts) between 20 and 22) then dateformat(viewing_starts,'yyyy-mm-dd 20:00:00')
                                  when (hour(viewing_starts) between 23 and 23) then dateformat(viewing_starts,'yyyy-mm-dd 23:00:00')
                                    else null
                                end,

           period_end_time    = case
                                  when (hour(viewing_starts) between  0 and  3) then dateformat(viewing_starts,'yyyy-mm-dd 04:00:00')
                                  when (hour(viewing_starts) between  4 and  5) then dateformat(viewing_starts,'yyyy-mm-dd 06:00:00')
                                  when (hour(viewing_starts) between  6 and  9) then dateformat(viewing_starts,'yyyy-mm-dd 10:00:00')
                                  when (hour(viewing_starts) between 10 and 14) then dateformat(viewing_starts,'yyyy-mm-dd 15:00:00')
                                  when (hour(viewing_starts) between 15 and 19) then dateformat(viewing_starts,'yyyy-mm-dd 20:00:00')
                                  when (hour(viewing_starts) between 20 and 22) then dateformat(viewing_starts,'yyyy-mm-dd 23:00:00')
                                  when (hour(viewing_starts) between 23 and 23) then dateformat(viewing_starts + 1,'yyyy-mm-dd 00:00:00')
                                    else null
                                end,

           day_start_time     = dateformat(viewing_starts,'yyyy-mm-dd 00:00:00'),

           day_end_time       = dateformat(viewing_starts + 1,'yyyy-mm-dd 00:00:00')
    commit

    delete from V079_pull_cache
     where viewing_day <> @scanning_day
    commit


      -- And also, we need to stitch in the channel mapping bit too: we've t a new
      -- construction from Martin to help with the channel mapping, it gets built over
      -- in "V079 targeted channel mapping.sql". For live stuff the link is service key...
    /*
    update V079_pull_cache
       set channel = tcl.techedge_name
      from V079_pull_cache inner join V079_Techedge_Channel_Lookup as tcl
        on V079_pull_cache.service_key = tcl.service_key
     where timeshifting = 'LIVE'
    commit

    -- For timeshifted stuff, the link is service id (part of that tripple key but the other
    -- flags don't indicate anything about the channel)
    update V079_pull_cache
       set channel = tcl.techedge_name
      from V079_pull_cache inner join V079_Techedge_Channel_Lookup as tcl
        on V079_pull_cache.service_id = tcl.service_id
     where timeshifting <> 'LIVE'
    commit

    -- OK, and now, we can clip out things that don't belong either on the panel or to the
    -- channels of calibratory interest:
    delete from V079_pull_cache
     where channel is null
    commit
    */

      -- ###########################################################################################
      -- ######  Creating aggregated view by DAY & TIME PERIOD                                ######
      -- ###########################################################################################
    delete from V079_aggregate_collection
    commit


      -- ############## FOR THE ENTIRE DAY ##############
    insert into V079_aggregate_collection
    select
          'Daily ' || ' [' || timeshifting || '] - ' || dateformat(viewing_day, 'yyyy-mm-dd'),
          min(viewing_starts),                                                              -- min_viewing_starts
          max(viewing_starts),                                                              -- max_viewing_startss
          sum(                                                                              -- total_seconds
                -- And in here lives the calculation for viewing within the hour of interest:
              datediff(
                  second
                  ,case
                      when (viewing_starts >= day_start_time) and (viewing_starts <= day_end_time)   then viewing_starts
                      when (viewing_starts <  day_start_time) and (viewing_stops  >= day_start_time) then day_start_time
                        else null
                   end
                  ,case
                      when (viewing_stops >= day_start_time) and (viewing_stops  <= day_end_time) then viewing_stops
                      when (viewing_stops >= day_end_time)   and (viewing_starts <= day_end_time) then day_end_time
                        else null
                   end
              )
            ),
          count(distinct case                                                               -- households
                           when (viewing_starts <= day_end_time) and (viewing_stops >= day_start_time) then account_number
                             else null
                         end
               )
      from V079_pull_cache
     group by viewing_day, timeshifting
     order by 1
    commit


      -- ############## BY TIME PERIOD ##############
    insert into V079_aggregate_collection
    select
          'Hourly ' || ' [' || a.timeshifting || '] - ' || dateformat(a.viewing_day, 'yyyy-mm-dd') || ' (' || b.viewing_period || ')',
          min(a.viewing_starts),                                                            -- min_viewing_starts
          max(a.viewing_starts),                                                            -- max_viewing_startss
          sum(                                                                              -- total_seconds
                -- And in here lives the calculation for viewing within the hour of interest:
              datediff(
                  second
                  ,case
                      when (a.viewing_starts >= b.period_start_time) and (a.viewing_starts <= b.period_end_time)   then a.viewing_starts
                      when (a.viewing_starts <  b.period_start_time) and (a.viewing_stops  >= b.period_start_time) then b.period_start_time
                        else null
                   end
                  ,case
                      when (a.viewing_stops >= b.period_start_time) and (a.viewing_stops  <= b.period_end_time) then a.viewing_stops
                      when (a.viewing_stops >= b.period_end_time)   and (a.viewing_starts <= b.period_end_time) then b.period_end_time
                        else null
                   end
              )
            ),
          count(distinct case                                                               -- households
                           when (a.viewing_starts <= b.period_end_time) and (a.viewing_stops >= b.period_start_time) then a.account_number
                             else null
                         end
               )
      from V079_pull_cache a,
           V079_time_periods b
     where a.viewing_starts < b.period_end_time
       and a.viewing_stops >= b.period_start_time
     group by a.viewing_day, b.viewing_period, a.timeshifting
     order by 1
    commit


      -- ###########################################################################################
      -- ######  Rename table to maintain history                                             ######
      -- ###########################################################################################

    set @varSql = '
                      if object_id(''V079_Res__Aggr_Results_' || dateformat(@scanning_day, 'yyyymmdd') || ''') is not null drop table V079_Res__Aggr_Results_' || dateformat(@scanning_day, 'yyyymmdd') || '
                  '
    execute(@varSql)

    -- set @varSql = '
    --                   if object_id(''V079_Res__Raw_Data_' || dateformat(@scanning_day, 'yyyymmdd') || ''') is not null drop table V079_Res__Raw_Data_' || dateformat(@scanning_day, 'yyyymmdd') || '
    --               '
    -- execute(@varSql)



    set @varSql = '
                      alter table V079_aggregate_collection rename V079_Res__Aggr_Results_' || dateformat(@scanning_day, 'yyyymmdd') || '
                  '
    execute(@varSql)

    if object_id('V079_pull_cache') is not null drop table V079_pull_cache

    -- set @varSql = '
    --                   alter table V079_pull_cache rename V079_Res__Raw_Data_' || dateformat(@scanning_day, 'yyyymmdd') || '
    --               '
    -- execute(@varSql)

    commit




end;
go