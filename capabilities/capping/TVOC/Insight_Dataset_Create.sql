       declare @dp_tname       varchar(50)
        declare @from_dt        integer
        declare @to_dt          integer
        declare @first_date       datetime
       declare @process_date           datetime
       declare @i              integer
        declare @numdays        integer
        declare @seasonloop     integer


        set @numdays = 14
--- this the number of days in a season loop

        select @i=0
        select @seasonloop=1

--- there will be two season loops - Summer and Winter


        while @i<@numdays and @seasonloop<3
---  loop through day by day, collecting and aggregating viewing event data
begin
        if @seasonloop = 1
                set @first_date='20151106'      ----- start of winter data
            else
                set @first_date='20150710'                     ----- start of summer data (includes some ashes tests)

        select @process_date =DateAdd(day, @i, @first_date)
--- get the date to be processed on this loop

        select  @from_dt        = cast((dateformat(@process_date,'YYYYMMDD')||'00') as integer)
        select  @to_dt          = cast((dateformat(@process_date,'YYYYMMDD')||'23') as integer)
--- set beginning and end times for event starts for the process date

-------------------------------
-- Creating event level data for a day
-------------------------------

--- first create the event data table for the day

    if object_id ('TVOC_capping_data_tempshelf') is not null
        begin
            drop table TVOC_capping_data_tempshelf
            MESSAGE cast(now() as timestamp)||' | TVOC  - Tempshelf table dropped' TO CLIENT
        end
        if object_id('TVOC_capping_data_tempshelf') is null
        begin

                MESSAGE cast(now() as timestamp)||' | @ TVOC 0.2: Creating Table TVOC_capping_data_tempshelf' TO CLIENT

                create table TVOC_capping_data_tempshelf(

                        account_number          varchar(20) not null
                        ,subscriber_id          decimal(10) not null
                        ,event_id                       bigint          default null
                        ,event_Start_utc        timestamp       not null
                        ,event_end_utc          timestamp       not null
                        ,capping_process_flag   smallint
                        ,programme_genre        varchar(20)     default null
                        ,session_daypart        varchar(11)     default null
                        ,channel_pack           varchar(40) default null
                        ,event_start_dim        int                     not null            --exists
                        ,event_end_dim          int                     not null            --exists
                        ,event_capped_duration  int
                        ,event_uncapped_duration    int
                        ,event_start_month      smallint
                        ,event_start_dow        smallint
                        ,event_start_hour       smallint
                        ,long_event_flag_120       smallint
                        ,long_event_flag_150       smallint
                        ,long_event_flag_180       smallint
                        ,long_event_flag_210       smallint
                        ,long_event_flag_240       smallint
                        ,View_Type              varchar(20)     default null
                        ,channel_name           varchar(50)     default null
                        ,service_key            integer
                        ,PS_flag                varchar(8)      default 'unknown'
                )

                commit

                create hg index hg1     on  TVOC_capping_data_tempshelf(account_number)
                create hg index hg2     on  TVOC_capping_data_tempshelf(subscriber_id)
                create hg index hg3     on  TVOC_capping_data_tempshelf(event_id)
                create hg index hg4     on TVOC_capping_data_tempshelf(event_start_dim)
                create hg index hg5     on TVOC_capping_data_tempshelf(event_end_dim)
                create hg index hg6     on TVOC_capping_data_tempshelf(service_key)
                create dttm index dttm1 on TVOC_capping_data_tempshelf(event_start_utc)
                create dttm index dttm2 on TVOC_capping_data_tempshelf(event_end_utc)

                commit



                grant all privileges on TVOC_capping_data_tempshelf to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ TVOC 0.2: Creating Table TVOC_capping_data_tempshelf DONE' TO CLIENT

        end


-----------------------------------------
-- M07.1 - Compacting Data at Event level
-----------------------------------------

                MESSAGE cast(now() as timestamp)||' | @ Beginning  TVOC 1.1 - Compacting Data at Event level' TO CLIENT

--- now create the event data.  note that there are two identiacl pieces of code here (apart from dates used)
--- one for summer and one for winter.  this could have been written using dynamic sql - but this works!




if @seasonloop=1
begin
--- WINTER LOOP
insert into TVOC_capping_data_tempshelf
(
 account_number
    ,subscriber_id
    ,event_id
    ,event_start_utc
    ,event_end_utc
    ,capping_process_flag
    ,event_start_dim
    ,event_end_dim
    ,event_capped_duration
    ,event_uncapped_duration
    ,event_start_month
    ,event_start_dow
    ,event_start_hour
    ,long_event_flag_120
    ,long_event_flag_150
    ,long_event_flag_180
    ,long_event_flag_210
    ,long_event_flag_240
    ,View_Type
    ,service_key
    ,programme_genre
    ,PS_flag
  )
select
    base.account_number
    ,base.subscriber_id
    ,base.event_id
    ,base.event_start_utc
    ,base.event_end_utc
    ,base.capping_process_flag
    ,base.event_start_dim
    ,base.event_end_dim
    ,base.event_capped_duration
    ,base.event_uncapped_duration
    ,event_start_month
    ,event_start_dow
    ,event_start_hour
    ,long_event_flag_120
    ,long_event_flag_150
    ,long_event_flag_180
    ,long_event_flag_210
    ,long_event_flag_240
    ,View_Type
    ,base.service_key
    ,lookup.genre_description       as programme_genre
    ,'unknown' as PS_flag
                from    (
       select  account_number
               ,subscriber_id
               ,min(pk_viewing_prog_instance_fact)         as event_id
               ,event_start_date_time_utc                              as event_start_utc
               ,case   when min(capping_end_Date_time_utc) is not null then min(capping_end_Date_time_utc)
                                  else event_end_date_time_utc
                                  end     as event_end_utc
                ,case when min(capping_end_Date_time_utc) is not null then 1 else 0 end as capping_process_flag
               ,min(dk_event_start_datehour_dim)           as event_start_dim
               ,min(dk_event_end_datehour_dim)             as event_end_dim
               ,datediff(ss,event_start_utc,event_end_utc) as event_capped_duration
               ,datediff(ss,event_start_utc,event_end_date_time_utc) as event_uncapped_duration
                ,DATEPART( MONTH, event_start_utc ) as event_start_month
                ,DATEPART( weekday, event_start_utc ) as event_start_dow
                ,DATEPART( hour, event_start_utc ) as event_start_hour
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 120*60 then 1 else 0 end as long_event_flag_120
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 150*60 then 1 else 0 end as long_event_flag_150
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 180*60 then 1 else 0 end as long_event_flag_180
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 210*60 then 1 else 0 end as long_event_flag_210
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 240*60 then 1 else 0 end as long_event_flag_240
               ,case
                    when  type_of_viewing_event  in ( 'TV Channel Viewing','HD Viewing Event') then  'Live'
                    when service_key=65535 then 'PullVOD'
                    when service_key between 4094 and 4098 then 'PushVOD'
                    when type_of_viewing_event = 'Sky+ time-shifted viewing event' and datediff(day, min(broadcast_start_date_time_utc),min(event_start_date_time_utc)) = 0
                            and min(time_in_seconds_since_recording)<3600 then 'VOSDAL_1hr'
                    when type_of_viewing_event = 'Sky+ time-shifted viewing event' and datediff(day, min(broadcast_start_date_time_utc),min(event_start_date_time_utc)) = 0
                            and min(time_in_seconds_since_recording) between 3600 and 86400 then 'VOSDAL_1to24hr'
                    when type_of_viewing_event = 'Sky+ time-shifted viewing event' and datediff(day, min(broadcast_start_date_time_utc),min(event_start_date_time_utc)) = 0
                            and min(time_in_seconds_since_recording) > 86400 then 'Playback'
                    when type_of_viewing_event = 'Sky+ time-shifted viewing event' and datediff(day, min(broadcast_start_date_time_utc),min(event_start_date_time_utc)) <> 0
                            then'Playback'
                    else 'Other'
                end as View_Type
                ,service_key
                               from SK_PROD.VESPA_DP_PROG_VIEWED_201511 as a

                where dk_event_start_datehour_dim between @from_dt and @to_dt
                        and datediff(ss,event_start_utc,event_end_date_time_utc) >=7
       group   by  account_number
                   ,subscriber_id
                   ,event_start_date_time_utc
                   ,event_end_date_time_utc
                    ,service_key
                    ,type_of_viewing_event
                  )   as base
       inner join SK_PROD.VESPA_DP_PROG_VIEWED_201511 as lookup
             on  base.event_id   =   lookup.pk_viewing_prog_instance_fact
end
else
begin
---SUMMER LOOP
insert into TVOC_capping_data_tempshelf
(
 account_number
    ,subscriber_id
    ,event_id
    ,event_start_utc
    ,event_end_utc
    ,capping_process_flag
    ,event_start_dim
    ,event_end_dim
    ,event_capped_duration
    ,event_uncapped_duration
    ,event_start_month
    ,event_start_dow
    ,event_start_hour
    ,long_event_flag_120
    ,long_event_flag_150
    ,long_event_flag_180
    ,long_event_flag_210
    ,long_event_flag_240
    ,View_Type
    ,service_key
    , programme_genre
    , PS_flag)
select
    base.account_number
    ,base.subscriber_id
    ,base.event_id
    ,base.event_start_utc
    ,base.event_end_utc
    ,base.capping_process_flag
    ,base.event_start_dim
    ,base.event_end_dim
    ,base.event_capped_duration
    ,base.event_uncapped_duration
    ,event_start_month
    ,event_start_dow
    ,event_start_hour
    ,long_event_flag_120
    ,long_event_flag_150
    ,long_event_flag_180
    ,long_event_flag_210
    ,long_event_flag_240
    ,View_Type
    ,base.service_key
    ,lookup.genre_description       as programme_genre
    ,'unknown' as PS_flag
                from    (
       select  account_number
               ,subscriber_id
               ,min(pk_viewing_prog_instance_fact)         as event_id
               ,event_start_date_time_utc                              as event_start_utc
               ,case   when min(capping_end_Date_time_utc) is not null then min(capping_end_Date_time_utc)
                                  else event_end_date_time_utc
                                  end     as event_end_utc
                ,case when min(capping_end_Date_time_utc) is not null then 1 else 0 end as capping_process_flag
               ,min(dk_event_start_datehour_dim)           as event_start_dim
               ,min(dk_event_end_datehour_dim)             as event_end_dim
               ,datediff(ss,event_start_utc,event_end_utc) as event_capped_duration
               ,datediff(ss,event_start_utc,event_end_date_time_utc) as event_uncapped_duration
                ,DATEPART( MONTH, event_start_utc ) as event_start_month
                ,DATEPART( weekday, event_start_utc ) as event_start_dow
                ,DATEPART( hour, event_start_utc ) as event_start_hour
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 120*60 then 1 else 0 end as long_event_flag_120
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 150*60 then 1 else 0 end as long_event_flag_150
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 180*60 then 1 else 0 end as long_event_flag_180
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 210*60 then 1 else 0 end as long_event_flag_210
                ,case when  datediff(ss,event_start_utc,event_end_date_time_utc) > 240*60 then 1 else 0 end as long_event_flag_240
               ,case
                    when  type_of_viewing_event  in ( 'TV Channel Viewing','HD Viewing Event') then  'Live'
                    when service_key=65535 then 'PullVOD'
                    when service_key between 4094 and 4098 then 'PushVOD'
                    when type_of_viewing_event = 'Sky+ time-shifted viewing event' and datediff(day, min(broadcast_start_date_time_utc),min(event_start_date_time_utc)) = 0
                            and min(time_in_seconds_since_recording)<3600 then 'VOSDAL_1hr'
                    when type_of_viewing_event = 'Sky+ time-shifted viewing event' and datediff(day, min(broadcast_start_date_time_utc),min(event_start_date_time_utc)) = 0
                            and min(time_in_seconds_since_recording) between 3600 and 86400 then 'VOSDAL_1to24hr'
                    when type_of_viewing_event = 'Sky+ time-shifted viewing event' and datediff(day, min(broadcast_start_date_time_utc),min(event_start_date_time_utc)) = 0
                            and min(time_in_seconds_since_recording) > 86400 then 'Playback'
                    when type_of_viewing_event = 'Sky+ time-shifted viewing event' and datediff(day, min(broadcast_start_date_time_utc),min(event_start_date_time_utc)) <> 0
                            then'Playback'

                    else 'Other'
                end as View_Type
                ,service_key
                               from SK_PROD.VESPA_DP_PROG_VIEWED_201507 as a

                where dk_event_start_datehour_dim between @from_dt and @to_dt
                        and datediff(ss,event_start_utc,event_end_date_time_utc) >=7
       group   by  account_number
                   ,subscriber_id
                   ,event_start_date_time_utc
                   ,event_end_date_time_utc
                    ,service_key
                    ,type_of_viewing_event
                  )   as base
       inner join SK_PROD.VESPA_DP_PROG_VIEWED_201507 as lookup
             on  base.event_id   =   lookup.pk_viewing_prog_instance_fact

end

                commit

                MESSAGE cast(now() as timestamp)||' | @ TVOC 1.1: Compacting Data at Event level DONE' TO CLIENT


                MESSAGE cast(now() as timestamp)||' | Begining  TVOC 1.2 - Appending Dimensions' TO CLIENT

                -- adding Session_daypart (probably not needed but leave code here anyway)

                update  TVOC_capping_data_tempshelf
                set     session_daypart =   case    when cast(event_start_utc as time) between '00:00:00.000' and '05:59:59.000' then 'night'
                                                                                        when cast(event_start_utc as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
                                                                                        when cast(event_start_utc as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
                                                                                        when cast(event_start_utc as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
                                                                                        when cast(event_start_utc as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
                                                                                        when cast(event_start_utc as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
                                                                                        when cast(event_start_utc as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
                                                                        end

                commit

                MESSAGE cast(now() as timestamp)||' | @ TVOC 1.2: Appending Session_Daypart DONE' TO CLIENT

                -- adding Channel_pack and channel name (using vespa name here though we can use others)
                -- anything else from this mapping table useful?

                update  TVOC_capping_data_tempshelf
                set     channel_pack    = cm.channel_pack
                ,channel_name=cm.vespa_name
                from    TVOC_capping_data_tempshelf                                           as dpraw
                                inner join (
                select * from vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES where  @process_date between effective_from and effective_to) as cm
                                on  dpraw.service_key   = cm.service_key


                commit

                MESSAGE cast(now() as timestamp)||' | @ TVOC 1.2: Appending Channel_Pack DONE' TO CLIENT


                --make update to PS_Flag from SBV table (this is a snapshot, so there should be a 1:1 relationship on subscriberID)
                UPDATE  TVOC_capping_data_tempshelf dpraw
                   SET  PS_flag = sbv.PS_flag
                  FROM vespa_analysts.vespa_single_box_view as sbv
                 WHERE dpraw.subscriber_id   = sbv.subscriber_id

                commit

                MESSAGE cast(now() as timestamp)||' | @ TVOC 1.2: Appending Primary/Secondary Box Flag (PS_flag) DONE' TO CLIENT


-----------------------------------------------
--- create aggregation
-----------------------------------------------

if @i=0 and @seasonloop=1
begin
    if object_id('TVOC_data_set_5') is not null  drop table TVOC_data_set_5

    select
                        programme_genre
                        ,channel_pack
                        ,event_start_month
                        ,event_start_dow
                        ,event_start_hour
                        ,View_Type
                        ,channel_name
                        ,capping_process_flag
                        ,PS_flag
                        ,floor(event_start_dim/100)  as event_start_date
                        ,sum(event_capped_duration)/60 as sum_event_capped__duration_mins
                        ,sum(event_uncapped_duration)/60 as sum_event_uncapped__duration_mins
                        ,count(*) as count_events
                        ,sum(long_event_flag_120) as count_long_event_120
                        ,sum(long_event_flag_150) as count_long_event_150
                        ,sum(long_event_flag_180) as count_long_event_180
                        ,sum(long_event_flag_210) as count_long_event_210
                        ,sum(long_event_flag_240) as count_long_event_240


into TVOC_data_set_5
from TVOC_capping_data_tempshelf
group by
                        programme_genre
                        ,channel_pack
                        ,event_start_month
                        ,event_start_dow
                        ,event_start_hour
                        ,View_Type
                        ,channel_name
                        ,capping_process_flag
                        ,PS_flag
                        ,floor(event_start_dim/100)
    end
    else
begin
insert into TVOC_data_set_5 (
                        programme_genre
                        ,channel_pack
                        ,event_start_month
                        ,event_start_dow
                        ,event_start_hour
                        ,View_Type
                        ,channel_name
                        ,capping_process_flag
                        ,PS_flag
                        ,event_start_date
                        ,sum_event_capped__duration_mins
                        ,sum_event_uncapped__duration_mins
                        ,count_events
                        ,count_long_event_120
                        ,count_long_event_150
                        ,count_long_event_180
                        ,count_long_event_210
                        ,count_long_event_240
                        )
 select
                        programme_genre
                        ,channel_pack
                        ,event_start_month
                        ,event_start_dow
                        ,event_start_hour
                        ,View_Type
                        ,channel_name
                        ,capping_process_flag
                        ,PS_flag
                        ,floor(event_start_dim/100)  as event_start_date
                        ,sum(event_capped_duration)/60 as sum_event_capped__duration_mins
                        ,sum(event_uncapped_duration)/60 as sum_event_uncapped__duration_mins
                        ,count(*) as count_events
                        ,sum(long_event_flag_120) as count_long_event_120
                        ,sum(long_event_flag_150) as count_long_event_150
                        ,sum(long_event_flag_180) as count_long_event_180
                        ,sum(long_event_flag_210) as count_long_event_210
                        ,sum(long_event_flag_240) as count_long_event_240
from TVOC_capping_data_tempshelf
group by
                        programme_genre
                        ,channel_pack
                        ,event_start_month
                        ,event_start_dow
                        ,event_start_hour
                        ,View_Type
                        ,channel_name
                        ,capping_process_flag
                        ,PS_flag
                        ,floor(event_start_dim/100)
    end


MESSAGE cast(now() as timestamp)||' | @ TVOC 1.2: agg dataset created:'  TO CLIENT

MESSAGE cast(now() as timestamp)||' | @ TVOC 0.1: Day loop number ended:' ||@i TO CLIENT
set @i=@i+1

if @i=@numdays
    begin
        MESSAGE cast(now() as timestamp)||' | @ TVOC 0.1: Season loop ended:' ||@seasonloop TO CLIENT
        set @i=0
        set @seasonloop=@seasonloop+1
    end
end

     MESSAGE cast(now() as timestamp)||' | @ TVOC 3.0: Aggregate day datasets created:'  TO CLIENT

--- Left to do:
--- weights  (probably not needed for this piece of work)


