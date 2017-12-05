/*###############################################################################
# Created on:   26/11/2013
# Created by:   Sebastian Bednaszynski
# Description:  Opinion formers - viewing data summary
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 26/11/2013  SBE   Initial version
#
###############################################################################*/



  -- ##############################################################################################################
  -- ##### STEP 1 - Get raw viewing events                                                                    #####
  -- ##############################################################################################################
if object_id('OpForm_02_Raw_Viewing_Events') is not null then drop table OpForm_02_Raw_Viewing_Events end if;
create table OpForm_02_Raw_Viewing_Events (
      Source_Pk                           bigint            null     default 0,
      Account_Number                      varchar(20)                default null,
      Subscriber_Id                       bigint            null     default 0,
      Service_Key                         bigint            null     default 0,
      Dk_Programme_Instance_Dim           bigint            null     default 0,
      Event_Type                          varchar(30)                default null,
      Time_Since_Recording                bigint            null     default 0,

      Genre_Description                   varchar(50)       null     default 'Unknown',
      Sub_Genre_Description               varchar(50)       null     default 'Unknown',
      Programme_Name                      varchar(100)      null     default 'Unknown',

      Viewing_Day                         date                       default null,
      Weekend_Flag                        varchar(3)        null     default 'No',

      Channel_Name                        varchar(50)                default null,
      Channel_Type                        varchar(50)                default null,
      Format                              varchar(10)                default null,

      Playback_Flag                       smallint          null     default 0,
      Prog_Instance_Broadcast_Duration    bigint            null     default 0,
      Dk_Instance_Start_Datehour_Dim      bigint            null     default 0,

      Broadcast_Start_Date_Time           datetime          null     default null,
      Broadcast_End_Date_Time             datetime          null     default null,

      Daytime_Broadcast                   bit               null     default 0,
      Primetime_Broadcast                 bit               null     default 0,
      Nighttime_Broadcast                 bit               null     default 0,

      Daytime_Viewing                     bit               null     default 0,
      Primetime_Viewing                   bit               null     default 0,
      Nighttime_Viewing                   bit               null     default 0,

      Event_Start_Date                    date              null     default null,
      Event_Start_Date_Time               datetime          null     default null,
      Event_End_Date_Time_Capped          datetime          null     default null,

      Instance_Start_Date                 date              null     default null,
      Instance_Start_Date_Time            datetime          null     default null,
      Instance_End_Date_Time_Capped       datetime          null     default null,

      Instance_Duration                   bigint            null     default 0,
      Instance_Duration_DayPrimetime      bigint            null     default 0,
      Instance_Duration_Daytime           bigint            null     default 0,
      Instance_Duration_Primetime         bigint            null     default 0

);

create unique hg index idx01 on OpForm_02_Raw_Viewing_Events(Source_Pk);
create        hg index idx02 on OpForm_02_Raw_Viewing_Events(Account_Number);
create        hg index idx03 on OpForm_02_Raw_Viewing_Events(Subscriber_Id);
create      date index idx04 on OpForm_02_Raw_Viewing_Events(Viewing_Day);
create        hg index idx05 on OpForm_02_Raw_Viewing_Events(Dk_Instance_Start_Datehour_Dim);
create        LF index idx06 on OpForm_02_Raw_Viewing_Events(Event_Type);
create        LF index idx07 on OpForm_02_Raw_Viewing_Events(Playback_Flag);
grant select on OpForm_02_Raw_Viewing_Events to vespa_group_low_security;


--truncate table OpForm_02_Raw_Viewing_Events;

create variable @varDaytimeStart                time;
create variable @varDaytimeEnd                  time;
create variable @varDaytimeEndSubs              time;
create variable @varPrimtimeStart               time;
create variable @varPrimetimeEnd                time;
create variable @varPrimetimeEndSubs            time;

set @varDaytimeStart      = '09:00:00.000';
set @varDaytimeEnd        = '16:59:59.999';
set @varDaytimeEndSubs    = '17:00:00.000';
set @varPrimtimeStart     = '17:00:00.000';
set @varPrimetimeEnd      = '22:59:59.999';
set @varPrimetimeEndSubs  = '23:00:00.000';



begin

      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varStartDate                   date
      declare @varEndDate                     date
      declare @varEventStartHour              int
      declare @varEventEndHour                int

      set @varBuildId   = -1
      set @varStartDate = '2013-09-16'
      set @varEndDate   = '2013-10-02'


      while @varStartDate <= @varEndDate
          begin

              set @varEventStartHour    = (dateformat(@varStartDate - 1, 'yyyymmdd23'))         -- summer time adjustments, hence 23hrs the day before
              set @varEventEndHour      = (dateformat(@varStartDate, 'yyyymmdd22'))             -- summer time adjustments, hence until 22:59 only

              delete from OpForm_02_Raw_Viewing_Events
               where Viewing_Day = @varStartDate
              commit

              execute logger_add_event @varBuildId, 3, 'Viewing data rows for current period removed', @@rowcount

              insert into OpForm_02_Raw_Viewing_Events
                select
                      vw.Pk_Viewing_Prog_Instance_Fact                                  as Source_Pk,
                      vw.Account_Number,
                      vw.Subscriber_Id,
                      vw.Service_Key,
                      vw.Dk_Programme_Instance_Dim,

                      case
                        when type_of_viewing_event in ('Other Service Viewing Event') then 'PPV'
                        when type_of_viewing_event in ('HD Viewing Event', 'TV Channel Viewing', 'Sky+ time-shifted viewing event') then 'Linear'
                          else 'Unknown'
                      end                                                               as Event_Type,
                      vw.time_in_seconds_since_recording                                as Time_Since_Recording,

                      vw.Genre_Description,
                      vw.Sub_Genre_Description,
                      vw.Programme_Name,

                      date( dateadd(hour, 1, vw.Instance_Start_Date_Time_Utc) )         as Viewing_Day,
                      case
                        when datepart(weekday, dateadd(hour, 1, vw.Instance_Start_Date_Time_Utc)) in (1, 7) then 'Yes'
                          else 'No'
                      end                                                               as Weekend_Flag,

                      cm.Vespa_Name                                                     as Channel_Name,
                      cm.Channel_Type,
                      cm.Format,

                      case
                        when vw.live_recorded = 'LIVE' then 0
                        when time_in_seconds_since_recording <= 60 * 60 then 2                                               -- Live pause
                          else 1                                                                                             -- True playback
                      end                                                               as Playback_Flag,
                      datediff(second, vw.broadcast_Start_date_time_utc, vw.broadcast_end_date_time_utc)
                                                                                        as Prog_Instance_Broadcast_Duration,
                      cast( dateformat( dateadd(hour, 1, vw.Instance_Start_Date_Time_Utc), 'yyyymmddhh' ) as bigint)
                                                                                        as Dk_Instance_Start_Datehour_Dim,

                      dateadd(hour, 1, vw.Broadcast_Start_Date_Time_Utc)                as Broadcast_Start_Date_Time,
                      dateadd(hour, 1, vw.Broadcast_End_Date_Time_Utc)                  as Broadcast_End_Date_Time,

                      case
                        when cast( dateadd(hour, 1, vw.Broadcast_Start_Date_Time_Utc) as time ) between @varDaytimeStart and @varDaytimeEnd  then 1
                          else 0
                      end                                                               as Daytime_Broadcast,
                      case
                        when cast( dateadd(hour, 1, vw.Broadcast_Start_Date_Time_Utc) as time ) between @varPrimtimeStart and @varPrimetimeEnd  then 1
                          else 0
                      end                                                               as Primetime_Broadcast,
                      case
                        when cast( dateadd(hour, 1, vw.Broadcast_Start_Date_Time_Utc) as time ) < @varDaytimeStart then 1
                        when cast( dateadd(hour, 1, vw.Broadcast_Start_Date_Time_Utc) as time ) > @varPrimetimeEnd then 1
                          else 0
                      end                                                               as Nighttime_Broadcast,

                      0                                                                 as Daytime_Viewing,
                      0                                                                 as Primetime_Viewing,
                      0                                                                 as Nighttime_Viewing,

                      date( dateadd(hour, 1, vw.Event_Start_Date_Time_Utc) )            as Event_Start_Date,
                      dateadd(hour, 1, vw.Event_Start_Date_Time_Utc)                    as Event_Start_Date_Time,
                      case
                        when vw.capping_end_date_time_utc is not null then dateadd(hour, 1, vw.capping_end_date_time_utc)
                          else dateadd(hour, 1, vw.event_end_date_time_utc)
                      end                                                               as Event_End_Date_Time_Capped,

                      date ( dateadd(hour, 1, vw.Instance_Start_Date_Time_Utc) )        as Instance_Start_Date,
                      dateadd(hour, 1, vw.Instance_Start_Date_Time_Utc)                 as Instance_Start_Date_Time,
                      case
                        when vw.capped_partial_flag = 1 then dateadd(hour, 1, vw.capping_end_date_time_utc)
                          else dateadd(hour, 1, vw.instance_end_date_time_utc)
                      end                                                               as Instance_End_Date_Time_Capped,

                      datediff(second, Instance_Start_Date_Time, Instance_End_Date_Time_Capped)
                                                                                        as Instance_Duration,
                      coalesce(
                               datediff(second,
                                        case
                                          when cast(Instance_Start_Date_Time as time) > @varPrimetimeEnd or cast(Instance_End_Date_Time_Capped as time) < @varDaytimeStart then null
                                          when cast(Instance_Start_Date_Time as time) < @varDaytimeStart then cast( date(Instance_Start_Date_Time) || ' ' || @varDaytimeStart as datetime )
                                            else Instance_Start_Date_Time
                                        end,
                                        case
                                          when cast(Instance_Start_Date_Time as time) > @varPrimetimeEnd or cast(Instance_End_Date_Time_Capped as time) < @varDaytimeStart then null
                                          when cast(Instance_End_Date_Time_Capped as time) > @varPrimetimeEnd then cast( date(Instance_End_Date_Time_Capped) || ' ' || @varPrimetimeEnd as datetime )
                                            else Instance_End_Date_Time_Capped
                                        end),
                               0
                              )                                                         as Instance_Duration_DayPrimetime,
                      coalesce(
                               datediff(second,
                                        case
                                          when cast(Instance_Start_Date_Time as time) > @varPrimetimeEnd or cast(Instance_End_Date_Time_Capped as time) < @varDaytimeStart then null
                                          when cast(Instance_Start_Date_Time as time) < @varDaytimeStart then cast( date(Instance_Start_Date_Time) || ' ' || @varDaytimeStart as datetime )
                                            else Instance_Start_Date_Time
                                        end,
                                        case
                                          when cast(Instance_Start_Date_Time as time) > @varDaytimeEnd or cast(Instance_End_Date_Time_Capped as time) < @varDaytimeStart then null
                                          when cast(Instance_End_Date_Time_Capped as time) > @varDaytimeEnd then cast( date(Instance_End_Date_Time_Capped) || ' ' || @varDaytimeEndSubs as datetime )
                                            else Instance_End_Date_Time_Capped
                                        end),
                               0
                              )                                                         as Instance_Duration_Daytime,
                      coalesce(
                               datediff(second,
                                        case
                                          when cast(Instance_Start_Date_Time as time) > @varPrimetimeEnd or cast(Instance_End_Date_Time_Capped as time) < @varPrimtimeStart then null
                                          when cast(Instance_Start_Date_Time as time) < @varPrimtimeStart then cast( date(Instance_Start_Date_Time) || ' ' || @varPrimtimeStart as datetime )
                                            else Instance_Start_Date_Time
                                        end,
                                        case
                                          when cast(Instance_Start_Date_Time as time) > @varPrimetimeEnd or cast(Instance_End_Date_Time_Capped as time) < @varPrimtimeStart then null
                                          when cast(Instance_End_Date_Time_Capped as time) > @varPrimetimeEnd then cast( date(Instance_End_Date_Time_Capped) || ' ' || @varPrimetimeEndSubs as datetime )
                                            else Instance_End_Date_Time_Capped
                                        end),
                               0
                              )                                                         as Instance_Duration_Primetime

                  from sk_prod.vespa_dp_prog_viewed_201309 vw
                  --from sk_prod.vespa_dp_prog_viewed_201310 vw
                          left join VESPA_Analysts.Channel_Map_Prod_Service_Key_Attributes cm     on vw.Service_Key = cm.Service_Key
                                                                                                 and cm.Effective_From < '2013-10-13'
                                                                                                 and cm.Effective_To >= '2013-10-13'
                 where capped_full_flag = 0
                   and panel_id = 12
                   and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                   and (reported_playback_speed is null or reported_playback_speed = 2)
                   and broadcast_start_date_time_utc >= dateadd(hour, -(24*28), event_start_date_time_utc)
                   and account_number is not null
                   and subscriber_id is not null
                   and (
                         vw.type_of_viewing_event in ('HD Viewing Event', 'TV Channel Viewing')
                         or
                         (
                           vw.type_of_viewing_event = 'Other Service Viewing Event'
                           and
                           cm.Channel_Type in ('Retail - Pay-per-night', 'Retail - Pay-per-view',
                                               'Retail - PPV HD', 'NR - Pay-per-view')
                         )
                         or
                         (
                           vw.type_of_viewing_event = 'Sky+ time-shifted viewing event'
                           and
                           cm.Channel_Type <> 'NR - FTA - Radio'
                         )
                       )
                   and datediff(second, event_start_date_time_utc, event_end_date_time_utc) > 6
                   and vw.Dk_Instance_Start_Datehour_Dim between @varEventStartHour and @varEventEndHour

              commit

              execute logger_add_event @varBuildId, 3, 'Day processed: ' || dateformat(@varStartDate, 'dd/mm/yyyy'), @@rowcount

              set @varStartDate = @varStartDate + 1
          end

      execute logger_add_event @varBuildId, 3, 'Viewing data created', null

end;


-- Delete uncapped viewing
delete from OpForm_02_Raw_Viewing_Events
 where Event_Start_Date < Viewing_Day - 1;
commit;



-- Add Anytime+
insert into OpForm_02_Raw_Viewing_Events
       (Source_Pk, Account_Number, Event_Type, Genre_Description, Viewing_Day, Weekend_Flag,
        Prog_Instance_Broadcast_Duration, Dk_Instance_Start_Datehour_Dim, Event_Start_Date, Event_Start_Date_Time,
        Event_End_Date_Time_Capped, Instance_Start_Date, Instance_Start_Date_Time, Instance_End_Date_Time_Capped,
        Instance_Duration, Instance_Duration_DayPrimetime, Instance_Duration_Daytime, Instance_Duration_Primetime)
  select
        Cb_Row_Id                                                         as Source_Pk,
        Account_Number,
        'Anytime+ DL'                                                     as Event_Type,

        Genre_Desc                                                        as Genre_Description,

        date(last_modified_dt)                                            as Viewing_Day,
        case
          when datepart(weekday, last_modified_dt) in (1, 7) then 'Yes'
            else 'No'
        end                                                               as Weekend_Flag,

        Run_Time                                                          as Prog_Instance_Broadcast_Duration,
        cast( dateformat( last_modified_dt, 'yyyymmddhh' ) as bigint)     as Dk_Instance_Start_Datehour_Dim,

        last_modified_dt                                                  as Event_Start_Date,
        last_modified_dt                                                  as Event_Start_Date_Time,
        last_modified_dt                                                  as Event_End_Date_Time_Capped,

        last_modified_dt                                                  as Instance_Start_Date,
        last_modified_dt                                                  as Instance_Start_Date_Time,
        last_modified_dt                                                  as Instance_End_Date_Time_Capped,

        case
          when x_Download_Size_Mb = 0 then 0
            else 100.0 * x_actual_downloaded_size_mb / x_Download_Size_Mb
        end                                                              as Instance_Duration,
        case
          when x_Download_Size_Mb = 0 then 0
          when cast(last_modified_dt as time) between @varDaytimeStart and @varPrimetimeEnd then 100.0 * x_actual_downloaded_size_mb / x_Download_Size_Mb
            else 0
        end                                                              as Instance_Duration_DayPrimetime,
        case
          when x_Download_Size_Mb = 0 then 0
          when cast(last_modified_dt as time) between @varDaytimeStart and @varDaytimeEnd then 100.0 * x_actual_downloaded_size_mb / x_Download_Size_Mb
            else 0
        end                                                              as Instance_Duration_Daytime,
        case
          when x_Download_Size_Mb = 0 then 0
          when cast(last_modified_dt as time) between @varPrimtimeStart and @varPrimetimeEnd then 100.0 * x_actual_downloaded_size_mb / x_Download_Size_Mb
            else 0
        end                                                              as Instance_Duration_Primetime

    from sk_prod.cust_anytime_plus_downloads
   where x_content_type_desc = 'PROGRAMME'    --  to exclude trailers
     and x_actual_downloaded_size_mb > 1      -- to exclude any spurious header/trailer download records
     and last_modified_dt between '2013-09-16 00:00:00.000' and '2013-10-13 23:59:59.999';
commit;


update OpForm_02_Raw_Viewing_Events
   set Daytime_Viewing    = case when Instance_Duration_Daytime >= 0 then 1 else 0 end,
       Primetime_Viewing  = case when Instance_Duration_Primetime >= 0 then 1 else 0 end,
       Nighttime_Viewing  = case when Instance_Duration_DayPrimetime = 0 then 1 else 0 end
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################





