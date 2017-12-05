--Project 022 - ATL Optimisation

--P10

-- Here is what the code does:
--
-- Loads a file of spot data from Techedge
-- Loads the lookup so we can match it to all channels (including all regional variations) that they were shown on
-- Gets all viewing data for the period and does capping (we did versions for 1% max cap and 10%, but we’re only using the 10% version now)
-- Builds a table of accounts with all the fields we need
-- Joins them together to see who was watching each ad spot. (also find out who was watching anything at the time a spot was showing, so we can do an index)

--PART A Create Lookup table of Tech Edge Channel and Corresponding BARB Channels
--PART B Viewing data for programmes broadcast between 7th Nov and 2nd Dec
--PART C Create table of accounts
--PART D create table of accounts that viewed each spot




--PART A Create Lookup table of Tech Edge Channel and Corresponding BARB Channels
-- create table vespa_analysts.channel_name_and_techedge_channel(channel              varchar(90)
--                                                              ,channel_name_grouped varchar(90)
--                                                              ,channel_name_inc_hd  varchar(90)
--                                                              ,techedge_channel     varchar(90)
-- );
--
-- input into vespa_analysts.channel_name_and_techedge_channel
--       from 'G:\RTCI\Sky Projects\Vespa\Phase1b\Channel Lookup\Channel Lookup Info With Techedge Channelv2.csv'
--     format ascii;
drop table channel_lookup;
create table channel_lookup(Service_key                int
                           ,techedge_channel           varchar(50)
                           ,lookup_combined            varchar(50)
                           ,techedge_log_station_code  varchar(50)
                           ,techedge_sti_code          varchar(50)
                           ,barb_log_station_code      varchar(50)
                           ,sti_code                   varchar(50)
                           ,channel_type               varchar(50)
                           ,channel_name               varchar(50)
                           ,notes                      varchar(100)
                           ,user_interface_description varchar(50)
);
load table channel_lookup(Service_key',',
                          techedge_channel',',
                          lookup_combined',',
                          techedge_log_station_code',',
                          techedge_sti_code',',
                          barb_log_station_code',',
                          sti_code',',
                          channel_type',',
                          channel_name',',
                          notes',',
                          user_interface_description'\n'
)
from '/SKP2x2f1/prod/sky/olive/data/share/clarityq/export/Jon/20120229 BARB_spot_channel_map.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000;
commit;
delete from channel_lookup where techedge_channel='Techedge_Channel';
grant all on channel_lookup to public;

--   update channel_lookup
--    set lookup_combined = left(lookup_combined,len(lookup_combined)-1)
-- ;
--
drop table project_022_all_techedge_spots;
create table vespa_analysts.project_022_all_techedge_spots(
             channel             varchar(90)
            ,Spot_Date           date
            ,spot_start          varchar(10)
            ,spot_end            varchar(10)
            ,duration            tinyint
            ,Advertiser          varchar(20)
            ,brand               varchar(35)
            ,log_station         varchar(50)
            ,platforms           varchar(10)
            ,sti_code            varchar(50)
            ,film_code           varchar(30)
            ,TVR                 real
            ,Impacts             real
            ,spot_start_datetime datetime
            ,spot_end_datetime   datetime
            ,id                  integer identity
            ,lookup_combined varchar(30)
);

load table project_022_all_techedge_spots(
           channel',',
           Spot_Date',',
           spot_start',',
           spot_end',',
           duration',',
           Advertiser',',
           brand',',
           log_station',',
           platforms',',
           sti_code',',
           film_code',',
           TVR',',
           Impacts'\n'
)
from '/SKP2x2f1/prod/sky/olive/data/share/clarityq/export/Jon/20120228 Proof point ATL extract_v3.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000;
update project_022_all_techedge_spots
   set lookup_combined = case when log_station = '-none-' then channel
                                                          else log_station end
;

--we are only interested in digital satellite
--select platforms,count(*) from project_022_all_techedge_spots group by platforms
delete from project_022_all_techedge_spots where platforms='DT';

---Add Vespa Channel Detail on to spot data
--alter table vespa_analysts.project_022_all_techedge_spots delete channel_name_grouped;

update vespa_analysts.project_022_all_techedge_spots
   set spot_start_datetime = cast(case when cast(left(spot_start, 2) as int) > 23 then dateadd(day, 1, spot_date) || ' ' || right('0' || cast(left(spot_start, 2) as int) - 24, 2) || right(spot_start, 6)
                                                                                  else spot_date                  || ' ' || spot_start end as datetime)
      ,spot_end_datetime   = cast(case when cast(left(spot_end  , 2) as int) > 23 then dateadd(day, 1, spot_date) || ' ' || right('0' || cast(left(spot_end  , 2) as int) - 24, 2) || right(spot_end  , 6)
                                                                                  else spot_date                  || ' ' || spot_end   end as datetime)
;
--PART B - Viewing data for programmes broadcast between 7th Nov and 2nd Dec----------------------------------------------------------------------------

  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2011-11-10';
SET @var_prog_period_end    = '2011-12-16';

SET @var_cntr = 0;
SET @var_num_days = @var_prog_period_end - @var_prog_period_start;

create variable @target_date date;
set @target_date= '2011-11-10';
drop table project_022_all_viewing_records_20111107_20111209;
-- To store all the viewing records:
create table vespa_analysts.project_022_all_viewing_records_20111107_20111209(
             cb_row_ID                             bigint       not null primary key
            ,Account_Number                        varchar(20)  not null
            ,Subscriber_Id                         decimal(8,0) not null
            ,Cb_Key_Household                      bigint
            ,Cb_Key_Family                         bigint
            ,Cb_Key_Individual                     bigint
            ,Event_Type                            varchar(20)  not null
            ,X_Type_Of_Viewing_Event               varchar(40)  not null
            ,Adjusted_Event_Start_Time             datetime
            ,X_Adjusted_Event_End_Time             datetime
            ,X_Viewing_Start_Time                  datetime
            ,X_Viewing_End_Time                    datetime
            ,Tx_Start_Datetime_UTC                 datetime
            ,Tx_End_Datetime_UTC                   datetime
            ,Recorded_Time_UTC                     datetime
            ,Play_Back_Speed                       decimal(4,0)
            ,X_Event_Duration                      decimal(10,0)
            ,X_Programme_Duration                  decimal(10,0)
            ,X_Programme_Viewed_Duration           decimal(10,0)
            ,X_Programme_Percentage_Viewed         decimal(3,0)
            ,X_Viewing_Time_Of_Day                 varchar(15)
            ,Programme_Trans_Sk                    bigint       not null
            ,Channel_Name                          varchar(30)
            ,service_key                           int
            ,Epg_Title                             varchar(50)
            ,Genre_Description                     varchar(30)
            ,Sub_Genre_Description                 varchar(30)
            ,x_cumul_programme_viewed_duration     bigint
            ,live                                  bit         default 0
            ,channel_name_inc_hd                   varchar(40)
            ,capped_x_viewing_start_time_1         datetime
            ,capped_x_viewing_end_time_1           datetime
            ,capped_x_viewing_start_time_10        datetime
            ,capped_x_viewing_end_time_10          datetime
            ,capped_x_programme_viewed_duration_1  int
            ,capped_x_programme_viewed_duration_10 int
            ,capped_flag_1                         tinyint     default 0
            ,capped_flag_10                        tinyint     default 0
            ,viewing_record_start_time_utc         datetime
            ,viewing_record_start_time_local       datetime
            ,viewing_record_end_time_utc_1         datetime
            ,viewing_record_end_time_utc_10        datetime
            ,viewing_record_end_time_local_1       datetime
            ,viewing_record_end_time_local_10      datetime
);

-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into vespa_analysts.project_022_all_viewing_records_20111107_20111209
    select vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
          ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
          ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
          ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
          ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
          ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
          ,prog.channel_name,prog.service_key
          ,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
          ,sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration
          ,0,'''','''','''','''','''',0,0,0,0,'''','''','''','''','''',''''
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*##      as vw
          left  join sk_prod.VESPA_EPG_DIM             as prog on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where video_playing_flag = 1
      and adjusted_event_start_time <> x_adjusted_event_end_time
      and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
           or (    x_type_of_viewing_event = (''Other Service Viewing Event'')
               and x_si_service_type = ''High Definition TV test service''))
      and panel_id in (4,5)'
;

  while @var_cntr < @var_num_days
  begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))
    commit
    set @var_cntr = @var_cntr + 1
  end; --1h58m 2h22 3h18

  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set live = case when play_back_speed is null then 1 else 0 end
;
commit;
drop table vespa_201111_201112_max_caps;
  select event_start_day
        ,event_start_hour
        ,live
        ,min(dur_mins) as min_dur_mins
        ,10 as cap
    into vespa_201111_201112_max_caps
    from (select cast(Adjusted_Event_Start_Time as date) as event_start_day
                ,datepart(hour,Adjusted_Event_Start_Time) as event_start_hour
                ,live
                ,datediff(minute,Adjusted_Event_Start_Time,x_Adjusted_Event_end_Time) as dur_mins
                ,ntile(100) over (partition by event_start_day, event_start_hour, live
                                      order by dur_mins) as ntile_100
            from project_022_all_viewing_records_20111107_20111209) as sub
   where ntile_100 = 91
group by event_start_day
        ,event_start_hour
        ,live
;

  insert into vespa_201111_201112_max_caps
  select event_start_day
        ,event_start_hour
        ,live
        ,min(dur_mins) as min_dur_mins
        ,1 as cap
    from (select cast(Adjusted_Event_Start_Time as date) as event_start_day
                ,datepart(hour,Adjusted_Event_Start_Time) as event_start_hour
                ,live
                ,datediff(minute,Adjusted_Event_Start_Time,x_Adjusted_Event_end_Time) as dur_mins
                ,ntile(100) over (partition by event_start_day, event_start_hour, live
                                      order by dur_mins) as ntile_100
            from project_022_all_viewing_records_20111107_20111209) as sub
   where ntile_100 = 100
group by event_start_day
        ,event_start_hour
        ,live


---Create Capping rules limits
---Add on derived variables for viewing
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
    from vespa_analysts.project_022_all_viewing_records_20111107_20111209 as base
         left join vespa_analysts.channel_name_and_techedge_channel as det on base.Channel_Name = det.Channel
; --6m

commit;
create hg   index idx1 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(subscriber_id);
create dttm index idx2 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(recorded_time_utc);
--create lf   index idx4 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(live);
create dttm index idx5 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(x_viewing_end_time);
create hng  index idx7 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(x_cumul_programme_viewed_duration);
create hg   index idx8 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(programme_trans_sk);
create hg   index idx9 on vespa_analysts.project_022_all_viewing_records_20111107_20111209(channel_name_inc_hd);

-- append fields to table to store additional metrics for capping
-- update the viewing start and end times for playback records
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
   where recorded_time_utc is not null
; --1h 5m
commit;
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
   where recorded_time_utc is not null
; --12m
commit;

-- update table to create capped start and end times
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_x_viewing_start_time_1 = case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
         end
        ,capped_x_viewing_end_time_1 = case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) > x_viewing_end_time then x_viewing_end_time
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, adjusted_event_start_time)
         end
  from vespa_analysts.project_022_all_viewing_records_20111107_20111209 base
       left join vespa_201111_201112_max_caps as caps on date(base.adjusted_event_start_time) = caps.event_start_day
                                                         and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
                                                         and base.live = caps.live
 and cap=1
; --7m

  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_x_viewing_start_time_10 = case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
         end
        ,capped_x_viewing_end_time_10 = case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) > x_viewing_end_time then x_viewing_end_time
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, adjusted_event_start_time)
         end
  from vespa_analysts.project_022_all_viewing_records_20111107_20111209 base
       left join vespa_201111_201112_max_caps as caps on date(base.adjusted_event_start_time) = caps.event_start_day
                                                         and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
                                                         and base.live = caps.live
 and cap=10
;

commit;

-- calculate capped_x_programme_viewed_duration
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_x_programme_viewed_duration_1 = datediff(second, capped_x_viewing_start_time_1, capped_x_viewing_end_time_1)
; --4m

  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_x_programme_viewed_duration_10 = datediff(second, capped_x_viewing_start_time_10, capped_x_viewing_end_time_10)
;
-- set capped_flag based on nature of capping
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_flag_1 =
        case
            when capped_x_viewing_start_time_1 is null then 2
            when capped_x_viewing_end_time_1 < x_viewing_end_time then 1
            else 0
        end
;

  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_x_viewing_start_time_1        = null
        ,capped_x_viewing_end_time_1          = null
        ,capped_x_programme_viewed_duration_1 = null
        ,capped_flag_1                        = 3
    from vespa_201111_201112_min_cap
   where capped_x_programme_viewed_duration_1 < cap_secs
; --2m

  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_flag_10 =
        case
            when capped_x_viewing_start_time_10 is null then 2
            when capped_x_viewing_end_time_10 < x_viewing_end_time then 1
            else 0
        end
; --4m
commit;

  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_x_viewing_start_time_1        = null
        ,capped_x_viewing_end_time_1          = null
        ,capped_x_programme_viewed_duration_1 = null
        ,capped_flag_1                          = 3
    from vespa_201111_201112_min_cap
   where capped_x_programme_viewed_duration_1 < cap_secs
;

  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set capped_x_viewing_start_time_10        = null
        ,capped_x_viewing_end_time_10          = null
        ,capped_x_programme_viewed_duration_10 = null
        ,capped_flag_10                        = 3
    from vespa_201111_201112_min_cap
   where capped_x_programme_viewed_duration_10 < cap_secs
; --2m
commit;

select capped_flag_1,capped_flag_10,count(*) from  vespa_analysts.project_022_all_viewing_records_20111107_20111209 group by capped_flag_1,capped_flag_10
--delete from vespa_analysts.project_022_all_viewing_records_20111107_20111209 where capped_flag in (2,3)
; --16m

---Add in Event start and end time and add in local time activity---

update vespa_analysts.project_022_all_viewing_records_20111107_20111209
   set viewing_record_start_time_utc=case when recorded_time_utc         < tx_start_datetime_utc then tx_start_datetime_utc
                                          when recorded_time_utc         >=tx_start_datetime_utc then recorded_time_utc
                                          when adjusted_event_start_time < tx_start_datetime_utc then tx_start_datetime_utc
                                          when adjusted_event_start_time >=tx_start_datetime_utc then adjusted_event_start_time else null end
  from vespa_analysts.project_022_all_viewing_records_20111107_20111209
; --3m

commit;
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set viewing_record_end_time_utc_1  = dateadd(second,capped_x_programme_viewed_duration_1,viewing_record_start_time_utc)
        ,viewing_record_end_time_utc_10 = dateadd(second,capped_x_programme_viewed_duration_10,viewing_record_start_time_utc)
    from vespa_analysts.project_022_all_viewing_records_20111107_20111209
;  --3m
commit;

update vespa_analysts.project_022_all_viewing_records_20111107_20111209
   set viewing_record_start_time_local = case when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-02'
                                                or dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-02'
                                                or dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-02' then dateadd(hh,1,viewing_record_start_time_utc)
                                              else viewing_record_start_time_utc  end
      ,viewing_record_end_time_local_1   = case when dateformat(viewing_record_end_time_utc_1  ,'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-02'
                                                  or dateformat(viewing_record_end_time_utc_1  ,'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-02'
                                                  or dateformat(viewing_record_end_time_utc_1  ,'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-02' then dateadd(hh,1,viewing_record_end_time_utc_1)
                                                else viewing_record_end_time_utc_1  end
      ,viewing_record_end_time_local_10  = case when dateformat(viewing_record_end_time_utc_10  ,'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-02'
                                                  or dateformat(viewing_record_end_time_utc_10  ,'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-02'
                                                  or dateformat(viewing_record_end_time_utc_10  ,'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-02' then dateadd(hh,1,viewing_record_end_time_utc_10)
                                                else viewing_record_end_time_utc_10 end
  from vespa_analysts.project_022_all_viewing_records_20111107_20111209
; --15m

--add scaling variables (from wiki)
   alter table vespa_analysts.project_022_all_viewing_records_20111107_20111209
     add (weighting_date        date
         ,scaling_segment_ID    int
         ,weightings            float default 0
);

commit;
create index for_weightings on vespa_analysts.project_022_all_viewing_records_20111107_20111209 (weighting_date);

  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set weighting_date = cast(viewing_record_start_time_local as date)
;

-- First, get the segmentation for the account at the time of viewing
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set scaling_segment_ID = l.scaling_segment_ID
    from vespa_analysts.project_022_all_viewing_records_20111107_20111209 as b
         inner join vespa_analysts.scaling_dialback_intervals as l on b.account_number = l.account_number
                                                                  and b.weighting_date between l.reporting_starts and l.reporting_ends
;

commit;

-- Find out the weight for that segment on that day
  update vespa_analysts.project_022_all_viewing_records_20111107_20111209
     set weightings = s.weighting
    from vespa_analysts.project_022_all_viewing_records_20111107_20111209 as b
         inner join vespa_analysts.scaling_weightings                     as s on b.weighting_date     = s.scaling_day
                                                                              and b.scaling_segment_ID = s.scaling_segment_ID
;

commit;

--PART C Create table of accounts ----------------------------------------------------------------------------------------------------------------------
--Add info for Account Number and Primary/Secondary Box
drop table project_022_sky_accounts;

  select account_number
        ,cb_key_household
        ,current_short_description
        ,service_instance_id
        ,SUBSCRIPTION_SUB_TYPE
        ,rank() over (partition by account_number ,SUBSCRIPTION_SUB_TYPE,service_instance_id order by effective_from_dt, cb_row_id) as rank
    into project_022_sky_accounts
    from sk_prod.cust_subs_hist as csh
   where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
     and effective_to_dt = '9999-09-09'
     and effective_from_dt<>effective_to_dt
     and account_number is not null
--there are too many records to process, so limit to the vespa accounts, which is all we need
     and account_number in (select account_number from sk_prod.VESPA_SUBSCRIBER_STATUS)
;

 alter table vespa_analysts.project_022_sky_accounts
   add (hd_model_score      real
       ,hd_model_decile     int
       ,movies_model_decile int
       ,movies_model_score  real
);

commit;
delete from vespa_analysts.project_022_sky_accounts where rank > 1;
commit;

---Create src_system_id lookup
  select src_system_id
        ,min(cast(si_external_identifier as integer)) as subscriberid
    into #subs_details
    from sk_prod.CUST_SERVICE_INSTANCE as b
   where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
group by src_system_id
;

commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';

--alter table vespa_analysts.F1_analysis_20111104 delete subscription_type;
alter table vespa_analysts.project_022_sky_accounts add subscriber_id bigint;

  update vespa_analysts.project_022_sky_accounts
     set subscriber_id=b.subscriberid
    from vespa_analysts.project_022_sky_accounts as a
         left outer join #subs_details as b on a.service_instance_id=b.src_system_id
;
commit;

--Add on HD Account Status and Movies Status as at 7th Nov---
  select account_number
        ,max(case when SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing') and status_code in ('AC','PC') and cel.prem_movies > 0                                    then 1 else 0 end) as ever_had_movies
        ,max(case when SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing') and status_code in ('AC','PC') and cel.prem_movies > 0 and effective_to_dt > @target_date then 1 else 0 end) as currently_has_movies
        ,max(case when SUBSCRIPTION_SUB_TYPE in ('DTV HD')              and status_code in ('AC','PC')                                                            then 1 else 0 end) as ever_had_hd_sub
        ,max(case when SUBSCRIPTION_SUB_TYPE in ('DTV HD')              and status_code in ('AC','PC')                         and effective_to_dt > @target_date then 1 else 0 end) as currently_has_hd_sub
    into #sky_accounts_movies_hd_status_
    from sk_prod.cust_subs_hist as csh
         left outer join sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV HD')
     and effective_from_dt <= @target_date
     and account_number is not null
group by account_number
;
jg
--select distinct SUBSCRIPTION_SUB_TYPE from sk_prod.cust_subs_hist
--select count (distinct account_number) from #sky_accounts_movies_hd_status_
alter table vespa_analysts.project_022_sky_accounts add ever_had_movies tinyint;
alter table vespa_analysts.project_022_sky_accounts add currently_has_movies tinyint;
alter table vespa_analysts.project_022_sky_accounts add ever_had_hd_sub tinyint;
alter table vespa_analysts.project_022_sky_accounts add currently_has_hd_sub tinyint;
commit;
create hg index idx1 on #sky_accounts_movies_hd_status_(account_number);

  update vespa_analysts.project_022_sky_accounts
     set ever_had_movies=b.ever_had_movies
        ,currently_has_movies=b.currently_has_movies
        ,ever_had_hd_sub = b.ever_had_hd_sub
        ,currently_has_hd_sub=b.currently_has_hd_sub
    from vespa_analysts.project_022_sky_accounts as a
         left outer join #sky_accounts_movies_hd_status_ as b on a.account_number=b.account_number
;

--select ever_had_movies , currently_has_movies ,count(*) from vespa_analysts.project_022_sky_accounts group by ever_had_movies , currently_has_movies
--select ever_had_hd_sub , currently_has_hd_sub ,count(*) from vespa_analysts.project_022_sky_accounts group by ever_had_hd_sub , currently_has_hd_sub


-----Add on box details----
commit;
Alter table vespa_analysts.project_022_sky_accounts
  add pvr         tinyint    default 0,
  add box_type    varchar(2) default 'SD',
  add primary_box bit        default 0;
commit;

--Add on box details – most recent dw_created_dt for a box (where a box hasn’t been replaced at that date)  taken from cust_set_top_box.
--This removes instances where more than one box potentially live for a subscriber_id at a time (due to null box installed and replaced dates).

  SELECT account_number
        ,service_instance_id
        ,max(dw_created_dt) as max_dw_created_dt
    INTO #boxes -- drop table #boxes
    FROM sk_prod.CUST_SET_TOP_BOX
   WHERE (box_installed_dt <= cast('2011-11-07'  as date)
     AND box_replaced_dt   >  cast('2011-11-07'  as date)) or box_installed_dt is null
group by account_number
        ,service_instance_id
;

commit;
exec sp_create_tmp_table_idx '#boxes', 'account_number';
exec sp_create_tmp_table_idx '#boxes', 'service_instance_id';
exec sp_create_tmp_table_idx '#boxes', 'max_dw_created_dt';

---Create table of one record per service_instance_id---
  SELECT acc.account_number
        ,acc.service_instance_id
        ,min(stb.x_pvr_type) as pvr_type
        ,min(stb.x_box_type) as box_type
        ,min(stb.x_description) as description_x
        ,min(stb.x_manufacturer) as manufacturer
        ,min(stb.x_model_number) as model_number
    INTO #boxes_with_model_info -- drop table #boxes
    FROM #boxes  AS acc
         left outer join sk_prod.CUST_SET_TOP_BOX AS stb ON acc.account_number = stb.account_number
                                                        and acc.max_dw_created_dt=stb.dw_created_dt
group by acc.account_number
        ,acc.service_instance_id
;

commit;
exec sp_create_tmp_table_idx '#boxes_with_model_info', 'service_instance_id';


alter table vespa_analysts.project_022_sky_accounts add x_pvr_type      varchar(50);
alter table vespa_analysts.project_022_sky_accounts add x_box_type      varchar(20);
alter table vespa_analysts.project_022_sky_accounts add x_description   varchar(100);
alter table vespa_analysts.project_022_sky_accounts add x_manufacturer  varchar(50);
alter table vespa_analysts.project_022_sky_accounts add x_model_number  varchar(50);

  update vespa_analysts.project_022_sky_accounts
     set x_pvr_type=b.pvr_type
        ,x_box_type=b.box_type
        ,x_description=b.description_x
        ,x_manufacturer=b.manufacturer
        ,x_model_number=b.model_number
    from vespa_analysts.project_022_sky_accounts as a
         left outer join #boxes_with_model_info as b on a.service_instance_id=b.service_instance_id
;
commit;

  update vespa_analysts.project_022_sky_accounts
     set pvr      = case when x_pvr_type like '%PVR%' then 1    else 0    end
        ,box_type = case when x_box_type like '%HD%'  then 'HD' else 'SD' end
    from vespa_analysts.project_022_sky_accounts
;

--HD lapsed or enabled
  SELECT stb.account_number
    INTO #hda
    FROM sk_prod.CUST_SET_TOP_BOX AS stb
         INNER JOIN vespa_analysts.project_022_sky_accounts AS acc on stb.account_number = acc.account_number
   WHERE box_installed_dt <= @target_date
     AND box_replaced_dt  >  @target_date
     AND current_product_description like '%HD%'
GROUP BY stb.account_number
; --

commit;
CREATE UNIQUE hg INDEX idx1 ON #hda(account_number);
 alter table vespa_analysts.project_022_sky_accounts
   add (hd_lapsed_enabled bit default 0
       ,bb                bit default 0
       ,onnet             bit default 0
);

update vespa_analysts.project_022_sky_accounts as bas
   set hd_lapsed_enabled = 1
  from #hda
 where bas.account_number = #hda.account_number
;

  update vespa_analysts.project_022_sky_accounts as bas
     set hd_lapsed_enabled = 1
   where ever_had_hd_sub   = 1
;

  update vespa_analysts.project_022_sky_accounts as bas
     set hd_lapsed_enabled = 0
   where currently_has_hd_sub = 1
;

--BB
  update vespa_analysts.project_022_sky_accounts as bas
     set bb = 01
    from sk_prod.cust_subs_hist as csh
   where csh.subscription_sub_type = 'Broadband DSL Line'
     and effective_from_dt <= @target_date
     and effective_to_dt   >  @target_date
     and (   status_code in ('AC','AB')
          or (status_code = 'PC' and prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
          or (status_code = 'CF' and prev_status_code = 'PC')
          or (status_code = 'AP' and sale_type        = 'SNS Bulk Migration'))
     and bas.account_number = csh.account_number
; --4m

--add Postcode so we can calculate onnet status
alter table vespa_analysts.project_022_sky_accounts
  add postcode_no_space varchar(10);

commit;
create hg index idx_cb_key_household_hg on vespa_analysts.project_022_sky_accounts(cb_key_household);

  update vespa_analysts.project_022_sky_accounts as bas
     set postcode_no_space = replace(cb_address_postcode,' ','')
    from sk_prod.cust_single_account_view as sav
   where sav.cb_key_household = bas.cb_key_household
; --1,552,012 --1631748

--Onnet code from wiki
-- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes
    SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
      INTO #bpe
      FROM sk_prod.BROADBAND_POSTCODE_EXCHANGE
  GROUP BY postcode;

  UPDATE #bpe SET postcode = REPLACE(postcode,' ',''); -- Remove spaces for matching

-- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
    SELECT postcode, MAX(exchange_id) as exchID
      INTO #p2e
      FROM sk_prod.BB_POSTCODE_TO_EXCHANGE
  GROUP BY postcode;

  UPDATE #p2e SET postcode = REPLACE(postcode,' ','');  -- Remove spaces for matching

-- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
  SELECT #bpe.postcode, COALESCE(#p2e.exchID, #bpe.exchID) as exchange_id, 'OFFNET' as exchange
    INTO #onnet_lookup
    FROM #bpe FULL JOIN #p2e ON #bpe.postcode = #p2e.postcode;

-- 4) Update with latest Easynet exchange information
  UPDATE #onnet_lookup
     SET exchange = 'ONNET'
    FROM #onnet_lookup AS base
         INNER JOIN sk_prod.easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
  WHERE easy.exchange_status = 'ONNET';

-- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
--   spaces removed so your table will either need to have a similar filed or use a REPLACE
--   function in the join

  UPDATE vespa_analysts.project_022_sky_accounts
     SET onnet = CASE WHEN tgt.exchange = 'ONNET'
                      THEN 1 else 0 END
    FROM vespa_analysts.project_022_sky_accounts AS base
         INNER JOIN #onnet_lookup AS tgt on base.postcode_no_space = tgt.postcode
; --

--add model scores
  update vespa_analysts.project_022_sky_accounts as bas
     set bas.hd_model_score  = mod.score
        ,bas.hd_model_decile = mod.decile
    from models.model_scores as mod
   where model_run_date = '2011-12-21'
     and model_name     = 'HD Mailed'
     and bas.account_number = mod.account_number
;
--take out those with hd/movies

  update vespa_analysts.project_022_sky_accounts as bas
     set bas.movies_model_score  = mod.score
        ,bas.movies_model_decile = mod.decile
    from models.model_scores as mod
   where model_run_date      = '2011-12-21'
     and model_name          = 'Movies Mailed'
     and bas.account_number  = mod.account_number
;

--redo model deciles just for this Vespa panel
  alter table project_022_sky_accounts
    add hd_decile_recalc     tinyint
   ,add movies_decile_recalc tinyint
   ,add hd_score_recalc      real
   ,add movies_score_recalc  real
;

create variable @hd_records int;
set @hd_records = (select count(*) from project_022_sky_accounts where hd_model_decile>0);
create variable @movies_records int;
set @movies_records = (select count(*) from project_022_sky_accounts where movies_model_decile>0);

  select hd_model_score
        ,ntile(10) over (order by hd_model_score)                    as hd_recalc
        ,cast(cast(rank()   over (order by hd_model_score) as real) / cast(@hd_records as real) as real) as hd_rerank
    into #temp
    from project_022_sky_accounts
   where hd_model_score >0
;

  update project_022_sky_accounts as bas
     set bas.hd_decile_recalc = sub.hd_recalc
        ,bas.hd_score_recalc  = sub.hd_rerank
    from #temp as sub
   where bas.hd_model_score     = sub.hd_model_score
;

drop table #temp;
  select movies_model_score
        ,ntile(10) over (order by movies_model_score)                    as movies_recalc
        ,cast(cast(rank()   over (order by movies_model_score) as real) / cast(@movies_records as real) as real) as movies_rerank
    into #temp
    from project_022_sky_accounts
   where movies_model_score >0
;
  update project_022_sky_accounts as bas
     set bas.movies_decile_recalc = sub.movies_recalc
        ,bas.movies_score_recalc  = sub.movies_rerank
    from #temp as sub
   where bas.movies_model_score     = sub.movies_model_score
;

--checks
select avg(movies_rerank),min(movies_rerank),max(movies_rerank) from #temp
select hd_decile_recalc,count(*) from project_022_sky_accounts group by hd_decile_recalc;
select movies_decile_recalc,count(*) from project_022_sky_accounts group by movies_decile_recalc;
select movies_decile_recalc,count(*) from project_022_sky_acounts_by_account group by movies_decile_recalc;
select min(movies_rerank),max(movies_rerank),avg(movies_rerank) from #temp;
select max(movies_score_recalc) from project_022_sky_accounts

drop table project_022_sky_acounts_by_account;
  select account_number
        ,max(onnet)                as onnet
        ,max(bb)                   as bb
        ,max(hd_model_decile)      as hd_decile
        ,max(movies_model_decile)  as movies_decile
        ,max(currently_has_hd_sub) as currently_has_hd_sub
        ,max(currently_has_movies) as currently_has_movies
        ,max(hd_lapsed_enabled)    as hd_lapsed_enabled
        ,max(ever_had_movies)      as ever_had_movies
        ,max(hd_model_score)       as hd_model_score
        ,max(movies_model_score)   as movies_model_score
        ,max(hd_decile_recalc)     as hd_decile_recalc
        ,max(movies_decile_recalc) as movies_decile_recalc
        ,max(hd_score_recalc)      as hd_score_recalc
        ,max(movies_score_recalc)  as movies_score_recalc
    into project_022_sky_acounts_by_account
    from project_022_sky_accounts
group by account_number
; --416,947

commit;
create hg index idx_account_number_hg on project_022_sky_acounts_by_account(account_number);

--PART D create table of accounts that viewed each spot ------------------------------------------------------------------------------------------------
drop table project_022_accounts_spots;
create table project_022_accounts_spots(account_number varchar(30)
                                       ,weightings     real
                                       ,id             int
                                       ,id_type        varchar(10)
);

commit;
create dttm index idx_spot_start_datetime_dttm             on project_022_all_techedge_spots(spot_start_datetime);
create dttm index idx_spot_end_datetime_dttm               on project_022_all_techedge_spots(spot_end_datetime);
create lf   index idx_channel_lf                           on project_022_all_techedge_spots(channel);
create lf   index idx_channel_name_lf                      on project_022_all_viewing_records_20111107_20111209(channel_name);
create lf   index idx1                                     on channel_lookup                (service_key);
create lf   index idx2                                     on channel_lookup                (techedge_channel);
create lf   index idx3                                     on channel_lookup                (Techedge_STI_code);
create lf   index idx4                                     on channel_lookup                (Techedge_log_station_code);
create lf   index idx10                                    on project_022_all_viewing_records_20111107_20111209(service_key);
create lf   index idx6                                     on project_022_all_techedge_spots(sti_code);
create lf   index idx7                                     on project_022_all_techedge_spots(log_station);

create variable @counter int;
set @dt=@var_prog_period_start;

drop table project_022_daily_viewing
create table project_022_daily_viewing(account_number varchar(30)
                ,viewing_record_start_time_local datetime
                ,viewing_record_end_time_local_1 datetime
                ,viewing_record_end_time_local_10 datetime
                ,adjusted_event_start_time datetime
                ,weightings int
                ,service_key int
                ,capped_flag_1 tinyint
                ,capped_flag_10 tinyint
                ,play_back_speed int
);

create dttm index idxa on project_022_daily_viewing(viewing_record_start_time_local);
create dttm index idxb on project_022_daily_viewing(viewing_record_end_time_local_1);
create dttm index idxc on project_022_daily_viewing(viewing_record_end_time_local_10);
create dttm index idxd on project_022_daily_viewing(adjusted_event_start_time);
create dttm index idxa on project_022_daily_spots(spot_start_datetime);
create dttm index idxb on project_022_daily_spots(spot_end_datetime);
create lf index idx1 on project_022_daily_spots(lookup_combined);
create lf index idx11 on channel_lookup(lookup_combined);

while @dt <= @var_prog_period_end
begin
        truncate table project_022_daily_viewing
          insert into project_022_daily_viewing(account_number
                ,viewing_record_start_time_local
                ,viewing_record_end_time_local_1
                ,viewing_record_end_time_local_10
                ,adjusted_event_start_time
                ,weightings
                ,service_key
                ,capped_flag_1
                ,capped_flag_10
                ,play_back_speed
                )
          select account_number
                ,viewing_record_start_time_local
                ,viewing_record_end_time_local_1
                ,viewing_record_end_time_local_10
                ,adjusted_event_start_time
                ,weightings
                ,service_key
                ,capped_flag_1
                ,capped_flag_10
                ,play_back_speed
            from vespa_analysts.project_022_all_viewing_records_20111107_20111209
           where weighting_date = @dt
--             and play_back_speed =2
             and play_back_speed is null
             and capped_flag_10 in (0,1)

        truncate table project_022_daily_spots
          insert into project_022_daily_spots(id
                                            ,spot_start_datetime
                                            ,spot_end_datetime
                                            ,lookup_combined)
          select id
                ,spot_start_datetime
                ,spot_end_datetime
                ,lookup_combined
            from vespa_analysts.project_022_all_techedge_spots
           where spot_date = @dt
--           where spot_date between @dt - 7 and @dt



-- --channel specific
--           insert into project_022_accounts_spots(account_number, weightings, id, id_type)
--           select viw.account_number
--                 ,viw.weightings
--                 ,spt.id
--                 ,'full 1%'
--             from vespa_analysts.project_022_daily_viewing          as viw
--                  inner join channel_lookup                         as lkp on viw.service_key     = lkp.service_key
--                  inner join vespa_analysts.project_022_daily_spots as spt on lkp.lookup_combined = spt.lookup_combined
--            where viewing_record_start_time_local < spot_start_datetime
--              and viewing_record_end_time_local_1 > spot_end_datetime
--              and viewing_record_start_time_local <= dateadd(day,7,adjusted_event_start_time)
--              and capped_flag_1 in (0,1)
--              and (   play_back_speed is null
--                   or play_back_speed = 2)

--           insert into project_022_accounts_spots(account_number, weightings, id, id_type)
--           select viw.account_number
--                 ,viw.weightings
--                 ,spt.id
--                 ,'full 10%'
--             from vespa_analysts.project_022_daily_viewing          as viw
--                  inner join channel_lookup                         as lkp on viw.service_key     = lkp.service_key
--                  inner join vespa_analysts.project_022_daily_spots as spt on lkp.lookup_combined = spt.lookup_combined
--            where viewing_record_start_time_local  < spot_start_datetime
--              and viewing_record_end_time_local_10 > spot_end_datetime
--              and viewing_record_start_time_local <= dateadd(day,7,adjusted_event_start_time)
--              and capped_flag_10 in (0,1)
--
-- --any channel
--          truncate table project_022_temp
--            insert into project_022_temp(account_number, weightings, id, id_type)
--            select viw.account_number
--                  ,viw.weightings
--                  ,spt.id
--                  ,'other 1%' as id_type
--              from vespa_analysts.project_022_daily_viewing          as viw
--                   cross join vespa_analysts.project_022_daily_spots as spt
--             where viewing_record_start_time_local < spot_start_datetime
--               and viewing_record_end_time_local_1   > spot_end_datetime
--               and viewing_record_start_time_local <= dateadd(day,7,adjusted_event_start_time)
--               and (   play_back_speed is null
--                    or play_back_speed = 2)
--               and capped_flag_1 in (0,1)
--

           insert into project_022_accounts_spots(account_number, weightings, id, id_type)
           select viw.account_number
                 ,viw.weightings
                 ,spt.id
                 ,'other 10%'
             from vespa_analysts.project_022_daily_viewing          as viw
                  inner join vespa_analysts.project_022_daily_spots as spt
              on viewing_record_start_time_local < spot_start_datetime
              and viewing_record_end_time_local_10   > spot_end_datetime
--              and viewing_record_start_time_local <= dateadd(day,7,adjusted_event_start_time)
--            insert into project_022_accounts_spots(account_number, weightings, id, id_type)
--            select account_number
--                  ,weightings
--                  ,id
--                  ,id_type
--              from project_022_temp
--          group by account_number
--                  ,weightings
--                  ,id
--                  ,id_type
--

        truncate table project_022_daily_viewing
          insert into project_022_daily_viewing(account_number
                ,viewing_record_start_time_local
                ,viewing_record_end_time_local_1
                ,viewing_record_end_time_local_10
                ,adjusted_event_start_time
                ,weightings
                ,service_key
                ,capped_flag_1
                ,capped_flag_10
                ,play_back_speed
                )
          select account_number
                ,viewing_record_start_time_local
                ,viewing_record_end_time_local_1
                ,viewing_record_end_time_local_10
                ,adjusted_event_start_time
                ,weightings
                ,service_key
                ,capped_flag_1
                ,capped_flag_10
                ,play_back_speed
            from vespa_analysts.project_022_all_viewing_records_20111107_20111209
           where weighting_date = @dt
             and play_back_speed =2
--             and play_back_speed is null
             and capped_flag_10 in (0,1)

        truncate table project_022_daily_spots
          insert into project_022_daily_spots(id
                                            ,spot_start_datetime
                                            ,spot_end_datetime
                                            ,lookup_combined)
          select id
                ,spot_start_datetime
                ,spot_end_datetime
                ,lookup_combined
            from vespa_analysts.project_022_all_techedge_spots
           where spot_date between @dt - 7 and @dt

           insert into project_022_accounts_spots(account_number, weightings, id, id_type)
           select viw.account_number
                 ,viw.weightings
                 ,spt.id
                 ,'other 10%'
             from vespa_analysts.project_022_daily_viewing          as viw
                  inner join vespa_analysts.project_022_daily_spots as spt
              on viewing_record_start_time_local < spot_start_datetime
              and viewing_record_end_time_local_10   > spot_end_datetime
              and viewing_record_start_time_local <= dateadd(day,7,adjusted_event_start_time)

           set @dt = @dt + 1
           commit

end;


--Output--
drop table project_022_output;
  select spt.id
        ,spt.channel
        ,cast(spt.spot_start_datetime as date)    as spot_date
        ,cast(spt.spot_start_datetime as varchar) as spot_start
        ,cast(spt.spot_end_datetime   as varchar) as spot_end
        ,spt.duration
        ,spt.advertiser
        ,spt.brand
        ,spt.tvr                                         as barb_tvr
        ,spt.impacts                                     as barb_impacts
        ,sum(case when lnk.id is null then 0 else 1 end) as vespa_impacts_raw
        ,sum(weightings)                                 as vespa_impacts_weighted
        ,sum(onnet * weightings)                         as onnet
        ,sum(bb    * weightings)                         as bb

        ,sum(case when hd_decile     =  1 then weightings else 0 end) as hd_decile_1
        ,sum(case when hd_decile     =  2 then weightings else 0 end) as hd_decile_2
        ,sum(case when hd_decile     =  3 then weightings else 0 end) as hd_decile_3
        ,sum(case when hd_decile     =  4 then weightings else 0 end) as hd_decile_4
        ,sum(case when hd_decile     =  5 then weightings else 0 end) as hd_decile_5
        ,sum(case when hd_decile     =  6 then weightings else 0 end) as hd_decile_6
        ,sum(case when hd_decile     =  7 then weightings else 0 end) as hd_decile_7
        ,sum(case when hd_decile     =  8 then weightings else 0 end) as hd_decile_8
        ,sum(case when hd_decile     =  9 then weightings else 0 end) as hd_decile_9
        ,sum(case when hd_decile     = 10 then weightings else 0 end) as hd_decile_10
        ,sum(case when movies_decile =  1 then weightings else 0 end) as movies_decile_1
        ,sum(case when movies_decile =  2 then weightings else 0 end) as movies_decile_2
        ,sum(case when movies_decile =  3 then weightings else 0 end) as movies_decile_3
        ,sum(case when movies_decile =  4 then weightings else 0 end) as movies_decile_4
        ,sum(case when movies_decile =  5 then weightings else 0 end) as movies_decile_5
        ,sum(case when movies_decile =  6 then weightings else 0 end) as movies_decile_6
        ,sum(case when movies_decile =  7 then weightings else 0 end) as movies_decile_7
        ,sum(case when movies_decile =  8 then weightings else 0 end) as movies_decile_8
        ,sum(case when movies_decile =  9 then weightings else 0 end) as movies_decile_9
        ,sum(case when movies_decile = 10 then weightings else 0 end) as movies_decile_10

        ,sum(currently_has_hd_sub * weightings)                                                                           as hd
        ,sum(currently_has_movies * weightings)                                                                           as movies
        ,sum(hd_lapsed_enabled    * weightings)                                                                           as hd_lapsed
        ,sum(case when currently_has_movies = 0 and ever_had_movies = 1 then weightings                       else 0 end) as movies_lapsed
        ,sum(case when hd_model_score       > 0                         then hd_model_score      * weightings * .7194022356133004 else 0 end) as hd_model_score --these constants were calculated (in quick_calcs.sql to weight up the deciles to the sky base)
        ,sum(case when movies_model_score   > 0                         then movies_model_score  * weightings * .9203644912640983 else 0 end) as movies_model_score
    into project_022_output
    from vespa_analysts.project_022_all_techedge_spots                as spt
         left join project_022_accounts_spots                         as lnk on lnk.id             = spt.id
         left join vespa_analysts.project_022_sky_acounts_by_account  as acc on lnk.account_number = acc.account_number
   where id_type = 'full 10%' or id_type is null
group by spt.id
        ,spt.channel
        ,spot_date
        ,spot_start
        ,spot_end
        ,spt.duration
        ,spt.advertiser
        ,spt.brand
        ,barb_tvr
        ,barb_impacts
; --
--Output part 2
drop table project_022_output2;
  select id
        ,sum(case when hd_decile     =  1 then weightings else 0 end) as hd_decile_1
        ,sum(case when hd_decile     =  2 then weightings else 0 end) as hd_decile_2
        ,sum(case when hd_decile     =  3 then weightings else 0 end) as hd_decile_3
        ,sum(case when hd_decile     =  4 then weightings else 0 end) as hd_decile_4
        ,sum(case when hd_decile     =  5 then weightings else 0 end) as hd_decile_5
        ,sum(case when hd_decile     =  6 then weightings else 0 end) as hd_decile_6
        ,sum(case when hd_decile     =  7 then weightings else 0 end) as hd_decile_7
        ,sum(case when hd_decile     =  8 then weightings else 0 end) as hd_decile_8
        ,sum(case when hd_decile     =  9 then weightings else 0 end) as hd_decile_9
        ,sum(case when hd_decile     = 10 then weightings else 0 end) as hd_decile_10
        ,sum(case when movies_decile =  1 then weightings else 0 end) as movies_decile_1
        ,sum(case when movies_decile =  2 then weightings else 0 end) as movies_decile_2
        ,sum(case when movies_decile =  3 then weightings else 0 end) as movies_decile_3
        ,sum(case when movies_decile =  4 then weightings else 0 end) as movies_decile_4
        ,sum(case when movies_decile =  5 then weightings else 0 end) as movies_decile_5
        ,sum(case when movies_decile =  6 then weightings else 0 end) as movies_decile_6
        ,sum(case when movies_decile =  7 then weightings else 0 end) as movies_decile_7
        ,sum(case when movies_decile =  8 then weightings else 0 end) as movies_decile_8
        ,sum(case when movies_decile =  9 then weightings else 0 end) as movies_decile_9
        ,sum(case when movies_decile = 10 then weightings else 0 end) as movies_decile_10
        ,sum(weightings)                          as vespa_impacts_weighted
    into project_022_output2
    from project_022_accounts_spots                        as lnk
         inner join vespa_analysts.project_022_sky_acounts_by_account as acc on lnk.account_number = acc.account_number
   where id_type = 'other 10%'
group by id
;

---
drop table project_022_output2;
  select id
        ,sum(case when hd_decile     =  1 then weightings else 0 end) as hd_decile_1
        ,sum(case when hd_decile     =  2 then weightings else 0 end) as hd_decile_2
        ,sum(case when hd_decile     =  3 then weightings else 0 end) as hd_decile_3
        ,sum(case when hd_decile     =  4 then weightings else 0 end) as hd_decile_4
        ,sum(case when hd_decile     =  5 then weightings else 0 end) as hd_decile_5
        ,sum(case when hd_decile     =  6 then weightings else 0 end) as hd_decile_6
        ,sum(case when hd_decile     =  7 then weightings else 0 end) as hd_decile_7
        ,sum(case when hd_decile     =  8 then weightings else 0 end) as hd_decile_8
        ,sum(case when hd_decile     =  9 then weightings else 0 end) as hd_decile_9
        ,sum(case when hd_decile     = 10 then weightings else 0 end) as hd_decile_10
        ,sum(case when movies_decile =  1 then weightings else 0 end) as movies_decile_1
        ,sum(case when movies_decile =  2 then weightings else 0 end) as movies_decile_2
        ,sum(case when movies_decile =  3 then weightings else 0 end) as movies_decile_3
        ,sum(case when movies_decile =  4 then weightings else 0 end) as movies_decile_4
        ,sum(case when movies_decile =  5 then weightings else 0 end) as movies_decile_5
        ,sum(case when movies_decile =  6 then weightings else 0 end) as movies_decile_6
        ,sum(case when movies_decile =  7 then weightings else 0 end) as movies_decile_7
        ,sum(case when movies_decile =  8 then weightings else 0 end) as movies_decile_8
        ,sum(case when movies_decile =  9 then weightings else 0 end) as movies_decile_9
        ,sum(case when movies_decile = 10 then weightings else 0 end) as movies_decile_10
        ,sum(weightings)                          as vespa_impacts_weighted
    into project_022_output2
    from project_022_accounts_spots                        as lnk
         inner join vespa_analysts.project_022_sky_acounts_by_account as acc on lnk.account_number = acc.account_number
   where id_type = 'other 10%'
group by id
;






--sky model totals
  select sum(score)
        ,decile
    from models.model_scores as mod
   where model_run_date = '2011-12-21'
     and model_name     = 'HD Mailed'
group by decile
;

sum(mod.score)  decile
48611.9612429163575     1
26925.0524890723765     2
19498.800424919498      3
14527.9568989991069     4
10607.2124440585911     5
7361.1395189708209      6
4435.81565400763988     7
2178.80196299476862     8
333.517222000750184     9
126.351746999808863     10

  select sum(score)
        ,decile
    from models.model_scores as mod
   where model_run_date = '2011-12-21'
     and model_name     = 'Movies Mailed'
group by decile
;

sum(mod.score)  decile
151439.453500130296     1
72858.319574995327      2
52258.4251050133109     3
40408.9341239830911     4
32076.1968729749262     5
25534.8069530314356     6
20125.3427260094166     7
15951.5766430158049     8
12291.2184660062417     9
8178.55818800808191     10

--vespa model totals
select sum(hd_model_score)
,hd_decile
from project_022_sky_acounts_by_account
group by hd_decile

2090.2607807777822      1
1152.86632191762328     2
764.330804014578462     3
533.022794086486101     4
306.800038206391037     5
182.281255938112736     6
104.49669260205701      7
62.9549259031191468     8
5.31622399957268536     9
2.13692899833404243     10

select sum(movies_model_score)
,movies_decile
from project_022_sky_acounts_by_account
group by movies_decile

11353.8422532975674     1
3786.90479526668787     2
2461.89500145614147     3
1827.48017673939466     4
1354.40381322801113     5
1020.37469607591629     6
692.552624464035034     7
435.314733013510704     8
237.613842396065593     9
90.9684541285969257     10

  select id
        ,sum(case when hd_decile= 1 then 48611.9612429163575/2090.2607807777822
                  when hd_decile= 2 then 26925.0524890723765/1152.86632191762328
                  when hd_decile= 3 then 19498.800424919498/764.330804014578462
                  when hd_decile= 4 then 14527.9568989991069/533.022794086486101
                  when hd_decile= 5 then 10607.2124440585911/306.800038206391037
                  when hd_decile= 6 then 7361.1395189708209/182.281255938112736
                  when hd_decile= 7 then 4435.81565400763988/104.49669260205701
                  when hd_decile= 8 then 2178.80196299476862/62.9549259031191468
                  when hd_decile= 9 then 333.517222000750184/5.31622399957268536
                  when hd_decile=10 then 126.351746999808863/2.13692899833404243
                  else 0 end * weightings) as hd_score
        ,sum(case when movies_decile= 1 then 151439.453500130296/11353.8422532975674
                  when movies_decile= 2 then 72858.319574995327/3786.90479526668787
                  when movies_decile= 3 then 52258.4251050133109/2461.89500145614147
                  when movies_decile= 4 then 40408.9341239830911/1827.48017673939466
                  when movies_decile= 5 then 32076.1968729749262/1354.40381322801113
                  when movies_decile= 6 then 25534.8069530314356/1020.37469607591629
                  when movies_decile= 7 then 20125.3427260094166/692.552624464035034
                  when movies_decile= 8 then 15951.5766430158049/435.314733013510704
                  when movies_decile= 9 then 12291.2184660062417/237.613842396065593
                  when movies_decile=10 then 8178.55818800808191/90.9684541285969257
                  else 0 end * weightings) as movies_score
            from project_022_accounts_spots                                  as lnk
                 inner join vespa_analysts.project_022_sky_acounts_by_account as acc on lnk.account_number = acc.account_number
           where id_type = 'ful 10%' or id_type is null
group by id


--3 April additional questions
--Question1 affluence by movies crosstab for Vespa Panel
select top 10 * from project_022_sky_acounts_by_account
alter table project_022_sky_acounts_by_account add cb_key_family bigint;
alter table project_022_sky_acounts_by_account add affluence varchar(50);

update project_022_sky_acounts_by_account as bas
   set bas.cb_key_family = sav.cb_key_family
  from sk_prod.cust_single_account_view as sav
 where bas.account_number = sav.account_number
;

SELECT  cb_row_id
       ,account_number
       ,CASE WHEN P1 = 1  THEN 1
             WHEN P2 = 1  THEN 2
             ELSE              3
         END AS Correspondent
       ,rank() over(PARTITION BY account_number ORDER BY Correspondent asc, cb_row_id desc) as rank
 INTO  #ILU
 FROM (
            SELECT  ilu.cb_row_id
                   ,base.account_number
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'P1' THEN 1 ELSE 0 END) as P1
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'P2' THEN 1 ELSE 0 END) as P2
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'OR' THEN 1 ELSE 0 END) as OR1
              FROM  sk_prod.ilu AS ilu
                    INNER JOIN project_022_sky_acounts_by_account as base  on ilu.cb_key_family = base.cb_key_family
                                               and  base.cb_key_family is not null
                                               and  base.cb_key_family > 0
          GROUP BY  ilu.cb_row_id, base.account_number
            HAVING  P1 + P2 + OR1 > 0
        )as tgt;

DELETE FROM #ILU where rank > 1;

UPDATE project_022_sky_acounts_by_account
   SET affluence = CASE WHEN ilu.ilu_hhafflu in (1,2,3,4)  THEN 'Very Low'
                        WHEN ilu.ilu_hhafflu in (5,6)      THEN 'Low'
                        WHEN ilu.ilu_hhafflu in (7,8)      THEN 'Mid Low'
                        WHEN ilu.ilu_hhafflu in (9,10)     THEN 'Mid'
                        WHEN ilu.ilu_hhafflu in (11,12)    THEN 'Mid High'
                        WHEN ilu.ilu_hhafflu in (13,14,15) THEN 'High'
                        WHEN ilu.ilu_hhafflu in (16,17)    THEN 'Very High'
                        ELSE                                    'Unknown'
                      END
  FROM project_022_sky_acounts_by_account as base
       INNER JOIN #ILU on base.account_number = #ilu.account_number
       INNER JOIN sk_prod.ilu as ilu on #ilu.cb_row_id = ilu.cb_row_id;

alter table project_022_sky_acounts_by_account add household_composition int;

SELECT   cb_key_household
        ,cb_row_id
        ,head_of_household
        ,rank() over(partition by cb_key_household ORDER BY head_of_household desc, cb_row_id desc) as rank_hh
INTO #cv_keys
FROM sk_prod.EXPERIAN_CONSUMERVIEW;

DELETE FROM #cv_keys WHERE rank_hh != 1

--to flag the individual or the family variable
CREATE UNIQUE HG INDEX idx01 ON #cv_keys(cb_row_id);
CREATE        HG INDEX idx02 ON #cv_keys(cb_key_household);
CREATE        LF INDEX idx04 ON #cv_keys(rank_hh);

UPDATE #cv_keys
   SET rank_hh = NULL
WHERE cb_key_household IS NULL
    OR cb_key_household = 0;

alter table project_022_sky_acounts_by_account add cb_key_household bigint;

update project_022_sky_acounts_by_account as bas
   set bas.cb_key_household = lnk.cb_key_household
  from project_022_sky_accounts as lnk
 where bas.account_number = lnk.account_number

UPDATE  project_022_sky_acounts_by_account as bas
   SET  bas.household_composition  = cv.household_composition
 FROM #cv_keys
      INNER JOIN sk_prod.EXPERIAN_CONSUMERVIEW  AS cv ON #cv_keys.cb_row_id = cv.cb_row_id
where bas.cb_key_household = #cv_keys.cb_key_household AND #cv_keys.rank_hh = 1
;
COMMIT;

select top 10( cb_key_household) from project_022_sky_acounts_by_account








--Question1 output by affluence
select affluence
      ,sum(currently_has_movies) as yes
      ,sum(1-currently_has_movies) as no
from project_022_sky_acounts_by_account
group by affluence
;

--Question1 output by HH comp
select case household_composition when 00 then 'Families'
                                  when 01 then 'Extended family'
                                  when 02 then 'Extended household'
                                  when 03 then 'Pseudo family'
                                  when 04 then 'Single male'
                                  when 05 then 'Single female'
                                  when 06 then 'Male homesharers'
                                  when 07 then 'Female homesharers'
                                  when 08 then 'Mixed homesharers'
                                  when 09 then 'Abbreviated male families'
                                  when 10 then 'Abbreviated female families'
                                  when 11 then 'Multi-occupancy dwelling'
                                  else  'Unclassified'
         end as hhcomp
      ,sum(currently_has_movies) as yes
      ,sum(1-currently_has_movies) as no
from project_022_sky_acounts_by_account
group by hhcomp
;

--Question1 output by HH comp and Affluence - all
select sum(case when household_composition = 00 then 1 else 0 end) as 'Families'
      ,sum(case when household_composition = 01 then 1 else 0 end) as 'Extended family'
      ,sum(case when household_composition = 02 then 1 else 0 end) as 'Extended household'
      ,sum(case when household_composition = 03 then 1 else 0 end) as 'Pseudo family'
      ,sum(case when household_composition = 04 then 1 else 0 end) as 'Single male'
      ,sum(case when household_composition = 05 then 1 else 0 end) as 'Single female'
      ,sum(case when household_composition = 06 then 1 else 0 end) as 'Male homesharers'
      ,sum(case when household_composition = 07 then 1 else 0 end) as 'Female homesharers'
      ,sum(case when household_composition = 08 then 1 else 0 end) as 'Mixed homesharers'
      ,sum(case when household_composition = 09 then 1 else 0 end) as 'Abbreviated male families'
      ,sum(case when household_composition = 10 then 1 else 0 end) as 'Abbreviated female families'
      ,sum(case when household_composition = 11 then 1 else 0 end) as 'Multi-occupancy dwelling'
      ,sum(case when household_composition is null then 1 else 0 end) as 'Unclassified'
,affluence
from project_022_sky_acounts_by_account
group by affluence
;

--Question1 output by HH comp and Affluence - has movies
select sum(case when household_composition = 00 then 1 else 0 end) as 'Families'
      ,sum(case when household_composition = 01 then 1 else 0 end) as 'Extended family'
      ,sum(case when household_composition = 02 then 1 else 0 end) as 'Extended household'
      ,sum(case when household_composition = 03 then 1 else 0 end) as 'Pseudo family'
      ,sum(case when household_composition = 04 then 1 else 0 end) as 'Single male'
      ,sum(case when household_composition = 05 then 1 else 0 end) as 'Single female'
      ,sum(case when household_composition = 06 then 1 else 0 end) as 'Male homesharers'
      ,sum(case when household_composition = 07 then 1 else 0 end) as 'Female homesharers'
      ,sum(case when household_composition = 08 then 1 else 0 end) as 'Mixed homesharers'
      ,sum(case when household_composition = 09 then 1 else 0 end) as 'Abbreviated male families'
      ,sum(case when household_composition = 10 then 1 else 0 end) as 'Abbreviated female families'
      ,sum(case when household_composition = 11 then 1 else 0 end) as 'Multi-occupancy dwelling'
      ,sum(case when household_composition is null then 1 else 0 end) as 'Unclassified'
,affluence
from project_022_sky_acounts_by_account
where currently_has_movies =1
group by affluence
;




--Question2 product takeup
alter table project_022_sky_acounts_by_account add hd_first_view date;
alter table project_022_sky_acounts_by_account add movies_first_view date;
alter table project_022_sky_acounts_by_account add hd_takeup date;
alter table project_022_sky_acounts_by_account add movies_takeup date;

  select * into project_022_accounts_spots_cut
    from project_022_accounts_spots
   where id_type = 'ful 10%'
;

create lf index idx_id_type_lf        on project_022_accounts_spots_cut(id_type);
create hg index idx_account_number_hg on project_022_accounts_spots_cut(account_number);
create hg index idx_id_hg             on project_022_accounts_spots_cut(id);

  select min(spt.id) as minid
        ,account_number
    into V022_minid_hd
    from project_022_accounts_spots_cut            as bas
         inner join project_022_all_techedge_spots as spt on bas.id = spt.id
   where brand   = 'Sky plus high definition'
group by account_number
;

  select min(spt.id) as minid
        ,account_number
    into V022_minid_movies
    from project_022_accounts_spots_cut            as bas
         inner join project_022_all_techedge_spots as spt on bas.id = spt.id
   where brand   <> 'Sky plus high definition'
     and advertiser like 'British sky broadcas%'
group by account_number
;

--redo the above query for WCR film codes only
drop table V022_minid_movies;
  select min(spt.id) as minid
        ,account_number
    into V022_minid_movies
    from project_022_accounts_spots_early            as bas
         inner join project_022_all_techedge_spots as spt on bas.id = spt.id
   where brand   like 'Sky movies%'
group by account_number
;

  update project_022_sky_acounts_by_account as bas
     set bas.hd_first_view = spt.spot_date
    from V022_minid_hd                             as mni
         inner join project_022_all_techedge_spots as spt on mni.minid = spt.id
   where bas.account_number = mni.account_number
;

  update project_022_sky_acounts_by_account as bas
     set bas.movies_first_view = spt.spot_date
    from V022_minid_movies                         as mni
         inner join project_022_all_techedge_spots as spt on mni.minid = spt.id
   where bas.account_number = mni.account_number
;

  select bas.account_number
    into #has_hd
    from sk_prod.cust_subs_hist                        as csh
         inner join project_022_sky_acounts_by_account as bas on csh.account_number = bas.account_number
   where subscription_sub_type = 'DTV HD'
     and status_code in ('AC','AB','PC')
     and effective_from_dt < hd_first_view
     and effective_to_dt   > hd_first_view
;
  update project_022_sky_acounts_by_account as bas
     set hd_takeup = '9999-09-09'
    from #has_hd as has
   where bas.account_number = has.account_number
;

  select bas.account_number
        ,min(effective_from_dt) as mindt
    into #mindt
    from sk_prod.cust_subs_hist                        as csh
         inner join project_022_sky_acounts_by_account as bas on csh.account_number = bas.account_number
   where subscription_sub_type = 'DTV HD'
     and effective_to_dt       > effective_from_dt
     and status_code in ('AC')
     and (   effective_from_dt     > hd_first_view
          or (hd_first_view is null and effective_from_dt > '2011-11-11'))
group by bas.account_number
;

  update project_022_sky_acounts_by_account as bas
     set bas.hd_takeup = mnd.mindt
    from #mindt as mnd
   where bas.account_number = mnd.account_number
     and hd_takeup is null
;

  select bas.account_number
    into #has_movies
    from sk_prod.cust_subs_hist                        as csh
         inner join project_022_sky_acounts_by_account as bas on csh.account_number             = bas.account_number
         inner join sk_prod.cust_entitlement_lookup    as cel on csh.current_short_description  = cel.short_description
   where csh.subscription_sub_type = 'DTV Primary Viewing'
     and status_code in ('AC','AB','PC')
     and effective_from_dt < movies_first_view
     and effective_to_dt   > movies_first_view
     and cel.prem_movies > 0
;

  update project_022_sky_acounts_by_account as bas
     set movies_takeup = '9999-09-09'
    from #has_movies as has
   where bas.account_number = has.account_number
;

  select bas.account_number
        ,min(effective_from_dt) as mindt
    into #minmvdt
    from sk_prod.cust_subs_hist                        as csh
         inner join project_022_sky_acounts_by_account as bas on csh.account_number             = bas.account_number
         inner join sk_prod.cust_entitlement_lookup    as ncl on csh.current_short_description  = ncl.short_description
         inner join sk_prod.cust_entitlement_lookup    as ocl on csh.previous_short_description = ocl.short_description
   where csh.subscription_sub_type = 'DTV Primary Viewing'
     and effective_to_dt       > effective_from_dt
     and status_code in ('AC')
     and (   effective_from_dt     > movies_first_view
          or (movies_first_view is null and effective_from_dt > '2011-11-11'))
     and ncl.prem_movies > 0
     and ocl.prem_movies = 0
group by bas.account_number
;

  update project_022_sky_acounts_by_account as bas
     set bas.movies_takeup = mnd.mindt
    from #minmvdt as mnd
   where bas.account_number = mnd.account_number
     and (movies_takeup <> '9999-09-09' or movies_takeup is null)
;

  select sum(case when hd_first_view     is not null                                                          and hd_takeup     <> '9999-09-09' then 1 else 0 end) as hd_viewed
        ,sum(case when hd_first_view     is not null and hd_takeup     <= dateadd(day, 31, hd_first_view)     and hd_takeup     <> '9999-09-09' then 1 else 0 end) as hd_viewed_taken
        ,sum(case when hd_first_view     is     null                                                          and hd_takeup     <> '9999-09-09' then 1 else 0 end) as hd_not_viewed
        ,sum(case when hd_first_view     is     null and hd_takeup     <= '2011-12-12'                        and hd_takeup     <> '9999-09-09' then 1 else 0 end) as hd_not_viewed_taken
        ,sum(case when movies_first_view is not null                                                          and movies_takeup <> '9999-09-09' then 1 else 0 end) as movies_viewed
        ,sum(case when movies_first_view is not null and movies_takeup <= dateadd(day, 31, movies_first_view) and movies_takeup <> '9999-09-09' then 1 else 0 end) as movies_viewed_taken
        ,sum(case when movies_first_view is     null                                                          and movies_takeup <> '9999-09-09' then 1 else 0 end) as movies_not_viewed
        ,sum(case when movies_first_view is     null and movies_takeup <= '2011-12-12'                        and movies_takeup <> '9999-09-09' then 1 else 0 end) as movies_not_viewed_taken
    from project_022_sky_acounts_by_account
;

drop table #temp;
  select case when movies_first_view is null then 0 else 1 end                                                                                                                                 as viewed
        ,case when (movies_first_view is not null and movies_takeup <= dateadd(day, 31, movies_first_view)) or (movies_first_view is null and movies_takeup <= '2011-12-12') then 1 else 0 end as taken
        ,affluence
        ,household_composition
        ,sum(movies_weightings) as value
--        ,sum(1) as value
    into #temp
    from project_022_sky_acounts_by_account
   where movies_takeup <> '9999-09-09'
group by viewed, taken,affluence,household_composition
;

--%s
select affluence
       ,sum(cast(case when household_composition = 00 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 00 then value else 0 end) as Families
       ,sum(cast(case when household_composition = 01 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 01 then value else 0 end) as Extended_family
       ,sum(cast(case when household_composition = 02 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 02 then value else 0 end) as Extended_household
       ,sum(cast(case when household_composition = 03 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 03 then value else 0 end) as Pseudo_family
       ,sum(cast(case when household_composition = 04 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 04 then value else 0 end) as Single_male
       ,sum(cast(case when household_composition = 05 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 05 then value else 0 end) as Single_female
       ,sum(cast(case when household_composition = 06 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 06 then value else 0 end) as Male_homesharers
       ,sum(cast(case when household_composition = 07 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 07 then value else 0 end) as Female_homesharers
       ,sum(cast(case when household_composition = 08 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 08 then value else 0 end) as Mixed_homesharers
       ,sum(cast(case when household_composition = 09 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 09 then value else 0 end) as Abbreviated_male_families
       ,sum(cast(case when household_composition = 10 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 10 then value else 0 end) as Abbreviated_female_families
--       ,sum(cast(case when household_composition = 11 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 11 then value else 0 end) as Multi_occupancy_dwelling
--       ,sum(cast(case when household_composition not between 0 and 11 and taken = 1 then value else 0 end as float)) / sum(case when household_composition not between 0 and 11 and taken = 1 then value else 0 end) as Unclassified
       ,sum(cast(case when                                taken = 1 then value else 0 end as float)) / sum(value) as total
from #temp
where viewed = 1
--where viewed = 0
--and taken=1
group by affluence

--total %s
select sum(cast(case when household_composition = 00 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 00 then value else 0 end) as Families
       ,sum(cast(case when household_composition = 01 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 01 then value else 0 end) as Extended_family
       ,sum(cast(case when household_composition = 02 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 02 then value else 0 end) as Extended_household
       ,sum(cast(case when household_composition = 03 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 03 then value else 0 end) as Pseudo_family
       ,sum(cast(case when household_composition = 04 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 04 then value else 0 end) as Single_male
       ,sum(cast(case when household_composition = 05 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 05 then value else 0 end) as Single_female
       ,sum(cast(case when household_composition = 06 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 06 then value else 0 end) as Male_homesharers
       ,sum(cast(case when household_composition = 07 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 07 then value else 0 end) as Female_homesharers
       ,sum(cast(case when household_composition = 08 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 08 then value else 0 end) as Mixed_homesharers
       ,sum(cast(case when household_composition = 09 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 09 then value else 0 end) as Abbreviated_male_families
       ,sum(cast(case when household_composition = 10 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 10 then value else 0 end) as Abbreviated_female_families
--       ,sum(cast(case when household_composition = 11 and taken = 1 then value else 0 end as float)) / sum(case when household_composition = 11 then value else 0 end) as Multi_occupancy_dwelling
--       ,sum(cast(case when household_composition not between 0 and 11 and taken = 1 then value else 0 end as float)) / sum(case when household_composition not between 0 and 11 and taken = 1 then value else 0 end) as Unclassified
       ,sum(cast(case when                                taken = 1 then value else 0 end as float)) / sum(value) as total
from #temp
where viewed = 1
--where viewed = 0
and taken=1


select affluence
       ,sum(case when household_composition = 00 then value else 0 end) as Families
       ,sum(case when household_composition = 01 then value else 0 end) as Extended_family
       ,sum(case when household_composition = 02 then value else 0 end) as Extended_household
       ,sum(case when household_composition = 03 then value else 0 end) as Pseudo_family
       ,sum(case when household_composition = 04 then value else 0 end) as Single_male
       ,sum(case when household_composition = 05 then value else 0 end) as Single_female
       ,sum(case when household_composition = 06 then value else 0 end) as Male_homesharers
       ,sum(case when household_composition = 07 then value else 0 end) as Female_homesharers
       ,sum(case when household_composition = 08 then value else 0 end) as Mixed_homesharers
       ,sum(case when household_composition = 09 then value else 0 end) as Abbreviated_male_families
       ,sum(case when household_composition = 10 then value else 0 end) as Abbreviated_female_families
       ,sum(case when household_composition = 11 then value else 0 end) as Multi_occupancy_dwelling
       ,sum(case when household_composition not between 0 and 11 and taken = 1 then value else 0 end) as Unclassified
       ,sum(value) as total
from #temp
--where viewed = 1
where viewed = 0
--and taken=1
group by affluence

select viewed,taken,affluence
       ,case household_composition when 00 then 'Families'
       when 01 then 'Extended_family'
       when 02 then 'Extended_household'
       when 03 then 'Pseudo_family'
       when 04 then 'Single_male'
       when 05 then 'Single_female'
       when 06 then 'Male_homesharers'
       when 07 then 'Female_homesharers'
       when 08 then 'Mixed_homesharers'
       when 09 then 'Abbreviated_male_families'
       when 10 then 'Abbreviated_female_families'
       when 11 then 'Multi_occupancy_dwelling'
       else 'Unclassified' end as hhcomp
       ,sum(value) as total
from #temp
group by viewed,taken,affluence, hhcomp


alter table project_022_sky_acounts_by_account add movies_scaling_segment_ID int;
  update vespa_analysts.project_022_sky_acounts_by_account as bas
     set movies_scaling_segment_ID = scl.scaling_segment_ID
    from vespa_analysts.scaling_dialback_intervals as scl
   where bas.account_number = scl.account_number
     and bas.movies_first_view between scl.reporting_starts and scl.reporting_ends
;

  update vespa_analysts.project_022_sky_acounts_by_account as bas
     set movies_scaling_segment_ID = scl.scaling_segment_ID
    from vespa_analysts.scaling_dialback_intervals as scl
   where bas.account_number = scl.account_number
     and bas.movies_first_view is null
     and '2011-11-11' between scl.reporting_starts and scl.reporting_ends
;

select top 100 *
from vespa_analysts.project_022_sky_acounts_by_account as bas
inner join scaling_dialback_intervals as scl on bas.account_number = scl.account_number
where movies_first_view is null
and '2011-11-11' between reporting_starts and reporting_ends

alter table project_022_sky_acounts_by_account add movies_weightings float default 0;
  update vespa_analysts.project_022_sky_acounts_by_account as bas
     set bas.movies_weightings = wei.weighting
    from vespa_analysts.scaling_weightings as wei
   where bas.movies_first_view         = wei.scaling_day
     and bas.movies_scaling_segment_ID = wei.scaling_segment_ID
;


;




















  select sum(case when movies_first_view is not null
           and movies_takeup <> '9999-09-09' then 1 else 0 end) as movies_viewed
    from project_022_sky_acounts_by_account
;

  select sum(case when (movies_first_view is not null
  and movies_takeup <= dateadd(day, 31, movies_first_view)) then 1 else 0 end)
--  or (movies_first_view is null and movies_takeup <= '2011-12-12') then 1 else 0 end) as taken
    from project_022_sky_acounts_by_account
   where movies_takeup <> '9999-09-09'
;





;

select count(1),currently_has_movies,case when movies_takeup='9999-09-09' or movies_takeup is null then movies_takeup else '1111-11-11' end as taken
from project_022_sky_acounts_by_account
group by currently_has_movies,taken

select sum(value) from #temp



select top 100 * from project_022_sky_acounts_by_account





