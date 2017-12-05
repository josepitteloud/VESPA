
----Project 022 - ATL Optimisation

---SkProd10

---Create Lookup tble of Tech Edge Channel and Corresponding BARB Channels
--drop table vespa_analysts.channel_name_and_techedge_channel;
create table vespa_analysts.channel_name_and_techedge_channel
(channel varchar(90)
,channel_name_grouped varchar(90)
,channel_name_inc_hd varchar(90)
,techedge_channel varchar(90)
)
;

input into vespa_analysts.channel_name_and_techedge_channel from 'G:\RTCI\Sky Projects\Vespa\Phase1b\Channel Lookup\Channel Lookup Info With Techedge Channelv2.csv' format ascii;
commit;

create table vespa_analysts.project_022_all_techedge_spots
(channel varchar(90)
,Spot_Date date
,spot_start varchar(10)
,spot_end varchar(10)
,duration tinyint
,Advertiser varchar(20)
,brand varchar(35)
,TVR real
,Impacts real
)
;
input into vespa_analysts.project_022_all_techedge_spots from 'C:\Users\barnetd\Documents\Project 022 - ATL Optimisation\Spot Data.csv' format ascii;
commit;

---Add Vespa Channel Detail on to spot data


--alter table vespa_analysts.project_022_all_techedge_spots delete channel_name_grouped;
alter table vespa_analysts.project_022_all_techedge_spots add channel_name_grouped varchar(90);

update vespa_analysts.project_022_all_techedge_spots
set channel_name_grouped = b.channel_name_inc_hd
from vespa_analysts.project_022_all_techedge_spots as a
left outer join vespa_analysts.channel_name_and_techedge_channel as b
on upper(a.channel)=upper(b.techedge_channel)
;
commit;

--Part B - Viewing data for programmes broadcast between 7th Nov and 2nd Dec---


  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2011-11-07';
SET @var_prog_period_end    = '2011-12-09';


SET @var_cntr = 0;
SET @var_num_days = 33;       -- 

-- To store all the viewing records:
create table vespa_analysts.VESPA_all_viewing_records_20111107_20111209 ( -- drop table vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    cb_row_ID                       bigint      not null primary key
    ,Account_Number                 varchar(20) not null
    ,Subscriber_Id                  decimal(8,0) not null
    ,Cb_Key_Household               bigint
    ,Cb_Key_Family                  bigint
    ,Cb_Key_Individual              bigint
    ,Event_Type                     varchar(20) not null
    ,X_Type_Of_Viewing_Event        varchar(40) not null
    ,Adjusted_Event_Start_Time      datetime
    ,X_Adjusted_Event_End_Time      datetime
    ,X_Viewing_Start_Time           datetime
    ,X_Viewing_End_Time             datetime
    ,Tx_Start_Datetime_UTC          datetime
    ,Tx_End_Datetime_UTC            datetime
    ,Recorded_Time_UTC              datetime
    ,Play_Back_Speed                decimal(4,0)
    ,X_Event_Duration               decimal(10,0)
    ,X_Programme_Duration           decimal(10,0)
    ,X_Programme_Viewed_Duration    decimal(10,0)
    ,X_Programme_Percentage_Viewed  decimal(3,0)
    ,X_Viewing_Time_Of_Day          varchar(15)
    ,Programme_Trans_Sk             bigint      not null
    ,Channel_Name                   varchar(30)
    ,Epg_Title                      varchar(50)
    ,Genre_Description              varchar(30)
    ,Sub_Genre_Description          varchar(30)
    ,x_cumul_programme_viewed_duration bigint
);
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
, sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration 
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where 
video_playing_flag = 1
     and adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
          or (x_type_of_viewing_event = (''Other Service Viewing Event'')
              and x_si_service_type = ''High Definition TV test service''))
     and panel_id in ( 4,5)'
      ;


  -- ####### Loop through to populate table: Sybase Interactive style (not entirely tested) ######
--FLT_1: LOOP

    --EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd'));

    --SET @var_cntr = @var_cntr + 1;
    --IF @var_cntr > @var_num_days THEN LEAVE FLT_1;
    --END IF ;

--END LOOP FLT_1;
  -- ####### End of loop (this loop structure not tested yet) ######

  -- ####### Alternate Loop: WinSQL style (tested, good) ######
while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;


--select play_back_speed , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111107_20111209 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111107_20111209 group by day_view order by day_view;


commit;

alter table vespa_analysts.VESPA_all_viewing_records_20111107_20111209 add live tinyint;

update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.VESPA_all_viewing_records_20111107_20111209
;
commit;

--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20111107_20111209;

---Create Capping rules limits for Nov and dec--

---Add on derived variables for viewing

commit;

alter table vespa_analysts.VESPA_all_viewing_records_20111107_20111209 add channel_name_inc_hd varchar(40);

update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.VESPA_all_viewing_records_20111107_20111209 as base
left outer join vespa_analysts.channel_name_and_techedge_channel  det
 on base.Channel_Name = det.Channel
;
commit;


-- add indexes to improve performance
create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(subscriber_id);
create dttm index idx2 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(recorded_time_utc);
create lf index idx4 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(live)
create dttm index idx5 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(x_viewing_end_time);
create hng index idx7 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(programme_trans_sk);
create hg index idx9 on vespa_analysts.VESPA_all_viewing_records_20111107_20111209(channel_name_inc_hd);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records
update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    set capped_x_viewing_start_time =
        case  
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
        end
        , capped_x_viewing_end_time =
        case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) > x_viewing_end_time then x_viewing_end_time
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, adjusted_event_start_time)
        end
from
        vespa_analysts.VESPA_all_viewing_records_20111107_20111209 base left outer join vespa_201111_201112_max_caps caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    set capped_flag = 
        case
            when capped_x_viewing_end_time < x_viewing_end_time then 1
            when capped_x_viewing_start_time is null then 2
            else 0
        end
;
commit;

-- cap based on min duration of seconds (from min_cap) and set capping flag
-- this nullifies capped_x times as for long duration cap and sets capped_flag = 3
-- note that some capped_flag = 1 records may also be updated if the capping of the end of
-- a long view resulted in a very short view
update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
    set capped_x_viewing_start_time = null
        , capped_x_viewing_end_time = null
        , capped_x_programme_viewed_duration = null
        , capped_flag = 3
    from
        vespa_201111_201112_min_cap
    where
        capped_x_programme_viewed_duration < cap_secs 
;
commit;

--select top 500 *  from vespa_analysts.VESPA_all_viewing_records_20111107_20111209 where capped_flag=1;

-----Remove any Records that have a capped flag of 2 or 3---

delete from vespa_analysts.VESPA_all_viewing_records_20111107_20111209
where capped_flag in (2,3)
;
commit;



---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table vespa_analysts.VESPA_all_viewing_records_20111107_20111209 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.VESPA_all_viewing_records_20111107_20111209 add viewing_record_start_time_local datetime;


alter table vespa_analysts.VESPA_all_viewing_records_20111107_20111209 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.VESPA_all_viewing_records_20111107_20111209 add viewing_record_end_time_local datetime;

update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.VESPA_all_viewing_records_20111107_20111209
;
commit;


---
update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.VESPA_all_viewing_records_20111107_20111209
;
commit;

--select top 100 * from vespa_analysts.VESPA_all_viewing_records_20111107_20111209;

update vespa_analysts.VESPA_all_viewing_records_20111107_20111209
set viewing_record_start_time_local= case 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,viewing_record_start_time_utc) 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,viewing_record_start_time_utc) 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,viewing_record_start_time_utc) 
                    else viewing_record_start_time_utc  end
,viewing_record_end_time_local=case 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,viewing_record_end_time_utc) 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,viewing_record_end_time_utc) 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,viewing_record_end_time_utc) 
                    else viewing_record_end_time_utc  end
from vespa_analysts.VESPA_all_viewing_records_20111107_20111209
;
commit;


--select top 100 * from vespa_analysts.VESPA_all_viewing_records_20111107_20111209;




--Create Minute By Minute by channel summary (Unweighted)---



----Run Weighted by channel---

---Run only for Sky1 Initially---
--select  distinct channel_name_inc_hd from vespa_analysts.VESPA_all_viewing_records_20111107_20111209 order by channel_name_inc_hd ;

create variable @min_tx_start_time_local datetime;
create variable @max_tx_end_time_local datetime;
create variable @minute datetime;

select  * into  vespa_analysts.VESPA_all_viewing_records_20111107_20111209_sky1 from vespa_analysts.VESPA_all_viewing_records_20111107_20111209 where channel_name_inc_hd= 'Sky 1';
commit;
set @min_tx_start_time_local = cast ('2011-11-07 00:00:00' as datetime);
set @max_tx_end_time_local = cast ('2011-12-03 00:00:00' as datetime);

---Loop by Channel---
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;

--select channel_name_inc_hd , count(*) from  vespa_analysts.VESPA_all_viewing_records_20110811_20110818 group by channel_name_inc_hd
--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20111107_20111209;
--select count(*) from vespa_analysts.All_viewing_minute_by_minute_20110811;
--if object_id('vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec') is not null drop table vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec;

if object_id('vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec_sky1') is not null drop table vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec_sky1;
commit;
create table vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec_sky1
(
subscriber_id  bigint           null
--,channel_name_inc_hd  varchar(40)
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live          smallint            not null
,seconds_viewed_in_minute_playback_within_163_hours          smallint            not null
,activity_date date);
commit;

--select max(minute) from vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec;

---Start of Loop
--drop table vespa_analysts.All_viewing_minute_by_minute_unweighted_20111129;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;

--select @min_tx_start_time_local;
---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec_sky1
    select a.subscriber_id
--    ,channel_name_inc_hd
    ,@minute as minute

,sum(case when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute

    ,sum(case when live = 0 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_live

    ,sum(case when live =1 then 0 when  dateadd(hour,163,recorded_time_utc)<adjusted_event_start_time then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_playback_within_163_hours

,min(cast (Adjusted_Event_Start_Time as date)) as activity_date

from  vespa_analysts.VESPA_all_viewing_records_20111107_20111209_sky1 as a
where  (play_back_speed is null or play_back_speed = 2) and (
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute)))
    group by a.subscriber_id
--    ,channel_name_inc_hd
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

--select @minute

--select count(*) from vespa_analysts.VESPA_all_viewing_records_20111107_20111209;


---Add Index on for subscriber_id
create hg index idx1 on vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec_sky1(subscriber_id);
commit;
--select count(*) from vespa_analysts.minute_by_minute_unweighted_by_channel_7nov_2dec_sky1;

---Add info for Account Number and Primary/Secondary Box



select account_number
        , cb_key_household
        , csh.current_short_description
        ,service_instance_id
        ,SUBSCRIPTION_SUB_TYPE
        , rank() over (partition by account_number ,SUBSCRIPTION_SUB_TYPE,service_instance_id order by effective_from_dt, cb_row_id) as rank
into  vespa_analysts.project_022_sky_accounts -- drop table #sky_accounts
from sk_prod.cust_subs_hist as csh
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription') --the DTV + Multiroom sub Type
   and effective_to_dt = '9999-09-09'               
   and effective_from_dt<>effective_to_dt            -
and account_number is not null;
commit;



delete from vespa_analysts.project_022_sky_accounts where rank>1;
commit;

---Create src_system_id lookup

select src_system_id
,min(cast(si_external_identifier as integer)) as subscriberid
into #subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
group by src_system_id
;


commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';

commit;


--alter table vespa_analysts.F1_analysis_20111104 delete subscription_type;
alter table vespa_analysts.project_022_sky_accounts add subscriber_id bigint;

update vespa_analysts.project_022_sky_accounts
set subscriber_id=b.subscriberid
from vespa_analysts.project_022_sky_accounts as a
left outer join #subs_details as b
on a.service_instance_id=b.src_system_id
;
commit;

---



--Add on HD Account Status and Movies Status as at 7th Nov---
--select top 10 * from  sk_prod.cust_entitlement_lookup;
--drop table #sky_accounts_movies_hd_status_;
select account_number
 ,max(case when SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing') and status_code in ('AC','PC') and b.prem_movies>0 then 1 else 0 end) as ever_had_movies
 ,max(case when SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing') and status_code in ('AC','PC') 
            and b.prem_movies>0 and effective_to_dt >'2011-11-07' then 1 else 0 end) as currently_has_movies
 ,max(case when SUBSCRIPTION_SUB_TYPE in ('DTV HD') and status_code in ('AC','PC') then 1 else 0 end) as ever_had_hd_sub
,max(case when SUBSCRIPTION_SUB_TYPE in ('DTV HD') and status_code in ('AC','PC') and effective_to_dt >'2011-11-07' then 1 else 0 end) as currently_has_hd_sub
into #sky_accounts_movies_hd_status_ -- drop table #sky_accounts
from sk_prod.cust_subs_hist as csh
left outer join sk_prod.cust_entitlement_lookup as b
on csh.current_short_description = b.short_description
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV HD') --the DTV + Multiroom sub Type

   and effective_from_dt <= '2011-11-07'
--and cb_key_household > 0
--and cb_key_household is not null
and account_number is not null
group by account_number;

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
left outer join #sky_accounts_movies_hd_status_ as b
on a.account_number=b.account_number
;
commit;

--select ever_had_movies , currently_has_movies ,count(*) from vespa_analysts.project_022_sky_accounts group by ever_had_movies , currently_has_movies
--select ever_had_hd_sub , currently_has_hd_sub ,count(*) from vespa_analysts.project_022_sky_accounts group by ever_had_hd_sub , currently_has_hd_sub


-----Add on box details----



Alter table vespa_analysts.project_022_sky_accounts
add pvr tinyint default 0,
add box_type varchar(2) default 'SD',
add primary_box bit default 0;
commit;


--Add on box details – most recent dw_created_dt for a box (where a box hasn’t been replaced at that date)  taken from cust_set_top_box.  
--This removes instances where more than one box potentially live for a subscriber_id at a time (due to null box installed and replaced dates).

SELECT account_number
,service_instance_id
,max(dw_created_dt) as max_dw_created_dt
  INTO #boxes -- drop table #boxes
  FROM sk_prod.CUST_SET_TOP_BOX  
 WHERE (box_installed_dt <= cast('2011-11-07'  as date) 
   AND box_replaced_dt   > cast('2011-11-07'  as date)) or box_installed_dt is null
group by account_number
,service_instance_id
 ;

--select count(*) from vespa_analysts.aug_22_base_details;
commit;


commit;
exec sp_create_tmp_table_idx '#boxes', 'account_number';
exec sp_create_tmp_table_idx '#boxes', 'service_instance_id';
exec sp_create_tmp_table_idx '#boxes', 'max_dw_created_dt';
--select account_number , count(di

---Create table of one record per service_instance_id---
SELECT acc.account_number
,acc.service_instance_id
,min(stb.x_pvr_type) as pvr_type
,min(stb.x_box_type) as box_type
,min(stb.x_description) as description_x
,min(stb.x_manufacturer) as manufacturer
,min(stb.x_model_number) as model_number
  INTO #boxes_with_model_info -- drop table #boxes
  FROM #boxes  AS acc left outer join sk_prod.CUST_SET_TOP_BOX AS stb 
        ON acc.account_number = stb.account_number
 and acc.max_dw_created_dt=stb.dw_created_dt
group by acc.account_number
,acc.service_instance_id
 ;

commit;
exec sp_create_tmp_table_idx '#boxes_with_model_info', 'service_instance_id';


alter table vespa_analysts.project_022_sky_accounts add x_pvr_type  varchar(50);
alter table vespa_analysts.project_022_sky_accounts add x_box_type  varchar(20);
alter table vespa_analysts.project_022_sky_accounts add x_description  varchar(100);
alter table vespa_analysts.project_022_sky_accounts add x_manufacturer  varchar(50);
alter table vespa_analysts.project_022_sky_accounts add x_model_number  varchar(50);

update  vespa_analysts.project_022_sky_accounts
set x_pvr_type=b.pvr_type
,x_box_type=b.box_type

,x_description=b.description_x
,x_manufacturer=b.manufacturer
,x_model_number=b.model_number
from vespa_analysts.project_022_sky_accounts as a
left outer join #boxes_with_model_info as b
on a.service_instance_id=b.service_instance_id
;
commit;

update vespa_analysts.project_022_sky_accounts
set pvr =case when x_pvr_type like '%PVR%' then 1 else 0 end
,box_type =case when x_box_type like '%HD%' then 'HD' else 'SD' end
from vespa_analysts.project_022_sky_accounts
;

--Upweight figures




/*
select channel , channel_name_grouped , count(*) as records  from vespa_analysts.project_022_all_techedge_spots
where channel_name_grouped is null
 group by channel,channel_name_grouped order by records desc
*/
--select top 500 * from vespa_analysts.project_022_all_techedge_spots
--select count(*) from vespa_analysts.project_022_all_techedge_spots


--select distinct channel_name from sk_prod.vespa_epg_dim order by UPPER (channel_name)







/*
select * from sk_prod.vespa_epg_dim where channel_name = 'ITV3' and tx_date_utc ='2011-12-02' order by tx_date_time_utc


select channel_name 
, min(pay_free_indicator) as pay_free_min
, max(pay_free_indicator) as pay_free_max
from sk_prod.vespa_epg_dim
group by channel_name
order by channel_name

commit;

*/
