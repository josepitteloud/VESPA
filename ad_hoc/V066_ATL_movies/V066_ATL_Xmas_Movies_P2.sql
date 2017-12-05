/*

                Id:             V066
                Name:           ATL xmas movies Phase 2
                Lead:           Sarah Moore
                Date:           26/07/2012
                Analyst:        Susanne Chan

                QA date:
                QA analyst:

                Notes:

**CODE SECTIONS**

-- PART A. CREATE A LIST OF ACCOUNTS WITHOUT SKY MOVIES SUBS @05/12/11
--PART B GET VIEWING DATA
--B. Add Universe flags
--PART C:  SOV
--PART D: OUTPUT

**TABLES**

v066_base : All accounts with No Movies Subs

v066_output_1 : Takes the required fields from v066_viewing_records by accounts,
                        granular: Channel_name, Service_key, Day, Daypart, Genre, Subgenre, EPG Title level.
                        where Reach is counted for each Household that has at least 1 viewing event of at least 3 mins (BARB standard)

v066_output_1_M : For customers already having Movies

v066_output_2 : As v066_output_1, but aggregated channels together (Main, HD, +1).  Includes Media pack matched to channel, Universe flags.
                        This is the RAW data table to use.
                        Requires aggregating, summarising for each universe and granular levels for calculating reach
                        as accounts & weights has repeated rows for every level of granularity

v066_output_2_M : For customers already having Movies

-----------------------------------------------------------------------------------------------------------------------------------------------------

-- PART A. CREATE A LIST OF ACCOUNTS WITHOUT SKY MOVIES SUBS @05/12/11
*/
CREATE VARIABLE @target_dt datetime;
SET @target_dt  = '2011-12-05';

SELECT   sav.account_number
        ,sav.Cb_Key_Household
        ,sav.Cb_Key_Family
        ,sav.Cb_Key_Individual
INTO #v066_Base
FROM sk_prod.cust_subs_hist as csh
        inner join sk_prod.cust_entitlement_lookup as cel
                on csh.current_short_description = cel.short_description
                        inner join sk_prod.cust_single_account_view AS sav
                        ON csh.account_number = sav.account_number
WHERE subscription_sub_type = 'DTV Primary Viewing'
      and status_code IN ('AC','AB','PC')
      and (csh.effective_from_dt  <= @target_dt and csh.effective_to_dt > @target_dt)
      and cel.prem_movies = 0
;
commit;

--5775763 Row(s) affected


select distinct(account_number)
into #nodupes
from #v066_base;
--5775157 Row(s) affected



select  no.account_number
        ,max(base.cb_key_household)
        ,max(base.cb_key_family)
        ,max(base.cb_key_individual)
INTO    v066_base
FROM    #nodupes no LEFT JOIN #v066_base base
                ON no.account_number = base.account_number
Group by no.account_number
;
--5840437 Row(s) affected

--drop table v066_Base;


create hg index indx1 on v066_base(account_number);


--grant all on v066_Base to public;
-------------------------------------------------------------PART B GET VIEWING DATA
-- variable creation - run once only
commit
CREATE VARIABLE @var_prog_period_start datetime;
CREATE VARIABLE @var_date_counter      datetime;
CREATE VARIABLE @var_prog_period_end   datetime;
CREATE VARIABLE @dt                    char(8);
CREATE VARIABLE @var_sql               varchar(15000);

SET @var_prog_period_start  = '2011-11-22';
SET @var_prog_period_end    = '2011-12-05';  --2 week viewing period before date used for extracting non-movies subscribers,
                                             --to ensure we get viewing behaviour of non subs

--IF object_id('V066_viewing_records') IS NOT NULL DROP TABLE V066_viewing_records;

-- To store all the viewing records:
create table #V066_viewing_records(
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
            ,capped_x_programme_viewed_duration    int
            ,capped_flag                           tinyint     default 0
            ,viewing_record_start_time_utc         datetime
            ,viewing_record_start_time_local       datetime
            ,viewing_record_end_time_utc           datetime
            ,viewing_record_end_time_local         datetime
            ,viewing_category                      varchar(20)
            ,HD_channel                            bit         default 0
            ,Pay_Channel                           bit         default 0
            ,capped_x_viewing_start_time           datetime
            ,capped_x_viewing_end_time             datetime
            ,FTA_Movies                            bit         default 0
);


-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into #V066_viewing_records
    select vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
          ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
          ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
          ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
          ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
          ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
          ,prog.channel_name,prog.service_key
          ,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
          ,sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration
          ,0,'''',0,0,'''','''','''','''','''',0,0,'''','''',0
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*##      as vw
          left  join sk_prod.VESPA_EPG_DIM             as prog on vw.programme_trans_sk = prog.programme_trans_sk
          /*inner join v066_base                         as acc on vw.account_number = acc.account_number*/
        -- Filter for viewing events during extraction
    where video_playing_flag = 1
      and adjusted_event_start_time <> x_adjusted_event_end_time
      and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
           or (    x_type_of_viewing_event = (''Other Service Viewing Event'')
               and x_si_service_type = ''High Definition TV test service''))
      and panel_id in (4,5)'
;


    set @var_date_counter = @var_prog_period_start;

  while @var_date_counter <= @var_prog_period_end
  begin
      set @dt = left(@var_date_counter,4) || substr(@var_date_counter,6,2) || substr(@var_date_counter,9,2)
      EXECUTE(replace(@var_sql,'##^^*^*##',@dt))
      commit
      set @var_date_counter = dateadd(day, 1, @var_date_counter)
  end;--6762189 Row(s) affected
  --40m


update V066_viewing_records
      set live = case when play_back_speed is null then 1 else 0 end
;
--71503944 Row(s) affected

  update V066_viewing_records
     set hd_channel = 1
   where channel_name like '%HD%'
;
--12864166 Row(s) affected



/*-- QA CHECK ---
select top 10 * from V066_viewing_records

SELECT DISTINCT(CAST(viewing_record_start_time_utc as date))as viewing_record_start_time_utc
        ,count(*) as count
        ,sum(live)
 from V066_viewing_records
 group by viewing_record_start_time_utc
 order by viewing_record_start_time_utc
-- alot of content outside of the date range -- these are all playback events from the beginning of vespa -- will throw off capping and scaling


SELECT DISTINCT(CAST(adjusted_event_start_time as date))as adjusted_event_start_time
        ,count(*) as count
        ,sum(live)
 from V066_viewing_records
 group by adjusted_event_start_time
 order by adjusted_event_start_time
-- alot of content outside of the date range -- these are all playback events from the beginning of vespa -- will throw off capping and scaling
-- we can use   adjusted_event_start_time instead (when the playback was viewed)
-- need to change the scaling code



SELECT DISTINCT(CAST(x_viewing_start_time as date))as x_viewing_start_time
        ,count(*) as count
        ,sum(live)
 from V066_viewing_records
 group by x_viewing_start_time
 order by x_viewing_start_time
-- this should be fine for capping use (x_viewing_start_time)

*/


-------------------------------------------------------------B.3 CAP VIEWING DATA

IF object_id('V066_max_caps') IS NOT NULL DROP TABLE V066_max_caps;

 select event_start_day
        ,event_start_hour
        ,live
        ,min(dur_mins) as min_dur_mins
    into V066_max_caps
    from (select cast(Adjusted_Event_Start_Time as date) as event_start_day
                ,datepart(hour,Adjusted_Event_Start_Time) as event_start_hour
                ,live
                ,datediff(minute,Adjusted_Event_Start_Time,x_Adjusted_Event_end_Time) as dur_mins
                ,ntile(100) over (partition by event_start_day, event_start_hour, live
                                      order by dur_mins) as ntile_100
                                      into ntiles
            from V066_viewing_records) as sub
   where (ntile_100 = 91 and live = 1)   --live capped at 90%
        OR (ntile_100 = 100 and live = 0) --playback capped at 99%
group by event_start_day
        ,event_start_hour
        ,live
; --2m
--672 Row(s) affected

commit;


---Create Capping rules limits



create hg   index idx1 on V066_viewing_records(subscriber_id);
create dttm index idx2 on V066_viewing_records(adjusted_event_start_time);
create dttm index idx3 on V066_viewing_records(recorded_time_utc);
create dttm index idx5 on V066_viewing_records(x_viewing_start_time);
create dttm index idx6 on V066_viewing_records(x_viewing_end_time);
create hng  index idx7 on V066_viewing_records(x_cumul_programme_viewed_duration);
create hg   index idx8 on V066_viewing_records(programme_trans_sk);
create hg   index idx9 on V066_viewing_records(channel_name_inc_hd)
; --10m

-- update the viewing start and end times for playback records
  update V066_viewing_records
     set x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
   where recorded_time_utc is not null
; --3m
--19512319 Row(s) affected
commit;

  update V066_viewing_records
     set x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
   where recorded_time_utc is not null
; --3m --19512319 Row(s) affected
commit;

-- update table to create capped start and end times
  update V066_viewing_records as bas
     set capped_x_viewing_start_time = case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
         end
        ,capped_x_viewing_end_time = case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) > x_viewing_end_time then x_viewing_end_time
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, adjusted_event_start_time)
         end
  from V066_max_caps as caps
 where date(bas.adjusted_event_start_time) = caps.event_start_day
                                           and datepart(hour, bas.adjusted_event_start_time) = caps.event_start_hour
;  --69949906 Row(s) affected

commit;


-- calculate capped_x_programme_viewed_duration
  update V066_viewing_records
     set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
; --2m
--69949906 Row(s) affected



-- set capped_flag based on nature of capping
/*
    0 programme view not affected by capping
    1 if programme view has been shortened by a long duration capping rule
    2 if programme view has been excluded by a long duration capping rule
*/


  update V066_viewing_records
     set capped_flag =
        case
            when capped_x_viewing_end_time < x_viewing_end_time then 1
            when capped_x_viewing_start_time is null then 2
            else 0
        end
; --1m
--69949906 Row(s) affected

-- Min Caps
IF object_id('vespa_min_cap') IS NOT NULL DROP TABLE vespa_min_cap;
create table vespa_min_cap (
    cap_secs as integer
);
insert into vespa_min_cap (cap_secs) values (6); --This is 6 seconds


  update V066_viewing_records as bas
     set capped_x_viewing_start_time        = null
        ,capped_x_viewing_end_time          = null
        ,capped_x_programme_viewed_duration = null
        ,capped_flag                        = 3
    from vespa_min_cap
   where capped_x_programme_viewed_duration < cap_secs
; --0m  --3854323 Row(s) affected

select top 10 * from sk_prod.VESPA_EVENTS_VIEWED_ALL

  update  V066_viewing_records
     set capped_flag =
        case
            when capped_x_viewing_start_time is null then 2
            when capped_x_viewing_end_time < x_viewing_end_time then 1
            else 0
        end
; --1m  --69949906 Row(s) affected
commit;

--QA:
/*
select capped_flag,count(1) from v066_viewing_records group by capped_flag;


        capped_flag     count(1)
0       63598197
2       5785565
1       566144

*/



delete from V066_viewing_records where capped_flag in (2,3); --3m  --5937164 Row(s) affected





-------------------------------------------------------------B.4 SCALE VIEWING DATA
   alter table V066_viewing_records
     add (weighting_date        date
         ,scaling_segment_ID    int
         ,weightings            float default 0
);

commit;
create index for_weightings on V066_viewing_records(account_number);

  update V066_viewing_records
     set weighting_date = cast(adjusted_event_start_time as date)
; --0m --65566780 Row(s) affected


--- check it:

--select distinct(weighting_date) from V066_viewing_records order by weighting_date



-- First, get the segmentation for the account at the time of viewing
  update V066_viewing_records as bas
     set bas.scaling_segment_ID = wei.scaling_segment_ID
    from vespa_analysts.scaling_dialback_intervals as wei
   where bas.account_number = wei.account_number
     and bas.weighting_date between wei.reporting_starts and wei.reporting_ends
; --2m
--65560608 Row(s) affected

commit;



-- Find out the weight for that segment on that day
update V066_viewing_records
     set weightings = wei.weighting
    from V066_viewing_records as bas INNER JOIN vespa_analysts.scaling_weightings as wei
                                        ON bas.weighting_date = wei.scaling_day
                                        and bas.scaling_segment_ID = wei.scaling_segment_ID
;--65560608 Row(s) affected
commit;



delete from V066_viewing_records where weightings = 0;  --6172 Row(s) affected
--4m

--QA
/*
select count(*) from v066_viewing_records  --35322540
where weightings =0 ;  --1546088

select count(distinct(account_number)) from v066_viewing_records;  --123273

select top 100 * from v066_viewing_records
where weightings = 0 and scaling_segment_ID is not null;

select top 100 * from scaling_weightings
where scaling_day = '2011-11-17';

select top 100 * from scaling_dialback_intervals
where account_number = '200001426580'
scaling_day = '2011-11-17';


*/


--select top 10 * from V066_viewing_records



--select distinct(weighting_date) from V066_viewing_records order by weighting_date


--select count(distinct(account_number)) from v066_viewing_records
--196156

---------------------------------------------------------------------------------------------------------------
-- now we need the nominal/average scaling factor for each account over the period.
---------------------------------------------------------------------------------------------------------------




--drop table #temp

select  account_number
        ,weighting_date
       ,max(weightings) as weighting
into #temp
from V066_viewing_records
group by account_number, weighting_date
;--1159152 Row(s) affected

--drop table normalised_weights

select distinct account_number
        , avg(weighting) as average_weighting
into normalised_weights
from #temp
group by account_number
;--128389 Row(s) affected


--select top 10 * from normaised_weights


-- now we need to normalise the weightings so they tie back to our original universe:

 --select sum(average_weighting) from normaised_weights
--7659523 million customers -- this is higher than the base we started with!



-- the base that we started with:



select count(*) from v066_base --5775157 Non movies base
--4342594 Movies base

-- lets normalise it:
create variable
@normal_factor float;

set @normal_factor = (select 4342594/sum(average_weighting) from normalised_weights)

Select @normal_factor -- 0.77 (discounting factor) Non movies base
-- 0.72 (discounting factor) Movies base

-- Step 3: calculate the normalised wieght and add this to a table
alter table normalised_weights
add normalised_weighting as float

update normalised_weights
set normalised_weighting = average_weighting*@normal_factor
--128389 Row(s) affected

select sum(normalised_weighting) from normalised_weights -- thats better!


-- alter table normalised_weights
-- drop normalised_weighting

-- now add this to the viewing table (it is the average weight)


alter table V066_viewing_records
add(normalised_weight float);

update V066_viewing_records
set normalised_weight = normalised_weighting
from V066_viewing_records as viw
left join normalised_weights nwe
on viw.account_number = nwe.account_number
--64159942 Row(s) affected


-- it worked - use normalised weight going forward
--select top 200 * from V066_viewing_records







----------------------------------------------------------------------
-- LETS GET THE VIEWING DETAILS SUMMARISED FOR EACH CUSTOMER
---------------------------------------------------------------------




---------------------------------Determine day of week

alter table  V066_viewing_records
add(Day as varchar(10));


update V066_viewing_records
set day =  CASE WHEN CAST(capped_x_viewing_start_time AS DATE) IN ('2011-11-22', '2011-11-29') THEN 'Tuesday'
                WHEN CAST(capped_x_viewing_start_time AS DATE) IN ('2011-11-23', '2011-11-30') THEN 'Wednesday'
                WHEN CAST(capped_x_viewing_start_time AS DATE) IN ('2011-11-24', '2011-12-01') THEN 'Thursday'
                WHEN CAST(capped_x_viewing_start_time AS DATE) IN ('2011-11-25', '2011-12-02') THEN 'Friday'
                WHEN CAST(capped_x_viewing_start_time AS DATE) IN ('2011-11-26', '2011-12-03') THEN 'Saturday'
                WHEN CAST(capped_x_viewing_start_time AS DATE) IN ('2011-11-27', '2011-12-04') THEN 'Sunday'
                WHEN CAST(capped_x_viewing_start_time AS DATE) IN ('2011-11-28', '2011-12-05') THEN 'Monday'
                ELSE 'Error' END
;

/*CHECKS

--select top 10 * from V066_viewing_records

select day, count(*) from V066_viewing_records
GROUP BY day
;
select * from V066_viewing_records
where day ='Error'
;
Some records have been capped to a start date a day after the analysis period, we will delete these from analysis
*/

Delete from V066_viewing_records where day = 'Error';














------------------------------------------------------------------------PART C:  SOV

--******************************************************************PART C has not been run




--Group viewing records into required variables by account

-- ok lets find out how many minutes were watched on average per day (some people only returned data for 1 day so use average)
-- divide by 60 to get minutes

select account_number
       ,count(distinct(weighting_date)) as days_data_return
       ,((sum(case when pay_Channel = 1 then capped_x_programme_viewed_duration else 0 end)/60)/days_data_return) as average_pay_minutes
       ,((sum(case when pay_Channel = 0  then capped_x_programme_viewed_duration else 0 end)/60)/days_data_return) as average_free_minutes
       ,average_Total_minutes_day = average_pay_minutes + average_free_minutes

       , sov_pay_tv = cast(average_pay_minutes as float)/nullif(cast(average_Total_minutes_day as float), 0)
into v066_output_dur
from V066_viewing_records
group by account_number;
--130767 Row(s) affected

-- CHECK IT:
--select top 10 * from v066_output_dur
--select top 10 * from v066_viewing_records

--drop table v066_output_pay

/*

----------------Add variables: Genre, Sub genre, Channel, Day, Time of day

--drop table v066_genre

--select top 100 * from v066_genre order by account_number

select pay.account_number
       ,days_data_return
       ,GENRE_DESCRIPTION
       ,((sum(capped_x_programme_viewed_duration)/60)/days_data_return) as average_genre_minutes

        ,sov_genre = cast(average_genre_minutes as float)/nullif(cast(average_Total_minutes_day as float), 0)
INTO    v066_genre
FROM    V066_viewing_records vie INNER JOIN v066_output_pay pay
        ON vie.account_number = pay.account_number
GROUP BY pay.account_number
         ,GENRE_DESCRIPTION
         ,days_data_return
         ,average_Total_minutes_day
;--882514 Row(s) affected

select pay.account_number
       ,days_data_return
       ,GENRE_DESCRIPTION
       ,sub_genre_description
       ,((sum(capped_x_programme_viewed_duration)/60)/days_data_return) as average_subgenre_minutes

        ,sov_subgenre = cast(average_subgenre_minutes as float)/nullif(cast(average_Total_minutes_day as float), 0)
INTO    v066_subgenre
FROM    V066_viewing_records vie INNER JOIN v066_output_pay pay
        ON vie.account_number = pay.account_number
GROUP BY pay.account_number
         ,GENRE_DESCRIPTION
         ,SUB_GENRE_DESCRIPTION
         ,days_data_return
         ,average_Total_minutes_day
;--4450391 Row(s) affected

--select top 100 * from v066_timeofday order by account_number

select pay.account_number
       ,days_data_return
       ,channel_name
       ,((sum(capped_x_programme_viewed_duration)/60)/days_data_return) as average_channel_minutes

        ,sov_channel = cast(average_channel_minutes as float)/nullif(cast(average_Total_minutes_day as float), 0)
INTO    v066_channel
FROM    V066_viewing_records vie INNER JOIN v066_output_pay pay
        ON vie.account_number = pay.account_number
GROUP BY pay.account_number
         ,channel_name
         ,days_data_return
         ,average_Total_minutes_day
;--5214597 Row(s) affected

select pay.account_number
       ,days_data_return
       ,day
       ,((sum(capped_x_programme_viewed_duration)/60)/days_data_return) as average_Day_minutes

        ,sov_Day = cast(average_Day_minutes as float)/nullif(cast(average_Total_minutes_day as float), 0)
INTO    v066_Day
FROM    V066_viewing_records vie INNER JOIN v066_output_pay pay
        ON vie.account_number = pay.account_number
GROUP BY pay.account_number
         ,day
         ,days_data_return
         ,average_Total_minutes_day
;--740681 Row(s) affected


select pay.account_number
       ,days_data_return
       ,day
       ,x_viewing_time_of_day
       ,((sum(capped_x_programme_viewed_duration)/60)/days_data_return) as average_TimeOfDay_minutes

        ,sov_TimeOfDay = cast(average_TimeOfDay_minutes as float)/nullif(cast(average_Total_minutes_day as float), 0)
INTO    v066_TimeOfDay
FROM    V066_viewing_records vie INNER JOIN v066_output_pay pay
        ON vie.account_number = pay.account_number
GROUP BY pay.account_number
         ,day
         ,x_viewing_time_of_day
         ,days_data_return
         ,average_Total_minutes_day
;--3360274 Row(s) affected


--------------------------------------------------------------------------- create master analysis table: v066_output_day

--if object_id('v066_output') is not null drop table v066_output

-- LETS SET ALL OF THE NULLS TO ZERO
Update v066_subgenre
set  sov_subgenre = case when sov_subgenre is null then 0 else sov_subgenre end
;--4450391 Row(s) affected
Update v066_channel
SET    sov_channel = case when sov_channel is null then 0 else sov_channel end
;--5214597 Row(s) affected
Update v066_timeofday
SET    sov_timeofday = case when sov_timeofday is null then 0 else sov_timeofday end
;--3360274 Row(s) affected
Update v066_output_pay
SET    sov_pay_tv = case when sov_pay_tv is null then 0 else sov_pay_tv end
;--130767 Row(s) affected




-- add scaling

alter table v066_output_pay
add(weight float);


update v066_output_pay
set weight = normalised_weighting
from v066_output_pay as viw
left join normaised_weights nwe
on viw.account_number = nwe.account_number
;--130767 Row(s) affected

-- Check it: --
-- select top 10 * from v066_output_pay -- looks fine
-- select count(*) from v066_output_pay where weight is null -- none, good
-- select count(*) from v066_output_pay where weight = 0 -- none, good
-- select sum(weight) from v066_output_pay  --5,840,384 = base of non movies subscribers

----------------------------------------------------------------------
-- PART 2: LETS GET THE CUMULITIVE WEIGHTING TO ALLOCATE QUINTILES TO THE CUSTOMERS BASED ON THIER POPULATION REPRESENTATION
---------------------------------------------------------------------

--select top 10 * from v066_output_pay

/*
Quintile 7 things:
1.SOV pay
2.SOV Time of day
3.SOV FTA movies
4.Day of week
5.Specials = X Factor Final
6.Genre
7.Sub Genre


-- drop table #temp22
select  account_number
        ,weight
        ,average_Total_minutes_day -- all measures are average per day to allow comparison
        ,sov_pay_tv


        , sum(weight) over ( order by average_Total_minutes_day -- partition is not needed
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_viewing_cumul_weighting

        , sum(weight) over ( order by sov_pay_tv
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as pay_viewing_cumul_weighting

into #temp22
from v066_output_pay;--130767 Row(s) affected


----run each new variable into #temp22 after adding quintiles into customer_quintile table






--------------------------------------------------------------------------------
-- PART 3 - CREATE QUINTILE TABLE AND ADD WEIGHTING BANDINGS
--------------------------------------------------------------------------------


IF object_id('quintile_weights') IS NOT NULL DROP TABLE quintile_weights;


create table quintile_weights
        ( centile integer primary key);

create variable y int;
set y = 1;

while y <= 5
begin
        insert into quintile_weights values (y)
        set y = y + 1


end;


-- add two additional fields to this table - these will be used later
alter table quintile_weights
add sample float; -- this will indicate the maximum cumulitave weighting of customers allowed into the centile


update quintile_weights
set sample = ceil((centile)* -- the sample will be the lower boudary
  (select sum(weight) from #temp22)/5)


--check it:
--select top 10 * from quintile_weights -- everything looks fine - we will use < to allocate customers to a quintile.




--------------------------------------------------------------------------------
-- PART 4 - now allocate each account to the relevant quintile based on the cumulitave weighting
--------------------------------------------------------------------------------
--select top 10 account_number, cb_key_household
--from sk_prod.cust_subs_hist;


IF object_id('customer_quintile') IS NOT NULL DROP TABLE customer_quintile;

-- we need to copy the temp table into a real table so we can add columns etc
select * into customer_quintile from #temp22

-- we need to create 4 quintile allocations
alter table customer_quintile
add (

     total_viewing_quintile integer
    ,pay_SOV_quintile integer
    ,channel_SOV_quintile integer
    ,day_SOV_quintile integer
    ,genre_SOV_quintile integer
    );--

-- now lets use the different cumul weightings to allocate the different deciles. (again; these are averages today for intertemporal comparison)

-- total viewing quitile
update customer_quintile
        set total_viewing_quintile = centile
from customer_quintile as vdw
 inner join quintile_weights as cww
 on total_viewing_cumul_weighting <= sample


-- update the pay viewing quitile
update customer_quintile
        set pay_SOV_quintile = centile
from customer_quintile as vdw
 inner join quintile_weights as cww
 on pay_viewing_cumul_weighting <= sample

--------------------------------------------------------------------------------
-- PART 5 - put each customers QUINTILE allocation into master table v066_output_pay (profiling details can also be joined here for one big happy pivot)
--------------------------------------------------------------------------------




-- genre viewing quitile
update v066_output_pay
set     al1.pay_SOV_quintile  = csq.pay_SOV_quintile
        ,al1.total_viewing_quintile  = csq.total_viewing_quintile
from  v066_output_pay as al1
 left join customer_quintile as csq
 on csq.account_number = al1.account_number
--130767 Row(s) affected

 -- there are a bunch of columns in this table that we dont need anymore...
--select top 100 * from v066_output_pay





-- this is no longer needed
drop table customer_quintile

select * from v066_output_pay
;
*/
/*
--************
-- QA---
--************


select top 10 * from customer_quintile

we are looking to check the number of customers in each quintile - we hope they are in the same ball park (not the same as traditionally done)
also we want to make sure there are not huge differences between the average weighting of the customers within each Quintitle

select distinct(total_viewing_quintile), count(*) as count, avg(normalised_weight) from customer_quintile
group by total_viewing_quintile
order by total_viewing_quintile

select distinct(pay_SOV_quintile), count(*) as count, avg(normalised_weight) from customer_quintile
group by pay_SOV_quintile
order by pay_SOV_quintile

select distinct(PVR_SOV_quintile), count(*) as count, avg(normalised_weight) from customer_quintile
group by PVR_SOV_quintile
order by PVR_SOV_quintile


select distinct(FTA_movies_SOV_quintile), count(*) as count, avg(normalised_weight) from customer_quintile
group by FTA_movies_SOV_quintile
order by FTA_movies_SOV_quintile


 NOTE:
       QUINTITLE 1 = LOW VOLUME/SHARE OF VIEWING
       QUINTITLE 5 = HIGH VOLUME/SHARE OF VIEIWNG

^Suggested bandings^:

1 = very light/no viewing
2 = light viewing
3 = median viewing (since these are scaled quintitles, 3 represents the median of the 6 million non-movies cusotmers being investigated
4 = above average viewing
5 = heavy viewing

where talking about share of viewing:

1 = No share of viewing
2 = small share of viewing
3 = average share of viewing
4 = above average share of viewing
5 = very large share of viewing


-- Please not this is relative to Non-movies customers as defined in the universe - NOT THE UK BASE
*/

--******************************************************************PART C has not been run

--------------------------------------------------------------------------PART D: OUTPUT

--select top 10 * from V066_viewing_records
--drop table v066_output_1




---Create new output table from viewing_records table to calculate Reach and grouping into the categories we are interested in

select  max(normalised_weight) as weight
        ,account_number
        ,channel_name
        ,day
        ,x_viewing_time_of_day
        ,genre_description
        ,sub_genre_description
        ,EPG_title
        ,capped_x_programme_viewed_duration
        ,service_key
        ,cb_key_household
        ,cb_key_family
        ,cb_key_individual
INTO v066_output_1
FROM v066_viewing_records
WHERE capped_x_programme_viewed_duration >= 180 --only count as viewed where event prog view duration is at least 3 mins
GROUP BY account_number
        ,channel_name
        ,day
        ,x_viewing_time_of_day
        ,genre_description
        ,sub_genre_description
        ,EPG_title
        ,capped_x_programme_viewed_duration
        ,service_key
        ,cb_key_household
        ,cb_key_family
        ,cb_key_individual
;
---31385422 Row(s) affected

--select top 100 * from v066_output_1 order by capped_x_programme_viewed_duration asc



-------------------------------------------------------------------------Map Sky Media packs onto Channels

-----------------------------Map using SARE and service key

select ska.[service key] as service_key, ska.full_name, cgroup.primary_sales_house,
                (case when pack.name is null then cgroup.channel_group
                else pack.name end) as channel_category
into #packs
from patelj.channel_map_service_key_attributes ska
left join
        (select a.service_key, b.name
         from patelj.channel_map_service_key_landmark a
                join patelj.channel_map_landmark_channel_pack_lookup b
                        on a.sare_no between b.sare_no and b.sare_no + 999
        where a.service_key <> 0
         ) pack
        on ska.[service key] = pack.service_key
left join
        (select distinct a.service_key, b.primary_sales_house, b.channel_group
         from patelj.channel_map_service_key_barb a
                join patelj.channel_map_barb_channel_group b
                        on a.log_station_code = b.log_station_code
                        and a.sti_code = b.sti_code
        where service_key <>0) cgroup
        on ska.[service key] = cgroup.service_key
where cgroup.primary_sales_house is not null
order by cgroup.primary_sales_house, channel_category
;--438 Row(s) affected






--select top 10* from #pack
--drop table #packs

/*
-- diagnostic check - this should not return any records

select service_key, count(1)
from (

select distinct a.service_key, b.primary_sales_house, b.channel_group
from patelj.channel_map_service_key_barb a
        join patelj.channel_map_barb_channel_group b
        on a.log_station_code = b.log_station_code
        and a.sti_code = b.sti_code
        where service_key <>0
        ) c
group by service_key
having count(1) > 1;

select service_key, count(1)
from (
        select a.service_key, b.name
        from patelj.channel_map_service_key_landmark a
                join patelj.channel_map_landmark_channel_pack_lookup b
                        on a.sare_no between b.sare_no and b.sare_no + 999
        where a.service_key <> 0
     ) pack
group by service_key
having count(1) > 1;
*/



-----------------------------Correct channel category anomolies

SELECT  primary_sales_house
        ,service_key
        ,full_name
        ,(case
                when service_key = 3777 OR service_key = 6756 then 'LIFESTYLE & CULTURE'
                when service_key = 4040 then 'SPORTS'
                when service_key = 1845 OR service_key = 4069 OR service_key = 1859 then 'KIDS'
                when service_key = 4006 then 'MUSIC'
                when service_key = 3621 OR service_key = 4080 then 'ENTERTAINMENT'
                when service_key = 3760 then 'DOCUMENTARIES'
                when service_key = 1757 then 'MISCELLANEOUS'
                when service_key = 3639 OR service_key = 4057 then 'Media Partners'
                                                                                ELSE channel_category END) AS channel_category
INTO LkUpPack
FROM #packs
order by primary_sales_house, channel_category
;

--drop table #pack

------------------------------------------------------------Aggregate the Channels together where there are HD or +1 versions

--First, create a lookup table of cleaned channel names to map back against

select
        case when right(channel_name,2) = 'HD' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,2) = '+1' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,1) = '+' THEN LEFT(channel_name,(LEN(channel_name)-1))
                                                ELSE channel_name END AS Channel
        ,channel_name
INTO #channel1
FROM v066_viewing_records
group by channel_name
;--515 Row(s) affected


--select * from #channel2 order by channel;

SELECT RTRIM(channel) as Channel
        ,channel_name
INTO    #channel2
FROM    #channel1
;



SELECT
        case when channel = 'BBC ONE'     THEN 'BBC ONE HD'
             when left(channel,5) = 'BBC 1' THEN 'BBC 1'
             when left(channel,5) = 'BBC 2' THEN 'BBC 2'
             when channel_name = 'BBC HD' THEN 'BBC HD'
             when left(channel,4) = 'ITV1' THEN 'ITV1'
             when channel = 'ComedyCtrl' THEN 'ComedyCentral'
             when channel = 'Comedy Cen' THEN 'ComedyCentral'
             when channel = 'Sky Sp News' THEN 'Sky Spts News'
             when channel = 'Sky Sports HD1' THEN 'Sky Sports 1'
             when channel = 'Sky Sports HD1' THEN 'Sky Sports 1'
             when channel = 'FX+' THEN 'FX'
             when channel = 'Nick Replay' THEN 'Nickelodeon'
             when channel = 'Sky Sports HD2' THEN 'Sky Sports 2'
             when channel = 'Sky Sports HD3' THEN 'Sky Sports 3'
             when channel = 'Sky Sports HD4' THEN 'Sky Sports 4'
             when channel = 'mov4men2' THEN 'movies4men 2'
             when channel = 'mov4men' THEN 'movies4men'
             when channel = 'ComedyCtlX' THEN 'ComedyCtralX'
             when channel = 'horror ch' THEN 'horror channel'
             when channel = 'History +1 hour' THEN 'History'
             when channel = 'Disc.RT' THEN 'Disc.RealTime'
             when channel = 'Cartoon Net' THEN 'Cartoon Netwrk'
             when channel = 'Cartoon Net' THEN 'Cartoon Netwrk'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'Eurosport' THEN 'Eurosport UK'
             when channel = 'Food Netwrk' THEN 'Food Network'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'Animal Plnt' THEN 'Animal Planet'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'ESPN AmrcaHD' THEN 'ESPN America'
                                                    ELSE channel END AS Channel
        ,channel_name
INTO LkUpChannel
FROM #channel2
group by channel_name, channel
order by channel
;
--515 Row(s) affected


--drop table #names

--select * from LkUpChannel order by channel;
--select top 10 * from v066_output_1

Alter table v066_output_1
add     channel_name_agg Varchar(30)
;

SELECT  a.service_key
        ,(case when a.channel_name is null then full_name else a.channel_name end) as channel_name
into #name
FROM v066_output_1 a LEFT JOIN #pack b
        ON a.service_key = b.service_key
;


select distinct (service_key)
        ,channel_name
into #names
from #name
;

Update v066_output_1
SET channel_name = b.channel_name
FROM v066_output_1 a INNER JOIN #names b
        ON a.service_key = b.service_key
;

select * from v066_output_1 where channel_name_agg is null;

Update v066_output_1
SET channel_name_agg = channel
FROM v066_output_1 a INNER JOIN Lkupchannel b
        ON a.channel_name = b.channel_name
;
--31349849 Row(s) affected


select  max(weight) as weight
        ,account_number
        ,channel_name_agg
        ,day
        ,x_viewing_time_of_day
        ,genre_description
        ,sub_genre_description
        ,EPG_title
        ,capped_x_programme_viewed_duration
        ,service_key
        ,cb_key_household
        ,cb_key_family
        ,cb_key_individual
INTO v066_output_2_test
FROM v066_output_1
GROUP BY account_number
        ,channel_name_agg
        ,day
        ,x_viewing_time_of_day
        ,genre_description
        ,sub_genre_description
        ,EPG_title
        ,capped_x_programme_viewed_duration
        ,service_key
        ,cb_key_household
        ,cb_key_family
        ,cb_key_individual
;
--31385422 Row(s) affected

--drop table v066_output_2

select channel_name
        ,channel_name_agg
from v066_output_1_M
group by channel_name
,channel_name_agg
order by channel_name
;

select * from lkupchannel

-------Count number of upgraders in universe
select distinct(account_number)
--,u_upgraders
, max(weight) as weight
into #vol
 from v066_output_2_M
group by --u_upgraders
account_number
;

select --u_upgraders,
sum(weight) as vol
 from #vol
--group by u_upgraders
;


-----------------------------Add to master raw table

alter table v066_output_2
add (channel_category varchar(20));

update v066_output_2
set channel_category = p.channel_category
from v066_output_2 vie
left join  lkuppack p
on vie.service_key = p.service_key
;

select top 100 * from v066_output_2
order by channel_category desc, channel_name_agg desc
;


-------------------------------------------------------------------------Add Direct Marketing opt-in

SELECT  account_number
       ,CASE WHEN sav.cust_email_allowed             = 'Y' THEN 1 ELSE 0 END AS Email_Mkt_OptIn
       ,CASE WHEN sav.cust_postal_mail_allowed       = 'Y' THEN 1 ELSE 0 END AS Mail_Mkt_OptIn
       ,CASE WHEN sav.cust_telephone_contact_allowed = 'Y' THEN 1 ELSE 0 END AS Tel_Mkt_OptIn
       --,CASE WHEN sav.cust_sms_allowed               = 'Y' THEN 1 ELSE 0 END AS Txt_Mkt_OptIn  **Do not include as these are for service msg only
       ,CASE WHEN sav.cust_email_allowed             = 'Y'
              AND sav.cust_postal_mail_allowed       = 'Y'
              --AND sav.cust_sms_allowed               = 'Y'
              AND sav.cust_telephone_contact_allowed = 'Y'
             THEN 1
             ELSE 0
         END AS All_Mkt_OptIn
       ,CASE WHEN sav.cust_email_allowed             = 'Y'
               OR sav.cust_postal_mail_allowed       = 'Y'
               --OR sav.cust_sms_allowed               = 'Y'
               OR sav.cust_telephone_contact_allowed = 'Y'
             THEN 1
             ELSE 0
         END AS Any_Mkt_OptIn
  INTO #Opt_Ins
  FROM sk_prod.cust_single_account_view AS sav;
--24218957 Row(s) affected
--1m

select a.account_number
        ,max(All_Mkt_OptIn) as All_Mkt_OptIn
        ,max(ANY_Mkt_OptIn) as ANY_Mkt_OptIn
into #DMout
from #opt_ins a INNER JOIN v066_output_2 b
        ON a.account_number = b.account_number
group by a.account_number
;
--128133 Row(s) affected



ALTER TABLE v066_output_2
ADD (DM_opt_in_ANY tinyint
    ,DM_opt_in_ALL tinyint)
;

UPDATE v066_output_2
SET     DM_opt_in_ANY  = Any_Mkt_OptIn
        ,DM_opt_in_ALL = ALL_Mkt_OptIn
FROM v066_output_2 a LEFT JOIN #DMout b
        on a.account_number = b.account_number
;
--31385422 Row(s) affected

--select top 100 * from v066_output_2




------------------------------------------------------------------------------------------------
--B. Add Universe flags
------------------------------------------------------------------------------------------------
/*
flag each universe within this:
        1. Likely upgraders
        2. Premium Mums
        2. Segments - Aspiring Families, Enterprising & hardworking families, Middle income families
        3. Unstable customers - Customer Value Segments
        4. Offer Seekers
        5. Tenure - First year V 4-5 year

*/

--B1. Likely upgraders

SELECT  account_number
        ,Decile AS Ctrl_Decile
        ,score AS Ctrl_Score
INTO    #ATLmovies
FROM    models.model_scores
WHERE   model_run_date = '2011-11-22' AND model_name = 'Movies Control'
;--5855994 Row(s) affected

ALTER TABLE v066_output_2
ADD U_upgraders TINYINT;

UPDATE v066_output_2
SET U_upgraders = CASE WHEN Ctrl_Decile <5 THEN 1 ELSE 0 END
FROM v066_output_2 pay INNER JOIN #ATLmovies atl
        ON pay.account_number = atl.account_number
;--31149324 Row(s) affected








--B2. Premium Mums

/* Premium Mums = Female Family HH composition / Mid High – High Affluence
Likely to upgrade: Movies upgrade model deciles 1-4
*/

----------------------------------------------------------------add HH composition into agg viewing table (v066_output_2)

SELECT   cb_key_household
        ,household_composition
        ,cb_row_id
        ,head_of_household
        ,rank() over(partition by cb_key_household ORDER BY head_of_household desc, cb_row_id desc) as rank_hh
INTO #cv_keys
FROM sk_prod.EXPERIAN_CONSUMERVIEW;
--49655373 Row(s) affected

CREATE UNIQUE HG INDEX idx01 ON #cv_keys(cb_row_id);
CREATE        HG INDEX idx02 ON #cv_keys(cb_key_household);
CREATE        LF INDEX idx04 ON #cv_keys(rank_hh);

DELETE FROM #cv_keys WHERE rank_hh != 1

-------------------------------------------------------------------------------------here!!
SELECT   cv.cb_key_household
         ,case when household_composition = '00' then 'Families'
       when household_composition = '01' then 'Extended_family'
       when household_composition = '02' then 'Extended_household'
       when household_composition = '03' then 'Pseudo_family'
       when household_composition = '04' then 'Single_male'
       when household_composition = '05' then 'Single_female'
       when household_composition = '06' then 'Male_homesharers'
       when household_composition = '07' then 'Female_homesharers'
       when household_composition = '08' then 'Mixed_homesharers'
       when household_composition = '09' then 'Abbreviated_male_families'
       when household_composition = '10' then 'Abbreviated_female_families'
       when household_composition = '11' then 'Multi_occupancy_dwelling'
       else 'Unclassified' end as hhcomposition
INTO #HHcomp
FROM #cv_keys  AS cv
where           cv.cb_key_household in (select distinct cb_key_household from v066_output_2)
group by        cv.cb_key_household
                ,household_composition
;
--124015 Row(s) affected
commit;
--drop table #hhcomp;

select count(*) as count
        ,household_composition
from sk_prod.EXPERIAN_CONSUMERVIEW
group by  household_composition ;


alter table v066_output_2_M
add (HHcomposition varchar(60));

update v066_output_2_M
set HHcomposition = ilu.hhcomposition
from v066_output_2_M in1
left join  #HHcomp ilu
on in1.cb_key_household = ilu.cb_key_household
;
--31385422 Row(s) affected


----------------------------------------------------------------Add Affluence into agg viewing table (v066_output_2)


select          sav.account_number,
                max(CASE WHEN ilu_affluence in ('01','02','03','04')    THEN 'Very Low'
                     WHEN ilu_affluence in ('05','06')                  THEN 'Low'
                     WHEN ilu_affluence in ('07','08')                  THEN 'Mid Low'
                     WHEN ilu_affluence in ('09','10')                  THEN 'Mid'
                     WHEN ilu_affluence in ('11','12')                  THEN 'Mid High'
                     WHEN ilu_affluence in ('13','14','15')             THEN 'High'
                     WHEN ilu_affluence in ('16','17')                  THEN 'Very High'
                ELSE                                                         'Unknown'
                END) as sav_affluence,
                max(cb_key_household) as cb_key_household
into            #sav_dem                                                --drop table #sav_dem
from            sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
where           sav.account_number in (select distinct account_number from v066_output_2_M)
group by        sav.account_number;--114525 Row(s) affected
commit;


alter table v066_output_2_M
add( Affluence varchar(10));

update v066_output_2_M
set affluence = sav_affluence
from v066_output_2_M in1
left join  #sav_dem sav
on in1.account_number = sav.account_number;
--31385422 Row(s) affected

--select top 100 * from  v066_output_2



----------------------------------------------------------------Create the Prem Mums Universe Flag

alter table v066_output_2_M
add( U_Prem_Mum tinyint);

update v066_output_2_M
set U_Prem_Mum = CASE WHEN (
                            (affluence = 'Mid High' OR affluence = 'High' OR affluence = 'Very High')
                                AND (
                                        hhcomposition = 'Abbreviated_female_families'
                                        OR hhcomposition = 'Extended_family'
                                        OR hhcomposition = 'Extended_household'
                                        OR hhcomposition = 'Families'
                                        OR hhcomposition = 'Pseudo_family'
                                )
                            )THEN 1 ELSE 0 END;
--31385422 Row(s) affected


--2. Segments - Aspiring Families, Enterprising & hardworking families, Middle income families


------------------------------------------------------------------------------get mosaic data

SELECT  cb_row_id
        ,cb_key_household
        ,h_mosaic_uk_2009_group
        ,head_of_household
        ,rank() over(partition by cb_key_household ORDER BY head_of_household desc, cb_row_id desc) as rank_hh
INTO nodupes
FROM sk_prod.EXPERIAN_CONSUMERVIEW;
--3m
--49655373 Row(s) affected
CREATE UNIQUE HG INDEX idx01 ON nodupes(cb_row_id);
CREATE        HG INDEX idx02 ON nodupes(cb_key_household);
CREATE        LF INDEX idx04 ON nodupes(rank_hh);

DELETE FROM nodupes WHERE rank_hh != 1
--24758783 Row(s) affected



--- we need to understand what the segments mean:

alter table nodupes
add(mosaic_group_desc varchar(50));


update nodupes
set mosaic_group_desc =

case when h_mosaic_uk_2009_group =      'A'     then    'Alpha Territory'
 when h_mosaic_uk_2009_group =  'B'     then    'Professional Rewards'
 when h_mosaic_uk_2009_group =  'C'     then    'Rural Solitude'
 when h_mosaic_uk_2009_group =  'D'     then    'Small Town Diversity'
 when h_mosaic_uk_2009_group =  'E'     then    'Active Retirement'
 when h_mosaic_uk_2009_group =  'F'     then    'Suburban Mindsets'
 when h_mosaic_uk_2009_group =  'G'     then    'Careers and Kids'
 when h_mosaic_uk_2009_group =  'H'     then    'New Homemakers'
 when h_mosaic_uk_2009_group =  'I'     then    'Ex-Council Community'
 when h_mosaic_uk_2009_group =  'J'     then    'Claimant Cultures'
 when h_mosaic_uk_2009_group =  'K'     then    'Upper Floor Living'
 when h_mosaic_uk_2009_group =  'L'     then    'Elderly Needs'
 when h_mosaic_uk_2009_group =  'M'     then    'Industrial Heritage'
 when h_mosaic_uk_2009_group =  'N'     then    'Terraced Melting Pot'
 when h_mosaic_uk_2009_group =  'O'     then    'Liberal Opinions'
 when h_mosaic_uk_2009_group =  'U'     then    'Unclassified'
 else 'Unclassified' end;
--24896590 Row(s) affected

-- now we need to match this back to our master table


alter table nodupes
add(account_number varchar(20));


update nodupes
set nod.account_number  = vre.account_number
from nodupes nod
left join V066_output_2 vre
on nod.cb_key_household = vre.cb_key_household;

--24896590 Row(s) affected

-- --checks
-- select top 100 * from nodupes order by h_mosaic_uk_2009_type
-- select count(*) from nodupes where account_number is null -- good!
--
-- now put it in the master table

alter table v066_output_2
add    mosaic_group_desc varchar(50)
);


update v066_output_2
set    al1.mosaic_group_desc = nod.mosaic_group_desc
from v066_output_2 al1
        left join nodupes nod
on nod.account_number = al1.account_number
;

SELECT
        account_number,
        case when mosaic_group_desc in('Careers and Kids', 'New Homemakers') THEN 'Aspiring_Young_Families'
         when mosaic_group_desc in('Ex-Council Community'  ,'Claimant Cultures'
                ) THEN 'Enterprising_Hardworking_Families'
         when mosaic_group_desc in('Suburban Mindsets', 'Industrial Heritage', 'Terraced Melting Pot') THEN 'Middle_Income_Families'
         when mosaic_group_desc in('Alpha Territory', 'Professional Rewards', 'Rural Solitude') THEN 'Affluent_Successful_Families'
         when mosaic_group_desc in('Small Town Diversity', 'Active Retirement', 'Elderly Needs', 'Upper Floor Living' ) THEN 'Other'
         ELSE 'unknown' END AS segments
INTO #mov
FROM v066_output_2
;
--130767 Row(s) affected

alter table v066_output_2
add U_mosaic varchar(50);

Update  v066_output_2
SET sky.U_mosaic =  s.segments
from v066_output_2 sky INNER JOIN #mov s
on s.account_number = sky.account_number;
--130767 Row(s) affected



--3. Unstable customers - Customer Value Segments





------------------------------------------------ 01 - SAV = First Activation Date & Subscription ID

select          csh.account_number, min(status_start_dt) as first_activation_dt
into            v066_CVS
from            sk_prod.cust_subs_hist csh
        inner join v066_output_2 base
                on csh.account_number = base.account_number
where           subscription_sub_type in ('DTV Primary Viewing','Broadband DSL Line')
and             status_code = 'AC'
and             status_code_changed = 'Y'
group by        csh.account_number; --130767 Row(s) affected
commit;
CREATE HG INDEX idx1 ON v066_CVS(account_number);

--select top 10 * from v066_cvs;


------------------------------------------------ 02 - UPDATE table
ALTER table     v066_cvs
ADD(
       customer_value_segment  VARCHAR(20)     NULL
       ,active_days             INTEGER         NULL
       ,CUSCAN_ever             INTEGER         DEFAULT 0
       ,CUSCAN_2Yrs             INTEGER         DEFAULT 0
       ,SYSCAN_ever             INTEGER         DEFAULT 0
       ,SYSCAN_2Yrs             INTEGER         DEFAULT 0
       ,AB_ever                 INTEGER         DEFAULT 0
       ,AB_2Yrs                 INTEGER         DEFAULT 0
       ,PC_ever                 INTEGER         DEFAULT 0
       ,PC_2Yrs                 INTEGER         DEFAULT 0
       ,TA_2yrs                 INTEGER         DEFAULT 0
       ,min_prem_2yrs           INTEGER         DEFAULT 0
       ,max_prem_2yrs           INTEGER         DEFAULT 0
       ,snapshot_date           DATE            NULL
);

CREATE   LF INDEX idx03 ON v066_cvs(customer_value_segment);

UPDATE v066_CVS
SET snapshot_date  = '2011-11-22';

UPDATE          v066_cvs
SET             active_days = DATEDIFF(day,first_activation_dt, snapshot_date);


----------------------------------------------- 03 - Long term events

--historic status event changes
CREATE TABLE    #status_events (
                snapshot_date            date           not null
                ,account_number          VARCHAR(20)     NOT NULL
                ,effective_from_dt       DATE            NOT NULL
                ,status_code             VARCHAR(2)      NOT NULL
                ,event_type              VARCHAR(20)     NOT NULL
);

CREATE HG INDEX idx01 ON #status_events(account_number);
CREATE LF INDEX idx02 ON #status_events(event_type);
CREATE DATE INDEX idx03 ON #status_events(effective_from_dt);
CREATE DATE INDEX idx04 ON #status_events(snapshot_date);


INSERT INTO     #status_events (snapshot_date, account_number, effective_from_dt, status_code, event_type)
SELECT          base.snapshot_date
                ,csh.account_number
                ,csh.effective_from_dt
                ,csh.status_code
                ,CASE WHEN status_code = 'PO'              THEN 'CUSCAN'
                      WHEN status_code = 'SC'              THEN 'SYSCAN'
                      WHEN status_code = 'AB'              THEN 'ACTIVE BLOCK'
                      WHEN status_code = 'PC'              THEN 'PENDING CANCEL'
                END AS event_type
FROM            sk_prod.cust_subs_hist csh
        INNER JOIN v066_cvs base
                ON csh.account_number = base.account_number
WHERE           csh.subscription_sub_type = 'DTV Primary Viewing'
AND             csh.status_code_changed = 'Y'
AND             csh.effective_from_dt <= base.snapshot_date
AND             ((csh.status_code IN ('AB','PC') AND csh.prev_status_code = 'AC')
         OR (csh.status_code IN ('PO','SC') AND csh.prev_status_code IN ('AC','AB','PC')));


-- Update value Segments

UPDATE          v066_cvs
SET             CUSCAN_ever = tgt.CUSCAN_ever
                ,CUSCAN_2Yrs = tgt.CUSCAN_2Yrs
                ,SYSCAN_ever = tgt.SYSCAN_ever
                ,SYSCAN_2Yrs = tgt.SYSCAN_2Yrs
                ,AB_ever = tgt.AB_ever
                ,AB_2Yrs = tgt.AB_2Yrs
                ,PC_ever = tgt.PC_ever
                ,PC_2Yrs = tgt.PC_2Yrs
FROM            v066_cvs AS base
        INNER JOIN (
                    SELECT vs.snapshot_date, vs.account_number
                           --CUSCAN
                           ,SUM(CASE WHEN se.status_code = 'PO'
                                      AND  se.effective_from_dt <= vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS CUSCAN_ever
                           ,SUM(CASE WHEN se.status_code = 'PO'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.snapshot_date) AND vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS CUSCAN_2Yrs
                           --SYSCAN
                           ,SUM(CASE WHEN se.status_code = 'SC'
                                      AND  se.effective_from_dt <= vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS SYSCAN_ever
                           ,SUM(CASE WHEN se.status_code = 'SC'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.snapshot_date) AND vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS SYSCAN_2Yrs
                           --Active Block
                           ,SUM(CASE WHEN se.status_code = 'AB'
                                      AND se.effective_from_dt <= vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS AB_ever
                           ,SUM(CASE WHEN se.status_code = 'AB'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.snapshot_date) AND vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS AB_2Yrs
                           --Pending Cancel
                           ,SUM(CASE WHEN se.status_code = 'PC'
                                      AND se.effective_from_dt <= vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS PC_ever
                           ,SUM(CASE WHEN se.status_code = 'PC'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.snapshot_date) AND vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS PC_2Yrs
                      FROM v066_cvs AS vs
                           INNER JOIN #status_events AS se ON vs.account_number = se.account_number and vs.snapshot_date = se.snapshot_date
                  GROUP BY vs.snapshot_date, vs.account_number
       )AS tgt on base.account_number = tgt.account_number and base.snapshot_date = tgt.snapshot_date;


------------------------------------------------------------ 04 - TA Events


--List all unique days with TA event
SELECT  DISTINCT
        cca.account_number
        ,base.snapshot_date
       ,cca.attempt_date
  INTO #ta
  FROM sk_prod.cust_change_attempt AS cca
       INNER JOIN v066_cvs base  on cca.account_number = base.account_number
 WHERE change_attempt_type = 'CANCELLATION ATTEMPT'
   AND created_by_id NOT IN ('dpsbtprd', 'batchuser')
   AND Wh_Attempt_Outcome_Description_1 in ( 'Turnaround Saved'
                                            ,'Legacy Save'
                                            ,'Turnaround Not Saved'
                                            ,'Legacy Fail'
                                            ,'Home Move Saved'
                                            ,'Home Move Not Saved'
                                            ,'Home Move Accept Saved')
   AND cca.attempt_date BETWEEN DATEADD(day,-729,snapshot_date) AND snapshot_date ;


CREATE HG INDEX idx01 ON #ta(account_number);

-- Update TA flags
UPDATE  v066_cvs
   SET  TA_2Yrs = tgt.ta_2Yrs
  FROM v066_cvs AS base
       INNER JOIN (
                    SELECT vs.snapshot_date, vs.account_number
                          ,SUM(CASE WHEN ta.attempt_date BETWEEN DATEADD(day,-729,vs.snapshot_date) AND vs.snapshot_date
                                     THEN 1 ELSE 0 END) AS ta_2Yrs
                      FROM v066_cvs AS vs
                           INNER JOIN #ta AS ta ON vs.account_number = ta.account_number and vs.snapshot_date = ta.snapshot_date
                  GROUP BY vs.snapshot_date, vs.account_number
       )AS tgt on base.account_number = tgt.account_number and base.snapshot_date = tgt.snapshot_date;

------------------------------------------------------ 05 - Min Max Premiums

UPDATE  v066_cvs
   SET  min_prem_2Yrs = tgt.min_prem_lst_2_yrs
       ,max_prem_2Yrs = tgt.max_prem_lst_2_yrs
  FROM  v066_cvs AS acc
        INNER JOIN (
                   SELECT  base.snapshot_date, base.account_number
                          ,MAX(cel.prem_movies + cel.prem_sports ) as max_prem_lst_2_yrs
                          ,MIN(cel.prem_movies + cel.prem_sports ) as min_prem_lst_2_yrs
                     FROM sk_prod.cust_subs_hist as csh
                          INNER JOIN v066_cvs as base on csh.account_number = base.account_number
                          INNER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
                    WHERE csh.subscription_type      =  'DTV PACKAGE'
                      AND csh.subscription_sub_type  =  'DTV Primary Viewing'
                      AND status_code in ('AC','AB','PC')
                      AND ( -- During 2 year Period
                            (    csh.effective_from_dt BETWEEN DATEADD(day,-729,base.snapshot_date) AND base.snapshot_date
                             AND csh.effective_to_dt >= csh.effective_from_dt
                             )
                            OR -- at start of 2 yr period
                            (    csh.effective_from_dt <= DATEADD(day,-729,base.snapshot_date)
                             AND csh.effective_to_dt   > DATEADD(day,-729,base.snapshot_date)  -- limit to report period
                             )
                          )
                  GROUP BY base.snapshot_date, base.account_number
        )AS tgt ON acc.account_number = tgt.account_number and acc.snapshot_date = tgt.snapshot_date;



------------------------------------------------------ 06 - Make Value Segments


UPDATE v066_cvs
   SET       customer_value_segment =     CASE WHEN active_days < 365                            -- All accounts in first Year
                                THEN 'BEDDING IN - Yr1'

                                WHEN active_days BETWEEN 365 AND 730
                                THEN 'BEDDING IN - Yr2'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND ta_2Yrs = 0                                  -- No TA's in last 2 years
                                 AND min_prem_2yrs = 4                            -- Always top tier for last 2 years
                                THEN 'PLATINUM'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'GOLD'

                                WHEN CUSCAN_2Yrs + SYSCAN_2Yrs = 0                -- No Churn in last 2 years
                                 AND AB_2Yrs + PC_2Yrs = 0                        -- No AB/PC 's In last 2 Years
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'SILVER'

                                WHEN CUSCAN_ever + SYSCAN_ever > 0                -- All Churners
                                  OR AB_Ever + PC_Ever + ta_2Yrs >= 3             -- Blocks , cancels in last 2 years + ta in last 2 years >= 3
                                THEN 'UNSTABLE'

                                WHEN max_prem_2Yrs > 0                            -- Has Had prems in last 2 years
                                THEN 'BRONZE'

                                ELSE 'COPPER'                                        -- everyone else
                            END;
commit;

------------------------------------------------------ 07 - set cvs in output table
ALTER TABLE  v066_output_2_M
ADD U_CVS varchar(20) NULL;

UPDATE v066_output_2_M
SET       o.U_cvs = cvs.customer_value_segment
FROM v066_cvs cvs INNER JOIN v066_output_2_M o
                ON o.account_number = cvs.account_number
;--130767 Row(s) affected

select top 10 * from v066_output_2;

--4. Offer Seekers

/*----------------------------------------------------------------------------------------------------------------------------
PROJECT:        OFFER SEEKING BEHAVIOUR GROUP DERIVATION
                BASED ON THE SKY ANALYSIS SK4187 - OFFER SEEKERS - BASED ON THE CRITERIA STREAM 8
CREATED:        2012-06-19
CREATED BY:     RATTEE HENSIRISAKUL
VERSION:        1.0

SECTIONS:       PART A: EVENT TABLE STRUCTURE
                PART B: TA ATTEMPTS
                PART C: PREVIOUS TA / PAT EVENTS (FREQUENCY, RECENCY)
                PART E: OFFERS WITHIN TWO YEARS PRIOR TO THE TA EVENTS
                PART F: CONTINUOUS TENURE
                PART G: OFFER SEEKING SEGMENTS

NOTE:
--  PLEASE UPDATE A DATE RANGE IN THE CODE PART B (DERIVE TA ATTEMPTS) AND/OR
    JOIN THE SELECTION WITH ACCOUNTS FROM YOUR PROJECT UNIVERSE AS APPROPRIATE.
--  THE OFFER LOOK-UP TABLE IN PART E (CITEAM.SKY742_ATARI_OFFER_V3) CONTAINS OFFERS UP TO WK46.
    ANY NEW OFFERS RELEASED AFTER THAT ARE NOT PICKED UP. THE TABLE NEEDS UPDATING OVER TIME.
--  THE RECENT WORK ON OFFER SEEKING BEHAVIOUR - STREAM 8 - IGNORES USING CONTINUOUS TENURE AS PART OF THE CRITERIA
    PLEASE DISCUSS WITH THE PROJECT LEAD FOR FURTHER SOLUTION.


High Offer Seeking Behaviours:

-       Continuous Tenure of 2 Years+

-       Came into a TA attempt with existing offer(s) on account, OR within 3 months after the last offer ended

-       Were applied with at least 2 offers in the last 2 years.



Low Offer Seeking Behaviours:

-       Continuous Tenure of 2 Years+

-       Did not have any previous saved TA or PAT attempts in the last 2 years

-       Did not have offer applied to account in the last 2 years, OR the last offer ended more than 15 months ago.

----------------------------------------------------------------------------------------------------------------------------*/



------------------------------------------------------------------------------------------------ PART A: EVENT TABLE STRUCTURE
if object_id('SK4187_Events') is not null then drop table SK4187_Events end if;
create table    SK4187_Events
            (   id                          bigint              identity
                ,event_type                 varchar(3)          not null                -- TA / PAT
                ,account_number             varchar(20)         not null
                ,event_dt                   date                not null
                ,saved                      tinyint             default 0               -- 2 = saved / 1 = partial saved / 0 = not saved
                ,any_prev_saved_attempts_vol        integer     default 0               -- Saved attempts in the last two years prior to the event (Any Event Type)
                ,any_last_saved_attempt_dt          date        default null            -- Last saved attempt in the last two years (Any Event Type)
                ,any_last_saved_attempt_mth         tinyint     default null            -- Number of months bretween the latest attempt and the previous any saved attempt
                                                                                        -- 0 => Offer lastes for less then a month / null = no offer
                ,offer_vol                  integer             default 0               -- Number of offers applied in the last two years prior to the event (Exclude offers with zero amount)
                ,last_offer_end_dt          date                default null            -- If an account previously had any offer, what was the most recent offer end date
                ,on_offer                   bit                 default 0               -- If an account came into the event with offer already on account
                ,last_offer_end_mth         tinyint             default null            -- A period between the last offer end and the event date
                ,first_activation_dt        date                default null
                ,last_reinstate_dt          date                default null
                ,continuous_tenure          varchar(30)         default null    );

create hg index idx_id      on SK4187_Events(id);
create hg index idx_acc     on SK4187_Events(account_number);
create date index idx_evtdt on SK4187_Events(event_dt);
create unique hg index idx_acc_evt on SK4187_Events(account_number, event_dt);
create unique hg index idx_acc_evt_type on SK4187_Events(account_number,event_type, event_dt);
create date index idx_first_act_dt on SK4187_Events(first_activation_dt);
grant select on SK4187_Events to public;
commit;



---------------------------------------------------------------------------------------------------------- PART B: TA ATTEMPTS
-- DERIVE TA ATTEMPTS
select          ca.cb_row_id
                ,ca.account_number
                ,ca.attempt_date
                ,ca.attempt_datetime
                ,case   when ca.wh_attempt_outcome_description_1 in (   'Turnaround Saved'
                                                                        ,'Legacy Save'
                                                                        ,'Home Move Saved'
                                                                        ,'Home Move Accept Saved'
                                                                        ,'Nursery Saved'            )           then    2
                        else                                                                                            0
                 end as saved
into            #TA_Attempts -- DROP TABLE #TA_Attempts;
from            sk_prod.cust_change_attempt as ca
            inner join sk_prod.cust_subscriptions as sub
                on         ca.subscription_id = sub.subscription_id
where
            sub.ph_subs_subscription_sub_type = 'DTV Primary Viewing'
            and ca.change_attempt_type = 'CANCELLATION ATTEMPT'
            and ca.attempt_date between '2011-11-22' and '2011-05-22'           -- 6 mths pre target date of non-movies audience
            and ca.created_by_id not in ('dpsbtprd', 'batchuser')
            and ca.Wh_Attempt_Outcome_Description_1 in (    'Turnaround Saved'
                                                            ,'Legacy Save'
                                                            ,'Turnaround Not Saved'
                                                            ,'Legacy Fail'
                                                            ,'Home Move Saved'
                                                            ,'Home Move Not Saved'
                                                            ,'Home Move Accept Saved'
                                                            ,'Nursery Saved'
                                                            ,'Nursery Not Saved');
commit;

--

drop table  #TA_Attempts
-- REMOVE DUPES - SELECTING ONE ATTEMPT PER ACCOUNT PER DAY BASED ON THE MOST RECENT EVENT TIME
select          *
                ,rank() over(partition by ca.account_number, ca.attempt_date order by ca.attempt_datetime desc, ca.cb_row_id desc) as rank
into            #TA_Attempts_Deduped -- drop table #TA_Attempts_Deduped;
from            #TA_Attempts as ca
where           ca.account_number in (select distinct account_number from v066_output_2)
;
commit;

delete from     #TA_Attempts_Deduped
where           rank > 1;
commit;

-- COPY TA EVENTS TO THE EVENTS BASE TABLE
insert into     SK4187_Events (event_type,account_number, event_dt, saved)
select          'TA' as event_type
                ,account_number
                ,attempt_date
                ,saved
from            #TA_Attempts_Deduped;
commit;

/* CHECK DUPES AT THE EVENT LEVEL (ACCOUNT + ATTEMPT DATE)
NOTE: IDEALLY, THERE SHOULD NOT BE ANY DUPES AT THE EVENT LEVEL
select account_number, attempt_date from #TA_Attempts_Deduped group by account_number, attempt_date having count(*) > 1;
select account_number, event_dt from SK4187_Events group by account_number, event_dt having count(*) > 1;
*/

-- CLEAR DB SPACE
drop table      #TA_Attempts_Deduped;
drop table      #TA_Attempts;
commit;






------------------------------------------------------------------------------ PART C: PREVIOUS TA / PAT EVENTS (FREQUENCY, RECENCY)
-- DERIVE TA/PAT EVENTS IN THE LAST TWO YEARS PRIOR TO THE EVENTS IN THE EVENT BASE TABLE.
select          base.id
                ,base.account_number
                ,base.event_dt
                ,ca.attempt_date
                ,case   when ca.change_attempt_type = 'CANCELLATION ATTEMPT'        then 'TA'
                        when ca.change_attempt_type = 'DOWNGRADE ATTEMPT'           then 'PAT'
                 end as attempt_type
                ,case   when ca.Wh_Attempt_Outcome_Description_1 in (   'Turnaround Saved'
                                                                        ,'Legacy Save'
                                                                        ,'Home Move Saved'
                                                                        ,'Home Move Accept Saved'
                                                                        ,'Nursery Saved'
                                                                        ,'PAT Partial Save'
                                                                        ,'PAT Save'                 ) then  1
                        else                                                                                0
                 end as saved
                ,ca.attempt_datetime
                ,ca.cb_row_id
into            #Attempts_2Yrs -- drop table #Attempts_2Yrs;
from            sk_prod.cust_change_attempt as ca
            inner join SK4187_Events as base                on      ca.account_number = base.account_number
                                                                and ca.attempt_date between dateadd(year,-2,base.event_dt) and dateadd(day,-1,base.event_dt)
            inner join sk_prod.cust_subscriptions as sub    on      ca.subscription_id = sub.subscription_id
where           sub.ph_subs_subscription_sub_type= 'DTV Primary Viewing'
            and ca.change_attempt_type in ('CANCELLATION ATTEMPT','DOWNGRADE ATTEMPT')
            and ca.created_by_id not in ('dpsbtprd', 'batchuser')
            and ca.Wh_Attempt_Outcome_Description_1 in (    'Turnaround Saved'
                                                            ,'Legacy Save'
                                                            ,'Turnaround Not Saved'
                                                            ,'Legacy Fail'
                                                            ,'Home Move Saved'
                                                            ,'Home Move Not Saved'
                                                            ,'Home Move Accept Saved'
                                                            ,'Nursery Saved'
                                                            ,'Nursery Not Saved'
                                                            ,'PAT No Save'
                                                            ,'PAT Partial Save'
                                                            ,'PAT Save'                 );
commit;


-- DEDUPE RECORDS DOWN TO ONE EVENT PER ACCOUNT PER DAY
select          id
                ,account_number
                ,event_dt
                ,attempt_date
                ,attempt_type
                ,saved
                ,rank() over(partition by id, attempt_date order by attempt_type desc, attempt_datetime desc, cb_row_id desc) as rank
into            #Attempts_2Yrs_Deduped  -- drop table #Attempts_2Yrs_Deduped;
from            #Attempts_2Yrs;
commit;

-- REMOVE DUPES
delete from     #Attempts_2Yrs_Deduped
where           rank > 1;
commit;

/* CHECK
-- CHECK OUTCOME
select top 1000 * from #Attempts_2Yrs_Deduped;
-- CHECK DUPES AT THE EVENT LEVEL
select id, attempt_date from #Attempts_2Yrs_Deduped group by id, attempt_date having count(*) > 1;
*/

-- AGGREGATE ANY PREVIOUS SAVED ATTEMPT RECORDS TO GET FREQUENCY/RECENCY
update          SK4187_Events as base
set             base.any_prev_saved_attempts_vol = tgt.any_prev_saved_attempts_vol
                ,base.any_last_saved_attempt_dt = tgt.any_last_saved_attempt_dt
from
                    (   select          id
                                        ,count(account_number) as any_prev_saved_attempts_vol
                                        ,max(attempt_date) as any_last_saved_attempt_dt
                        from            #Attempts_2Yrs_Deduped
                        where           rank = 1            -- SELECT ONLY UNIQUE EVENTS (ONE CALL PER ACCOUNT PER DAY)
                                    and saved = 1           -- SELECT PREVIOUS EVENTS THAT WERE SAVED
                        group by        id
                     ) as tgt
where           base.id = tgt.id;
commit;
-- 2611626 Row(s) affected


-- CALCULATE NUMBER OF MONTHS BETWEEN THE LATEST ATTEMPT AND THE PREVIOUS ONE.
update          SK4187_Events
set             any_last_saved_attempt_mth = case when any_last_saved_attempt_dt is not null then datediff(month,any_last_saved_attempt_dt, event_dt) else null end;
commit;

/* CHECK LAYOUT
select top 300 * from SK4187_Events;
*/

-- CLEAR DB SPACE
drop table #Attempts_2Yrs;
drop table #Attempts_2Yrs_Deduped;
commit;




----------------------------------------------------------------------- PART E: OFFERS WITHIN TWO YEARS PRIOR TO THE TA EVENTS
-- CREATE OFFER STABLE STRUCTURE
if object_id('SK4187_Offers') is not null then drop table SK4187_Offers end if;
create table    SK4187_Offers
            (   id                          bigint                  identity
                ,account_number             varchar(20)             not null
                ,initial_effective_dt       date                    not null
                ,offer_start_dt             date                    default null
                ,offer_end_dt               date                    default null
                ,offer_id                   integer                 not null
                --,offer_amount               decimal(12,2)           default 0
                --,fully_loaded_amount        decimal(12,2)           default 0
                                                                                    );

create hg index idx_acc         on SK4187_Offers(account_number);
create hg index idx_off_id      on SK4187_Offers(offer_id);
create date index idx_ini_dt    on SK4187_Offers(initial_effective_dt);
create date index idx_start_dt  on SK4187_Offers(offer_start_dt);
create date index idx_end_dt    on SK4187_Offers(offer_end_dt);
grant select on SK4187_Offers to public;


-- DERIVE MIN/MAX TA ATTEMPT DATES FROM THE EVENT BASE TABLE
select          account_number
                ,min(event_dt) as min_event_dt
                ,max(event_dt) as max_event_dt
into            #Accounts
from            SK4187_Events
group by        account_number;

create hg index idx_acc         on #Accounts(account_number);
create date index idx_min_dt    on #Accounts(min_event_dt);
create date index idx_max_dt    on #Accounts(max_event_dt);
commit;


-- EXTRACT OFFERS TWO YEARS PRIOR TO THE TA EVENT
insert into     SK4187_Offers(account_number, offer_id, initial_effective_dt, offer_start_dt, offer_end_dt )
select          base.account_number
                ,cpo.offer_id
                ,cpo.initial_effective_dt
                ,coalesce(cpo.offer_start_dt, cpo.initial_effective_dt) as derived_offer_start_dt
                -- Manipulate offer date variables (Agreed by the business)
                        -- HW offers have the same start and end dates
                ,case   when        cpo.product_offer_type = 'NRC'                              then cast(coalesce(cpo.offer_end_dt,cpo.offer_start_dt, cpo.initial_effective_dt) as date)
                        -- SW offers => the duration is known, but the offer lasted for more than a year or less than 28 days => overwritten the end date based on the duration
                        when        cpo.product_offer_type = 'RC'
                                and cpo.offer_start_dt is not null
                                and cpo.offer_end_dt is not null
                                and cpo.offer_start_dt < cpo.offer_end_dt
                                and ao.offer_duration is not null
                                and (       datediff(day,offer_start_dt,offer_end_dt) < 28
                                        or  datediff(day,offer_start_dt,offer_end_dt) > 366)    then cast(dateadd(month, ao.offer_duration, cpo.offer_start_dt) as date)

                        -- SW offers => the offer lasted for more than a year => change the end date to a year from the start date
                        when        cpo.product_offer_type = 'RC'
                                and cpo.offer_start_dt is not null
                                and cpo.offer_end_dt is not null
                                and cpo.offer_start_dt < cpo.offer_end_dt
                                and datediff(day,offer_start_dt,offer_end_dt) > 366             then cast(dateadd(year, 1, cpo.offer_start_dt) as date)
                        -- SW offers => the offer lasted for more than a year => change the end date to a year from the derived start date
                        when        cpo.product_offer_type = 'RC'
                                and cpo.offer_end_dt is not null
                                and derived_offer_start_dt < cpo.offer_end_dt
                                and datediff(day,derived_offer_start_dt,offer_end_dt) > 366     then cast(dateadd(year, 1, derived_offer_start_dt) as date)
                        -- SW offers => normal offer records
                        when        cpo.product_offer_type = 'RC'
                                and cpo.offer_start_dt is not null
                                and cpo.offer_end_dt is not null
                                and cpo.offer_start_dt <= cpo.offer_end_dt
                                and datediff(day,cpo.offer_start_dt,cpo.offer_end_dt) between 0 and 366 then cpo.offer_end_dt
                        -- SW offers => near normal offer records (using the derived start dt)
                        when        cpo.product_offer_type = 'RC'
                                and cpo.offer_end_dt is not null
                                and derived_offer_start_dt <= cpo.offer_end_dt
                                and datediff(day,derived_offer_start_dt,offer_end_dt) between 0 and (366 + 14)  then cast(cpo.offer_end_dt as date)

                        -- SW offers => the offer started later than the offer end date, the duration is known => Still stick with the offer start date
                        when        cpo.product_offer_type = 'RC'
                                and cpo.offer_start_dt is not null
                                and cpo.offer_end_dt is not null
                                and cpo.offer_start_dt > cpo.offer_end_dt
                                and ao.offer_duration is not null                               then cast(dateadd(month,ao.offer_duration,cpo.offer_start_dt) as date)
                        -- SW offers => the offer started later than the offer end date => use the offer start date as the end date
                        when        cpo.product_offer_type = 'RC'
                                and cpo.offer_start_dt is not null
                                and cpo.offer_end_dt is not null
                                and cpo.offer_start_dt > cpo.offer_end_dt                       then cpo.offer_start_dt
                        -- SW offers => the rest scenarios
                        when        cpo.product_offer_type = 'RC'                               then cast(coalesce(cpo.offer_end_dt,dateadd(month,coalesce(ao.offer_duration,0),derived_offer_start_dt),derived_offer_start_dt) as date)
                        -- The rest use the start date
                        else cast(coalesce(cpo.offer_start_dt, cpo.initial_effective_dt) as date)   -- Use offer start/applied date if end date can't be calculated.
                 end as derived_offer_end_dt
from            sk_prod.cust_product_offers as cpo

            inner join #Accounts as base on     cpo.account_number = base.account_number
                                            and cpo.initial_effective_dt between dateadd(year, -2, min_event_dt) and dateadd(day,-1,max_event_dt)

            inner join CITeam.SKY742_Atari_Offer_V3 as ao on cpo.offer_id = ao.offer_id

where           cpo.offer_id not in (   select      offer_id
                                        from        citeam.sk2010_offers_to_exclude )
            and cpo.offer_id not in (40392, 40393)  -- offer desc ('PPV #1 Administration Charge','PPV EURO1 Administration Charge')
            and cpo.offer_amount < 0
            and (       cpo.orig_portfolio_offer_id is null
                    or  cpo.orig_portfolio_offer_id = '?'   )                               -- Exclude auto-transferred offers
            and ao.offer_group not in (     '5 10 15 Price Point Discount'                  -- Exclude certain offer groups.
                                            ,'Free Relocate Equipment'
                                            ,'Free Relocate Equipment with MR'
                                            ,'Global - Price Change'
                                            ,'Global Offer'
                                            ,'Price Hold Offer'
                                            ,'Upfront Payment'
                                            ,'0'                  );
commit;

-- APPEND OFFER VOLUME TO THE EVENT BASE TABLE
update          SK4187_Events as base
set             base.offer_vol = tgt.aggregated_offer_vol
from                (
                        select          evt.account_number
                                        ,evt.event_dt
                                        ,count(offers.offer_id) as aggregated_offer_vol
                        from            SK4187_Events as evt
                                    inner join SK4187_Offers as offers  on      offers.account_number = evt.account_number
                                                                            and offers.initial_effective_dt between dateadd(year,-2,evt.event_dt) and dateadd(day,-1,evt.event_dt)
                        group by        evt.account_number
                                        ,evt.event_dt
                     ) as tgt
where           base.account_number = tgt.account_number
            and base.event_dt = tgt.event_dt;
commit;


-- APPEND THE LAST OFFER END DATE
update          SK4187_Events as base
set             base.last_offer_end_dt = tgt.last_offer_end_dt
from            (
                    select          evt.id
                                    ,max(offers.offer_end_dt) as last_offer_end_dt
                    from            SK4187_Events as evt
                                inner join SK4187_Offers as offers  on      evt.account_number = offers.account_number
                                                                        and offers.initial_effective_dt between dateadd(year,-2,evt.event_dt) and dateadd(day,-1,evt.event_dt)
                                                                                -- Offer initiated before the event date
                    where           offers.offer_start_dt < evt.event_dt        -- Offer started before the event date
                                and offers.offer_end_dt <= evt.event_dt         -- Offer ended before or on the event date
                    group by        evt.id

                 ) as tgt
where           base.id = tgt.id;
commit;
-- NOTE: ON_OFFER FLAG TAKES PRIORITY OVER LAST_OFFER_END_DT VARIABLE AS THE QUERY ABOVE ONLY CONSIDERS OFFERS THAT ENDED BEFORE OR ON THE EVENT DATE


-- FLAG EVENT THAT WAS ON OFFER
update          SK4187_Events as base
set             base.on_offer = tgt.on_offer
from            (
                    select          evt.id
                                    ,max(case when offers.offer_end_dt > evt.event_dt then 1 else 0 end) as on_offer
                    from            SK4187_Events as evt
                                inner join SK4187_Offers as offers  on      evt.account_number = offers.account_number
                                                                        and offers.initial_effective_dt between dateadd(year,-2,evt.event_dt) and dateadd(day,-1,evt.event_dt)
                                                                                -- Offer initiated before the event date
                    where           offers.offer_start_dt < evt.event_dt        -- Offer started before the event date
                    group by        evt.id

                 ) as tgt
where           base.id = tgt.id;
commit;

-- NUMBER OF MONTH BETWEEN THE LAST OFFER END AND THE EVENT
update          SK4187_Events
set             last_offer_end_mth = case when last_offer_end_dt is not null then datediff(month,last_offer_end_dt, event_dt) else null end;

/* CHECK
select top 300 * from SK4187_Events;
select min(datediff(day,last_offer_end_dt, event_dt)), max(datediff(day,last_offer_end_dt, event_dt)) from SK4187_Events where last_offer_end_dt is not null;
NOTE: ON_OFFER FLAG TAKES PRIORITY OVER LAST_OFFER_END_DT VARIABLE.
*/

-- CLEAR DB SPACE
drop table #Accounts;
commit;

---------------------------------------------------------------------------------------------------- PART F: CONTINUOUS TENURE
-- IDENTIFY THE FIRST DTV ACTIVATION DATE
update          SK4187_Events as base
set             base.first_activation_dt = tgt.first_activation_dt
from
                (
                    select          base.account_number
                                    ,min(csh.status_start_dt) as first_activation_dt            -- First activation date
                    from            (   select          account_number
                                        from            SK4187_Events
                                        group by        account_number  ) as base
                                inner join sk_prod.cust_subs_hist as csh    on      base.account_number = csh.account_number
                    where           csh.subscription_sub_type = 'DTV Primary Viewing'           -- DTV subscription
                                and csh.status_code_changed = 'Y'                               -- Status code has changed
                                and csh.status_code = 'AC'                                      -- Subscription process started
                                and csh.prev_status_code not in ('PO','SC')                     -- Not previously churned
                                and csh.status_start_dt < csh.status_end_dt
                                and csh.cb_key_household is not null
                                and csh.cb_key_household <> 0
                    group by        base.account_number
                 ) as tgt
where           base.account_number = tgt.account_number;
commit;

/* CHECK
select count(*), count(first_activation_dt) from SK4187_Events;
*/

-- IDENTIFY THE LATEST REINSTATE ASSOCIATED WITH EACH EVENT
update          SK4187_Events as base
set             base.last_reinstate_dt = tgt.last_reinstate_dt
from            (
                    select          base.id
                                    ,max(csh.status_start_dt) as last_reinstate_dt              -- Latest reinstate date
                    from            SK4187_Events as base
                                inner join sk_prod.cust_subs_hist as csh    on      base.account_number = csh.account_number
                                                                                and base.event_dt > csh.status_start_dt
                    where           csh.subscription_sub_type = 'DTV Primary Viewing'           -- DTV subscription
                                and csh.status_code_changed = 'Y'                               -- Status code has changed
                                and csh.status_code = 'AC'                                      -- Subscription process started
                                and csh.prev_status_code in ('PO','SC')                         -- Previously churned
                                and csh.status_start_dt < csh.status_end_dt
                                and csh.cb_key_household is not null
                                and csh.cb_key_household <> 0
                    group by        base.id
                 ) as tgt
where           base.id = tgt.id;
commit;


-- CONTINUOUS TENURE
update          SK4187_Events
set             continuous_tenure =     case    when datediff(month, coalesce(last_reinstate_dt,first_activation_dt),event_dt) between 0 and 6 then       'A) 0-6 Months'
                                                when datediff(month, coalesce(last_reinstate_dt,first_activation_dt),event_dt) between 7 and 12 then      'B) 7-12 Months'
                                                when datediff(month, coalesce(last_reinstate_dt,first_activation_dt),event_dt) between 13 and 24 then     'C) 1-2 Years'
                                                when datediff(month, coalesce(last_reinstate_dt,first_activation_dt),event_dt) between 24 and 60 then     'D) 2-5 Years'
                                                else                                                                                                                    'E) 5+ Years'
                                        end;
commit;

/* CHECK
select top 300 * from SK4187_Events;
select continuous_tenure, count(*) from SK4187_Events group by continuous_tenure;
*/


----------------------------------------------------------------------------------------------- PART G: OFFER SEEKING SEGMENTS

-- DERIVE THE UNIQUE TA EVENT FOR EACH ACCOUNT (BASED ON THE MOST RECENT TA ATTEMPT)
if object_id('SK4187_Most_Recent_Events') is not null then drop table SK4187_Most_Recent_Events end if;
select          *
                ,cast(null as varchar(15)) as offer_seeking_group8
                ,rank() over(partition by account_number order by event_dt desc) as rank  -- Identidy the most recent event for each period.
into            SK4187_Most_Recent_Events
from            SK4187_Events;

create hg index idx_acc         on SK4187_Most_Recent_Events(account_number);
create date index idx_evtdt     on SK4187_Most_Recent_Events(event_dt);
create hg index idx_acc_evtdt   on SK4187_Most_Recent_Events(account_number, event_dt);
grant select on SK4187_Most_Recent_Events to public;
commit;

-- DEDUPE RECORDS
delete from     SK4187_Most_Recent_Events
where           rank > 1;
commit;

-- APPEND OFFER SEEKING SEGMENTS
update          SK4187_Most_Recent_Events
set             offer_seeking_group8 =  case    when        continuous_tenure in ('D) 2-5 Years','E) 5+ Years')
                                                        -- and evt.any_prev_saved_attempts_vol > 0
                                                        and (on_offer  = 1 or last_offer_end_mth between 0 and 3)
                                                        and offer_vol >= 2                                                          then 'High'
                                                when        continuous_tenure in ('D) 2-5 Years','E) 5+ Years')
                                                        and (       (any_prev_saved_attempts_vol = 0 and offer_vol = 0)
                                                                or  (any_prev_saved_attempts_vol = 0 and last_offer_end_mth > 15)
                                                             )                                                                      then 'Low'
                                                else                                                                                     'Medium'
                                        end;
commit;


/* CHECK
select count(distinct account_number) from SK4187_Events;
select count(*) from SK4187_Most_Recent_Events;
select offer_seeking_group8, count(*) from SK4187_Most_Recent_Events group by offer_seeking_group8;
*/


-------------------------------------------------------------------------------------------------------------------------- END





--      5. Tenure - First year V's 4-5 year



---------------------------------------------------------------------------------------------------------------------------------------
/*
OUTPUT CHECK

Investigate reason why the 'efficiency' of packs for non-movies customers are all over 100%
-is it because they watch more TV?
-is it because they watch a larger breadth of channels?
*/
Select top 10 * from v066_output_2;
Select top 10 * from v066_output_dur;
Select top 10 * from #breadth2;

ALTER TABLE v066_output_2
ADD tot_view_min int;

UPDATE v066_output_2
SET tot_view_min = average_total_minutes_day
FROM v066_output_2 a INNER JOIN v066_output_dur b
        on a.account_number = b.account_number
;--31385422 Row(s) affected

select account_number
        ,COUNT(DISTINCT(channel_name_agg)) AS No_of_channels
into #breadth2
from v066_output_2
group by account_number
;--2597703 Row(s) affected
--128133 Row(s) affected
drop table #breadth2;

ALTER TABLE v066_output_2
ADD No_of_channels int;

UPDATE v066_output_2
SET a.No_of_channels = b.No_of_channels
FROM v066_output_2 a INNER JOIN #breadth2 b
        on a.account_number = b.account_number
;--

----------------------------------------------------------------------------------------------------------------------------------------------------------
/*
***********************************************************OUTPUT****************************************************************************************
*/
__________________________________________________________________________________________________________________________________________________________

-------------------------------------------------HH viewed by SPECIALS

select max(weight) as weight
        ,account_number
        ,EPG_title
        ,u_upgraders
into #special
from v066_output_2
where
epg_title like '%Downton Abbey%'
         OR epg_title like 'X Factor%'
group by account_number
        ,EPG_title
        ,u_upgraders
;
--5531 Row(s) affected

select weight
        ,account_number
        ,CASE WHEN EPG_title like 'X Factor%' then 'X Factor' ELSE EPG_title end as EPG_Title
        ,u_upgraders
into #spec
from #special
;--5531 Row(s) affected


select distinct (EPG_title)
from v066_output_2
order by EPG_title asc;
--
select top 10  * from v066_output_2
where epg_title like '%Downton Abbey%'

--drop table #special
select top 100 * from #spec
order by channel_name_agg desc

select sum(weight) as vol
      ,EPG_title
        ,u_upgraders
into #specials
from #spec
group by EPG_title
        ,u_upgraders
;--6

----------------------------------------------------HH viewed by media pack

select top 10 * from v066_output_2;

select  case when channel_category = 'SKY ENTERTAINMENT' then 'ENTERTAINMENT' ELSE channel_category end as channel_category
        ,account_number
        ,max(weight) as vol_weight
        ,u_cvs
        ,u_upgraders
        --,U_prem_mum
        --,max(dm_opt_in_all) as dm_opt_in_all
        --,max(dm_opt_in_any) as dm_opt_in_any
into #cat
from v066_output_2
--where --(kids is null AND music is null AND ent is null AND SKYent is null AND UKTV is null AND NEWS is null AND DOCUMENTARIES is null AND LIFESTYLE is null AND SPORTS is null)
--u_upgraders = 1
group by channel_category
        ,account_number
        ,u_cvs
        ,u_upgraders
        --,u_prem_mum
;--64915 Row(s) affected
drop table #cat
select top 100 * from #cat
order by account_number
        ,u_prem_mum
        ,channel_category

select  RTRIM(channel_category) as channel_category
        --,count(account_number) as vol_sample
        ,sum(vol_weight) as vol_weight
        ,u_cvs
        ,u_upgraders
        -- ,U_prem_mum
         --,dm_opt_in_all
       -- ,dm_opt_in_any
from #cat
group by
        channel_category
        ,u_cvs
       --,dm_opt_in_all
       --,dm_opt_in_any
        ,u_upgraders
     -- ,U_prem_mum
;

select
distinct(account_number)
,weight
,u_upgraders
--,U_prem_mum
,u_cvs
into #prem_M
from  v066_output_2
;

select
sum (weight) as weight
--,U_prem_mum
,u_upgraders
,u_cvs
from  #prem_M
group by
--U_prem_mum
u_cvs
,u_upgraders
;

select distinct(count(account_number)) from #prem -- 128181
order by account_number
;
drop table #prem_m;

select top 100  * from #prem_m;
from v066_output_2
where account_number ='200000853990'
;

-- select  a.service_key as service_key_ATL
--         ,b.service_key as service_key_SOCI
--         ,b.full_name
--         ,a.channel_category
--         ,pack_name
--         ,channel_name_agg
-- from v066_output_2 a LEFT JOIN #packs b
--         on a.service_key = b.service_key
-- group by a.channel_category
--         ,pack_name
--         ,channel_name_agg
--         ,a.service_key
--         ,b.service_key
--         ,full_name
-- ;
--Grant select on v066_output_2 to public;



------------------------------------------------------------------------------------------------------CHECK
--upgraders viewing behaviour to account for over 100% efficiency throughout pack range

select  RTRIM(channel_category) as channel_category
        ,account_number
        ,max(weight) as vol
        ,u_upgraders
        ,max(No_of_channels) as No_of_channels
        ,max(tot_view_min) as tot_view_min
into #cat
from v066_output_2
group by channel_category
        ,account_number
        ,u_upgraders
;--1266799 Row(s) affected
drop table #cat

select   channel_category
        ,account_number
        ,(vol*tot_view_min) as tot_view_min_SUM
        ,u_upgraders
        ,(vol*No_of_channels) as No_of_channels_SUM
        , vol
into #dur
from #cat
;--1267277 Row(s) affected

select channel_category
        ,sum(vol) as vol_SUM
        ,sum(tot_view_min_SUM) as tot_view_min_SUM
        ,sum(No_of_channels_SUM) as No_of_channels_SUM
        ,u_upgraders
from #dur
group by channel_category
        ,U_UPGRADERS
;
---------------------------------------------------------------------------------------------------------
/* WATERFALL
Analysis in Excel selected 'KIDS' as top pack for spots.

Re-reun process with accounts hit by 'KIDS' removed, to assess next best pack to maximise HH coverage
*/
SELECT TOP 10 * FROM V066_OUTPUT_2_M

SELECT distinct(account_number)
into #waterfall
from v066_output_2_M
where channel_category LIKE 'UKTV%'
;
--34456 Row(s) affected
select top 100 * from #ENT
drop  table #WATERFALL

ALTER TABLE v066_output_2_m
ADD UKTV tinyint;

UPDATE v066_output_2_M
SET UKTV = 1
FROM #WATERFALL a INNER JOIN v066_output_2_M b
        ON a.account_number = b.account_number
;--11374891 Row(s) affected

select count(distinct(account_number))
from v066_output_2
where kids = 1;--34456

--select top 100 * from v066_output_2
-------------------------------------------------HH viewed by channel
select max(weight) as weight
        ,account_number
        ,channel_name_agg
        ,channel_category
        --,u_upgraders
        --,max(service_key) as service_key
        --,day
        --,x_viewing_time_of_day
        --,genre_description
        --,sub_genre_description
into #channel
from v066_output_2_M
group by account_number
        ,channel_name_agg
        ,channel_category
        --,u_upgraders
        --,day
        --,x_viewing_time_of_day
        --,genre_description
        --,sub_genre_description
;
--9992187 Row(s) affected

--drop table #channel2
select top 100 * from #channel
order by channel_name_agg desc

select weight
        ,account_number
        ,channel_name_agg
        ,u_upgraders
        ,(genre_description+' '+sub_genre_description) as genre
into #channel2
from #channel
;
--9992187 Row(s) affected



select sum(weight) as vol
        ,RTRIM(channel_name_agg) as  channel_name_agg
        ,RTRIM(channel_category) as  channel_category
        --,u_upgraders
        --,max(service_key) as service_key
       -- ,genre
        --,genre_description
        --,sub_genre_description
--into
--output_channel_day
--output_channel_genre
from #channel
group by channel_name_agg
,channel_category
        --,u_upgraders
        --,genre
        --,genre_description
        --,sub_genre_description

;--37525 Row(s) affected



ALTER TABLE output_channel_M
ADD channel_category varchar(20);

Update output_channel_M
SET channel_category = b.channel_category
        from output_channel_M a inner join lkuppack b
                on a.service_key = b.service_key;
--637 Row(s) affected

drop table output_channel_day;
select  * from output_channel_genre;
order by channel_name_agg
;
select count(*) from #channel;

select * from v066_output_2 where  channel_name_agg is null and channel_category is not null;
select top 10 * from lkuppack;


-------------------------------------------------HH viewed by top programmes per pack

select RTRIM(channel_category) as channel_category
        ,account_number
        ,max(weight) as vol
       -- ,u_upgraders
        ,CASE WHEN EPG_title like 'X Factor%' then 'X Factor' ELSE EPG_title end as EPG_Title
        ,RTRIM(channel_name_agg) as channel_name_agg
INTO #programmes
FROM v066_output_2_M
group by channel_category
        ,account_number
        --,u_upgraders
        ,EPG_title
        ,channel_name_agg
;--12659404 Row(s) affected


drop table #programme;

select channel_category
        ,sum(vol) as vol
        ,EPG_Title
        --,u_upgraders
--INTO #programme
FROM #programmes
group by channel_category
        ,EPG_title
       -- ,u_upgraders
;--19798 Row(s) affected

select * from  #programme
order by channel_category asc
        ,EPG_title asc
;

--------CHECK

select sum(vol) as vol
        ,epg_title
        --,channel_category
from #programmes
where u_upgraders = 1
group by epg_title
        --,channel_category
order by epg_title
;

-------------------------------------------------HH viewed by pack with DM opt-in cut

select RTRIM(channel_category) as channel_category
        ,account_number
        ,max(weight) as vol
        ,u_upgraders
        ,dm_opt_in_any
INTO #dm
FROM v066_output_2
group by channel_category
        ,account_number
        ,u_upgraders
        ,dm_opt_in_any
;--1267277 Row(s) affected


drop table #dm;

select channel_category
        ,sum(vol) as vol
        ,dm_opt_in_any
FROM #dm
where u_upgraders = 1  --get those who are likely to upgrade
group by channel_category
        ,dm_opt_in_any
;--

select top 10 * from v066_output_2_M;
