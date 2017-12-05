/*

                Id:             V066
                Name:           ATL xmas movies
                Lead:           Sarah Moore
                Date:           19/06/2012
                Analyst:        Susanne Chan

                QA date:
                QA analyst:

                Notes:



-----------------------------------------------------------------------------------------------------------------------------------------------------

-- A. CREATE A SINGLE LIST OF ACCOUNTS FROM THE 3 UNIVERSES OF INTEREST:
        1. Upgrade potential in Nov 2011
        2. Previous upgraders during Nov/Dec 2011
        3. Package stability (universe 2 upgraders on movies package 3 months later)
        4. Sky base (Active @22/11/11)

*/

-------------------------------------------------------------Universe 1 Upgrade potential

create variable @target_dt date;

set @target_dt = '2011-11-22';


SELECT  account_number
        ,Decile AS Ctrl_Decile
        ,score AS Ctrl_Score
INTO    #ATLmovies
FROM    models.model_scores
WHERE   model_run_date = @target_dt AND model_name = 'Movies Control'
;



-- QA CHECKS--
--SELECT TOP 10 * from #ATLmovies
--select distinct ctrl_decile from #ATLmovies


--5855994 Row(s) affected


select account_number
        ,Decile AS Ctrl_Decile
        ,score AS Ctrl_Score
       INTO #HD
from models.model_scores
where model_name = 'HD Control' AND model_run_date = '2011-11-23'
--5300824 Row(s) affected

select top 100 * from #hd;
-------------------------------------------------------------Universe 2 Previous xmas period upgraders

SELECT  csh.Account_number
         ,csh.effective_from_dt as Upgrade_date
         ,csh.current_short_description
         ,ncel.prem_movies as current_premiums
         ,ocel.prem_movies as old_premiums
         ,RANK() OVER (PARTITION BY csh.account_number ORDER BY effective_from_dt ASC, csh.cb_row_id DESC) AS 'RANK' -- takes first date for household

INTO    #starting
    FROM sk_prod.cust_subs_hist as csh
         inner join sk_prod.cust_entitlement_lookup as ncel
                    on csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup as ocel
                    on csh.previous_short_description = ocel.short_description
  WHERE csh.effective_from_dt BETWEEN '20111101' and '20111231' --xmas period
    AND csh.effective_to_dt > csh.effective_from_dt
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code in ('AC','PC','AB')   -- Active records
    AND current_premiums > old_premiums   -- Increase in premiums
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;
--386297 Row(s) affected

select * into #upgrades from #starting
where rank = 1
;
--384081 Row(s) affected


------------------------------------------------------Upgraders with offers starting at xmas period
select  cpo.account_number
        ,cpo.initial_effective_dt
        ,cpo.offer_start_dt
        ,cpo.offer_end_dt
        ,cpo.offer_dim_offer_title_description
into    #Offers    -- drop table #Offers
from    sk_prod.cust_product_offers cpo
                inner join #upgrades offf
                on      cpo.account_number = offf.account_number

where   offer_start_dt BETWEEN '20111101' and '20111231'
        AND cpo.offer_dim_offer_title_description LIKE '%Movies%'
;
COMMIT;

select top 100 * from sk_prod.cust_product_offers
select top 100 * from #Offers
-------------------------------------------------------------Universe 3 Package stability
--Make a working table
-- CREATE TABLE  #prem_movement (
--          account_number         varchar(20)     not null
--         ,premiums_at_start      integer         null
--         ,premiums_at_end        integer         null
--         ,next_movement_date     date            null
--         ,next_movement_premiums integer         null
--         );
-- drop table #prem_movement


--Populate with Xmas period upgraders (universe 2)

IF object_id('v066_stability') IS NOT NULL DROP TABLE v066_stability;

select  account_number
        ,current_premiums as premiums_at_start
INTO    v066_stability
from #upgrades
;
--384081 Row(s) affected


--   FROM  sk_prod.cust_subs_hist AS csh
--         inner join sk_prod.cust_entitlement_lookup AS cel
--                    ON csh.current_short_description = cel.short_description
--  WHERE  subscription_sub_type = 'DTV Primary Viewing'
--    AND  csh.status_code IN ('AC','AB','PC')
--    AND  effective_from_dt BETWEEN '20111101' and '20111231' AND effective_to_dt > '2011-11-01'
--    AND account_number IN (SELECT account_number
--                             FROM #upgrades
--                          )
;--718053 Row(s) affected


-- INSERT INTO #prem_movement  (account_number,premiums_at_start)
-- SELECT account_number,premiums
--   FROM #start_accounts
--  WHERE rank = 1;
--384081 Row(s) affected


CREATE UNIQUE hg INDEX idx1 ON v066_stability(account_number); --Unique to make sure no dupes!

--Update with info at end of period



Alter table v066_stability
ADD premiums_at_end INT;

UPDATE v066_stability
   SET premiums_at_end = tgt.premiums
  FROM v066_stability AS pm
       inner join (
                        SELECT   csh.account_number
                                ,cel.prem_movies AS premiums
                          FROM  sk_prod.cust_subs_hist AS csh
                                inner join sk_prod.cust_entitlement_lookup AS cel
                                           ON csh.current_short_description = cel.short_description
                         WHERE  subscription_sub_type = 'DTV Primary Viewing'
                           AND  csh.status_code IN ('AC','AB','PC')
                           AND  effective_from_dt <= '2012-03-31'  AND effective_to_dt > '2012-03-31'
                           AND  account_number IN (SELECT account_number FROM #upgrades)
       )as tgt ON pm.account_number = tgt.account_number;
--373827 Row(s) affected




-----RESULTS -------
--Nov vs Mar

Alter table v066_stability
add Stable_Flag tinyint;

Update v066_stability
set Stable_Flag = (case when premiums_at_start > premiums_at_end AND premiums_at_end = 0 THEN 0
                        when premiums_at_end is null                                      THEN 0
                 ELSE 1 END)
FROM v066_stability
;
--384081 Row(s) affected

/*
select count(*),stable_flag from v066_stability
group by stable_flag;

select top 100 * from v066_stability

SELECT CASE WHEN (premiums_at_start > premiums_at_end AND premiums_at_end = 0)    THEN 'DOWNGRADE to 0'
            ELSE                                                                'STABLE'
        END AS movement
        ,stable_flag
       ,count(*) AS total
  FROM v066_stability
group by movement;



movement        total
STABLE          256511
DOWNGRADE to 0  127570
*/


-------------------------------------------------------------Universe 4. Sky Base

/*
All active customers as at 22 Nov 2011
*/
SELECT   account_number
INTO #Sky
FROM sk_prod.cust_subs_hist
WHERE subscription_sub_type = 'DTV Primary Viewing'
      and status_code IN ('AC','AB','PC')
      and (effective_from_dt  <= @target_dt and effective_to_dt > @target_dt)
;
--10104989 Row(s) affected



-------------------------------------------------------------B. CREATE VIEWING TABLE

/*
-- B. CREATE THE UNIVERSES WITHIN THE VESPA DATA SET
        1. CREATE A TABLE OF ACCOUNTS FROM THE UNIVERSES ON THE VESPA PANEL
        2. GET VIEWING DATA
        3. CAP VIEWING DATA
        4. SCALE VIEWING DATA

*/


-------------------------------------------------------------B.1 CREATE TABLE UNIQUE ACCOUNT NUMBERS FROM ALL UNIVERSES
--Use this list of accounts against Daily tables to get relevant accounts

IF object_id('v066_base_accounts') IS NOT NULL DROP TABLE v066_base_accounts;

SELECT * INTO v066_base_accounts FROM
(
select account_number FROM #ATLmovies atl -- upgrade potential
UNION
select account_number FROM #upgrades -- these are all customers that upgraded - dont need universe 3(its a subset of this)
UNION
select account_number FROM #Sky
) as account_number
;

--10130940 Row(s) affected

--- QA CHECKS--
--select top 10 * from v066_base_accounts
--select count(*) from v066_base_accounts


-------------------------------------------------------------B.2. GET VIEWING DATA
-- variable creation - run once only

CREATE VARIABLE @var_prog_period_start datetime;
CREATE VARIABLE @var_date_counter      datetime;
CREATE VARIABLE @var_prog_period_end   datetime;
CREATE VARIABLE @dt                    char(8);
CREATE VARIABLE @var_sql               varchar(15000);

SET @var_prog_period_start  = '2011-11-28';
SET @var_prog_period_end    = '2011-12-04';

IF object_id('V066_viewing_records') IS NOT NULL DROP TABLE V066_viewing_records;

-- To store all the viewing records:
create table V066_viewing_records(
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
    insert into V066_viewing_records
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
          inner join v066_base_accounts                 as acc on vw.account_number = acc.account_number
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
  end;  --7799360 Row(s) affected
  --22m


update V066_viewing_records
      set live = case when play_back_speed is null then 1 else 0 end
;
--41704032 Row(s) affected

  update V066_viewing_records
     set hd_channel = 1
   where channel_name like '%HD%'
;--7084771 Row(s) affected



-- QA CHECK ---
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
   where ntile_100 = 91   --all (live and playback) capped at 90%
group by event_start_day
        ,event_start_hour
        ,live
; --1m
--336 Row(s) affected

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
; --6m

-- update the viewing start and end times for playback records
  update V066_viewing_records
     set x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
   where recorded_time_utc is not null
; --3m
--11634479 Row(s) affected
commit;

  update V066_viewing_records
     set x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
   where recorded_time_utc is not null
; --2m --11634479 Row(s) affected
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
;  --41704032 Row(s) affected
commit;


-- calculate capped_x_programme_viewed_duration
  update V066_viewing_records
     set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
; --1m
--41704032 Row(s) affected


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
--41704032 Row(s) affected

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
; --0m  --2330719 Row(s) affected

  update V066_viewing_records
     set capped_flag =
        case
            when capped_x_viewing_start_time is null then 2
            when capped_x_viewing_end_time < x_viewing_end_time then 1
            else 0
        end
; --1m  --41704032 Row(s) affected

commit;

--QA:
/*
select capped_flag,count(1) from v066_viewing_records group by capped_flag;

capped_flag     count(1)
0       33069422
2       6381492
1       2253118
*/

delete from V066_viewing_records where capped_flag in (2,3); --3m  --6381492 Row(s) affected



---Add in Event start and end time and add in local time activity---
-- update V066_viewing_records
--    set viewing_record_start_time_utc = case when recorded_time_utc         <  tx_start_datetime_utc then tx_start_datetime_utc
--                                             when recorded_time_utc         >= tx_start_datetime_utc then recorded_time_utc
--                                             when adjusted_event_start_time <  tx_start_datetime_utc then tx_start_datetime_utc
--                                             when adjusted_event_start_time >= tx_start_datetime_utc then adjusted_event_start_time else null end
--       ,viewing_record_end_time_utc   = dateadd(second, capped_x_programme_viewed_duration, viewing_record_start_time_utc)
-- ;  --0m  --35322540 Row(s) affected
--
-- 
-- -- british summer time end? -- ended 28th October?? -- dont need this (this is when the program aired)
-- update V066_viewing_records
--    set viewing_record_start_time_local = case when dateformat(viewing_record_start_time_utc, 'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-02'
--                                                 or dateformat(viewing_record_start_time_utc, 'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-02'
--                                                 or dateformat(viewing_record_start_time_utc, 'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-02' then dateadd(hh,1,viewing_record_start_time_utc)
--                                               else viewing_record_start_time_utc  end
--       ,viewing_record_end_time_local   = case when dateformat(viewing_record_end_time_utc,   'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-02'
--                                                 or dateformat(viewing_record_end_time_utc,   'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-02'
--                                                 or dateformat(viewing_record_end_time_utc,   'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-02' then dateadd(hh,1,viewing_record_end_time_utc)
--                                               else viewing_record_end_time_utc  end
-- ; --1m
-- --35322540 Row(s) affected
-- 
-- select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20111101


----QA CHECK--
-- something is wrong with the above:

-- select distinct(cast(viewing_record_start_time_utc as date)) as viewing_record_start_time_utc
-- from V066_viewing_records
-- order by viewing_record_start_time_utc
-- DATES ARE STARTING IN MAY!

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
; --0m --35322540 Row(s) affected

--- check it:

select distinct(weighting_date) from V066_viewing_records order by weighting_date
-- changed to adjusted_event_start_time -- working now


-- First, get the segmentation for the account at the time of viewing
  update V066_viewing_records as bas
     set bas.scaling_segment_ID = wei.scaling_segment_ID
    from vespa_analysts.scaling_dialback_intervals as wei
   where bas.account_number = wei.account_number
     and bas.weighting_date between wei.reporting_starts and wei.reporting_ends
; --1m
--33776452 Row(s) affected

commit;



-- Find out the weight for that segment on that day
update V066_viewing_records
     set weightings = wei.weighting
    from V066_viewing_records as bas INNER JOIN vespa_analysts.scaling_weightings as wei
                                        ON bas.weighting_date = wei.scaling_day
                                        and bas.scaling_segment_ID = wei.scaling_segment_ID
;--33776452 Row(s) affected
commit;



--delete from V066_viewing_records where weightings = 0;  --15,46,088 Row(s) affected
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


drop table #temp

select  account_number
        ,weighting_date
       ,max(weightings) as weighting
into #temp
from V066_viewing_records
group by account_number, weighting_date


drop table normaised_weights

select distinct account_number
        , avg(weighting) as average_weighting
into normaised_weights
from #temp
group by account_number

select top 10 * from normaised_weights


-- now we need to normalise the weightings so they tie back to our original universe:

 select sum(average_weighting) from normaised_weights -- 7 million customers -- this is higher than the base we started with!
--11,737,834

-- the base that we started with:
<<<<<<< HEAD
select count(*) from v066_base_accounts

=======
select count(*) from v066_base_accounts -- 10,130,940
>>>>>>> de8650524dd9311d207ca625ba67716df557c9b0

-- lets normalise it:
create variable
@normal_factor float;

set @normal_factor = (select 10130940/sum(average_weighting) from normaised_weights)

Select @normal_factor -- 0.96 (discounting factor)


-- Step 3: calculate the normalised wieght and add this to a table
alter table normaised_weights
add normalised_weighting as float

update normaised_weights
set normalised_weighting = average_weighting*@normal_factor

select sum(normalised_weighting) from normaised_weights -- thats better!


-- alter table normaised_weights
-- drop normalised_weighting

-- now add this to the viewing table (it is the average weight)


alter table V066_viewing_records
add(normalised_weight float);

update V066_viewing_records
set normalised_weight = normalised_weighting
from V066_viewing_records as viw
left join normaised_weights nwe
on viw.account_number = nwe.account_number

-- it worked - use normalised weight going forward
select top 10 * from V066_viewing_records







----------------------------------------------------------------------
-- LETS GET THE VIEWING DETAILS SUMMARISED FOR EACH CUSTOMER
---------------------------------------------------------------------



-- we need to determine if the channel is pay or free:

alter table  V066_viewing_records
add(pay_free as varchar(10));

update V066_viewing_records
set pay_free = pay_free_indicator
from V066_viewing_records vw
left join sk_prod.VESPA_EPG_DIM epg
on vw.programme_trans_sk = epg.programme_trans_sk

select distinct(pay_free), count(*) from V066_viewing_records group by pay_free
-- there are always a small number of nulls but that the best that we have:

select account_number
        ,avg(weightings) as weight
into #pop
from v066_viewing_records
group by account_number
;
drop table #pop
select sum (weight) from #pop;
--10,545,913

-- ok lets find out how many minutes were watched on average per day (some people only returned data for 1 day so use average)
-- divide by 60 to get minutes

if object_id('v066_allinone') is not null drop table v066_allinone


select account_number
       ,count(distinct(weighting_date)) as days_data_return
       ,((sum(case when pay_free = 'PAY TV' then capped_x_programme_viewed_duration else null end)/60)/days_data_return) as average_pay_minutes
       ,((sum(case when pay_free = 'FREE TV' or pay_free = 'RADIO' then capped_x_programme_viewed_duration else null end)/60)/days_data_return) as average_free_minutes
       ,average_Total_minutes_day = average_pay_minutes + average_free_minutes
       ,((sum(case when live = 0 then capped_x_programme_viewed_duration else null end)/60)/days_data_return) as average_PVR_minutes
       ,((sum(case when GENRE_DESCRIPTION = 'Movies' and pay_free = 'FREE TV' then capped_x_programme_viewed_duration else null end)/60)/days_data_return)
        as average_FTA_movie_minutes

       , sov_pay_tv = cast(average_pay_minutes as float)/nullif(cast(average_Total_minutes_day as float), 0)
       , sov_pvr = cast(average_PVR_minutes as float)/nullif(cast(average_Total_minutes_day as float),0)
       , sov_FTA_movies = cast(average_FTA_movie_minutes as float)/nullif(cast(average_Total_minutes_day as float),0)
into v066_allinone
from V066_viewing_records
group by account_number

-- CHECK IT:
--select top 10 * from v066_allinone -- great!
--select top 10 * from v066_viewing_records;


-- LETS SET ALL OF THE NULLS TO ZERO
Update v066_allinone
set  average_pay_minutes = case when average_pay_minutes is null then 0 else average_pay_minutes end
    ,average_free_minutes = case when average_free_minutes is null then 0 else average_free_minutes end
    ,average_Total_minutes_day = case when average_Total_minutes_day is null then 0 else average_Total_minutes_day end
    ,average_PVR_minutes = case when average_PVR_minutes is null then 0 else average_PVR_minutes end
    ,average_FTA_movie_minutes = case when average_FTA_movie_minutes is null then 0 else average_FTA_movie_minutes end
    ,sov_pay_tv = case when sov_pay_tv is null then 0 else sov_pay_tv end
    ,sov_pvr = case when sov_pvr is null then 0 else sov_pvr end
    ,sov_FTA_movies = case when sov_FTA_movies is null then 0 else sov_FTA_movies end


 -- select top 10 * from v066_allinone


-- consider this the master analysis table
-- we need scaling numbers in this table:



-- LETS SET ALL OF THE NULLS TO ZERO

Update v066_allinone
set  average_pay_minutes = case when average_pay_minutes is null then 0 else average_pay_minutes end
    ,average_free_minutes = case when average_free_minutes is null then 0 else average_free_minutes end
    ,average_Total_minutes_day = case when average_Total_minutes_day is null then 0 else average_Total_minutes_day end
    ,average_PVR_minutes = case when average_PVR_minutes is null then 0 else average_PVR_minutes end
    ,average_FTA_movie_minutes = case when average_FTA_movie_minutes is null then 0 else average_FTA_movie_minutes end
    ,sov_pay_tv = case when sov_pay_tv is null then 0 else sov_pay_tv end
    ,sov_pvr = case when sov_pvr is null then 0 else sov_pvr end
    ,sov_FTA_movies = case when sov_FTA_movies is null then 0 else sov_FTA_movies end



alter table v066_allinone
add(normalised_weight float);


update v066_allinone
set normalised_weight = normalised_weighting
from v066_allinone as viw
left join normaised_weights nwe
on viw.account_number = nwe.account_number


-- Check it: --
--select top 10 * from v066_allinone -- looks fine
--select count(*) from v066_allinone where normalised_weight is null -- none, good
--select count(*) from v066_allinone where normalised_weight = 0 -- none, good

-- 30 accounts with no scoring - lets delete these as they are probably non UK

delete from v066_allinone where normalised_weight = 0


----------------------------------------------------------------------
-- PART 2: LETS GET THE CUMULITIVE WEIGHTING TO ALLOCATE QUINTILES TO THE CUSTOMERS BASED ON THIER POPULATION REPRESENTATION
---------------------------------------------------------------------

--select top 10 * from v066_allinone

-- we only need to quintile 4things: total minutes, SOV pay, SOV PVR, SOV FTA Movies. Day of week and genre can be done in a different table (many rows)


-- drop table #temp22
select  account_number
        ,average_Total_minutes_day -- all measures are average per day to allow comparison
        ,sov_pay_tv
        ,sov_pvr
        ,sov_FTA_movies
        ,normalised_weight

        , sum(normalised_weight) over ( order by average_Total_minutes_day -- partition is not needed
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_viewing_cumul_weighting

        , sum(normalised_weight) over ( order by sov_pay_tv
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as pay_viewing_cumul_weighting

         ,sum(normalised_weight) over ( order by sov_pvr
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as PVR_viewing_cumul_weighting


         ,sum(normalised_weight) over ( order by sov_FTA_movies
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as FTA_movies_cumul_weighting

into #temp22
from v066_allinone

--- the figures above are arranged such that lower viewing is first and higher viewing last - will transalte to higher cumul weightings for more viewing

--select top 100 * from #temp22 -- looks right





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
  (select sum(normalised_weight) from #temp22)/5)


--check it:
--select top 10 * from quintile_weights -- everything looks fine - we will use < to allocate customers to a quintile.




--------------------------------------------------------------------------------
-- PART 4 - now allocate each account to the relevant quintile based on the cumulitave weighting
--------------------------------------------------------------------------------



IF object_id('customer_quintile') IS NOT NULL DROP TABLE customer_quintile;

-- we need to copy the temp table into a real table so we can add columns etc
select * into customer_quintile from #temp22

-- we need to create 4 quintile allocations
alter table customer_quintile
add (

     total_viewing_quintile integer
    ,pay_SOV_quintile integer
    ,PVR_SOV_quintile integer
    ,FTA_movies_SOV_quintile integer);

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


-- update the pvr_viewing quitile
update customer_quintile
        set PVR_SOV_quintile = centile
from customer_quintile as vdw
 inner join quintile_weights as cww
 on PVR_viewing_cumul_weighting <= sample


-- update the free to air movie viewing quitile:
update customer_quintile
        set FTA_movies_SOV_quintile = centile
from customer_quintile as vdw
 inner join quintile_weights as cww
 on FTA_movies_cumul_weighting <= sample

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




--------------------------------------------------------------------------------
-- PART 5 - put each customers QUINTILE allocation into master table v066_allinone (profiling details can also be joined here for one big happy pivot)
--------------------------------------------------------------------------------

....

alter table v066_allinone
add (  total_viewing_quintile integer
    ,pay_SOV_quintile integer
    ,PVR_SOV_quintile integer
    ,FTA_movies_SOV_quintile integer);



-- total viewing quitile
update v066_allinone
set al1.total_viewing_quintile  = csq.total_viewing_quintile
    ,al1.pay_SOV_quintile       = csq.pay_SOV_quintile
    ,al1.PVR_SOV_quintile       = csq.PVR_SOV_quintile
    ,al1.FTA_movies_SOV_quintile = csq.FTA_movies_SOV_quintile
from  v066_allinone as al1
 left join customer_quintile as csq
 on csq.account_number = al1.account_number


 -- there are a bunch of columns in this table that we dont need anymore...
--select top 10 * from v066_allinone






-- this is no longer needed
drop table customer_quintile




----------------------------------------------------------------------------

------------------------------------------------------------------------------------------------
--C. Add profiling variables
------------------------------------------------------------------------------------------------
/*
Get keys into v066_allinone table for getting the profiling variables

*/

alter table v066_allinone
add(    cb_key_household        bigint default null
        ,cb_key_individual      bigint default null
        ,cb_key_family          bigint default null
);


update v066_allinone
set     in1.cb_key_household = vie.cb_key_household
        ,in1.cb_key_individual = vie.cb_key_individual
        ,in1.cb_key_family = vie.cb_key_family
from v066_allinone as in1 INNER JOIN v066_viewing_records vie
on in1.account_number = vie.account_number
;


-------------------------------------------------------------C.1 Demographic metrics



----------------------------------------------------------------C.1.1 TV ISBA Region


alter table v066_allinone
add( Region varchar(25));


update          v066_allinone base
set             Region = CASE WHEN sav.isba_tv_region = 'Not Defined'
                                       THEN 'UNKNOWN'
                                       ELSE sav.isba_tv_region
                                   END
FROM            v066_allinone base INNER JOIN sk_prod.cust_single_account_view AS sav
                        ON base.account_number = sav.account_number;


--select top 10 * from v066_allinone



----------------------------------------------------------------C.1.2 HH composition

select      distinct base.account_number
            ,max(CASE WHEN ilu_hhcomposition = 'A1'                     THEN 'Female Single Parent'
                     WHEN ilu_hhcomposition = 'A2'                      THEN 'Female single pensioner'
                     WHEN ilu_hhcomposition = 'A3'                      THEN 'Female single other'
                     WHEN ilu_hhcomposition = 'B2'                      THEN 'Male single pensioner'
                     WHEN ilu_hhcomposition = 'B3'                      THEN 'Male single non-pensioner'
                     WHEN ilu_hhcomposition = 'C1'                      THEN 'Married couple with dependent children'
                     WHEN ilu_hhcomposition = 'C2'                      THEN 'Married couple pensioners'
                     WHEN ilu_hhcomposition = 'C3'                      THEN 'Married couple other'
                     WHEN ilu_hhcomposition = 'D1'                      THEN 'Co-habiting couple with dependent children'
                     WHEN ilu_hhcomposition = 'D3'                      THEN 'Co-habiting couple - no dependents'
                     WHEN ilu_hhcomposition = 'E1'                      THEN 'Two person household with dependent children'
                     WHEN ilu_hhcomposition = 'E2'                      THEN 'Two person pensioner household'
                     WHEN ilu_hhcomposition = 'E3'                      THEN 'Two person other'
                     WHEN ilu_hhcomposition = 'F1'                      THEN 'Married family - grown up and dependent children at home'
                     WHEN ilu_hhcomposition = 'F2'                      THEN 'Married family - pensioners and grown up children at home'
                     WHEN ilu_hhcomposition = 'F3'                      THEN 'Married family - grown up children at home'
                     WHEN ilu_hhcomposition = 'G3'                      THEN 'Non married family - grown up children at home'
                     WHEN ilu_hhcomposition = 'H1'                      THEN 'Married couple and 1 other adult - dependent children'
                     WHEN ilu_hhcomposition = 'H2'                      THEN 'Married couple and 1 other adult - Pensioners'
                     WHEN ilu_hhcomposition = 'H3'                      THEN 'Married couple and 1 other adult - Other'
                     WHEN ilu_hhcomposition = 'I1'                      THEN 'Married couple and 2+ other adults - dependent children'
                     WHEN ilu_hhcomposition = 'I2'                      THEN 'Married couple and 2+ other adults - Pensioners'
                     WHEN ilu_hhcomposition = 'I3'                      THEN 'Married couple and 2+ other adults - Other'
                     WHEN ilu_hhcomposition = 'J1'                      THEN 'Co-habiting couple and 1+ other adults - dependent children'
                     WHEN ilu_hhcomposition = 'J3'                      THEN 'Co-habiting couple and 1+ other adults - no dependents'
                     WHEN ilu_hhcomposition = 'K1'                      THEN 'Mixed household of 3+ adults - dependent children'
                     WHEN ilu_hhcomposition = 'K3'                      THEN 'Mixed household of 3+ adults - no dependents'
                ELSE                                                         'Unknown'
                END) as hhcomposition
                ,max(ilu.cb_key_household) as cb_key_household
into            #ilu_hh                                                --drop table #ilu_hh
from            sk_prod.ilu as ilu INNER JOIN v066_viewing_records as base
                        ON ilu.cb_key_household = base.cb_key_household
group by        base.account_number
;
commit;
--1m
--117922 Row(s) affected



alter table v066_allinone
add( HHcomposition varchar(60));

update v066_allinone
set HHcomposition = ilu.hhcomposition
from v066_allinone in1
left join  #ilu_hh ilu
on in1.account_number = ilu.account_number
;



----------------------------------------------------------------C.1.3 Affluence


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
where           sav.account_number in (select distinct account_number from v066_viewing_records)
group by        sav.account_number;
commit;


alter table v066_allinone
add( Affluence varchar(10));

update v066_allinone
set affluence = sav_affluence
from v066_allinone in1
left join  #sav_dem sav
on in1.account_number = sav.account_number

--select top 10 * from  v066_allinone


----------------------------------------------------------------C.1.4 Financial Stress


SELECT   CV.cb_key_individual
        ,max(CASE WHEN CV.person_financial_stress = '0' THEN 'Very low'
                WHEN CV.person_financial_stress = '1' THEN 'Low'
                WHEN CV.person_financial_stress = '2' THEN 'Medium'
                WHEN CV.person_financial_stress = '3' THEN 'High'
                WHEN CV.person_financial_stress = '4' THEN 'Very High'
                WHEN CV.person_financial_stress = 'U' THEN 'Unclassified'
            ELSE                                         'Unknown'
            END) as financial_stress
INTO #v066_stress
FROM sk_prod.EXPERIAN_CONSUMERVIEW cv
where cb_key_individual in (select distinct(cb_key_individual) from V066_viewing_records)
GROUP BY CV.cb_key_individual
;

select top 100 * from #v066_stress
--114246 Row(s) affected

alter table v066_allinone
add( financial_stress varchar(12));

update v066_allinone
set financial_stress = sts.financial_stress
from v066_allinone in1
left join  #v066_stress STS
on in1.cb_key_individual = sts.cb_key_individual
;

update v066_allinone
set financial_stress = case when financial_stress is null then 'Unknown' end


-- CHECKS -- NOT WORKING
--
-- SELECT TOP 10 * FROM #v066_stress
-- SELECT TOP 10 * FROM v066_allinone
-- SELECT financial_stress, COUNT(*) FROM v066_allinone GROUP BY financial_stress
-- ----------------------------------------------------------------C.1.5 Mosaic segments


IF OBJECT_ID ('nodupes') IS NOT NULL DROP TABLE nodupes


-- this is new
SELECT   CV.cb_key_household
        ,CV.h_mosaic_uk_2009_group
        ,CV.cb_change_date
        ,CV.h_mosaic_uk_2009_type
        ,rank() over(PARTITION BY cv.cb_key_household ORDER BY cb_row_id desc) AS rank_id
--       ,(RTRIM(Lifestage)||RTRIM(Family_Lifestage)||RTRIM(tenure)||RTRIM(h_mosaic_uk_2009_group)||RTRIM(h_mosaic_uk_2009_type)||RTRIM(h_fss_v3_group)) AS exp
INTO nodupes
FROM sk_prod.EXPERIAN_CONSUMERVIEW cv
where cb_key_household in (select distinct(cb_key_household) from V066_viewing_records)

delete from nodupes where rank_id > 1



--- we need to understand what the segments mean:

-- lets do mosaic type first the group later
alter table nodupes
add(mosaic_type_desc varchar(50));


update nodupes
set mosaic_type_desc =

case when h_mosaic_uk_2009_type =       '01'    then    'Global Power Brokers'
 when h_mosaic_uk_2009_type =   '02'    then    'Voices of Authority'
 when h_mosaic_uk_2009_type =   '03'    then    'Business Class'
 when h_mosaic_uk_2009_type =   '04'    then    'Serious Money'
 when h_mosaic_uk_2009_type =   '05'    then    'Mid-Career Climbers'
 when h_mosaic_uk_2009_type =   '06'    then    'Yesterdays Captains'
 when h_mosaic_uk_2009_type =   '07'    then    'Distinctive Success'
 when h_mosaic_uk_2009_type =   '08'    then    'Dormitory Villagers'
 when h_mosaic_uk_2009_type =   '09'    then    'Escape to the Country'
 when h_mosaic_uk_2009_type =   '10'    then    'Parish Guardians'
 when h_mosaic_uk_2009_type =   '11'    then    'Squires Among Locals'
 when h_mosaic_uk_2009_type =   '12'    then    'Country Loving Elders'
 when h_mosaic_uk_2009_type =   '13'    then    'Modern Agribusiness'
 when h_mosaic_uk_2009_type =   '14'    then    'Farming Today'
 when h_mosaic_uk_2009_type =   '15'    then    'Upland Struggle'
 when h_mosaic_uk_2009_type =   '16'    then    'Side Street Singles'
 when h_mosaic_uk_2009_type =   '17'    then    'Jacks of All Trades'
 when h_mosaic_uk_2009_type =   '18'    then    'Hardworking Families'
 when h_mosaic_uk_2009_type =   '19'    then    'Innate Conservatives'
 when h_mosaic_uk_2009_type =   '20'    then    'Golden Retirement'
 when h_mosaic_uk_2009_type =   '21'    then    'Bungalow Quietude'
 when h_mosaic_uk_2009_type =   '22'    then    'Beachcombers'
 when h_mosaic_uk_2009_type =   '23'    then    'Balcony Downsizers'
 when h_mosaic_uk_2009_type =   '24'    then    'Garden Suburbia'
 when h_mosaic_uk_2009_type =   '25'    then    'Production Managers'
 when h_mosaic_uk_2009_type =   '26'    then    'Mid-Market Families'
 when h_mosaic_uk_2009_type =   '27'    then    'Shop Floor Affluence'
 when h_mosaic_uk_2009_type =   '28'    then    'Asian Attainment'
 when h_mosaic_uk_2009_type =   '29'    then    'Footloose Managers'
 when h_mosaic_uk_2009_type =   '30'    then    'Soccer Dads and Mums'
 when h_mosaic_uk_2009_type =   '31'    then    'Domestic Comfort'
 when h_mosaic_uk_2009_type =   '32'    then    'Childcare Years'
 when h_mosaic_uk_2009_type =   '33'    then    'Military Dependants'
 when h_mosaic_uk_2009_type =   '34'    then    'Buy-to-Let Territory'
 when h_mosaic_uk_2009_type =   '35'    then    'Brownfield Pioneers'
 when h_mosaic_uk_2009_type =   '36'    then    'Foot on the Ladder'
 when h_mosaic_uk_2009_type =   '37'    then    'First to Move In'
 when h_mosaic_uk_2009_type =   '38'    then    'Settled Ex-Tenants'
 when h_mosaic_uk_2009_type =   '39'    then    'Choice Right to Buy'
 when h_mosaic_uk_2009_type =   '40'    then    'Legacy of Labour'
 when h_mosaic_uk_2009_type =   '41'    then    'Stressed Borrowers'
 when h_mosaic_uk_2009_type =   '42'    then    'Worn-Out Workers'
 when h_mosaic_uk_2009_type =   '43'    then    'Streetwise Kids'
 when h_mosaic_uk_2009_type =   '44'    then    'New Parents in Need'
 when h_mosaic_uk_2009_type =   '45'    then    'Small Block Singles'
 when h_mosaic_uk_2009_type =   '46'    then    'Tenement Living'
 when h_mosaic_uk_2009_type =   '47'    then    'Deprived View'
 when h_mosaic_uk_2009_type =   '48'    then    'Multicultural Towers'
 when h_mosaic_uk_2009_type =   '49'    then    'Re-Housed Migrants'
 when h_mosaic_uk_2009_type =   '50'    then    'Pensioners in Blocks'
 when h_mosaic_uk_2009_type =   '51'    then    'Sheltered Seniors'
 when h_mosaic_uk_2009_type =   '52'    then    'Meals on Wheels'
 when h_mosaic_uk_2009_type =   '53'    then    'Low Spending Elders'
 when h_mosaic_uk_2009_type =   '54'    then    'Clocking Off'
 when h_mosaic_uk_2009_type =   '55'    then    'Backyard Regeneration'
 when h_mosaic_uk_2009_type =   '56'    then    'Small Wage Owners'
 when h_mosaic_uk_2009_type =   '57'    then    'Back-to-Back Basics'
 when h_mosaic_uk_2009_type =   '58'    then    'Asian Identities'
 when h_mosaic_uk_2009_type =   '59'    then    'Low-Key Starters'
 when h_mosaic_uk_2009_type =   '60'    then    'Global Fusion'
 when h_mosaic_uk_2009_type =   '61'    then    'Convivial Homeowners'
 when h_mosaic_uk_2009_type =   '62'    then    'Crash Pad Professionals'
 when h_mosaic_uk_2009_type =   '63'    then    'Urban Cool'
 when h_mosaic_uk_2009_type =   '64'    then    'Bright Young Things'
 when h_mosaic_uk_2009_type =   '65'    then    'Anti-Materialists'
 when h_mosaic_uk_2009_type =   '66'    then    'University Fringe'
 when h_mosaic_uk_2009_type =   '67'    then    'Study Buddies'
 when h_mosaic_uk_2009_type =   '99'    then    'Unclassified'
 else   'Unclassified' end



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
 else 'Unclassified' end


-- now we need to match this back to our all in one table


alter table nodupes
add(account_number varchar(20));


update nodupes
set nod.account_number  = vre.account_number
from nodupes nod
left join V066_viewing_records vre
on nod.cb_key_household = vre.cb_key_household


-- --checks
-- select top 100 * from nodupes order by h_mosaic_uk_2009_type
-- select count(*) from nodupes where account_number is null -- good!
--
-- now put it in the all in one table

alter table v066_allinone
add(    mosaic_group_desc varchar(50)
        ,mosaic_type_desc varchar(20));


update v066_allinone
set    al1.mosaic_group_desc = nod.mosaic_group_desc
       ,al1.mosaic_type_desc = nod.mosaic_type_desc
from v066_allinone al1
left join nodupes nod
on nod.account_number = al1.account_number



-- lets check it:
--select top 100 * from v066_allinone


----------------------------------------------------------------C.1.6 Personicx segments

IF object_id('cust_ilu') IS NOT NULL DROP TABLE cust_ilu;


-- lets get the personicx segment code and match back to CB_key_household
select cb_key_household
       ,ils_hhpersonicx2
       ,rank() over(PARTITION BY cb_key_household ORDER BY cb_row_id desc) AS rank_id
into cust_ilu
from sk_prod.ilu
where cb_key_household in (select cb_key_household from nodupes) -- this is distinct so faster!


delete from cust_ilu where rank_id > 1


--check it:
select count(*) from cust_ilu -- 117k!
select top 100 * from cust_ilu order by cb_key_household

-- now we need to know what the codes mean:

-- create a lookup
SELECT 'HX201' AS code, 'HX201 - Student Life' as description INTO #psx_lookup
UNION SELECT 'HF502', 'HF502 - Full Time Single Mums'
UNION SELECT 'HX303', 'HX303 - Mid-Income Traders'
UNION SELECT 'HX204', 'HX204 - Affluent Adventurers'
UNION SELECT 'HF405', 'HF405 - Value Pack Renters'
UNION SELECT 'HX206', 'HX206 - Funky DINKYs'
UNION SELECT 'HX307', 'HX307 - Healthy and Herbal'
UNION SELECT 'HX208', 'HX208 - Active Professionals'
UNION SELECT 'HF309', 'HF309 - Family Dial-ups'
UNION SELECT 'HF310', 'HF310 - Working Parents'
UNION SELECT 'HX111', 'HX111 - Off-Piste Online'
UNION SELECT 'HF212', 'HF212 - Toddlers and Toy Shops'
UNION SELECT 'HF313', 'HF313 - Single Mums and Students'
UNION SELECT 'HX514', 'HX514 - Single Renters '
UNION SELECT 'HF115', 'HF115 - Professional Parents'
UNION SELECT 'HX316', 'HX316 - Guys No Dolls'
UNION SELECT 'HF517', 'HF517 - Cash Limited'
UNION SELECT 'HF418', 'HF418 - Big Families – Local Focus'
UNION SELECT 'HF319', 'HF319 - Nuclear Families'
UNION SELECT 'HF320', 'HF320 - Factory Worker Families'
UNION SELECT 'HX221', 'HX221 - Mobile Mid-Management'
UNION SELECT 'HF122', 'HF122 - Rich Returns'
UNION SELECT 'HF123', 'HF123 - Black Run Connoisseurs'
UNION SELECT 'HF224', 'HF224 - Borrowers and Spenders'
UNION SELECT 'HX225', 'HX225 - Free Liberals'
UNION SELECT 'HX326', 'HX326 - Nearly Empty Nesters'
UNION SELECT 'HX227', 'HX227 - Gregarious Grandparents'
UNION SELECT 'HX328', 'HX328 - Cosy Arts'
UNION SELECT 'HX129', 'HX129 - Portfolio Professionals'
UNION SELECT 'HX430', 'HX430 - Odds On Factory Workers'
UNION SELECT 'HX231', 'HX231 - Deluxe Investments'
UNION SELECT 'HX132', 'HX132 - Vintage Professionals'
UNION SELECT 'HX533', 'HX533 - Roll-ups and Rent'
UNION SELECT 'HX334', 'HX334 - Own Value'
UNION SELECT 'HX335', 'HX335 - Working Grandparents'
UNION SELECT 'HX336', 'HX336 - Settled Solos'
UNION SELECT 'HX237', 'HX237 - High Flying Solos'
UNION SELECT 'HR438', 'HR438 - Place Your Bets'
UNION SELECT 'HR339', 'HR339 - Just Retired'
UNION SELECT 'HR240', 'HR240 - Healthy Income'
UNION SELECT 'HR441', 'HR441 - Bingo and Buses'
UNION SELECT 'HR442', 'HR442 - Comfortable Couples'
UNION SELECT 'HR343', 'HR343 - Silver Reserves'
UNION SELECT 'HR544', 'HR544 - Bus Trips and Bungalows'
UNION SELECT 'HR245', 'HR245 - Mature Money'
UNION SELECT 'HR446', 'HR446 - Pastoral Pleasures'
UNION SELECT 'HR547', 'HR547 - Mobile Pensioners'
UNION SELECT 'HR448', 'HR448 - Local Shoppers'
UNION SELECT 'HR349', 'HR349 - Grey Volunteers'
UNION SELECT 'HR450', 'HR450 - Savers and Givers'
UNION SELECT 'HR551', 'HR551 - Elderly Pensioners'
UNION SELECT 'HR552', 'HR552 - Coaches and Crosswords'
;

select top 10 * from #psx_lookup

-- add the flag
alter table cust_ilu
add(personicx_desc varchar (35));

update cust_ilu
set personicx_desc = description
from cust_ilu
left join #psx_lookup
on ils_hhpersonicx2 = code



-- lets get rid of the nulls
update cust_ilu
set personicx_desc = case when personicx_desc is null then 'Unknown' else personicx_desc end

select personicx_desc, count(*) from cust_ilu group by personicx_desc -- there are none!



-- add account_number

alter table cust_ilu
add(account_number varchar (20));

update cust_ilu
set ilu.account_number = nod.account_number
from cust_ilu as ilu
left join nodupes nod
on ilu.cb_key_household = nod.cb_key_household



--
-- -- check it:
-- select top 100 * from cust_ilu
-- select count(*) from cust_ilu where account_number is null
--


-- now lets put this into the all in one table for analysis:

alter table v066_allinone
add(personicx_desc varchar(35));


update v066_allinone
set    al1.personicx_desc = nod.personicx_desc
from v066_allinone al1
left join cust_ilu nod
on nod.account_number = al1.account_number


-- lets check it:
-- select top 100 * from v066_allinone -- there seem to be blanks in this table
--
--
-- select personicx_desc, count(*) from v066_allinone group by personicx_desc -- a large number were blank, i dont know why?!

-- lets get rid of the nulls (data is'nt avaiable for all of our customers.
update v066_allinone
set personicx_desc = case when personicx_desc is null then 'Unknown' else personicx_desc end



---------------------------------------------------------------------------------C.2 Sky behavioural metrics

----------------------------------------------------------------C.2.1 Package



alter table     v066_allinone
add(            tv_package varchar(50) default 'Unknown',
                tv_premiums varchar (100) default 'Unknown');

SELECT          base.account_number,
                case when cel.prem_sports + cel.prem_movies = 4         then 'Top Tier'
                     when cel.prem_sports = 2 and cel.prem_movies = 1   then 'Dual Sports Single Movies'
                     when cel.prem_sports = 2 and cel.prem_movies = 0   then 'Dual Sports'
                     when cel.prem_sports = 1 and cel.prem_movies = 2   then 'Single Sports Dual Movies'
                     when cel.prem_sports = 0 and cel.prem_movies = 2   then 'Dual Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 1   then 'Single Sports Single Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 0   then 'Single Sports'
                     when cel.prem_sports = 0 and cel.prem_movies = 1   then 'Single Movies'
                     when cel.prem_sports + cel.prem_movies = 0         then 'Basic'
                else                                                    'Unknown'
                end as tv_premiums,
                case when (music = 0 AND news_events = 0 AND kids = 0 AND knowledge = 0)
                     then 'Entertainment'
                     when (music = 1 or news_events = 1 or kids = 1 or knowledge = 1)
                     then 'Entertainment Extra'
                     else 'Unknown' end as tv_package
into            #tvpackage
FROM            sk_prod.cust_subs_hist as csh
        inner join sk_prod.cust_entitlement_lookup as cel
                on csh.current_short_description = cel.short_description
        inner join v066_allinone base
                on csh.account_number = base.account_number
WHERE           csh.subscription_sub_type ='DTV Primary Viewing'
AND             csh.subscription_type = 'DTV PACKAGE'
AND             csh.status_code in ('AC','AB','PC')
AND             csh.effective_from_dt < @target_dt
AND             csh.effective_to_dt   >= @target_dt
AND             csh.effective_from_dt != csh.effective_to_dt;
commit;

create hg index indx1 on #tvpackage(account_number);

update          v066_allinone base
set             base.tv_package = tvp.tv_package,
                base.tv_premiums = tvp.tv_premiums
from            #tvpackage tvp
where           base.account_number = tvp.account_number
commit;




--QA
/*
select top 100 * from v066_allinone
where tv_premiums <> 'Basic' and tv_package = 'Entertainment';
*/

----------------------------------------------------------------C.2.2 Box AND C.2.3 Product




Alter table v066_allinone
ADD
        (
        HD  int
        ,MR  int
        ,SP  int
        ,BB  int
        ,Talk  int
        ,WLR  int
        ,DTV_only int
        )
;


UPDATE v066_allinone
   SET HD        = tgt.hdtv
      ,MR        = tgt.multiroom
      ,SP        = tgt.skyplus
      ,BB        = tgt.broadband
      ,talk      = tgt.skytalk
      ,WLR       = tgt.wlr
 FROM v066_allinone AS base
      INNER JOIN (
                    SELECT  csh.account_number
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS skyplus
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS multiroom
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END)  AS hdtv
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='Broadband DSL Line'
                                       AND (       status_code in ('AC','AB')
                                               OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                               OR (status_code='CF' AND prev_status_code='PC'                                  )
                                               OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                                            )                                    THEN 1 ELSE 0 END)  AS broadband
                           ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
                                       AND (     csh.status_code = 'A'
                                             OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                             OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                             OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                                            )                                  THEN 1 ELSE 0 END)   AS skytalk
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='SKY TALK LINE RENTAL'
                                       AND csh.status_code in ('A','CRQ','R')  THEN 1 ELSE 0 END) AS wlr
                      FROM sk_prod.cust_subs_hist AS csh
                           INNER JOIN v066_allinone AS base ON csh.account_number = base.account_number
                           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
                     WHERE csh.effective_from_dt <= @target_dt
                       AND csh.effective_to_dt    > @target_dt
                       AND csh.subscription_sub_type  IN ( 'DTV Primary Viewing'
                                                          ,'DTV Sky+'
                                                          ,'DTV Extra Subscription'
                                                          ,'DTV HD'
                                                          ,'Broadband DSL Line'
                                                          ,'SKY TALK SELECT'
                                                          ,'SKY TALK LINE RENTAL'  )  --< Optimises the code, limit to what is needed
                       AND csh.effective_from_dt <> csh.effective_to_dt
                  GROUP BY csh.account_number
        )AS tgt ON base.account_number = tgt.account_number;

COMMIT;

--123225 Row(s) affected
--5m


UPDATE v066_allinone
   SET DTV_only = 1
   from v066_allinone
        where   bb = 0
                AND talk = 0
                AND wlr = 0
;


-- HD Box

ALTER TABLE v066_allinone
ADD HD_box tinyint;

UPDATE v066_allinone
   SET HD_box = 1
  FROM v066_allinone AS base
       INNER JOIN (
                    SELECT DISTINCT stb.account_number
                      FROM sk_prod.CUST_SET_TOP_BOX AS stb
                           INNER JOIN v066_allinone AS acc on stb.account_number = acc.account_number
                     WHERE box_installed_dt   <= @target_dt
                       AND box_replaced_dt    >  @target_dt
                       AND current_product_description like '%HD%'
       ) AS tgt ON base.account_number = tgt.account_number;
--85062 Row(s) affected
COMMIT;



-- Derive Box Type



ALTER TABLE v066_allinone
ADD box_type varchar(15) ;

UPDATE v066_allinone
   SET box_type = CASE WHEN HD =1 AND MR = 1            THEN 'HD_Combi'
                       WHEN HD =1                       THEN 'HD'
                       WHEN HD_box =1 AND MR = 1        THEN 'HDx_Combi'
                       WHEN HD_box =1                   THEN 'HDx'
                       WHEN SP =1 AND MR = 1            THEN 'SkyPlus_Combi'
                       WHEN SP =1                       THEN 'SkyPlus'
                       WHEN MR =1                       THEN 'Multiroom'
                       ELSE                                  'FDB'
                    END;
--123225 Row(s) affected


select top 10 * from v066_allinone


----------------------------------------------------------------C.2.4 Sky Tenure

Alter table v066_allinone
Add Sky_tenure_yrs int;

Update v066_allinone
SET Sky_tenure_yrs = DATEDIFF(day,sav.acct_first_account_activation_dt, @target_dt)/365
 FROM v066_allinone AS base
       INNER JOIN sk_prod.cust_single_account_view AS sav ON base.account_number = sav.account_number;

----------------------------------------------------------------C.2.5 Lapsed movie subscriber


SELECT  csh.Account_number
         ,csh.effective_from_dt as downgrade_date
         ,csh.current_short_description
         ,ncel.prem_movies as current_movies
         ,ocel.prem_movies as old_movies
         ,RANK() OVER (PARTITION BY csh.account_number ORDER BY csh.effective_from_dt DESC, csh.cb_row_id DESC) AS 'RANK' -- DESC ADDED TO EFFECTIVE FROM
    INTO #down
    FROM v066_allinone as base
         inner join sk_prod.cust_subs_hist as csh
                    on base.account_number = csh.account_number
         inner join sk_prod.cust_entitlement_lookup as ncel
                    on csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup as ocel
                    on csh.previous_short_description = ocel.short_description
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code in ('AC','PC','AB')   -- Active records
    AND current_movies < old_movies   -- movies downgrade
    AND current_movies = 0
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;



--select top 100 * from sk_prod.cust_entitlement_lookup

 alter table     v066_allinone
add(            lapsed_movies integer default 0);



update v066_allinone
set lapsed_movies = (case when atl.account_number = in1.account_number then 1 else lapsed_movies end)
from v066_allinone in1
left join #DOWN atl
on  atl.account_number = in1.account_number

-- select top 10 * from v066_allinone


----------------------------------------------------------------C.2.6 Tenure of previous movie subscription

----------------------------------------------------------------C.2.7 Previous offer user







----------------------------------------------------------------C.2.8 Call behaviour


----------------------------------------------------------------C.2.9 RTM of new customers






----------------------------------------------------------------------------D.1 Flag each Universe in our dataset

/*
Full dataset with all accounts are in table v066_allinone

select top 10 * from v066_allinone
*/

---------------------------------------------D1.1 Universe 1 Non-movie accounts (upgrade-potential scores)
alter table v066_allinone
add (Movies_model_flag integer default 0
     ,movies_model_decile integer default 0);

update v066_allinone
set Movies_model_flag = (case when atl.account_number = in1.account_number then 1 else Movies_model_flag end)
    ,movies_model_decile = ctrl_decile
from v066_allinone in1
left join #ATLmovies atl
on  atl.account_number = in1.account_number


-- the customer must not have a movies package:

update v066_allinone
set Movies_model_flag = (case when tv_premiums in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies', 'Top Tier') then 0 else Movies_model_flag end)
--196126 Row(s) affected

/* QA changes (update table as altered to include top tier as movie subs)

alter table v066_allinone
drop Movies_model_flag;

alter table v066_allinone
add Movies_model_flag integer default 0;

update v066_allinone
set Movies_model_flag = (case when atl.account_number = in1.account_number then 1 else Movies_model_flag end)
from v066_allinone in1
left join #ATLmovies atl
on  atl.account_number = in1.account_number

update v066_allinone
set Movies_model_flag = (case when tv_premiums in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies', 'Top Tier') then 0 else Movies_model_flag end)

*/
---------------------------------------------D1.1a Non movies accounts (High Potential upgraders)

alter table v066_allinone
add U_high_potential_upgraders tinyint;

update v066_allinone
set U_high_potential_upgraders = (case when movies_model_flag = 1 AND movies_model_decile in(1,2,3,4) THEN 1 else 0 END)
FROM v066_allinone
;
--196126 Row(s) affected

/*
select count(*) from v066_allinone
where high_potential_upgraders = 1; --52535

select top 100 * from v066_allinone;

alter table v066_allinone
drop high_potential_upgraders
*/

---------------------------------------------D1.1b Non movies accounts (Low Potential upgraders)

alter table v066_allinone
add U_low_potential_upgraders tinyint;

update v066_allinone
set U_low_potential_upgraders = (case when movies_model_flag = 1 AND movies_model_decile in(5,6,7,8,9,10) THEN 1 else 0 END)
FROM v066_allinone
;


/*
QA check

alter table v066_allinone
drop low_potential_upgraders

select top 100 * from v066_allinone

select count(*) from v066_allinone
where Movies_model_flag = 1

alter table v066_allinone
drop Movies_model_flag

select distinct(tv_premiums) from v066_allinone

select *
into #flag
from v066_allinone
where Movies_model_flag = 1 AND tv_premiums in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies', 'Top Tier')
--235 Row(s) affected

select * from #flag
*/

---------------------------------------------D1.2a Universe 2 Xmas 2011 period upgraders
alter table v066_allinone
add(upgraded integer default 0);

--select top 10 * from #upgrades


update v066_allinone
set upgraded  = (case when atl.account_number = in1.account_number then 1 else 0 end)
from v066_allinone in1
left join #upgrades atl
on  atl.account_number = in1.account_number
--196126 Row(s) affected

--select upgraded, count(*) from v066_allinone group by upgraded





---------------------------------------------D1.3 Universe 3 Stable Xmas period upgraders


alter table v066_allinone
add (Stable_up integer default 0);



update v066_allinone
set a.Stable_up  = b.stable_flag
from v066_allinone a
left join v066_stability b
on  a.account_number = b.account_number
;
--196126 Row(s) affected

/*
select count(*), stable_up from v066_allinone
group by stable_up
*/


---------------------------------------------D1.4 Universe 4 Sky base

alter table v066_allinone
add( Sky_Base integer default 0)
;

--select top 10 * from #sky

update v066_allinone
set Sky_Base  = (case when atl.account_number = in1.account_number then 1 else 0 end)
from v066_allinone in1
left join #sky atl
on  atl.account_number = in1.account_number

--196126 Row(s) affected



---------------------------------------------D1.2b Universe 2b Non movies accounts who didn't upgrade over xmas period 2011
alter table v066_allinone
add( Non_upgraded integer default 0);

--select top 10 * from #upgrades


update v066_allinone
set Non_upgraded  = (case when Sky_base = 1
                        AND upgraded = 0
                        AND tv_premiums not in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier')
                                                THEN 1 else 0 end)
from v066_allinone
;

select sum(normalised_weight) from v066_allinone
 where sky_base = 1 AND tv_premiums  in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier')

select sum(normalised_weight),tv_premiums from v066_allinone
group by tv_premiums

/* QA changes (update table as altered to include top tier as movie subs)

alter table v066_allinone
drop Non_upgraded;

alter table v066_allinone
add U_Non_upgraded integer default 0;


update v066_allinone
set U_Non_upgraded  = (case when Sky_base = 1
                        AND upgraded = 0
                        AND tv_premiums not in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier')                                                THEN 1 else 0 end)
from v066_allinone
;
*/

---------------------------------------------D1.5 Universe 5 HD upgrade


--------------------------D1.5a HD upgrade likely
alter table v066_allinone
add U_High_potential_HD tinyint;


update v066_allinone
set U_High_potential_HD  = (case when atl.account_number = in1.account_number AND movies_model_decile in(1,2,3) then 1 else 0 end)
from v066_allinone in1
left join #HD atl
on  atl.account_number = in1.account_number
;
--196126 Row(s) affected

--------------------------D1.5a HD upgrade unlikely

alter table v066_allinone
add U_low_potential_HD tinyint;

update v066_allinone
set U_low_potential_HD  = (case when atl.account_number = in1.account_number AND movies_model_decile in(4,5,6,7,8,9,10) then 1 else 0 end)
from v066_allinone in1
left join #HD atl
on  atl.account_number = in1.account_number
;
--196126 Row(s) affected


--------------------------D1.6 CQM scores

-- add the CQM score to base table and the banding definitions
alter table gillh.v066_allinone
add cqm_score tinyint default null
,add cqm_grouping varchar(20) default null;

update gillh.v066_allinone xx
set xx.cqm_score = zz.model_score
  ,xx.cqm_grouping = CASE WHEN zz.model_score >= 26     THEN   'A)LOW quality'
                             WHEN zz.model_score >= 18     THEN   'B)MED quality'
                             WHEN zz.model_score >= 1      THEN   'C)High quality'
                             else                            'D)UNKNOWN'

                        end
from sk_prod.id_v_universe_all zz
where xx.cb_key_household = zz.cb_key_household
;
----------------------------------------------------------------------------E.1 OUTPUT
SELECT TOP 10 * FROM gillh.v066_allinone;

SELECT count(*) FROM v066_allinone;  --196126

select sum(normalised_weight) FROM v066_allinone
where Sky_Base = 1;
select sum(normalised_weight) FROM v066_allinone
where  Upgraded= 1;
select sum(normalised_weight) FROM v066_allinone
where Stable_up = 1;
select sum(normalised_weight) FROM v066_allinone
where Non_upgraded = 1;
select sum(normalised_weight) FROM v066_allinone
where high_potential_upgraders = 1;
select sum(normalised_weight) FROM v066_allinone
where low_potential_upgraders = 1;


SELECT
Stable_up
,upgraded
,Sky_Base
,U_high_potential_upgraders
,U_low_potential_upgraders
,U_Non_upgraded
,U_high_potential_HD
,U_low_potential_HD
,normalised_weight
,average_pay_minutes
,average_free_minutes
,average_Total_minutes_day
,average_PVR_minutes
,average_FTA_movie_minutes
--,sov_pay_tv
--,sov_pvr
--,sov_FTA_movies
,Region
,HHcomposition
,Affluence
--,financial_stress
,mosaic_group_desc
--,mosaic_type_desc
--,personicx_desc
,tv_package
,tv_premiums
,HD
,MR
--,SP
--,BB
--,Talk
--,WLR
--,DTV_only
--,HD_box
,box_type
,Sky_tenure_yrs
,lapsed_movies
,cqm_grouping
FROM gillh.v066_allinone;

--- AFFLUENCE
SELECT AFFLUENCE
        ,COUNT(CASE WHEN MOVIES_MODEL_FLAG = 1 AND MOVIES_MODEL_DECILE BETWEEN 1 AND 4 THEN ACCOUNT_NUMBER ELSE NULL END) AS UNIVERSE1_UPGRADE_POTENTIAL_HIGH
        ,COUNT(CASE WHEN MOVIES_MODEL_FLAG = 1 AND MOVIES_MODEL_DECILE BETWEEN 5 AND 10 THEN ACCOUNT_NUMBER ELSE NULL END) AS UNIVERSE4_UPGRADE_POTENTIAL_LOW
FROM v066_allinone
GROUP BY AFFLUENCE




--- REGION
SELECT REGION
        ,COUNT(CASE WHEN MOVIES_MODEL_FLAG = 1 AND MOVIES_MODEL_DECILE BETWEEN 1 AND 4 THEN ACCOUNT_NUMBER ELSE NULL END) AS UNIVERSE1_UPGRADE_POTENTIAL_HIGH
        ,COUNT(CASE WHEN MOVIES_MODEL_FLAG = 1 AND MOVIES_MODEL_DECILE BETWEEN 5 AND 10 THEN ACCOUNT_NUMBER ELSE NULL END) AS UNIVERSE4_UPGRADE_POTENTIAL_LOW
FROM v066_allinone
GROUP BY REGION


--- tv_premiums
SELECT tv_premiums
        ,COUNT(CASE WHEN MOVIES_MODEL_FLAG = 1 AND MOVIES_MODEL_DECILE BETWEEN 1 AND 4 THEN ACCOUNT_NUMBER ELSE NULL END) AS UNIVERSE1_UPGRADE_POTENTIAL_HIGH
        ,COUNT(CASE WHEN MOVIES_MODEL_FLAG = 1 AND MOVIES_MODEL_DECILE BETWEEN 5 AND 10 THEN ACCOUNT_NUMBER ELSE NULL END) AS UNIVERSE4_UPGRADE_POTENTIAL_LOW
FROM v066_allinone
GROUP BY tv_premiums


--- lapsed_movies
SELECT lapsed_movies
        ,COUNT(CASE WHEN MOVIES_MODEL_FLAG = 1 AND MOVIES_MODEL_DECILE BETWEEN 1 AND 4 THEN ACCOUNT_NUMBER ELSE NULL END) AS UNIVERSE1_UPGRADE_POTENTIAL_HIGH
        ,COUNT(CASE WHEN MOVIES_MODEL_FLAG = 1 AND MOVIES_MODEL_DECILE BETWEEN 5 AND 10 THEN ACCOUNT_NUMBER ELSE NULL END) AS UNIVERSE4_UPGRADE_POTENTIAL_LOW
FROM v066_allinone
GROUP BY lapsed_movies




Single Sports Single Movies
Dual Movies
Single Movies
Single Sports Dual Movies
Dual Sports Single Movies


---------------------------------------------------------------------------------------------------------Z.1 Actual Sky Base Profiles

----------------------------------------------------------Z.1.0 Get sky base accounts, package & premiums

---------Accounts

Select * into v066_Sky_Base_Actual
FROM #Sky;

--10104921 Row(s) affected

alter table v066_Sky_Base_Actual
add(    cb_key_household        bigint default null
        ,cb_key_individual      bigint default null
        ,cb_key_family          bigint default null
);


update v066_Sky_Base_Actual
set     in1.cb_key_household = sav.cb_key_household
        ,in1.cb_key_individual = sav.cb_key_individual
        ,in1.cb_key_family = sav.cb_key_family
from v066_Sky_Base_Actual as in1 INNER JOIN sk_prod.cust_single_account_view sav
on in1.account_number = sav.account_number
;

create hg   index idx1 on v066_Sky_Base_Actual(cb_key_household);
create hg   index idx2 on v066_Sky_Base_Actual(account_number);

-------------------------------------------------------------Z.1 Demographic metrics



----------------------------------------------------------------Z.1.1 TV ISBA Region


alter table v066_Sky_Base_Actual
add( Region varchar(25));


update          v066_Sky_Base_Actual base
set             Region = CASE WHEN sav.isba_tv_region = 'Not Defined'
                                       THEN 'UNKNOWN'
                                       ELSE sav.isba_tv_region
                                   END
FROM            v066_Sky_Base_Actual base INNER JOIN sk_prod.cust_single_account_view AS sav
                        ON base.account_number = sav.account_number;


--select top 10 * from v066_Sky_Base_Actual



----------------------------------------------------------------Z.1.2 HH composition

select      distinct base.account_number
            ,max(CASE WHEN ilu_hhcomposition = 'A1'                     THEN 'Female Single Parent'
                     WHEN ilu_hhcomposition = 'A2'                      THEN 'Female single pensioner'
                     WHEN ilu_hhcomposition = 'A3'                      THEN 'Female single other'
                     WHEN ilu_hhcomposition = 'B2'                      THEN 'Male single pensioner'
                     WHEN ilu_hhcomposition = 'B3'                      THEN 'Male single non-pensioner'
                     WHEN ilu_hhcomposition = 'C1'                      THEN 'Married couple with dependent children'
                     WHEN ilu_hhcomposition = 'C2'                      THEN 'Married couple pensioners'
                     WHEN ilu_hhcomposition = 'C3'                      THEN 'Married couple other'
                     WHEN ilu_hhcomposition = 'D1'                      THEN 'Co-habiting couple with dependent children'
                     WHEN ilu_hhcomposition = 'D3'                      THEN 'Co-habiting couple - no dependents'
                     WHEN ilu_hhcomposition = 'E1'                      THEN 'Two person household with dependent children'
                     WHEN ilu_hhcomposition = 'E2'                      THEN 'Two person pensioner household'
                     WHEN ilu_hhcomposition = 'E3'                      THEN 'Two person other'
                     WHEN ilu_hhcomposition = 'F1'                      THEN 'Married family - grown up and dependent children at home'
                     WHEN ilu_hhcomposition = 'F2'                      THEN 'Married family - pensioners and grown up children at home'
                     WHEN ilu_hhcomposition = 'F3'                      THEN 'Married family - grown up children at home'
                     WHEN ilu_hhcomposition = 'G3'                      THEN 'Non married family - grown up children at home'
                     WHEN ilu_hhcomposition = 'H1'                      THEN 'Married couple and 1 other adult - dependent children'
                     WHEN ilu_hhcomposition = 'H2'                      THEN 'Married couple and 1 other adult - Pensioners'
                     WHEN ilu_hhcomposition = 'H3'                      THEN 'Married couple and 1 other adult - Other'
                     WHEN ilu_hhcomposition = 'I1'                      THEN 'Married couple and 2+ other adults - dependent children'
                     WHEN ilu_hhcomposition = 'I2'                      THEN 'Married couple and 2+ other adults - Pensioners'
                     WHEN ilu_hhcomposition = 'I3'                      THEN 'Married couple and 2+ other adults - Other'
                     WHEN ilu_hhcomposition = 'J1'                      THEN 'Co-habiting couple and 1+ other adults - dependent children'
                     WHEN ilu_hhcomposition = 'J3'                      THEN 'Co-habiting couple and 1+ other adults - no dependents'
                     WHEN ilu_hhcomposition = 'K1'                      THEN 'Mixed household of 3+ adults - dependent children'
                     WHEN ilu_hhcomposition = 'K3'                      THEN 'Mixed household of 3+ adults - no dependents'
                ELSE                                                         'Unknown'
                END) as hhcomposition
                ,max(ilu.cb_key_household) as cb_key_household
into            #ilu_hh                                                --drop table #ilu_hh
from            sk_prod.ilu as ilu INNER JOIN v066_Sky_Base_Actual as base
                        ON ilu.cb_key_household = base.cb_key_household
group by        base.account_number
;
commit;
--200m
--9044765 Row(s) affected

alter table v066_Sky_Base_Actual
add( HHcomposition varchar(60));

update v066_Sky_Base_Actual
set HHcomposition = ilu.hhcomposition
from v066_Sky_Base_Actual in1
left join  #ilu_hh ilu
on in1.account_number = ilu.account_number
;



----------------------------------------------------------------Z.1.3 Affluence


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
where           sav.account_number in (select distinct account_number from v066_Sky_Base_Actual)
group by        sav.account_number;
commit;


alter table v066_Sky_Base_Actual
add( Affluence varchar(10));

update v066_Sky_Base_Actual
set affluence = sav_affluence
from v066_Sky_Base_Actual in1
left join  #sav_dem sav
on in1.account_number = sav.account_number





--select top 100 * from v066_sky_base_actual;

-- ----------------------------------------------------------------Z.1.5 Mosaic segments


IF OBJECT_ID ('nodupes') IS NOT NULL DROP TABLE nodupes


-- this is new
SELECT   CV.cb_key_household
        ,CV.h_mosaic_uk_2009_group
        ,CV.cb_change_date
        ,CV.h_mosaic_uk_2009_type
        ,rank() over(PARTITION BY cv.cb_key_household ORDER BY cb_row_id desc) AS rank_id
--       ,(RTRIM(Lifestage)||RTRIM(Family_Lifestage)||RTRIM(tenure)||RTRIM(h_mosaic_uk_2009_group)||RTRIM(h_mosaic_uk_2009_type)||RTRIM(h_fss_v3_group)) AS exp
INTO nodupes
FROM sk_prod.EXPERIAN_CONSUMERVIEW cv
where cb_key_household in (select distinct(cb_key_household) from v066_Sky_Base_Actual)
--28739208 Row(s) affected
delete from nodupes where rank_id > 1



--- we need to understand what the segments mean:

-- lets do mosaic type first the group later
alter table nodupes
add(mosaic_type_desc varchar(50));


update nodupes
set mosaic_type_desc =

case when h_mosaic_uk_2009_type =       '01'    then    'Global Power Brokers'
 when h_mosaic_uk_2009_type =   '02'    then    'Voices of Authority'
 when h_mosaic_uk_2009_type =   '03'    then    'Business Class'
 when h_mosaic_uk_2009_type =   '04'    then    'Serious Money'
 when h_mosaic_uk_2009_type =   '05'    then    'Mid-Career Climbers'
 when h_mosaic_uk_2009_type =   '06'    then    'Yesterdays Captains'
 when h_mosaic_uk_2009_type =   '07'    then    'Distinctive Success'
 when h_mosaic_uk_2009_type =   '08'    then    'Dormitory Villagers'
 when h_mosaic_uk_2009_type =   '09'    then    'Escape to the Country'
 when h_mosaic_uk_2009_type =   '10'    then    'Parish Guardians'
 when h_mosaic_uk_2009_type =   '11'    then    'Squires Among Locals'
 when h_mosaic_uk_2009_type =   '12'    then    'Country Loving Elders'
 when h_mosaic_uk_2009_type =   '13'    then    'Modern Agribusiness'
 when h_mosaic_uk_2009_type =   '14'    then    'Farming Today'
 when h_mosaic_uk_2009_type =   '15'    then    'Upland Struggle'
 when h_mosaic_uk_2009_type =   '16'    then    'Side Street Singles'
 when h_mosaic_uk_2009_type =   '17'    then    'Jacks of All Trades'
 when h_mosaic_uk_2009_type =   '18'    then    'Hardworking Families'
 when h_mosaic_uk_2009_type =   '19'    then    'Innate Conservatives'
 when h_mosaic_uk_2009_type =   '20'    then    'Golden Retirement'
 when h_mosaic_uk_2009_type =   '21'    then    'Bungalow Quietude'
 when h_mosaic_uk_2009_type =   '22'    then    'Beachcombers'
 when h_mosaic_uk_2009_type =   '23'    then    'Balcony Downsizers'
 when h_mosaic_uk_2009_type =   '24'    then    'Garden Suburbia'
 when h_mosaic_uk_2009_type =   '25'    then    'Production Managers'
 when h_mosaic_uk_2009_type =   '26'    then    'Mid-Market Families'
 when h_mosaic_uk_2009_type =   '27'    then    'Shop Floor Affluence'
 when h_mosaic_uk_2009_type =   '28'    then    'Asian Attainment'
 when h_mosaic_uk_2009_type =   '29'    then    'Footloose Managers'
 when h_mosaic_uk_2009_type =   '30'    then    'Soccer Dads and Mums'
 when h_mosaic_uk_2009_type =   '31'    then    'Domestic Comfort'
 when h_mosaic_uk_2009_type =   '32'    then    'Childcare Years'
 when h_mosaic_uk_2009_type =   '33'    then    'Military Dependants'
 when h_mosaic_uk_2009_type =   '34'    then    'Buy-to-Let Territory'
 when h_mosaic_uk_2009_type =   '35'    then    'Brownfield Pioneers'
 when h_mosaic_uk_2009_type =   '36'    then    'Foot on the Ladder'
 when h_mosaic_uk_2009_type =   '37'    then    'First to Move In'
 when h_mosaic_uk_2009_type =   '38'    then    'Settled Ex-Tenants'
 when h_mosaic_uk_2009_type =   '39'    then    'Choice Right to Buy'
 when h_mosaic_uk_2009_type =   '40'    then    'Legacy of Labour'
 when h_mosaic_uk_2009_type =   '41'    then    'Stressed Borrowers'
 when h_mosaic_uk_2009_type =   '42'    then    'Worn-Out Workers'
 when h_mosaic_uk_2009_type =   '43'    then    'Streetwise Kids'
 when h_mosaic_uk_2009_type =   '44'    then    'New Parents in Need'
 when h_mosaic_uk_2009_type =   '45'    then    'Small Block Singles'
 when h_mosaic_uk_2009_type =   '46'    then    'Tenement Living'
 when h_mosaic_uk_2009_type =   '47'    then    'Deprived View'
 when h_mosaic_uk_2009_type =   '48'    then    'Multicultural Towers'
 when h_mosaic_uk_2009_type =   '49'    then    'Re-Housed Migrants'
 when h_mosaic_uk_2009_type =   '50'    then    'Pensioners in Blocks'
 when h_mosaic_uk_2009_type =   '51'    then    'Sheltered Seniors'
 when h_mosaic_uk_2009_type =   '52'    then    'Meals on Wheels'
 when h_mosaic_uk_2009_type =   '53'    then    'Low Spending Elders'
 when h_mosaic_uk_2009_type =   '54'    then    'Clocking Off'
 when h_mosaic_uk_2009_type =   '55'    then    'Backyard Regeneration'
 when h_mosaic_uk_2009_type =   '56'    then    'Small Wage Owners'
 when h_mosaic_uk_2009_type =   '57'    then    'Back-to-Back Basics'
 when h_mosaic_uk_2009_type =   '58'    then    'Asian Identities'
 when h_mosaic_uk_2009_type =   '59'    then    'Low-Key Starters'
 when h_mosaic_uk_2009_type =   '60'    then    'Global Fusion'
 when h_mosaic_uk_2009_type =   '61'    then    'Convivial Homeowners'
 when h_mosaic_uk_2009_type =   '62'    then    'Crash Pad Professionals'
 when h_mosaic_uk_2009_type =   '63'    then    'Urban Cool'
 when h_mosaic_uk_2009_type =   '64'    then    'Bright Young Things'
 when h_mosaic_uk_2009_type =   '65'    then    'Anti-Materialists'
 when h_mosaic_uk_2009_type =   '66'    then    'University Fringe'
 when h_mosaic_uk_2009_type =   '67'    then    'Study Buddies'
 when h_mosaic_uk_2009_type =   '99'    then    'Unclassified'
 else   'Unclassified' end



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
 else 'Unclassified' end


-- now we need to match this back to our all in one table


alter table nodupes
add(account_number varchar(20));


update nodupes
set nod.account_number  = vre.account_number
from nodupes nod
left join V066_Sky_Base_Actual vre
on nod.cb_key_household = vre.cb_key_household


-- --checks
-- select top 100 * from nodupes order by h_mosaic_uk_2009_type
-- select count(*) from nodupes where account_number is null -- good!
--
-- now put it in the all in one table

alter table v066_Sky_Base_Actual
add(    mosaic_group_desc varchar(50)
        ,mosaic_type_desc varchar(20));


update v066_Sky_Base_Actual
set    al1.mosaic_group_desc = nod.mosaic_group_desc
       ,al1.mosaic_type_desc = nod.mosaic_type_desc
from v066_Sky_Base_Actual al1
left join nodupes nod
on nod.account_number = al1.account_number



-- lets check it:
--select top 100 * from v066_Sky_Base_Actual


----------------------------------------------------------------Z.1.6 Personicx segments

IF object_id('cust_ilu') IS NOT NULL DROP TABLE cust_ilu;


-- lets get the personicx segment code and match back to CB_key_household
select cb_key_household
       ,ils_hhpersonicx2
       ,rank() over(PARTITION BY cb_key_household ORDER BY cb_row_id desc) AS rank_id
into cust_ilu
from sk_prod.ilu
where cb_key_household in (select cb_key_household from nodupes) -- this is distinct so faster!


delete from cust_ilu where rank_id > 1


--check it:
select count(*) from cust_ilu -- 117k!
select top 100 * from cust_ilu order by cb_key_household

-- now we need to know what the codes mean:

-- create a lookup
SELECT 'HX201' AS code, 'HX201 - Student Life' as description INTO #psx_lookup
UNION SELECT 'HF502', 'HF502 - Full Time Single Mums'
UNION SELECT 'HX303', 'HX303 - Mid-Income Traders'
UNION SELECT 'HX204', 'HX204 - Affluent Adventurers'
UNION SELECT 'HF405', 'HF405 - Value Pack Renters'
UNION SELECT 'HX206', 'HX206 - Funky DINKYs'
UNION SELECT 'HX307', 'HX307 - Healthy and Herbal'
UNION SELECT 'HX208', 'HX208 - Active Professionals'
UNION SELECT 'HF309', 'HF309 - Family Dial-ups'
UNION SELECT 'HF310', 'HF310 - Working Parents'
UNION SELECT 'HX111', 'HX111 - Off-Piste Online'
UNION SELECT 'HF212', 'HF212 - Toddlers and Toy Shops'
UNION SELECT 'HF313', 'HF313 - Single Mums and Students'
UNION SELECT 'HX514', 'HX514 - Single Renters '
UNION SELECT 'HF115', 'HF115 - Professional Parents'
UNION SELECT 'HX316', 'HX316 - Guys No Dolls'
UNION SELECT 'HF517', 'HF517 - Cash Limited'
UNION SELECT 'HF418', 'HF418 - Big Families – Local Focus'
UNION SELECT 'HF319', 'HF319 - Nuclear Families'
UNION SELECT 'HF320', 'HF320 - Factory Worker Families'
UNION SELECT 'HX221', 'HX221 - Mobile Mid-Management'
UNION SELECT 'HF122', 'HF122 - Rich Returns'
UNION SELECT 'HF123', 'HF123 - Black Run Connoisseurs'
UNION SELECT 'HF224', 'HF224 - Borrowers and Spenders'
UNION SELECT 'HX225', 'HX225 - Free Liberals'
UNION SELECT 'HX326', 'HX326 - Nearly Empty Nesters'
UNION SELECT 'HX227', 'HX227 - Gregarious Grandparents'
UNION SELECT 'HX328', 'HX328 - Cosy Arts'
UNION SELECT 'HX129', 'HX129 - Portfolio Professionals'
UNION SELECT 'HX430', 'HX430 - Odds On Factory Workers'
UNION SELECT 'HX231', 'HX231 - Deluxe Investments'
UNION SELECT 'HX132', 'HX132 - Vintage Professionals'
UNION SELECT 'HX533', 'HX533 - Roll-ups and Rent'
UNION SELECT 'HX334', 'HX334 - Own Value'
UNION SELECT 'HX335', 'HX335 - Working Grandparents'
UNION SELECT 'HX336', 'HX336 - Settled Solos'
UNION SELECT 'HX237', 'HX237 - High Flying Solos'
UNION SELECT 'HR438', 'HR438 - Place Your Bets'
UNION SELECT 'HR339', 'HR339 - Just Retired'
UNION SELECT 'HR240', 'HR240 - Healthy Income'
UNION SELECT 'HR441', 'HR441 - Bingo and Buses'
UNION SELECT 'HR442', 'HR442 - Comfortable Couples'
UNION SELECT 'HR343', 'HR343 - Silver Reserves'
UNION SELECT 'HR544', 'HR544 - Bus Trips and Bungalows'
UNION SELECT 'HR245', 'HR245 - Mature Money'
UNION SELECT 'HR446', 'HR446 - Pastoral Pleasures'
UNION SELECT 'HR547', 'HR547 - Mobile Pensioners'
UNION SELECT 'HR448', 'HR448 - Local Shoppers'
UNION SELECT 'HR349', 'HR349 - Grey Volunteers'
UNION SELECT 'HR450', 'HR450 - Savers and Givers'
UNION SELECT 'HR551', 'HR551 - Elderly Pensioners'
UNION SELECT 'HR552', 'HR552 - Coaches and Crosswords'
;

select top 10 * from #psx_lookup

-- add the flag
alter table cust_ilu
add(personicx_desc varchar (35));

update cust_ilu
set personicx_desc = description
from cust_ilu
left join #psx_lookup
on ils_hhpersonicx2 = code



-- lets get rid of the nulls
update cust_ilu
set personicx_desc = case when personicx_desc is null then 'Unknown' else personicx_desc end

select personicx_desc, count(*) from cust_ilu group by personicx_desc -- there are none!



-- add account_number

alter table cust_ilu
add(account_number varchar (20));

update cust_ilu
set ilu.account_number = nod.account_number
from cust_ilu as ilu
left join nodupes nod
on ilu.cb_key_household = nod.cb_key_household



--
-- -- check it:
-- select top 100 * from cust_ilu
-- select count(*) from cust_ilu where account_number is null
--


-- now lets put this into the all in one table for analysis:

alter table v066_Sky_Base_Actual
add(personicx_desc varchar(35));


update v066_Sky_Base_Actual
set    al1.personicx_desc = nod.personicx_desc
from v066_Sky_Base_Actual al1
left join cust_ilu nod
on nod.account_number = al1.account_number


-- lets check it:
-- select top 100 * from v066_Sky_Base_Actual -- there seem to be blanks in this table
--
--
-- select personicx_desc, count(*) from v066_Sky_Base_Actual group by personicx_desc -- a large number were blank, i dont know why?!

-- lets get rid of the nulls (data is'nt avaiable for all of our customers.
update v066_Sky_Base_Actual
set personicx_desc = case when personicx_desc is null then 'Unknown' else personicx_desc end







--QA
/*
select top 100 * from v066_Sky_Base_Actual
where tv_premiums <> 'Basic' and tv_package = 'Entertainment';
*/

----------------------------------------------------------------Z.2.2 Box AND Z.2.3 Product




Alter table v066_Sky_Base_Actual
ADD
        (
        HD  int
        ,MR  int
        ,SP  int
        ,BB  int
        ,Talk  int
        ,WLR  int
        ,DTV_only int
        )
;


UPDATE v066_Sky_Base_Actual
   SET HD        = tgt.hdtv
      ,MR        = tgt.multiroom
      ,SP        = tgt.skyplus
      ,BB        = tgt.broadband
      ,talk      = tgt.skytalk
      ,WLR       = tgt.wlr
 FROM v066_Sky_Base_Actual AS base
      INNER JOIN (
                    SELECT  csh.account_number
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS skyplus
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS multiroom
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END)  AS hdtv
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='Broadband DSL Line'
                                       AND (       status_code in ('AC','AB')
                                               OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                               OR (status_code='CF' AND prev_status_code='PC'                                  )
                                               OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                                            )                                    THEN 1 ELSE 0 END)  AS broadband
                           ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
                                       AND (     csh.status_code = 'A'
                                             OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                             OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                             OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                                            )                                  THEN 1 ELSE 0 END)   AS skytalk
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='SKY TALK LINE RENTAL'
                                       AND csh.status_code in ('A','CRQ','R')  THEN 1 ELSE 0 END) AS wlr
                      FROM sk_prod.cust_subs_hist AS csh
                           INNER JOIN v066_Sky_Base_Actual AS base ON csh.account_number = base.account_number
                           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
                     WHERE csh.effective_from_dt <= @target_dt
                       AND csh.effective_to_dt    > @target_dt
                       AND csh.subscription_sub_type  IN ( 'DTV Primary Viewing'
                                                          ,'DTV Sky+'
                                                          ,'DTV Extra Subscription'
                                                          ,'DTV HD'
                                                          ,'Broadband DSL Line'
                                                          ,'SKY TALK SELECT'
                                                          ,'SKY TALK LINE RENTAL'  )  --< Optimises the code, limit to what is needed
                       AND csh.effective_from_dt <> csh.effective_to_dt
                  GROUP BY csh.account_number
        )AS tgt ON base.account_number = tgt.account_number;

COMMIT;

--10104989 Row(s) affected
--4m


UPDATE v066_Sky_Base_Actual
   SET DTV_only = 1
   from v066_Sky_Base_Actual
        where   bb = 0
                AND talk = 0
                AND wlr = 0
;


-- HD Box

ALTER TABLE v066_Sky_Base_Actual
ADD HD_box tinyint;

UPDATE v066_Sky_Base_Actual
   SET HD_box = 1
  FROM v066_Sky_Base_Actual AS base
       INNER JOIN (
                    SELECT DISTINCT stb.account_number
                      FROM sk_prod.CUST_SET_TOP_BOX AS stb
                           INNER JOIN v066_Sky_Base_Actual AS acc on stb.account_number = acc.account_number
                     WHERE box_installed_dt   <= @target_dt
                       AND box_replaced_dt    >  @target_dt
                       AND current_product_description like '%HD%'
       ) AS tgt ON base.account_number = tgt.account_number;
--85062 Row(s) affected
COMMIT;



-- Derive Box Type



ALTER TABLE v066_Sky_Base_Actual
ADD box_type varchar(15) ;

UPDATE v066_Sky_Base_Actual
   SET box_type = CASE WHEN HD =1 AND MR = 1            THEN 'HD_Combi'
                       WHEN HD =1                       THEN 'HD'
                       WHEN HD_box =1 AND MR = 1        THEN 'HDx_Combi'
                       WHEN HD_box =1                   THEN 'HDx'
                       WHEN SP =1 AND MR = 1            THEN 'SkyPlus_Combi'
                       WHEN SP =1                       THEN 'SkyPlus'
                       WHEN MR =1                       THEN 'Multiroom'
                       ELSE                                  'FDB'
                    END;
--123225 Row(s) affected




--------------------------Z1.5a HD upgrade likely
alter table v066_Sky_Base_Actual
add U_High_potential_HD tinyint;


update v066_Sky_Base_Actual
set U_High_potential_HD  = (case when atl.account_number = in1.account_number AND atl.ctrl_decile in(1,2,3) then 1 else 0 end)
from v066_Sky_Base_Actual in1
left join #HD atl
on  atl.account_number = in1.account_number
;



---------Package & Premuims

alter table     v066_Sky_Base_Actual
add(            tv_package varchar(50) default 'Unknown',
                tv_premiums varchar (100) default 'Unknown');

SELECT          base.account_number,
                case when cel.prem_sports + cel.prem_movies = 4         then 'Top Tier'
                     when cel.prem_sports = 2 and cel.prem_movies = 1   then 'Dual Sports Single Movies'
                     when cel.prem_sports = 2 and cel.prem_movies = 0   then 'Dual Sports'
                     when cel.prem_sports = 1 and cel.prem_movies = 2   then 'Single Sports Dual Movies'
                     when cel.prem_sports = 0 and cel.prem_movies = 2   then 'Dual Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 1   then 'Single Sports Single Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 0   then 'Single Sports'
                     when cel.prem_sports = 0 and cel.prem_movies = 1   then 'Single Movies'
                     when cel.prem_sports + cel.prem_movies = 0         then 'Basic'
                else                                                    'Unknown'
                end as tv_premiums,
                case when (music = 0 AND news_events = 0 AND kids = 0 AND knowledge = 0)
                     then 'Entertainment'
                     when (music = 1 or news_events = 1 or kids = 1 or knowledge = 1)
                     then 'Entertainment Extra'
                     else 'Unknown' end as tv_package
into            #tvpackage
FROM            sk_prod.cust_subs_hist as csh
        inner join sk_prod.cust_entitlement_lookup as cel
                on csh.current_short_description = cel.short_description
        inner join v066_Sky_Base_Actual base
                on csh.account_number = base.account_number
WHERE           csh.subscription_sub_type ='DTV Primary Viewing'
AND             csh.subscription_type = 'DTV PACKAGE'
AND             csh.status_code in ('AC','AB','PC')
AND             csh.effective_from_dt < @target_dt
AND             csh.effective_to_dt   >= @target_dt
AND             csh.effective_from_dt != csh.effective_to_dt;
commit;

create hg index indx1 on #tvpackage(account_number);

update          v066_Sky_Base_Actual base
set             base.tv_package = tvp.tv_package,
                base.tv_premiums = tvp.tv_premiums
from            #tvpackage tvp
where           base.account_number = tvp.account_number
commit;


--------------------------------------------------------------Z.1.1 Sky Tenure

alter table     v066_Sky_Base_Actual
add            Sky_tenure_yrs int default 'Unknown'
;

Update v066_Sky_Base_Actual
SET Sky_tenure_yrs = DATEDIFF(day,sav.acct_first_account_activation_dt, @target_dt)/365
 FROM v066_Sky_Base_Actual AS base
       INNER JOIN sk_prod.cust_single_account_view AS sav ON base.account_number = sav.account_number;

-------------------------------------------------------------Z.1.2 Lapsed movie subscriber


SELECT  csh.Account_number
         ,csh.effective_from_dt as downgrade_date
         ,csh.current_short_description
         ,ncel.prem_movies as current_movies
         ,ocel.prem_movies as old_movies
         ,RANK() OVER (PARTITION BY csh.account_number ORDER BY csh.effective_from_dt DESC, csh.cb_row_id DESC) AS 'RANK' -- DESC ADDED TO EFFECTIVE FROM
    INTO #down
    FROM v066_Sky_Base_Actual as base
         inner join sk_prod.cust_subs_hist as csh
                    on base.account_number = csh.account_number
         inner join sk_prod.cust_entitlement_lookup as ncel
                    on csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup as ocel
                    on csh.previous_short_description = ocel.short_description
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code in ('AC','PC','AB')   -- Active records
    AND current_movies < old_movies   -- movies downgrade
    AND current_movies = 0
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;
--6987217 Row(s) affected

alter table     v066_Sky_Base_Actual
add(            lapsed_movies integer default 0)
;

update v066_Sky_Base_Actual
set lapsed_movies = (case when atl.account_number = lap.account_number then 1 else lapsed_movies end)
from v066_Sky_Base_Actual atl
left join #DOWN lap
on  atl.account_number = lap.account_number
;
COMMIT;

--------------------------------------------------------------------------------------------z.999 upgraders with offers

alter table     v066_Sky_Base_Actual
add            offer_upgrade integer default 0
;

update v066_Sky_Base_Actual
set offer_upgrade = (case when atl.account_number = lap.account_number then 1 else offer_upgrade end)
from v066_Sky_Base_Actual atl
left join #offers lap
on  atl.account_number = lap.account_number
;
COMMIT;

-------------------------------------------------------------Z.1.5 CQM Scores



-- CQM scores are joined on cb_key_household which is in the daily viewing table - lets copy the distinct accounts into a new table
--IF object_id('cb_temp') IS NOT NULL DROP TABLE cb_temp;

-- select distinct(account_number), max(cb_key_household) as cb_key_household
-- into cb_temp
-- from Daily_records2_capped
-- group by account_number


-- add the CQM score to sky base table and the banding definitions
alter table v066_Sky_Base_Actual
add cqm_score tinyint default null
,add cqm_indicator varchar(20) default null
,add cqm_grouping varchar(20) default null;

update v066_Sky_Base_Actual xx
set xx.cqm_score = zz.model_score
  , xx.cqm_indicator = case when zz.model_score between 1 and 22 then 'High quality'
                            when zz.model_score between 23 and 36 then 'Low quality'
                            else 'No Score!'
                        end
  ,xx.cqm_grouping = CASE WHEN zz.model_score >= 26     THEN   'A)LOW quality'
                             WHEN zz.model_score >= 18     THEN   'B)MED quality'
                             WHEN zz.model_score >= 1      THEN   'C)High quality'
                             else                            'D)UNKNOWN'

                        end
from sk_prod.id_v_universe_all zz
where xx.cb_key_household = zz.cb_key_household
;
--9118887 Row(s) affected









-- DO SOME QA checks -----

select max_score, count(distinct account_number) from CITeam.at_risk_all_by_month group by max_score

select top 10 * from v066_Sky_Base_Actual





-------------------------------------------------------------Z.1.6 At Risk Scores

alter table v066_Sky_Base_Actual
add at_risk_score int default null;

UPDATE v066_Sky_Base_Actual
   SET at_risk_score = 1
 FROM v066_Sky_Base_Actual AS base
       INNER JOIN (
                    SELECT DISTINCT account_number
                      FROM models.model_scores AS ms
                           INNER JOIN CITeam.SKY571_Models_timetable AS tt ON ms.model_name = tt.model_name -- This table determines the names of the models and their at risk cut offs
                     WHERE ms.model_run_date BETWEEN DATEADD(month,-1,'2011-11-22') AND '2011-11-22'        -- look at model run for previous month
                       AND ms.Percentiles <= tt.max_percentile                                              -- At risk
                       AND ms.model_run_date BETWEEN tt.start_date AND tt.end_date                          -- In lookup period limit
       ) AS tgt ON base.account_number = tgt.account_number;

COMMIT;





-------------------------------------------------------------------------Z.2.1 OUTPUT

select mosaic_group_desc
--, mosaic_type_desc
--, CQM_grouping
--, cqm_indicator
--, at_risk_score
, upgraded
--,stable_up
,offer_upgrade
, count(distinct account_number)
from v066_Sky_Base_Actual group by mosaic_group_desc,   upgraded ,offer_upgrade
;


Select  * from v066_Sky_Base_Actual;

select top 100 * from gillh.v066_allinone;

Alter table  v066_Sky_Base_Actual
add (stable_up tinyint
        ,upgraded tinyint)
;

Update  v066_Sky_Base_Actual
SET stable_up =  s.stable_up
        ,upgraded = s.upgraded
from v066_sky_base_actual sky INNER JOIN gillh.v066_allinone s
on s.account_number = sky.account_number
;


Alter table  v066_Sky_Base_Actual
add movies int;
(
        Aspiring_Young_Families varchar(35)
        ,Enterprising_Hardworking_Families varchar(35)
        ,Middle_Income_Families varchar(35)
        ,Young_Sharers varchar(35)
        ,Affluent_Successful_Families varchar(35)
        ,Elderly_Retired varchar(35)
)
;



SELECT
        account_number,
        case when mosaic_group_desc in('Careers and Kids', 'New Homemakers') THEN 'Aspiring_Young_Families'
         when mosaic_group_desc in('Ex-Council Community' -- ,'Claimant Cultures'
                ) THEN 'Enterprising_Hardworking_Families'
          when mosaic_group_desc in('Claimant Cultures')
                THEN 'Claimant Cultures'
         when mosaic_group_desc in('Suburban Mindsets', 'Industrial Heritage', 'Terraced Melting Pot') THEN 'Middle_Income_Families'
         when mosaic_group_desc in('Upper Floor Living') THEN 'Young_Sharers'
         when mosaic_group_desc in('Alpha Territory', 'Professional Rewards', 'Rural Solitude') THEN 'Affluent_Successful_Families'
         when mosaic_group_desc in('Small Town Diversity', 'Active Retirement', 'Elderly Needs') THEN 'Elderly_Retired'
         ELSE 'unknown' END AS segments
--         case when HHcomposition in ( 'Co-habiting couple and 1+ other adults - dependent children'
--             ,'Co-habiting couple with dependent children'
--             ,'Female Single Parent'
--             ,'Married couple and 1 other adult - dependent children'
--             ,'Married couple and 2+ other adults - dependent children'
--             ,'Married couple with dependent children'
--             ,'Married family - grown up and dependent children at home'
--             ,'Married family - grown up children at home'
--             ,'Two person household with dependent children'
--     ) THEN 1 else 0 end as families
       -- case when tv_premiums in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                --,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN 1 ELSE 0 END AS movies
INTO #mov
FROM v066_Sky_Base_Actual
;

alter table v066_Sky_Base_Actual
add segments varchar(50);

Update  v066_Sky_Base_Actual
SET sky.segments =  s.segments
from v066_sky_base_actual sky INNER JOIN #mov s
on s.account_number = sky.account_number;

--drop table #mos;

alter table v066_Sky_Base_Actual
add (Movies_model_flag integer default 0
     ,movies_model_decile integer default 0);

update v066_Sky_Base_Actual
set Movies_model_flag = (case when atl.account_number = in1.account_number then 1 else Movies_model_flag end)
    ,movies_model_decile = ctrl_decile
from v066_Sky_Base_Actual in1
left join #ATLmovies atl
on  atl.account_number = in1.account_number

alter table v066_Sky_Base_Actual
add U_high_potential_upgraders tinyint;

update v066_Sky_Base_Actual
set U_high_potential_upgraders = (case when movies_model_flag = 1 AND movies_model_decile in(1,2,3,4) THEN 1 else 0 END)
FROM v066_Sky_Base_Actual
;

select top 100 * from v066_sky_base_actual;
select top 100 * from #mos

SELECT
count(account_number) as vol
,segments
--,Region
--,Families
--,Affluence
--,tv_package
,movies
--,box_type
--,Sky_tenure_yrs
--,Lapsed_movies
,U_High_potential_HD
,U_high_potential_upgraders
,HD
,CQM_grouping
FROM v066_Sky_Base_Actual
GROUP BY segments
--,Region
--,Families
--,Affluence
--,tv_package
,movies
--,box_type
--,Sky_tenure_yrs
--,Lapsed_movies
,U_High_potential_HD
,U_high_potential_upgraders
,HD
,CQM_grouping
;

select distinct(segments) from v066_sky_base_actual
---Sky Base
SELECT  lapsed_movies
        ,COUNT(CASE WHEN tv_premiums not in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN ACCOUNT_NUMBER ELSE NULL END) AS Non_movies_subs
        ,COUNT(CASE WHEN tv_premiums in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN ACCOUNT_NUMBER ELSE NULL END) AS Movies_subs
FROM v066_Sky_Base_Actual
GROUP BY lapsed_movies
;


SELECT  Sky_tenure_yrs
        ,COUNT(CASE WHEN tv_premiums not in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN ACCOUNT_NUMBER ELSE NULL END) AS Non_movies_subs
        ,COUNT(CASE WHEN tv_premiums in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN ACCOUNT_NUMBER ELSE NULL END) AS Movies_subs
FROM v066_Sky_Base_Actual
GROUP BY Sky_tenure_yrs
Order by Sky_tenure_yrs
;


---VESPA SCALED BASE
SELECT  lapsed_movies
        ,COUNT(CASE WHEN tv_premiums not in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN ACCOUNT_NUMBER ELSE NULL END) AS Non_movies_subs
        ,COUNT(CASE WHEN tv_premiums in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN ACCOUNT_NUMBER ELSE NULL END) AS Movies_subs
FROM v066_allinone
GROUP BY lapsed_movies
;


SELECT  Sky_tenure_yrs
        ,COUNT(CASE WHEN tv_premiums not in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN ACCOUNT_NUMBER ELSE NULL END) AS Non_movies_subs
        ,COUNT(CASE WHEN tv_premiums in ('Single Sports Single Movies','Dual Movies','Single Movies'
                                                ,'Single Sports Dual Movies','Dual Sports Single Movies','Top Tier') THEN ACCOUNT_NUMBER ELSE NULL END) AS Movies_subs
FROM v066_allinone
GROUP BY Sky_tenure_yrs
Order by Sky_tenure_yrs
;
