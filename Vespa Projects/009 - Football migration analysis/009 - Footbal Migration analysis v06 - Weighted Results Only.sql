/*------------------------------------------------------------------------------
        Project: Vespa analysis V009 Football Migration Analysis
        Version: 01
        Created: 20111215
        Analyst: Dan Barnett
        SK Prod: 10
*/------------------------------------------------------------------------------
/*
        Purpose (From Brief)
        -------
       •	Our objective is to understand the drivers for how sport content is consumed when there is both a competitive TV landscape and a full programme of other sport content going on at the same time as the broadcast event
•	Linking to customer data, and specific around subscription and entitlement, can we identify specific and addressable causes of audience migration that can be mitigated through Sky adopting a different strategy?
o	Multiroom / multi device strategy to deliver incremental pay share vs cannibalisation of existing pay share
o	Customer experience 
o	Editorial 

        SECTIONS
        --------

        PART    A - Viewing Data
                B - UK Base and Profile Data
                C - Viewing Logs
             
             
        Tables
        -------

*/

---A01 Get EPG Data for Carling Cup Match between Arsenal and Man City---

select * from sk_prod.vespa_epg_dim 
where upper(channel_name) like ('%SPORT%') 
and tx_start_datetime_utc between
'2011-11-29 16:00:00' and  '2011-11-29 23:00:00'
order by tx_start_datetime_utc

select channel_name , epg_title, tx_start_datetime_utc ,tx_end_datetime_utc from sk_prod.vespa_epg_dim 
where upper(channel_name) like ('%SPORT%') 
and tx_start_datetime_utc between
'2011-11-29 18:00:00' and  '2011-11-29 23:00:00'
order by tx_start_datetime_utc

select channel_name , epg_title, tx_start_datetime_utc ,tx_end_datetime_utc
from sk_prod.vespa_epg_dim 
where tx_start_datetime_utc between
'2011-11-29 16:00:00' and  '2011-11-29 23:00:00'
and epg_title ='Arsenal v Man City-Live'

select programme_trans_sk
from sk_prod.vespa_epg_dim 
where tx_start_datetime_utc between
'2011-11-29 16:00:00' and  '2011-11-29 23:00:00'
and epg_title ='Arsenal v Man City-Live'
;

select channel_name , epg_title, tx_start_datetime_utc ,tx_end_datetime_utc
from sk_prod.vespa_epg_dim 
where tx_start_datetime_utc between
'2011-11-29 18:00:00' and  '2011-11-29 23:00:00'
and channel_name ='BBC 1 London'
order by tx_start_datetime_utc


select channel_name , epg_title, tx_start_datetime_utc ,tx_end_datetime_utc
from sk_prod.vespa_epg_dim 
where tx_start_datetime_utc between
'2011-11-29 18:00:00' and  '2011-11-29 23:00:00'
and channel_name ='ITV1 London'
order by tx_start_datetime_utc
/*
programme_trans_sk
201111300000015098
201111300000000212
201111300000003259
201111300000002325
201111300000001177
201111300000000346
201111300000014851
201111300000013327
201111300000016803
*/

----Part A02 Viewing Data for Programme ----


--------------------------------------------------------------------------------
-- PART A02 Viewing Data
--------------------------------------------------------------------------------

/*
PART A01 - Populate all viewing data between Date of Broadcast 11th Aug and End August when Vespa Suspended--
--select * from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc


--select programme_trans_sk from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc

*/
  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2011-11-29';
SET @var_prog_period_end    = '2011-12-06';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 

-- To store all the viewing records:
create table vespa_analysts.VESPA_all_viewing_records_20111129_20111206 ( -- drop table vespa_analysts.VESPA_all_viewing_records_20111129_20111206
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
    insert into vespa_analysts.VESPA_all_viewing_records_20111129_20111206
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


--select play_back_speed , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 group by day_view order by day_view;


commit;

alter table vespa_analysts.VESPA_all_viewing_records_20111129_20111206 add live tinyint;

update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.VESPA_all_viewing_records_20111129_20111206
;
commit;

if object_id('vespa_analysts.channel_name_lookup_old') is not null drop table vespa_analysts.channel_name_lookup_old;
create table vespa_analysts.channel_name_lookup_old 
(channel varchar(90)
,channel_name_grouped varchar(90)
,channel_name_inc_hd varchar(90)
)
;

input into vespa_analysts.channel_name_lookup_old from 'G:\RTCI\Sky Projects\Vespa\Phase1b\Channel Lookup\Channel Lookup Info Phase1b.csv' format ascii;
commit;

alter table vespa_analysts.VESPA_all_viewing_records_20111129_20111206 add channel_name_inc_hd varchar(40);

update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;


--select count(*) from vespa_analysts.VESPA_all_viewing_records_20111129_20111206;
--select distinct channel_name_inc_hd from vespa_analysts.channel_name_lookup_old order by channel_name_inc_hd;

--
/*
select count(distinct subscriber_id) from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 
where programme_trans_sk in
(201111300000015098,
201111300000000212,
201111300000003259,
201111300000002325,
201111300000001177,
201111300000000346,
201111300000014851,
201111300000013327,
201111300000016803
)
;

--drop table #football_viewers;
select account_number , subscriber_id 
,sum(X_Programme_Viewed_Duration) as tot_dur
into #football_viewers from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 
where programme_trans_sk in
(201111300000015098,
201111300000000212,
201111300000003259,
201111300000002325,
201111300000001177,
201111300000000346,
201111300000014851,
201111300000013327,
201111300000016803)
group by account_number ,subscriber_id
;

select account_number
,subscriber_id
,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-11-29 05:00:00' and '2011-11-30 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-11-29 05:00:00' and  adjusted_event_start_time <'2011-11-30 04:59:59' then 1
else 0
 end) as events_2011_11_29
into #viewing_log
from vespa_analysts.VESPA_all_viewing_records_20111129_20111206
group by account_number
,subscriber_id




*/

-- add indexes to improve performance
create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20111129_20111206(subscriber_id);
create dttm index idx2 on vespa_analysts.VESPA_all_viewing_records_20111129_20111206(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.VESPA_all_viewing_records_20111129_20111206(recorded_time_utc);
create lf index idx4 on vespa_analysts.VESPA_all_viewing_records_20111129_20111206(live)
create dttm index idx5 on vespa_analysts.VESPA_all_viewing_records_20111129_20111206(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.VESPA_all_viewing_records_20111129_20111206(x_viewing_end_time);
create hng index idx7 on vespa_analysts.VESPA_all_viewing_records_20111129_20111206(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.VESPA_all_viewing_records_20111129_20111206(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.VESPA_all_viewing_records_20111129_20111206
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records
update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
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
        vespa_analysts.VESPA_all_viewing_records_20111129_20111206 base left outer join vespa_201111_max_caps caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
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
update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
    set capped_x_viewing_start_time = null
        , capped_x_viewing_end_time = null
        , capped_x_programme_viewed_duration = null
        , capped_flag = 3
    from
        vespa_201111_min_cap
    where
        capped_x_programme_viewed_duration < cap_secs 
;
commit;

--select top 500 *  from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 where capped_flag=1;

-----Remove any Records that have a capped flag of 2 or 3---

delete from vespa_analysts.VESPA_all_viewing_records_20111129_20111206
where capped_flag in (2,3)
;
commit;



---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table vespa_analysts.VESPA_all_viewing_records_20111129_20111206 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.VESPA_all_viewing_records_20111129_20111206 add viewing_record_start_time_local datetime;


alter table vespa_analysts.VESPA_all_viewing_records_20111129_20111206 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.VESPA_all_viewing_records_20111129_20111206 add viewing_record_end_time_local datetime;

update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.VESPA_all_viewing_records_20111129_20111206
;
commit;


---
update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.VESPA_all_viewing_records_20111129_20111206
;
commit;

--select top 100 * from vespa_analysts.VESPA_all_viewing_records_20111129_20111206;

update vespa_analysts.VESPA_all_viewing_records_20111129_20111206
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
from vespa_analysts.VESPA_all_viewing_records_20111129_20111206
;
commit;




--------



--------------------------------------------------------------------------------
-- PART B SETUP - BASE TABLE
--------------------------------------------------------------------------------

/*
PART B   - Sky population and sample
     B01 - Create Base
     B02 - Populate ILU variables
     B03 - Populate Sky Variables
*/

--------------------------------------------------------------- A01 - Create Base
-- B01 - Create Base

--creating a date variable to use throughout the code (Date used in for end of day prior to target date (e.g., status at end of 28th Nov)
  create variable @target_date date;
     set @target_date = '20111128';


if object_id('sky_base_2011_11_29') is not null drop table vespa_analysts.sky_base_2011_11_29;
CREATE TABLE vespa_analysts.sky_base_2011_11_29 ( -- drop table govt_region_base
         account_number                  varchar(30)   NOT NULL
         ,cb_key_household                 bigint      
         ,current_short_description      varchar(70)
         ,postcode                        varchar(10)  default 'Unknown'
        ,service_instance_id            varchar(50) 
        ,SUBSCRIPTION_SUB_TYPE          varchar(50) 
);

--alter table vespa_analysts.sky_base_2011_11_29 add cb_key_household2 bigint;
--update  vespa_analysts.sky_base_2011_11_29 drop cb_key_household;
--update vespa_analysts.sky_base_2011_11_29 rename cb_key_household2 to cb_key_household;

--select count(*) from vespa_analysts.sky_base_2011_11_29 ;

--drop  index   vespa_analysts.sky_base_2011_11_29.idx1;

create hg index idx1 on vespa_analysts.sky_base_2011_11_29(account_number);
create hg index idx2 on vespa_analysts.sky_base_2011_11_29(cb_key_household);

grant all on vespa_analysts.sky_base_2011_11_29               to public;


select account_number
        , cb_key_household
        , csh.current_short_description
        ,service_instance_id
        ,SUBSCRIPTION_SUB_TYPE
        , rank() over (partition by account_number ,SUBSCRIPTION_SUB_TYPE order by effective_from_dt, cb_row_id) as rank
into #sky_accounts -- drop table #sky_accounts
from sk_prod.cust_subs_hist as csh
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription') --the DTV + Multiroom sub Type
   and status_code in ('AC','PC')               --Active Status Codes (ACtive Block Removed)
   and effective_from_dt <= @target_date             --Start on or before 1st Jan
   and effective_to_dt > @target_date                --ends after 1st Jan
   and effective_from_dt<>effective_to_dt            --ignore all but the last thing each customer did in a day
--and cb_key_household > 0
--and cb_key_household is not null
and account_number is not null;
commit;


delete from #sky_accounts where rank>1;
commit;

create hg index idx1 on #sky_accounts(service_instance_id);

create  hg index idx2 on #sky_accounts(cb_key_household);
--select count(distinct account_number) from #sky_accounts;
--select count(account_number), count(distinct account_number), count(cb_key_household), count(distinct cb_key_household), count(*) from vespa_analysts.sky_base_2011_11_29
--10035768        10035768        10035768        9546754 10035768

insert into vespa_analysts.sky_base_2011_11_29 (account_number, cb_key_household, current_short_description,service_instance_id,SUBSCRIPTION_SUB_TYPE)
select account_number, cb_key_household, current_short_description,service_instance_id,SUBSCRIPTION_SUB_TYPE
from #sky_accounts
;
commit;

/* One off code to exclude active block customers from the file as tables previously run with these included
--alter table vespa_analysts.sky_base_2011_11_29 add ac_pc_account integer default 0;

Update vespa_analysts.sky_base_2011_11_29 base
set base.ac_pc_account = case when acc.service_instance_id is not null then 1 else 0 end
from vespa_analysts.sky_base_2011_11_29 base
left outer join #sky_accounts acc
on base.service_instance_id = acc.service_instance_id;
commit;

select ac_pc_account , count(*) from vespa_analysts.sky_base_2011_11_29 base group by ac_pc_account order by ac_pc_account

delete from vespa_analysts.sky_base_2011_11_29 where ac_pc_account = 0;
commit;
*/

--select count(distinct account_number) from vespa_analysts.sky_base_2011_11_29

-------------------------------------------------------------------------------- A02 - Populate ILU variables

-- B02 - Populate ILU variables



Update vespa_analysts.sky_base_2011_11_29 base
set base.postcode = sav.cb_address_postcode
from sk_prod.cust_single_account_view sav
where base.account_number = sav.account_number;
commit;

--10033571 Row(s) affected

--8,842,102 Row(s) affected

create  hg index idx3 on vespa_analysts.sky_base_2011_11_29(postcode);
-- add ilu variables

alter table vespa_analysts.sky_base_2011_11_29
add pty_country_code varchar(10) default 'Unknown',
add HHAfflu varchar(10) default 'Unknown',
add gov_region varchar(50) default '14. Unknown',
add lifestage varchar(50) default 'Unknown';
commit;

--Use family key and correspondent flag for ILU data to create a linking table from your table to SK_PROD.ILU

SELECT  ilu.cb_row_id
                   ,base.account_number
                   ,base.cb_key_household
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'P1' THEN 1 ELSE 0 END) as P1
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'P2' THEN 1 ELSE 0 END) as P2
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'OR' THEN 1 ELSE 0 END) as OR1
                   into #temp -- drop table #temp
              FROM  sk_prod.ilu as ilu
                        inner join vespa_analysts.sky_base_2011_11_29 as base on base.cb_key_household = ilu.cb_key_household
                                                and base.cb_key_household is not null
                                                and  base.cb_key_household > 0
          GROUP BY  ilu.cb_row_id, base.account_number, base.cb_key_household
            HAVING  P1 + P2 + OR1 > 0;
            commit;

--19804573 Row(s) affected

SELECT  cb_row_id
       ,account_number
       ,cb_key_household
       ,CASE WHEN P1 = 1  THEN 1
             WHEN P2 = 1  THEN 2
             ELSE              3
         END AS Correspondent
       ,rank() over(PARTITION BY account_number ORDER BY Correspondent asc, cb_row_id desc) as rank
 INTO  #ILU -- drop table #ilu
 FROM #temp;

--select count(*) from #ILU;

--19,804,573 Row(s) affected

DELETE FROM #ILU where rank > 1;
--10,970,234 Row(s) affected


--8,834,339

 update vespa_analysts.sky_base_2011_11_29 as bas
     set pty_country_code=sav.pty_country_code
    from sk_prod.cust_single_account_view as sav
   where bas.account_number = sav.account_number;
   commit;

--10,035,542 Row(s) affected


  update vespa_analysts.sky_base_2011_11_29
     set lifestage = case ilu.ilu_hhlifestage when  1 then '18-24 ,Left home'
                                              when  2 then '25-34 ,Single (no kids)'
                                              when  3 then '25-34 ,Couple (no kids)'
                                              when  4 then '25-34 ,Child 0-4'
                                              when  5 then '25-34 ,Child5-7'
                                              when  6 then '25-34 ,Child 8-16'
                                              when  7 then '35-44 ,Single (no kids)'
                                              when  8 then '35-44 ,Couple (no kids)'
                                              when  9 then '45-54 ,Single (no kids)'
                                              when 10 then '45-54 ,Couple (no kids)'
                                              when 11 then '35-54 ,Child 0-4'
                                              when 12 then '35-54 ,Child 5-10'
                                              when 13 then '35-54 ,Child 11-16'
                                              when 14 then '35-54 ,Grown up children at home'
                                              when 15 then '55-64 ,Not retired - single'
                                              when 16 then '55-64 ,Not retired - couple'
                                              when 17 then '55-64 ,Retired'
                                              when 18 then '65-74 ,Not retired'
                                              when 19 then '65-74 ,Retired single'
                                              when 20 then '65-74 ,Retired couple'
                                              when 21 then '75+   ,Single'
                                              when 22 then '75+   ,Couple'
                                              else         'Unknown' end
    FROM vespa_analysts.sky_base_2011_11_29 as base
       INNER JOIN #ILU on base.account_number = #ilu.account_number
       INNER JOIN sk_prod.ilu as ilu on #ilu.cb_row_id = ilu.cb_row_id;
       --inner join sk_prod.ilu as ilu on ilu.cb_key_household = base.cb_key_household;

--select lifestagevespa_analysts.sky_base_2011_11_29 

--8,834,543 Row(s) affected

/*
QA
select account_number
into #nomatch
from vespa_analysts.sky_base_2011_11_29
 where lifestage is null
--1201429 Row(s) affected


select count(*)
from #nomatch as base
        inner join #ilu nm on nm.account_number = base.account_number
--0


select count(*)
from #nomatch as base
        inner join #temp nm on nm.account_number = base.account_number
--0
*/




--add third party vars
update vespa_analysts.sky_base_2011_11_29 t1
set
HHAfflu=CASE WHEN t2.ilu_hhafflu in (1,2,3,4)  THEN 'Very Low'
             WHEN t2.ilu_hhafflu in (5,6)      THEN 'Low'
             WHEN t2.ilu_hhafflu in (7,8)      THEN 'Mid Low'
             WHEN t2.ilu_hhafflu in (9,10)     THEN 'Mid'
             WHEN t2.ilu_hhafflu in (11,12)    THEN 'Mid High'
             WHEN t2.ilu_hhafflu in (13,14,15) THEN 'High'
             WHEN t2.ilu_hhafflu in (16,17)    THEN 'Very High'
             ELSE                                   'Unknown'
        END
FROM vespa_analysts.sky_base_2011_11_29 as t1
       INNER JOIN #ILU on t1.account_number = #ilu.account_number
       INNER JOIN sk_prod.ilu as t2 on #ilu.cb_row_id = t2.cb_row_id;
       --inner join sk_prod.ilu as t2 on t2.cb_key_household = t1.cb_key_household;
commit;

--8,834,543 Row(s) affected
--add government region
update vespa_analysts.sky_base_2011_11_29 t1
set gov_region=case when reg.government_region = 'North East'               Then '01. North East'
                    when reg.government_region = 'North West'               Then '02. North West'
                    when reg.government_region = 'Yorkshire and The Humber' Then '03. Yorkshire and The Humber'
                    when reg.government_region = 'East Midlands'            Then '04. East Midlands'
                    when reg.government_region = 'West Midlands'            Then '05. West Midlands'
                    when reg.government_region = 'East of England'          Then '06. East of England'
                    when reg.government_region = 'London'                   Then '07. London'
                    when reg.government_region = 'South East'               Then '08. South East'
                    when reg.government_region = 'South West'               Then '09. South West'
                    when reg.government_region = 'Scotland'                 Then '10. Scotland'
                    when reg.government_region = 'Northern Ireland'         Then '11. Northern Ireland'
                    when reg.government_region = 'Wales'                    Then '13. Wales'
                    when trim(t1.pty_country_code) = 'ROI'                  Then '12. ROI'
                    else '14. Unknown'
               end
from sk_prod.BROADBAND_POSTCODE_EXCHANGE as reg
where replace(t1.postcode, ' ','')=replace(reg.cb_address_postcode, ' ','');
commit;



--ISBA Region

alter table vespa_analysts.sky_base_2011_11_29
add isba_tv_region varchar(20) default 'Unknown';

update vespa_analysts.sky_base_2011_11_29 t1
set isba_tv_region=sav.isba_tv_region
from sk_prod.cust_single_account_view sav
where t1.account_number=sav.account_number;
commit;

--10,035,542 Row(s) affected

create lf index idx_lifestage_lf           on vespa_analysts.sky_base_2011_11_29(lifestage);
create lf index idx_hhafflu_lf             on vespa_analysts.sky_base_2011_11_29(hhafflu);
create lf index idx_isba_lf                on vespa_analysts.sky_base_2011_11_29(isba_tv_region);

-- Add Sample flags for vespa data

Alter table vespa_analysts.sky_base_2011_11_29
add vespa_flag tinyint default 0;
/*
Update vespa_analysts.sky_base_2011_11_29
   set vespa_flag = 1
from vespa_analysts.sky_base_2011_11_29 base
    inner join jchung.session_capping_dataset as scd on scd.account_number = base.account_number
commit;
*/

--354525 Row(s) affected

----------------------------------------------------------------------------------B03 - Populate Sky Variables

-- B03 - Populate Sky Variables


Alter table vespa_analysts.sky_base_2011_11_29
add pvr tinyint default 0,
add box_type varchar(2) default 'SD',
add primary_box bit default 0,
add package varchar(30) default 'Basic';
commit;


--Add on box details – most recent dw_created_dt for a box (where a box hasn’t been replaced at that date)  taken from cust_set_top_box.  
--This removes instances where more than one box potentially live for a subscriber_id at a time (due to null box installed and replaced dates).

SELECT account_number
,service_instance_id
,max(dw_created_dt) as max_dw_created_dt
  INTO #boxes -- drop table #boxes
  FROM sk_prod.CUST_SET_TOP_BOX  
 WHERE (box_installed_dt <= cast('2011-08-10'  as date) 
   AND box_replaced_dt   > cast('2011-08-10'  as date)) or box_installed_dt is null
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


alter table vespa_analysts.sky_base_2011_11_29 add x_pvr_type  varchar(50);
alter table vespa_analysts.sky_base_2011_11_29 add x_box_type  varchar(20);
alter table vespa_analysts.sky_base_2011_11_29 add x_description  varchar(100);
alter table vespa_analysts.sky_base_2011_11_29 add x_manufacturer  varchar(50);
alter table vespa_analysts.sky_base_2011_11_29 add x_model_number  varchar(50);

update  vespa_analysts.sky_base_2011_11_29
set x_pvr_type=b.pvr_type
,x_box_type=b.box_type

,x_description=b.description_x
,x_manufacturer=b.manufacturer
,x_model_number=b.model_number
from vespa_analysts.sky_base_2011_11_29 as a
left outer join #boxes_with_model_info as b
on a.service_instance_id=b.service_instance_id
;
commit;

update vespa_analysts.sky_base_2011_11_29
set pvr =case when x_pvr_type like '%PVR%' then 1 else 0 end
,box_type =case when x_box_type like '%HD%' then 'HD' else 'SD' end
from vespa_analysts.sky_base_2011_11_29
;

--exec gen_create_table 'sk_prod.cust_set_top_box'
--21,501,248 Row(s) affected

--10019501 Row(s) affected
--QA
--select pvr,box_type,count(*) as cow from vespa_analysts.sky_base_2011_11_29 group by  pvr,box_type

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
alter table vespa_analysts.sky_base_2011_11_29 add subscriber_id bigint;

update vespa_analysts.sky_base_2011_11_29
set subscriber_id=b.subscriberid
from vespa_analysts.sky_base_2011_11_29 as a
left outer join #subs_details as b
on a.service_instance_id=b.src_system_id
;
commit;



--select primary_box , count(*) from vespa_analysts.sky_base_2011_11_29 group by primary_box 
--283468 Row(s) affected

--select top 100 * from sk_prod.vespa_stb_log_snapshot;


--package
  update vespa_analysts.sky_base_2011_11_29
     set package = case when cel.prem_sports = 2 and cel.prem_movies = 2 then 'Top Tier'
                        when cel.prem_sports = 2 and cel.prem_movies = 0 then 'Dual Sports'
                        when cel.prem_sports = 0 and cel.prem_movies = 2 then 'Dual Movies'
                        when cel.prem_sports = 1 and cel.prem_movies = 0 then 'Single Sports'
                        when cel.prem_sports = 0 and cel.prem_movies = 1 then 'Single Movies'
                        when cel.prem_sports > 0 or  cel.prem_movies > 0 then 'Other Premiums'
                        else                                                  'Basic' end
    from vespa_analysts.sky_base_2011_11_29                     as bas
         inner join sk_prod.cust_entitlement_lookup as cel on bas.current_short_description = cel.short_description
--   where bas.account_number = csh.account_number;


 --10035720 Row(s) affected

create lf index idx_pvr_lf           on vespa_analysts.sky_base_2011_11_29(pvr);
create lf index idx_box_type_lf      on vespa_analysts.sky_base_2011_11_29(box_type);
create lf index idx_package_lf       on vespa_analysts.sky_base_2011_11_29(package);
/*
alter table vespa_analysts.sky_base_2011_11_29
add hh_package_rank integer default 0;
*/

----Add on Account Type (to only keep UK standard accounts)

alter table  vespa_analysts.sky_base_2011_11_29 add uk_standard_account tinyint default 0;

update  vespa_analysts.sky_base_2011_11_29
set uk_standard_account =case when b.acct_type='Standard' and b.account_number <>'?' and b.pty_country_code ='GBR' then 1 else 0 end
from  vespa_analysts.sky_base_2011_11_29 as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;

commit;

--Remove non-uk subscriptions to create the ‘UK Active Base’
--select uk_standard_account , count(*) from vespa_analysts.sky_base_2011_11_29 group by  uk_standard_account ;
delete from  vespa_analysts.sky_base_2011_11_29 where uk_standard_account=0;

-----UPDATE PRIMARY Box Definition

--select top 100 * from vespa_analysts.sky_base_2011_11_29

update vespa_analysts.sky_base_2011_11_29
set primary_box = case when subscription_sub_type = 'DTV Extra Subscription' then 0 else 1 end
from vespa_analysts.sky_base_2011_11_29
;

commit;


--select count(*) as records,count(distinct subscriber_id) ,  count(distinct case when event_type = 'evEmptyLog' then subscriber_id else null end) from #vespa_week_combined


----PART C Add On Viewing Logs-----------------------------------------------------------
-------------------------------------------Multiple Day Union-------------------------------

--drop table #vespa_week_combined;
select vev.subscriber_id , account_number
,vev.document_creation_date
,vev.stb_log_creation_date
,vev.adjusted_event_start_time
,vev.x_adjusted_event_end_time
,vev.event_type

into #vespa_week_combined
from sk_prod.VESPA_STB_PROG_EVENTS_20111129 as vev
where
panel_id in (4,5)


union all

select  vev2.subscriber_id , account_number
,vev2.document_creation_date
,vev2.stb_log_creation_date
,vev2.adjusted_event_start_time
,vev2.x_adjusted_event_end_time
,vev2.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20111130 as vev2
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


union all

select  vev3.subscriber_id , account_number
,vev3.document_creation_date
,vev3.stb_log_creation_date
,vev3.adjusted_event_start_time
,vev3.x_adjusted_event_end_time
,vev3.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20111201 as vev3
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


union all

select  vev4.subscriber_id , account_number
,vev4.document_creation_date
,vev4.stb_log_creation_date
,vev4.adjusted_event_start_time
,vev4.x_adjusted_event_end_time
,vev4.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20111202 as vev4
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)



union all

select  vev5.subscriber_id , account_number
,vev5.document_creation_date
,vev5.stb_log_creation_date
,vev5.adjusted_event_start_time
,vev5.x_adjusted_event_end_time
,vev5.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20111203 as vev5
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)



union all

select  vev6.subscriber_id , account_number
,vev6.document_creation_date
,vev6.stb_log_creation_date
,vev6.adjusted_event_start_time
,vev6.x_adjusted_event_end_time
,vev6.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20111204 as vev6
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


union all

select  vev7.subscriber_id , account_number
,vev7.document_creation_date
,vev7.stb_log_creation_date
,vev7.adjusted_event_start_time
,vev7.x_adjusted_event_end_time
,vev7.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20111205 as vev7
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


union all

select  vev8.subscriber_id , account_number
,vev8.document_creation_date
,vev8.stb_log_creation_date
,vev8.adjusted_event_start_time
,vev8.x_adjusted_event_end_time
,vev8.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20111206 as vev8
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


;



--drop table vespa_analysts.daily_summary_by_subscriber_20110811;
commit;
select subscriber_id

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-11-29 05:00:00' and '2011-11-30 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-11-29 05:00:00' and  adjusted_event_start_time <'2011-11-30 04:59:59' then 1
else 0
 end) as events_2011_11_29

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-11-30 05:00:00' and '2011-12-01 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-11-30 05:00:00' and  adjusted_event_start_time <'2011-12-01 04:59:59' then 1
else 0
 end) as events_2011_11_30

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-12-01 05:00:00' and '2011-12-02 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-12-01 05:00:00' and  adjusted_event_start_time <'2011-12-02 04:59:59' then 1
else 0
 end) as events_2011_12_01

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-12-02 05:00:00' and '2011-12-03 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-12-02 05:00:00' and  adjusted_event_start_time <'2011-12-03 04:59:59' then 1
else 0
 end) as events_2011_12_02

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-12-03 05:00:00' and '2011-12-04 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-12-03 05:00:00' and  adjusted_event_start_time <'2011-12-04 04:59:59' then 1
else 0
 end) as events_2011_12_03

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-12-04 05:00:00' and '2011-12-05 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-12-04 05:00:00' and  adjusted_event_start_time <'2011-12-05 04:59:59' then 1
else 0
 end) as events_2011_12_04

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-12-05 05:00:00' and '2011-12-06 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-12-05 05:00:00' and  adjusted_event_start_time <'2011-12-06 04:59:59' then 1
else 0
 end) as events_2011_12_05

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-12-06 05:00:00' and '2011-12-07 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-12-06 05:00:00' and  adjusted_event_start_time <'2011-12-07 04:59:59' then 1
else 0
 end) as events_2011_12_06

into vespa_analysts.daily_summary_by_subscriber_20111129
from #vespa_week_combined
group by subscriber_id
;

--select sum(events_2011_12_06) from vespa_analysts.daily_summary_by_subscriber_20111129;
commit;


---Append Number of distinct days returning viewing data (needs to be 8 for full data)----






--create hg index idx1 on vespa_analysts.daily_summary_by_subscriber_20110811(subscriber_id);
alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data integer;

alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data_2011_11_29 integer;
alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data_2011_11_30 integer;
alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data_2011_12_01 integer;
alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data_2011_12_02 integer;
alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data_2011_12_03 integer;
alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data_2011_12_04 integer;
alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data_2011_12_05 integer;
alter table vespa_analysts.sky_base_2011_11_29 add days_returning_data_2011_12_06 integer;



--alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_06_20 integer;

update vespa_analysts.sky_base_2011_11_29
set days_returning_data=case when events_2011_12_06 is null then 0 else events_2011_11_29+events_2011_11_30+events_2011_12_01+events_2011_12_02+events_2011_12_03+events_2011_12_04+events_2011_12_05+events_2011_12_06 end 
,days_returning_data_2011_11_29=case when events_2011_11_29 is null then 0 else events_2011_11_29 end 
,days_returning_data_2011_11_30=case when events_2011_11_30 is null then 0 else events_2011_11_30 end 
,days_returning_data_2011_12_01=case when events_2011_12_01 is null then 0 else events_2011_12_01 end 
,days_returning_data_2011_12_02=case when events_2011_12_02 is null then 0 else events_2011_12_02 end 
,days_returning_data_2011_12_03=case when events_2011_12_03 is null then 0 else events_2011_12_03 end 
,days_returning_data_2011_12_04=case when events_2011_12_04 is null then 0 else events_2011_12_04 end 
,days_returning_data_2011_12_05=case when events_2011_12_05 is null then 0 else events_2011_12_05 end 
,days_returning_data_2011_12_06=case when events_2011_12_06 is null then 0 else events_2011_12_06 end 
from vespa_analysts.sky_base_2011_11_29 as a
left outer join vespa_analysts.daily_summary_by_subscriber_20111129 as b
on a.subscriber_id=b.subscriber_id
;
commit;

--select sum(events_2011_11_29) from vespa_analysts.daily_summary_by_subscriber_20111129;

-----Part C2  Add weighting for Boxes----

---------------------------------------------------------------------------------- B02 - Work out size of sample for each combination

-- C2 - Work out size of sample for each combination


if object_id('vespa_analysts.stratifiedsampling_20111129') is not null drop table vespa_analysts.stratifiedsampling_20111129;
CREATE TABLE vespa_analysts.stratifiedsampling_20111129 ( -- drop table vespa_analysts.stratifiedsampling_20111129
         lifestage          varchar(50)
         ,HHAfflu           varchar(10)
         ,isba_tv_region    varchar(20)
         ,pvr               tinyint
         ,package           varchar(30)
        ,primary_box        integer
  --      ,HD_sub                 integer
         ,boxes             integer
         ,box_data_20111129         integer
         ,box_data_20111130         integer
         ,box_data_20111201         integer
         ,box_data_20111202         integer
         ,box_data_20111203         integer
         ,box_data_20111204         integer
         ,box_data_20111205         integer
         ,box_data_20111206         integer

         )

commit;
grant select, insert, delete, update on  vespa_analysts.stratifiedsampling_20111129 to greenj, dbarnett, jacksons, stafforr, bthakrar, sbednaszynski, jchung, smoore , vespa_analysts;

commit;

Insert into vespa_analysts.stratifiedsampling_20111129 (
         lifestage
         ,HHAfflu
         ,isba_tv_region
         ,pvr
         ,package
        ,primary_box
--        ,hd_sub
         ,boxes
         ,box_data_20111129         
         ,box_data_20111130         
         ,box_data_20111201         
         ,box_data_20111202         
         ,box_data_20111203         
         ,box_data_20111204         
         ,box_data_20111205         
         ,box_data_20111206         
)
select
          Lifestage
         ,HHAfflu
         ,isba_tv_region
         ,pvr
         ,package
        ,primary_box
--        ,hd_sub
         ,count(*) as boxes
         ,sum(days_returning_data_2011_11_29) as box_data_20111129         
         ,sum(days_returning_data_2011_11_30) as box_data_20111130         
         ,sum(days_returning_data_2011_12_01) as box_data_20111201         
         ,sum(days_returning_data_2011_12_02) as box_data_20111202         
         ,sum(days_returning_data_2011_12_03) as box_data_20111203         
         ,sum(days_returning_data_2011_12_04) as box_data_20111204         
         ,sum(days_returning_data_2011_12_05) as box_data_20111205         
         ,sum(days_returning_data_2011_12_06) as box_data_20111206         

from vespa_analysts.sky_base_2011_11_29 pb
group by lifestage, HHAfflu, isba_tv_region, pvr, package ,primary_box
--,hd_sub   
order by lifestage, HHAfflu, isba_tv_region, pvr, package ,primary_box
--,hd_sub;
commit;

--select * from vespa_analysts.stratifiedsampling_20111129 order by boxes desc;
--select count(*) from vespa_analysts.stratifiedsampling_20111129 ;
--select sum(boxes) as all_boxes_on_base, sum(box_data_20111129) as boxes_returning_data from vespa_analysts.stratifiedsampling_20111129 ;

--select top 100 * from vespa_analysts.sky_base_v2_2011_11_29;


---Add weightings figures to Base details---

alter table vespa_analysts.sky_base_2011_11_29 add weight_2011_11_29 integer;


alter table vespa_analysts.sky_base_2011_11_29 add weight_2011_11_30 integer;
alter table vespa_analysts.sky_base_2011_11_29 add weight_2011_12_01 integer;
alter table vespa_analysts.sky_base_2011_11_29 add weight_2011_12_02 integer;
alter table vespa_analysts.sky_base_2011_11_29 add weight_2011_12_03 integer;
alter table vespa_analysts.sky_base_2011_11_29 add weight_2011_12_04 integer;
alter table vespa_analysts.sky_base_2011_11_29 add weight_2011_12_05 integer;
alter table vespa_analysts.sky_base_2011_11_29 add weight_2011_12_06 integer;

update vespa_analysts.sky_base_2011_11_29 
set weight_2011_11_29=case  when box_data_20111129 =0 then 0 
                            when box_data_20111129 is null then 0
                            else boxes/box_data_20111129 end
,weight_2011_11_30=case  when box_data_20111130 =0 then 0 
                            when box_data_20111130 is null then 0
                            else boxes/box_data_20111130 end
,weight_2011_12_01=case  when box_data_20111201 =0 then 0 
                            when box_data_20111201 is null then 0
                            else boxes/box_data_20111201 end
,weight_2011_12_02=case  when box_data_20111202 =0 then 0 
                            when box_data_20111202 is null then 0
                            else boxes/box_data_20111202 end
,weight_2011_12_03=case  when box_data_20111203 =0 then 0 
                            when box_data_20111203 is null then 0
                            else boxes/box_data_20111203 end
,weight_2011_12_04=case  when box_data_20111204 =0 then 0 
                            when box_data_20111204 is null then 0
                            else boxes/box_data_20111204 end
,weight_2011_12_05=case  when box_data_20111205 =0 then 0 
                            when box_data_20111205 is null then 0
                            else boxes/box_data_20111205 end
,weight_2011_12_06=case  when box_data_20111206 =0 then 0 
                            when box_data_20111206 is null then 0
                            else boxes/box_data_20111206 end
from vespa_analysts.sky_base_2011_11_29 as a
left outer join vespa_analysts.stratifiedsampling_20111129 as b
on a.lifestage =b.lifestage
and a.HHAfflu=b.HHAfflu
and a.isba_tv_region =b.isba_tv_region
and a.pvr=b.pvr
and a.package =b.package
and a.primary_box=b.primary_box
--and a.hd_sub=b.hd_sub
;

--select top 100 * from vespa_analysts.stratifiedsampling_20111129 ;

--
/*
select weight_2011_11_29 , count(*) , sum(case when box_data_20111129 >0 then 1 else 0 end) from vespa_analysts.sky_base_2011_11_29 as a
left outer join vespa_analysts.stratifiedsampling_20111129 as b
on a.lifestage =b.lifestage
and a.HHAfflu=b.HHAfflu
and a.isba_tv_region =b.isba_tv_region
and a.pvr=b.pvr
and a.package =b.package
and a.primary_box=b.primary_box
group by weight_2011_11_29 
order by weight_2011_11_29 ;
*/



--select top 100 * from  vespa_analysts.sky_base_2011_11_29 where weight_2011_11_29 is null;

--select primary_box ,count(*) as boxes,sum(days_returning_data_2011_11_29) from  vespa_analysts.sky_base_2011_11_29 group by primary_box order by primary_box;
--select isba_tv_region ,count(*) as boxes,sum(days_returning_data_2011_11_29) from  vespa_analysts.sky_base_2011_11_29 group by isba_tv_region order by isba_tv_region;
--select package ,primary_box,count(*) as boxes,sum(days_returning_data_2011_11_29) from  vespa_analysts.sky_base_2011_11_29 group by package ,primary_box order by package ,primary_box;
--select pvr ,primary_box,count(*) as boxes,sum(days_returning_data_2011_11_29) from  vespa_analysts.sky_base_2011_11_29 group by pvr ,primary_box order by pvr,primary_box;


----Run Weighted by channel---

create variable @min_tx_start_time_local datetime;
create variable @max_tx_end_time_local datetime;
create variable @minute datetime;
set @min_tx_start_time_local = cast ('2011-11-29 18:00:00' as datetime);
set @max_tx_end_time_local = cast ('2011-11-29 23:00:00' as datetime);



--select @min_tx_start_time;
--select @max_tx_end_time_local;

---Loop by Channel---
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;

--select channel_name_inc_hd , count(*) from  vespa_analysts.VESPA_all_viewing_records_20110811_20110818 group by channel_name_inc_hd
--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20111129_20111206;
--select count(*) from vespa_analysts.All_viewing_minute_by_minute_20110811;
if object_id('vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel') is not null drop table vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel;
commit;
create table vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel
(
subscriber_id  bigint           null
,channel_name_inc_hd  varchar(40)
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live          smallint            not null
,seconds_viewed_in_minute_playback_within_163_hours          smallint            not null
,weighted_boxes bigint NULL

);
commit;

--select max(minute) from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel;

---Start of Loop
--drop table vespa_analysts.All_viewing_minute_by_minute_unweighted_20111129;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel
    select a.subscriber_id
    ,channel_name_inc_hd
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

,max(case   when cast (Adjusted_Event_Start_Time as date) ='2011-11-29' then weight_2011_11_29
            when cast (Adjusted_Event_Start_Time as date) ='2011-11-30' then weight_2011_11_30
            when cast (Adjusted_Event_Start_Time as date) ='2011-12-01' then weight_2011_12_01
            when cast (Adjusted_Event_Start_Time as date) ='2011-12-02' then weight_2011_12_02
            when cast (Adjusted_Event_Start_Time as date) ='2011-12-03' then weight_2011_12_03
            when cast (Adjusted_Event_Start_Time as date) ='2011-12-04' then weight_2011_12_04
            when cast (Adjusted_Event_Start_Time as date) ='2011-12-05' then weight_2011_12_05
            when cast (Adjusted_Event_Start_Time as date) ='2011-12-06' then weight_2011_12_06 else 0 end) as weighted_boxes


from  vespa_analysts.VESPA_all_viewing_records_20111129_20111206 as a
left outer join vespa_analysts.sky_base_2011_11_29 as b
on  a.subscriber_id=b.subscriber_id
where  (play_back_speed is null or play_back_speed = 2) and (
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute)))
    group by a.subscriber_id
    ,channel_name_inc_hd
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

--select top 100 * from vespa_analysts.VESPA_all_viewing_records_20111129_20111206;


---Add Index on for subscriber_id
create hg index idx1 on vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel(subscriber_id);
commit;
----Munute by Minute By Channel Output----
select channel_name_inc_hd
,minute
,count(*) as boxes
,sum(case when seconds_viewed_in_minute_live+seconds_viewed_in_minute >=31 then weighted_boxes else 0 end) as total_households

,sum(case   
            when seconds_viewed_in_minute_live+seconds_viewed_in_minute >=31 and seconds_viewed_in_minute_live>seconds_viewed_in_minute_playback_within_163_hours 
            then weighted_boxes else 0 end) as total_households_live

,sum(case   
            when seconds_viewed_in_minute_live+seconds_viewed_in_minute >=31 and seconds_viewed_in_minute_live<=seconds_viewed_in_minute_playback_within_163_hours
            then weighted_boxes else 0 end) as total_households_playback_163h


from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel
where channel_name_inc_hd='Sky Sports 2'
group by channel_name_inc_hd
,minute
order by channel_name_inc_hd
,minute
;
commit;
----Calculate Number of minutes watched per box per time period of programme, first minute watched (per period and overall) - Single Subscriber View


--select top 100 * from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel;
--drop table #subscriber_summary_for_match_viewing;
select subscriber_id
,min(minute) as first_minute_watched
,max(minute) as last_minute_watched
,count(minute) as number_of_minutes_watched
,min(weighted_boxes) as weighted_box_value_min
,max(weighted_boxes) as weighted_box_value_max

,min(case when seconds_viewed_in_minute_live+seconds_viewed_in_minute >=31 and seconds_viewed_in_minute_live>seconds_viewed_in_minute_playback_within_163_hours then minute else null end) as first_minute_watched_live

,min(case   when seconds_viewed_in_minute_live+seconds_viewed_in_minute >=31 and seconds_viewed_in_minute_live<=seconds_viewed_in_minute_playback_within_163_hours then minute else null end) as first_minute_watched_playback

,max(case when seconds_viewed_in_minute_live+seconds_viewed_in_minute >=31 and seconds_viewed_in_minute_live>seconds_viewed_in_minute_playback_within_163_hours then minute else null end) as last_minute_watched_live

,max(case   when seconds_viewed_in_minute_live+seconds_viewed_in_minute >=31 and seconds_viewed_in_minute_live<=seconds_viewed_in_minute_playback_within_163_hours then minute else null end) as last_minute_watched_playback

,sum(case when minute between '2011-11-29 19:30:00' and '2011-11-29 19:59:00' then 1 else 0 end) as watched_pre_match_minutes
,sum(case when minute between '2011-11-29 19:30:00' and '2011-11-29 19:59:00' and seconds_viewed_in_minute_live>seconds_viewed_in_minute_playback_within_163_hours then 1 else 0 end) as watched_pre_match_live_minutes
,sum(case when minute between '2011-11-29 19:30:00' and '2011-11-29 19:59:00' and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_live then 1 else 0 end) as watched_pre_match_playback_minutes

,sum(case when minute between '2011-11-29 20:00:00' and '2011-11-29 20:48:00' then 1 else 0 end) as watched_1st_half_minutes
,sum(case when minute between '2011-11-29 20:00:00' and '2011-11-29 20:48:00' and seconds_viewed_in_minute_live>seconds_viewed_in_minute_playback_within_163_hours then 1 else 0 end) as watched_1st_half_live_minutes
,sum(case when minute between '2011-11-29 20:00:00' and '2011-11-29 20:48:00' and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_live then 1 else 0 end) as watched_1st_half_playback_minutes

,sum(case when minute between '2011-11-29 20:49:00' and '2011-11-29 21:02:00' then 1 else 0 end) as watched_half_time_minutes
,sum(case when minute between '2011-11-29 20:49:00' and '2011-11-29 21:02:00' and seconds_viewed_in_minute_live>seconds_viewed_in_minute_playback_within_163_hours then 1 else 0 end) as watched_half_time_live_minutes
,sum(case when minute between '2011-11-29 20:49:00' and '2011-11-29 21:02:00' and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_live then 1 else 0 end) as watched_half_time_playback_minutes

,sum(case when minute between '2011-11-29 21:03:00' and '2011-11-29 21:53:00' then 1 else 0 end) as watched_2nd_half_minutes
,sum(case when minute between '2011-11-29 21:03:00' and '2011-11-29 21:53:00' and seconds_viewed_in_minute_live>seconds_viewed_in_minute_playback_within_163_hours then 1 else 0 end) as watched_2nd_half_live_minutes
,sum(case when minute between '2011-11-29 21:03:00' and '2011-11-29 21:53:00' and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_live then 1 else 0 end) as watched_2nd_half_playback_minutes

,sum(case when minute between '2011-11-29 21:54:00' and '2011-11-29 22:59:00' then 1 else 0 end) as watched_post_match_minutes
,sum(case when minute between '2011-11-29 21:54:00' and '2011-11-29 22:59:00' and seconds_viewed_in_minute_live>seconds_viewed_in_minute_playback_within_163_hours then 1 else 0 end) as watched_post_match_live_minutes
,sum(case when minute between '2011-11-29 21:54:00' and '2011-11-29 22:59:00' and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_live then 1 else 0 end) as watched_post_match_playback_minutes

into #subscriber_summary_for_match_viewing
from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel
where  seconds_viewed_in_minute_live+seconds_viewed_in_minute >=31 and 
       channel_name_inc_hd ='Sky Sports 2' and minute between '2011-11-29 19:30:00' and '2011-11-29 22:59:00'
      and weighted_boxes is not null
group by subscriber_id
;


--select count(*) from  #subscriber_summary_for_match_viewing where first_minute_watched_live is null and first_minute_watched_playback is null
--select * from  #subscriber_summary_for_match_viewing where first_minute_watched is null and 
--select count(*) , sum(case when weighted_box_value_min is null then 1 else 0 end) as no_weight from  #subscriber_summary_for_match_viewing


--select top 500 * from #subscriber_summary_for_match_viewing;

select first_minute_watched
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_summary_for_match_viewing
group by first_minute_watched
order by first_minute_watched
;

select first_minute_watched_live
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_summary_for_match_viewing
group by first_minute_watched_live
order by first_minute_watched_live
;


select first_minute_watched_playback
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_summary_for_match_viewing
group by first_minute_watched_playback
order by first_minute_watched_playback
;


select first_minute_watched_playback
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

,sum(case when first_minute_watched_live is null then 1 when first_minute_watched_playback<first_minute_watched_live then 1 else 0 end) as boxes_where_playback_watched_first
,sum(case when first_minute_watched_live is null then weighted_box_value_min when first_minute_watched_playback<first_minute_watched_live then weighted_box_value_min else 0 end) as weighted_boxes_where_playback_watched_first

from #subscriber_summary_for_match_viewing
where first_minute_watched_playback is not null
group by first_minute_watched_playback
order by first_minute_watched_playback
;

commit;

---Repeat but for last minute watched

select last_minute_watched
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_summary_for_match_viewing
group by last_minute_watched
order by last_minute_watched
;

select last_minute_watched_live
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_summary_for_match_viewing
group by last_minute_watched_live
order by last_minute_watched_live
;

select last_minute_watched_playback
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

--,sum(case when last_minute_watched_live is null then 1 when last_minute_watched_playback<last_minute_watched_live then 1 else 0 end) as boxes_where_playback_watched_last
--,sum(case when last_minute_watched_live is null then weighted_box_value_min when last_minute_watched_playback<last_minute_watched_live then weighted_box_value_min else 0 end) as weighted_boxes_where_playback_watched_last

from #subscriber_summary_for_match_viewing
where last_minute_watched_playback is not null
group by last_minute_watched_playback
order by last_minute_watched_playback
;

commit;


----First and Last Minute Watched---

select first_minute_watched , last_minute_watched
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_summary_for_match_viewing
group by first_minute_watched , last_minute_watched
order by first_minute_watched , last_minute_watched
;


---Distinct Viewers per period----


select sum(case when watched_pre_match_live_minutes>0 then 1 else 0 end) as pre_match_boxes
,sum(case when watched_1st_half_live_minutes>0 then 1 else 0 end) as first_half_boxes
,sum(case when watched_half_time_live_minutes>0 then 1 else 0 end) as half_time_boxes
,sum(case when watched_2nd_half_live_minutes>0 then 1 else 0 end) as second_half_boxes
,sum(case when watched_post_match_live_minutes>0 then 1 else 0 end) as post_match_boxes
,sum(case when  watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes>0 then 1 else 0 end) as any_of_match_boxes

,sum(case when watched_pre_match_live_minutes>0 then weighted_box_value_min else 0 end) as pre_match_weighted_boxes
,sum(case when watched_1st_half_live_minutes>0 then weighted_box_value_min else 0 end) as first_half_weighted_boxes
,sum(case when watched_half_time_live_minutes>0 then weighted_box_value_min else 0 end) as half_time_weighted_boxes
,sum(case when watched_2nd_half_live_minutes>0 then weighted_box_value_min else 0 end) as second_half_weighted_boxes
,sum(case when watched_post_match_live_minutes>0 then weighted_box_value_min else 0 end) as post_match_weighted_boxes
,sum(case when  watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes>0 then weighted_box_value_min else 0 end) as any_of_match_weighted_boxes
from #subscriber_summary_for_match_viewing
;

---Number of Events for Programmes (Live Viewing Only)

---Can use adjusted_event_start_time in this example as UTC and Local Time are the same time in Nov---
--drop table #subscriber_events_summary;
select a.subscriber_id
,weighted_box_value_min
,watched_pre_match_live_minutes
,watched_1st_half_live_minutes
,watched_half_time_live_minutes
,watched_2nd_half_live_minutes
,watched_post_match_live_minutes
,count(*) as viewing_events
,sum(case when adjusted_event_start_time <'2011-11-29 19:30:00' then 1 else 0 end) as event_start_pre_programme
,sum(case when adjusted_event_start_time between '2011-11-29 19:30:00' and '2011-11-29 19:59:59' then 1 else 0 end) as event_start_pre_match
,sum(case when adjusted_event_start_time between '2011-11-29 20:00:00' and '2011-11-29 20:48:59' then 1 else 0 end) as event_start_1st_half
,sum(case when adjusted_event_start_time between '2011-11-29 20:49:00' and '2011-11-29 21:02:59' then 1 else 0 end) as event_start_half_time
,sum(case when adjusted_event_start_time between '2011-11-29 21:03:00' and '2011-11-29 21:53:59' then 1 else 0 end) as event_start_second_half
,sum(case when adjusted_event_start_time between '2011-11-29 21:54:00' and '2011-11-29 22:59:59' then 1 else 0 end) as event_start_post_match

--subscriber_id
into #subscriber_events_summary
from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 as a
left outer join #subscriber_summary_for_match_viewing as b
on a.subscriber_id =  b.subscriber_id
where programme_trans_sk in (201111300000015098,
201111300000000212,
201111300000003259,
201111300000002325,
201111300000001177,
201111300000000346,
201111300000014851,
201111300000013327,
201111300000016803)
and  watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes>0
and play_back_speed is null and channel_name_inc_hd ='Sky Sports 2'
group by  a.subscriber_id
,weighted_box_value_min
,watched_pre_match_live_minutes
,watched_1st_half_live_minutes
,watched_half_time_live_minutes
,watched_2nd_half_live_minutes
,watched_post_match_live_minutes
;

commit;
---Number of Viewing Events overall

select viewing_events
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

,sum(case when watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes<=4 then weighted_box_value_min else 0 end) as Under_05_minutes_of_programme_watched

,sum(case when watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes between 5 and 14 then weighted_box_value_min else 0 end) as Between_05_14_minutes_of_programme_watched

,sum(case when watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes between 15 and 29 then weighted_box_value_min else 0 end) as Between_15_29_minutes_of_programme_watched

,sum(case when watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes between 30 and 59 then weighted_box_value_min else 0 end) as Between_30_59_minutes_of_programme_watched

,sum(case when watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes >=60 then weighted_box_value_min else 0 end) as sixty_plus_minutes_of_programme_watched
from #subscriber_events_summary
group by viewing_events
order by viewing_events
;


---Number of Viewing Events in each section

select event_start_pre_programme
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_events_summary
group by event_start_pre_programme
order by event_start_pre_programme
;

select event_start_pre_match
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_events_summary
group by event_start_pre_match
order by event_start_pre_match
;

select event_start_1st_half
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_events_summary
group by event_start_1st_half
order by event_start_1st_half
;



select event_start_half_time
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_events_summary
group by event_start_half_time
order by event_start_half_time
;


select event_start_second_half
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_events_summary
group by event_start_second_half
order by event_start_second_half
;

select event_start_post_match
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes
from #subscriber_events_summary
group by event_start_post_match
order by event_start_post_match
;

commit;





---Minutes Viewed Overall and by Period-----


select watched_pre_match_live_minutes
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by watched_pre_match_live_minutes
order by watched_pre_match_live_minutes
;


select watched_1st_half_live_minutes
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by watched_1st_half_live_minutes
order by watched_1st_half_live_minutes
;


select watched_half_time_live_minutes
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by watched_half_time_live_minutes
order by watched_half_time_live_minutes
;


select watched_2nd_half_live_minutes
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by watched_2nd_half_live_minutes
order by watched_2nd_half_live_minutes
;

select watched_post_match_live_minutes
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by watched_post_match_live_minutes
order by watched_post_match_live_minutes
;

select watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes as programme_minutes_watched
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by programme_minutes_watched
order by programme_minutes_watched
;


select watched_1st_half_live_minutes+watched_2nd_half_live_minutes as match_minutes_watched
,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by match_minutes_watched
order by match_minutes_watched
;

commit;
 

----Grouped First, Second Half and Half Time Viewing----


select case     when watched_1st_half_live_minutes = 0 then '01: None'
                when watched_1st_half_live_minutes between 1 and 15 then '02: 1-15 minutes'
                when watched_1st_half_live_minutes between 16 and 30 then '03: 16-30 minutes'
                when watched_1st_half_live_minutes >= 31  then '04: 31+ minutes' else '05: Other' end as first_half_minutes

,case     when watched_2nd_half_live_minutes = 0 then '01: None'
                when watched_2nd_half_live_minutes between 1 and 15 then '02: 1-15 minutes'
                when watched_2nd_half_live_minutes between 16 and 30 then '03: 16-30 minutes'
                when watched_2nd_half_live_minutes >= 31  then '04: 31+ minutes' else '05: Other' end as second_half_minutes

,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by first_half_minutes , second_half_minutes
order by first_half_minutes ,second_half_minutes
;



select case     when watched_1st_half_live_minutes = 0 then '01: None'
                when watched_1st_half_live_minutes between 1 and 15 then '02: 1-15 minutes'
                when watched_1st_half_live_minutes between 16 and 30 then '03: 16-30 minutes'
                when watched_1st_half_live_minutes >= 31  then '04: 31+ minutes' else '05: Other' end as first_half_minutes

,case     when watched_half_time_live_minutes = 0 then '01: None'
                when watched_half_time_live_minutes between 1 and 4 then '02: 1-4 minutes'
                when watched_half_time_live_minutes between 5 and 10 then '03: 5-10 minutes'
                when watched_half_time_live_minutes >= 11  then '04: 11+ minutes' else '05: Other' end as half_time_minutes

,case     when watched_2nd_half_live_minutes = 0 then '01: None'
                when watched_2nd_half_live_minutes between 1 and 15 then '02: 1-15 minutes'
                when watched_2nd_half_live_minutes between 16 and 30 then '03: 16-30 minutes'
                when watched_2nd_half_live_minutes >= 31  then '04: 31+ minutes' else '05: Other' end as second_half_minutes

,count(*) as boxes
,sum(weighted_box_value_min) as weighted_boxes

from #subscriber_events_summary
group by first_half_minutes ,half_time_minutes, second_half_minutes
order by first_half_minutes ,half_time_minutes,second_half_minutes
;


----How Long did people leave the channel for - Where someone has multiple live viewings what is the time between the end of one event and the start of the next---
--drop table #all_programme_viewing_events;
select a.subscriber_id
,weighted_box_value_min
,adjusted_event_start_time
,x_adjusted_event_end_time
,capped_x_viewing_start_time
,capped_x_viewing_end_time
,case when adjusted_event_start_time <'2011-11-29 19:30:00' then 1 else 0 end as event_start_pre_programme
,case when adjusted_event_start_time between '2011-11-29 19:30:00' and '2011-11-29 19:59:59' then 1 else 0 end as event_start_pre_match
,case when adjusted_event_start_time between '2011-11-29 20:00:00' and '2011-11-29 20:48:59' then 1 else 0 end as event_start_1st_half
,case when adjusted_event_start_time between '2011-11-29 20:49:00' and '2011-11-29 21:02:59' then 1 else 0 end as event_start_half_time
,case when adjusted_event_start_time between '2011-11-29 21:03:00' and '2011-11-29 21:53:59' then 1 else 0 end as event_start_second_half
,case when adjusted_event_start_time between '2011-11-29 21:54:00' and '2011-11-29 22:59:59' then 1 else 0 end as event_start_post_match
, rank() over (partition by a.subscriber_id  order by adjusted_event_start_time,x_adjusted_event_end_time) as rank
--subscriber_id
into #all_programme_viewing_events
from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 as a
left outer join #subscriber_summary_for_match_viewing as b
on a.subscriber_id =  b.subscriber_id
where programme_trans_sk in (201111300000015098,
201111300000000212,
201111300000003259,
201111300000002325,
201111300000001177,
201111300000000346,
201111300000014851,
201111300000013327,
201111300000016803)
and  watched_pre_match_live_minutes+
                watched_1st_half_live_minutes+
                watched_half_time_live_minutes+
                watched_2nd_half_live_minutes+
                watched_post_match_live_minutes>0
and play_back_speed is null and  channel_name_inc_hd ='Sky Sports 2'
order by a.subscriber_id
,weighted_box_value_min
,adjusted_event_start_time
,x_adjusted_event_end_time
;

--select top 500 * from #all_programme_viewing_events;
--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20111129_20111206;

commit;
create hg index idx1 on #all_programme_viewing_events(subscriber_id);
--select subscriber_id , max(rank) as max_rank into #sub_rank from #all_programme_viewing_events group by subscriber_id

--select max_rank , count(*) from #sub_rank group by max_rank order by max_rank;
--Match Table Back to Itself to get details of time between start of 1 and end of next event----
--drop table #next_record_per_sub;
select a.subscriber_id
,a.rank
,min (case when b.rank<=a.rank then null else b.rank end) as next_record_number
into #next_record_per_sub
from #all_programme_viewing_events as a
left outer join #all_programme_viewing_events as b
on a.subscriber_id = b.subscriber_id
group by a.subscriber_id
,a.rank
;


--select top 500 * from #next_recrod_per_sub order by subscriber_id , rank;
--drop table #time_left_and_returned_to_channel;
select a.subscriber_id
,a.weighted_box_value_min
,a.capped_x_viewing_start_time
,a.capped_x_viewing_end_time
,a.rank

,c.capped_x_viewing_start_time as next_event_viewing_start_time
,c.capped_x_viewing_end_time as next_event_viewing_end_time
,c.rank as next_event_rank
,b.next_record_number
,datediff(second, a.capped_x_viewing_end_time,next_event_viewing_start_time) as seconds_between_leaving_and_returning
into #time_left_and_returned_to_channel
from  #all_programme_viewing_events as a
left outer join #next_record_per_sub as b
on a.subscriber_id = b.subscriber_id and a.rank=b.rank
left outer join #all_programme_viewing_events as c
on b.subscriber_id = c.subscriber_id and b.next_record_number=c.rank
;

--select * from #time_left_and_returned_to_channel where seconds_between_leaving_and_returning = 0;



select case when seconds_between_leaving_and_returning = 0 then '01: 0 Seconds'
            when seconds_between_leaving_and_returning <=4 then '02: 1-4 Seconds'
            when seconds_between_leaving_and_returning <=9 then '03: 5-9 Seconds'
            when seconds_between_leaving_and_returning <=29 then '04: 10-29 Seconds'
            when seconds_between_leaving_and_returning <=59 then '05: 30-59 Seconds'
            when seconds_between_leaving_and_returning <=119 then '06: 60-119 Seconds'
            when seconds_between_leaving_and_returning <=299 then '07: 120-299 Seconds'
            when seconds_between_leaving_and_returning <=599 then '08: 300-599 Seconds' 
            when seconds_between_leaving_and_returning <=1199 then '09: 600-1199 Seconds' 
            when seconds_between_leaving_and_returning <=1799 then '10: 1200-1799 Seconds' 
            when seconds_between_leaving_and_returning <=2399 then '11: 1800-2399 Seconds' 
            when seconds_between_leaving_and_returning <=2999 then '12: 2400-2999 Seconds' 
            when seconds_between_leaving_and_returning <=3599 then '13: 3000-3599 Seconds' 
--            when seconds_between_leaving_and_returning <=1199 then '09: 600-1199 Seconds' 


else '14: 60+ Minutes'

            end  as seconds_between_leaving_and_returning_grouped
,count(*) as records
from #time_left_and_returned_to_channel
where seconds_between_leaving_and_returning>=0
group by seconds_between_leaving_and_returning_grouped
order by seconds_between_leaving_and_returning_grouped
;

---Split groups by time left---
select case when seconds_between_leaving_and_returning <=59 then '01: Under 1 Minute'
            when seconds_between_leaving_and_returning <=299 then '02: 1-5 Minutes'
            when seconds_between_leaving_and_returning <=1799 then '03: 5-30 Minutes'
else '04: Over 30 Minutes'
            end  as seconds_between_leaving_and_returning_grouped
,count(*) as records
,sum(case when  capped_x_viewing_end_time between '2011-11-29 19:30:00' and '2011-11-29 19:59:59' then 1 else 0 end) as event_start_pre_match
,sum(case when capped_x_viewing_end_time between '2011-11-29 20:00:00' and '2011-11-29 20:48:59' then 1 else 0 end) as event_start_1st_half
,sum(case when capped_x_viewing_end_time between '2011-11-29 20:49:00' and '2011-11-29 21:02:59' then 1 else 0 end) as event_start_half_time
,sum(case when capped_x_viewing_end_time between '2011-11-29 21:03:00' and '2011-11-29 21:53:59' then 1 else 0 end) as event_start_second_half
,sum(case when capped_x_viewing_end_time between '2011-11-29 21:54:00' and '2011-11-29 22:59:59' then 1 else 0 end) as event_start_post_match
from #time_left_and_returned_to_channel
where seconds_between_leaving_and_returning>0
group by seconds_between_leaving_and_returning_grouped
order by seconds_between_leaving_and_returning_grouped
;


-----Minute by Minute split for all viewers----

commit;
create hg index idx1 on #subscriber_events_summary(subscriber_id);

--select min(minute) , max(minute) from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel 

---Add Pay vs. Free Split



select channel_name_inc_hd 
--,sum(case   when seconds_viewed_in_minute_live>=31 then weighted_boxes else 0 end) as total_households_live
,sum(case   
            when seconds_viewed_in_minute_live>=31 then 1 else 0 end) as total_box_minutes_live
,count(distinct a.subscriber_id) as subs
from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel as a
left outer join #subscriber_events_summary as b
on a.subscriber_id=b.subscriber_id
where b.subscriber_id is not null and minute between '2011-11-29 19:30:00' and '2011-11-29 22:59:00'
group by channel_name_inc_hd
order by subs desc
;


--select count(*) from #subscriber_events_summary;

---Split By minute

select minute 
--,sum(case   when seconds_viewed_in_minute_live>=31 then weighted_boxes else 0 end) as total_households_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 2' then weighted_boxes else 0 end) as sky_sports_2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 1' then weighted_boxes else 0 end) as sky_sports_1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports News' then weighted_boxes else 0 end) as sky_sports_news_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC ONE' then weighted_boxes else 0 end) as BBC1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC TWO' then weighted_boxes else 0 end) as BBC2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'ITV1' then weighted_boxes else 0 end) as ITV1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Channel 4' then weighted_boxes else 0 end) as CH4_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Channel 5' then weighted_boxes else 0 end) as CH5_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky 1' then weighted_boxes else 0 end) as Sky1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'ITV2' then weighted_boxes else 0 end) as ITV2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 3' then weighted_boxes else 0 end) as sky_sports_3_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 4' then weighted_boxes else 0 end) as sky_sports_4_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC THREE' then weighted_boxes else 0 end) as BBC3_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd not in 
    ('Sky Sports 2','Sky Sports 1','Sky Sports News','BBC ONE','BBC TWO','ITV1','Channel 4','Channel 5',
     'Sky 1','ITV2','Sky Sports 3','Sky Sports 4','BBC THREE') 
    then weighted_boxes else 0 end) as Other_Channel_boxes_live

,count(distinct a.subscriber_id) as subs

,sum(case   when seconds_viewed_in_minute_live>=31 then weighted_boxes else 0 end) as total_boxes_live_viewing
from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel as a
left outer join #subscriber_events_summary as b
on a.subscriber_id=b.subscriber_id
where b.subscriber_id is not null 
--and minute between '2011-11-29 19:30:00' and '2011-11-29 22:59:00'
group by minute
order by minute
;


---Split By minute for Fully comitted Viewing Segment---

select minute 
--,sum(case   when seconds_viewed_in_minute_live>=31 then weighted_boxes else 0 end) as total_households_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 2' then weighted_boxes else 0 end) as sky_sports_2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 1' then weighted_boxes else 0 end) as sky_sports_1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports News' then weighted_boxes else 0 end) as sky_sports_news_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC ONE' then weighted_boxes else 0 end) as BBC1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC TWO' then weighted_boxes else 0 end) as BBC2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'ITV1' then weighted_boxes else 0 end) as ITV1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Channel 4' then weighted_boxes else 0 end) as CH4_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Channel 5' then weighted_boxes else 0 end) as CH5_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky 1' then weighted_boxes else 0 end) as Sky1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'ITV2' then weighted_boxes else 0 end) as ITV2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 3' then weighted_boxes else 0 end) as sky_sports_3_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 4' then weighted_boxes else 0 end) as sky_sports_4_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC THREE' then weighted_boxes else 0 end) as BBC3_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd not in 
    ('Sky Sports 2','Sky Sports 1','Sky Sports News','BBC ONE','BBC TWO','ITV1','Channel 4','Channel 5',
     'Sky 1','ITV2','Sky Sports 3','Sky Sports 4','BBC THREE') 
    then weighted_boxes else 0 end) as Other_Channel_boxes_live

,count(distinct a.subscriber_id) as subs

,sum(case   when seconds_viewed_in_minute_live>=31 then weighted_boxes else 0 end) as total_boxes_live_viewing
from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel as a
left outer join #subscriber_events_summary as b
on a.subscriber_id=b.subscriber_id
where b.subscriber_id is not null 
and watched_1st_half_live_minutes >= 31 and watched_2nd_half_live_minutes >= 31
--and minute between '2011-11-29 19:30:00' and '2011-11-29 22:59:00'
group by minute
order by minute
;

commit;
---Split By minute for Latecomers Viewing Segment---


select minute 

,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 2' then weighted_boxes else 0 end) as sky_sports_2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 1' then weighted_boxes else 0 end) as sky_sports_1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports News' then weighted_boxes else 0 end) as sky_sports_news_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC ONE' then weighted_boxes else 0 end) as BBC1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC TWO' then weighted_boxes else 0 end) as BBC2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'ITV1' then weighted_boxes else 0 end) as ITV1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Channel 4' then weighted_boxes else 0 end) as CH4_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Channel 5' then weighted_boxes else 0 end) as CH5_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky 1' then weighted_boxes else 0 end) as Sky1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'ITV2' then weighted_boxes else 0 end) as ITV2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 3' then weighted_boxes else 0 end) as sky_sports_3_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 4' then weighted_boxes else 0 end) as sky_sports_4_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC THREE' then weighted_boxes else 0 end) as BBC3_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd not in 
    ('Sky Sports 2','Sky Sports 1','Sky Sports News','BBC ONE','BBC TWO','ITV1','Channel 4','Channel 5',
     'Sky 1','ITV2','Sky Sports 3','Sky Sports 4','BBC THREE') 
    then weighted_boxes else 0 end) as Other_Channel_boxes_live

,count(distinct a.subscriber_id) as subs
,sum(case   when seconds_viewed_in_minute_live>=31 then weighted_boxes else 0 end) as total_boxes_live_viewing
from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel as a
left outer join #subscriber_events_summary as b
on a.subscriber_id=b.subscriber_id
where b.subscriber_id is not null 
and watched_1st_half_live_minutes <=15 and watched_2nd_half_live_minutes >= 16
--and minute between '2011-11-29 19:30:00' and '2011-11-29 22:59:00'
group by minute
order by minute
;


---Split By minute for Partially Interested Segment---

select minute 

,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 2' then weighted_boxes else 0 end) as sky_sports_2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 1' then weighted_boxes else 0 end) as sky_sports_1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports News' then weighted_boxes else 0 end) as sky_sports_news_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC ONE' then weighted_boxes else 0 end) as BBC1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC TWO' then weighted_boxes else 0 end) as BBC2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'ITV1' then weighted_boxes else 0 end) as ITV1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Channel 4' then weighted_boxes else 0 end) as CH4_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Channel 5' then weighted_boxes else 0 end) as CH5_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky 1' then weighted_boxes else 0 end) as Sky1_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'ITV2' then weighted_boxes else 0 end) as ITV2_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 3' then weighted_boxes else 0 end) as sky_sports_3_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'Sky Sports 4' then weighted_boxes else 0 end) as sky_sports_4_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd = 'BBC THREE' then weighted_boxes else 0 end) as BBC3_boxes_live
,sum(case when seconds_viewed_in_minute_live>=31 and channel_name_inc_hd not in 
    ('Sky Sports 2','Sky Sports 1','Sky Sports News','BBC ONE','BBC TWO','ITV1','Channel 4','Channel 5',
     'Sky 1','ITV2','Sky Sports 3','Sky Sports 4','BBC THREE') 
    then weighted_boxes else 0 end) as Other_Channel_boxes_live

,count(distinct a.subscriber_id) as subs
,sum(case   when seconds_viewed_in_minute_live>=31 then weighted_boxes else 0 end) as total_boxes_live_viewing
from vespa_analysts.All_viewing_minute_by_minute_weighted_20111129_by_channel as a
left outer join #subscriber_events_summary as b
on a.subscriber_id=b.subscriber_id
where b.subscriber_id is not null 
and watched_1st_half_live_minutes between 1 and 30 and watched_2nd_half_live_minutes <= 16
--and minute between '2011-11-29 19:30:00' and '2011-11-29 22:59:00'
group by minute
order by minute
;





case     when watched_1st_half_live_minutes = 0 then '01: None'
                when watched_1st_half_live_minutes between 1 and 15 then '02: 1-15 minutes'
                when watched_1st_half_live_minutes between 16 and 30 then '03: 16-30 minutes'
                when watched_1st_half_live_minutes >= 31  then '04: 31+ minutes' else '05: Other' end as first_half_minutes

,case     when watched_2nd_half_live_minutes = 0 then '01: None'
                when watched_2nd_half_live_minutes between 1 and 15 then '02: 1-15 minutes'
                when watched_2nd_half_live_minutes between 16 and 30 then '03: 16-30 minutes'
                when watched_2nd_half_live_minutes >= 31  then '04: 31+ minutes' else '05: Other' end as second_half_minutes



case     when watched_1st_half_live_minutes = 0 then '01: None'
                when watched_1st_half_live_minutes between 1 and 15 then '02: 1-15 minutes'
                when watched_1st_half_live_minutes between 16 and 30 then '03: 16-30 minutes'
                when watched_1st_half_live_minutes >= 31  then '04: 31+ minutes' else '05: Other' end as first_half_minutes

,case     when watched_2nd_half_live_minutes = 0 then '01: None'
                when watched_2nd_half_live_minutes between 1 and 15 then '02: 1-15 minutes'
                when watched_2nd_half_live_minutes between 16 and 30 then '03: 16-30 minutes'
                when watched_2nd_half_live_minutes >= 31  then '04: 31+ minutes' else '05: Other' end as second_half_minutes






commit;

--select top 500 * from #time_left_and_returned_to_channel order by subscriber_id , rank;

--select * from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 where subscriber_id = 83830
--select * from sk_prod.VESPA_STB_PROG_EVENTS_20111129 where subscriber_id = 83830 order by adjusted_event_start_time ,x_adjusted_event_end_time ; output to 'c:\example of zero second gap.xls' format excel;
/*
----Start time of events where still watching live at 22:59:59---

select viewing_record_end_time_local
,count(*) from vespa_analysts.VESPA_all_viewing_records_20111129_20111206
where programme_trans_sk in (201111300000015098,
201111300000000212,
201111300000003259,
201111300000002325,
201111300000001177,
201111300000000346,
201111300000014851,
201111300000013327,
201111300000016803)
group by viewing_record_end_time_local
order by viewing_record_end_time_local desc

select dateformat(viewing_record_start_time_local,'YYYY-MM-DD HH:MM') as minute_event_started
,count(*) from vespa_analysts.VESPA_all_viewing_records_20111129_20111206
where programme_trans_sk in (201111300000015098,
201111300000000212,
201111300000003259,
201111300000002325,
201111300000001177,
201111300000000346,
201111300000014851,
201111300000013327,
201111300000016803) and viewing_record_end_time_local='2011-11-29 23:00:00'
group by minute_event_started
order by minute_event_started 
;

*/


--
/*
select top 100 * from  vespa_analysts.VESPA_all_viewing_records_20111129_20111206 where programme_trans_sk in (201111300000015098,
201111300000000212,
201111300000003259,
201111300000002325,
201111300000001177,
201111300000000346,
201111300000014851,
201111300000013327,
201111300000016803);
*/
