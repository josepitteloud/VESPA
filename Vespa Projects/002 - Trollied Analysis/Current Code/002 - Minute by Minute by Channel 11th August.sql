/*------------------------------------------------------------------------------
        Project: All Viewing by Minute
        Version: 1
        Created: 20111212
        Analyst: Dan Barnett
        SK Prod: 10
*/------------------------------------------------------------------------------
/*
        Purpose (From Brief)
        -------
On Vespa, can we get someone (if available) to pull data for all viewing across all boxes please on the 11th August, using capping.  
We need to pull back all channels and total viewing

This should be 4 excel tables of channels across the top, minutes down the side with total minutes (weighted) in each box – one for live (all channels),
 one for playback (all channels), one for live (BARB reported channels), one for playback (BARB reported channels)


        SECTIONS
        --------
A01 to A03 already done previously for Trollied Analysis
        PART A - Raw Data
             A01 - Viewing Data - All viewing between 11th and 18th Aug
             A02 - Generate Active Sky Base in UK
             A03 - Generate Viewing Log Summaries      




             A04 - Add on Capped Start and end times
             A05 - Create Minute by Minute summary for viewing
             A06 - Trollied with second by second and capping metrics

        PART B - Log Data
             B01 - Days Returning Data by By Box
             
        Tables
        -------
        vespa_analysts.VESPA_Programmes_20110811
        vespa_analysts.VESPA_tmp_all_viewing_records_20110811
        vespa_analysts.uk_base_20110811

*/



--------------------------------------------------------------------------------
-- PART A01 Viewing Data
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
SET @var_prog_period_start  = '2011-08-11';
SET @var_prog_period_end    = '2011-08-18';

SET @var_cntr = 0;
SET @var_num_days =8;       -- Get events up to 30 days of the programme broadcast time (only 20 in this case due to Vespa Suspension at end August

-- To store all the viewing records:
create table VESPA_all_viewing_records_20110811_20110818 ( -- drop table VESPA_tmp_all_viewing_records_20110811
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
    insert into VESPA_all_viewing_records_20110811_20110818
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
          inner join sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction

where 
(cast(Adjusted_Event_Start_Time as date) between ''2011-08-10'' and ''2011-08-11'' or cast(Recorded_Time_UTC as date) between ''2011-08-10'' and ''2011-08-11'')
    and 
video_playing_flag = 1
     and adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'')
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


--select play_back_speed , count(*) as records from VESPA_tmp_all_viewing_records_20110811 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from VESPA_tmp_all_viewing_records_20110811 group by day_view order by day_view;


commit;



--A02 Create UK Base file (Likely to be changed at a later data to reflect work done on Stratification)
--1.    Create list of all subscriptions active (AC/PC exclude Active Block?) at start of day in question – taken from cust_subs_hist

if object_id('vespa_analysts.uk_base_20110811') is not null drop table vespa_analysts.uk_base_20110811;
select account_number
,service_instance_id
,SUBSCRIPTION_SUB_TYPE
into vespa_analysts.uk_base_20110811
from  sk_prod.cust_subs_hist 
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
and effective_from_dt<=cast('2011-08-10' as date)
and effective_to_dt>cast('2011-08-10' as date)
and status_code in ('AC','PC')
;

--select distinct SUBSCRIPTION_SUB_TYPE from sk_prod.cust_subs_hist
commit;

--2.    Add on country code – from cust_single_account_view only Include UK standard accounts e.g, exclude VIP/Staff

alter table vespa_analysts.uk_base_20110811 add uk_standard_account tinyint default 0;

update vespa_analysts.uk_base_20110811
set uk_standard_account =case when b.acct_type='Standard' and b.account_number <>'?' and b.pty_country_code ='GBR' then 1 else 0 end
from vespa_analysts.uk_base_20110811 as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;

commit;

--3.    Remove non-uk subscriptions to create the ‘UK Active Base’

delete from vespa_analysts.uk_base_20110811 where uk_standard_account=0;

commit;

---Add Index on service_instance_id--


create hg index idx1 on vespa_analysts.uk_base_20110811(service_instance_id);
commit;

--select top 5000 * into vespa_analysts.uk_base_20110811_test from vespa_analysts.uk_base_20110811;


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


alter table vespa_analysts.uk_base_20110811 add subscriber_id bigint;

update vespa_analysts.uk_base_20110811
set subscriber_id=b.subscriberid
from vespa_analysts.uk_base_20110811 as a
left outer join #subs_details as b
on a.service_instance_id=b.src_system_id
;
commit;







--select SUBSCRIPTION_SUB_TYPE, count(*) from vespa_analysts.uk_base_20110811 group by SUBSCRIPTION_SUB_TYPE;
--select  count(*),count(distinct service_instance_id) from vespa_analysts.uk_base_20110811;

--4.    Add on box details – most recent dw_created_dt for a box (where a box hasn’t been replaced at that date)  taken from cust_set_top_box.  
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


alter table vespa_analysts.uk_base_20110811 add x_pvr_type  varchar(50);
alter table vespa_analysts.uk_base_20110811 add x_box_type  varchar(20);
alter table vespa_analysts.uk_base_20110811 add x_description  varchar(100);
alter table vespa_analysts.uk_base_20110811 add x_manufacturer  varchar(50);
alter table vespa_analysts.uk_base_20110811 add x_model_number  varchar(50);

update  vespa_analysts.uk_base_20110811
set x_pvr_type=b.pvr_type
,x_box_type=b.box_type

,x_description=b.description_x
,x_manufacturer=b.manufacturer
,x_model_number=b.model_number
from vespa_analysts.uk_base_20110811 as a
left outer join #boxes_with_model_info as b
on a.service_instance_id=b.service_instance_id
;
commit;

--select distinct x_box_type from vespa_analysts.uk_base_20110811

--select distinct subscriber_id from vespa_analysts.uk_base_20110811 
---Add Package to base and viewer info
select account_number 
,min(current_short_description) as Package_code_on_day
into #package_code_by_day
FROM      sk_prod.cust_subs_hist 
where         effective_from_dt <= cast('2011-08-10' as date)
AND         effective_to_dt   > cast('2011-08-10' as date) 
AND         effective_from_dt <> effective_to_dt
AND        subscription_sub_type = 'DTV Primary Viewing'
group by account_number 
;

commit;
create hg index indx1 on #package_code_by_day (account_number);

alter table vespa_analysts.uk_base_20110811  add  Package_code_on_day     varchar(12);
--alter table dbarnett.vespa_6mth_100pc_october_programme_activity_summary_daily  delete  Package_on_day_group ;
alter table vespa_analysts.uk_base_20110811  add  Package_on_day_group     varchar(14) default '6: Unknown';

commit;
UPDATE      vespa_analysts.uk_base_20110811  a
SET         Package_code_on_day = b.Package_code_on_day 
FROM         vespa_analysts.uk_base_20110811 as a
left outer join #package_code_by_day as b
on       a.account_number = b.account_number 
;

COMMIT;
UPDATE      vespa_analysts.uk_base_20110811  a
SET         Package_on_day_group   = CASE WHEN prem_sports = 2 and prem_movies = 2       then '1: Top_tier'
                                              WHEN prem_sports = 0 and prem_movies = 2  then '3: Dual Movies'   
                                              WHEN prem_sports = 2 and prem_movies = 0  then '2: Dual Sports'    
                                              WHEN prem_sports = 0 and prem_movies = 0  then '5: No Prems'
                                              ELSE '4: Other prems' END     
FROM        sk_prod.cust_entitlement_lookup b
WHERE       a.Package_code_on_day = b.short_description;

COMMIT;




---A03 Viewing Log Data
--------------Add On Viewing Logs-----------------------------------------------------------
-------------------------------------------Multiple Day Union-------------------------------

--drop table #vespa_week_combined;
select vev.subscriber_id 
,vev.document_creation_date
,vev.stb_log_creation_date
,vev.adjusted_event_start_time
,vev.x_adjusted_event_end_time
,vev.event_type

into #vespa_week_combined
from sk_prod.VESPA_STB_PROG_EVENTS_20110811 as vev
where
panel_id=5


union all

select  vev2.subscriber_id 
,vev2.document_creation_date
,vev2.stb_log_creation_date
,vev2.adjusted_event_start_time
,vev2.x_adjusted_event_end_time
,vev2.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110812 as vev2
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id=5


union all

select  vev3.subscriber_id 
,vev3.document_creation_date
,vev3.stb_log_creation_date
,vev3.adjusted_event_start_time
,vev3.x_adjusted_event_end_time
,vev3.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110813 as vev3
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id=5


union all

select  vev4.subscriber_id 
,vev4.document_creation_date
,vev4.stb_log_creation_date
,vev4.adjusted_event_start_time
,vev4.x_adjusted_event_end_time
,vev4.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110814 as vev4
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id=5



union all

select  vev5.subscriber_id 
,vev5.document_creation_date
,vev5.stb_log_creation_date
,vev5.adjusted_event_start_time
,vev5.x_adjusted_event_end_time
,vev5.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110815 as vev5
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id=5



union all

select  vev6.subscriber_id 
,vev6.document_creation_date
,vev6.stb_log_creation_date
,vev6.adjusted_event_start_time
,vev6.x_adjusted_event_end_time
,vev6.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110816 as vev6
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id=5


union all

select  vev7.subscriber_id 
,vev7.document_creation_date
,vev7.stb_log_creation_date
,vev7.adjusted_event_start_time
,vev7.x_adjusted_event_end_time
,vev7.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110817 as vev7
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id=5


union all

select  vev8.subscriber_id 
,vev8.document_creation_date
,vev8.stb_log_creation_date
,vev8.adjusted_event_start_time
,vev8.x_adjusted_event_end_time
,vev8.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110818 as vev8
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id=5


;

--Not Working--
--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20110801;


commit;



--select top 500 * from #vespa_week_combined order by subscriber_id , adjusted_event_start_time,x_adjusted_event_end_time;




--drop table vespa_analysts.daily_summary_by_subscriber_20110811;
commit;
select subscriber_id
,min(adjusted_event_start_time) as first_event_date


,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-11 05:00:00' and '2011-08-12 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-11 05:00:00' and  adjusted_event_start_time <'2011-08-12 04:59:59' then 1
else 0
 end) as events_2011_08_11

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-12 05:00:00' and '2011-08-13 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-12 05:00:00' and  adjusted_event_start_time <'2011-08-13 04:59:59' then 1
else 0
 end) as events_2011_08_12

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-13 05:00:00' and '2011-08-14 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-13 05:00:00' and  adjusted_event_start_time <'2011-08-14 04:59:59' then 1
else 0
 end) as events_2011_08_13

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-14 05:00:00' and '2011-08-15 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-14 05:00:00' and  adjusted_event_start_time <'2011-08-15 04:59:59' then 1
else 0
 end) as events_2011_08_14

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-15 05:00:00' and '2011-08-16 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-15 05:00:00' and  adjusted_event_start_time <'2011-08-16 04:59:59' then 1
else 0
 end) as events_2011_08_15

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-16 05:00:00' and '2011-08-17 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-16 05:00:00' and  adjusted_event_start_time <'2011-08-17 04:59:59' then 1
else 0
 end) as events_2011_08_16

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-17 05:00:00' and '2011-08-18 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-17 05:00:00' and  adjusted_event_start_time <'2011-08-18 04:59:59' then 1
else 0
 end) as events_2011_08_17

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-18 05:00:00' and '2011-08-19 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-18 05:00:00' and  adjusted_event_start_time <'2011-08-19 04:59:59' then 1
else 0
 end) as events_2011_08_18
into vespa_analysts.daily_summary_by_subscriber_20110811
from #vespa_week_combined
group by subscriber_id

;

--select sum(events_2011_08_22) from vespa_analysts.daily_summary_by_subscriber_20110811;
commit;
---Append Number of distinct days returning viewing data (needs to be 8 for full data)----

create hg index idx1 on vespa_analysts.daily_summary_by_subscriber_20110811(subscriber_id);
alter table vespa_analysts.uk_base_20110811 add days_returning_data integer;

alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_08_11 integer;
alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_08_12 integer;
alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_08_13 integer;
alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_08_14 integer;
alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_08_15 integer;
alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_08_16 integer;
alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_08_17 integer;
alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_08_18 integer;



--alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_06_20 integer;

update vespa_analysts.uk_base_20110811
set days_returning_data=case when events_2011_08_18 is null then 0 else events_2011_08_11+events_2011_08_12+events_2011_08_13+events_2011_08_14+events_2011_08_15+events_2011_08_16+events_2011_08_17+events_2011_08_18 end 
,days_returning_data_2011_08_11=case when events_2011_08_11 is null then 0 else events_2011_08_11 end 
,days_returning_data_2011_08_12=case when events_2011_08_12 is null then 0 else events_2011_08_12 end 
,days_returning_data_2011_08_13=case when events_2011_08_13 is null then 0 else events_2011_08_13 end 
,days_returning_data_2011_08_14=case when events_2011_08_14 is null then 0 else events_2011_08_14 end 
,days_returning_data_2011_08_15=case when events_2011_08_15 is null then 0 else events_2011_08_15 end 
,days_returning_data_2011_08_16=case when events_2011_08_16 is null then 0 else events_2011_08_16 end 
,days_returning_data_2011_08_17=case when events_2011_08_17 is null then 0 else events_2011_08_17 end 
,days_returning_data_2011_08_18=case when events_2011_08_18 is null then 0 else events_2011_08_18 end 
from vespa_analysts.uk_base_20110811 as a
left outer join vespa_analysts.daily_summary_by_subscriber_20110811 as b
on a.subscriber_id=b.subscriber_id
;
commit;


---Update x_box_type to re-classify nulls

update vespa_analysts.uk_base_20110811
set x_box_type=case when x_box_type is null then 'UNK' else x_box_type end
from vespa_analysts.uk_base_20110811
;

alter table vespa_analysts.uk_base_20110811 add primary_sub integer;

update vespa_analysts.uk_base_20110811
set primary_sub=case when subscription_sub_type = 'DTV Primary Viewing' then 1 else 0 end
from vespa_analysts.uk_base_20110811
;
commit;
----------Create Counts by cell---
--select * from vespa_analysts.uk_base_20110811_weighting_values;
--drop table vespa_analysts.uk_base_20110811_weighting_values;
select package_on_day_group
,x_box_type
,case when subscription_sub_type = 'DTV Primary Viewing' then 1 else 0 end as primary_sub
,count(*) as accounts
,sum(case when days_returning_data=8 then 1 else 0 end) returning_data_all_days
,sum(case when days_returning_data between 1 and 7 then 1 else 0 end) returning_data_1_to_7_days
into vespa_analysts.uk_base_20110811_weighting_values
from vespa_analysts.uk_base_20110811
group by package_on_day_group
,x_box_type
,primary_sub
order by package_on_day_group
,x_box_type
,primary_sub
;

alter table vespa_analysts.uk_base_20110811_weighting_values add weighting decimal(20,5);

update vespa_analysts.uk_base_20110811_weighting_values
set weighting=case when returning_data_all_days=0 then 0 else accounts/cast(returning_data_all_days as real) end
from vespa_analysts.uk_base_20110811_weighting_values
;

commit;
--select top 100 * from vespa_analysts.uk_base_20110811_weighting_values;
--select top 100 * from vespa_analysts.uk_base_20110811;
--select * from vespa_analysts.uk_base_20110811_weighting_values;

alter table vespa_analysts.uk_base_20110811 add weighting decimal(20,5);

update vespa_analysts.uk_base_20110811
set weighting=b.weighting
from vespa_analysts.uk_base_20110811 as a
left outer join vespa_analysts.uk_base_20110811_weighting_values as b
on  a.package_on_day_group=b.package_on_day_group
    and a.x_box_type=b.x_box_type
    and a.primary_sub=b.primary_sub
;
commit;

---Add on Mix details for day (do they have Variety Package)---


--select count(*) from vespa_analysts.uk_base_20110811  where weighting is not null;


/*
-----Add on start/end of viewing activity----
delete from vespa_analysts.trollied_20110811_raw
where play_back_speed in (-60,-24,-12,-4,0,1,4,12,24,60)
;
commit;
*/



----A04 Add on Capped Start and end times

---Add flag for live/cancelled that can be used for capping

alter table VESPA_tmp_all_viewing_records_20110811 add live integer ;

update VESPA_tmp_all_viewing_records_20110811
set live = case when play_back_speed is null then 1 else 0 end 
from VESPA_tmp_all_viewing_records_20110811
;





---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table VESPA_tmp_all_viewing_records_20110811 add viewing_record_start_time_utc datetime;
alter table VESPA_tmp_all_viewing_records_20110811 add viewing_record_start_time_local datetime;


alter table VESPA_tmp_all_viewing_records_20110811 add viewing_record_end_time_utc datetime;
alter table VESPA_tmp_all_viewing_records_20110811 add viewing_record_end_time_local datetime;

update VESPA_tmp_all_viewing_records_20110811
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from VESPA_tmp_all_viewing_records_20110811
;
commit;


---
update VESPA_tmp_all_viewing_records_20110811
set viewing_record_end_time_utc= dateadd(second,x_programme_viewed_duration,viewing_record_start_time_utc)
from VESPA_tmp_all_viewing_records_20110811
;
commit;

--select top 100 * from VESPA_tmp_all_viewing_records_20110811;

update VESPA_tmp_all_viewing_records_20110811
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
from VESPA_tmp_all_viewing_records_20110811
;
commit;

alter table VESPA_tmp_all_viewing_records_20110811 add capped_end_time datetime ;


update VESPA_tmp_all_viewing_records_20110811
    set capped_end_time =
        case when recorded_time_utc is null then 
            -- if start of viewing_time is beyond start_time + cap then flag as null
             dateadd(minute, min_dur_mins, adjusted_event_start_time) 
            else dateadd(minute, min_dur_mins, recorded_time_utc) 
        end
from
        VESPA_tmp_all_viewing_records_20110811 base left outer join vespa_201108_max_caps caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )
;
commit;

--select top 500 * from VESPA_tmp_all_viewing_records_20110811



alter table VESPA_tmp_all_viewing_records_20110811 add capped_x_viewing_start_time_utc datetime ;
alter table VESPA_tmp_all_viewing_records_20110811 add capped_x_viewing_end_time_utc datetime ;


update VESPA_tmp_all_viewing_records_20110811
    set capped_x_viewing_start_time_utc = 
        case when viewing_record_start_time_utc >capped_end_time then null else 
           viewing_record_start_time_utc
        end
        , capped_x_viewing_end_time_utc =
        case when viewing_record_start_time_utc >capped_end_time then null
            when viewing_record_end_time_utc >=capped_end_time then capped_end_time else viewing_record_end_time_utc
        end
from
        VESPA_tmp_all_viewing_records_20110811 
;
commit;
--select top 500 * from VESPA_tmp_all_viewing_records_20110811 where play_back_speed =2;

--select * from vespa_analysts_gm_capping_test_dbarnett ;


alter table VESPA_tmp_all_viewing_records_20110811 add capped_x_viewing_start_time_local datetime ;
alter table VESPA_tmp_all_viewing_records_20110811 add capped_x_viewing_end_time_local datetime ;


update VESPA_tmp_all_viewing_records_20110811
set capped_x_viewing_start_time_local= case 
when dateformat(capped_x_viewing_start_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,capped_x_viewing_start_time_utc) 
when dateformat(capped_x_viewing_start_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,capped_x_viewing_start_time_utc) 
when dateformat(capped_x_viewing_start_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,capped_x_viewing_start_time_utc) 
                    else capped_x_viewing_start_time_utc  end
,capped_x_viewing_end_time_local=case 
when dateformat(capped_x_viewing_end_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,capped_x_viewing_end_time_utc) 
when dateformat(capped_x_viewing_end_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,capped_x_viewing_end_time_utc) 
when dateformat(capped_x_viewing_end_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,capped_x_viewing_end_time_utc) 
                    else capped_x_viewing_end_time_utc  end
from VESPA_tmp_all_viewing_records_20110811
;
commit;



---A05 Create Minute by Minute summary for viewing---
create variable @min_tx_start_time datetime;
create variable @max_tx_end_time datetime;

set @min_tx_start_time = (select min(tx_start_datetime_utc) from  VESPA_tmp_all_viewing_records_20110811);
set @max_tx_end_time = (select max(tx_end_datetime_utc) from  VESPA_tmp_all_viewing_records_20110811);

create variable @min_tx_start_time_local datetime;
create variable @max_tx_end_time_local datetime;
create variable @minute datetime;
set @min_tx_start_time_local = (select case 
when dateformat(@min_tx_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,@min_tx_start_time) 
when dateformat(@min_tx_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,@min_tx_start_time) 
when dateformat(@min_tx_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,@min_tx_start_time) 
                    else @min_tx_start_time  end);


set @max_tx_end_time_local = (select case 
when dateformat(@max_tx_end_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,@max_tx_end_time) 
when dateformat(@max_tx_end_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,@max_tx_end_time) 
when dateformat(@max_tx_end_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,@max_tx_end_time) 
                    else @max_tx_end_time  end);




--select @min_tx_start_time;
--select @max_tx_end_time;

---Loop by Channel---
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;

if object_id('vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811') is not null drop table vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811;
commit;
create table vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811
    select subscriber_id
    ,@minute as minute
    ,sum(case when 
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) then 60 when 
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from VESPA_tmp_all_viewing_records_20110811
where  (play_back_speed is null or play_back_speed = 2) and (
        (capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>@minute)
    or
        (capped_x_viewing_start_time_local between @minute and dateadd(second,59,@minute)))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

---Add weightings on to the minute by minute details---

alter table vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811 add weighting decimal(20,5);
alter table vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811  add days_returning_data integer;

update vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811
set weighting=b.weighting
,days_returning_data=b.days_returning_data
from vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811 as a
left outer join vespa_analysts.uk_base_20110811 as b
on  a.subscriber_id=b.subscriber_id

;
commit;

--select top 500 * from VESPA_tmp_all_viewing_records_20110811;




/*
select minute
, sum(case when seconds_viewed_in_minute>=30 then 1 else 0 end) as boxes
, sum(case when seconds_viewed_in_minute>=30 then weighting else 0 end) as weighted_boxes
from vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811
group by minute order by minute
;

select minute
, sum(case when seconds_viewed_in_minute>=30 then 1 else 0 end) as boxes
, sum(case when seconds_viewed_in_minute>=30 then weighting else 0 end) as weighted_boxes
from vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811
group by minute order by minute
;



*/

---A06 - Trollied with second by second and capping metrics-----

---Create second by second log---
create variable @programme_time_start datetime;
create variable @programme_time_end datetime;
create variable @programme_time datetime;

set @programme_time_start = cast('2011-08-11 20:30:00' as datetime);
set @programme_time_end =cast('2011-08-11 22:00:00' as datetime);
set @programme_time = @programme_time_start;

/*
--drop table vespa_analysts.manu_spurs_20110907_raw ;
select * into vespa_analysts.manu_spurs_20110907_raw 
from #sky_sports_man_u_spurs 
where right(cast(subscriber_id as varchar),2='45')
;
*/
commit;

--exec gen_create_table  'vespa_analysts.manu_spurs_20110907_raw';


commit;
--drop table vespa_analysts.trollied_20110811_second_by_second;
---Create table to insert into loop---
create table vespa_analysts.trollied_20110811_second_by_second
(

subscriber_id                       decimal(8)              not null
--,account_number                     varchar(20)             null
,second_viewed                      datetime                not null
,viewed                             smallint                not null
,viewed_live                        smallint                null
,viewed_playback                    smallint                null
,viewed_playback_within_163_hours   smallint                null

,viewed_playback_within_10_minutes                    smallint                null
,viewed_playback_within_10_30_minutes                    smallint                null
,viewed_playback_within_30_60_minutes                    smallint                null
,viewed_playback_within_1_2_hours                    smallint                null

,viewed_playback_within_2_3_hours                    smallint                null
,viewed_playback_within_3_4_hours                    smallint                null
,viewed_playback_within_4_24_hours                    smallint                null
,viewed_playback_within_1_2_days                    smallint                null


,viewed_playback_within_2_3_days                    smallint                null
,viewed_playback_within_3_4_days                    smallint                null
,viewed_playback_within_4_5_days                    smallint                null
,viewed_playback_within_5_6_days                    smallint                null
,viewed_playback_within_6_7_days                    smallint                null
,viewed_playback_within_7_14_days                    smallint                null
,viewed_playback_within_14_21_days                    smallint                null
,viewed_playback_within_21_28_days                    smallint                null



/*
,viewed_live_1hr_cap                smallint                null
,viewed_live_2hr_cap                smallint                null
,viewed_live_3hr_cap                smallint                null
,viewed_live_4hr_cap                smallint                null
,viewed_live_5hr_cap                smallint                null
,viewed_live_6hr_cap                smallint                null
*/
);
commit;

---Start of Loop
WHILE @programme_time <  @programme_time_end LOOP
insert into vespa_analysts.trollied_20110811_second_by_second
select subscriber_id
--,account_number
,@programme_time as second_viewed
,1 as viewed
,max(case when play_back_speed is null then 1 else 0 end) as viewed_live
,max(case when play_back_speed is not null then 1 else 0 end) as viewed_playback
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,163,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_163_hours
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(minute,10,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_10_minutes
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(minute,10,recorded_time_utc)<adjusted_event_start_time and dateadd(minute,30,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_10_30_minutes
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(minute,30,recorded_time_utc)<adjusted_event_start_time and dateadd(minute,60,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_30_60_minutes
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,1,recorded_time_utc)<adjusted_event_start_time and dateadd(hour,2,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_1_2_hours
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,2,recorded_time_utc)<adjusted_event_start_time and dateadd(hour,3,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_2_3_hours
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,3,recorded_time_utc)<adjusted_event_start_time and dateadd(hour,4,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_3_4_hours
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,4,recorded_time_utc)<adjusted_event_start_time and dateadd(hour,24,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_4_24_hours
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,1,recorded_time_utc)<adjusted_event_start_time and dateadd(day,2,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_1_2_days
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,2,recorded_time_utc)<adjusted_event_start_time and dateadd(day,3,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_2_3_days
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,3,recorded_time_utc)<adjusted_event_start_time and dateadd(day,4,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_3_4_days
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,4,recorded_time_utc)<adjusted_event_start_time and dateadd(day,5,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_4_5_days
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,5,recorded_time_utc)<adjusted_event_start_time and dateadd(day,6,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_5_6_days
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,6,recorded_time_utc)<adjusted_event_start_time and dateadd(day,7,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_6_7_days

,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,7,recorded_time_utc)<adjusted_event_start_time and dateadd(day,14,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_7_14_days
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,14,recorded_time_utc)<adjusted_event_start_time and dateadd(day,21,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_14_21_days
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,21,recorded_time_utc)<adjusted_event_start_time and dateadd(day,28,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_21_28_days



---Add in Capping related splits----
/*
,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap
,max(case when play_back_speed is null  and dateadd(hour,2,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_2hr_cap
,max(case when play_back_speed is null  and dateadd(hour,3,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_3hr_cap
,max(case when play_back_speed is null  and dateadd(hour,4,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_4hr_cap
,max(case when play_back_speed is null  and dateadd(hour,5,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_5hr_cap
,max(case when play_back_speed is null  and dateadd(hour,6,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_6hr_cap
*/
from VESPA_tmp_all_viewing_records_20110811
where  cast(capped_x_viewing_start_time_local as datetime)<=@programme_time and cast(capped_x_viewing_end_time_local as datetime)>@programme_time
and (play_back_speed is null or play_back_speed = 2)
group by subscriber_id
--,account_number 
,second_viewed,viewed
;

 SET @programme_time =dateadd(second,1,@programme_time);
    COMMIT;

END LOOP;
commit;

---Time between View and Playback----
/*
select dateformat(dateadd(hh,1,adjusted_event_start_time),'YYYY-MM-DD HH') as hour_event_start
,
sum(case when play_back_speed is not null then 1 else 0 end) as viewed_playback
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,163,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_163_hours
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(minute,10,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_10_minutes
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(minute,10,recorded_time_utc)<adjusted_event_start_time and dateadd(minute,30,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_10_30_minutes
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(minute,30,recorded_time_utc)<adjusted_event_start_time and dateadd(minute,60,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_30_60_minutes
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,1,recorded_time_utc)<adjusted_event_start_time and dateadd(hour,2,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_1_2_hours
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,2,recorded_time_utc)<adjusted_event_start_time and dateadd(hour,3,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_2_3_hours
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,3,recorded_time_utc)<adjusted_event_start_time and dateadd(hour,4,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_3_4_hours
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,4,recorded_time_utc)<adjusted_event_start_time and dateadd(hour,24,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_4_24_hours
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,1,recorded_time_utc)<adjusted_event_start_time and dateadd(day,2,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_1_2_days
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,2,recorded_time_utc)<adjusted_event_start_time and dateadd(day,3,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_2_3_days
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,3,recorded_time_utc)<adjusted_event_start_time and dateadd(day,4,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_3_4_days
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,4,recorded_time_utc)<adjusted_event_start_time and dateadd(day,5,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_4_5_days
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,5,recorded_time_utc)<adjusted_event_start_time and dateadd(day,6,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_5_6_days
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,6,recorded_time_utc)<adjusted_event_start_time and dateadd(day,7,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_6_7_days

,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,7,recorded_time_utc)<adjusted_event_start_time and dateadd(day,14,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_7_14_days
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,14,recorded_time_utc)<adjusted_event_start_time and dateadd(day,21,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_14_21_days
,sum(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(day,21,recorded_time_utc)<adjusted_event_start_time and dateadd(day,28,recorded_time_utc)>adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_21_28_days

,count(*) as records
from VESPA_tmp_all_viewing_records_20110811
where recorded_time_utc is not null
group by hours_difference
order by hours_difference
;
*/


---Second by Second Output---
--select top 100 * from  vespa_analysts.trollied_20110811_second_by_second;

--Unweighted Total---

select second_viewed
,sum(viewed) as total_boxes_viewing
,sum(case when viewed_playback=1 then 0 else viewed_live end) as total_boxes_live
,sum(viewed_playback) as total_boxes_playback
,sum(case when viewed_playback_within_163_hours=1 then 1 else viewed_live end) as total_boxes_viewing_within_barb_window

,sum(viewed_playback_within_10_minutes)                                   
,sum(viewed_playback_within_10_30_minutes)                                   
,sum(viewed_playback_within_30_60_minutes)                                   
,sum(viewed_playback_within_1_2_hours)                                   

,sum(viewed_playback_within_2_3_hours)                                   
,sum(viewed_playback_within_3_4_hours)                                   
,sum(viewed_playback_within_4_24_hours)                                   
,sum(viewed_playback_within_1_2_days)                                   


,sum(viewed_playback_within_2_3_days)                                   
,sum(viewed_playback_within_3_4_days)                                   
,sum(viewed_playback_within_4_5_days)                                   
,sum(viewed_playback_within_5_6_days)                                   
,sum(viewed_playback_within_6_7_days)                                   
,sum(viewed_playback_within_7_14_days)                                   
,sum(viewed_playback_within_14_21_days)                                   
,sum(viewed_playback_within_21_28_days)                                   


from vespa_analysts.trollied_20110811_second_by_second
group by second_viewed
order by second_viewed;

commit;


--A07 - Analysis of Time Viewing Event Starts----
select dateformat(dateadd(hh,1,adjusted_event_start_time),'YYYY-MM-DD HH') as hour_event_start
,sum(x_programme_viewed_duration) as viewed_dur 
,sum(case when play_back_speed is null then  x_programme_viewed_duration/3600 else 0 end) as viewed_dur_live
,sum(case when play_back_speed is not null then  x_programme_viewed_duration/3600 else 0 end) as viewed_dur_playback
 from VESPA_tmp_all_viewing_records_20110811 where epg_title = 'Trollied'
group by hour_event_start
order by hour_event_start ;

/*
select * , dateformat(adjusted_event_start_time,'YYYY-MM-DD HH') as hour_event_start from VESPA_tmp_all_viewing_records_20110811 where epg_title = 'Trollied'
and hour_event_start = '2011-08-11 00'

*/



---Match to base Universe 

--select top 100 * from vespa_analysts.uk_base_20110811_weighting_values;
--select top 100 * from vespa_analysts.uk_base_20110811;



---Barb Minute viewing --
---Get all viewing for these subscribers for all activity during the 11th-18th--


----B01 Days Returning Data by By Box---

--

select days_returning_data_2011_08_11
,days_returning_data_2011_08_12
,days_returning_data_2011_08_13
,days_returning_data_2011_08_14
,days_returning_data_2011_08_15
,days_returning_data_2011_08_16
,days_returning_data_2011_08_17
,days_returning_data_2011_08_18
,count(*) as records

from vespa_analysts.uk_base_20110811
group by days_returning_data_2011_08_11
,days_returning_data_2011_08_12
,days_returning_data_2011_08_13
,days_returning_data_2011_08_14
,days_returning_data_2011_08_15
,days_returning_data_2011_08_16
,days_returning_data_2011_08_17
,days_returning_data_2011_08_18
order by records desc
;

---Primary Boxes only--

select days_returning_data_2011_08_11
,days_returning_data_2011_08_12
,days_returning_data_2011_08_13
,days_returning_data_2011_08_14
,days_returning_data_2011_08_15
,days_returning_data_2011_08_16
,days_returning_data_2011_08_17
,days_returning_data_2011_08_18
,count(*) as records

from vespa_analysts.uk_base_20110811
where primary_sub=1
group by days_returning_data_2011_08_11
,days_returning_data_2011_08_12
,days_returning_data_2011_08_13
,days_returning_data_2011_08_14
,days_returning_data_2011_08_15
,days_returning_data_2011_08_16
,days_returning_data_2011_08_17
,days_returning_data_2011_08_18
order by records desc
;

------Non Primary Boxes only--
commit;
select days_returning_data_2011_08_11
,days_returning_data_2011_08_12
,days_returning_data_2011_08_13
,days_returning_data_2011_08_14
,days_returning_data_2011_08_15
,days_returning_data_2011_08_16
,days_returning_data_2011_08_17
,days_returning_data_2011_08_18
,count(*) as records

from vespa_analysts.uk_base_20110811
where primary_sub=0
group by days_returning_data_2011_08_11
,days_returning_data_2011_08_12
,days_returning_data_2011_08_13
,days_returning_data_2011_08_14
,days_returning_data_2011_08_15
,days_returning_data_2011_08_16
,days_returning_data_2011_08_17
,days_returning_data_2011_08_18
order by records desc
;






---Repeat but at Household (Account) level----
--drop table #summary_by_account;
select account_number
,count(*) as number_of_boxes
,sum(days_returning_data_2011_08_11) as boxes_return_data_2011_08_11
,sum(days_returning_data_2011_08_12) as boxes_return_data_2011_08_12
,sum(days_returning_data_2011_08_13) as boxes_return_data_2011_08_13
,sum(days_returning_data_2011_08_14) as boxes_return_data_2011_08_14
,sum(days_returning_data_2011_08_15) as boxes_return_data_2011_08_15
,sum(days_returning_data_2011_08_16) as boxes_return_data_2011_08_16
,sum(days_returning_data_2011_08_17) as boxes_return_data_2011_08_17
,sum(days_returning_data_2011_08_18) as boxes_return_data_2011_08_18
into #summary_by_account
from vespa_analysts.uk_base_20110811
where days_returning_data>0
group by account_number
;

select account_number
,number_of_boxes

,case when number_of_boxes=boxes_return_data_2011_08_11 then 1 else 0 end as all_boxes_return_data_2011_08_11
,case when number_of_boxes=boxes_return_data_2011_08_12 then 1 else 0 end as all_boxes_return_data_2011_08_12
,case when number_of_boxes=boxes_return_data_2011_08_13 then 1 else 0 end as all_boxes_return_data_2011_08_13
,case when number_of_boxes=boxes_return_data_2011_08_14 then 1 else 0 end as all_boxes_return_data_2011_08_14
,case when number_of_boxes=boxes_return_data_2011_08_15 then 1 else 0 end as all_boxes_return_data_2011_08_15
,case when number_of_boxes=boxes_return_data_2011_08_16 then 1 else 0 end as all_boxes_return_data_2011_08_16
,case when number_of_boxes=boxes_return_data_2011_08_17 then 1 else 0 end as all_boxes_return_data_2011_08_17
,case when number_of_boxes=boxes_return_data_2011_08_18 then 1 else 0 end as all_boxes_return_data_2011_08_18
into #account_level_summary
from #summary_by_account
;

select number_of_boxes

,all_boxes_return_data_2011_08_11
,all_boxes_return_data_2011_08_12
,all_boxes_return_data_2011_08_13
,all_boxes_return_data_2011_08_14
,all_boxes_return_data_2011_08_15
,all_boxes_return_data_2011_08_16
,all_boxes_return_data_2011_08_17
,all_boxes_return_data_2011_08_18
,count(*) as accounts
from #account_level_summary
group by number_of_boxes
,all_boxes_return_data_2011_08_11
,all_boxes_return_data_2011_08_12
,all_boxes_return_data_2011_08_13
,all_boxes_return_data_2011_08_14
,all_boxes_return_data_2011_08_15
,all_boxes_return_data_2011_08_16
,all_boxes_return_data_2011_08_17
,all_boxes_return_data_2011_08_18
order by number_of_boxes
,accounts desc;







--select top 1000 *  from   vespa_analysts.vespa_phase1b_minute_by_minute_trollied_20110811_anytime;

-- and channel_name in ('Sky 1','Sky1 HD')
--and tx_date_time_utc = '2011-08-11 20:00:00'





/*
select service_instance_id from  sk_prod.cust_set_top_box where account_number = '220017143987'
select * from  vespa_analysts.uk_base_20110811 where account_number = '220017143987'
select src_system_id from  sk_prod.CUST_SERVICE_INSTANCE where account_number = '220017143987'
CC2072922_99S


select days_returning_data_2011_06_20, days_returning_data , count(*)  from vespa_analysts.uk_base_20110811 group by days_returning_data_2011_06_20,days_returning_data order by days_returning_data_2011_06_20,days_returning_data;

select  x_box_type , count(*),sum(days_returning_data_2011_06_20)  from vespa_analysts.uk_base_20110811 group by x_box_type order by x_box_type;
x_box_type
*/


--------- 
/*

select * from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_end_datetime_utc between '2011-08-11 18:00:00' and '2011-08-11 23:00:00' order by tx_start_datetime_utc


select * from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc


select programme_trans_sk ,epg_title ,channel_name , bss_name, tx_date_time_utc , tx_end_datetime_utc from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc


select programme_trans_sk from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc

select  * from  vespa_analysts_gm_capping_test_dbarnett where subscriber_id = 14007009 order by adjusted_event_start_time;
commit;

select top 100 *
into #test
from
-- test for a day in August where we have capping rules...
    sk_prod.VESPA_STB_PROG_EVENTS_20110801

select promo_start_time , promo_duration,promo_product_description ,preceeding_programme_trans_sk 
FROM vespa_analysts.promos_all as pa
    
where 
  promo_start_time        >= '2011-08-11'
  and promo_end_time        < '2011-08-12'
and preceeding_programme_trans_sk in 
 (
201108120000014047,
201108120000000714,
201108120000002451,
201108120000014061,
201108120000000728,
201108120000002465,
201108120000014075,
201108120000002479,
201108120000000742)

order by promo_start_time


select promo_start_time , promo_duration,promo_product_description ,succeeding_programme_trans_sk , *
FROM vespa_analysts.promos_all as pa
    
where 
  promo_start_time        >= '2011-08-11'
  and promo_end_time        < '2011-08-12'

order by channel , promo_start_time


select count(*) as records
,sum(case when preceeding_programme_trans_sk = succeeding_programme_trans_sk then 1 else 0 end) as same_sk

FROM vespa_analysts.promos_all as pa
    
where 
  promo_start_time        >= '2011-08-11'
  and promo_end_time        < '2011-08-12'

--records,same_sk
--3294,2924


select promo_start_time , promo_duration,promo_product_description ,succeeding_programme_trans_sk , *
FROM vespa_analysts.promos_all as pa
    
where 
  promo_start_time        >= '2011-08-11'
  and promo_end_time        < '2011-08-12'
and preceeding_programme_trans_sk <> succeeding_programme_trans_sk
order by channel , promo_start_time



*/


