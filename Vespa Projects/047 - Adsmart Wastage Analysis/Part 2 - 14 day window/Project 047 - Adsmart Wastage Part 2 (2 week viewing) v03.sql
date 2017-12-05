

----Project 047 Part 2 -----
--http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=47&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2FProjectInceptionView%2Easpx

/*  From Sharepoint
To understand the viability of various AdSmart and linear trading approaches, we wish understand how much inventory could be created 
through just trading linear ‘wastage’.  In order to identify the reachable wastage from the BARB traded audience we have had to define a set of
 ‘Mirror Segments’, which define the reachable audiences for each BARB traded audience. 
The reason we cannot reach all of the wastage is that AdSmart cannot identify which householder is watching at any one point in time, 
therefore whilst there might be wastage in an advert bought against housewives with children if someone other than the housewife is 
viewing the television there is no way for us to identify and then serve a different advert to this person. However we could in this 
situation still deliver alternative advertising to any household with no children as nobody in that household (with the exception of visitors) 
would fall into the BARB traded demographic. The brief is still being refined by Rory Skrebowski. 
*/

---Two Week Live Viewing Activity (Mon 2nd - Sun 15th Jan 2012 inclusive)

---PART A  - Live Viewing of Sky Channels ---

--------------------------------------------------------------------------------
-- PART A02 Viewing Data
--------------------------------------------------------------------------------

---Looking at 31 days worth of tables but only return viewing for 15th Jan 2012

---Also for initial part of query looking at all records, not just live/regular speed playback.




---PART2 - Viewing Data

---Run Capping for Jan/feb first - do not remove any activity <=5 seconds for this activity 
---(Cap for Live and Single Speed playback will be applied at a later point)--
-- Populate all viewing data between around 15th Jan--
--select * from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc


--select programme_trans_sk from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc


  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-01-02';
SET @var_prog_period_end    = '2012-01-16';


SET @var_cntr = 0;
SET @var_num_days = 15;       -- 
--select top 500 * from vespa_analysts.project047_sky_channels_live_2nd_15th_jan;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project047_sky_channels_live_2nd_15th_jan;
-- To store all the viewing records:
create table vespa_analysts.project047_sky_channels_live_2nd_15th_jan ( -- drop table vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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
    insert into vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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
video_playing_flag = 1 and    
      adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
          or (x_type_of_viewing_event = (''Other Service Viewing Event'')
              and x_si_service_type = ''High Definition TV test service''))
     and panel_id in (4,5)
and play_back_speed is null
and (
        upper(left(channel_name,3)) = ''SKY''
         or 
        channel_name in (''PICK TV'',''PICK TV+1'',''Challenge'',''Challenge+1'')
    )
'     ;

--select distinct channel_name from  sk_prod.VESPA_EPG_DIM order by channel_name;
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

alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add live tinyint;

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
;
commit;

if object_id('vespa_analysts.channel_name_lookup_old') is not null drop table vespa_analysts.channel_name_lookup_old;
create table vespa_analysts.channel_name_lookup_old 
(channel varchar(90)
,channel_name_grouped varchar(90)
,channel_name_inc_hd varchar(90)
)
;
commit;
input into vespa_analysts.channel_name_lookup_old from 'G:\RTCI\Sky Projects\Vespa\Phase1b\Channel Lookup\Channel Lookup Info Phase1b.csv' format ascii;
commit;

alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add channel_name_inc_hd varchar(40);

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;


-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(subscriber_id);
create dttm index idx2 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(recorded_time_utc);
create lf index idx4 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(live)
create dttm index idx5 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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
        vespa_analysts.project047_sky_channels_live_2nd_15th_jan base left outer join vespa_max_caps_jan_feb_2012 caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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

--select capped_flag  , count(*) from vespa_analysts.project047_sky_channels_live_2nd_15th_jan where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project047_sky_channels_live_2nd_15th_jan where capped_flag=1;

-----Remove any Records that have a capped flag of 2 or 3---

---Deletion of capped records commented out initially - for evaluation purposes---


/*
delete from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
where capped_flag in (2,3)
;
commit;
*/


---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add viewing_record_start_time_local datetime;


alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add viewing_record_end_time_local datetime;

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
;
commit;


---
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
;
commit;

--select top 100 * from vespa_analysts.project047_sky_channels_live_2nd_15th_jan;

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set viewing_record_start_time_local= case 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-00' then dateadd(hh,1,viewing_record_start_time_utc) 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-00' then dateadd(hh,1,viewing_record_start_time_utc) 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-00'  then dateadd(hh,1,viewing_record_start_time_utc) 
                    else viewing_record_start_time_utc  end
,viewing_record_end_time_local=case 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-00' then dateadd(hh,1,viewing_record_end_time_utc) 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-00' then dateadd(hh,1,viewing_record_end_time_utc) 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-00' then dateadd(hh,1,viewing_record_end_time_utc) 
                    else viewing_record_end_time_utc  end
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
;
commit;

--select  dateformat(Adjusted_Event_Start_Time,'YYYY-MM-DD') as day_detail,count(*) from vespa_analysts.project047_sky_channels_live_2nd_15th_jan group by day_detail order by day_detail;



--Remove Capped Records

--Add weightings
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add weighting double;

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set weighting = c.weighting
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
left outer join vespa_analysts.scaling_weightings as c
on b.scaling_segment_id=c.scaling_segment_id
where cast (a.Adjusted_Event_Start_Time as date)  between b.reporting_starts and b.reporting_ends
and c.scaling_day = cast (a.Adjusted_Event_Start_Time as date)
;
commit;


--Create a Lookup table of all boxes retruning data in the 2 week period and add whether boxes are adsmartable or not and box type details

select subscriber_id
,min(cb_key_household) as household_key
,min(account_number) as account_num
into vespa_analysts.project047_all_boxes_returning_data_2_weeks
from  vespa_analysts.project047_sky_channels_live_2nd_15th_jan
group by subscriber_id;
--select count(*) from vespa_analysts.vespa_data_subscribers_20120115;

commit;

Alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks rename household_key to cb_key_household;
Alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks rename account_num to account_number;


Alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks
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
 WHERE (box_installed_dt <= cast('2012-01-15'  as date) 
   AND box_replaced_dt   > cast('2012-01-15'  as date)) or box_installed_dt is null
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


---Create src_system_id lookup
--drop table  #subs_details;
select src_system_id
,min(cast(si_external_identifier as integer)) as subscriberid
,max(case when si_service_instance_type in ('Primary DTV') then 1 else 0 end) as primary_box
into #subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
group by src_system_id
;
commit;

commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';

commit;

--alter table  vespa_analysts.project047_all_boxes_returning_data_2_weeks add primary_box tinyint;
alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks add x_pvr_type  varchar(50);
alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks add x_box_type  varchar(20);
alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks add x_description  varchar(100);
alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks add x_manufacturer  varchar(50);
alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks add x_model_number  varchar(50);

update vespa_analysts.project047_all_boxes_returning_data_2_weeks
set primary_box =b.primary_box
,x_pvr_type=c.pvr_type
,x_box_type=c.box_type
,x_description=c.description_x
,x_manufacturer=c.manufacturer
,x_model_number=c.model_number
from  vespa_analysts.project047_all_boxes_returning_data_2_weeks as a
left outer join #subs_details as b
on a.subscriber_id=b.subscriberid
left outer join #boxes_with_model_info as c
on b.src_system_id=c.service_instance_id
;


commit;
--exec gen_create_table 'vespa_analysts.sky_base_2012_01_15';

--select top 500 * from vespa_analysts.project047_all_boxes_returning_data_2_weeks;

--select top 100 * from vespa_analysts.project047_all_boxes_returning_data_2_weeks;

update vespa_analysts.project047_all_boxes_returning_data_2_weeks
set pvr =case when x_pvr_type like '%PVR%' then 1 else 0 end
,box_type =case when x_box_type like '%HD%' then 'HD' else 'SD' end
from vespa_analysts.project047_all_boxes_returning_data_2_weeks
;

commit;


alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks add adsmartable_box  tinyint;

update vespa_analysts.project047_all_boxes_returning_data_2_weeks
set adsmartable_box =case when x_pvr_type in ('PVR5','PVR6','PVR7') 
            OR ( x_pvr_type='PVR4' AND  x_manufacturer in ('Pace','Samsung','Thomson')) 
          then 1 else 0 end
from vespa_analysts.project047_all_boxes_returning_data_2_weeks
;
commit;
--select top 100 * from  vespa_analysts.project047_all_boxes_returning_data_2_weeks ;

--select x_manufacturer , count(*) from vespa_analysts.project047_all_boxes_returning_data_2_weeks group by x_manufacturer;


--Create HH Profile Variables and Demographics for Spot Analysis (Using Current ILU data)

select cb_key_household
,min(ILU_HHSocioEcon) as socio_demographic_level
,max(ILU_HHAfflu) as household_affluence_level
,max(case when ILU_Agef in (1,2,3) then 1 else 0 end) as adult_18_34
,max(case when ILU_Agef in (1,2,3,4,5) then 1 else 0 end) as adult_18_44
,max(case when ILU_Agef in (1,2,3,4,5,6,7) then 1 else 0 end) as adult_18_54
,max(case when ILU_Agef in (2,3,4,5) then 1 else 0 end) as adult_25_44
,max(case when ILU_HHSocioEcon in  (1)  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('2','5','7')  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('6') and ILU_HHAfflu>=9 then 1 
          when ILU_HHSocioEcon in  (5) and  ILU_HHAfflu>=8   then 1
          else 0 end) as adult_ABC1
,max(case when ILU_iKid0004+ILU_iKid0507+ILU_iKid0810+ILU_iKid1116>0 then 1 else 0 end) as hh_with_children
,max(case when ILU_Gender in (0,1) then 1 else 0 end) as adult_male   --Includes Unknown gender (approx 4% of people)
,max(case when ILU_Agef in (1,2,3) and ILU_Gender in (0,1) then 1 else 0 end) as adult_male_18_34

,max(case when ILU_Gender not in (0,1) then 0
          when ILU_HHSocioEcon in  (1)  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('2','5','7')  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('6') and ILU_HHAfflu>=9 then 1 
          when ILU_HHSocioEcon in  (5) and  ILU_HHAfflu>=8   then 1
          else 0 end) as adult_male_ABC1

,max(case when ILU_Agef in (1,2,3) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_34
,max(case when ILU_Agef in (1,2,3,4,5) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_44
,max(case when ILU_Agef in (1,2,3,4,5,6,7) and ILU_Gender in (2) then 1 else 0 end) as adult_female_18_54

,max(case when ILU_Gender not in (2) then 0
          when ILU_HHSocioEcon in  (1)  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('2','5','7')  then 1 
          when ILU_HHSocioEcon in  (2) and ILU_OccY3 in ('6') and ILU_HHAfflu>=9 then 1 
          when ILU_HHSocioEcon in  (5) and  ILU_HHAfflu>=8   then 1
          else 0 end) as adult_female_ABC1

,max(case when ILU_Gender in (2) then 1 else 0 end) as adult_female   
,max(case when ILU_OccY3 in ('1') then 1 else 0 end) as occ_caftsman_in_hh   
,max(case when ILU_OccY3 in ('2') then 1 else 0 end) as occ_education_in_hh   
,max(case when ILU_OccY3 in ('3') then 1 else 0 end) as occ_housewife_in_hh   
,max(case when ILU_OccY3 in ('4') then 1 else 0 end) as occ_manual_in_hh   
,max(case when ILU_OccY3 in ('5') then 1 else 0 end) as occ_middle_management_in_hh   
,max(case when ILU_OccY3 in ('6') then 1 else 0 end) as occ_office_clerical_in_hh   
,max(case when ILU_OccY3 in ('7') then 1 else 0 end) as occ_professional_senior_in_hh   
,max(case when ILU_OccY3 in ('8') then 1 else 0 end) as occ_retired_in_hh   
,max(case when ILU_OccY3 in ('9','0','U') then 1 else 0 end) as occ_other_in_hh   

,max(case when ILU_Correspondent  in ('P1') then ILU_Agef else null end) as head_hh_agef
into #household_summary_details
from sk_prod.ILU
where ILU_Correspondent  in ('P1','P2','OR')  and cb_address_status = '1' and cb_address_dps is not null  
group by cb_key_household
;

commit;
create hg index idx1 on  #household_summary_details(cb_key_household);


---Create a table of all households where there is a HH key match to the Vespa viewing data in the period

select b.*
into vespa_analysts.project047_all_boxes_returning_data_2_weeks_hh_info
from vespa_analysts.project047_all_boxes_returning_data_2_weeks as a
left outer join #household_summary_details as b
on a.cb_key_household=b.cb_key_household
where b.cb_key_household is not null
;
commit;
create hg index idx1 on vespa_analysts.project047_all_boxes_returning_data_2_weeks_hh_info(cb_key_household);
---Add on TV region details from Cust_Single_Account_View


alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks_hh_info add isba_tv_region varchar(20) ;

update vespa_analysts.project047_all_boxes_returning_data_2_weeks_hh_info
set isba_tv_region=b.isba_tv_region
from vespa_analysts.project047_all_boxes_returning_data_2_weeks_hh_info as a
left outer join sk_prod.cust_single_Account_view as b
on a.cb_key_household = b.cb_key_household
;

commit;

--select top 100 * from vespa_analysts.project047_all_boxes_returning_data_2_weeks_hh_info;

---Add TV region on to Box summary data
alter table vespa_analysts.project047_all_boxes_returning_data_2_weeks add isba_tv_region varchar(20) ;

update vespa_analysts.project047_all_boxes_returning_data_2_weeks
set isba_tv_region=b.isba_tv_region
from vespa_analysts.project047_all_boxes_returning_data_2_weeks as a
left outer join sk_prod.cust_single_Account_view as b
on a.cb_key_household = b.cb_key_household
;

commit;


---Part 02 - Load in Spot Traded demographic information (Supplied by Chris Thomas)---
--drop table vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan ;
create table vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan
(
    channel varchar(32)
    , partner varchar(16)
    , country varchar(10)
    , timeshift varchar(1)
    , break_date varchar(8)
    ,tx_time varchar(6)
    ,demo_code varchar(6)
    ,demograph varchar(32)
    ,product varchar(64)
    ,length integer
    ,nominal_price decimal(10,2)
    ,actual_impacts decimal(10,2)
);

commit;
input into vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan 
from 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\Part 2 - 2 Week Sky Channel Analysis\SkySpots 2 week data.csv' format ascii;
commit;


alter table vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan add corrected_spot_transmission_date date;

update vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan
set raw_corrected_spot_time= case 
      when left (tx_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (tx_time,2) as integer)-24 ||right (tx_time,4) 
      else tx_time end 
,corrected_spot_transmission_date  = case when left (tx_time,2) in ('24','25','26','27','28','29') then cast(break_date as date)+1
else cast(break_date as date) end
from vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan
;

alter table  vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;



--drop table vespa_analysts.project047_spots_viewing_channel_lookup;
if object_id('vespa_analysts.project047_spots_viewing_channel_lookup') is not null drop table vespa_analysts.project047_spots_viewing_channel_lookup;
create table vespa_analysts.project047_spots_viewing_channel_lookup
(    spot_channel varchar(64)
    , channel_name_inc_hd varchar(64)
);

commit;
input into vespa_analysts.project047_spots_viewing_channel_lookup
from 'C:\Users\barnetd\Documents\Project 047 - Adsmart Wastage Analysis\spot to viewing channel name lookup.csv' format ascii;
commit;

--select top 100 * from vespa_analysts.project047_spots_and_traded_demographics;
--select count(*) from vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan;
--delete from vespa_analysts.project047_spots_and_traded_demographics where country is null;

alter table  vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan add channel_name_inc_hd varchar(64);

update vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan
set channel_name_inc_hd = b.channel_name_inc_hd
from vespa_analysts.project047_spots_and_traded_demographics_02Jan_16_jan as a
left outer join vespa_analysts.project047_spots_viewing_channel_lookup as b
on a.channel = b.spot_channel
;
commit;













