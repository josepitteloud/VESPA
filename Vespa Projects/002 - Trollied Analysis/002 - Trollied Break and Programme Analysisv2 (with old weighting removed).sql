/*------------------------------------------------------------------------------
        Project: Trollied Analysis and Ad Data Investigation
        Version: 1
        Created: 20111122
        Analyst: Dan Barnett
        SK Prod: 10
*/------------------------------------------------------------------------------
/*
        Purpose (From Brief)
        -------
        Build a hierarchy for each BARB minute during the ad breaks that maximises utilisation of the total available viewing audience 
        – imagine we could adsmart every ad spot to multiple, unique audiences including advertisers and internally to Sky

        SECTIONS
        --------

        PART A - Raw Data
             A01 - Viewing Data - Viewing of selected programme_trans_sk that relate to Trollied and the programmes surrounding it
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
        vespa_analysts.VESPA_tmp_all_viewing_records_trollied_20110811
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
SET @var_prog_period_end    = '2011-09-01';

select
      programme_trans_sk
      ,Channel_Name
      ,Epg_Title
      ,Genre_Description
      ,Sub_Genre_Description
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
  into VESPA_Programmes_20110811 -- drop table VESPA_Programmes_20110811
  from sk_prod.VESPA_EPG_DIM
 where programme_trans_sk in (
201108120000014047,
201108120000000714,
201108120000002451,
201108120000014061,
201108120000000728,
201108120000002465,
201108120000014075,
201108120000002479,
201108120000000742)
;
--select * from VESPA_Programmes_20110811 where epg_title = 'Trollied';
--select programme_trans_sk from VESPA_Programmes_20110811 where epg_title = 'Trollied';


create unique hg index idx1 on VESPA_Programmes_20110811(programme_trans_sk);

SET @var_cntr = 0;
SET @var_num_days = 21;       -- Get events up to 30 days of the programme broadcast time (only 20 in this case due to Vespa Suspension at end August

-- To store all the viewing records:
create table VESPA_tmp_all_viewing_records_trollied_20110811 ( -- drop table VESPA_tmp_all_viewing_records_trollied_20110811
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
);
exec gen_Create_table 'sk_prod.VESPA_STB_PROG_EVENTS_20110811'
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into VESPA_tmp_all_viewing_records_trollied_20110811
    select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
          inner join VESPA_Programmes_20110811 as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
     where 
--(play_back_speed is null or play_back_speed = 2) and 
        
 x_programme_viewed_duration > 0
        and Panel_id in (4,5)
        and x_type_of_viewing_event <> ''Non viewing event'''
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

--select top 500 * from VESPA_tmp_all_viewing_records_trollied_20110811 where play_back_speed = 2;


--select play_back_speed , count(*) as records from VESPA_tmp_all_viewing_records_trollied_20110811 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from VESPA_tmp_all_viewing_records_trollied_20110811 group by day_view order by day_view;

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

alter table VESPA_tmp_all_viewing_records_trollied_20110811 add live integer ;

update VESPA_tmp_all_viewing_records_trollied_20110811
set live = case when play_back_speed is null then 1 else 0 end 
from VESPA_tmp_all_viewing_records_trollied_20110811
;





---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table VESPA_tmp_all_viewing_records_trollied_20110811 add viewing_record_start_time_utc datetime;
alter table VESPA_tmp_all_viewing_records_trollied_20110811 add viewing_record_start_time_local datetime;


alter table VESPA_tmp_all_viewing_records_trollied_20110811 add viewing_record_end_time_utc datetime;
alter table VESPA_tmp_all_viewing_records_trollied_20110811 add viewing_record_end_time_local datetime;

update VESPA_tmp_all_viewing_records_trollied_20110811
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from VESPA_tmp_all_viewing_records_trollied_20110811
;
commit;


---
update VESPA_tmp_all_viewing_records_trollied_20110811
set viewing_record_end_time_utc= dateadd(second,x_programme_viewed_duration,viewing_record_start_time_utc)
from VESPA_tmp_all_viewing_records_trollied_20110811
;
commit;

--select top 100 * from VESPA_tmp_all_viewing_records_trollied_20110811;

update VESPA_tmp_all_viewing_records_trollied_20110811
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
from VESPA_tmp_all_viewing_records_trollied_20110811
;
commit;

alter table VESPA_tmp_all_viewing_records_trollied_20110811 add capped_end_time datetime ;


update VESPA_tmp_all_viewing_records_trollied_20110811
    set capped_end_time =
        case when recorded_time_utc is null then 
            -- if start of viewing_time is beyond start_time + cap then flag as null
             dateadd(minute, min_dur_mins, adjusted_event_start_time) 
            else dateadd(minute, min_dur_mins, recorded_time_utc) 
        end
from
        VESPA_tmp_all_viewing_records_trollied_20110811 base left outer join vespa_201108_max_caps caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )
;
commit;

--select top 500 * from VESPA_tmp_all_viewing_records_trollied_20110811



alter table VESPA_tmp_all_viewing_records_trollied_20110811 add capped_x_viewing_start_time_utc datetime ;
alter table VESPA_tmp_all_viewing_records_trollied_20110811 add capped_x_viewing_end_time_utc datetime ;


update VESPA_tmp_all_viewing_records_trollied_20110811
    set capped_x_viewing_start_time_utc = 
        case when viewing_record_start_time_utc >capped_end_time then null else 
           viewing_record_start_time_utc
        end
        , capped_x_viewing_end_time_utc =
        case when viewing_record_start_time_utc >capped_end_time then null
            when viewing_record_end_time_utc >=capped_end_time then capped_end_time else viewing_record_end_time_utc
        end
from
        VESPA_tmp_all_viewing_records_trollied_20110811 
;
commit;
--select top 500 * from VESPA_tmp_all_viewing_records_trollied_20110811 where play_back_speed =2;

--select capped_x_viewing_start_time_LOCAL , COUNT(*) from VESPA_tmp_all_viewing_records_trollied_20110811 where play_back_speed =2 GROUP BY capped_x_viewing_start_time_LOCAL ORDER BY capped_x_viewing_start_time_LOCAL ;

--select capped_x_viewing_start_time_LOCAL ,capped_x_viewing_end_time_LOCAL , count(*) as records from VESPA_tmp_all_viewing_records_trollied_20110811 where play_back_speed =2 group by capped_x_viewing_start_time_LOCAL ,capped_x_viewing_end_time_LOCAL   ORDER BY capped_x_viewing_start_time_LOCAL ;

--select * from vespa_analysts_gm_capping_test_dbarnett ;


alter table VESPA_tmp_all_viewing_records_trollied_20110811 add capped_x_viewing_start_time_local datetime ;
alter table VESPA_tmp_all_viewing_records_trollied_20110811 add capped_x_viewing_end_time_local datetime ;


update VESPA_tmp_all_viewing_records_trollied_20110811
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
from VESPA_tmp_all_viewing_records_trollied_20110811
;
commit;



---A05 Create Minute by Minute summary for viewing---
create variable @min_tx_start_time datetime;
create variable @max_tx_end_time datetime;

set @min_tx_start_time = (select min(tx_start_datetime_utc) from  VESPA_tmp_all_viewing_records_trollied_20110811);
set @max_tx_end_time = (select max(tx_end_datetime_utc) from  VESPA_tmp_all_viewing_records_trollied_20110811);

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

from VESPA_tmp_all_viewing_records_trollied_20110811
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

--select top 500 * from VESPA_tmp_all_viewing_records_trollied_20110811;




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
from VESPA_tmp_all_viewing_records_trollied_20110811
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
from VESPA_tmp_all_viewing_records_trollied_20110811
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
 from VESPA_tmp_all_viewing_records_trollied_20110811 where epg_title = 'Trollied'
group by hour_event_start
order by hour_event_start ;

/*
select * , dateformat(adjusted_event_start_time,'YYYY-MM-DD HH') as hour_event_start from VESPA_tmp_all_viewing_records_trollied_20110811 where epg_title = 'Trollied'
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

 select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
into #example_viewing
     from sk_prod.VESPA_STB_PROG_EVENTS_20110811 as vw
          inner join VESPA_Programmes_20110811 as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
     where 
--subscriber_id = 26315398 and 
--(play_back_speed is null or play_back_speed = 2) and 
        
-- x_programme_viewed_duration > 0
        and Panel_id in (4,5)
--        and x_type_of_viewing_event <> 'Non viewing event'
order by Adjusted_Event_Start_Time
      ;

select x_type_of_viewing_event ,play_back_speed ,count(*) as records from #example_viewing group by x_type_of_viewing_event ,play_back_speed order by records desc

select X_Adjusted_Event_End_Time ,count(*) as records from #example_viewing group by X_Adjusted_Event_End_Time order by records desc

select  Adjusted_Event_Start_Time ,count(*) as records from #example_viewing where play_back_speed = 2 group by  Adjusted_Event_Start_Time order by records desc




select * from VESPA_tmp_all_viewing_records_trollied_20110811 where subscriber_id = 26315398 order by Adjusted_Event_Start_Time

select subscriber_id 
,programme_trans_sk
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:09:00' and capped_x_viewing_end_time_local >'2011-08-11 21:09:00' and play_back_speed=2 then 1 else 0 end) as viewing_21_09_playback
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:09:00' and capped_x_viewing_end_time_local >'2011-08-11 21:09:00' and play_back_speed is null then 1 else 0 end) as viewing_21_09_live
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:16:00' and capped_x_viewing_end_time_local >'2011-08-11 21:16:00' and play_back_speed=2 then 1 else 0 end) as viewing_21_16_playback
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:16:00' and capped_x_viewing_end_time_local >'2011-08-11 21:16:00' and play_back_speed is null then 1 else 0 end) as viewing_21_16_live
into #sub_viewing4
from VESPA_tmp_all_viewing_records_trollied_20110811
where programme_trans_sk in (201108120000014061
,201108120000000728
,201108120000002465)
--where play_back_speed=2
group by subscriber_id
,programme_trans_sk;

select programme_trans_sk,viewing_21_09_live , viewing_21_16_live ,viewing_21_09_playback , viewing_21_16_playback, count(*) as subs 
from #sub_viewing4
group by programme_trans_sk,viewing_21_09_live , viewing_21_16_live ,viewing_21_09_playback , viewing_21_16_playback
order by programme_trans_sk,viewing_21_09_live , viewing_21_16_live ,viewing_21_09_playback , viewing_21_16_playback

commit;

Try repeating but without the 'Non- Viewing Event' criteria --

select * from #sub_viewing4 where viewing_21_09_playback = 0 and viewing_21_16_playback = 1

select * from  VESPA_tmp_all_viewing_records_trollied_20110811 where subscriber_id = 22307325 order by adjusted_event_start_time;



programme_trans_sk

select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
into #example_viewing
     from sk_prod.VESPA_STB_PROG_EVENTS_20110811 as vw
          inner join VESPA_Programmes_20110811 as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
     where 
--subscriber_id = 26315398 and 
--(play_back_speed is null or play_back_speed = 2) and 
        
-- x_programme_viewed_duration > 0
        and 
Panel_id in (4,5)
--        and x_type_of_viewing_event <> 'Non viewing event'
order by Adjusted_Event_Start_Time
      ;


 select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
     from sk_prod.VESPA_STB_PROG_EVENTS_2011_08_11 as vw
          inner join VESPA_Programmes_20110811 as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
     where 
--(play_back_speed is null or play_back_speed = 2) and 
        
 x_programme_viewed_duration > 0
        and Panel_id in (4,5)
        and x_type_of_viewing_event <> ''Non viewing event'''
      ;




*/
