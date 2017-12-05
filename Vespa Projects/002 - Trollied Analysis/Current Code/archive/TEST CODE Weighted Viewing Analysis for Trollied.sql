/*------------------------------------------------------------------------------
        Project: Weighted Trollied Viewing
        Version: 1
        Created: 20111205
        Lead: Dan Barnett
        Analyst: 
        SK Prod: 10
        --This Code uses the UK base table and the viewing data created for the Trollied analysis to 
        --Create weighted minute by minute views

        Tables Used

        vespa_analysts.sky_base_v2_2011_08_11 
        VESPA_tmp_all_viewing_records_trollied_20110811

        V2 20111208 - Excludes any boxes with any 'Unknown' viewing issues

*/------------------------------------------------------------------------------
---Create table for calculations from UK base that only has boxes that returned data in period---
--drop table vespa_analysts.trollied_analysis_box_returning_data;
select * into vespa_analysts.trollied_analysis_box_returning_data
from vespa_analysts.sky_base_v2_2011_08_11
where days_returning_data>0
;

commit;

--select top 100 * from vespa_analysts.sky_base_v2_2011_08_11;

--select count(*) , count(distinct subscriber_id) from vespa_analysts.trollied_analysis_box_returning_data;
--select top 100 * from vespa_analysts.trollied_analysis_box_returning_data;

create hg index idx1 on vespa_analysts.trollied_analysis_box_returning_data(subscriber_id);



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

--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;

---Add weightings on to the minute by minute details---
if object_id('vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM') is not null drop table vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM;
commit;
create table vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live          smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
,seconds_viewed_in_minute_playback_within_163_hours smallint            not null
,weighted_boxes bigint 
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;

--select top 100 * from vespa_analysts.trollied_analysis_box_returning_data;
---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM
    select a.subscriber_id
    ,@minute as minute
    ,sum(case when 
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) then 60 when 
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute

    ,sum(case when live = 0 then 0 when
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) then 60 when 
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_live

    ,sum(case when live =1 then 0 when
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) then 60 when 
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_playback

    ,sum(case when live =1 then 0 when  dateadd(hour,163,recorded_time_utc)<adjusted_event_start_time then 0 when
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) then 60 when 
    capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,capped_x_viewing_end_time_local) when
    capped_x_viewing_start_time_local>@minute and capped_x_viewing_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,capped_x_viewing_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_playback_within_163_hours

,max(case   when cast (Adjusted_Event_Start_Time as date) ='2011-08-11' then weight_2011_08_11
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-12' then weight_2011_08_12
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-13' then weight_2011_08_13
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-14' then weight_2011_08_14
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-15' then weight_2011_08_15
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-16' then weight_2011_08_16
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-17' then weight_2011_08_17
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-18' then weight_2011_08_18 else 0 end) as weighted_boxes

--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from VESPA_tmp_all_viewing_records_trollied_20110811 as a 
left outer join vespa_analysts.trollied_analysis_box_returning_data as b
on a.subscriber_id=b.subscriber_id
where b.subscriber_id is not null and  (play_back_speed is null or play_back_speed = 2) and 
        (
        (capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>@minute)
    or
        (capped_x_viewing_start_time_local between @minute and dateadd(second,59,@minute))
        )
    
    group by a.subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

--select count(*) from vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM;

--select top 500 *  from vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM order by subscriber_id;

----Add on Extra Variables to add on to account_details


commit;



--drop table vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile;
select minute
,b.lifestage         
        ,b.HHAfflu           
        ,b.isba_tv_region    
        ,b.package
        ,has_pvr
        ,has_non_pvr
        ,multi_box_hh
,sum(case when seconds_viewed_in_minute >=31 then weighted_boxes else 0 end) as total_households
,sum(case   when seconds_viewed_in_minute_playback_within_163_hours >=31 then 0 when seconds_viewed_in_minute_playback_within_163_hours>seconds_viewed_in_minute_live then 0 
            when seconds_viewed_in_minute >=31 then weighted_boxes else 0 end) as total_households_live

,sum(case   when seconds_viewed_in_minute_live >=31 then 0 when seconds_viewed_in_minute_live>=seconds_viewed_in_minute_playback_within_163_hours then 0 
            when seconds_viewed_in_minute >=31 then weighted_boxes else 0 end) as total_households_playback_163h

into vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile
from vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM as a
left outer join vespa_analysts.sky_base_v2_2011_08_11 as b
on a.subscriber_id =b.subscriber_id
left outer join vespa_analysts.sky_base_2011_08_11_by_account as c
on b.account_number =c.account_number

group by minute
,b.lifestage         
        ,b.HHAfflu           
        ,b.isba_tv_region    
        ,b.package
        ,has_pvr
        ,has_non_pvr
        ,multi_box_hh
;


--select * from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile;

select minute
,sum(total_households) as households
,sum(total_households_live) as hh_live
,sum(total_households_playback_163h) as hh_playback_barb

from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile

group by minute
order by minute
;

commit;
select minute
,sum(total_households) as households
,sum(total_households_live) as hh_live
,sum(total_households_playback_163h) as hh_playback_barb

,sum(case when multi_box_hh = 1 then total_households else 0 end) as multi_box_households
,sum(case when multi_box_hh = 1 then total_households_live else 0 end) as multi_box_hh_live
,sum(case when multi_box_hh = 1 then total_households_playback_163h else 0 end ) as multi_box_hh_playback_barb

,sum(case when multi_box_hh = 0 then total_households else 0 end) as single_box_households
,sum(case when multi_box_hh = 0 then total_households_live else 0 end) as single_box_hh_live
,sum(case when multi_box_hh = 0 then total_households_playback_163h else 0 end ) as single_box_hh_playback_barb

from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile as a

group by minute
order by minute
;

--select distinct HHAfflu from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile;
--select multi_box_hh ,count(*) from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile group by multi_box_hh;



select minute
,sum(total_households) as households
,sum(case when HHAfflu = 'Very High' then total_households else 0 end) as very_high_aff_households
,sum(case when HHAfflu = 'High' then total_households else 0 end) as high_aff_households
,sum(case when HHAfflu = 'Mid High' then total_households else 0 end) as mid_high_aff_households
,sum(case when HHAfflu = 'Mid' then total_households else 0 end) as mid_aff_households
,sum(case when HHAfflu = 'Mid Low' then total_households else 0 end) as mid_low_aff_households
,sum(case when HHAfflu = 'Low' then total_households else 0 end) as low_aff_households
,sum(case when HHAfflu = 'Very Low' then total_households else 0 end) as very_low_aff_households
,sum(case when HHAfflu = 'Unknown' then total_households else 0 end) as unknwon_aff_households
from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile as a

group by minute
order by minute
;

select count(*) from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile where hhafflu is null




select minute
,sum(total_households) as households
,sum(case when has_pvr = 1 then total_households else 0 end) as has_pvr_households
,sum(case when has_pvr = 0 then total_households else 0 end) as non_pvr_households
from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile as a

group by minute
order by minute
;




commit;

select minute
,sum(total_households) as households
,sum(case when HHAfflu = 'Very High' then total_households_live else 0 end) as very_high_aff_households_live
,sum(case when HHAfflu = 'High' then total_households_live else 0 end) as high_aff_households_live
,sum(case when HHAfflu = 'Mid High' then total_households_live else 0 end) as mid_high_aff_households_live
,sum(case when HHAfflu = 'Mid' then total_households_live else 0 end) as mid_aff_households_live
,sum(case when HHAfflu = 'Mid Low' then total_households_live else 0 end) as mid_low_aff_households_live
,sum(case when HHAfflu = 'Low' then total_households_live else 0 end) as low_aff_households_live
,sum(case when HHAfflu = 'Very Low' then total_households_live else 0 end) as very_low_aff_households_live
,sum(case when HHAfflu = 'Unknown' then total_households_live else 0 end) as unknwon_aff_households_live


,sum(case when HHAfflu = 'Very High' then total_households_playback_163h else 0 end) as very_high_aff_households_playback
,sum(case when HHAfflu = 'High' then total_households_playback_163h else 0 end) as high_aff_households_playback
,sum(case when HHAfflu = 'Mid High' then total_households_playback_163h else 0 end) as mid_high_aff_households_playback
,sum(case when HHAfflu = 'Mid' then total_households_playback_163h else 0 end) as mid_aff_households_playback
,sum(case when HHAfflu = 'Mid Low' then total_households_playback_163h else 0 end) as mid_low_aff_households_playback
,sum(case when HHAfflu = 'Low' then total_households_playback_163h else 0 end) as low_aff_households_playback
,sum(case when HHAfflu = 'Very Low' then total_households_playback_163h else 0 end) as very_low_aff_households_playback
,sum(case when HHAfflu = 'Unknown' then total_households_playback_163h else 0 end) as unknwon_aff_households_playback

from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile as a

group by minute
order by minute
;


--select top 100 * from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile;
---------

--select * from vespa_analysts.sky_base_2011_08_11_count_by_account_attributes;
-----------Weighted Second by Second-----------------------



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
,weighted_boxes bigint NULL

);
commit;

---Start of Loop
WHILE @programme_time <  @programme_time_end LOOP
insert into vespa_analysts.trollied_20110811_second_by_second
select a.subscriber_id
--,account_number
,@programme_time as second_viewed
,1 as viewed
,max(case when play_back_speed is null then 1 else 0 end) as viewed_live
,max(case when play_back_speed is not null then 1 else 0 end) as viewed_playback
,max(case when play_back_speed is null then 0 when play_back_speed is not null and dateadd(hour,163,recorded_time_utc)>=adjusted_event_start_time   then 1 else 0 end) as viewed_playback_within_163_hours

,max(case   when cast (Adjusted_Event_Start_Time as date) ='2011-08-11' then weight_2011_08_11
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-12' then weight_2011_08_12
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-13' then weight_2011_08_13
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-14' then weight_2011_08_14
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-15' then weight_2011_08_15
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-16' then weight_2011_08_16
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-17' then weight_2011_08_17
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-18' then weight_2011_08_18 else 0 end) as weighted_boxes
from VESPA_tmp_all_viewing_records_trollied_20110811 as a
left outer join vespa_analysts.trollied_analysis_box_returning_data as b
on a.subscriber_id=b.subscriber_id
where b.subscriber_id is not null and  cast(capped_x_viewing_start_time_local as datetime)<=@programme_time and cast(capped_x_viewing_end_time_local as datetime)>@programme_time
and (play_back_speed is null or play_back_speed = 2)
group by a.subscriber_id
--,account_number 
,second_viewed,viewed
;

 SET @programme_time =dateadd(second,1,@programme_time);
    COMMIT;

END LOOP;
commit;






/*



select * from vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM order by minute;

select * from vespa_analysts.sky_base_v2_2011_08_11 where subscriber_id = 21183733

select weighted_boxes ,count(*) as records from vespa_analysts.vespa_phase1b_TEST_TROLLIED_MBM group by weighted_boxes order by records desc

select * from sk_prod.VESPA_STB_PROG_EVENTS_20110801 where subscriber_id = 21183733

select * from vespa_analysts.daily_summary_by_subscriber_20110811 where subscriber_id = 21183733

vespa_analysts.sky_base_v2_2011_08_11

select * 
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)') and cast(si_external_identifier as integer)=21183733



select subscription_sub_type ,service_instance_id, effective_from_dt , effective_to_dt , status_code ,cb_key_household from sk_prod.cust_subs_hist where account_number = '621092838930' order by effective_from_dt

select * from vespa_analysts.sky_base_v2_2011_08_11
where account_number = '621092838930'

select subscription_sub_type, count(*) from vespa_analysts.sky_base_v2_2011_08_11 group by subscription_sub_type

left outer join vespa_analysts.sky_base_2011_08_11_count_by_account_attributes as b
on a.lifestage   =b.  lifestage    
and        a.HHAfflu    =b.HHAfflu        
and        a.isba_tv_region   =b. isba_tv_region 
and        a.package =b.package
and        a.has_pvr =b.has_pvr
and        a.has_non_pvr =b.has_non_pvr
and        a.multi_box_hh =b.multi_box_hh



---Split by Prescence of Children
select minute
,sum(total_households) as households
,sum(case when lifestage in ( then total_households else 0 end) as very_high_aff_households



from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile as a

group by minute
order by minute
;

--select distinct isbavespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile
select minute
,isba_tv_region
,sum(total_households) as households


from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile as a

group by minute
,isba_tv_region
order by minute
,isba_tv_region
;
commit;
--select top 100 * from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile;
select minute
,multi_box_hh
,sum(total_households) as households


from vespa_analysts.TEST_TROLLIED_minute_by_minute_weighted_profile as a

group by minute
,multi_box_hh
order by minute
,multi_box_hh
;

--select multi_box_hh , count(*) from 

--select top 100 * from vespa_analysts.sky_base_v2_2011_08_11;



select subscriber_id 
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:09:00' and capped_x_viewing_end_time_local >'2011-08-11 21:09:00' and play_back_speed=2 then 1 else 0 end) as viewing_21_09_playback
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:09:00' and capped_x_viewing_end_time_local >'2011-08-11 21:09:00' and play_back_speed is null then 1 else 0 end) as viewing_21_09_live
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:16:00' and capped_x_viewing_end_time_local >'2011-08-11 21:16:00' and play_back_speed=2 then 1 else 0 end) as viewing_21_16_playback
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:16:00' and capped_x_viewing_end_time_local >'2011-08-11 21:16:00' and play_back_speed is null then 1 else 0 end) as viewing_21_16_live
into #sub_viewing
from VESPA_tmp_all_viewing_records_trollied_20110811
where programme_trans_sk in (201108120000014061
,201108120000000728
,201108120000002465)
--where play_back_speed=2
group by subscriber_id
;

----MAtch up to UK table
select case when viewing_21_09_playback = 1 and viewing_21_16_playback = 1 then '1: Both on Playback'
when viewing_21_09_playback = 0 and viewing_21_16_playback = 1 then '2: Only 9:16 on Playback' else '3: Other' end as playback_type
,pvr
,box_type
,x_pvr_type
,x_description
,x_manufacturer
,isba_tv_region
,count(*) as subscribers

from #sub_viewing as a
left outer join vespa_analysts.sky_base_v2_2011_08_11 as b
on a.subscriber_id=b.subscriber_id
group by playback_type,pvr
,box_type
,x_pvr_type
,x_description
,x_manufacturer
,isba_tv_region
order by playback_type,pvr
,box_type
,x_pvr_type
,x_description
,x_manufacturer
,isba_tv_region
;

commit;

*/


