/*##################################################################################
*   FILE HEADER
*****************************************************************************
*   Product:          SQL
*   Version:          3.0
*   Author:           Dan Barnett
*   Creation Date:    07/11/2011
*   Latest Version Date 14/11/11
*   Description:      Run Analysis for Qualifying/Race/Highlights Programmes for each of the 8 F1 GP
*                     that have taken place between late May and August
*###################################################################################
*
*   Process depends on: -
*
*###################################################################################
*   REVISION HISTORY
************************************************************************************
*   Date    Author Version   Description
*   07/11/2011   DB    1.0      Initial version
*
*###################################################################################
*   DESCRIPTION
*   Section 1
*   Returns viewing data on all programme_trans_sk that relate to the 8 F1 GP
*   Creates Minute by Minute Summaries by subscriber_id for each of the programmes
*
*   Section 2 
*   Creates a summary by subscriber_id of all boxes returning data on each of the 16 Days that there was Activity
*
*   Section 3
*   Create Minute by Minute Outputs for each qualifying/race/highlights
*##################################################################################*/

---Get list of programme SKs for all F1 programmes---

select programme_trans_sk
,channel_name
,tx_date_time_utc
,epg_title
,synopsis
,case   when left(synopsis,10) ='Qualifying' then '01: Qualifying' 
        when left(synopsis,10) ='Highlights' then '03: Highlights' 
        when left(synopsis,19) ='...Prix. Qualifying' then '01: Qualifying' 
        when left(synopsis,19) ='...Prix. Highlights' then '03: Highlights'
 else '02: Main Race' end
as programme_type
from sk_prod.vespa_epg_dim
where  upper(channel_name) like '%BBC%' and tx_date_time_utc>='2011-05-20' and tx_date_time_utc<'2011-09-01'
and left(epg_title,9)='Formula 1'
order by tx_date_time_utc
;

-----Get all viewing from F1 programmes in the period----
if object_id('vespa_analysts.F1_analysis_20111104') is not null drop table vespa_analysts.F1_analysis_20111104;
select * ,case   when left(synopsis,10) ='Qualifying' then '01: Qualifying' 
        when left(synopsis,10) ='Highlights' then '03: Highlights' 
        when left(synopsis,19) ='...Prix. Qualifying' then '01: Qualifying' 
        when left(synopsis,19) ='...Prix. Highlights' then '03: Highlights'
 else '02: Main Race' end
as programme_type into vespa_analysts.F1_analysis_20111104 from sk_prod.vespa_events_view where programme_trans_sk
in (
201105210000007927
,201105210000007633
,201105210000006069
,201105210000005793
,201105210000008823
,201105210000004032
,201105210000008235
,201105210000004900
,201105210000004603
,201105210000002466
,201105210000005872
,201105210000004897
,201105210000002556
,201105210000007203
,201105210000005725
,201105210000006363
,201105210000002262
,201105210000008124
,201105210000005431
,201105220000005557
,201105220000004849
,201105220000005957
,201105220000004449
,201105220000005586
,201105220000008362
,201105220000007815
,201105220000007259
,201105220000002346
,201105220000004340
,201105220000002682
,201105220000007479
,201105220000004757
,201105220000008165
,201105220000005732
,201105220000008907
,201105220000005893
,201105220000005835
,201105220000006293
,201105220000003442
,201105220000007933
,201105280000006153
,201105280000002332
,201105280000008613
,201105280000005333
,201105280000005627
,201105280000004765
,201105280000007049
,201105280000004687
,201105280000007717
,201105280000008166
,201105280000008025
,201105280000007423
,201105280000005373
,201105280000005859
,201105280000004393
,201105280000005872
,201105280000002626
,201105280000003864
,201105280000004774
,201105290000005583
,201105290000007105
,201105290000007171
,201105290000006125
,201105290000004645
,201105290000008515
,201105290000002598
,201105290000005746
,201105290000005040
,201105290000007479
,201105290000004337
,201105290000005389
,201105290000004723
,201105290000003794
,201105290000007801
,201105290000005817
,201105290000005697
,201105290000008152
,201105290000002290
,201105290000007625
,201105290000003246
,201106110000005887
,201106110000008249
,201106110000005529
,201106110000002794
,201106110000002528
,201106110000008334
,201106110000008823
,201106110000004673
,201106110000005263
,201106110000007969
,201106110000007021
,201106110000004407
,201106110000006026
,201106110000003920
,201106110000004760
,201106110000005513
,201106110000006153
,201106110000004947
,201106110000007703
,201106120000004827
,201106120000008221
,201106120000005557
,201106120000005249
,201106120000008376
,201106120000004519
,201106120000002738
,201106120000007591
,201106120000006531
,201106120000007899
,201106120000002430
,201106120000005250
,201106120000008935
,201106120000006223
,201106120000007553
,201106120000005045
,201106120000005877
,201106120000005830
,201106120000004032
,201106120000006095
,201106120000007396
,201106120000007639
,201106120000007325
,201106120000002340
,201106120000007723
,201106120000003540
,201106250000005403
,201106250000008221
,201106250000002612
,201106250000002906
,201106250000008529
,201106250000004407
,201106250000007927
,201106250000007147
,201106250000005697
,201106250000006251
,201106250000004718
,201106250000005957
,201106250000009131
,201106250000005648
,201106250000005569
,201106250000008432
,201106250000004835
,201106250000003864
,201106250000004701
,201106260000004421
,201106260000005110
,201106260000007815
,201106260000008376
,201106260000005179
,201106260000004807
,201106260000004771
,201106260000008529
,201106260000007497
,201106260000005863
,201106260000002976
,201106260000002626
,201106260000003878
,201106260000006111
,201106260000005942
,201106260000008165
,201106260000005529
,201106260000006461
,201106260000009285
,201106260000008003
,201106260000003484
,201107100000006013
,201107100000004788
,201107100000007423
,201107100000008627
,201107100000005191
,201107100000002388
,201107100000006839
,201107100000007956
,201107100000005719
,201107100000005459
,201107100000004533
,201107100000004239
,201107100000005165
,201107100000008025
,201107100000002682
,201107100000004667
,201107100000003934
,201107100000005760
,201107100000007717
,201107110000007619
,201107110000005165
,201107110000005082
,201107110000002752
,201107110000007297
,201107110000005789
,201107110000002430
,201107110000004709
,201107110000004547
,201107110000008683
,201107110000007049
,201107110000004225
,201107110000007732
,201107110000005611
,201107110000003948
,201107110000005487
,201107110000006111
,201107110000005592
,201107110000007955
,201107110000003260
,201107110000007807
,201107110000003344
,201107240000007871
,201107240000005802
,201107240000008725
,201107240000007605
,201107240000008151
,201107240000004774
,201107240000003934
,201107240000005415
,201107240000005389
,201107240000006615
,201107240000004491
,201107240000004757
,201107240000005789
,201107240000002262
,201107240000005123
,201107240000002528
,201107240000004555
,201107240000008068
,201107240000005523
,201107250000007830
,201107250000005527
,201107250000003976
,201107250000002514
,201107250000005522
,201107250000004267
,201107250000004625
,201107250000006013
,201107250000004631
,201107250000007283
,201107250000008795
,201107250000005649
,201107250000005557
,201107250000002878
,201107250000006979
,201107250000008025
,201107250000007647
,201107250000005193
,201107250000005166
,201107250000007709
,201107250000003120
,201107310000005359
,201107310000004673
,201107310000007955
,201107310000005928
,201107310000002304
,201107310000004018
,201107310000005473
,201107310000008837
,201107310000006797
,201107310000008249
,201107310000008138
,201107310000007675
,201107310000002584
,201107310000005753
,201107310000004393
,201107310000005943
,201107310000004858
,201107310000004765
,201107310000005663
,201108010000002388
,201108010000004337
,201108010000005459
,201108010000002738
,201108010000005928
,201108010000007928
,201108010000008739
,201108010000004919
,201108010000005180
,201108010000007983
,201108010000005555
,201108010000006097
,201108010000007619
,201108010000005809
,201108010000007091
,201108010000005747
,201108010000004004
,201108010000004687
,201108010000007269
,201108010000003358
,201108010000007737
,201108280000007801
,201108280000005149
,201108280000004211
,201108280000005879
,201108280000002598
,201108280000005844
,201108280000007521
,201108280000004491
,201108280000004816
,201108280000005509
,201108280000003948
,201108280000008095
,201108280000008446
,201108280000008711
,201108280000002318
,201108280000006363
,201108280000004485
,201108280000005229
,201108280000005599
,201108290000004583
,201108290000005138
,201108290000008081
,201108290000005593
,201108290000005417
,201108290000006517
,201108290000005781
,201108290000002808
,201108290000004004
,201108290000005592
,201108290000008334
,201108290000004211
,201108290000005229
,201108290000007717
,201108290000008459
,201108290000004575
,201108290000009229
,201108290000002444
,201108290000005681
,201108290000007507
,201108290000007424
,201108290000002620
,201108290000008003
,201108290000005717
,201108290000003218
)
and video_playing_flag=1 and adjusted_event_start_time<>x_adjusted_event_end_time
and
   (    x_type_of_viewing_event in ('TV Channel Viewing','Sky+ time-shifted viewing event')
    or ( x_type_of_viewing_event = ('Other Service Viewing Event') and x_si_service_type = 'High Definition TV test service')
   )
and panel_id = 5
;

commit;

commit;
create hg index idx1 on vespa_analysts.F1_analysis_20111104(subscriber_id);
/*
-----Add on start/end of viewing activity----
delete from vespa_analysts.F1_analysis_20111104
where play_back_speed in (-60,-24,-12,-4,0,1,4,12,24,60)
;
commit;
*/
---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.F1_analysis_20111104 order by log_id;
alter table vespa_analysts.F1_analysis_20111104 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.F1_analysis_20111104 add viewing_record_start_time_local datetime;


alter table vespa_analysts.F1_analysis_20111104 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.F1_analysis_20111104 add viewing_record_end_time_local datetime;

update vespa_analysts.F1_analysis_20111104
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.F1_analysis_20111104
;
commit;


---
update vespa_analysts.F1_analysis_20111104
set viewing_record_end_time_utc= dateadd(second,x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.F1_analysis_20111104
;
commit;

--select top 100 * from vespa_analysts.F1_analysis_20111104;

update vespa_analysts.F1_analysis_20111104
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
from vespa_analysts.F1_analysis_20111104
;
commit;

alter table vespa_analysts.F1_analysis_20111104 add seconds_from_broadcast_to_event_start integer;
alter table vespa_analysts.F1_analysis_20111104 add seconds_from_broadcast_to_event_end integer;

update vespa_analysts.F1_analysis_20111104
set seconds_from_broadcast_to_event_start = datediff(second,tx_start_datetime_utc,viewing_record_start_time_utc)
,seconds_from_broadcast_to_event_end= datediff(second,tx_start_datetime_utc,viewing_record_end_time_utc)
from vespa_analysts.F1_analysis_20111104
commit;

---Create table of each viewing date-----
--select top 500 * from vespa_analysts.F1_analysis_20111104;
--drop table #all_subs_and_dates;
select account_number
,dateformat(adjusted_event_start_time,'YYYY-MM-DD' ) as event_date
into #all_subs_and_dates
from vespa_analysts.F1_analysis_20111104
group by account_number
,event_date
;

commit;
exec sp_create_tmp_table_idx '#all_subs_and_dates', 'account_number';
exec sp_create_tmp_table_idx '#all_subs_and_dates', 'event_date';
---get all primary and secondary sub details for all accounts with viewing on any box

select a.account_number
,b.service_instance_id
,min(case when effective_from_dt <=cast(event_date as date) and effective_to_dt>cast(event_date as date) 
then SUBSCRIPTION_SUB_TYPE else null end) as sub_type
into #all_boxes_info
from  #all_subs_and_dates as a
left outer join sk_prod.cust_subs_hist as b
on a.account_number = b.account_number
where b.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
group by a.account_number
,b.service_instance_id
;


commit;
exec sp_create_tmp_table_idx '#all_boxes_info', 'account_number';
exec sp_create_tmp_table_idx '#all_boxes_info', 'service_instance_id';

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
alter table vespa_analysts.F1_analysis_20111104 add subscription_type varchar(40);

update vespa_analysts.F1_analysis_20111104
set subscription_type=c.sub_type
from vespa_analysts.F1_analysis_20111104 as a
left outer join #subs_details as b
on a.subscriber_id=b.subscriberid
left outer join #all_boxes_info as c
on b.src_system_id=c.service_instance_id
;
commit;

--select top 500 * from vespa_analysts.F1_analysis_20111104 order by subscriber_id where play_back_speed=2;



----Spanish Grand Prix Minute By Minute Analysis----

---Generate Minute by minute summary for Qualifying---

create variable @min_tx_start_time datetime;
create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-05-21 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-05-21 13:15:00' as datetime);

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

---Loop by Channel---
--drop table vespa_analysts.vespa_spanish_gp_qualifying;

if object_id('vespa_analysts.vespa_spanish_gp_qualifying') is not null drop table vespa_analysts.vespa_spanish_gp_qualifying;
commit;
create table vespa_analysts.vespa_spanish_gp_qualifying
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_spanish_gp_qualifying
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc = '2011-05-21 11:10:00' and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

------Repeat for Race----

---Generate Minute by minute summary for Race---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-05-22 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-05-22 14:15:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_spanish_gp_qualifying;

if object_id('vespa_analysts.vespa_spanish_gp_race') is not null drop table vespa_analysts.vespa_spanish_gp_race;
commit;
create table vespa_analysts.vespa_spanish_gp_race
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_spanish_gp_race
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


------Repeat for highlights----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-05-22 18:00:00' as datetime);
set @max_tx_end_time =cast ('2011-05-22 19:00:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_spanish_gp_qualifying;

if object_id('vespa_analysts.vespa_spanish_gp_highlights') is not null drop table vespa_analysts.vespa_spanish_gp_highlights;
commit;
create table vespa_analysts.vespa_spanish_gp_highlights
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_spanish_gp_highlights
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


-----------------------------------------------------------


----monaco Grand Prix Minute By Minute Analysis----

---Generate Minute by minute summary for Qualifying---

--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-05-28 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-05-28 13:45:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_monaco_gp_qualifying;

if object_id('vespa_analysts.vespa_monaco_gp_qualifying') is not null drop table vespa_analysts.vespa_monaco_gp_qualifying;
commit;
create table vespa_analysts.vespa_monaco_gp_qualifying
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_monaco_gp_qualifying
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc = @min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

------Repeat for Race----

---Generate Minute by minute summary for Race---

--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-05-29 11:05:00' as datetime);
set @max_tx_end_time =cast ('2011-05-29 14:35:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_monaco_gp_qualifying;

if object_id('vespa_analysts.vespa_monaco_gp_race') is not null drop table vespa_analysts.vespa_monaco_gp_race;
commit;
create table vespa_analysts.vespa_monaco_gp_race
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_monaco_gp_race
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


------Repeat for highlights----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-05-29 18:00:00' as datetime);
set @max_tx_end_time =cast ('2011-05-29 19:00:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_monaco_gp_qualifying;

if object_id('vespa_analysts.vespa_monaco_gp_highlights') is not null drop table vespa_analysts.vespa_monaco_gp_highlights;
commit;
create table vespa_analysts.vespa_monaco_gp_highlights
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_monaco_gp_highlights
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;



-----------------------------------------------------------


----canadian Grand Prix Minute By Minute Analysis----

---Generate Minute by minute summary for Qualifying---

--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-06-11 16:15:00' as datetime);
set @max_tx_end_time =cast ('2011-06-11 18:15:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_canadian_gp_qualifying;

if object_id('vespa_analysts.vespa_canadian_gp_qualifying') is not null drop table vespa_analysts.vespa_canadian_gp_qualifying;
commit;
create table vespa_analysts.vespa_canadian_gp_qualifying
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_canadian_gp_qualifying
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc = @min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

------Repeat for Race----

---Generate Minute by minute summary for Race---

--select adjusted_event_start_time , count(*) as records from vespa_analysts.F1_analysis_20111104 where tx_date_time_utc = '2011-06-12 20:00:00' group by adjusted_event_start_time order by adjusted_event_start_time;


--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-06-12 16:00:00' as datetime);
set @max_tx_end_time =cast ('2011-06-12 21:30:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_canadian_gp_qualifying;

if object_id('vespa_analysts.vespa_canadian_gp_race') is not null drop table vespa_analysts.vespa_canadian_gp_race;
commit;
create table vespa_analysts.vespa_canadian_gp_race
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_canadian_gp_race
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc in (@min_tx_start_time,'2011-06-12 20:00:00') and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;




------Repeat for highlights----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-06-13 00:10:00' as datetime);
set @max_tx_end_time =cast ('2011-06-13 01:10:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_canadian_gp_qualifying;

if object_id('vespa_analysts.vespa_canadian_gp_highlights') is not null drop table vespa_analysts.vespa_canadian_gp_highlights;
commit;
create table vespa_analysts.vespa_canadian_gp_highlights
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_canadian_gp_highlights
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


-----------------------------------------------------------


----european Grand Prix Minute By Minute Analysis----

---Generate Minute by minute summary for Qualifying---

--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-06-25 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-06-25 13:20:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_european_gp_qualifying;

if object_id('vespa_analysts.vespa_european_gp_qualifying') is not null drop table vespa_analysts.vespa_european_gp_qualifying;
commit;
create table vespa_analysts.vespa_european_gp_qualifying
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_european_gp_qualifying
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc = @min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

------Repeat for Race----

---Generate Minute by minute summary for Race---

--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-06-26 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-06-26 14:30:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_european_gp_qualifying;

if object_id('vespa_analysts.vespa_european_gp_race') is not null drop table vespa_analysts.vespa_european_gp_race;
commit;
create table vespa_analysts.vespa_european_gp_race
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_european_gp_race
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


------Repeat for highlights----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-06-26 18:00:00' as datetime);
set @max_tx_end_time =cast ('2011-06-26 18:45:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_european_gp_qualifying;

if object_id('vespa_analysts.vespa_european_gp_highlights') is not null drop table vespa_analysts.vespa_european_gp_highlights;
commit;
create table vespa_analysts.vespa_european_gp_highlights
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_european_gp_highlights
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


-----------------------------------------------------------


----british Grand Prix Minute By Minute Analysis----

---Generate Minute by minute summary for Qualifying---

--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-09 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-07-09 14:00:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_british_gp_qualifying;

if object_id('vespa_analysts.vespa_british_gp_qualifying') is not null drop table vespa_analysts.vespa_british_gp_qualifying;
commit;
create table vespa_analysts.vespa_british_gp_qualifying
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_british_gp_qualifying
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc = @min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

------Repeat for Race----

---Generate Minute by minute summary for Race---

--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-10 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-07-10 14:30:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_british_gp_qualifying;

if object_id('vespa_analysts.vespa_british_gp_race') is not null drop table vespa_analysts.vespa_british_gp_race;
commit;
create table vespa_analysts.vespa_british_gp_race
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_british_gp_race
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


------Repeat for highlights (second set of highlights)----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-11 00:00:00' as datetime);
set @max_tx_end_time =cast ('2011-07-11 01:00:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_british_gp_qualifying;

if object_id('vespa_analysts.vespa_british_gp_highlights2') is not null drop table vespa_analysts.vespa_british_gp_highlights2;
commit;
create table vespa_analysts.vespa_british_gp_highlights2
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_british_gp_highlights2
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-10 18:00:00' as datetime);
set @max_tx_end_time =cast ('2011-07-10 19:00:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_british_gp_qualifying;

if object_id('vespa_analysts.vespa_british_gp_highlights') is not null drop table vespa_analysts.vespa_british_gp_highlights;
commit;
create table vespa_analysts.vespa_british_gp_highlights
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_british_gp_highlights
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

-----------------------------------------------------------


----german Grand Prix Minute By Minute Analysis----

---Generate Minute by minute summary for Qualifying---

--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-23 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-07-23 13:20:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_german_gp_qualifying;

if object_id('vespa_analysts.vespa_german_gp_qualifying') is not null drop table vespa_analysts.vespa_german_gp_qualifying;
commit;
create table vespa_analysts.vespa_german_gp_qualifying
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_german_gp_qualifying
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc = @min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

------Repeat for Race----

---Generate Minute by minute summary for Race---

--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-24 11:05:00' as datetime);
set @max_tx_end_time =cast ('2011-07-24 14:20:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_german_gp_qualifying;

if object_id('vespa_analysts.vespa_german_gp_race') is not null drop table vespa_analysts.vespa_german_gp_race;
commit;
create table vespa_analysts.vespa_german_gp_race
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_german_gp_race
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


------Repeat for highlights----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-24 18:00:00' as datetime);
set @max_tx_end_time =cast ('2011-07-24 19:00:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_german_gp_qualifying;

if object_id('vespa_analysts.vespa_german_gp_highlights') is not null drop table vespa_analysts.vespa_german_gp_highlights;
commit;
create table vespa_analysts.vespa_german_gp_highlights
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_german_gp_highlights
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;



-----------------------------------------------------------


----hungarian Grand Prix Minute By Minute Analysis----

---Generate Minute by minute summary for Qualifying---

--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-30 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-07-30 13:20:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_hungarian_gp_qualifying;

if object_id('vespa_analysts.vespa_hungarian_gp_qualifying') is not null drop table vespa_analysts.vespa_hungarian_gp_qualifying;
commit;
create table vespa_analysts.vespa_hungarian_gp_qualifying
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_hungarian_gp_qualifying
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc = @min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

------Repeat for Race----

---Generate Minute by minute summary for Race---

--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-31 11:05:00' as datetime);
set @max_tx_end_time =cast ('2011-07-31 14:25:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_hungarian_gp_qualifying;

if object_id('vespa_analysts.vespa_hungarian_gp_race') is not null drop table vespa_analysts.vespa_hungarian_gp_race;
commit;
create table vespa_analysts.vespa_hungarian_gp_race
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_hungarian_gp_race
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


------Repeat for highlights----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-07-31 18:00:00' as datetime);
set @max_tx_end_time =cast ('2011-07-31 19:00:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_hungarian_gp_qualifying;

if object_id('vespa_analysts.vespa_hungarian_gp_highlights') is not null drop table vespa_analysts.vespa_hungarian_gp_highlights;
commit;
create table vespa_analysts.vespa_hungarian_gp_highlights
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_hungarian_gp_highlights
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


-----------------------------------------------------------


----belgian Grand Prix Minute By Minute Analysis----

---Generate Minute by minute summary for Qualifying---

--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-08-27 11:10:00' as datetime);
set @max_tx_end_time =cast ('2011-08-27 13:15:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_belgian_gp_qualifying;

if object_id('vespa_analysts.vespa_belgian_gp_qualifying') is not null drop table vespa_analysts.vespa_belgian_gp_qualifying;
commit;
create table vespa_analysts.vespa_belgian_gp_qualifying
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_belgian_gp_qualifying
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc = @min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

------Repeat for Race----

---Generate Minute by minute summary for Race---

--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-08-28 11:05:00' as datetime);
set @max_tx_end_time =cast ('2011-08-28 14:30:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_belgian_gp_qualifying;

if object_id('vespa_analysts.vespa_belgian_gp_race') is not null drop table vespa_analysts.vespa_belgian_gp_race;
commit;
create table vespa_analysts.vespa_belgian_gp_race
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_belgian_gp_race
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


------Repeat for highlights----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-08-28 16:30:00' as datetime);
set @max_tx_end_time =cast ('2011-08-28 17:30:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_belgian_gp_qualifying;

if object_id('vespa_analysts.vespa_belgian_gp_highlights') is not null drop table vespa_analysts.vespa_belgian_gp_highlights;
commit;
create table vespa_analysts.vespa_belgian_gp_highlights
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_belgian_gp_highlights
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;


------Repeat for highlights (second set of highlights----

---Generate Minute by minute summary for highlights---
--drop variable @min_tx_start_time; drop variable @max_tx_end_time; drop variable @min_tx_start_time_local; drop variable @max_tx_end_time_local; drop variable @minute; 
--create variable @min_tx_start_time datetime;
--create variable @max_tx_end_time datetime;

set @min_tx_start_time = cast ('2011-08-28 16:30:00' as datetime);
set @max_tx_end_time =cast ('2011-08-28 17:30:00' as datetime);

--create variable @min_tx_start_time_local datetime;
--create variable @max_tx_end_time_local datetime;
--create variable @minute datetime;
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

---Loop by Channel---
--drop table vespa_analysts.vespa_belgian_gp_qualifying;

if object_id('vespa_analysts.vespa_belgian_gp_highlights2') is not null drop table vespa_analysts.vespa_belgian_gp_highlights2;
commit;
create table vespa_analysts.vespa_belgian_gp_highlights2
(
subscriber_id  bigint           null
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live         smallint            not null
,seconds_viewed_in_minute_playback          smallint            not null
);
commit;



---Start of Loop
--drop table vespa_analysts.vespa_phase1b_minute_by_minute_by_channel;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @min_tx_start_time_local;


---Loop by Minute---
    WHILE @minute < @max_tx_end_time_local LOOP
    insert into vespa_analysts.vespa_belgian_gp_highlights2
    select subscriber_id
    ,@minute as minute
    ,sum(case when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute
    ,sum(case when play_back_speed =2 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_live
    ,sum(case when play_back_speed is null then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute)
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0
    end) as seconds_viewed_in_minute_playback



--,max(case when play_back_speed is null  and dateadd(hour,1,adjusted_event_start_time)> tx_start_datetime_utc then 1 else 0 end) as viewed_live_1hr_cap

from vespa_analysts.F1_analysis_20111104
where tx_date_time_utc =@min_tx_start_time and (play_back_speed is null or play_back_speed =2 ) and
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute))
    group by subscriber_id
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

-----Section 2-----

-----Derive which boxes returned data each day----
----Box Return data---

---Which boxes return data for each of the days that a Grand Prix takes place---

select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29

into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
from sk_prod.VESPA_STB_PROG_EVENTS_20110521
group by subscriber_id

;commit;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110522
group by subscriber_id

;




insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110523
group by subscriber_id

;



insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110528
group by subscriber_id

;



insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110529
group by subscriber_id

;



insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110530
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110611
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110612
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110613
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110625
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110626
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110627
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110709
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110710
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110711
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110723
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110724
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110725
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110730
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110731
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110801
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110827
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110828
group by subscriber_id

;


insert into vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-21 05:00:00' and '2011-05-22 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-21 05:00:00' and  adjusted_event_start_time <'2011-05-22 04:59:59' then 1
else 0
 end) as events_2011_05_21

,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-22 05:00:00' and '2011-05-23 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-22 05:00:00' and  adjusted_event_start_time <'2011-05-23 04:59:59' then 1
else 0
 end) as events_2011_05_22

--Monaco GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-28 05:00:00' and '2011-05-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-28 05:00:00' and  adjusted_event_start_time <'2011-05-29 04:59:59' then 1
else 0
 end) as events_2011_05_28
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-05-29 05:00:00' and '2011-05-30 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-05-29 05:00:00' and  adjusted_event_start_time <'2011-05-30 04:59:59' then 1
else 0
 end) as events_2011_05_29

--Canadian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-11 05:00:00' and '2011-06-12 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-11 05:00:00' and  adjusted_event_start_time <'2011-06-12 04:59:59' then 1
else 0
 end) as events_2011_06_11
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-12 05:00:00' and '2011-06-13 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-12 05:00:00' and  adjusted_event_start_time <'2011-06-13 04:59:59' then 1
else 0
 end) as events_2011_06_12

--European GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-25 05:00:00' and '2011-06-26 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-25 05:00:00' and  adjusted_event_start_time <'2011-06-26 04:59:59' then 1
else 0
 end) as events_2011_06_25
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-06-26 05:00:00' and '2011-06-27 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-06-26 05:00:00' and  adjusted_event_start_time <'2011-06-27 04:59:59' then 1
else 0
 end) as events_2011_06_26

--British GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-09 05:00:00' and '2011-07-10 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-09 05:00:00' and  adjusted_event_start_time <'2011-07-10 04:59:59' then 1
else 0
 end) as events_2011_07_09
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-10 05:00:00' and '2011-07-11 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-10 05:00:00' and  adjusted_event_start_time <'2011-07-11 04:59:59' then 1
else 0
 end) as events_2011_07_10

--German GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-23 05:00:00' and '2011-07-24 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-23 05:00:00' and  adjusted_event_start_time <'2011-07-24 04:59:59' then 1
else 0
 end) as events_2011_07_23
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-24 05:00:00' and '2011-07-25 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-24 05:00:00' and  adjusted_event_start_time <'2011-07-25 04:59:59' then 1
else 0
 end) as events_2011_07_24

--Hungarian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-30 05:00:00' and '2011-07-31 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-30 05:00:00' and  adjusted_event_start_time <'2011-07-31 04:59:59' then 1
else 0
 end) as events_2011_07_30
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-07-31 05:00:00' and '2011-08-01 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-07-31 05:00:00' and  adjusted_event_start_time <'2011-08-01 04:59:59' then 1
else 0
 end) as events_2011_07_31

--Belgian GP
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-27 05:00:00' and '2011-08-28 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-27 05:00:00' and  adjusted_event_start_time <'2011-08-28 04:59:59' then 1
else 0
 end) as events_2011_08_27
,max(case   when event_type = 'evEmptyLog' and stb_log_creation_date between '2011-08-28 05:00:00' and '2011-08-29 04:59:59' then 1
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-28 05:00:00' and  adjusted_event_start_time <'2011-08-29 04:59:59' then 1
else 0
 end) as events_2011_08_29


from sk_prod.VESPA_STB_PROG_EVENTS_20110829
group by subscriber_id

;




----Create overall summary from daily versions---
select subscriber_id
--,min(adjusted_event_start_time) as first_event_date

--Spanish GP
,max(events_2011_05_21) as events_2011_05_21_any

,max(events_2011_05_22) as events_2011_05_22_any

--Monaco GP
,max(events_2011_05_28) as events_2011_05_28_any
,max(events_2011_05_29) as events_2011_05_29_any

--Canadian GP
,max(events_2011_06_11) as events_2011_06_11_any
,max(events_2011_06_12) as events_2011_06_12_any

--European GP
,max(events_2011_06_25) as events_2011_06_25_any
,max(events_2011_06_26) as events_2011_06_26_any

--British GP
,max(events_2011_07_09) as events_2011_07_09_any
,max(events_2011_07_10) as events_2011_07_10_any

--German GP
,max(events_2011_07_23) as events_2011_07_23_any
,max(events_2011_07_24) as events_2011_07_24_any

--Hungarian GP
,max(events_2011_07_30) as events_2011_07_30_any
,max(events_2011_07_31) as events_2011_07_31_any

--Belgian GP
,max(events_2011_08_27) as events_2011_08_27_any
,max(events_2011_08_29) as events_2011_08_29_any

into vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_full
group by subscriber_id

;
commit;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped rename events_2011_08_29_any to events_2011_08_28_any;
commit;

--select top 100 * from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped;
--select sum(events_2011_05_22_any),sum(events_2011_08_29_any) from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped;
drop table  vespa_analysts.daily_summary_by_subscriber_f1_event_days_full;
commit;

-----Section 3----

----Create Minute by Minute Outputs per programme----

---Create Summary by subscriber for each Programme---

---Spanish---
---Qualifying---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #spanish_qualifying_by_minute
from vespa_analysts.vespa_spanish_gp_qualifying
group by minute
order by minute
;

---Race---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #spanish_race_by_minute
from vespa_analysts.vespa_spanish_gp_race
group by minute
order by minute
;


---Highlights---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #spanish_highlights_by_minute
from vespa_analysts.vespa_spanish_gp_Highlights
group by minute
order by minute
;

----Summarise by Subscriber_level---
---Qualifying---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_spanish
from vespa_analysts.vespa_spanish_gp_qualifying
group by subscriber_id
;

---Race---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race_spanish
from vespa_analysts.vespa_spanish_gp_race
group by subscriber_id
;

---Highlights---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights_spanish
from vespa_analysts.vespa_spanish_gp_highlights
group by subscriber_id
;



---monaco---
---Qualifying---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #monaco_qualifying_by_minute
from vespa_analysts.vespa_monaco_gp_qualifying
group by minute
order by minute
;

---Race---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #monaco_race_by_minute
from vespa_analysts.vespa_monaco_gp_race
group by minute
order by minute
;


---Highlights---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #monaco_highlights_by_minute
from vespa_analysts.vespa_monaco_gp_Highlights
group by minute
order by minute
;

----Summarise by Subscriber_level---
---Qualifying---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_monaco
from vespa_analysts.vespa_monaco_gp_qualifying
group by subscriber_id
;

---Race---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race_monaco
from vespa_analysts.vespa_monaco_gp_race
group by subscriber_id
;

---Highlights---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights_monaco
from vespa_analysts.vespa_monaco_gp_highlights
group by subscriber_id
;








---canadian---
---Qualifying---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #canadian_qualifying_by_minute
from vespa_analysts.vespa_canadian_gp_qualifying
group by minute
order by minute
;

---Race---
--drop table #canadian_race_by_minute;
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #canadian_race_by_minute
from vespa_analysts.vespa_canadian_gp_race
group by minute
order by minute
;


---Highlights---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #canadian_highlights_by_minute
from vespa_analysts.vespa_canadian_gp_Highlights
group by minute
order by minute
;

----Summarise by Subscriber_level---
---Qualifying---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_canadian
from vespa_analysts.vespa_canadian_gp_qualifying
group by subscriber_id
;

---Race---
--drop table #sub_summary_race_canadian;
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race_canadian
from vespa_analysts.vespa_canadian_gp_race
group by subscriber_id
;

---Highlights---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights_canadian
from vespa_analysts.vespa_canadian_gp_highlights
group by subscriber_id
;





---european---
---Qualifying---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #european_qualifying_by_minute
from vespa_analysts.vespa_european_gp_qualifying
group by minute
order by minute
;

---Race---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #european_race_by_minute
from vespa_analysts.vespa_european_gp_race
group by minute
order by minute
;


---Highlights---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #european_highlights_by_minute
from vespa_analysts.vespa_european_gp_Highlights
group by minute
order by minute
;

----Summarise by Subscriber_level---
---Qualifying---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_european
from vespa_analysts.vespa_european_gp_qualifying
group by subscriber_id
;

---Race---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race_european
from vespa_analysts.vespa_european_gp_race
group by subscriber_id
;

---Highlights---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights_european
from vespa_analysts.vespa_european_gp_highlights
group by subscriber_id
;




---british---
---Qualifying---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #british_qualifying_by_minute
from vespa_analysts.vespa_british_gp_qualifying
group by minute
order by minute
;

---Race---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #british_race_by_minute
from vespa_analysts.vespa_british_gp_race
group by minute
order by minute
;


---Highlights---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #british_highlights_by_minute
from vespa_analysts.vespa_british_gp_Highlights
group by minute
order by minute
;

----Summarise by Subscriber_level---
---Qualifying---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_british
from vespa_analysts.vespa_british_gp_qualifying
group by subscriber_id
;

---Race---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race_british
from vespa_analysts.vespa_british_gp_race
group by subscriber_id
;

---Highlights---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights_british
from vespa_analysts.vespa_british_gp_highlights
group by subscriber_id
;






---german---
---Qualifying---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #german_qualifying_by_minute
from vespa_analysts.vespa_german_gp_qualifying
group by minute
order by minute
;

---Race---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #german_race_by_minute
from vespa_analysts.vespa_german_gp_race
group by minute
order by minute
;


---Highlights---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #german_highlights_by_minute
from vespa_analysts.vespa_german_gp_Highlights
group by minute
order by minute
;

----Summarise by Subscriber_level---
---Qualifying---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_german
from vespa_analysts.vespa_german_gp_qualifying
group by subscriber_id
;

---Race---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race_german
from vespa_analysts.vespa_german_gp_race
group by subscriber_id
;

---Highlights---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights_german
from vespa_analysts.vespa_german_gp_highlights
group by subscriber_id
;



---hungarian---
---Qualifying---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #hungarian_qualifying_by_minute
from vespa_analysts.vespa_hungarian_gp_qualifying
group by minute
order by minute
;

---Race---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #hungarian_race_by_minute
from vespa_analysts.vespa_hungarian_gp_race
group by minute
order by minute
;


---Highlights---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #hungarian_highlights_by_minute
from vespa_analysts.vespa_hungarian_gp_Highlights
group by minute
order by minute
;

----Summarise by Subscriber_level---
---Qualifying---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_hungarian
from vespa_analysts.vespa_hungarian_gp_qualifying
group by subscriber_id
;

---Race---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race_hungarian
from vespa_analysts.vespa_hungarian_gp_race
group by subscriber_id
;

---Highlights---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights_hungarian
from vespa_analysts.vespa_hungarian_gp_highlights
group by subscriber_id
;





---belgian---
---Qualifying---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #belgian_qualifying_by_minute
from vespa_analysts.vespa_belgian_gp_qualifying
group by minute
order by minute
;

---Race---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #belgian_race_by_minute
from vespa_analysts.vespa_belgian_gp_race
group by minute
order by minute
;


---Highlights---
select minute
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #belgian_highlights_by_minute
from vespa_analysts.vespa_belgian_gp_Highlights
group by minute
order by minute
;

----Summarise by Subscriber_level---
---Qualifying---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_belgian
from vespa_analysts.vespa_belgian_gp_qualifying
group by subscriber_id
;

---Race---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race_belgian
from vespa_analysts.vespa_belgian_gp_race
group by subscriber_id
;

---Highlights---
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback =0
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback<>0 then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights_belgian
from vespa_analysts.vespa_belgian_gp_highlights
group by subscriber_id
;


----Section 4-----
--select count(*) from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped;
---Add viewing data to master table of Box information generated in Section 2

/*
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_qualifying_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_qualifying_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_qualifying_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_qualifying_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_qualifying_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_qualifying_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_qualifying_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_qualifying_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_qualifying_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_qualifying_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_qualifying_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_qualifying_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_qualifying_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_qualifying_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_qualifying_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_qualifying_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_qualifying_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_qualifying_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_qualifying_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_qualifying_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_qualifying_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_qualifying_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_qualifying_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_qualifying_minutes_playback integer default 0;
*/




update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set spanish_gp_qualifying_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,spanish_gp_qualifying_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,spanish_gp_qualifying_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_qualifying_spanish as b
on a.subscriber_id=b.subscriber_id
;


update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set monaco_gp_qualifying_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,monaco_gp_qualifying_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,monaco_gp_qualifying_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_qualifying_monaco as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set canadian_gp_qualifying_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,canadian_gp_qualifying_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,canadian_gp_qualifying_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_qualifying_canadian as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set european_gp_qualifying_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,european_gp_qualifying_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,european_gp_qualifying_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_qualifying_european as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set british_gp_qualifying_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,british_gp_qualifying_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,british_gp_qualifying_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_qualifying_british as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set german_gp_qualifying_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,german_gp_qualifying_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,german_gp_qualifying_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_qualifying_german as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set hungarian_gp_qualifying_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,hungarian_gp_qualifying_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,hungarian_gp_qualifying_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_qualifying_hungarian as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set belgian_gp_qualifying_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,belgian_gp_qualifying_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,belgian_gp_qualifying_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_qualifying_belgian as b
on a.subscriber_id=b.subscriber_id
;


----Repeat for Race----

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_race_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_race_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_race_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_race_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_race_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_race_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_race_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_race_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_race_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_race_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_race_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_race_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_race_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_race_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_race_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_race_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_race_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_race_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_race_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_race_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_race_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_race_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_race_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_race_minutes_playback integer default 0;





update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set spanish_gp_race_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,spanish_gp_race_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,spanish_gp_race_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_race_spanish as b
on a.subscriber_id=b.subscriber_id
;


update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set monaco_gp_race_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,monaco_gp_race_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,monaco_gp_race_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_race_monaco as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set canadian_gp_race_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,canadian_gp_race_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,canadian_gp_race_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_race_canadian as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set european_gp_race_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,european_gp_race_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,european_gp_race_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_race_european as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set british_gp_race_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,british_gp_race_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,british_gp_race_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_race_british as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set german_gp_race_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,german_gp_race_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,german_gp_race_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_race_german as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set hungarian_gp_race_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,hungarian_gp_race_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,hungarian_gp_race_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_race_hungarian as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set belgian_gp_race_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,belgian_gp_race_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,belgian_gp_race_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_race_belgian as b
on a.subscriber_id=b.subscriber_id
;


----Repeat for highlights----

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_highlights_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_highlights_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add spanish_gp_highlights_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_highlights_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_highlights_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add monaco_gp_highlights_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_highlights_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_highlights_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add canadian_gp_highlights_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_highlights_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_highlights_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add european_gp_highlights_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_highlights_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_highlights_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add british_gp_highlights_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_highlights_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_highlights_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_gp_highlights_minutes_playback integer default 0;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_highlights_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_highlights_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add hungarian_gp_highlights_minutes_playback integer default 0;


alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_highlights_minutes_all integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_highlights_minutes_live integer default 0;
alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add belgian_gp_highlights_minutes_playback integer default 0;





update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set spanish_gp_highlights_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,spanish_gp_highlights_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,spanish_gp_highlights_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_highlights_spanish as b
on a.subscriber_id=b.subscriber_id
;


update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set monaco_gp_highlights_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,monaco_gp_highlights_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,monaco_gp_highlights_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_highlights_monaco as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set canadian_gp_highlights_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,canadian_gp_highlights_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,canadian_gp_highlights_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_highlights_canadian as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set european_gp_highlights_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,european_gp_highlights_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,european_gp_highlights_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_highlights_european as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set british_gp_highlights_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,british_gp_highlights_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,british_gp_highlights_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_highlights_british as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set german_gp_highlights_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,german_gp_highlights_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,german_gp_highlights_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_highlights_german as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set hungarian_gp_highlights_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,hungarian_gp_highlights_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,hungarian_gp_highlights_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_highlights_hungarian as b
on a.subscriber_id=b.subscriber_id
;

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set belgian_gp_highlights_minutes_all = case when b.minutes_viewed is null then 0 else b.minutes_viewed end
,belgian_gp_highlights_minutes_live = case when b.minutes_viewed_live is null then 0 else b.minutes_viewed_live end
,belgian_gp_highlights_minutes_playback = case when b.minutes_viewed_playback is null then 0 else b.minutes_viewed_playback end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a
left outer join #sub_summary_highlights_belgian as b
on a.subscriber_id=b.subscriber_id
;




--select top 100 * from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped;

-- Output minute by minute summaries------

select * from #monaco_qualifying_by_minute order by minute;
select * from #monaco_race_by_minute order by minute;
select * from #monaco_highlights_by_minute order by minute;

commit;
select * from #canadian_qualifying_by_minute order by minute;
select * from #canadian_race_by_minute order by minute;
select * from #canadian_highlights_by_minute order by minute;


select * from #european_qualifying_by_minute order by minute;
select * from #european_race_by_minute order by minute;
select * from #european_highlights_by_minute order by minute;


select * from #british_qualifying_by_minute order by minute;
select * from #british_race_by_minute order by minute;
select * from #british_highlights_by_minute order by minute;


select * from #german_qualifying_by_minute order by minute;
select * from #german_race_by_minute order by minute;
select * from #german_highlights_by_minute order by minute;


select * from #hungarian_qualifying_by_minute order by minute;
select * from #hungarian_race_by_minute order by minute;
select * from #hungarian_highlights_by_minute order by minute;


select * from #belgian_qualifying_by_minute order by minute;
select * from #belgian_race_by_minute order by minute;
select * from #belgian_highlights_by_minute order by minute;



----Section 5 -- Individual GP analysis---


----Spanish---

--Only Use those who have returned data for both the Saturday and the Sunday---

select case when spanish_gp_qualifying_minutes_all =0 then 'a: Not Watched' when spanish_gp_qualifying_minutes_all <=5 then 'b: 1-5 Minutes' 
                when spanish_gp_qualifying_minutes_all <=15 then 'c: 6-15 Minutes' 
when spanish_gp_qualifying_minutes_all <=30 then 'd: 16-30 Minutes' 
when spanish_gp_qualifying_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as qualifying_minutes

, case when spanish_gp_race_minutes_all =0 then 'a: Not Watched'  when spanish_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when spanish_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when spanish_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when spanish_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes

,case when spanish_gp_highlights_minutes_all =0 then 'a: Not Watched' when spanish_gp_highlights_minutes_all <=5 then 'b: 1-5 Minutes' 
                when spanish_gp_highlights_minutes_all <=15 then 'c: 6-15 Minutes' 
when spanish_gp_highlights_minutes_all <=30 then 'd: 16-30 Minutes' 
when spanish_gp_highlights_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as highlights_minutes
,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_05_21_any = 1 and events_2011_05_22_any=1 
group by qualifying_minutes,race_minutes,highlights_minutes
order by qualifying_minutes,race_minutes,highlights_minutes
;


---Monaco
select case when monaco_gp_qualifying_minutes_all =0 then 'a: Not Watched' when monaco_gp_qualifying_minutes_all <=5 then 'b: 1-5 Minutes' 
                when monaco_gp_qualifying_minutes_all <=15 then 'c: 6-15 Minutes' 
when monaco_gp_qualifying_minutes_all <=30 then 'd: 16-30 Minutes' 
when monaco_gp_qualifying_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as qualifying_minutes

, case when monaco_gp_race_minutes_all =0 then 'a: Not Watched'  when monaco_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when monaco_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when monaco_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when monaco_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes

,case when monaco_gp_highlights_minutes_all =0 then 'a: Not Watched' when monaco_gp_highlights_minutes_all <=5 then 'b: 1-5 Minutes' 
                when monaco_gp_highlights_minutes_all <=15 then 'c: 6-15 Minutes' 
when monaco_gp_highlights_minutes_all <=30 then 'd: 16-30 Minutes' 
when monaco_gp_highlights_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as highlights_minutes
,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_05_28_any = 1 and events_2011_05_29_any=1 
group by qualifying_minutes,race_minutes,highlights_minutes
order by qualifying_minutes,race_minutes,highlights_minutes
;

commit;



---canadian
select case when canadian_gp_qualifying_minutes_all =0 then 'a: Not Watched' when canadian_gp_qualifying_minutes_all <=5 then 'b: 1-5 Minutes' 
                when canadian_gp_qualifying_minutes_all <=15 then 'c: 6-15 Minutes' 
when canadian_gp_qualifying_minutes_all <=30 then 'd: 16-30 Minutes' 
when canadian_gp_qualifying_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as qualifying_minutes

, case when canadian_gp_race_minutes_all =0 then 'a: Not Watched'  when canadian_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when canadian_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when canadian_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when canadian_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes

,case when canadian_gp_highlights_minutes_all =0 then 'a: Not Watched' when canadian_gp_highlights_minutes_all <=5 then 'b: 1-5 Minutes' 
                when canadian_gp_highlights_minutes_all <=15 then 'c: 6-15 Minutes' 
when canadian_gp_highlights_minutes_all <=30 then 'd: 16-30 Minutes' 
when canadian_gp_highlights_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as highlights_minutes
,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_06_11_any = 1 and events_2011_06_12_any=1 
group by qualifying_minutes,race_minutes,highlights_minutes
order by qualifying_minutes,race_minutes,highlights_minutes
;


---european
select case when european_gp_qualifying_minutes_all =0 then 'a: Not Watched' when european_gp_qualifying_minutes_all <=5 then 'b: 1-5 Minutes' 
                when european_gp_qualifying_minutes_all <=15 then 'c: 6-15 Minutes' 
when european_gp_qualifying_minutes_all <=30 then 'd: 16-30 Minutes' 
when european_gp_qualifying_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as qualifying_minutes

, case when european_gp_race_minutes_all =0 then 'a: Not Watched'  when european_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when european_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when european_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when european_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes

,case when european_gp_highlights_minutes_all =0 then 'a: Not Watched' when european_gp_highlights_minutes_all <=5 then 'b: 1-5 Minutes' 
                when european_gp_highlights_minutes_all <=15 then 'c: 6-15 Minutes' 
when european_gp_highlights_minutes_all <=30 then 'd: 16-30 Minutes' 
when european_gp_highlights_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as highlights_minutes
,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_06_25_any = 1 and events_2011_06_26_any=1 
group by qualifying_minutes,race_minutes,highlights_minutes
order by qualifying_minutes,race_minutes,highlights_minutes
;



---british
select case when british_gp_qualifying_minutes_all =0 then 'a: Not Watched' when british_gp_qualifying_minutes_all <=5 then 'b: 1-5 Minutes' 
                when british_gp_qualifying_minutes_all <=15 then 'c: 6-15 Minutes' 
when british_gp_qualifying_minutes_all <=30 then 'd: 16-30 Minutes' 
when british_gp_qualifying_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as qualifying_minutes

, case when british_gp_race_minutes_all =0 then 'a: Not Watched'  when british_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when british_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when british_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when british_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes

,case when british_gp_highlights_minutes_all =0 then 'a: Not Watched' when british_gp_highlights_minutes_all <=5 then 'b: 1-5 Minutes' 
                when british_gp_highlights_minutes_all <=15 then 'c: 6-15 Minutes' 
when british_gp_highlights_minutes_all <=30 then 'd: 16-30 Minutes' 
when british_gp_highlights_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as highlights_minutes
,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_07_09_any = 1 and events_2011_07_10_any=1 
group by qualifying_minutes,race_minutes,highlights_minutes
order by qualifying_minutes,race_minutes,highlights_minutes
;



---german
select case when german_gp_qualifying_minutes_all =0 then 'a: Not Watched' when german_gp_qualifying_minutes_all <=5 then 'b: 1-5 Minutes' 
                when german_gp_qualifying_minutes_all <=15 then 'c: 6-15 Minutes' 
when german_gp_qualifying_minutes_all <=30 then 'd: 16-30 Minutes' 
when german_gp_qualifying_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as qualifying_minutes

, case when german_gp_race_minutes_all =0 then 'a: Not Watched'  when german_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when german_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when german_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when german_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes

,case when german_gp_highlights_minutes_all =0 then 'a: Not Watched' when german_gp_highlights_minutes_all <=5 then 'b: 1-5 Minutes' 
                when german_gp_highlights_minutes_all <=15 then 'c: 6-15 Minutes' 
when german_gp_highlights_minutes_all <=30 then 'd: 16-30 Minutes' 
when german_gp_highlights_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as highlights_minutes
,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_07_23_any = 1 and events_2011_07_24_any=1 
group by qualifying_minutes,race_minutes,highlights_minutes
order by qualifying_minutes,race_minutes,highlights_minutes
;


---hungarian
select case when hungarian_gp_qualifying_minutes_all =0 then 'a: Not Watched' when hungarian_gp_qualifying_minutes_all <=5 then 'b: 1-5 Minutes' 
                when hungarian_gp_qualifying_minutes_all <=15 then 'c: 6-15 Minutes' 
when hungarian_gp_qualifying_minutes_all <=30 then 'd: 16-30 Minutes' 
when hungarian_gp_qualifying_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as qualifying_minutes

, case when hungarian_gp_race_minutes_all =0 then 'a: Not Watched'  when hungarian_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when hungarian_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when hungarian_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when hungarian_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes

,case when hungarian_gp_highlights_minutes_all =0 then 'a: Not Watched' when hungarian_gp_highlights_minutes_all <=5 then 'b: 1-5 Minutes' 
                when hungarian_gp_highlights_minutes_all <=15 then 'c: 6-15 Minutes' 
when hungarian_gp_highlights_minutes_all <=30 then 'd: 16-30 Minutes' 
when hungarian_gp_highlights_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as highlights_minutes
,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_07_30_any = 1 and events_2011_07_31_any=1 
group by qualifying_minutes,race_minutes,highlights_minutes
order by qualifying_minutes,race_minutes,highlights_minutes
;



---Belgian
select case when Belgian_gp_qualifying_minutes_all =0 then 'a: Not Watched' when Belgian_gp_qualifying_minutes_all <=5 then 'b: 1-5 Minutes' 
                when Belgian_gp_qualifying_minutes_all <=15 then 'c: 6-15 Minutes' 
when Belgian_gp_qualifying_minutes_all <=30 then 'd: 16-30 Minutes' 
when Belgian_gp_qualifying_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as qualifying_minutes

, case when Belgian_gp_race_minutes_all =0 then 'a: Not Watched'  when Belgian_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when Belgian_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when Belgian_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when Belgian_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes

,case when Belgian_gp_highlights_minutes_all =0 then 'a: Not Watched' when Belgian_gp_highlights_minutes_all <=5 then 'b: 1-5 Minutes' 
                when Belgian_gp_highlights_minutes_all <=15 then 'c: 6-15 Minutes' 
when Belgian_gp_highlights_minutes_all <=30 then 'd: 16-30 Minutes' 
when Belgian_gp_highlights_minutes_all >30 then 'e: Over 30 Minutes' else 'f: Other' end as highlights_minutes
,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_08_27_any = 1 and events_2011_08_28_any=1 
group by qualifying_minutes,race_minutes,highlights_minutes
order by qualifying_minutes,race_minutes,highlights_minutes
;

commit;

----Race Viewing for German/Hungarian/Belgian

select case when german_gp_race_minutes_all =0 then 'a: Not Watched'  when german_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when german_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when german_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when german_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes_german

,case when hungarian_gp_race_minutes_all =0 then 'a: Not Watched'  when hungarian_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when hungarian_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when hungarian_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when hungarian_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes_hungarian

, case when Belgian_gp_race_minutes_all =0 then 'a: Not Watched'  when Belgian_gp_race_minutes_all <=5 then 'b: 1-5 Minutes' 
                when Belgian_gp_race_minutes_all <=30 then 'c: 6-30 Minutes' 
when Belgian_gp_race_minutes_all <=60 then 'd: 31-60 Minutes' 
when Belgian_gp_race_minutes_all >60 then 'e: Over 60 Minutes' else 'f: Other' end as race_minutes_belgian


,count(*) as records
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
where events_2011_07_24_any=1  and events_2011_07_31_any=1 and events_2011_08_28_any=1 
group by race_minutes_german,race_minutes_hungarian,race_minutes_belgian
order by race_minutes_german,race_minutes_hungarian,race_minutes_belgian
;

commit;

alter table vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped add german_hungarian_belgian_segments varchar (32);

update vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
set german_hungarian_belgian_segments = case when events_2011_07_24_any  + events_2011_07_31_any + events_2011_08_28_any<3 then '9: Not full data'
            when german_gp_race_minutes_all>60 and hungarian_gp_race_minutes_all>60 and belgian_gp_race_minutes_all>60 then '1: Over 1hr for each GP'
            when german_gp_race_minutes_all>30 and hungarian_gp_race_minutes_all>30 and belgian_gp_race_minutes_all>30 then '2: Over 30 min for each GP'
            when german_gp_race_minutes_all>60 and hungarian_gp_race_minutes_all>60  then '3: Over 1hr for 2 GP'
            when hungarian_gp_race_minutes_all>60 and belgian_gp_race_minutes_all>60  then '3: Over 1hr for 2 GP'
            when german_gp_race_minutes_all>60 and belgian_gp_race_minutes_all>60  then     '3: Over 1hr for 2 GP'

            when german_gp_race_minutes_all>30 and hungarian_gp_race_minutes_all>30  then '4: Over 30 min for 2 GP'
            when hungarian_gp_race_minutes_all>30 and belgian_gp_race_minutes_all>30  then '4: Over 30 min for 2 GP'
            when german_gp_race_minutes_all>30 and belgian_gp_race_minutes_all>30  then     '4: Over 30 min for 2 GP'

            when german_gp_race_minutes_all>60  then '5: Over 1hr for 1 GP'
            when hungarian_gp_race_minutes_all>60 then '5: Over 1hr for 1 GP'
            when belgian_gp_race_minutes_all>60 then '5: Over 1hr for 1 GP'

            when german_gp_race_minutes_all>30  then '6: Over 30 min for 1 GP'
            when hungarian_gp_race_minutes_all>30 then '6: Over 30 min for 1 GP'
            when belgian_gp_race_minutes_all>30 then '6: Over 30 min for 1 GP'

            when german_gp_race_minutes_all>5 and hungarian_gp_race_minutes_all>5 and belgian_gp_race_minutes_all>5 then '7: All watched over 5 min'

            else '8: Other' end
from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped
;

commit;





--alter table add vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped pvr tinyint;
--case when x_pvr_type= 'N/A' then 0 else 1 end as pvr



----Run profile as at date of German GP (24th July)-----


--1.    Create list of all subscriptions active (AC/PC exclude Active Block?) at start of day in question � taken from cust_subs_hist

if object_id('vespa_analysts.uk_base_20110724') is not null drop table vespa_analysts.uk_base_20110724;
select account_number
,service_instance_id
,SUBSCRIPTION_SUB_TYPE
into vespa_analysts.uk_base_20110724
from  sk_prod.cust_subs_hist
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
and effective_from_dt<=cast('2011-07-23' as date)
and effective_to_dt>cast('2011-07-23' as date)
and status_code in ('AC','PC')
;

--select distinct SUBSCRIPTION_SUB_TYPE from sk_prod.cust_subs_hist


--2.    Add on country code � from cust_single_account_view only Include UK standard accounts e.g, exclude VIP/Staff

alter table vespa_analysts.uk_base_20110724 add uk_standard_account tinyint default 0;

update vespa_analysts.uk_base_20110724
set uk_standard_account =case when b.acct_type='Standard' and b.account_number <>'?' and b.pty_country_code ='GBR' then 1 else 0 end
from vespa_analysts.uk_base_20110724 as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;

commit;

--3.    Remove non-uk subscriptions to create the �UK Active Base�

delete from vespa_analysts.uk_base_20110724 where uk_standard_account=0;

commit;




---Add Index on service_instance_id--


create hg index idx1 on vespa_analysts.uk_base_20110724(service_instance_id);
commit;


--select top 5000 * from vespa_analysts.uk_base_20110724;


--select SUBSCRIPTION_SUB_TYPE, count(*) from vespa_analysts.uk_base_20110724 group by SUBSCRIPTION_SUB_TYPE;
--select  count(*),count(distinct service_instance_id) from vespa_analysts.uk_base_20110724;

--4.    Add on box details � most recent dw_created_dt for a box (where a box hasn�t been replaced at that date)  taken from cust_set_top_box.
--This removes instances where more than one box potentially live for a subscriber_id at a time (due to null box installed and replaced dates).

SELECT account_number
,service_instance_id
,max(dw_created_dt) as max_dw_created_dt
  INTO #boxes -- drop table #boxes
  FROM sk_prod.CUST_SET_TOP_BOX
 WHERE (box_installed_dt <= cast('2011-07-23'  as date)
   AND box_replaced_dt   > cast('2011-07-23'  as date)) or box_installed_dt is null
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


alter table vespa_analysts.uk_base_20110724 add x_pvr_type  varchar(50);
alter table vespa_analysts.uk_base_20110724 add x_box_type  varchar(20);
alter table vespa_analysts.uk_base_20110724 add x_description  varchar(100);
alter table vespa_analysts.uk_base_20110724 add x_manufacturer  varchar(50);
alter table vespa_analysts.uk_base_20110724 add x_model_number  varchar(50);

update  vespa_analysts.uk_base_20110724
set x_pvr_type=b.pvr_type
,x_box_type=b.box_type

,x_description=b.description_x
,x_manufacturer=b.manufacturer
,x_model_number=b.model_number
from vespa_analysts.uk_base_20110724 as a
left outer join #boxes_with_model_info as b
on a.service_instance_id=b.service_instance_id
;
commit;

---Add on Subscriber ID to base table ----

select cast(si_external_identifier as integer) as subscriberid
,si_service_instance_type
,src_system_id
into #subscriber_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
;


commit;
exec sp_create_tmp_table_idx '#subscriber_details', 'src_system_id';

alter table vespa_analysts.uk_base_20110724 add subscriber_id integer;

update vespa_analysts.uk_base_20110724
set subscriber_id=b.subscriberid
from vespa_analysts.uk_base_20110724 as a
left outer join #subscriber_details as b
on a.service_instance_id=b.src_system_id
;
commit;

--select distinct subscriber_id from vespa_analysts.uk_base_20110724 
---Add Package to base and viewer info
select account_number
,min(current_short_description) as Package_code_on_day
into #package_code_by_day
FROM      sk_prod.cust_subs_hist
where         effective_from_dt <= cast('2011-07-23' as date)
AND         effective_to_dt   > cast('2011-07-23' as date)
AND         effective_from_dt <> effective_to_dt
AND        subscription_sub_type = 'DTV Primary Viewing'
group by account_number
;

commit;
create hg index indx1 on #package_code_by_day (account_number);

alter table vespa_analysts.uk_base_20110724  add  Package_code_on_day     varchar(12);
--alter table dbarnett.vespa_6mth_100pc_october_programme_activity_summary_daily  delete  Package_on_day_group ;
alter table vespa_analysts.uk_base_20110724  add  Package_on_day_group     varchar(14) default '6: Unknown';

commit;
UPDATE      vespa_analysts.uk_base_20110724  a
SET         Package_code_on_day = b.Package_code_on_day
FROM         vespa_analysts.uk_base_20110724 as a
left outer join #package_code_by_day as b
on       a.account_number = b.account_number
;

COMMIT;
UPDATE      vespa_analysts.uk_base_20110724  a
SET         Package_on_day_group   = CASE WHEN prem_sports = 2 and prem_movies = 2       then '1: Top_tier'
                                              WHEN prem_sports = 0 and prem_movies = 2  then '3: Dual Movies'
                                              WHEN prem_sports = 2 and prem_movies = 0  then '2: Dual Sports'
                                              WHEN prem_sports = 0 and prem_movies = 0  then '5: No Prems'
                                              ELSE '4: Other prems' END
FROM        sk_prod.cust_entitlement_lookup b
WHERE       a.Package_code_on_day = b.short_description;

COMMIT;

---Add Tenure----
alter table vespa_analysts.uk_base_20110724 add tenure varchar (32);
update vespa_analysts.uk_base_20110724
   set tenure             = case
                                    when datediff(day, ph_subs_first_activation_dt,cast ('2011-07-24' as date)) <=    730 then 'a) 0-24 months'
                                    when datediff(day, ph_subs_first_activation_dt,cast ('2011-07-24' as date)) <=   1825 then 'b) 2-5 Years'
                                    when     datediff(day, ph_subs_first_activation_dt,cast ('2011-07-24' as date)) >1825  then 'c) Over 5 Years'
                                      else 'd) Unknown'
                                  end
from vespa_analysts.uk_base_20110724  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;


---Add Lifestage----
alter table vespa_analysts.uk_base_20110724 add lifestage varchar (32);
update vespa_analysts.uk_base_20110724
   set lifestage             = case when ilu_lifestage = '01'  then '01: Under 35'
        when ilu_lifestage = '02'      then '01: Under 35'
        when ilu_lifestage = '03'      then '01: Under 35'
        when ilu_lifestage = '04'      then '01: Under 35'
        when ilu_lifestage in ('05','06','07')         then '01: Under 35'
        when ilu_lifestage = '08'      then '02: 35-54 No Kids'
        when ilu_lifestage = '09'      then '02: 35-54 No Kids'
        when ilu_lifestage = '10'      then '02: 35-54 No Kids'
        when ilu_lifestage = '11'      then '02: 35-54 No Kids'
        when ilu_lifestage in ('12','13','14','15')    then '03: 35-54 With Kids'
        when ilu_lifestage = '16'      then '04: 55-64'
        when ilu_lifestage = '17'      then '04: 55-64'
        when ilu_lifestage = '18'      then '04: 55-64'
        when ilu_lifestage = '19'      then '05: 65+'
        when ilu_lifestage = '20'      then '05: 65+'
        when ilu_lifestage = '21'      then '05: 65+'
        when ilu_lifestage = '22'      then '05: 65+'
        when ilu_lifestage = '23'      then '05: 65+'
        else                                            '06: Unknown'
                                  end
from vespa_analysts.uk_base_20110724  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;

commit;
--alter table  vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped delete pvr ;
alter table  vespa_analysts.uk_base_20110724 add pvr tinyint;

update vespa_analysts.uk_base_20110724
set pvr = case when x_pvr_type= 'N/A' then 0 else 1 end
from vespa_analysts.uk_base_20110724
;

----Creat base counts to be used to reweight base----
--drop table #active_uk_base_20110724_group_counts;
select subscription_sub_type
,pvr
,tenure
,lifestage
,Package_on_day_group
,count(*) as boxes
,sum(case when b.subscriber_id is not null and german_hungarian_belgian_segments<> '9: Not full data' then 1 else 0 end) as returned_data_all_3_days
into #active_uk_base_20110724_group_counts
from vespa_analysts.uk_base_20110724 as a
left outer join vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as b
on a.subscriber_id = b.subscriber_id
group by subscription_sub_type
,pvr
,tenure
,Package_on_day_group
,lifestage
;
--
--select * from #active_uk_base_20110724_group_counts;



--select x_pvr_type ,x_box_type , count(*) from vespa_analysts.uk_base_20110724 group by x_pvr_type ,x_box_type

select 
german_hungarian_belgian_segments
,c.subscription_sub_type
,b.pvr
,b.tenure
,b.lifestage
,b.Package_on_day_group
,count(*) as unweighted_boxes
,sum(boxes/returned_data_all_3_days) as weighted_boxes

--,sum(case when c.subscription_sub_type = 'DTV Primary Viewing' then 1 else 0 end) as unweighted_boxes_primary_subs
--,sum(case when c.subscription_sub_type = 'DTV Primary Viewing' then boxes/returned_data_all_3_days else 0 end) as weighted_boxes_primary_subs

from  vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a

left outer join vespa_analysts.uk_base_20110724 as b
on a.subscriber_id=b.subscriber_id

left outer join #active_uk_base_20110724_group_counts as c
on b.subscription_sub_type = c.subscription_sub_type and b.pvr=c.pvr  and b.tenure=c.tenure and b.lifestage=c.lifestage and b.Package_on_day_group=c.Package_on_day_group
where b.subscriber_id is not null and a.german_hungarian_belgian_segments<> '9: Not full data'
group by german_hungarian_belgian_segments,c.subscription_sub_type
,b.pvr
,b.tenure
,b.Package_on_day_group
,b.lifestage
order by german_hungarian_belgian_segments,c.subscription_sub_type
,b.pvr
,b.Package_on_day_group
,b.tenure
,b.lifestage
;


commit;
--select distinct german_hungarian_belgian_segments from 

---Create a second version of the segmentation
---1hr+ on 2+ GP , 1hr+ on 1 GP, 30 Min + on any GP, Other

--select distinct german_hungarian_belgian_segments from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped order by german_hungarian_belgian_segments

select case when german_hungarian_belgian_segments in ('1: Over 1hr for each GP','3: Over 1hr for 2 GP') then '1: Over 1hr for 2+ GP'
            when german_hungarian_belgian_segments in ('2: Over 30 min for each GP','4: Over 30 min for 2 GP') then '2: Over 30 min for 2+ GP'
            when german_hungarian_belgian_segments in ('5: Over 1hr for 1 GP','6: Over 30 min for 1 GP') then '3: Over 30 min for 1+ GP'
            when german_hungarian_belgian_segments in ('7: All watched over 5 min','8: Other') then '4: Other viewing behaviour'
            when german_hungarian_belgian_segments in ('9: Not full data') then '5: Not full viewing data' else null end as segment
,c.subscription_sub_type
,b.pvr
,b.tenure
,b.lifestage
,b.Package_on_day_group
,count(*) as unweighted_boxes
,sum(boxes/returned_data_all_3_days) as weighted_boxes

--,sum(case when c.subscription_sub_type = 'DTV Primary Viewing' then 1 else 0 end) as unweighted_boxes_primary_subs
--,sum(case when c.subscription_sub_type = 'DTV Primary Viewing' then boxes/returned_data_all_3_days else 0 end) as weighted_boxes_primary_subs

from  vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped as a

left outer join vespa_analysts.uk_base_20110724 as b
on a.subscriber_id=b.subscriber_id

left outer join #active_uk_base_20110724_group_counts as c
on b.subscription_sub_type = c.subscription_sub_type and b.pvr=c.pvr  and b.tenure=c.tenure and b.lifestage=c.lifestage and b.Package_on_day_group=c.Package_on_day_group
where b.subscriber_id is not null and a.german_hungarian_belgian_segments<> '9: Not full data'
group by segment,c.subscription_sub_type
,b.pvr
,b.tenure
,b.Package_on_day_group
,b.lifestage
order by segment,c.subscription_sub_type
,b.pvr
,b.Package_on_day_group
,b.tenure
,b.lifestage
;
commit;




----Derive second by second activity for all boxes for activity that took place of 24th July







--select german_hungarian_belgian_segments ,count(*) from vespa_analysts.daily_summary_by_subscriber_f1_event_days_deduped group by german_hungarian_belgian_segments order by german_hungarian_belgian_segments



/*
---Section x----
----Create summary table for GP-----
---Table of all those who watched any of the Spanish GP----
--drop table  vespa_analysts.F1_any_viewing_all_subscribers;
select  subscriber_id
,max(case when upper(epg_title) like '%SPANISH%' then 1 else 0 end) as spanish_gp
,max(case when upper(epg_title) like '%MONACO%' then 1 else 0 end) as monaco_gp
,max(case when upper(epg_title) like '%CANADIAN%' then 1 else 0 end) as canadian_gp
,max(case when upper(epg_title) like '%EUROPEAN%' then 1 else 0 end) as european_gp
,max(case when upper(epg_title) like '%BRITISH%' then 1 else 0 end) as british_gp
,max(case when upper(epg_title) like '%GERMAN%' then 1 else 0 end) as german_gp
,max(case when upper(epg_title) like '%HUNGARIAN%' then 1 else 0 end) as hungarian_gp
,max(case when upper(epg_title) like '%BELGIAN%' then 1 else 0 end) as belgian_gp
into vespa_analysts.F1_any_viewing_all_subscribers
from vespa_analysts.F1_analysis_20111104
group by subscriber_id
;
*/



/*
----Create overall summary by subscriber_id for GP---
--select top 100 * from vespa_analysts.vespa_spanish_gp_qualifying;
select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_live >=seconds_viewed_in_minute_playback 
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback >seconds_viewed_in_minute_live then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying_spanish
from vespa_analysts.vespa_spanish_gp_qualifying
group by subscriber_id
;

--select * from #sub_summary_qualifying;

select minutes_viewed_live
,minutes_viewed_playback
,count(*) as sub_ids
from #sub_summary_qualifying_spanish
group by minutes_viewed_live
,minutes_viewed_playback
order by minutes_viewed_live
,minutes_viewed_playback

commit;

----sub summary Race

select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_live >=seconds_viewed_in_minute_playback 
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback >seconds_viewed_in_minute_live then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_race
from vespa_analysts.vespa_spanish_gp_race
group by subscriber_id
;

--select * from #sub_summary_race;

select minutes_viewed_live
,minutes_viewed_playback
,count(*) as sub_ids
from #sub_summary_race
group by minutes_viewed_live
,minutes_viewed_playback
order by minutes_viewed_live
,minutes_viewed_playback


----sub summary highlights

select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_live >=seconds_viewed_in_minute_playback 
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback >seconds_viewed_in_minute_live then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_highlights
from vespa_analysts.vespa_spanish_gp_highlights
group by subscriber_id
;

--select * from #sub_summary_highlights;

select minutes_viewed_live
,minutes_viewed_playback
,count(*) as sub_ids
from #sub_summary_highlights
group by minutes_viewed_live
,minutes_viewed_playback
order by minutes_viewed_live
,minutes_viewed_playback

----Create summary table for GP-----
---Table of all those who watched any of the Spanish GP----
--drop table  vespa_analysts.F1_any_viewing_all_subscribers;
select  subscriber_id
,max(case when upper(epg_title) like '%SPANISH%' then 1 else 0 end) as spanish_gp
,max(case when upper(epg_title) like '%MONACO%' then 1 else 0 end) as monaco_gp
,max(case when upper(epg_title) like '%CANADIAN%' then 1 else 0 end) as canadian_gp
,max(case when upper(epg_title) like '%EUROPEAN%' then 1 else 0 end) as european_gp
,max(case when upper(epg_title) like '%BRITISH%' then 1 else 0 end) as british_gp
,max(case when upper(epg_title) like '%GERMAN%' then 1 else 0 end) as german_gp
,max(case when upper(epg_title) like '%HUNGARIAN%' then 1 else 0 end) as hungarian_gp
,max(case when upper(epg_title) like '%BELGIAN%' then 1 else 0 end) as belgian_gp
into vespa_analysts.F1_any_viewing_all_subscribers
from vespa_analysts.F1_analysis_20111104
group by subscriber_id
;
commit;
--select top 100 * from vespa_analysts.F1_any_viewing_all_subscribers;

select sum(spanish_gp)
,sum(monaco_gp)
,sum(canadian_gp)
,sum(european_gp)
,sum(british_gp)
,sum(german_gp)
,sum(hungarian_gp)
,sum(belgian_gp)
from vespa_analysts.F1_any_viewing_all_subscribers

commit;


select spanish_gp
,monaco_gp
,canadian_gp
,european_gp
,british_gp
,german_gp
,hungarian_gp
,belgian_gp
,count(*) as records
from vespa_analysts.F1_any_viewing_all_subscribers
group by spanish_gp
,monaco_gp
,canadian_gp
,european_gp
,british_gp
,german_gp
,hungarian_gp
,belgian_gp
order by records desc




















--select * from vespa_analysts.vespa_spanish_gp_qualifying;

select minute
, sum(case when seconds_viewed_in_minute>=30 then 1 else 0 end) as boxes
, sum(case when seconds_viewed_in_minute_live>=30 then 1 else 0 end) as boxes_live
, sum(case when seconds_viewed_in_minute_playback>=30 then 1 else 0 end) as boxes_playback
from vespa_analysts.vespa_spanish_gp_qualifying
group by minute order by minute
;

select minute
, sum(case when seconds_viewed_in_minute>=30 then 1 else 0 end) as boxes
, sum(case when seconds_viewed_in_minute_live>=30 then 1 else 0 end) as boxes_live
, sum(case when seconds_viewed_in_minute_playback>=30 then 1 else 0 end) as boxes_playback
from vespa_analysts.vespa_spanish_gp_race
group by minute order by minute
;


select minute
, sum(case when seconds_viewed_in_minute>=30 then 1 else 0 end) as boxes
, sum(case when seconds_viewed_in_minute_live>=30 then 1 else 0 end) as boxes_live
, sum(case when seconds_viewed_in_minute_playback>=30 then 1 else 0 end) as boxes_playback
from vespa_analysts.vespa_spanish_gp_highlights
group by minute order by minute
;




---Add on hrs between playback and event---


--select top 100 * from vespa_201108_max_caps;




select epg_title
,channel_name
, synopsis , tx_date_time_utc,tx_end_datetime_utc,duration , count(*) as records
from vespa_analysts.F1_analysis_20111104
where upper(epg_title) like '%SPANISH%'
group by epg_title
,channel_name
, synopsis , tx_date_time_utc,tx_end_datetime_utc,duration
order  by tx_date_time_utc


select epg_title
--,channel_name
, synopsis , tx_date_time_utc,tx_end_datetime_utc,duration , count(*) as records
from vespa_analysts.F1_analysis_20111104
--where upper(epg_title) like '%SPANISH%'
group by epg_title
--,channel_name
, synopsis , tx_date_time_utc,tx_end_datetime_utc,duration
order  by tx_date_time_utc


select epg_title
--,channel_name
, synopsis , tx_date_time_utc,tx_end_datetime_utc,duration , count(*) as records
from vespa_analysts.F1_analysis_20111104
where upper(epg_title) like '%GERMAN%'
group by epg_title
--,channel_name
, synopsis , tx_date_time_utc,tx_end_datetime_utc,duration
order  by tx_date_time_utc



--



/*
select programme_trans_sk
,channel_name
,epg_title
,tx_date_time_utc
--,*
--,tx_end_date_time_utc
from sk_prod.vespa_epg_dim
where  upper(channel_name) like '%BBC%' and tx_date_time_utc>='2011-05-20' and tx_date_time_utc<'2011-09-01'
and left(epg_title,9)='Formula 1' and epg_title like '%German%' 
order by tx_date_time_utc
;


select programme_trans_sk
,channel_name
,epg_title
,tx_date_time_utc
,*
--,tx_end_date_time_utc
from sk_prod.vespa_epg_dim
where  upper(channel_name) like '%SPORTS%' and tx_date_time_utc>='2011-08-28' and tx_date_time_utc<'2011-08-29'
--and left(epg_title,9)='Formula 1' 
and epg_title='Nott''m Forest v West Ham-Live'
order by tx_date_time_utc
;
commit;



select programme_trans_sk
,channel_name
,epg_title
,tx_date_time_utc
--,*
--,tx_end_date_time_utc
from sk_prod.vespa_epg_dim
where  upper(channel_name) like '%BBC HD%' and tx_date_time_utc>='2011-07-10' and tx_date_time_utc<'2011-07-12'

order by tx_date_time_utc
;

commit;
programme_trans_sk,channel_name
201106120000007396,'BBC 2 England'
201106120000006095,'BBC 2 NI'
201106120000007325,'BBC 2 Wales'
201106120000002340,'BBC 2 Scotland'
201106120000007639,'BBC HD'


select epg_channel , count(*) from vespa_analysts.F1_analysis_20111104 where 
programme_trans_sk in (
201106120000007396,
201106120000006095,
201106120000007325,
201106120000002340,
201106120000007639)
group by epg_channel


select subscriber_id , adjusted_event_start_time,x_adjusted_event_end_time , recorded_time_utc 
,viewing_record_start_time_utc
,viewing_record_end_time_utc
,tx_start_datetime_utc
,tx_end_datetime_utc
, channel_name,epg_title 

from vespa_analysts.F1_analysis_20111104 where play_back_speed=2
order by subscriber_id ,adjusted_event_start_time;

commit;


select panel_id , count(distinct subscriber_id) as subs
from sk_prod.VESPA_STB_PROG_EVENTS_20111111
group by panel_id




*/
*/




