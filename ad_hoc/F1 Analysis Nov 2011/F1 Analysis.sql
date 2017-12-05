/*##################################################################################
*   FILE HEADER
*****************************************************************************
*   Product:          SQL
*   Version:          1.0
*   Author:           Dan Barnett
*   Creation Date:    07/11/2011
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
*
*   Returns viewing data on all programme_trans_sk that relate to the 8 F1 GP
*   Creates Minute by Minute Summaries by subscriber_id for each of the programmes
*
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

















/*
----Create overall summary by subscriber_id for GP---

select subscriber_id
,sum(case   when seconds_viewed_in_minute>=30 then 1 else 0 end) as minutes_viewed
,sum(case   when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_live >=seconds_viewed_in_minute_playback 
            then 1 else 0 end) as minutes_viewed_live
,sum(case when seconds_viewed_in_minute>=30 and  seconds_viewed_in_minute_playback >seconds_viewed_in_minute_live then 1  else 0 end) as minutes_viewed_playback
into #sub_summary_qualifying
from vespa_analysts.vespa_spanish_gp_qualifying
group by subscriber_id
;

--select * from #sub_summary_qualifying;

select minutes_viewed_live
,minutes_viewed_playback
,count(*) as sub_ids
from #sub_summary_qualifying
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
where upper(epg_title) like '%MONACO%'
group by epg_title
--,channel_name
, synopsis , tx_date_time_utc,tx_end_datetime_utc,duration
order  by tx_date_time_utc



--



/*
select programme_trans_sk

from sk_prod.vespa_epg_dim
where  upper(channel_name) like '%BBC%' and tx_date_time_utc>='2011-05-20' and tx_date_time_utc<'2011-09-01'
and left(epg_title,9)='Formula 1'
order by tx_date_time_utc
;


select subscriber_id , adjusted_event_start_time,x_adjusted_event_end_time , recorded_time_utc 
,viewing_record_start_time_utc
,viewing_record_end_time_utc
,tx_start_datetime_utc
,tx_end_datetime_utc
, channel_name,epg_title 

from vespa_analysts.F1_analysis_20111104 where play_back_speed=2
order by subscriber_id ,adjusted_event_start_time;

commit;

*/
*/
