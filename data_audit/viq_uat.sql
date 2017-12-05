--Test 1 - VIQ_Date - check that there is 1 record per datehour
  select utc_datehour
        ,count(1) as cow
    into #test1
    from sk_uat.viq_date
group by utc_datehour
order by utc_datehour

  select cow,count(1) as cowcow
    from #test1 group by cow
--cow cowcow
--2   105617
--1        7

  select date(utc_datehour) as dt
        ,count(1) as cow
    into #test2
    from sk_uat.viq_date
group by dt

select cow,count(cow) as cowcow from #test2 group by cow
-- cow cowcow
-- 24  8801
-- 17     1

select * from #test1 where cow=1
--2024-02-05 05:00:00.000000 to 11

select * from #test2 where cow=17
--2024-02-05

select min(dt), max(dt) from #test2
--2000-01-01, 2024-02-05




--Test 2 - VIQ_Time - check that there is 1 record per minute
select top 10 * from sk_uat.viq_time order by vespa_time
select count(distinct local_time_minute) from sk_uat.viq_time
--86400 (which is equal to the number of seconds in a day
select 60*60*24

select count(distinct utc_time_minute) from sk_uat.viq_time
--86400

select count(1)
into #test2_temp
from sk_uat.viq_time
group by vespa_time,clock_offset_type_id
--205200

select count(1)
from sk_uat.viq_time
--205200





--Test 3 - Time_Minute check that there is 1 record per minute
select * from sk_uat.viq_time_minute order by vespa_time
select count(1) from sk_uat.viq_time_minute
--3420

select count(1)
into #test3_temp
from sk_uat.viq_time_minute
group by vespa_time,clock_offset_type_id
--3420

select count(distinct local_time_minute) from sk_uat.viq_time_minute
--1440

select count(distinct utc_time_minute) from sk_uat.viq_time_minute
--1440





--Test 4 - check channels list
select top 10 * from sk_uat.viq_channel
select count(1),count(distinct channel_name) from sk_uat.viq_channel
select channel_name,count(1) as cow from sk_uat.viq_channel group by channel_name having cow>1
select * from sk_uat.viq_channel where channel_name='Crime'
--we have duplicate channels, with the same service_key. Don't know whether that's a problem





--Test 5 - check that there is 1 record per interval
select count(1) from sk_uat.viq_all_intervals
--1815361
select top 10 * from sk_uat.viq_all_intervals
select count(distinct hour_min) from sk_uat.viq_all_intervals
select count(1),hour_min from sk_uat.viq_all_intervals group by hour_min
--hmm, I don't know what this table is

select * from sk_uat.viq_all_intervals where hour_min='10001'
select * from sk_uat.viq_all_intervals where interval_key='1000010'





--Test 5 - check that there is 1 record per interval hour minute
select top 10 * from sk_uat.viq_interval_hour_minutes
select count(1) from sk_uat.viq_interval_hour_minutes
--87841
select count(distinct interval_hour_minutes_key) from sk_uat.viq_interval_hour_minutes
--87841





--Test 6 - PLATFORM_SERVICE
select top 10 * from sk_uat.viq_platform_service
--there are only 4 records. Looks fine.





--Test 7 - TIME_SHIFT
select count(1) from sk_uat.viq_time_shift
--770
select * from sk_uat.viq_time_shift
--looks reasonable too. 1 record per hour, per live/rec or vosdal fo 30 days





--Test 8 - VIQ_PROG_SCHED_PROPERTIES
select count(1) from sk_uat.VIQ_PROG_SCHED_PROPERTIES; --65537
select top 100 * from sk_uat.VIQ_PROG_SCHED_PROPERTIES;
--don't really know how to check this
select count(1)
,sum(wide_screen_flag)
,sum(true_hd_flag)
,sum(subtitle_flag)
,sum(closed_caption_flag)
,sum(new_show_flag)
,sum(stereo_flag)
,sum(surround_flag)
,sum(audio_described_flag)
,sum(sign_language_flag)
,sum(premiere_indicator_flag)
,sum(three_d_flag)
,sum(black_and_white_flag)
,sum(series_inclusion_flag)
,sum(repeat_flag)
,sum(new_series_indicator_flag)
,sum(last_in_series_indicator_flag)
  from sk_uat.VIQ_PROG_SCHED_PROPERTIES;
--all equal 32768. it's a binary number - combinations thing.
select count(1) from (
     select 1 as x
       from sk_uat.VIQ_PROG_SCHED_PROPERTIES
   group by wide_screen_flag,true_hd_flag,subtitle_flag,closed_caption_flag,new_show_flag,stereo_flag,surround_flag,audio_described_flag,sign_language_flag,premiere_indicator_flag,three_d_flag,black_and_white_flag
           ,series_inclusion_flag,repeat_flag,new_series_indicator_flag,last_in_series_indicator_flag
     ) as sub; --65536 so just 1 duplication

     select count(1) as cow,wide_screen_flag,true_hd_flag,subtitle_flag,closed_caption_flag,new_show_flag,stereo_flag,surround_flag,audio_described_flag,sign_language_flag,premiere_indicator_flag,three_d_flag,black_and_white_flag
           ,series_inclusion_flag,repeat_flag,new_series_indicator_flag,last_in_series_indicator_flag
       from sk_uat.VIQ_PROG_SCHED_PROPERTIES
   group by wide_screen_flag,true_hd_flag,subtitle_flag,closed_caption_flag,new_show_flag,stereo_flag,surround_flag,audio_described_flag,sign_language_flag,premiere_indicator_flag,three_d_flag,black_and_white_flag
           ,series_inclusion_flag,repeat_flag,new_series_indicator_flag,last_in_series_indicator_flag
having cow>1; --it's the one where they are all zero

select * from sk_uat.VIQ_PROG_SCHED_PROPERTIES
where wide_screen_flag+true_hd_flag+subtitle_flag+closed_caption_flag+new_show_flag+stereo_flag+surround_flag+audio_described_flag+sign_language_flag+premiere_indicator_flag+three_d_flag+black_and_white_flag
           +series_inclusion_flag+repeat_flag+new_series_indicator_flag+last_in_series_indicator_flag=0; --0 and -1





--Test 9 - check that each programme is in VIQ_programme table
select count(1) from sk_uat.viq_viewing_data
--16,873,575
select top 10 * from sk_uat.viq_viewing_data
viewing_data_id
select top 10 * from sk_uat.viq_programme
select max(programme_id) from sk_uat.viq_programme; --all null
select min(date_to) from sk_uat.viq_programme; -- no sensible dates
select top 10 * from sk_uat.viq_viewing_data where prog_inst_programme_key=21209664;
--and none of the key fields match, so I can't see a use for the programme table...
select top 10 * from sk_uat.viq_programme where pk_programme_dim=15904318
drop table #test8
select prog_inst_programme_key
      ,min(broadcast_start_date_key) as dt
      ,cast(0 as bit) as match_
  into #test8
  from sk_uat.viq_viewing_data
group by prog_inst_programme_key
;--213,042

update #test8 as tst
   set match_ = 1
  from sk_uat.viq_programme as prg
 where tst.prog_inst_programme_key = prg.pk_programme_dim
;--168,052

select dt
      ,sum(match_) as sm
      ,count(match_) as cn
      ,sm*100/cn as pc
from #test9
group by dt
having pc < 100
order by dt
--so data is complete from June 5th onwards



--Test 10 - check that there is data for each minute for broadcast time
select distinct(broadcast_start_time_key)
into #test10
from sk_uat.viq_viewing_data
; --2300

select top 10 * from sk_uat.viq_time

select count(distinct pk_time_dim)
  from sk_uat.viq_time as tim
       inner join #test9 as tst on tim.pk_time_dim = tst.broadcast_start_time_key
--2300. Good, it even works only checking the start time of broadcasts





--Test 11 - check that there is data for each minute for viewed time
select distinct(viewing_start_time_key)
into #test11
from sk_uat.viq_viewing_data
; --2300

select count(distinct pk_time_dim)
  from sk_uat.viq_time as tim
       inner join #test11 as tst on tim.pk_time_dim = tst.viewing_start_time_key
; --2300






--Test 12 - Household
select count(distinct household_key)
  from sk_uat.viq_viewing_data    as viw
; --194,001

select count(distinct viw.household_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_household as sub on viw.household_key = sub.household_key
--1

select top 10 * from sk_uat.viq_household
select top 10 household_key from sk_uat.viq_viewing_data
select max(household_key) from sk_uat.viq_viewing_data
select count(household_key) from sk_uat.viq_viewing_data where household_key = -1
5,036,151
select count(household_key) from sk_uat.viq_viewing_data where household_key != -1
11,837,424
select count(household_key) from sk_uat.viq_household where household_key = -1
1
select count(household_key) from sk_uat.viq_household where household_key != -1
606692
select 5036151/11837424.0
--43%





--Test 13 - broadcast_duration
select count(distinct broadcast_duration)
  from sk_uat.viq_viewing_data    as viw
--541 not sure what we can chjeck this against...





--Test 14 - broadcast_end_date_key
select count(distinct broadcast_end_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_date as dat on viw.broadcast_end_date_key = dat.pk_datehour_dim
--1645

select count(distinct broadcast_end_date_key)
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_date as dat on viw.broadcast_end_date_key = dat.pk_datehour_dim
where dat.pk_datehour_dim is null
--0





--Test 15 - broadcast_end_time_key
select top 10 * from sk_uat.viq_time;
select count(distinct broadcast_end_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time as tim on viw.broadcast_end_time_key = tim.pk_time_dim
--2323

select count(distinct broadcast_end_date_key)
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_time  as tim on viw.broadcast_end_time_key = tim.pk_time_dim
where tim.pk_time_dim is null
--0





--Test 16 - broadcast_interval_key
select top 10 * from sk_uat.viq_INTERVAL_HOUR_MINUTES;
select count(distinct broadcast_end_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_INTERVAL_HOUR_MINUTES as lnk on viw.broadcast_interval_key = cast(lnk.INTERVAL_HOUR_MINUTES_KEY as int)
--2323

select count(distinct broadcast_end_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_INTERVAL_HOUR_MINUTES as lnk on viw.broadcast_interval_key = cast(lnk.INTERVAL_HOUR_MINUTES_KEY as int)
where lnk.INTERVAL_HOUR_MINUTES_KEY is null
--0





--Test 17 - broadcast_start_date_key
select count(distinct broadcast_start_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_date as dat on viw.broadcast_start_date_key = dat.pk_datehour_dim
--1645

select count(distinct broadcast_start_date_key)
  from sk_uat.viq_viewing_data   as viw
       left join sk_uat.viq_date as dat on viw.broadcast_start_date_key = dat.pk_datehour_dim
where dat.pk_datehour_dim is null
--0





--Test 18 - broadcast_start_time_key
select count(distinct broadcast_start_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time as tim on viw.broadcast_start_time_key = tim.pk_time_dim
--2300

select count(distinct broadcast_start_date_key)
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_time  as tim on viw.broadcast_start_time_key = tim.pk_time_dim
where tim.pk_time_dim is null
--0





--Test 19 - platform_service_key
select top 10 * from sk_uat.viq_platform_service;
select count(distinct platform_service_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_platform_service as lnk on viw.platform_service_key = lnk.pk_platform_service_dim
--0
select distinct(platform_service_key) from sk_uat.viq_viewing_data;
--they are all -1





--Test 20 - previous channel key
select top 10 * from sk_uat.viq_channel
select count(distinct previous_channel_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_channel as lnk on viw.previous_channel_key = lnk.pk_channel_dim
; --632

select count(distinct previous_channel_key)
  from sk_uat.viq_viewing_data    as viw
      left join sk_uat.viq_channel as lnk on viw.previous_channel_key = lnk.pk_channel_dim
 where lnk.pk_channel_dim is null
; --544 channels are not in channel table!

select distinct (previous_channel_key)
  from sk_uat.viq_viewing_data    as viw
      left join sk_uat.viq_channel as lnk on viw.previous_channel_key = lnk.pk_channel_dim
 where lnk.pk_channel_dim is null
; --544





--Test 21 - previous_prog_inst_end_date_key
select count(distinct previous_prog_inst_end_date_key)
  from sk_uat.viq_viewing_data    as viw
; --1
select count(distinct previous_prog_inst_end_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_date as dat on viw.previous_prog_inst_end_date_key = dat.pk_datehour_dim
; --0

select top 10 previous_prog_inst_end_date_key
  from sk_uat.viq_viewing_data    as viw
; --all have the same value 2099123123, which is not a valid date





--Test 22 - previous_prog_inst_end_time_key
select count(distinct previous_prog_inst_end_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time as tim on viw.previous_prog_inst_end_time_key = tim.pk_time_dim
--1

select count(distinct previous_prog_inst_end_time_key)
  from sk_uat.viq_viewing_data    as viw
--1

select top 10 previous_prog_inst_end_time_key
  from sk_uat.viq_viewing_data    as viw
--1000000





--Test 23 - previous_prog_inst_start_date_key
select count(distinct previous_prog_inst_start_date_key)
  from sk_uat.viq_viewing_data    as viw
; --1613

select count(distinct previous_prog_inst_start_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_date as dat on viw.previous_prog_inst_start_date_key = dat.pk_datehour_dim
; --1612

select distinct previous_prog_inst_start_date_key
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_date as dat on viw.previous_prog_inst_start_date_key = dat.pk_datehour_dim
 where dat.pk_datehour_dim is null
--2099123123

select distinct previous_prog_inst_start_date_key
  from sk_uat.viq_viewing_data    as viw





--Test 24 - previous_prog_inst_start_time_key
select count(distinct previous_prog_inst_start_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time as tim on viw.previous_prog_inst_start_time_key = tim.pk_time_dim
--2845

select count(distinct previous_prog_inst_start_time_key)
  from sk_uat.viq_viewing_data    as viw
--2845





--Test 25 - previous_programme_key
select count(distinct previous_programme_key)
  from sk_uat.viq_viewing_data    as viw
--216,327

select count(distinct previous_programme_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_programme as prg on viw.previous_programme_key = prg.pk_programme_dim
--169,999

select count(distinct previous_programme_key)
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_programme as prg on viw.previous_programme_key = prg.pk_programme_dim
 where prg.pk_programme_dim is null
--46328

select top 10 previous_programme_key
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_programme as prg on viw.previous_programme_key = prg.pk_programme_dim
 where prg.pk_programme_dim is null





--Test 26 - prog_inst_channel_key
select count(distinct prog_inst_channel_key)
  from sk_uat.viq_viewing_data    as viw
--1041

select count(distinct prog_inst_channel_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_channel as chn on viw.prog_inst_channel_key = chn.pk_channel_dim
--563

select count(distinct prog_inst_channel_key)
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_channel as chn on viw.prog_inst_channel_key = chn.pk_channel_dim
where chn.pk_channel_dim is null
--478

select top 10 prog_inst_channel_key
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_channel as chn on viw.prog_inst_channel_key = chn.pk_channel_dim
where chn.pk_channel_dim is null





--Test 27 - prog_inst_end_date_key
select count(distinct prog_inst_end_date_key)
  from sk_uat.viq_viewing_data    as viw
; --1616

select count(distinct prog_inst_end_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_date as dat on viw.prog_inst_end_date_key = dat.pk_datehour_dim
; --1616





--Test 28 - prog_inst_end_time_key
select count(distinct prog_inst_end_time_key)
  from sk_uat.viq_viewing_data    as viw
; --2375

select count(distinct prog_inst_end_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time as tim on viw.previous_prog_inst_end_time_key = tim.pk_time_dim
--2375





--Test 29 - prog_inst_programme_key
select count(distinct prog_inst_programme_key)
  from sk_uat.viq_viewing_data    as viw
--213,042

select count(distinct prog_inst_programme_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_programme as prg on viw.prog_inst_programme_key = prg.pk_programme_dim
--168,052

select count(distinct prog_inst_programme_key)
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_programme as prg on viw.prog_inst_programme_key = prg.pk_programme_dim
 where prg.pk_programme_dim is null
--4490

select top 10 prog_inst_programme_key
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_programme as prg on viw.prog_inst_programme_key = prg.pk_programme_dim
 where prg.pk_programme_dim is null





--Test 30 - prog_inst_properties_key
select count(distinct  prog_inst_properties_key)
  from sk_uat.viq_viewing_data    as viw
--40

select count(distinct  prog_inst_properties_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_PROG_SCHED_PROPERTIES as psp on viw.prog_inst_properties_key = psp.pk_PROGRAMME_instance_PROPERTIES

select distinct  prog_inst_properties_key
  from sk_uat.viq_viewing_data    as viw
--40





--Test 31 - prog_inst_start_date_key
select count(distinct prog_inst_start_date_key)
  from sk_uat.viq_viewing_data    as viw
; --1607

select count(distinct prog_inst_start_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_date as dat on viw.prog_inst_start_date_key = dat.pk_datehour_dim
; --1607





--Test 32 - prog_inst_start_time_key
select count(distinct prog_inst_start_time_key)
  from sk_uat.viq_viewing_data    as viw
; --2228

select count(distinct prog_inst_start_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time as tim on viw.prog_inst_start_time_key = tim.pk_time_dim
; --2228





--Test 33 - event viewed flag
select distinct(event_viewed_flag)
  from sk_uat.viq_viewing_data    as viw





--Test 34 - programme_viewed_flag
select distinct(programme_viewed_flag)
  from sk_uat.viq_viewing_data    as viw





--Test 35 - time_shift_key
select count(distinct time_shift_key)
  from sk_uat.viq_viewing_data    as viw
--773

select count(distinct time_shift_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time_shift as tim on viw.time_shift_key = tim.pk_timeshift_dim
--770

select top 10 * from sk_uat.viq_time_shift

select distinct time_shift_key
  from sk_uat.viq_viewing_data    as viw
       left join sk_uat.viq_time_shift as tim on viw.time_shift_key = tim.pk_timeshift_dim
where tim.pk_timeshift_dim is null
--





--Test 36 - viewed duration
select count(distinct viewed_duration)
  from sk_uat.viq_viewing_data    as viw
--60

select distinct viewed_duration
  from sk_uat.viq_viewing_data    as viw





--Test 37 - viewed_interval_key
select count(distinct viewed_interval_key)
  from sk_uat.viq_viewing_data    as viw
--84669

select count(distinct viewed_interval_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_INTERVAL_HOUR_MINUTES as ihm on viw.viewed_interval_key = cast(ihm.INTERVAL_HOUR_MINUTES_KEY as int)
--84669





--Test 38 - viewing_data_id
select count(distinct viewing_data_id)
  from sk_uat.viq_viewing_data    as viw
--16,873,575

select count(1)
  from sk_uat.viq_viewing_data    as viw
--16,873,575





--Test 39 - viewing_end_date_key
select count(distinct viewing_end_date_key)
  from sk_uat.viq_viewing_data    as viw
; --1645

select count(distinct viewing_end_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_date as dat on viw.viewing_end_date_key = dat.pk_datehour_dim
; --1645





--Test 40 - viewing_end_time_key
select count(distinct viewing_end_time_key)
  from sk_uat.viq_viewing_data    as viw
; --2323

select count(distinct viewing_end_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time as tim on viw.viewing_end_time_key = tim.pk_time_dim
--2323





--Test 41 - viewing_start_date_key
select count(distinct viewing_start_date_key)
  from sk_uat.viq_viewing_data    as viw
; --1645

select count(distinct viewing_start_date_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_date as dat on viw.viewing_start_date_key = dat.pk_datehour_dim
; --1645





--Test 42 - viewing_end_time_key
select count(distinct viewing_start_time_key)
  from sk_uat.viq_viewing_data    as viw
; --2300

select count(distinct viewing_start_time_key)
  from sk_uat.viq_viewing_data    as viw
       inner join sk_uat.viq_time as tim on viw.viewing_start_time_key = tim.pk_time_dim
--2300





--Test 43 - check that the channels list matches the EPG data
select count(1) from sk_uat.viq_channel
select top 10 * from sk_uat.viq_channel
select count(distinct channel_name) from sk_uat.viq_channel
--663
select distinct channel_name from sk_uat.viq_channel

--Prod4
select count(distinct channel_name) from sk_prod.vespa_programme_schedule
--802
--Checked in excel - All from UAT are on Prod4, but not the other way round





--Test 44 - check that row counts matches EPG data for each channel
select count(1), channel_name
  from sk_uat.viq_programme_schedule as sch
       inner join sk_uat.viq_channel as chn on sch.dk_channel_id = chn.pk_channel_dim
 where dk_start_datehour between 2012090100 and 2012090199
group by channel_name
--597

--Prod4
select channel_name,count(1) as cow from sk_prod.vespa_programme_schedule
where date(broadcast_start_date_time_utc) = '20120901'
group by channel_name
--636
--checked in excel: All from uat are in prod. 39 channels in prod are not in uat





--Test 45 - check that row counts match EPG data for each day
  select count(1), left(cast(dk_start_datehour as varchar), 8) as dt
    from sk_uat.viq_programme_schedule as sch
group by dt

--Prod4
  select count(1), date(broadcast_start_date_time_utc) as dt
    from sk_prod.vespa_programme_schedule
group by dt

--the numbers don't match for 2 days - 30Aug (45 missing from UAT) and 31Aug (4 missing)





--Test 46 - check the row counts match the daily viewing tables for each channel
  select count(1)
        ,channel_name
    from sk_uat.viq_viewing_data as viw
         inner join sk_uat.viq_channel as chn on viw.prog_inst_channel_key = chn.pk_channel_dim
   where cast(broadcast_start_date_key as varchar) like '20120801%'
group by channel_name
--doesn't match by broadcast date

--Prod4
  select count(1),channel
  from sk_prod.vespa_events_all
where date(broadcast_start_Date) = '2012-08-01'
  group by channel

  select count(1)
        ,channel_name
    from sk_uat.viq_viewing_data as viw
         inner join sk_uat.viq_channel as chn on viw.prog_inst_channel_key = chn.pk_channel_dim
   where cast(viewing_start_date_key as varchar) like '20120801%'
group by channel_name
--doesn't match by viewed date either

--Prod4
  select count(1),channel_name
  from sk_prod.vespa_events_all
where dk_event_start_datehour_dim between 2012080100 and 2012080199
  group by channel_name
--doesn't match by viewed date either





--Test 47 - check the row counts match the daily viewing tables for each day
  select count(1)
        ,viewing_start_date_key/100 as dt
    from sk_uat.viq_viewing_data as viw
         inner join sk_uat.viq_channel as chn on viw.prog_inst_channel_key = chn.pk_channel_dim
   where channel_name = 'The Box'
group by dt
order by dt

--Prod4
  select count(1)
        ,dk_event_start_datehour_dim/100 as dt
    from sk_prod.vespa_events_all
   where channel_name = 'The Box'
group by dt
--don't match at all

select *
    from sk_uat.viq_viewing_data as viw
         inner join sk_uat.viq_channel as chn on viw.prog_inst_channel_key = chn.pk_channel_dim
   where channel_name = 'The Box'
     and viewing_start_date_key/100 = 20120731





select count(1) from sk_uat.VIQ_PROGRAMME_SCHEDULE
select count(1) from sk_uat.VIQ_household

select count(1) from viq_PROG_SCHED_PROPERTIES
select count(1) from viq_VIEWING_DATA_SCALING

