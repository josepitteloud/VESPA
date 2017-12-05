

---List of Spots on Sky 1 on 15th Jan---

--drop table vespa_analysts.vespa_spot_data_sky_one_15_jan;

select  break_start_time
, spot_start_time
, break_type 
, clearcast_commercial_no 
, date_of_transmission 
, station_code 
, preceding_programme_name 
, succeeding_programme_name
, spot_duration 
, spot_break_total_duration 
,case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
 as raw_corrected_spot_time 

,case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then date_of_transmission+1
else date_of_transmission end as corrected_spot_transmission_date
into vespa_analysts.vespa_spot_data_15_jan
from sk_prodreg.MDS_V01_20120214_CBAF_ACQUISITION_20120214 
where 
--station_code = '04924' and 
corrected_spot_transmission_date='2012-01-15'
order by date_of_transmission , spot_start_time;

commit;

--select * from  vespa_analysts.vespa_spot_data_sky_one_15_jan;

alter table  vespa_analysts.vespa_spot_data_15_jan add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_date)
;
commit;

---Import lookup to match to viewing-----


create table vespa_analysts.barb_station_code_lookup_mar_2012
(
station_code_text                      varchar(5)
,station_code                            integer
,channel_name           varchar(64)
)
;

commit;
input into vespa_analysts.barb_station_code_lookup_mar_2012 from 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\Barbcode lookup march 2012.csv' format ascii;
commit;





update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;


update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

---Add Spot end time (end second not inclusive)

alter table  vespa_analysts.vespa_spot_data_15_jan add corrected_spot_transmission_end_datetime datetime;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_end_datetime = dateadd(second, cast(spot_duration as integer),corrected_spot_transmission_start_datetime)
;
commit;


alter table  vespa_analysts.vespa_spot_data_15_jan add channel_name varchar(64);

update vespa_analysts.vespa_spot_data_15_jan
set channel_name = b.channel_name
from vespa_analysts.vespa_spot_data_15_jan as a
left outer join vespa_analysts.barb_station_code_lookup_mar_2012 as b
on a.station_code = b.station_code_text
;

--select * from vespa_analysts.barb_station_code_lookup_mar_2012;


--select * from  vespa_analysts.vespa_spot_data_sky_one_15_jan;


--drop table vespa_analysts.vespa_spot_data_15_jan_ad_sample;
select * into vespa_analysts.vespa_spot_data_15_jan_ad_sample from vespa_analysts.vespa_spot_data_15_jan
where channel_name in (
'LIVING'	,
'LIVING +1'	,
'LIVING Loves'	,
'LIVING2'	,
'Sky Arts 1'	,
'Sky Arts 2'	,
'Sky Movies Action & Adventure'	,
'Sky Movies Classics'	,
'Sky Movies Comedy'	,
'Sky Movies Crime & Thriller'	,
'Sky Movies Drama & Romance'	,
'Sky Movies Family'	,
'Sky Movies Indie'	,
'Sky Movies Modern'	,
'Sky Movies Premiere'	,
'Sky Movies Premiere +1'	,
'Sky Movies Sci-Fi / Horror'	,
'Sky Movies Showcase'	,
'Sky News'	,
'Sky Sports 1'	,
'Sky Sports 2'	,
'Sky Sports 3'	,
'Sky Sports 4'	,
'Sky Sports News'	,
'Sky1'	,
'Sky2'	,
'Sky3'	,
'Sky3+1'	)
;

create hg index idx1 on vespa_analysts.vespa_spot_data_15_jan_ad_sample(channel_name);

create hg index idx2 on vespa_analysts.vespa_spot_data_15_jan_ad_sample(corrected_spot_transmission_start_datetime);
create hg index idx3 on vespa_analysts.vespa_spot_data_15_jan_ad_sample(corrected_spot_transmission_end_datetime);


/*
create hg index idx1 on vespa_analysts.vespa_spot_data_15_jan(channel_name);
create hg index idx2 on vespa_analysts.vespa_spot_data_15_jan(corrected_spot_transmission_start_datetime);
create hg index idx3 on vespa_analysts.vespa_spot_data_15_jan(corrected_spot_transmission_end_datetime);
*/



--select distinct channel_name from vespa_analysts.vespa_spot_data_15_jan order by channel_name;


commit;
--select * from vespa_analysts.vespa_spot_data_15_jan_ad_sample

--drop table vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels;
select * into vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels from vespa_analysts.VESPA_all_viewing_records_20120115
where   (
            (play_back_speed is null and capped_flag in (0,1) )
                OR
            (play_back_speed = 2 and capped_flag in (0,1) )
                OR
            (play_back_speed in (4,12,24,60))
        )
and viewing_record_end_time_local is not null
--and channel_name_inc_hd in ('Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4')
;


commit;

create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels(subscriber_id);
create hg index idx2 on vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels(channel_name_inc_hd);
create hg index idx3 on vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels(viewing_record_start_time_local);
create hg index idx4 on vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels(viewing_record_end_time_local);



--drop table vespa_analysts.vespa_spot_data_By_channel;

---Match to viewing data----
select subscriber_id
, station_code
, channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
, sum(case  when b.play_back_speed is not null then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_live
      
, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>2 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_playback
  
, sum(case  when (b.play_back_speed is not null and play_back_speed<>2) then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_live_or_playback

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>4 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_2x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>12 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_6x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>24 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_12x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>60 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_30x_speed


into vespa_analysts.vespa_spot_data_By_channel
from vespa_analysts.vespa_spot_data_15_jan_ad_sample as a
left outer join vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels as b
on a.channel_name=b.channel_name_inc_hd
where   (viewing_record_start_time_local<corrected_spot_transmission_end_datetime and viewing_record_end_time_local>corrected_spot_transmission_start_datetime)
group by subscriber_id
, station_code 
, channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
;

commit;

--select channel_name_inc_hd, count(*) from vespa_analysts.vespa_spot_data_By_channel group by channel_name_inc_hd;






select  channel_name_inc_hd
,corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
, case when seconds_of_spot_viewed_live_or_playback>spot_duration then spot_duration else seconds_of_spot_viewed_live_or_playback end as seconds_of_ad_viewed
,count(*) as boxes
from vespa_analysts.vespa_spot_data_By_channel
group by channel_name_inc_hd
,corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
, seconds_of_ad_viewed
order by channel_name_inc_hd
,corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
;



select  corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
, case when seconds_of_spot_viewed_live>spot_duration then spot_duration else seconds_of_spot_viewed_live end as seconds_of_ad_viewed
,count(*) as boxes
from vespa_analysts.vespa_spot_data_sample_info
group by corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
, seconds_of_ad_viewed
;


commit;


--select * from vespa_analysts.vespa_spot_data_sample_info order by subscriber_id ,corrected_spot_transmission_start_datetime ;

--select count(*) from vespa_analysts.vespa_spot_data_sample_info order by subscriber_id ,corrected_spot_transmission_start_datetime ;


















--select * from #spot_viewing_info order by subscriber_id ,corrected_spot_transmission_start_datetime ;

select play_back_speed,viewing_record_start_time_local ,viewing_record_end_time_local,adjusted_event_start_time  from vespa_analysts.VESPA_all_viewing_records_20120115_sky1_1pc 

where subscriber_id = 502058
order by viewing_record_start_time_local ,viewing_record_end_time_local
--and play_back_speed is null

select *  from vespa_analysts.VESPA_all_viewing_records_20120115_sky1_1pc 

where subscriber_id = 502058
order by viewing_record_start_time_local ,viewing_record_end_time_local

subscriber_id,station_code,corrected_spot_transmission_start_datetime,corrected_spot_transmission_end_datetime,spot_duration,seonds_of_spot_viewed_live
1094658,'04924','2012-01-15 17:57:11.000000','2012-01-15 17:57:41.000000',30,0
1094658,'04924','2012-01-15 17:57:41.000000','2012-01-15 17:58:11.000000',30,53
1094658,'04924','2012-01-15 17:58:11.000000','2012-01-15 17:58:41.000000',30,23
1094658,'04924','2012-01-15 17:58:41.000000','2012-01-15 17:59:11.000000',30,79

subscriber_id,station_code,corrected_spot_transmission_start_datetime,corrected_spot_transmission_end_datetime,spot_duration,seconds_of_spot_viewed_live
10495658,'04924','2012-01-15 17:57:41.000000','2012-01-15 17:58:11.000000',30,17
10495658,'04924','2012-01-15 17:58:11.000000','2012-01-15 17:58:41.000000',30,30
10495658,'04924','2012-01-15 17:58:41.000000','2012-01-15 17:59:11.000000',30,11





subscriber_id,station_code,corrected_spot_transmission_start_datetime,corrected_spot_transmission_end_datetime,spot_duration,seconds_of_spot_viewed_live,seconds_of_spot_viewed_playback,seconds_of_spot_viewed_live_or_playback,seconds_of_spot_viewed_2x_speed,seconds_of_spot_viewed_6x_speed,seconds_of_spot_viewed_12x_speed,seconds_of_spot_viewed_30x_speed
502058,'04924','2012-01-15 17:58:11.000000','2012-01-15 17:58:41.000000',30,0,19,19,0,0,0,99
502058,'04924','2012-01-15 17:58:41.000000','2012-01-15 17:59:11.000000',30,0,0,0,0,0,0,120


play_back_speed,viewing_record_start_time_local,viewing_record_end_time_local,adjusted_event_start_time
60,'2012-01-15 17:58:12.000000','2012-01-15 18:00:58.000000','2012-01-15 18:22:03.000000'
60,'2012-01-15 17:58:12.000000','2012-01-15 18:41:32.000000','2012-01-16 07:22:55.000000'
60,'2012-01-15 17:58:17.000000','2012-01-15 18:01:22.000000','2012-02-04 16:34:27.000000'
60,'2012-01-15 17:58:24.000000','2012-01-15 18:01:07.000000','2012-02-04 16:21:32.000000'





select subscriber_id
, station_code 
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
,viewing_record_start_time_local
,viewing_record_end_time_local
, case  when b.play_back_speed is not null then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end as seonds_of_spot_viewed_live
        



from vespa_analysts.vespa_spot_data_sky_one_15_jan_ad_sample as a
left outer join vespa_analysts.VESPA_all_viewing_records_20120115_sky1_1pc as b
on a.channel_name=b.channel_name_inc_hd
where b.subscriber_id = 1094658
;












select station_code
,count(*)
from sk_prodreg.MDS_V01_20120214_CBAF_ACQUISITION_20120214 
where 
--station_code = '04924' and 
date_of_transmission='2012-01-15'
group by station_code

select top 100 * from sk_prod.vespa_epg_dim;

select barb_code
,epg_channel
,count(*)
from sk_prod.vespa_epg_dim
where tx_date_utc = '2012-01-15' 
--and barb_code is not null
group by barb_code
,epg_channel
order by epg_channel
;
commit;

select *
from sk_prod.vespa_epg_dim
where tx_date_utc = '2012-01-15' and left(epg_channel,3) = 'ITV'
--and barb_code is not null
group by barb_code
,epg_channel
order by epg_channel
;
commit;


----------
/*
select *
from sk_prodreg.MDS_V01_20120214_CBAF_ACQUISITION_20120214 
where station_code = '04924' and date_of_transmission='2012-01-15'
order by date_of_transmission , spot_start_time;




select  *
,case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
 as raw_corrected_spot_time 

,case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then date_of_transmission+1
else date_of_transmission end as corrected_spot_transmission_date

from sk_prodreg.MDS_V01_20120214_CBAF_ACQUISITION_20120214 
where  corrected_spot_transmission_date='2012-01-16' and upper(succeeding_programme_name) = 'THIS MORNING'
order by date_of_transmission , spot_start_time;


select distinct channel_name_inc_hd from 
vespa_analysts.VESPA_all_viewing_records_20120115 order by channel_name_inc_hd



select * into vespa_analysts.VESPA_all_viewing_records_20120115_sky1 from vespa_analysts.VESPA_all_viewing_records_20120115
where   (
            (play_back_speed is null and capped_flag in (0,1) )
                OR
            (play_back_speed = 2 and capped_flag in (0,1) )
                OR
            (play_back_speed in (4,12,24,60))
        )
and viewing_record_end_time_local is not null
and channel_name_inc_hd = 'Sky 1'
;
