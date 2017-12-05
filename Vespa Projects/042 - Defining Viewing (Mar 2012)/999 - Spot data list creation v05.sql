

---List of Spots ---

--drop table vespa_analysts.vespa_spot_data_15_jan;

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
      when left (break_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (break_start_time,2) as integer)-24 ||right (break_start_time,4) 
      else break_start_time end 
 as raw_corrected_break_start_time 

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

--select *  from sk_prodreg.MDS_V01_20120214_CBAF_ACQUISITION_20120214 where upper(preceding_programme_name) = 'BOARDWALK EMPIRE'
--select station_code , count(*)  from sk_prodreg.MDS_V01_20120214_CBAF_ACQUISITION_20120214 group by station_code order by station_code;

--select * from  vespa_analysts.vespa_spot_data_sky_one_15_jan;

alter table  vespa_analysts.vespa_spot_data_15_jan add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_date)
;
commit;

alter table  vespa_analysts.vespa_spot_data_15_jan add corrected_spot_transmission_break_start_time  datetime;
update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_break_start_time = dateadd(hour, cast(left(raw_corrected_break_start_time,2) as integer),corrected_spot_transmission_date )
;
commit;


---Import lookup to match to viewing-----

--drop table vespa_analysts.barb_station_code_lookup_mar_2012;
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

---Add Break Start Time----



update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_break_start_time = dateadd(minute, cast(substr(raw_corrected_break_start_time,3,2) as integer),corrected_spot_transmission_break_start_time)
;
commit;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_break_start_time = dateadd(second, cast(right(raw_corrected_break_start_time,2) as integer),corrected_spot_transmission_break_start_time)
;
commit;



----

alter table  vespa_analysts.vespa_spot_data_15_jan add channel_name varchar(64);

update vespa_analysts.vespa_spot_data_15_jan
set channel_name = b.channel_name
from vespa_analysts.vespa_spot_data_15_jan as a
left outer join vespa_analysts.barb_station_code_lookup_mar_2012 as b
on a.station_code = b.station_code_text
;

--select * from vespa_analysts.barb_station_code_lookup_mar_2012;

---Create an equivalen of channel_name_inc_hd to match to viewing data---
--drop table vespa_analysts.spot_data_viewing_channel_lookup;
create table vespa_analysts.spot_data_viewing_channel_lookup 
( channel_name varchar(90)
,channel_name_inc_hd varchar(90)
)
;
commit;
input into vespa_analysts.spot_data_viewing_channel_lookup from 
'C:\Users\barnetd\Documents\Project 042 - Definition of a view\Spot to Viewing channel lookup.csv' format ascii;
commit;

alter table  vespa_analysts.vespa_spot_data_15_jan add channel_name_inc_hd varchar(64);

update vespa_analysts.vespa_spot_data_15_jan
set channel_name_inc_hd = b.channel_name_inc_hd
from vespa_analysts.vespa_spot_data_15_jan as a
left outer join vespa_analysts.spot_data_viewing_channel_lookup as b
on a.channel_name = b.channel_name
;

commit;

alter table  vespa_analysts.vespa_spot_data_15_jan add spot_position_in_break varchar(32);

update vespa_analysts.vespa_spot_data_15_jan
set spot_position_in_break = case  when break_start_time = spot_start_time 
            then '01: First Spot in break' 
        when  dateadd(second, cast(spot_break_total_duration as integer),corrected_spot_transmission_break_start_time)=corrected_spot_transmission_end_datetime 
            then '02: Last Spot in break'
        else '03: Mid Spot in break' end 
from vespa_analysts.vespa_spot_data_15_jan as a
;

commit;


--select * from vespa_analysts.barb_station_code_lookup_mar_2012;

--select channel_name , channel_name_inc_hd from vespa_analysts.vespa_spot_data_15_jan group by channel_name , channel_name_inc_hd;
--select * from  vespa_analysts.vespa_spot_data_sky_one_15_jan;
--select distinct channel_name_inc_hd from vespa_analysts.vespa_spot_data_15_jan

--select *  from vespa_analysts.vespa_spot_data_15_jan;

--drop table vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots;
select * into vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots from vespa_analysts.vespa_spot_data_15_jan
where channel_name_inc_hd in (
'Sky Living',
'Sky Living +1',
'Sky Living Loves',
'Sky Livingit',
'Sky Arts 1',
'Sky Arts 2',
'Sky Movies Action',
'Sky Movies Classics',
'Sky Movies Comedy',
'Sky Movies Thriller',
'Sky DramaRom',
'Sky Movies Family',
'Sky Movies Indie',
'Sky Movies Mdn Greats',
'Sky Premiere',
'Sky Prem+1',
'Sky Movies Sci-Fi/Horror',
'Sky Movies Showcase',
'Sky News',
'Sky Sports 1',
'Sky Sports 2',
'Sky Sports 3',
'Sky Sports 4',
'Sky Sports News',
'Sky 1',
'Sky 2',
'Sky 3',
'Sky 3+1',
'Sky Atlantic'
	)
;

create hg index idx1 on vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots(channel_name_inc_hd);
create hg index idx2 on vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots(corrected_spot_transmission_start_datetime);
create hg index idx3 on vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots(corrected_spot_transmission_end_datetime);

--select spot_duration , count(*) from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots group by spot_duration order by spot_duration;

commit;

/*
create hg index idx1 on vespa_analysts.vespa_spot_data_15_jan(channel_name);
create hg index idx2 on vespa_analysts.vespa_spot_data_15_jan(corrected_spot_transmission_start_datetime);
create hg index idx3 on vespa_analysts.vespa_spot_data_15_jan(corrected_spot_transmission_end_datetime);
*/



--select distinct channel_name from vespa_analysts.vespa_spot_data_15_jan order by channel_name;


commit;
--select * from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots
--select channel_name_inc_hd , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20120115 group by channel_name_inc_hd order by records desc
--select top 500 *  from vespa_analysts.VESPA_all_viewing_records_20120115 where capped_flag = 3;


--drop table vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels ;
select *,dateformat(adjusted_event_start_time,'YYYY-MM-DD') as event_date 
into vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels 
from vespa_analysts.VESPA_all_viewing_records_20120115
where   (
            (play_back_speed is null and capped_flag in (0,1) )
                OR
            (play_back_speed = 2 and capped_flag in (0,1) )
                OR
            (play_back_speed in (4,12,24,60))
        )
and viewing_record_end_time_local is not null
and channel_name_inc_hd in (
'Sky Living',
'Sky Living +1',
'Sky Living Loves',
'Sky Livingit',
'Sky Arts 1',
'Sky Arts 2',
'Sky Movies Action',
'Sky Movies Classics',
'Sky Movies Comedy',
'Sky Movies Thriller',
'Sky DramaRom',
'Sky Movies Family',
'Sky Movies Indie',
'Sky Movies Mdn Greats',
'Sky Premiere',
'Sky Prem+1',
'Sky Movies Sci-Fi/Horror',
'Sky Movies Showcase',
'Sky News',
'Sky Sports 1',
'Sky Sports 2',
'Sky Sports 3',
'Sky Sports 4',
'Sky Sports News',
'Sky 1',
'Sky 2',
'Sky 3',
'Sky 3+1',
'Sky Atlantic')
;


commit;

create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(subscriber_id);
create hg index idx2 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(channel_name_inc_hd);
create hg index idx3 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(viewing_record_start_time_local);
create hg index idx4 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(viewing_record_end_time_local);
create hg index idx5 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(account_number);
create hg index idx6 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(event_date);

--select distinct channel_name_inc_hd from vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels;
--drop table vespa_analysts.vespa_spot_data_By_channel;

---Match to viewing data----
select account_number
, subscriber_id
, station_code
, a.channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
,min(event_date) as date_watched
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
from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots as a
left outer join vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels as b
on a.channel_name_inc_hd=b.channel_name_inc_hd
where   (viewing_record_start_time_local<corrected_spot_transmission_end_datetime and viewing_record_end_time_local>corrected_spot_transmission_start_datetime)
group by account_number
,subscriber_id
, station_code 
, a.channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
;

commit;

--select channel_name_inc_hd, count(*) from vespa_analysts.vespa_spot_data_By_channel group by channel_name_inc_hd order by channel_name_inc_hd;


--select top 500 * from vespa_analysts.vespa_spot_data_By_channel;
--select top 500 * from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots;

---Add other spot information back on to table ---
commit;
alter table vespa_analysts.vespa_spot_data_By_channel add break_type varchar(2);

update  vespa_analysts.vespa_spot_data_By_channel 
set break_type = b.break_type
from vespa_analysts.vespa_spot_data_By_channel as a 
left outer join vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots as b
on a.station_code=b.station_code and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;

--select top 100 * from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots;

alter table vespa_analysts.vespa_spot_data_By_channel add spot_position_in_break varchar(32);

update  vespa_analysts.vespa_spot_data_By_channel 
set spot_position_in_break = b.spot_position_in_break
from vespa_analysts.vespa_spot_data_By_channel as a 
left outer join vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots as b
on a.station_code=b.station_code and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;



--select top 100 * from vespa_analysts.vespa_spot_data_By_channel;
--alter table vespa_analysts.vespa_spot_data_By_channel delete viewing_time_of_day ;
alter table vespa_analysts.vespa_spot_data_By_channel add viewing_time_of_day varchar(32);

update vespa_analysts.vespa_spot_data_By_channel 
set viewing_time_of_day = case  when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('00','01','02','03','04','05') 
                                    then '01: Night (00:00 - 05:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('06','07','08') 
                                    then '02: Breakfast (06:00 - 08:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('09','10','11') 
                                    then '03: Morning (09:00 - 11:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('12','13','14') 
                                    then '04: Lunch (12:00 - 14:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('15','16','17') 
                                    then '05: Early Prime (15:00 - 17:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('18','19','20') 
                                    then '06: Prime (18:00 - 20:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('21','22','23') 
                                    then '07: Late Night (21:00 - 23:59)' 

else '08: Other' end
from vespa_analysts.vespa_spot_data_By_channel 
;
commit;

--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels ;



---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----

---Add Scaling ID each account is to be assigned to based on the day they view the spot
--alter table vespa_analysts.vespa_spot_data_By_channel delete scaling_segment_id
alter table vespa_analysts.vespa_spot_data_By_channel add scaling_segment_id integer;

update vespa_analysts.vespa_spot_data_By_channel 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.vespa_spot_data_By_channel  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.date_watched as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.vespa_spot_data_By_channel  group by scaling_segment_id order by scaling_segment_id;

---Add weight for each scaling ID for each record

alter table vespa_analysts.vespa_spot_data_By_channel add weighting double;

update vespa_analysts.vespa_spot_data_By_channel 
set weighting=b.weighting
from vespa_analysts.vespa_spot_data_By_channel  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.date_watched as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.vespa_spot_data_By_channel add affluence varchar(10) ;
alter table vespa_analysts.vespa_spot_data_By_channel add pvr tinyint;

update vespa_analysts.vespa_spot_data_By_channel 
set affluence=b.affluence
,pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.vespa_spot_data_By_channel  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;


--select weighting , count(*) from vespa_analysts.vespa_spot_data_By_channel  group by weighting;
--select affluence , count(*) from vespa_analysts.vespa_spot_data_By_channel  group by affluence;




--select viewing_time_of_day ,count(*) from vespa_analysts.vespa_spot_data_By_channel group by viewing_time_of_day;
--select break_type ,count(*) from vespa_analysts.vespa_spot_data_By_channel group by break_type;

--select top 500 * from vespa_analysts.vespa_spot_data_By_channel ;









--select distinct channel_name from vespa_analysts.vespa_spot_data_15_jan_ad_sample order by channel_name;
--select channel_name, channel_name_inc_hd from vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels group by channel_name, channel_name_inc_hd order by channel_name_inc_hd;


--select min(tx_date) from sk_prod.vespa_epg_dim where upper(channel_name) like '%DRAMA%'
--select channel_name_inc_hd , count(*) from vespa_analysts.vespa_spot_data_By_channel group by channel_name_inc_hd order by channel_name_inc_hd

---Pivot For Live or Playback Viewing

select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live_or_playback>spot_duration then spot_duration else seconds_of_spot_viewed_live_or_playback end as seconds_of_ad_viewed
,affluence
,pvr

,count(*) as boxes
,sum(weighting) as weighted_boxes
into #live_playback_channel_pivot
from vespa_analysts.vespa_spot_data_By_channel
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;
commit;

--select channel_name_inc_hd , count(*) from #live_playback_channel_pivot group by channel_name_inc_hd;

select * from #live_playback_channel_pivot;

output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_playback_pivot_data.csv' format ascii;





---Pivot For Live Only Viewing
select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live>spot_duration then spot_duration else seconds_of_spot_viewed_live end as seconds_of_ad_viewed
,affluence
,pvr
,sum(weighting) as weighted_boxes
,count(*) as boxes
into #live_only_pivot
from vespa_analysts.vespa_spot_data_By_channel
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;

select * from #live_only_pivot;

output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_only_pivot_data.csv' format ascii;


commit;




---Repeat for non-Sky Channels

select * into vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots from vespa_analysts.vespa_spot_data_15_jan
where channel_name_inc_hd in (
'E4'
,'ITV2'
,'ITV3'
,'UKTV Dave'
,'Comedy Central'
,'Watch'
,'Nick Jr'
,'Cartoonito'
,'ESPN'
)
;

create hg index idx1 on vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots(channel_name_inc_hd);
create hg index idx2 on vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots(corrected_spot_transmission_start_datetime);
create hg index idx3 on vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots(corrected_spot_transmission_end_datetime);

--select spot_duration , count(*) from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots group by spot_duration order by spot_duration;

commit;


commit;
--select * from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots
--select channel_name_inc_hd , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20120115 group by channel_name_inc_hd order by records desc
--select top 500 *  from vespa_analysts.VESPA_all_viewing_records_20120115 where capped_flag = 3;


--drop table vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels ;
select *,dateformat(adjusted_event_start_time,'YYYY-MM-DD') as event_date 
into vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels 
from vespa_analysts.VESPA_all_viewing_records_20120115
where   (
            (play_back_speed is null and capped_flag in (0,1) )
                OR
            (play_back_speed = 2 and capped_flag in (0,1) )
                OR
            (play_back_speed in (4,12,24,60))
        )
and viewing_record_end_time_local is not null
and channel_name_inc_hd in (
'E4'
,'ITV2'
,'ITV3'
,'UKTV Dave'
,'Comedy Central'
,'Watch'
,'Nick Jr'
,'Cartoonito'
,'ESPN')
;


commit;

create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(subscriber_id);
create hg index idx2 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(channel_name_inc_hd);
create hg index idx3 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(viewing_record_start_time_local);
create hg index idx4 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(viewing_record_end_time_local);
create hg index idx5 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(account_number);
create hg index idx6 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(event_date);

--select distinct channel_name_inc_hd from vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels;
--drop table vespa_analysts.vespa_spot_data_By_channel;

---Match to viewing data----
select account_number
, subscriber_id
, station_code
, a.channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
,min(event_date) as date_watched
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


into vespa_analysts.vespa_spot_data_By_non_sky_channel
from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots as a
left outer join vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels as b
on a.channel_name_inc_hd=b.channel_name_inc_hd
where   (viewing_record_start_time_local<corrected_spot_transmission_end_datetime and viewing_record_end_time_local>corrected_spot_transmission_start_datetime)
group by account_number
,subscriber_id
, station_code 
, a.channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
;

commit;

--select channel_name_inc_hd, count(*) from vespa_analysts.vespa_spot_data_By_non_sky_channel group by channel_name_inc_hd order by channel_name_inc_hd;


--select top 500 * from vespa_analysts.vespa_spot_data_By_non_sky_channel;
--select top 500 * from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots;

---Add other spot information back on to table ---
commit;
alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add break_type varchar(2);

update  vespa_analysts.vespa_spot_data_By_non_sky_channel 
set break_type = b.break_type
from vespa_analysts.vespa_spot_data_By_non_sky_channel as a 
left outer join vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots as b
on a.station_code=b.station_code and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;

--select top 100 * from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots;

alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add spot_position_in_break varchar(32);

update  vespa_analysts.vespa_spot_data_By_non_sky_channel 
set spot_position_in_break = b.spot_position_in_break
from vespa_analysts.vespa_spot_data_By_non_sky_channel as a 
left outer join vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots as b
on a.station_code=b.station_code and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;



--select top 100 * from vespa_analysts.vespa_spot_data_By_non_sky_channel;
--alter table vespa_analysts.vespa_spot_data_By_non_sky_channel delete viewing_time_of_day ;
alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add viewing_time_of_day varchar(32);

update vespa_analysts.vespa_spot_data_By_non_sky_channel 
set viewing_time_of_day = case  when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('00','01','02','03','04','05') 
                                    then '01: Night (00:00 - 05:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('06','07','08') 
                                    then '02: Breakfast (06:00 - 08:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('09','10','11') 
                                    then '03: Morning (09:00 - 11:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('12','13','14') 
                                    then '04: Lunch (12:00 - 14:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('15','16','17') 
                                    then '05: Early Prime (15:00 - 17:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('18','19','20') 
                                    then '06: Prime (18:00 - 20:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('21','22','23') 
                                    then '07: Late Night (21:00 - 23:59)' 

else '08: Other' end
from vespa_analysts.vespa_spot_data_By_non_sky_channel 
;
commit;

--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels ;



---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----

---Add Scaling ID each account is to be assigned to based on the day they view the spot
--alter table vespa_analysts.vespa_spot_data_By_non_sky_channel delete scaling_segment_id
alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add scaling_segment_id integer;

update vespa_analysts.vespa_spot_data_By_non_sky_channel 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.vespa_spot_data_By_non_sky_channel  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.date_watched as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.vespa_spot_data_By_non_sky_channel  group by scaling_segment_id order by scaling_segment_id;

---Add weight for each scaling ID for each record

alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add weighting double;

update vespa_analysts.vespa_spot_data_By_non_sky_channel 
set weighting=b.weighting
from vespa_analysts.vespa_spot_data_By_non_sky_channel  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.date_watched as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add affluence varchar(10) ;
alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add pvr tinyint;

update vespa_analysts.vespa_spot_data_By_non_sky_channel 
set affluence=b.affluence
,pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.vespa_spot_data_By_non_sky_channel  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;


---Pivot For Live or Playback Viewing

select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live_or_playback>spot_duration then spot_duration else seconds_of_spot_viewed_live_or_playback end as seconds_of_ad_viewed
,affluence
,pvr

,count(*) as boxes
,sum(weighting) as weighted_boxes
into #live_playback_channel_pivot_non_sky
from vespa_analysts.vespa_spot_data_By_non_sky_channel
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;
commit;

--select channel_name_inc_hd , count(*) from #live_playback_channel_pivot group by channel_name_inc_hd;

select * from #live_playback_channel_pivot_non_sky;

output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_playback_pivot_data_non_sky.csv' format ascii;





---Pivot For Live Only Viewing
select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live>spot_duration then spot_duration else seconds_of_spot_viewed_live end as seconds_of_ad_viewed
,affluence
,pvr
,sum(weighting) as weighted_boxes
,count(*) as boxes
into #live_only_pivot_non_sky
from vespa_analysts.vespa_spot_data_By_non_sky_channel
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;

select * from #live_only_pivot_non_sky;

output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_only_pivot_data_non_sky.csv' format ascii;


commit;

























--select distinct channel_name_inc_hd from vespa_analysts.vespa_spot_data_15_jan order by channel_name_inc_hd;

----Sky Atlantic re-run---
/*

---Pivot For Live or Playback Viewing

select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live_or_playback>spot_duration then spot_duration else seconds_of_spot_viewed_live_or_playback end as seconds_of_ad_viewed
,affluence
,pvr

,count(*) as boxes
,sum(weighting) as weighted_boxes
into #live_playback_channel_pivot_atlantic
from vespa_analysts.vespa_spot_data_By_non_sky_channel
where channel_name_inc_hd='Sky Atlantic'
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;
commit;

--select channel_name_inc_hd , count(*) from #live_playback_channel_pivot group by channel_name_inc_hd;

select * from #live_playback_channel_pivot_atlantic;

output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_playback_pivot_data_atlantic.csv' format ascii;





---Pivot For Live Only Viewing
select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live>spot_duration then spot_duration else seconds_of_spot_viewed_live end as seconds_of_ad_viewed
,affluence
,pvr
,sum(weighting) as weighted_boxes
,count(*) as boxes
into #live_only_pivot_atlantic
from vespa_analysts.vespa_spot_data_By_non_sky_channel
where channel_name_inc_hd='Sky Atlantic'
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;

select * from #live_only_pivot_atlantic;

output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_only_pivot_data_atlantic.csv' format ascii;


commit;

--select distinct channel_name_inc_hd from vespa_analysts.vespa_spot_data_15_jan order by channel_name_inc_hd;
