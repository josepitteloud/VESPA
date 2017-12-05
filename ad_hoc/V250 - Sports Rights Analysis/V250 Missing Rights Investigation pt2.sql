
select  dk_programme_instance_dim
,broadcast_datetime
into #capital_one_cup_details
from 
dbarnett.v250_sports_rights_epg_data_for_analysis as b

where analysis_right in (
'Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup')
and live = 1
group by dk_programme_instance_dim
,broadcast_datetime
order by broadcast_datetime
;
commit;
select b.account_number
,a.broadcast_datetime
,sum(viewing_duration) as tot_dur
into #dur_by_prog
from #capital_one_cup_details as a
left outer join dbarnett.v250_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by b.account_number
,a.broadcast_datetime
;
commit;
--select top 100 * from #dur_by_prog
--select * from #capital_one_cup_details;

select broadcast_datetime
,sum(case when tot_dur>900 then 1 else 0 end) as accounts
from #dur_by_prog
group by broadcast_datetime
order by broadcast_datetime
;

select * from #capital_one_cup_details where broadcast_datetime = '2012-12-19 19:00:00'

dk_programme_instance_dim,broadcast_datetime
372571227,'2012-12-19 19:00:00.000'
359264898,'2012-12-19 19:00:00.000'
559658364,'2012-12-19 19:00:00.000'

select * from sk_prod.Vespa_programme_schedule where dk_programme_instance_dim in (372571227,359264898,559658364)


select * from sk_prod.Vespa_programme_schedule where broadcast_start_date_time_local ='2012-12-19 19:00:00'
and left(channel_name,12)='Sky Sports 1'
order by service_key

select dk_programme_instance_dim,service_key into #dim_details from sk_prod.Vespa_programme_schedule where broadcast_start_date_time_local ='2012-12-19 19:00:00'
and left(channel_name,12)='Sky Sports 1'
order by service_key

select a.dk_programme_instance_dim
,a.service_key
,sum(viewing_duration) as tot_dur
,count(distinct account_number) as acs
--into #dur_by_sk
from #dim_details as a
left outer join dbarnett.v250_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by a.dk_programme_instance_dim
,a.service_key
order by a.dk_programme_instance_dim
,a.service_key
;



--------------------




select  dk_programme_instance_dim
,broadcast_datetime
,analysis_right_new
into #details
 from 
dbarnett.v250_sports_rights_epg_data_for_analysis as b

--where 
--analysis_right_new in (
--'Premier League Football - Sky Sports (Sun 4pm)')
--and live = 1

group by dk_programme_instance_dim
,broadcast_datetime
,analysis_right_new
order by broadcast_datetime
;
commit;
--drop table #dur_by_prog
select a.broadcast_datetime
,analysis_right_new
,sum(viewing_duration) as tot_dur
into #dur_by_progv3
from #details as a
left outer join dbarnett.v250_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by a.broadcast_datetime
,analysis_right_new
;
commit;
select * from #dur_by_progv3 order by analysis_right_new, broadcast_datetime;

output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\Missing Data Analysis\duration_by_right_detailsv3.csv' format ascii;
commit;


----Total Viewing by Sk and Programme--
--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20121219 ;
select programme_trans_sk
,pk_programme_instance_dim
,dk_programme_instance_dim
,programme_instance_name
,genre_description
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
,count(*) as viewing_events
from  vespa_analysts.VESPA_DAILY_AUGS_20121219 a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
--where left(channel_name,12)='Sky Sports 1' 
--panel = 12 and 
--right(account_number,3)=''086''
--genre_description = ''Sports'' 
--and 
--viewing_duration>=180
group by programme_trans_sk
, pk_programme_instance_dim
,dk_programme_instance_dim
,programme_instance_name
,genre_description
order by viewing_duration desc
;

pk_programme_instance_dim,dk_programme_instance_dim,programme_instance_name,genre_description,viewing_duration,viewing_events
202594910,202594910,'Live Ford Monday Night Football','Sports',125209308,170384
202299908,202299908,'Live Ford Monday Night Football','Sports',109847924,155492


select * from sk_prod.Vespa_programme_schedule where broadcast_start_date_time_utc ='2012-11-05 19:00:00' and service_key = 4002

commit;

---------------Rerun for reworked EPG Dataset--------------

select  dk_programme_instance_dim
,broadcast_datetime
into #capital_one_cup_details_v2
from 
dbarnett.v250_sports_rights_epg_data_for_analysis_reworked as b

where analysis_right in (
'Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup')
and live = 1
group by dk_programme_instance_dim
,broadcast_datetime
order by broadcast_datetime
;
commit;
select b.account_number
,a.broadcast_datetime
,sum(viewing_duration) as tot_dur
into #dur_by_prog_v2
from #capital_one_cup_details_v2 as a
left outer join dbarnett.v250_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by b.account_number
,a.broadcast_datetime
;
commit;
--select top 100 * from #dur_by_prog
--select * from #capital_one_cup_details;

select broadcast_datetime
,sum(case when tot_dur>900 then 1 else 0 end) as accounts
from #dur_by_prog_v2
group by broadcast_datetime
order by broadcast_datetime
;

----------Repeat for New EPG Dataset----



select  dk_programme_instance_dim
,broadcast_datetime
,analysis_right_new
into #details_v4
 from 
dbarnett.v250_sports_rights_epg_data_for_analysis_reworked as b

--where 
--analysis_right_new in (
--'Premier League Football - Sky Sports (Sun 4pm)')
--and live = 1

group by dk_programme_instance_dim
,broadcast_datetime
,analysis_right_new
order by broadcast_datetime
;
commit;
--drop table #dur_by_prog
select a.broadcast_datetime
,analysis_right_new
,sum(viewing_duration) as tot_dur
into #dur_by_progv4
from #details_v4 as a
left outer join dbarnett.v250_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by a.broadcast_datetime
,analysis_right_new
;
commit;
select * from #dur_by_progv4 order by analysis_right_new, broadcast_datetime;

output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\Missing Data Analysis\duration_by_right_detailsv4.csv' format ascii;
commit;







