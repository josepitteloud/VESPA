/*------------------------------------------------------------------------------------------------------------------
        Project:    V200-Project Gold
        Program:    Zero Mix Analysis
        Version:    4
        Created:    20130909
        Analyst:    Dan Barnett
        SK Prod:    5
        QA:         
------------------------------------------------------------------------------------------------------------------*/

--PART A - EPG Data

---Load in CSV File containing Pay/Free Channel Info and Channel Grouping---
--drop table v200_channel_lookup_with_channel_family;

create table v200_channel_lookup_with_channel_family
(channel_name varchar(100)
,channel_name_inc_hd_staggercast_channel_families  varchar(100)
,pay_channel integer
)
;

input into v200_channel_lookup_with_channel_family
from 'C:\Users\barnetd\Documents\Project 200 - Project Gold\Project Gold Channel Name Lookup.csv' format ascii;


--select * from v200_channel_lookup_with_channel_family order by channel_name;


---Create Bespoke category for Sports/Movies seperated out for Zero Mix Project

alter table v200_channel_lookup_with_channel_family add channel_category_inc_sports_movies varchar(23);

update v200_channel_lookup_with_channel_family
set channel_category_inc_sports_movies= case    when channel_name_inc_hd_staggercast_channel_families =  'Sky Sports Channels' then '01: Sky Sports'
                                                when channel_name_inc_hd_staggercast_channel_families =  'Sky Movies Channels' then '02: Sky Movies'
                                                when pay_channel=1 then '03: Pay Channel' else '04: FTA Channel' end 

from v200_channel_lookup_with_channel_family as a
;
commit;


CREATE HG INDEX idx1 ON v200_channel_lookup_with_channel_family (channel_name);


--select top 100 programme_trans_sk from sk_prod.vespa_dp_prog_viewed_201302;

----PART B -

--Viewing from Feb-Jul 2013
--Viewing Of Pay Sports/Movies/Other Pay/Non Pay (Initial Breakdown) - May return to look at specific channels--
--drop table v200_zero_mix_viewing_201302;
---Feb 2013
select 
a.account_number
,channel_name
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,broadcast_start_date_time_utc
,programme_instance_duration
into v200_zero_mix_viewing_201302
from  sk_prod.vespa_dp_prog_viewed_201302 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;
--select count(*) from v200_zero_mix_viewing_201302;
CREATE HG INDEX idx1 ON v200_zero_mix_viewing_201302 (account_number);

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201302 (channel_name);
CREATE HG INDEX idx3 ON v200_zero_mix_viewing_201302 (broadcast_start_date_time_utc );

--Create Summary by Programme---
select account_number
,channel_name
,broadcast_start_date_time_utc
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201302
from v200_zero_mix_viewing_201302
group by account_number
,channel_name
,broadcast_start_date_time_utc
;

commit;
--select count(*) from v200_zero_mix_viewing_201302;
CREATE HG INDEX idx1 ON #summary_by_prog_201302 (account_number);
CREATE HG INDEX idx2 ON #summary_by_prog_201302 (channel_name);

--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201302_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies' then seconds_viewed else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel' then  seconds_viewed else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel' then  seconds_viewed else 0 end) as seconds_viewed_FTA

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then seconds_viewed else 0 end) as seconds_viewed_engaged_Other_Pay

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then 1 else 0 end) as programmes_viewed_engaged_Other_Pay

into v200_zero_mix_viewing_201302_summary_by_account
from #summary_by_prog_201302 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201302_summary_by_account (account_number);
--select top 500 * from v200_zero_mix_viewing_201302_summary_by_account;

--




--drop table #days_viewing_by_account_201302;
select account_number 
,count (distinct cast(viewing_starts as date)) as distinct_viewing_days
into #days_viewing_by_account_201302
from v200_zero_mix_viewing_201302 
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04')
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  #days_viewing_by_account_201302 (account_number);

alter table v200_zero_mix_viewing_201302_summary_by_account add distinct_viewing_days integer;

update v200_zero_mix_viewing_201302_summary_by_account
set distinct_viewing_days=case when b.distinct_viewing_days is null then 0 else b.distinct_viewing_days end
from v200_zero_mix_viewing_201302_summary_by_account as a
left outer join #days_viewing_by_account_201302 as b
on a.account_number = b.account_number
;
commit;

drop table v200_zero_mix_viewing_201302;

commit;

--select * into dbarnett.f_fixtures from shaha.F_Fixtures order by fixtures_date;commit;
-----Repeat for all months

---201303
--drop table v200_zero_mix_viewing_201303;
select 
a.account_number
,channel_name
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,broadcast_start_date_time_utc
,programme_instance_duration

into v200_zero_mix_viewing_201303
from  sk_prod.vespa_dp_prog_viewed_201303 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;

CREATE HG INDEX idx1 ON v200_zero_mix_viewing_201303 (account_number);

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201303 (channel_name);


--Create Summary by Programme---
select account_number
,channel_name
,broadcast_start_date_time_utc
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201303
from v200_zero_mix_viewing_201303
group by account_number
,channel_name
,broadcast_start_date_time_utc
;

--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201303_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then seconds_viewed else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then seconds_viewed else 0 end) as seconds_viewed_FTA
,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then seconds_viewed else 0 end) as seconds_viewed_engaged_Other_Pay

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then 1 else 0 end) as programmes_viewed_engaged_Other_Pay

into v200_zero_mix_viewing_201303_summary_by_account
from #summary_by_prog_201303 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201303_summary_by_account (account_number);
--select count(*) from v200_zero_mix_viewing_201303_summary_by_account;
--drop table #days_viewing_by_account_201303;
select account_number 
,count (distinct cast(viewing_starts as date)) as distinct_viewing_days
into #days_viewing_by_account_201303
from v200_zero_mix_viewing_201303 
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04')
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  #days_viewing_by_account_201303 (account_number);
--select top 100 * from v200_zero_mix_viewing_201303_summary_by_account
alter table v200_zero_mix_viewing_201303_summary_by_account add distinct_viewing_days integer;

update v200_zero_mix_viewing_201303_summary_by_account
set distinct_viewing_days=case when b.distinct_viewing_days is null then 0 else b.distinct_viewing_days end
from v200_zero_mix_viewing_201303_summary_by_account as a 
left outer join #days_viewing_by_account_201303 as b
on a.account_number = b.account_number
;
commit;
--select top 100 * from v200_zero_mix_viewing_201303_summary_by_account;
drop table v200_zero_mix_viewing_201303;

commit;


---201304

select 
a.account_number
,channel_name
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,broadcast_start_date_time_utc
,programme_instance_duration
into v200_zero_mix_viewing_201304
from  sk_prod.vespa_dp_prog_viewed_201304 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;

CREATE HG INDEX idx1 ON v200_zero_mix_viewing_201304 (account_number);

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201304 (channel_name);

--Create Summary by Programme---
select account_number
,channel_name
,broadcast_start_date_time_utc
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201304
from v200_zero_mix_viewing_201304
group by account_number
,channel_name
,broadcast_start_date_time_utc
;
commit;
CREATE HG INDEX idx1 ON #summary_by_prog_201304 (account_number);

CREATE HG INDEX idx2 ON #summary_by_prog_201304 (channel_name);
--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201304_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then seconds_viewed else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then seconds_viewed else 0 end) as seconds_viewed_FTA
,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then seconds_viewed else 0 end) as seconds_viewed_engaged_Other_Pay

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then 1 else 0 end) as programmes_viewed_engaged_Other_Pay

into v200_zero_mix_viewing_201304_summary_by_account
from #summary_by_prog_201304 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;
--select top 500 * from v200_zero_mix_viewing_201304_summary_by_account;
 HG INDEX idx1 ON  v200_zero_mix_viewing_201304_summary_by_account (account_number);

--drop table #days_viewing_by_account_201304;
select account_number 
,count (distinct cast(viewing_starts as date)) as distinct_viewing_days
into #days_viewing_by_account_201304
from v200_zero_mix_viewing_201304 
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04')
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  #days_viewing_by_account_201304 (account_number);

alter table v200_zero_mix_viewing_201304_summary_by_account add distinct_viewing_days integer;

update v200_zero_mix_viewing_201304_summary_by_account
set distinct_viewing_days=case when b.distinct_viewing_days is null then 0 else b.distinct_viewing_days end
from v200_zero_mix_viewing_201304_summary_by_account as a 
left outer join #days_viewing_by_account_201304 as b
on a.account_number = b.account_number
;
commit;

drop table v200_zero_mix_viewing_201304;

commit;
---201305
select 
a.account_number
,channel_name
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,broadcast_start_date_time_utc
,programme_instance_duration
into v200_zero_mix_viewing_201305
from  sk_prod.vespa_dp_prog_viewed_201305 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;

CREATE HG INDEX idx1 ON v200_zero_mix_viewing_201305 (account_number);

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201305 (channel_name);

--Create Summary by Programme---
select account_number
,channel_name
,broadcast_start_date_time_utc
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201305
from v200_zero_mix_viewing_201305
group by account_number
,channel_name
,broadcast_start_date_time_utc
;
--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201305_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then seconds_viewed else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then seconds_viewed else 0 end) as seconds_viewed_FTA
,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then seconds_viewed else 0 end) as seconds_viewed_engaged_Other_Pay

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then 1 else 0 end) as programmes_viewed_engaged_Other_Pay

into v200_zero_mix_viewing_201305_summary_by_account
from  #summary_by_prog_201305 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;


--select count(*) from v200_zero_mix_viewing_201306_summary_by_account

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201305_summary_by_account (account_number);

--drop table #days_viewing_by_account_201305;
select account_number 
,count (distinct cast(viewing_starts as date)) as distinct_viewing_days
into #days_viewing_by_account_201305
from v200_zero_mix_viewing_201305 
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04')
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  #days_viewing_by_account_201305 (account_number);

alter table v200_zero_mix_viewing_201305_summary_by_account add distinct_viewing_days integer;

update v200_zero_mix_viewing_201305_summary_by_account
set distinct_viewing_days=case when b.distinct_viewing_days is null then 0 else b.distinct_viewing_days end
from v200_zero_mix_viewing_201305_summary_by_account as a 
left outer join #days_viewing_by_account_201305 as b
on a.account_number = b.account_number
;
commit;

drop table v200_zero_mix_viewing_201305;

commit;

--select top 500 * from v200_zero_mix_viewing_201306_summary_by_account;

---201306
--drop table v200_zero_mix_viewing_201306;
select 
a.account_number
,channel_name
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,broadcast_start_date_time_utc
,programme_instance_duration
into v200_zero_mix_viewing_201306
from  sk_prod.vespa_dp_prog_viewed_201306 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;
--select count(*) from v200_zero_mix_viewing_201306
CREATE HG INDEX idx1 ON v200_zero_mix_viewing_201306 (account_number);

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201306 (channel_name);

--Create Summary by Programme---
select account_number
,channel_name
,broadcast_start_date_time_utc
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201306
from v200_zero_mix_viewing_201306
group by account_number
,channel_name
,broadcast_start_date_time_utc
;

commit;
CREATE HG INDEX idx1 ON #summary_by_prog_201306 (account_number);

CREATE HG INDEX idx2 ON #summary_by_prog_201306 (channel_name);
--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---

--select count(*) from v200_zero_mix_viewing_201307_summary_by_account;

--drop table v200_zero_mix_viewing_201306_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then seconds_viewed else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then seconds_viewed else 0 end) as seconds_viewed_FTA
,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then seconds_viewed else 0 end) as seconds_viewed_engaged_Other_Pay

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then 1 else 0 end) as programmes_viewed_engaged_Other_Pay

into v200_zero_mix_viewing_201306_summary_by_account
from #summary_by_prog_201306 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201306_summary_by_account (account_number);

--drop table #days_viewing_by_account_201306;
select account_number 
,count (distinct cast(viewing_starts as date)) as distinct_viewing_days
into #days_viewing_by_account_201306
from v200_zero_mix_viewing_201306 
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04')
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  #days_viewing_by_account_201306 (account_number);

alter table v200_zero_mix_viewing_201306_summary_by_account add distinct_viewing_days integer;

update v200_zero_mix_viewing_201306_summary_by_account
set distinct_viewing_days=case when b.distinct_viewing_days is null then 0 else b.distinct_viewing_days end
from v200_zero_mix_viewing_201306_summary_by_account as a 
left outer join #days_viewing_by_account_201306 as b
on a.account_number = b.account_number
;
commit;

drop table v200_zero_mix_viewing_201306;

commit;
---201307
select 
a.account_number
,channel_name
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,broadcast_start_date_time_utc
,programme_instance_duration
into v200_zero_mix_viewing_201307
from  sk_prod.vespa_dp_prog_viewed_201307 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;

CREATE HG INDEX idx1 ON v200_zero_mix_viewing_201307 (account_number);

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201307 (channel_name);
--Create Summary by Programme---
select account_number
,channel_name
,broadcast_start_date_time_utc
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201307
from v200_zero_mix_viewing_201307
group by account_number
,channel_name
,broadcast_start_date_time_utc
;

--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201307_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then seconds_viewed else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then seconds_viewed else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then seconds_viewed else 0 end) as seconds_viewed_FTA
,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then seconds_viewed else 0 end) as seconds_viewed_engaged_Other_Pay

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then 1 else 0 end) as programmes_viewed_engaged_Other_Pay

into v200_zero_mix_viewing_201307_summary_by_account
from #summary_by_prog_201307 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201307_summary_by_account (account_number);

--drop table #days_viewing_by_account_201307;
select account_number 
,count (distinct cast(viewing_starts as date)) as distinct_viewing_days
into #days_viewing_by_account_201307
from v200_zero_mix_viewing_201307 
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04')
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  #days_viewing_by_account_201307 (account_number);

alter table v200_zero_mix_viewing_201307_summary_by_account add distinct_viewing_days integer;

update v200_zero_mix_viewing_201307_summary_by_account
set distinct_viewing_days=case when b.distinct_viewing_days is null then 0 else b.distinct_viewing_days end
from v200_zero_mix_viewing_201307_summary_by_account as a 
left outer join #days_viewing_by_account_201307 as b
on a.account_number = b.account_number
;
commit;

drop table v200_zero_mix_viewing_201307;

commit;
--select top 100 * from v200_zero_mix_viewing_201307_summary_by_account;
--select * from #days_viewing_by_account_201307;

/*
drop table v200_zero_mix_viewing_201303_summary_by_account;
drop table v200_zero_mix_viewing_201304_summary_by_account;
drop table v200_zero_mix_viewing_201305_summary_by_account;
drop table v200_zero_mix_viewing_201306_summary_by_account;
drop table v200_zero_mix_viewing_201307_summary_by_account;
commit;
*/


--Join all Tables to create a 6 month Master View--
--drop table v200_zero_mix_full_account_list;
create table v200_zero_mix_full_account_list
(account_number varchar(50)

)
;

commit;
insert into v200_zero_mix_full_account_list
(select account_number
from v200_zero_mix_viewing_201302_summary_by_account)
;

--Add on March Viewing Accounts
insert into v200_zero_mix_full_account_list
(select a.account_number
from v200_zero_mix_viewing_201303_summary_by_account as a
left outer join  v200_zero_mix_full_account_list as b
on a.account_number=b.account_number
where b.account_number is null)
;
--Add on April Viewing Accounts
insert into v200_zero_mix_full_account_list
(select a.account_number
from v200_zero_mix_viewing_201304_summary_by_account as a
left outer join  v200_zero_mix_full_account_list as b
on a.account_number=b.account_number
where b.account_number is null)
;

--Add on May Viewing Accounts
insert into v200_zero_mix_full_account_list
(select a.account_number
from v200_zero_mix_viewing_201305_summary_by_account as a
left outer join  v200_zero_mix_full_account_list as b
on a.account_number=b.account_number
where b.account_number is null)
;

--Add on June Viewing Accounts
insert into v200_zero_mix_full_account_list
(select a.account_number
from v200_zero_mix_viewing_201306_summary_by_account as a
left outer join  v200_zero_mix_full_account_list as b
on a.account_number=b.account_number
where b.account_number is null)
;

--Add on July Viewing Accounts
insert into v200_zero_mix_full_account_list
(select a.account_number
from v200_zero_mix_viewing_201307_summary_by_account as a
left outer join  v200_zero_mix_full_account_list as b
on a.account_number=b.account_number
where b.account_number is null)
;

commit;

alter table v200_zero_mix_full_account_list add 
(seconds_viewed_sky_sports_201302 integer default 0
,seconds_viewed_sky_movies_201302 integer default 0
,seconds_viewed_other_pay_201302 integer default 0
,seconds_viewed_FTA_201302 integer default 0
,seconds_viewed_engaged_Other_Pay_201302  integer default 0
,programmes_viewed_engaged_Other_Pay_201302  integer default 0
,distinct_viewing_days_201302 integer default 0

,seconds_viewed_sky_sports_201303 integer default 0
,seconds_viewed_sky_movies_201303 integer default 0
,seconds_viewed_other_pay_201303 integer default 0
,seconds_viewed_FTA_201303 integer default 0
,seconds_viewed_engaged_Other_Pay_201303  integer default 0
,programmes_viewed_engaged_Other_Pay_201303  integer default 0
,distinct_viewing_days_201303 integer default 0

,seconds_viewed_sky_sports_201304 integer default 0
,seconds_viewed_sky_movies_201304 integer default 0
,seconds_viewed_other_pay_201304 integer default 0
,seconds_viewed_FTA_201304 integer default 0
,seconds_viewed_engaged_Other_Pay_201304  integer default 0
,programmes_viewed_engaged_Other_Pay_201304  integer default 0
,distinct_viewing_days_201304 integer default 0

,seconds_viewed_sky_sports_201305 integer default 0
,seconds_viewed_sky_movies_201305 integer default 0
,seconds_viewed_other_pay_201305 integer default 0
,seconds_viewed_FTA_201305 integer default 0
,seconds_viewed_engaged_Other_Pay_201305  integer default 0
,programmes_viewed_engaged_Other_Pay_201305  integer default 0
,distinct_viewing_days_201305 integer default 0

,seconds_viewed_sky_sports_201306 integer default 0
,seconds_viewed_sky_movies_201306 integer default 0
,seconds_viewed_other_pay_201306 integer default 0
,seconds_viewed_FTA_201306 integer default 0
,seconds_viewed_engaged_Other_Pay_201306  integer default 0
,programmes_viewed_engaged_Other_Pay_201306  integer default 0
,distinct_viewing_days_201306 integer default 0

,seconds_viewed_sky_sports_201307 integer default 0
,seconds_viewed_sky_movies_201307 integer default 0
,seconds_viewed_other_pay_201307 integer default 0
,seconds_viewed_FTA_201307 integer default 0
,seconds_viewed_engaged_Other_Pay_201307  integer default 0
,programmes_viewed_engaged_Other_Pay_201307  integer default 0
,distinct_viewing_days_201307 integer default 0
)
;

--Update days viewing and seconds viewed by type

update  v200_zero_mix_full_account_list
set seconds_viewed_sky_sports_201302=b.seconds_viewed_sky_sports
,seconds_viewed_sky_movies_201302=b.seconds_viewed_sky_movies
,seconds_viewed_other_pay_201302=b.seconds_viewed_other_pay
,seconds_viewed_FTA_201302=b.seconds_viewed_FTA

,seconds_viewed_engaged_Other_Pay_201302 =b.seconds_viewed_engaged_Other_Pay
,programmes_viewed_engaged_Other_Pay_201302 =b.programmes_viewed_engaged_Other_Pay
,distinct_viewing_days_201302=b.distinct_viewing_days
from v200_zero_mix_full_account_list as a 
left outer join v200_zero_mix_viewing_201302_summary_by_account as b
on a.account_number = b.account_number
where b.account_number is not null
;


update  v200_zero_mix_full_account_list
set seconds_viewed_sky_sports_201303=b.seconds_viewed_sky_sports
,seconds_viewed_sky_movies_201303=b.seconds_viewed_sky_movies
,seconds_viewed_other_pay_201303=b.seconds_viewed_other_pay
,seconds_viewed_FTA_201303=b.seconds_viewed_FTA
,seconds_viewed_engaged_Other_Pay_201303 =b.seconds_viewed_engaged_Other_Pay
,programmes_viewed_engaged_Other_Pay_201303 =b.programmes_viewed_engaged_Other_Pay
,distinct_viewing_days_201303=b.distinct_viewing_days
from v200_zero_mix_full_account_list as a 
left outer join v200_zero_mix_viewing_201303_summary_by_account as b
on a.account_number = b.account_number
where b.account_number is not null
;

update  v200_zero_mix_full_account_list
set seconds_viewed_sky_sports_201304=b.seconds_viewed_sky_sports
,seconds_viewed_sky_movies_201304=b.seconds_viewed_sky_movies
,seconds_viewed_other_pay_201304=b.seconds_viewed_other_pay
,seconds_viewed_FTA_201304=b.seconds_viewed_FTA
,seconds_viewed_engaged_Other_Pay_201304 =b.seconds_viewed_engaged_Other_Pay
,programmes_viewed_engaged_Other_Pay_201304 =b.programmes_viewed_engaged_Other_Pay
,distinct_viewing_days_201304=b.distinct_viewing_days
from v200_zero_mix_full_account_list as a 
left outer join v200_zero_mix_viewing_201304_summary_by_account as b
on a.account_number = b.account_number
where b.account_number is not null
;

update  v200_zero_mix_full_account_list
set seconds_viewed_sky_sports_201305=b.seconds_viewed_sky_sports
,seconds_viewed_sky_movies_201305=b.seconds_viewed_sky_movies
,seconds_viewed_other_pay_201305=b.seconds_viewed_other_pay
,seconds_viewed_FTA_201305=b.seconds_viewed_FTA
,seconds_viewed_engaged_Other_Pay_201305 =b.seconds_viewed_engaged_Other_Pay
,programmes_viewed_engaged_Other_Pay_201305 =b.programmes_viewed_engaged_Other_Pay
,distinct_viewing_days_201305=b.distinct_viewing_days
from v200_zero_mix_full_account_list as a 
left outer join v200_zero_mix_viewing_201305_summary_by_account as b
on a.account_number = b.account_number
where b.account_number is not null
;

update  v200_zero_mix_full_account_list
set seconds_viewed_sky_sports_201306=b.seconds_viewed_sky_sports
,seconds_viewed_sky_movies_201306=b.seconds_viewed_sky_movies
,seconds_viewed_other_pay_201306=b.seconds_viewed_other_pay
,seconds_viewed_FTA_201306=b.seconds_viewed_FTA
,seconds_viewed_engaged_Other_Pay_201306 =b.seconds_viewed_engaged_Other_Pay
,programmes_viewed_engaged_Other_Pay_201306 =b.programmes_viewed_engaged_Other_Pay
,distinct_viewing_days_201306=b.distinct_viewing_days
from v200_zero_mix_full_account_list as a 
left outer join v200_zero_mix_viewing_201306_summary_by_account as b
on a.account_number = b.account_number
where b.account_number is not null
;

update  v200_zero_mix_full_account_list
set seconds_viewed_sky_sports_201307=b.seconds_viewed_sky_sports
,seconds_viewed_sky_movies_201307=b.seconds_viewed_sky_movies
,seconds_viewed_other_pay_201307=b.seconds_viewed_other_pay
,seconds_viewed_FTA_201307=b.seconds_viewed_FTA
,seconds_viewed_engaged_Other_Pay_201307 =b.seconds_viewed_engaged_Other_Pay
,programmes_viewed_engaged_Other_Pay_201307 =b.programmes_viewed_engaged_Other_Pay
,distinct_viewing_days_201307=b.distinct_viewing_days
from v200_zero_mix_full_account_list as a 
left outer join v200_zero_mix_viewing_201307_summary_by_account as b
on a.account_number = b.account_number
where b.account_number is not null
;

commit;


--select top 100 * from v200_zero_mix_full_account_list

---Add on Combined activity across the 6 months--
alter table v200_zero_mix_full_account_list add 
(seconds_viewed_sky_sports_201302_to_201307 integer default 0
,seconds_viewed_sky_movies_201302_to_201307 integer default 0
,seconds_viewed_other_pay_201302_to_201307 integer default 0
,seconds_viewed_FTA_201302_to_201307 integer default 0
,seconds_viewed_engaged_Other_Pay_201302_to_201307 integer default 0
,programmes_viewed_engaged_Other_Pay_201302_to_201307 integer default 0
,distinct_viewing_days_201302_to_201307 integer default 0
)
;
update v200_zero_mix_full_account_list
set seconds_viewed_sky_sports_201302_to_201307=seconds_viewed_sky_sports_201302+seconds_viewed_sky_sports_201303+seconds_viewed_sky_sports_201304
                                               +seconds_viewed_sky_sports_201305+seconds_viewed_sky_sports_201306+seconds_viewed_sky_sports_201307
,seconds_viewed_sky_movies_201302_to_201307=seconds_viewed_sky_movies_201302+seconds_viewed_sky_movies_201303+seconds_viewed_sky_movies_201304
                                               +seconds_viewed_sky_movies_201305+seconds_viewed_sky_movies_201306+seconds_viewed_sky_movies_201307
,seconds_viewed_other_pay_201302_to_201307=seconds_viewed_other_pay_201302+seconds_viewed_other_pay_201303+seconds_viewed_other_pay_201304
                                               +seconds_viewed_other_pay_201305+seconds_viewed_other_pay_201306+seconds_viewed_other_pay_201307
,seconds_viewed_FTA_201302_to_201307=seconds_viewed_FTA_201302+seconds_viewed_FTA_201303+seconds_viewed_FTA_201304
                                               +seconds_viewed_FTA_201305+seconds_viewed_FTA_201306+seconds_viewed_FTA_201307


,seconds_viewed_engaged_Other_Pay_201302_to_201307=seconds_viewed_engaged_Other_Pay_201302+seconds_viewed_engaged_Other_Pay_201303+seconds_viewed_engaged_Other_Pay_201304
                                               +seconds_viewed_engaged_Other_Pay_201305+seconds_viewed_engaged_Other_Pay_201306+seconds_viewed_engaged_Other_Pay_201307

,programmes_viewed_engaged_Other_Pay_201302_to_201307=programmes_viewed_engaged_Other_Pay_201302+programmes_viewed_engaged_Other_Pay_201303+programmes_viewed_engaged_Other_Pay_201304
                                               +programmes_viewed_engaged_Other_Pay_201305+programmes_viewed_engaged_Other_Pay_201306+programmes_viewed_engaged_Other_Pay_201307

,distinct_viewing_days_201302_to_201307=distinct_viewing_days_201302+distinct_viewing_days_201303+distinct_viewing_days_201304
                                               +distinct_viewing_days_201305+distinct_viewing_days_201306+distinct_viewing_days_201307

;
commit;

---Add on account attributes (Current Status Sports/Movies/Mixes etc.,)
alter table v200_zero_mix_full_account_list add (
current_status_code varchar(2)
,number_of_sports_premiums integer default 0
,number_of_movies_premiums integer default 0
,mix_type varchar(40)
,entertainment_extra_flag tinyint default 0
);

update v200_zero_mix_full_account_list
set current_status_code = b.acct_status_code
,number_of_sports_premiums=b.PROD_LATEST_ENTITLEMENT_PREM_SPORTS
,number_of_movies_premiums=b.PROD_LATEST_ENTITLEMENT_PREM_MOVIES

,mix_type=CASE WHEN  cel.mixes = 0                     THEN 'A) 0 Mixes'
            WHEN  cel.mixes = 1
             AND (style_culture = 1 OR variety = 1) THEN 'B) 1 Mix - Variety or Style&Culture'
            WHEN  cel.mixes = 1                     THEN 'C) 1 Mix - Other'
            WHEN  cel.mixes = 2
             AND  style_culture = 1
             AND  variety = 1                       THEN 'D) 2 Mixes - Variety and Style&Culture'
            WHEN  cel.mixes = 2
             AND (style_culture = 0 OR variety = 0) THEN 'E) 2 Mixes - Other Combination'
            WHEN  cel.mixes = 3                     THEN 'F) 3 Mixes'
            WHEN  cel.mixes = 4                     THEN 'G) 4 Mixes'
            WHEN  cel.mixes = 5                     THEN 'H) 5 Mixes'
            WHEN  cel.mixes = 6                     THEN 'I) 6 Mixes'
            ELSE                                         'J) Unknown'
        END 
from v200_zero_mix_full_account_list as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
left outer join sk_prod.cust_entitlement_lookup as cel
on b.PROD_LATEST_ENTITLEMENT_CODE = cel.short_description
where b.account_number is not null
;

update v200_zero_mix_full_account_list
set entertainment_extra_flag=CASE WHEN mix_type IN ( 'A) 0 Mixes'
                                            ,'B) 1 Mix - Variety or Style&Culture'
                                            ,'D) 2 Mixes - Variety and Style&Culture')
                          THEN 0

                          WHEN mix_type IN ( 'C) 1 Mix - Other'
                                            ,'E) 2 Mixes - Other Combination'
                                            ,'F) 3 Mixes'
                                            ,'G) 4 Mixes'
                                            ,'H) 5 Mixes'
                                            ,'I) 6 Mixes')
                          THEN  1
                          ELSE  0 end
from  v200_zero_mix_full_account_list;
commit;

---Panel Selection---

select distinct_viewing_days_201302_to_201307 ,count(*) , sum(case when current_status_code in ('AC','AB','PC') then 1 else 0 end) as active
from v200_zero_mix_full_account_list
group by distinct_viewing_days_201302_to_201307
order by distinct_viewing_days_201302_to_201307
;


/*
select distinct_viewing_days_201307 ,count(*) , sum(case when current_status_code in ('AC','AB','PC') then 1 else 0 end) as active
from v200_zero_mix_full_account_list
group by distinct_viewing_days_201307
order by distinct_viewing_days_201307
;
*/
commit;
select count(*) , sum(case when current_status_code in ('AC','AB','PC') then 1 else 0 end) as active

, sum(case when distinct_viewing_days_201302>=10 and distinct_viewing_days_201303>=10 and distinct_viewing_days_201304
                                               >=10 and distinct_viewing_days_201305>=10 and distinct_viewing_days_201306>=10 and distinct_viewing_days_201307>=10 and
 current_status_code in ('AC','AB','PC') then 1 else 0 end) as active_10_days_plus_each_month

, sum(case when distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC') then 1 else 0 end) as active_15_days_plus_each_month

, sum(case when distinct_viewing_days_201302>=20 and distinct_viewing_days_201303>=20 and distinct_viewing_days_201304
                                               >=20 and distinct_viewing_days_201305>=20 and distinct_viewing_days_201306>=20 and distinct_viewing_days_201307>=20 and
 current_status_code in ('AC','AB','PC') then 1 else 0 end) as active_20_days_plus_each_month
from v200_zero_mix_full_account_list
--where distinct_viewing_days_201302_to_201307>=90
;

commit;

---Average non-Premium Viewing per day from eligible accounts to analyse
select case when seconds_viewed_other_pay_201302_to_201307/60/distinct_viewing_days_201302_to_201307 >=660 then 660 else 
abs(round(seconds_viewed_other_pay_201302_to_201307/60/distinct_viewing_days_201302_to_201307,0)/5)*5 end 
as average_pay_exp_premium_viewing_per_day
,count(*) as accounts
from v200_zero_mix_full_account_list
where distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC')
group by average_pay_exp_premium_viewing_per_day
order by average_pay_exp_premium_viewing_per_day
;

--Repeat but just on engaged seconds
select case when seconds_viewed_engaged_Other_Pay_201302_to_201307/60/distinct_viewing_days_201302_to_201307 >=660 then 660 else 
abs(round(seconds_viewed_engaged_Other_Pay_201302_to_201307/60/distinct_viewing_days_201302_to_201307,0)/5)*5 end 
as average_pay_exp_premium_viewing_per_day
,count(*) as accounts
from v200_zero_mix_full_account_list
where distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC')
group by average_pay_exp_premium_viewing_per_day
order by average_pay_exp_premium_viewing_per_day
;

--Compare engaged and unengaged seconds---
select case when seconds_viewed_engaged_Other_Pay_201302_to_201307=0 then '01: No Engaged non-Premium Pay TV Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.3 then '02: Under 30% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.4 then '03: Under 40% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.5 then '04: Under 50% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.6 then '05: Under 60% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.7 then '06: Under 70% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.8 then '07: Under 80% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.9 then '08: Under 90% of non-Premium Pay TV Viewing from Engaged Viewing'
else '09: 90%+' end as proportion_from_engaged_viewing    
,count(*) as accounts
from v200_zero_mix_full_account_list
where distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC')
group by proportion_from_engaged_viewing
order by proportion_from_engaged_viewing
;
commit;


---Split proportion engaged and total duration
select case when seconds_viewed_other_pay_201302_to_201307/60/distinct_viewing_days_201302_to_201307 >=660 then 660 else 
abs(round(seconds_viewed_other_pay_201302_to_201307/60/distinct_viewing_days_201302_to_201307,0)/15)*15 end 
as average_pay_exp_premium_viewing_per_day
,case when seconds_viewed_engaged_Other_Pay_201302_to_201307=0 then '01: No Engaged non-Premium Pay TV Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.3 then '02: Under 30% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.4 then '03: Under 40% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.5 then '04: Under 50% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.6 then '05: Under 60% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.7 then '06: Under 70% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.8 then '07: Under 80% of non-Premium Pay TV Viewing from Engaged Viewing'
            when seconds_viewed_engaged_Other_Pay_201302_to_201307/cast(seconds_viewed_other_pay_201302_to_201307 as real)<0.9 then '08: Under 90% of non-Premium Pay TV Viewing from Engaged Viewing'
else '09: 90%+' end as proportion_from_engaged_viewing    
,count(*) as accounts
from v200_zero_mix_full_account_list
where distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC')
group by average_pay_exp_premium_viewing_per_day ,proportion_from_engaged_viewing
order by average_pay_exp_premium_viewing_per_day,proportion_from_engaged_viewing
;



---Average non-Premium Engaged Programmes per week from eligible accounts to analyse
select case when (programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/distinct_viewing_days_201302_to_201307 >=140 then 140 else 
abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0)) end 
as average_pay_exp_premium_viewing_programmes_per_week
,count(*) as accounts
from v200_zero_mix_full_account_list
where distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC')
group by average_pay_exp_premium_viewing_programmes_per_week
order by average_pay_exp_premium_viewing_programmes_per_week
;
commit;

---Total Seconds Viewed
select sum(distinct_viewing_days_201302_to_201307)
,sum(seconds_viewed_engaged_Other_Pay_201302_to_201307)
,sum(seconds_viewed_Other_Pay_201302_to_201307)
from v200_zero_mix_full_account_list
where distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC')


----PART B -- Add Account Variables

alter table v200_zero_mix_full_account_list add(                           
                            tenure                     varchar(20)
                            ,cb_key_household           bigint
                            ,isba_tv_region             varchar(20)
                            ,hh_composition             varchar(2)
                            ,hh_affluence               varchar(2)
                            ,head_hh_age                varchar(1)
                            ,num_children_in_hh         varchar(1) 
                            ,num_bedrooms               varchar(1) 
                            ,residence_type             varchar(1)        
                            ,Sky_Go_Reg                 smallint     
                            ,BB_type                    varchar(20)
                            ,hdtv                       smallint     
                            ,multiroom                  smallint     
                            ,skyplus                    smallint   
                            ,subscription_3d            smallint 
                            ,value_segment              varchar(20) 
);


----


--TENURE
select          distinct(account_number) as account_number
                ,case when max(datediff(day,acct_first_account_activation_dt,'2013-09-12')) <=  365 then 'A) 0-12 Months'
                when max(datediff(day,acct_first_account_activation_dt,'2013-09-12')) <=  730 then 'B) 1-2 Years'
                when max(datediff(day,acct_first_account_activation_dt,'2013-09-12')) <= 1095 then 'C) 2-3 Years'
                when max(datediff(day,acct_first_account_activation_dt,'2013-09-12')) <= 1825 then 'D) 3-5 Years'
                when max(datediff(day,acct_first_account_activation_dt,'2013-09-12')) <= 3650 then 'E) 5-10 Years'
                else                                                                          'F) 10 Years+' end as tenure         
into            #tenure
from            sk_prod.cust_single_account_view
group by        account_number
order by        account_number
;

CREATE HG INDEX idx1 ON #tenure (account_number);

Update          v200_zero_mix_full_account_list
set             a.tenure = case when b.tenure is null then 'UNK*' else b.tenure end
from            v200_zero_mix_full_account_list as a
left join       #tenure as b 
on              a.account_number = b.account_number;
commit;
;

--ISBA Region
update          v200_zero_mix_full_account_list
set             a.isba_tv_region= case when b.isba_tv_region is null then 'UNK*' else b.isba_tv_region end
                ,a.cb_key_household=b.cb_key_household
from            v200_zero_mix_full_account_list as a
left join       sk_prod.cust_single_account_view as b
on              a.account_number=b.account_number
;

create hg index idx1 on v200_zero_mix_full_account_list(cb_key_household);
commit;



--EXPERIAN - COMPOSITION, AFFLUENCE, AGE, NUMB CHILDREN
select          cb_key_household
                ,max(h_household_composition)                   as hh_composition
                ,max(h_affluence_v2)                            as hh_affluence
                ,max(h_age_coarse)                              as head_hh_age
                ,max(h_number_of_children_in_household_2011)    as num_children_in_hh
                ,max(h_number_of_bedrooms)                      as num_bedrooms
                ,max(h_residence_type_v2)                       as residence_type
                ,min(h_tenure_v2) as household_ownership_type
                ,min(filler_char15) as affluence_septile

into            #experian_hh_summary
FROM            sk_prod.experian_consumerview
where           cb_address_status = '1' 
and             cb_address_dps IS NOT NULL 
and             cb_address_organisation IS NULL
group by        cb_key_household
;
commit;

exec sp_create_tmp_table_idx '#experian_hh_summary', 'cb_key_household';
commit;

update          v200_zero_mix_full_account_list
set             hh_composition= b.hh_composition
                ,hh_affluence=b.hh_affluence
                ,head_hh_age=b.head_hh_age
                ,num_children_in_hh=b.num_children_in_hh
                ,num_bedrooms=b.num_bedrooms
                ,residence_type=b.residence_type
                ,household_ownership_type=b.household_ownership_type
                ,affluence_septile=b.affluence_septile
from            v200_zero_mix_full_account_list as a
left join       #experian_hh_summary as b
on              a.cb_key_household=b.cb_key_household
;

--SKY GO USAGE -LAST 6 MONTHS;
select          distinct(account_number)
                ,1 as sky_go_reg
into            #skygo_usage
from            SK_PROD.SKY_PLAYER_USAGE_DETAIL
where           cb_data_date >= cast('2013-09-12' as date)-182
and             cb_data_date <='2013-09-12'
group by        account_number
order by        account_number
;
commit;
exec sp_create_tmp_table_idx '#skygo_usage', 'account_number';
commit;
Update          v200_zero_mix_full_account_list
set             a.sky_go_reg = case when b.sky_go_reg=1 then 1 else 0 end
from            v200_zero_mix_full_account_list as a
left join       #skygo_usage as b 
on              a.account_number = b.account_number;
commit;
;
drop table #skygo_usage;



--BB_TYPE;
Select          distinct account_number
                ,CASE WHEN current_product_sk=43373 THEN '1) Unlimited (New)'
                WHEN current_product_sk=42128 THEN '2) Unlimited (Old)'
                WHEN current_product_sk=42129 THEN '3) Everyday'
                WHEN current_product_sk=42130 THEN '4) Everyday Lite'
                WHEN current_product_sk=42131 THEN '5) Connect'
                ELSE 'NA'
                END AS bb_type
                ,rank() over(PARTITION BY account_number ORDER BY effective_to_dt desc) AS rank_id
INTO            #bb
FROM            sk_prod.cust_subs_hist
WHERE           subscription_sub_type = 'Broadband DSL Line'
and             effective_from_dt <= '2013-09-12'
and             effective_to_dt > '2013-09-12'
and             effective_from_dt != effective_to_dt
and             (status_code IN ('AC','AB') 
                OR (status_code='PC' and prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA') )
                OR (status_code='CF' AND prev_status_code='PC')
                OR (status_code='AP' AND sale_type='SNS Bulk Migration'))
GROUP BY        account_number
                ,bb_type
                ,effective_to_dt
;
DELETE FROM #bb where rank_id >1;
commit;
commit;
exec sp_create_tmp_table_idx '#bb', 'account_number';
commit;

select          distinct account_number, BB_type
                ,rank() over(PARTITION BY account_number ORDER BY BB_type desc) AS rank_id
into            #bbb
from            #bb;
commit;

DELETE FROM #bbb where rank_id >1;
commit;
commit;
exec sp_create_tmp_table_idx '#bbb', 'account_number';
commit;
Update          v200_zero_mix_full_account_list
set             a.bb_type = case when b.bb_type is null then '6) NA' else b.bb_type end
from            v200_zero_mix_full_account_list as a
left join       #bbb as b 
on              a.account_number = b.account_number;
commit;
;

drop table #bb;
drop table #bbb;

--MULTI-ROOM, SKY+, HDTV & 3DTV;
SELECT          account_number
                ,MAX(CASE WHEN subscription_sub_type ='DTV Extra Subscription'                      THEN 1 ELSE 0 END) AS multiroom
                ,MAX(CASE WHEN subscription_sub_type ='DTV HD'                                      THEN 1 ELSE 0 END) AS hdtv
                ,MAX(CASE WHEN subscription_sub_type ='DTV Sky+'                                    THEN 1 ELSE 0 END) AS skyplus
                ,max(case when subscription_type = 'A-LA-CARTE' and subscription_sub_type = '3DTV'  THEN 1 ELSE 0 END) AS subscription_3d
INTO            #box_prods
FROM            sk_prod.cust_subs_hist
WHERE           subscription_sub_type  IN ('DTV Extra Subscription','DTV HD','DTV Sky+','3DTV')
AND             effective_from_dt <> effective_to_dt
AND             effective_from_dt <= '2013-09-12'
AND             effective_to_dt    >  '2013-09-12'
AND             status_code in  ('AC','AB','PC')
GROUP BY        account_number;


commit;
exec sp_create_tmp_table_idx '#bbb', 'account_number';
commit;

Update          v200_zero_mix_full_account_list
set             a.multiroom = case when b.multiroom =1 then b.multiroom else 0 end
                ,a.hdtv = case when b.hdtv =1 then b.hdtv else 0 end
                ,a.skyplus = case when b.skyplus =1 then b.skyplus else 0 end
                ,a.subscription_3d = case when b.subscription_3d =1 then b.subscription_3d else 0 end
from            v200_zero_mix_full_account_list as a
left join       #box_prods as b 
on              a.account_number = b.account_number;
commit;

--VALUE SEGMENT
--select value_seg_date, count(*) from sk_prod.value_segments_five_yrs group by value_seg_date order by value_seg_date desc;
select          account_number
                ,value_segment
into            #vs
from            sk_prod.value_segments_five_yrs
where           value_seg_date='2013-09-02'
order by        account_number
;

commit;
exec sp_create_tmp_table_idx '#vs', 'account_number';
commit;

Update          v200_zero_mix_full_account_list
set             a.value_segment = case when b.value_segment is null then 'UNKNOWN' else b.value_segment end
from            v200_zero_mix_full_account_list as a
left join       #vs as b 
on              a.account_number = b.account_number;
commit;



----







/*
select case when (programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/distinct_viewing_days_201302_to_201307 >=140 then 140 else 
abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/distinct_viewing_days_201302_to_201307,0)) end 
as average_pay_exp_premium_viewing_programmes_per_week
,(programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real))
,abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0) 
as average_pay_exp_premium_viewing_programmes_per_weekv2
--,count(*) as accounts
from v200_zero_mix_full_account_list
where distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC')
group by average_pay_exp_premium_viewing_programmes_per_week
order by average_pay_exp_premium_viewing_programmes_per_week
;
*/


