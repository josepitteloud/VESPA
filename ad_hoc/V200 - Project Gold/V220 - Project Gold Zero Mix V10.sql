/*------------------------------------------------------------------------------------------------------------------
        Project:    V200-Project Gold
        Program:    Zero Mix Analysis
        Version:    1
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
from 'G:\RTCI\Lookup Tables\Project Gold Channel Name Lookup.csv' format ascii;

commit;
--select * from v200_channel_lookup_with_channel_family order by channel_name;
grant all on dbarnett.v200_channel_lookup_with_channel_family to public;

---Create Bespoke category for Sports/Movies seperated out for Zero Mix Project

alter table v200_channel_lookup_with_channel_family add channel_category_inc_sports_movies varchar(23);

--For purposes of this analysis BT Sport grouped in with Free channels as not Pay via Sky

update v200_channel_lookup_with_channel_family
set channel_category_inc_sports_movies= case    when channel_name_inc_hd_staggercast_channel_families =  'Sky Sports Channels' then '01: Sky Sports'
                                                when channel_name_inc_hd_staggercast_channel_families =  'Sky Movies Channels' then '02: Sky Movies'
                                                when channel_name_inc_hd_staggercast_channel_families =  'BT Sport' then '04: FTA Channel'
                                                when pay_channel=1 then '03: Pay Channel' else '04: FTA Channel' end 

from v200_channel_lookup_with_channel_family as a
;
commit;


CREATE HG INDEX idx1 ON v200_channel_lookup_with_channel_family (channel_name);
grant all on  v200_channel_lookup_with_channel_family to public;
commit;

--select top 100 programme_trans_sk from sk_prod.vespa_dp_prog_viewed_201302;

----PART B -

--Viewing from Feb-Jul 2013
--Viewing Of Pay Sports/Movies/Other Pay/Non Pay (Initial Breakdown) - May return to look at specific channels--
--drop table v200_zero_mix_viewing_201302;
---Feb 2013
select 
a.account_number
,case when channel_name='Other TV' and genre_description='Sports' then 'BT Sport'
when channel_name='Other TV' and genre_description='Movies' then 'Sky Movies Disney' else channel_name end as channel_name_updated

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

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201302 (channel_name_updated);
CREATE HG INDEX idx3 ON v200_zero_mix_viewing_201302 (broadcast_start_date_time_utc );

--Create Summary by Programme---
--drop table #summary_by_prog_201302;
select account_number
,channel_name_updated
,broadcast_start_date_time_utc
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201302
from v200_zero_mix_viewing_201302
group by account_number
,channel_name_updated
,broadcast_start_date_time_utc
;

commit;
--select count(*) from v200_zero_mix_viewing_201302;
CREATE HG INDEX idx1 ON #summary_by_prog_201302 (account_number);
CREATE HG INDEX idx2 ON #summary_by_prog_201302 (channel_name_updated);

--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
drop table v200_zero_mix_viewing_201302_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies' then seconds_viewed else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel' then  seconds_viewed else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel' then  seconds_viewed
 when channel_category_inc_sports_movies is null then  seconds_viewed else 0 end) as seconds_viewed_FTA

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then seconds_viewed else 0 end) as seconds_viewed_engaged_Other_Pay

,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and 
               round(((cast(seconds_viewed as real)/cast(prog_duration as real))*100),0)>=60
              then 1 else 0 end) as programmes_viewed_engaged_Other_Pay

into v200_zero_mix_viewing_201302_summary_by_account
from #summary_by_prog_201302 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name_updated = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201302_summary_by_account (account_number);
--select top 500 * from v200_zero_mix_viewing_201302_summary_by_account;
commit;
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
--select top 100 * from v200_zero_mix_viewing_201303;
--drop table v200_zero_mix_viewing_201303;
select 
a.account_number
,case when channel_name='Other TV' and genre_description='Sports' then 'BT Sport'
when channel_name='Other TV' and genre_description='Movies' then 'Sky Movies Disney' else channel_name end as channel_name_updated
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

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201303 (channel_name_updated);


--Create Summary by Programme---
select account_number
,channel_name_updated
,broadcast_start_date_time_utc
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201303
from v200_zero_mix_viewing_201303
group by account_number
,channel_name_updated
,broadcast_start_date_time_utc
;

--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;
---TOHERE
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
--select top 100 * from v200_zero_mix_viewing_201304_summary_by_account;
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
--drop table v200_zero_mix_viewing_201307;
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


----PART B -- create Account Variables for All Active Accounts

create table v220_zero_mix_active_uk_accounts (account_number varchar(20));
commit;
insert into v220_zero_mix_active_uk_accounts
select  distinct account_number
--into 
from sk_prod.cust_single_account_view
where acct_type='Standard' and account_number <>'?' and pty_country_code ='GBR'
and acct_status_code in ('AC','PC','AB')
;
commit;
alter table v220_zero_mix_active_uk_accounts add(
                                                       
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
                            ,household_ownership_type varchar(20)
                ,affluence_septile   varchar(20)
                
                ,days_with_sport integer
                ,days_without_sport integer
                ,sports_downgrades integer
,sports_upgrades integer
,all_upgrades integer
,all_downgrades integer
,date_of_last_upgrade datetime
,date_of_last_downgrade datetime
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
commit;
CREATE HG INDEX idx1 ON #tenure (account_number);

Update          v220_zero_mix_active_uk_accounts
set             a.tenure = case when b.tenure is null then 'UNK*' else b.tenure end
from            v220_zero_mix_active_uk_accounts as a
left join       #tenure as b 
on              a.account_number = b.account_number;
commit;
;

--ISBA Region
update          v220_zero_mix_active_uk_accounts
set             a.isba_tv_region= case when b.isba_tv_region is null then 'UNK*' else b.isba_tv_region end
                ,a.cb_key_household=b.cb_key_household
from            v220_zero_mix_active_uk_accounts as a
left join       sk_prod.cust_single_account_view as b
on              a.account_number=b.account_number
;

create hg index idx1 on v220_zero_mix_active_uk_accounts(cb_key_household);
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

update          v220_zero_mix_active_uk_accounts
set             hh_composition= b.hh_composition
                ,hh_affluence=b.hh_affluence
                ,head_hh_age=b.head_hh_age
                ,num_children_in_hh=b.num_children_in_hh
                ,num_bedrooms=b.num_bedrooms
                ,residence_type=b.residence_type
                ,household_ownership_type=b.household_ownership_type
                ,affluence_septile=b.affluence_septile
from            v220_zero_mix_active_uk_accounts as a
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
Update          v220_zero_mix_active_uk_accounts
set             a.sky_go_reg = case when b.sky_go_reg=1 then 1 else 0 end
from            v220_zero_mix_active_uk_accounts as a
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
Update          v220_zero_mix_active_uk_accounts
set             a.bb_type = case when b.bb_type is null then '6) NA' else b.bb_type end
from            v220_zero_mix_active_uk_accounts as a
left join       #bbb as b 
on              a.account_number = b.account_number;
commit;
;
--select bb_type  , count(*) from v220_zero_mix_active_uk_accounts group by bb_type 
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
exec sp_create_tmp_table_idx '#box_prods', 'account_number';
commit;

Update          v220_zero_mix_active_uk_accounts
set             a.multiroom = case when b.multiroom =1 then b.multiroom else 0 end
                ,a.hdtv = case when b.hdtv =1 then b.hdtv else 0 end
                ,a.skyplus = case when b.skyplus =1 then b.skyplus else 0 end
                ,a.subscription_3d = case when b.subscription_3d =1 then b.subscription_3d else 0 end
from            v220_zero_mix_active_uk_accounts as a
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

Update          v220_zero_mix_active_uk_accounts
set             a.value_segment = case when b.value_segment is null then 'UNKNOWN' else b.value_segment end
from            v220_zero_mix_active_uk_accounts as a
left join       #vs as b 
on              a.account_number = b.account_number;
commit;

select csh.account_number
,effective_from_dt
,effective_to_dt
,cel.prem_sports as sports_premiums_new
,cel_old.prem_sports as sports_premiums_old
,ent_cat_prod_changed
,status_code
,cel.contribution_gbp as new_contribution
,cel_old.contribution_gbp as old_contribution
,current_short_description
,previous_short_description
into            #all_accounts_with_sports_prem_details_added
FROM            sk_prod.cust_subs_hist csh
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel_old on csh.previous_short_description = cel_old.short_description
WHERE          csh.subscription_sub_type ='DTV Primary Viewing'
                and csh.subscription_type = 'DTV PACKAGE'
                and status_code in ('AC','AB','PC')
--and ent_cat_prod_changed='Y'
;
--select * from sk_prod.cust_entitlement_lookup ;
commit;
create  hg index idx1 on #all_accounts_with_sports_prem_details_added (account_number);
--select top 100 * from #all_accounts_with_sports_prem_details_added;
commit;
--drop table #account_summary;
select account_number
,sum(case when sports_premiums_new >0  and effective_to_dt = '9999-09-09' then datediff(day,effective_from_dt,cast('2013-08-29' as date))
            when sports_premiums_new >0  then datediff(day,effective_from_dt,effective_to_dt)
 else 0 end) as days_with_sport
,sum(case when sports_premiums_new >0 then 0  
            when effective_to_dt = '9999-09-09'  then datediff(day,effective_from_dt,cast('2013-08-29' as date))            
else datediff(day,effective_from_dt,effective_to_dt) end) as days_without_sport
,sum( case when ent_cat_prod_changed='Y' and previous_short_description is not null and sports_premiums_new<sports_premiums_old then 1 else 0 end) as sports_downgrades
,sum( case when ent_cat_prod_changed='Y' and previous_short_description is not null and sports_premiums_new>sports_premiums_old then 1 else 0 end) as sports_upgrades

---Add on Previous Upgrades/downgrades overall

,sum(case when ent_cat_prod_changed='Y' and previous_short_description is not null and new_contribution>old_contribution then 1 else 0 end) as all_upgrades
,sum(case when ent_cat_prod_changed='Y' and previous_short_description is not null and new_contribution<old_contribution then 1 else 0 end) as all_downgrades
,max(case when ent_cat_prod_changed='Y' and previous_short_description is not null and new_contribution>old_contribution then effective_from_dt else null end) as date_of_last_upgrade
,max(case when ent_cat_prod_changed='Y' and previous_short_description is not null and new_contribution<old_contribution then effective_from_dt else null end) as date_of_last_downgrade

into #account_summary
from #all_accounts_with_sports_prem_details_added
group by account_number
;
--select * from #account_summary

commit;
create hg index idx1 on #account_summary(account_number);
commit;

Update          v220_zero_mix_active_uk_accounts
set             a.days_with_sport = b.days_with_sport
                ,days_without_sport =b.days_without_sport
                ,sports_downgrades =b.sports_downgrades
                ,sports_upgrades =b.sports_upgrades
                ,all_upgrades =b.all_upgrades
                ,all_downgrades =b.all_downgrades
                ,date_of_last_upgrade =b.date_of_last_upgrade
                ,date_of_last_downgrade =b.date_of_last_downgrade
from            v220_zero_mix_active_uk_accounts as a
left join       #account_summary as b 
on              a.account_number = b.account_number;
commit;

--select all_upgrades , count(*),sum(vespa_zero_mix_panel_account) from v220_zero_mix_active_uk_accounts group by all_upgrades order by all_upgrades
--select top 100 * from v220_zero_mix_active_uk_accounts
----


SELECT  distinct account_number
        ,cb_address_postcode
      ,rank() over(PARTITION BY SAV.account_number ORDER BY SAV.cb_address_postcode desc) AS rank_id
INTO postcode
  FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
where acct_type='Standard' and account_number <>'?' and pty_country_code ='GBR'
and acct_status_code in ('AC','PC','AB')
commit;

DELETE FROM postcode where rank_id > 1;
commit;

--      create index on BB
CREATE   HG INDEX idx10 ON postcode(account_number);
commit;

--cable area
SELECT account_number
      ,CASE  WHEN cable_postcode ='N' THEN 'N'
             WHEN cable_postcode ='n' THEN 'N'
             WHEN cable_postcode ='Y' THEN 'Y'
             WHEN cable_postcode ='y' THEN 'Y'
                                      ELSE 'N/A'
       END AS Cable_area
into #cable_area
  FROM postcode as ads
       LEFT OUTER JOIN sk_prod.broadband_postcode_exchange  AS bb
       ON replace(ads.cb_address_postcode, ' ','') = replace(bb.cb_address_postcode,' ','')
;
commit;

commit;
CREATE HG INDEX idx1 ON #cable_area (account_number);



-- delete temp file
drop table postcode; commit;

select a.account_number
,sum(a.total_paid_amt*-1) as last_12m_bill_paid
into #last_12M_paid_amt 
from sk_prod.cust_bills  as a
where payment_due_dt between '2012-09-01' and '2013-08-31'
group by a.account_number
;

commit;
CREATE HG INDEX idx1 ON #last_12M_paid_amt  (account_number);

alter table v220_zero_mix_active_uk_accounts add(                         
                            Cable_area                     varchar(4)
                           , last_12m_bill_paid decimal(10,2)
);

UPDATE v220_zero_mix_active_uk_accounts
SET  Cable_area = b.Cable_area
FROM v220_zero_mix_active_uk_accounts  AS a
  INNER JOIN #cable_area AS b
        ON a.account_number = b.account_number;
commit;

UPDATE v220_zero_mix_active_uk_accounts
SET  last_12m_bill_paid = b.last_12m_bill_paid
FROM v220_zero_mix_active_uk_accounts  AS a
  INNER JOIN #last_12M_paid_amt  AS b
        ON a.account_number = b.account_number;
commit;

alter table v220_zero_mix_active_uk_accounts add vespa_zero_mix_panel_account integer default 0;

UPDATE v220_zero_mix_active_uk_accounts
SET  vespa_zero_mix_panel_account = case when b.distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC') then 1 else 0 end
FROM v220_zero_mix_active_uk_accounts  AS a
left outer join v200_zero_mix_full_account_list as b
on a.account_number=b.account_number
commit;

---Sky Talk Status

alter table v220_zero_mix_active_uk_accounts add talk_product varchar(20);

SELECT DISTINCT account_number
       ,min(CASE WHEN UCASE(current_product_description) LIKE '%UNLIMITED%'
             THEN 'Unlimited'
             ELSE 'Freetime'
          END) as talk_product
         INTO talk
FROM sk_prod.cust_subs_hist AS CSH
WHERE subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          OR (status_code = 'FBP' AND prev_status_code IN ('PC','A'))
          OR (status_code = 'RI'  AND prev_status_code IN ('FBP','A'))
          OR (status_code = 'PC'  AND prev_status_code = 'A'))
     AND effective_to_dt != effective_from_dt
     AND csh.effective_from_dt <= '2013-09-12'
     AND csh.effective_to_dt > '2013-09-12'
GROUP BY account_number;
commit;



--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number);
commit;

UPDATE v220_zero_mix_active_uk_accounts
SET  talk_product = talk.talk_product
FROM v220_zero_mix_active_uk_accounts  AS Base
  INNER JOIN talk AS talk
        ON base.account_number = talk.account_number
ORDER BY base.account_number;
commit;

DROP TABLE talk;
commit;


alter table v220_zero_mix_active_uk_accounts add (
current_status_code varchar(2)
,number_of_sports_premiums integer default 0
,number_of_movies_premiums integer default 0
,mix_type varchar(40)
,entertainment_extra_flag tinyint default 0
);

update v220_zero_mix_active_uk_accounts
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
from v220_zero_mix_active_uk_accounts as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
left outer join sk_prod.cust_entitlement_lookup as cel
on b.PROD_LATEST_ENTITLEMENT_CODE = cel.short_description
where b.account_number is not null
;

update v220_zero_mix_active_uk_accounts
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
from  v220_zero_mix_active_uk_accounts;
commit;


-----


-- Capture all active boxes for this week
SELECT    csh.service_instance_id
          ,csh.account_number
          ,subscription_sub_type
          ,rank() over (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id desc) AS rank
  INTO accounts -- drop table accounts
  FROM sk_prod.cust_subs_hist as csh
      
 WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
   AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
   AND csh.effective_from_dt <= '2013-09-12'
   AND csh.effective_to_dt > '2013-09-12'
   AND csh.effective_from_dt <> effective_to_dt;
commit;

-- De-dupe active boxes
DELETE FROM accounts WHERE rank>1;
commit;

CREATE HG INDEX idx14 ON accounts(service_instance_id);
commit;

-- Identify HD boxes
SELECT  stb.service_instance_id
       ,SUM(CASE WHEN current_product_description LIKE '%HD%'     THEN 1  ELSE 0 END) AS HD
       ,SUM(CASE WHEN current_product_description LIKE '%HD%1TB%'
                   or current_product_description LIKE '%HD%2TB%' THEN 1  ELSE 0 END) AS HD1TB -- combine 1 and 2 TB
INTO hda -- drop table hda
FROM sk_prod.CUST_SET_TOP_BOX AS stb
        INNER JOIN accounts AS acc
        ON stb.service_instance_id = acc.service_instance_id
WHERE box_installed_dt <= '2013-09-12'
        AND box_replaced_dt   >'2013-09-12'
        AND current_product_description like '%HD%'
GROUP BY stb.service_instance_id;
commit;

CREATE HG INDEX idx14 ON hda(service_instance_id);
commit;

--select top 100 * from hda;


drop table scaling_box_level_viewing;
commit;

SELECT  --acc.service_instance_id,
       acc.account_number
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
       ,MAX(CASE  WHEN hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
       ,MAX(CASE  WHEN hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
INTO scaling_box_level_viewing
FROM sk_prod.cust_subs_hist AS csh
        INNER JOIN accounts AS acc
        ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
                LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                ON csh.current_short_description = cel.short_description
                        LEFT OUTER JOIN hda
                        ON csh.service_instance_id = hda.service_instance_id --< Links to the HD Set Top Boxes
 WHERE csh.effective_FROM_dt <= '2013-09-12'
   AND csh.effective_to_dt    > '2013-09-12'
   AND csh.status_code IN  ('AC','AB','PC')
   AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
   AND csh.effective_FROM_dt <> csh.effective_to_dt
GROUP BY acc.service_instance_id ,acc.account_number;
commit;

--select top 100 * from accounts;
--select top 100 * from scaling_box_level_viewing;

drop table accounts; commit;
drop table hda; commit;



-- Identify boxtype of each box and whether it is a primary or a secondary box
SELECT  tgt.account_number
       ,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
       ,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS pb -- FDB
       ,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS sb -- FDB
        ,convert(varchar(20), null) as universe
        ,convert(varchar(30), null) as boxtype
  INTO boxtype_ac -- drop table boxtype_ac
  FROM scaling_box_level_viewing AS tgt
GROUP BY tgt.account_number;
commit;

--select top 100 * from boxtype_ac;


-- Build the combined flags
update boxtype_ac
set universe = CASE WHEN mr_boxes = 0 THEN 'Single box HH'
                         WHEN mr_boxes = 1 THEN 'Dual box HH'
                         ELSE 'Multiple box HH' END
    ,boxtype  =
        CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'HDx & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'HD & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'Skyplus & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'FDB & No_secondary_box'
             WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'HD & HD'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'HD & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'HD & FDB'
             WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'HDx & HDx'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'HDx & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'HDx & FDB'
             WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'Skyplus & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'Skyplus & FDB'
                        ELSE   'FDB & FDB' END
;
commit;

--select top 100 * from boxtype_ac;
--select top 100 * from AdSmart;

alter table v220_zero_mix_active_uk_accounts add box_type varchar(30);
--update AdSmart file
UPDATE v220_zero_mix_active_uk_accounts
SET  base.Box_type = bt.Boxtype
FROM v220_zero_mix_active_uk_accounts AS Base INNER JOIN boxtype_ac AS bt
ON base.account_number = bt.account_number;
commit;

-- delete temp file
drop table boxtype_ac;
commit;


---Create Weights---
---Add in Account Variables




alter table v220_zero_mix_active_uk_accounts  add product_holding_group varchar(50);
alter table v220_zero_mix_active_uk_accounts  add tv_package_group varchar(50);
alter table v220_zero_mix_active_uk_accounts  add box_type_group varchar(50);

alter table v220_zero_mix_active_uk_accounts  add value_segment_group varchar(50);
alter table v220_zero_mix_active_uk_accounts  add affluence_group varchar(50);

alter table v220_zero_mix_active_uk_accounts  add tenure_group varchar(50);
commit;

--select distinct talk_product from v220_zero_mix_active_uk_accounts
--select tenure , count(*),sum(vespa_zero_mix_panel_account) from v220_zero_mix_active_uk_accounts group by tenure order by tenure;
--select box_type , count(*),sum(vespa_zero_mix_panel_account) from v220_zero_mix_active_uk_accounts group by box_type order by box_type;

update v220_zero_mix_active_uk_accounts
set product_holding_group=case when bb_type  in ('NA','6) NA') and talk_product is null then 'a) TV Only' else 'b) TV with BB and or Talk' end
, tv_package_group=case when number_of_sports_premiums=2 and number_of_movies_premiums=2 then 'a) Top Tier' 
                        when number_of_sports_premiums=2 and number_of_movies_premiums=0 then 'b) Dual Sports' 
                        when number_of_sports_premiums=0 and number_of_movies_premiums=2 then 'c) Dual Movies' 
                        when number_of_sports_premiums+ number_of_movies_premiums>0 then 'd) Other Premiums' else 'e) Other' end

,box_type_group=case          when box_type in ('FDB & FDB','HD & FDB','HD & HD','HD & Skyplus','HDx & FDB','HDx & HDx','HDx & Skyplus','Skyplus & FDB','Skyplus & Skyplus') then 'a) Multiple Boxes'  
                        when box_type in ('HD & No_secondary_box','HDx & No_secondary_box','Skyplus & No_secondary_box') then 'b) Single Non-FDB Box' 
                        when box_type in ('FDB & No_secondary_box') then 'c) FDB Box' else 'b) Single Non-FDB Box'  end ---Set non-Matches to Most popular group

,value_segment_group= case when value_segment = 'UNSTABLE' then 'UNSTABLE' else 'STABLE' end
,affluence_group=case when affluence_septile in ('4','5','6') then '01) High Affluence' else '02) Non-High Affluence' end 
,tenure_group = case when tenure in ('A) 0-12 Months','B) 1-2 Years','C) 2-3 Years') then 'A-C) 0-36 Months' else tenure end
from v220_zero_mix_active_uk_accounts as a
;
--drop table #uk_and_vespa_accounts;
select product_holding_group
, tv_package_group
,box_type_group
,value_segment_group
,tenure_group
,affluence_group
, count(*) as accounts 
, sum(vespa_zero_mix_panel_account) as vespa_accounts
into #uk_and_vespa_accounts
from v220_zero_mix_active_uk_accounts
--where full_months_tenure>=15
group by product_holding_group
, tv_package_group
,box_type_group
,value_segment_group
,tenure_group
,affluence_group
order by vespa_accounts
;

---Add Weight back on---
--alter table v220_zero_mix_active_uk_accounts  delete weight_value;
alter table v220_zero_mix_active_uk_accounts  add weight_value real;
commit;

commit;
update v220_zero_mix_active_uk_accounts
set weight_value = b.accounts/b.vespa_accounts
from v220_zero_mix_active_uk_accounts as a
left outer join #uk_and_vespa_accounts as b
on  a.product_holding_group=b.product_holding_group
and a.tv_package_group=b.tv_package_group
and a.box_type_group=b.box_type_group
and a.value_segment_group=b.value_segment_group
and a.tenure_group=b.tenure_group
and a.affluence_group=b.affluence_group 
where vespa_zero_mix_panel_account=1 and b.vespa_accounts>0
;
commit;


--select tenure , count(*) , sum(weight_value) from v220_zero_mix_active_uk_accounts group by tenure order by tenure;

---Add weight back on to Vespa Database--
--alter table v200_zero_mix_full_account_list  delete weight_value ;
alter table v200_zero_mix_full_account_list  add weight_value real;
commit;
update v200_zero_mix_full_account_list
set weight_value = b.weight_value
from v200_zero_mix_full_account_list as a
left outer join v220_zero_mix_active_uk_accounts as b
on  a.account_number=b.account_number
;
commit;

alter table v200_zero_mix_full_account_list add vespa_zero_mix_panel_account integer default 0;

UPDATE v200_zero_mix_full_account_list
SET  vespa_zero_mix_panel_account = case when b.distinct_viewing_days_201302>=15 and distinct_viewing_days_201303>=15 and distinct_viewing_days_201304
                                               >=15 and distinct_viewing_days_201305>=15 and distinct_viewing_days_201306>=15 and distinct_viewing_days_201307>=15 and
 current_status_code in ('AC','AB','PC') then 1 else 0 end
FROM v200_zero_mix_full_account_list as b
commit;


grant all on v200_zero_mix_full_account_list to public;commit;
grant all on v220_zero_mix_active_uk_accounts to public;commit;
--Analysis of 
--select sum(weight_value) from v200_zero_mix_full_account_list;

---Create Non_premium Pay Bandings---

--V1 #Engaged Progs per week---
--select * from  v200_zero_mix_full_account_list;
/*
alter table v200_zero_mix_full_account_list delete non_premium_pay_engagement;
alter table v200_zero_mix_full_account_list delete non_premium_pay_duration;
alter table v200_zero_mix_full_account_list delete sports_premium_duration;
alter table v200_zero_mix_full_account_list delete movies_premium_duration;
alter table v200_zero_mix_full_account_list delete FTA_duration;
alter table v200_zero_mix_full_account_list delete average_total_tv_viewing_durationm_pay_duration;
commit;

*/

alter table v200_zero_mix_full_account_list add non_premium_pay_engagement varchar(30);
alter table v200_zero_mix_full_account_list add non_premium_pay_duration varchar(30);
update v200_zero_mix_full_account_list
set non_premium_pay_engagement = 
case when 
 abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=3 then 'a) <=3 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=10 then 'b) 4-10 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=20 then 'c) 11-20 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))>20 then 'd) 20+ per week'
else 'e) Unknown' end
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
;
commit;

update v200_zero_mix_full_account_list
set  non_premium_pay_duration=case when seconds_viewed_other_pay_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real)) >=180 then 'a) 180+ minutes per day'
when seconds_viewed_other_pay_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real)) >=90 then 'b) 90-179 minutes per day'
when seconds_viewed_other_pay_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real)) >=30 then 'c) 30-89 minutes per day'
else 'd) Under 30 Minutes per day' end
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
;
commit;

---Create an expanded version of non-premium pay engaged--
alter table v200_zero_mix_full_account_list add non_premium_pay_engagement_expanded varchar(30);
update v200_zero_mix_full_account_list
set non_premium_pay_engagement_expanded = 
case 
when programmes_viewed_engaged_Other_Pay_201302_to_201307=0 then 'a) No Viewing'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))=0 then 'b) <1 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))=1 then 'c) >=1 and <2 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))=2 then 'd) >=2 and <3 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=3 then 'e) >=3 and <4 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=10 then 'f) 4-10 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=20 then 'g) 11-20 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))>20 then 'h) 20+ per week'
else 'h) Unknown' end
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
;
commit;

--select non_premium_pay_engagement_expanded , count(*) from v200_zero_mix_full_account_list group by non_premium_pay_engagement_expanded order by non_premium_pay_engagement_expanded
--select abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0)) as values1 , count(*) from v200_zero_mix_full_account_list where vespa_zero_mix_panel_account=1   group by values1 order by values1


alter table v200_zero_mix_full_account_list add sports_premium_duration varchar(30);

update v200_zero_mix_full_account_list
set sports_premium_duration = 
case when round((seconds_viewed_Sky_sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)=0 then 'a) No sports Viewed'
when round((seconds_viewed_Sky_sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'b) Under 60 min per Week'
when round((seconds_viewed_Sky_sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<120 then 'c) 60-119 min per Week'
when round((seconds_viewed_Sky_sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<240 then 'd) 120-239 min per Week'
else 'e) 4+ Hours per week' end 
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
;

--Create an Expanded version of non_premium_pay_engagement--




--Add on the duration splits for Movies/FTA and Total Viewing--
alter table v200_zero_mix_full_account_list add movies_premium_duration varchar(30);
update v200_zero_mix_full_account_list
set movies_premium_duration = 
case when round((seconds_viewed_Sky_movies_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)=0 then 'a) No movies Viewed'
when round((seconds_viewed_Sky_movies_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'b) Under 60 min per Week'
when round((seconds_viewed_Sky_movies_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<120 then 'c) 60-119 min per Week'
when round((seconds_viewed_Sky_movies_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<240 then 'd) 120-239 min per Week'
else 'e) 4+ Hours per week' end 
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
;

alter table v200_zero_mix_full_account_list add FTA_duration varchar(30);
update v200_zero_mix_full_account_list
set FTA_duration = 
case when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'a) Under 60 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<120 then 'b) 60-119 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<180 then 'c) 120-179 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<240 then 'd) 180-239 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<300 then 'e) 240-299 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<360 then 'f) 300-359 min per Day'
else 'g) 6+ Hours per Day' end 
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
;

alter table v200_zero_mix_full_account_list add average_total_tv_viewing_duration varchar(30);
update v200_zero_mix_full_account_list
set average_total_tv_viewing_duration = 
case when round((
(seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)
/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<180 then 'a) Under 180 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<240 then 'b) 180-239 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<300 then 'c) 240-299 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<360 then 'd) 300-359 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<420 then 'e) 360-419 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<480 then 'f) 420-479 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<540 then 'g) 480-539 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<600 then 'h) 540-599 min per Day'
else 'i) 10+ Hours per Day' end 
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
;
commit;

---Add Subsequent Churn/Active Block/Pending Cancel/TA events---
select account_number
,max(case when status_code ='PO' then 1 else 0 end) as aug_sep_cuscan_event
,max(case when status_code ='SC' then 1 else 0 end) as aug_sep_syscan_event
,max(case when status_code ='PC' then 1 else 0 end) as aug_sep_pending_cancel_event
,max(case when status_code ='AB' then 1 else 0 end) as aug_sep_active_block_event
into #churn_and_pending_churn_events
from sk_prod.cust_subs_hist
where SUBSCRIPTION_TYPE = 'DTV PACKAGE' and SUBSCRIPTION_SUB_TYPE ='DTV Primary Viewing'
 and effective_from_dt between '2013-08-01' and '2013-09-30'
group by account_number
;

commit;
CREATE HG INDEX idx1 ON #churn_and_pending_churn_events (account_number);

alter table v200_zero_mix_full_account_list add aug_sep_cuscan_event tinyint default 0;
alter table v200_zero_mix_full_account_list add aug_sep_syscan_event tinyint default 0;
alter table v200_zero_mix_full_account_list add aug_sep_pending_cancel_event tinyint default 0;
alter table v200_zero_mix_full_account_list add aug_sep_active_block_event tinyint default 0;

update v200_zero_mix_full_account_list
set aug_sep_cuscan_event=b.aug_sep_cuscan_event
,aug_sep_syscan_event=b.aug_sep_syscan_event
,aug_sep_pending_cancel_event=b.aug_sep_pending_cancel_event
,aug_sep_active_block_event=b.aug_sep_active_block_event
from v200_zero_mix_full_account_list as a
left outer join #churn_and_pending_churn_events as b
on a.account_number =b.account_number
;
commit;

SELECT      cca.account_number
            ,1 as ta_event        
INTO        #ta_events
FROM        sk_prod.cust_change_attempt AS cca
inner join  sk_prod.cust_subscriptions AS subs
ON          cca.subscription_id = subs.subscription_id
WHERE       cca.change_attempt_type                  = 'CANCELLATION ATTEMPT'
AND         subs.ph_subs_subscription_sub_type       = 'DTV Primary Viewing'
AND         cca.attempt_date                           >= '2013-08-01' 
AND         cca.attempt_date                           < '2013-10-01'    
AND         cca.created_by_id  NOT IN ('dpsbtprd', 'batchuser')
AND         cca.Wh_Attempt_Outcome_Description_1 in 
            ('Turnaround Saved','Legacy Save','Home Move Saved','Home Move Accept Saved'
            ,'Turnaround Not Saved','Legacy Fail','Home Move Not Saved')
group by    cca.account_number,ta_event
order by    cca.account_number,ta_event
;
commit;
CREATE HG INDEX idx1 ON #ta_events (account_number);

alter table v200_zero_mix_full_account_list add aug_sep_turnaround_event tinyint default 0;

update v200_zero_mix_full_account_list
set aug_sep_turnaround_event=b.ta_event
from v200_zero_mix_full_account_list as a
left outer join #ta_events as b
on a.account_number =b.account_number
;
commit;




--select top 100 * from v220_zero_mix_active_uk_accounts;
--drop table dbarnett.v220_pivot_activity_data;
select non_premium_pay_engagement
,non_premium_pay_duration
,non_premium_pay_engagement_expanded
,movies_premium_duration
,FTA_duration
,average_total_tv_viewing_duration
,case when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)=0 then 'a) No Sports Viewed'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'b) Under 60 min per Week'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<180 then 'c) 60-179 min per Week'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<360 then 'd) 180-359 min per Week'
else 'e) 6+ Hours per week' end 

 as minutes_sports_per_week
,case when tv_package_group<>'e) Other' then tv_package_group when 

 a.entertainment_extra_flag=1 then 'e) Entertainment Extra with No Premiums' 
when a.entertainment_extra_flag=0
then 'f) Entertainment with No Premiums' else 'g) Other' end as package_type
,b.tenure
,b.isba_tv_region
,CASE b.hh_composition      when   '00' then 	'a) Family'
when '01'	then 'a) Family'
when '02'	then 'a) Family'
when '03'	then 'a) Family'
when '04'	then 'b) Single'
when '05'	then 'b) Single'
when '06'	then 'c) Homesharer'
when '07'	then 'c) Homesharer'
when '08'	then 'c) Homesharer'
when '09'	then 'a) Family'
when '10'	then 'a) Family'
when '11'	then 'c) Homesharer'
when 'U' 	then 'd) Unclassified'
else 'd) Unclassified' end as household_composition
,case when date_of_last_downgrade>='2013-03-12' then 1 else 0 end as downgrade_in_last_06M      
,case when all_downgrades>=5 then 'a) 5+ downgrades ever'
      when all_downgrades>=2 then 'b) 2-4 downgrades ever'
      when all_downgrades>0 then 'c) 1 downgrade ever' else 'd) Never Downgraded' end as downgrade_ever

,case when sports_downgrades>=5 then 'a) 5+ sports downgrades ever'
      when sports_downgrades>=2 then 'b) 2-4 sports downgrades ever'
      when sports_downgrades>0 then 'c) 1 sports downgrades ever' else 'd) Never Downgraded Sports' end as downgrade_ever_sports_channels

,case when all_upgrades>=5 then 'a) 5+ upgrades ever'
      when all_upgrades>=2 then 'b) 2-4 upgrades ever'
      when all_upgrades>0 then 'c) 1 upgrade ever' else 'd) Never upgraded' end as upgrade_ever

,case when sports_upgrades>=5 then 'a) 5+ sports upgrades ever'
      when sports_upgrades>=2 then 'b) 2-4 sports upgrades ever'
      when sports_upgrades>0 then 'c) 1 sports upgrade ever' else 'd) Never upgraded Sports' end as upgrade_ever_sports_channels
,case when b.cable_area='Y' then 1 else 0 end as cable_area_hh
,b.value_segment
,case when b.affluence_septile is null then 'U' 
        when b.affluence_septile = '0' then '0: Lowest Affluence' 
        when b.affluence_septile = '6' then '6: Highest Affluence' else b.affluence_septile end as affluence_septile_type
,b.box_type_group

,case when c.bb_type in ('1) Fibre','2) Unlimited','3) Everyday','4) Everyday Lite','5) Connect') then 1 else 0 end as has_bb
,case when c.bb_type in ('1) Fibre') then 1 else 0 end as has_bb_fibre
,case when talk_product is not null then 1 else 0 end as has_talk
,case when has_bb=1 and has_talk =1 then 'a) TV, BB and Talk'
      when has_bb=1 and has_talk =0 then 'b) TV and BB'
      when has_bb=0 and has_talk =1 then 'c) TV and Talk' else 'd) TV Only' end as tv_bb_talk
,case   when last_12m_bill_paid<200 then 'a) Under £200'
        when last_12m_bill_paid<300 then 'b) £200-£299'
        when last_12m_bill_paid<400 then 'c) £300-£399'
        when last_12m_bill_paid<500 then 'd) £400-£499'
        when last_12m_bill_paid<600 then 'e) £500-£599'
        when last_12m_bill_paid<700 then 'f) £600-£699'
        when last_12m_bill_paid<800 then 'g) £700-£799' else 'h) £800+' end as last_12mths_bill_amt

--Add in subsequent Churn/TA events
,aug_sep_cuscan_event
,aug_sep_syscan_event
,aug_sep_pending_cancel_event
,aug_sep_active_block_event
,aug_sep_turnaround_event

--Add in Extra Profile Variables--
,CQM 
,case when adsmartable_hh =1 then 1 else 0 end as adsmartable_household
,social_grade
,case when social_grade in ('A','B','C1') then 1 else 0 end as social_grade_ABC1
,Mirror_Men
,Mirror_Women
,Mirror_has_children as Mirror_Children

,
case mosaic_group
when 'A' then 	'Alpha Territory'
when 'B' then 	'Professional Rewards'
when 'C' then 	'Rural Solitude'
when 'D' then 	'Small Town Diversity'
when 'E' then 	'Active Retirement'
when 'F' then 	'Suburban Mindsets'
when 'G' then 	'Careers and Kids'
when 'H' then 	'New Homemakers'
when 'I' then 	'Ex-Council Community'
when 'J' then 	'Claimant Cultures'
when 'K' then 	'Upper Floor Living'
when 'L' then 	'Elderly Needs'
when 'M' then 	'Industrial Heritage'
when 'N' then 	'Terraced Melting Pot'
when 'O' then 	'Liberal Opinions'
when 'U' then 	'Unclassified'
else null end as h_mosaic_uk_group

,case True_Touch_Type 
when 1 then 'A: Experienced Netizens'
when 2 then 'A: Experienced Netizens'
when 3 then 'A: Experienced Netizens'
when 4 then 'A: Experienced Netizens'
when 5 then 'B: Cyber Tourists'
when 6 then 'B: Cyber Tourists'
when 7 then 'B: Cyber Tourists'
when 8 then 'B: Cyber Tourists'
when 9 then 'C: Digital Culture'
when 10 then 'C: Digital Culture'
when 11 then 'C: Digital Culture'
when 12 then 'D: Modern Media Margins'
when 13 then 'D: Modern Media Margins'
when 14 then 'D: Modern Media Margins'
when 15 then 'D: Modern Media Margins'
when 16 then 'E: Traditional Approach'
when 17 then 'E: Traditional Approach'
when 18 then 'E: Traditional Approach'
when 19 then 'E: Traditional Approach'
when 20 then 'E: Traditional Approach'
when 21 then 	'F: New tech Novices'
when 22 then 'F: New tech Novices'
when 99 then 	'G: Unclassified'
else 'G: Unclassified' end as True_Touch_Group
               
                ,child_hh_00_to_04
                ,child_hh_05_to_11
                ,child_hh_12_to_17
,case financial_stress 
when '0' then '0: Very low'
when '1' then '1: Low'
when '2' then '2: Medium'
when '3' then '3: High'
when '4' then '4: Very high'
when 'U' then '5: Unclassified'
else '5: Unclassified' end as financial_stress_hh

,sum(a.weight_value) as weighted_accounts
,sum(a.weight_value*last_12m_bill_paid) as weighted_accounts_12m_revenue

--Add in Upgrade/Downgrade Total
,sum(all_upgrades+all_downgrades) as total_upgrades_and_downgrades


into dbarnett.v220_pivot_activity_data
from v200_zero_mix_full_account_list as a
left outer join  v220_zero_mix_active_uk_accounts as b
on a.account_number = b.account_number
left outer join  v223_single_profiling_view as c
on a.account_number = c.account_number

where a.vespa_zero_mix_panel_account=1 
group by non_premium_pay_engagement
,non_premium_pay_duration
,non_premium_pay_engagement_expanded
,movies_premium_duration
,FTA_duration
,average_total_tv_viewing_duration
,minutes_sports_per_week
,package_type
,b.tenure
,b.isba_tv_region
,household_composition
,downgrade_in_last_06M      
,downgrade_ever
,downgrade_ever_sports_channels
,upgrade_ever
,upgrade_ever_sports_channels
,cable_area_hh
,b.value_segment
,affluence_septile_type
,box_type_group
,has_bb
,has_bb_fibre
,has_talk
,tv_bb_talk
,last_12mths_bill_amt
,aug_sep_cuscan_event
,aug_sep_syscan_event
,aug_sep_pending_cancel_event
,aug_sep_active_block_event
,aug_sep_turnaround_event
,CQM 
,adsmartable_household
,social_grade
,social_grade_ABC1
,Mirror_Men
,Mirror_Women
,Mirror_Children

,h_mosaic_uk_group

,True_Touch_Group
,child_hh_00_to_04
,child_hh_05_to_11
,child_hh_12_to_17
,financial_stress_hh
;

commit;

--select upgrade_ever ,count(*),sum(weighted_accounts) from dbarnett.v220_pivot_activity_data group by upgrade_ever
--select average_total_tv_viewing_duration ,count(*),sum(weighted_accounts),sum(weighted_accounts_12m_revenue) from dbarnett.v220_pivot_activity_data group by average_total_tv_viewing_duration order by average_total_tv_viewing_duration


grant all on dbarnett.v220_pivot_activity_data to public;
commit;



--------Part C Ad Hoc Analysis
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
;

--Make tables Public;

grant all on dbarnett.v200_zero_mix_viewing_201302_summary_by_account to public;
grant all on dbarnett.v200_zero_mix_viewing_201303_summary_by_account to public;
grant all on dbarnett.v200_zero_mix_viewing_201304_summary_by_account to public;
grant all on dbarnett.v200_zero_mix_viewing_201305_summary_by_account to public;
grant all on dbarnett.v200_zero_mix_viewing_201306_summary_by_account to public;
grant all on dbarnett.v200_zero_mix_viewing_201307_summary_by_account to public;
commit;


grant all on dbarnett.v200_zero_mix_full_account_list to public;
grant all on dbarnett.v220_zero_mix_active_uk_accounts to public;
commit;


---

--select top 500 * from dbarnett.v220_pivot_activity_data;



/*
select round(last_12m_bill_paid,-1) as bill_value
,count(*) as accounts
from v220_zero_mix_active_uk_accounts
group by bill_value
order by bill_value


select all_upgrades
,count(*) as accounts
from v220_zero_mix_active_uk_accounts
group by all_upgrades
order by all_upgrades


select  case when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)=0 then 'a) No Sports Viewed'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'b) Under 60 min per Week'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<180 then 'c) 60-179 min per Week'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<360 then 'd) 180-359 min per Week'
else 'e) 6+ Hours per week' end 

 as minutes_sports
,count(*) as records
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
group by minutes_sports
order by minutes_sports
;

select  case when round((seconds_viewed_Sky_movies_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)=0 then 'a) No movies Viewed'
when round((seconds_viewed_Sky_movies_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'b) Under 60 min per Week'
when round((seconds_viewed_Sky_movies_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<120 then 'c) 60-119 min per Week'
when round((seconds_viewed_Sky_movies_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<240 then 'd) 120-239 min per Week'
else 'e) 4+ Hours per week' end 

 as minutes_movies
,count(*) as records
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
group by minutes_movies
order by minutes_movies
;

---Repeat for FTA---
select  case when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'a) Under 60 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<120 then 'b) 60-119 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<180 then 'c) 120-179 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<240 then 'd) 180-239 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<300 then 'e) 240-299 min per Day'
when round((seconds_viewed_FTA_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<360 then 'f) 300-359 min per Day'
else 'g) 6+ Hours per Day' end 

 as minutes_FTA
,count(*) as records
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
group by minutes_FTA
order by minutes_FTA
;

---Repeat for Total Viewing
select  case when round((
(seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)
/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<180 then 'a) Under 180 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<240 then 'b) 180-239 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<300 then 'c) 240-299 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<360 then 'd) 300-359 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<420 then 'e) 360-419 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<480 then 'f) 420-479 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<540 then 'g) 480-539 min per Day'
when round(((seconds_viewed_FTA_201302_to_201307+seconds_viewed_Sky_movies_201302_to_201307+seconds_viewed_Sky_sports_201302_to_201307+seconds_viewed_Other_Pay_201302_to_201307)/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<600 then 'h) 540-599 min per Day'
else 'i) 10+ Hours per Day' end 

 as minutes_FTA
,count(*) as records
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
group by minutes_FTA
order by minutes_FTA
;








select  case when round(((seconds_viewed_Sky_Sports_201302_to_201307+seconds_viewed_Sky_Movies_201302_to_201307)*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)=0 then 'a) No Sports Viewed'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<60 then 'b) Under 60 min per Week'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<180 then 'c) 60-179 min per Week'
when round((seconds_viewed_Sky_Sports_201302_to_201307*7/60/(cast(distinct_viewing_days_201302_to_201307 as real))),0)<360 then 'd) 180-359 min per Week'
else 'e) 6+ Hours per week' end 

 as minutes_sports
,count(*) as records
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
group by minutes_sports
order by minutes_sports
;





non_premium_pay_duration=case when seconds_viewed_Sky_Sports_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real)) >=180 then 'a) 180+ minutes per day'
when seconds_viewed_other_pay_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real)) >=90 then 'b) 90-179 minutes per day'
when seconds_viewed_other_pay_201302_to_201307/60/(cast(distinct_viewing_days_201302_to_201307 as real)) >=30 then 'c) 30-89 minutes per day'
else 'd) Under 30 Minutes per day' end
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 



*/











/*
 a.number_of_sports_premiums=2 and a.number_of_movies_premiums=2 then 'a) Top Tier'
when a.number_of_sports_premiums=2  then 'b) Dual Sports'
when a.number_of_sports_premiums=1  then 'c) Single Sports'
when a.number_of_sports_premiums=0 and  a.number_of_movies_premiums>0 then 'd) Movies with no Sports'
when

select  number_of_sports_premiums , number_of_movies_premiums
,count(*) from v220_zero_mix_active_uk_accounts
group by number_of_sports_premiums , number_of_movies_premiums
commit;
select PROD_LATEST_ENTITLEMENT_PREM_SPORTS
,PROD_LATEST_ENTITLEMENT_PREM_MOVIES
,count(*)
from sk_prod.cust_single_account_view
where acct_status_code in ('AC')
group by 
PROD_LATEST_ENTITLEMENT_PREM_SPORTS
,PROD_LATEST_ENTITLEMENT_PREM_MOVIES
order by 
PROD_LATEST_ENTITLEMENT_PREM_SPORTS
,PROD_LATEST_ENTITLEMENT_PREM_MOVIES

--select panel, count (distinct account_number) as accounts,sum(cust_active_dtv) from VESPA_ANALYSTS.VESPA_SINGLE_BOX_VIEW group by panel
--select top 100 * from  VESPA_ANALYSTS.VESPA_SINGLE_BOX_VIEW ;
select total_paid_amt
,payment_due_dt
from sk_prod.cust_bills  as a
where payment_due_dt between '2012-09-01' and '2013-08-31' 

;
grant all on dbarnett.v190_all_active_accounts to public;
commit;







/*
alter table v200_zero_mix_full_account_list add average_weekly_TV_duration real;
update v200_zero_mix_full_account_list
set average_weekly_TV_duration = 
case when 
 abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=3 then 'a) <=3 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=10 then 'b) 4-10 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))<=20 then 'c) 11-20 per week'
when abs(round((programmes_viewed_engaged_Other_Pay_201302_to_201307*7)/(cast(distinct_viewing_days_201302_to_201307 as real)),0))>20 then 'd) 20+ per week'
else 'e) Unknown' end
from v200_zero_mix_full_account_list
where vespa_zero_mix_panel_account=1 
;
*/
/
