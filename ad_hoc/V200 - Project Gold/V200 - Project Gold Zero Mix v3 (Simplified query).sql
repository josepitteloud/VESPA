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
from 'C:\Users\barnetd\Documents\Project 200 - Project Gold\Project Gold Channel Name Lookup.csv' format ascii;


--select * from v200_channel_lookup_with_channel_family;


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


----PART B -

--Viewing from Feb-Jul 2013
--Viewing Of Pay Sports/Movies/Other Pay/Non Pay (Initial Breakdown) - May return to look at specific channels--
--drop table v200_zero_mix_viewing_201302;
---Feb 2013
select 
a.account_number
,subscriber_id
,duration
,time_in_seconds_since_recording
,programme_instance_name
,channel_name
,panel_id
,live_recorded
,playback_speed
,reported_playback_speed
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,dateadd(second,time_in_seconds_since_recording*-1,instance_start_date_time_utc) as viewing_broadcast_start_time
,dateadd(second,(datediff(second,viewing_stops,viewing_starts)),instance_start_date_time_utc) as viewing_broadcast_end_time
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


--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201302_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_FTA

into v200_zero_mix_viewing_201302_summary_by_account
from v200_zero_mix_viewing_201302 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201302_summary_by_account (account_number);

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


--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201303_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_FTA

into v200_zero_mix_viewing_201303_summary_by_account
from v200_zero_mix_viewing_201303 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201303_summary_by_account (account_number);

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

alter table v200_zero_mix_viewing_201303_summary_by_account add distinct_viewing_days integer;

update v200_zero_mix_viewing_201303_summary_by_account
set distinct_viewing_days=case when b.distinct_viewing_days is null then 0 else b.distinct_viewing_days end
from v200_zero_mix_viewing_201303_summary_by_account as a 
left outer join #days_viewing_by_account_201303 as b
on a.account_number = b.account_number
;
commit;

drop table v200_zero_mix_viewing_201303;

commit;


---201304

select 
a.account_number
,channel_name
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
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


--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201304_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_FTA

into v200_zero_mix_viewing_201304_summary_by_account
from v200_zero_mix_viewing_201304 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v200_zero_mix_viewing_201304_summary_by_account (account_number);

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


--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201305_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_FTA

into v200_zero_mix_viewing_201305_summary_by_account
from v200_zero_mix_viewing_201305 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;
commit;

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

--select top 500 * from v200_zero_mix_viewing_201307_summary_by_account;

---201306
select 
a.account_number
,channel_name
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
into v200_zero_mix_viewing_201306
from  sk_prod.vespa_dp_prog_viewed_201306 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;

CREATE HG INDEX idx1 ON v200_zero_mix_viewing_201306 (account_number);

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201306 (channel_name);


--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201306_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_FTA

into v200_zero_mix_viewing_201306_summary_by_account
from v200_zero_mix_viewing_201306 as a
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


--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v200_zero_mix_viewing_201307_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel'  then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_FTA

into v200_zero_mix_viewing_201307_summary_by_account
from v200_zero_mix_viewing_201307 as a
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
,distinct_viewing_days_201302 integer default 0

,seconds_viewed_sky_sports_201303 integer default 0
,seconds_viewed_sky_movies_201303 integer default 0
,seconds_viewed_other_pay_201303 integer default 0
,seconds_viewed_FTA_201303 integer default 0
,distinct_viewing_days_201303 integer default 0

,seconds_viewed_sky_sports_201304 integer default 0
,seconds_viewed_sky_movies_201304 integer default 0
,seconds_viewed_other_pay_201304 integer default 0
,seconds_viewed_FTA_201304 integer default 0
,distinct_viewing_days_201304 integer default 0

,seconds_viewed_sky_sports_201305 integer default 0
,seconds_viewed_sky_movies_201305 integer default 0
,seconds_viewed_other_pay_201305 integer default 0
,seconds_viewed_FTA_201305 integer default 0
,distinct_viewing_days_201305 integer default 0

,seconds_viewed_sky_sports_201306 integer default 0
,seconds_viewed_sky_movies_201306 integer default 0
,seconds_viewed_other_pay_201306 integer default 0
,seconds_viewed_FTA_201306 integer default 0
,distinct_viewing_days_201306 integer default 0

,seconds_viewed_sky_sports_201307 integer default 0
,seconds_viewed_sky_movies_201307 integer default 0
,seconds_viewed_other_pay_201307 integer default 0
,seconds_viewed_FTA_201307 integer default 0
,distinct_viewing_days_201307 integer default 0
)
;

--Update days viewing and seconds viewed by type

update  v200_zero_mix_full_account_list
set seconds_viewed_sky_sports_201302=b.seconds_viewed_sky_sports
,seconds_viewed_sky_movies_201302=b.seconds_viewed_sky_movies
,seconds_viewed_other_pay_201302=b.seconds_viewed_other_pay
,seconds_viewed_FTA_201302=b.seconds_viewed_FTA
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









