

--Project 250 -- Viewing Data Summary


--select top 100 * from sk_prod.vespa_dp_prog_viewed_201308;
commit;
--drop table v223_unbundling_viewing_201308;
--select * from dbarnett.viewing_month_order_lookup;
grant all on dbarnett.viewing_month_order_lookup to public;

--Run from previous project way of running monthly data within a macro--
/*
create table dbarnett.viewing_month_order_lookup
(month varchar(6)
,month_order integer
)
;

input into dbarnett.viewing_month_order_lookup
from 'G:\RTCI\Lookup Tables\Month Order Lookup.csv' format ascii;

commit;
*/
--Create Lookup Table of All Daily Augs Tables that are populated---
--drop table Augs_Tables_Dates_Available;
CREATE TABLE Augs_Tables_Dates_Available (Date_ DATE, Rank INT); --  drop table F_Dates_Augs -- select * from F_Dates_Augs

INSERT INTO Augs_Tables_Dates_Available (Date_ , Rank)
(
SELECT DATEFORMAT(CAST(SUBSTRING(table_name, 18, 8) AS DATE), 'yyyy-mm-dd') AS Date_
        ,RANK() OVER ( PARTITION BY NULL ORDER BY Date_ ASC) AS Rank
        FROM   SP_TABLES()
        WHERE  table_owner = 'vespa_analysts'
        AND LOWER(table_name) LIKE 'vespa_daily_augs_%'
        AND LOWER(table_name) NOT LIKE '%invalid%'
--        GROUP  BY DATEFORMAT(CAST(SUBSTRING(table_name, 18, 8) AS DATE), 'yyyy-mm-dd') 
       
GROUP  BY Date_
ORDER  BY Date_ ASC
);

grant all on Augs_Tables_Dates_Available to public;
commit;


---select * from  Augs_Tables_Dates_Available;


---Create Empty table to insert all sports programmes viewed--
--drop table dbarnett.v250_all_programmes_viewed_sample;
create table dbarnett.v250_all_programmes_viewed_sample
(account_number varchar(20)
,dk_programme_instance_dim bigint
,viewing_duration int
,viewing_events int
)
;
commit;


--select count(*) from dbarnett.v250_all_programmes_viewed_sample;
--select * from dbarnett.viewing_month_order_lookup;
--select 1 into dbarnett.v250_month_counter;
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @month           varchar(6);
CREATE VARIABLE @var_sql                varchar(3000);
SET @var_cntr = 37;    --37= 201301

SET @var_sql = '
insert into dbarnett.v250_all_programmes_viewed_sample
(account_number
,dk_programme_instance_dim 
,viewing_duration 
,viewing_events
)

select 
a.account_number
,dk_programme_instance_dim
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
,count(*) as viewing_events
from  sk_prod.vespa_dp_prog_viewed_##^^*^*## as a
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and right(account_number,3)=''086'' and duration>=180
group by a.account_number
,dk_programme_instance_dim
;


drop table dbarnett.v250_month_counter;

select @var_cntr into dbarnett.v250_month_counter;

';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

--delete from Project_161_viewing_table;
commit;
while @var_cntr <= 46       --46=Oct 2013
begin
set @month=(select month from dbarnett.viewing_month_order_lookup where month_order=@var_cntr)
    EXECUTE(replace(@var_sql,'##^^*^*##',@month))
--    commit

    set @var_cntr = @var_cntr+1
end;
commit;
--delete from dbarnett.v223_all_sports_programmes_viewed where dk_programme_instance_dim=-1; commit;
---Repeat for Daily Activity between 1st Aug and End Sep (28th when 1st Run)---

--select * from dbarnett.v223_all_sports_programmes_viewed where dk_programme_instance_dim is null
--alter table dbarnett.v223_all_sports_programmes_viewed delete table_date;
--alter table dbarnett.v223_all_sports_programmes_viewed add table_date varchar(10);
--drop  VARIABLE @day       ;


CREATE VARIABLE @day           varchar(8);
SET @var_cntr = 95;    --95= 1st Nov 2012

SET @var_sql = '
insert into dbarnett.v250_all_programmes_viewed_sample
(account_number
,dk_programme_instance_dim 
,viewing_duration 
,viewing_events
--,table_date
) 


select 
a.account_number
,pk_programme_instance_dim
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
,count(*) as viewing_events
from  vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where 
--panel = 12 and 
right(account_number,3)=''086'' and a.viewing_duration>=180
group by a.account_number
,pk_programme_instance_dim
;

drop table dbarnett.v250_month_counter;

select @var_cntr into dbarnett.v250_month_counter;
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;
--select * into dbarnett.v223_all_sports_programmes_viewed_backup from dbarnett.v223_all_sports_programmes_viewed; commit;
--delete from Project_161_viewing_table;
commit;
while @var_cntr <= 155 --155 = 31st Dec 2012
begin
set @day=(select replace(cast(Date_ as varchar),'-','') from  Augs_Tables_Dates_Available where rank=@var_cntr)
    EXECUTE(replace(@var_sql,'##^^*^*##',@day))
--    commit

    set @var_cntr = @var_cntr+1
end;
commit;
--select * from   Augs_Tables_Dates_Available
---Add Index---


CREATE HG INDEX idx1 ON dbarnett.v250_all_programmes_viewed_sample (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_all_programmes_viewed_sample (dk_programme_instance_dim);

commit;
--select count(*) from dbarnett.v250_all_programmes_viewed_sample;

--select top 500 * from dbarnett.v250_all_programmes_viewed_sample;
--select top 500 * from sk_prod.Vespa_programme_schedule;

---Add EPG Info--

alter table dbarnett.v250_all_programmes_viewed_sample add broadcast_start_date_time_local datetime;


update dbarnett.v250_all_programmes_viewed_sample
set broadcast_start_date_time_local=b.broadcast_start_date_time_local
from dbarnett.v250_all_programmes_viewed_sample as a
left outer join sk_prod.Vespa_programme_schedule as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
;

commit;


---Show date split info to test all dates returned--

select cast(broadcast_start_date_time_local as date) as broadcast_date
,count(*) as records
from dbarnett.v250_all_programmes_viewed_sample
group by broadcast_date
order by broadcast_date
;



































/*
--Project 250 -- Viewing Data Summary
---Get All Viewing Details from Sep 2013--
--drop table dbarnett.project250_sep_2013_viewing_sample;
--select top 100 * from sk_prod.vespa_dp_prog_viewed_201309 ;
select 
a.account_number
,dk_programme_instance_dim
,service_key
,min(programme_instance_name) as programme_name
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
,count(*) as viewing_events
into dbarnett.project250_sep_2013_viewing_sample
from  sk_prod.vespa_dp_prog_viewed_201309 as a
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and right(account_number,3)='086' and duration>=180
group by a.account_number
,dk_programme_instance_dim
,service_key
;



select cast(instance_start_date_time_utc as date) as view_day
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #sep
from sk_prod.vespa_dp_prog_viewed_201309
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and service_key in (3625,3627,3661,3663) and live_recorded='LIVE'
group by view_day
order by view_day
;
--select * from #sep
select cast(instance_start_date_time_utc as date) as view_day
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #oct
from sk_prod.vespa_dp_prog_viewed_201310
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and service_key in (3625,3627,3661,3663) and live_recorded='LIVE'
group by view_day
order by view_day
;

select service_key,
cast(instance_start_date_time_utc as date) as view_day
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #ss2_aug2
from sk_prod.vespa_dp_prog_viewed_201308
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and 
service_key in (1302,4081) 
and live_recorded<>'LIVE'
group by service_key,view_day
order by service_key,view_day
;

select service_key,
cast(instance_start_date_time_utc as date) as view_day
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #ss2_sep
from sk_prod.vespa_dp_prog_viewed_201309
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and 
service_key in (1302,4081) and live_recorded='LIVE'
group by service_key,view_day
order by service_key,view_day
;



select service_key,
programme_instance_name
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration

from sk_prod.vespa_dp_prog_viewed_201307
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and 
service_key in (1301,4002) 
and cast(instance_start_date_time_utc as date) = '2013-07-14'
group by service_key,
programme_instance_name
order by service_key,
programme_instance_name
;

select service_key,
programme_instance_name
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration

from sk_prod.vespa_dp_prog_viewed_201307
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and 
service_key in (4002) 
and cast(instance_start_date_time_utc as date) = '2013-07-14'
group by service_key,
programme_instance_name
order by service_key,
programme_instance_name
;


select dk_programme_instance_dim
,genre_description
,sub_genre_description
from sk_prod.vespa_dp_prog_viewed_201307
where panel_id = 12 and 
service_key in (4002) 
and cast(instance_start_date_time_utc as date) = '2013-07-14'
and programme_instance_name is null


select programme_instance_name
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
from sk_prod.vespa_dp_prog_viewed_201307
where panel_id = 12 and 
service_key in (2076) 
and cast(instance_start_date_time_utc as date) = '2013-07-14'
--and programme_instance_name is null





select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2

from sk_prod.vespa_dp_prog_viewed_201307 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key

--select * from #no_name4

commit;



select top 100 *  from sk_prod.vespa_dp_prog_viewed_201307

--select * from #ss2_aug
--select * from #ss2_aug2
--select * from #ss2_sep
commit;
select * from #ss2_jul;
select * from #oct;
select * from #ss2_jul;



select cast(viewing_starts as date) as date1
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
from  vespa_analysts.VESPA_DAILY_AUGS_20130605 a

left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where 
--panel_id = 12 and
 service_key in (4081) and timeshifting = 'LIVE' and duration>=180
group by date1


--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20130805;


commit;



select cast(instance_start_date_time_utc as date) as view_day
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration

from sk_prod.vespa_dp_prog_viewed_201211
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and service_key in (4081) and live_recorded='LIVE'
group by view_day
order by view_day
;


--select top 100 * from dbarnett.v250_sports_rights_epg_detail;
--drop table dbarnett.project250_viewing_with_rights_details_sample;
select a.account_number
,Channel_name
,case when b.master_deal is null then sub_genre_description||' '||Channel_name else b.master_deal end as master_deal_type
,case when b.deal  is null then sub_genre_description||' '||Channel_name  else b.deal end as deal_type
,sum(viewing_duration) as total_duration_viewed
,sum(case when viewing_duration>=180 then 1 else 0 end) as programmes_viewed_03min_plus
,sum(case when viewing_duration>=900 then 1 else 0 end) as programmes_viewed_15min_plus
into dbarnett.project250_viewing_with_rights_details_sample
from dbarnett.project250_sep_2013_viewing_sample as a

left outer join dbarnett.v250_sports_rights_epg_detail as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
where b.dk_programme_instance_dim is not null
group by  account_number
,Channel_name
,master_deal_type
,deal_type 
;
commit;




select live_recorded,cast(instance_start_date_time_utc as date) as view_day,count(*)
from sk_prod.vespa_dp_prog_viewed_201308
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12 
and service_key in (4081)
group by live_recorded,view_day
order by live_recorded,view_day
;

select count(*) from vespa_analysts.VESPA_DAILY_AUGS_20130628
commit;

select account_number 
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #sky_sports_2_hd_viewing2
from sk_prod.vespa_dp_prog_viewed_201308
where  
capping_end_date_time_utc >instance_start_date_time_utc
and 
panel_id = 12 
and service_key in (4081)
group by account_number
order by account_number
;

select * from #sky_sports_2_hd_viewing2 order by viewing_duration desc


select instance_start_date_time_utc,instance_end_date_time_utc,channel_name,live_recorded,
time_in_seconds_since_recording
,programme_name,next_programme_name from sk_prod.vespa_dp_prog_viewed_201308 where account_number = '240012731594'
and cast(instance_start_date_time_utc as date)>='2013-08-10' order by instance_start_date_time_utc


select instance_start_date_time_utc,instance_end_date_time_utc,channel_name,live_recorded,
time_in_seconds_since_recording
,programme_name from sk_prod.vespa_dp_prog_viewed_201308 where account_number = '220014595254'
and cast(instance_start_date_time_utc as date)>='2013-08-01' order by instance_start_date_time_utc

select instance_start_date_time_utc,instance_end_date_time_utc,channel_name,live_recorded,
time_in_seconds_since_recording
,programme_name from sk_prod.vespa_dp_prog_viewed_201308 where account_number = '621036081886'
and cast(instance_start_date_time_utc as date)>='2013-08-01' order by instance_start_date_time_utc





*/



