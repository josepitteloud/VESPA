--v250 - Genre Level Sports Analysis v01
--select top 100 * from dbarnett.v250_master_account_list;

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

--select count(*) from dbarnett.v250_all_sports_programmes_viewed;
---Create Empty table to insert all sports programmes viewed--
----select 00 into dbarnett.v250_loop_counter02; 
select account_number into dbarnett.v250_master_account_list_with_weight from dbarnett.v250_master_account_list where account_weight>0;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_master_account_list_with_weight (account_number);
grant all on dbarnett.v250_master_account_list_with_weight to public;
--select sum(account_weight) from dbarnett.v250_master_account_list
--selct count(*) from dbarnett.v250_master_account_list_with_weight;

drop table dbarnett.v250_all_sports_programmes_viewed;
create table dbarnett.v250_all_sports_programmes_viewed
(account_number varchar(20)
,dk_programme_instance_dim bigint
,viewing_duration int
,viewing_events int
)
;
commit;


--select count(*) from dbarnett.v250_all_sports_programmes_viewed;
--select * from dbarnett.viewing_month_order_lookup;
--select 1 into dbarnett.v250_month_counter;
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @month           varchar(6);
CREATE VARIABLE @var_sql                varchar(3000);
SET @var_cntr = 37;    --37= 201301

SET @var_sql = '
insert into dbarnett.v250_all_sports_programmes_viewed
(account_number
,dk_programme_instance_dim 
,viewing_duration 
,viewing_events
)

select 
a.account_number
,dk_programme_instance_dim
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,count(*) as viewing_events
from  sk_prod.vespa_dp_prog_viewed_##^^*^*## as a
where  panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc and duration>=180
and genre_description = ''Sports''
group by a.account_number
,dk_programme_instance_dim
;

commit;
drop table dbarnett.v250_loop_counter02;
commit;
select @var_cntr into dbarnett.v250_loop_counter02;

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
SET @var_cntr = 95;    --95= 1st Nov 2012 - rerun part way through at 108

SET @var_sql = '
insert into dbarnett.v250_all_sports_programmes_viewed
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
--right(account_number,3)=''086''
genre_description = ''Sports'' and a.viewing_duration>=180
group by a.account_number
,pk_programme_instance_dim
;

commit;
drop table dbarnett.v250_loop_counter02;
commit;
select @var_cntr into dbarnett.v250_loop_counter02;
drop table dbarnett.v250_month_counter;

select @var_cntr into dbarnett.v250_month_counter;
';
--select @var_cntr
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


CREATE HG INDEX idx1 ON dbarnett.v250_all_sports_programmes_viewed (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_all_sports_programmes_viewed (dk_programme_instance_dim);

commit;
--select count(*) from dbarnett.v250_all_sports_programmes_viewed;
--select count(*) from dbarnett.v250_all_sports_programmes_viewed where  cast(viewing_duration as real)/180<viewing_events;
--select top 500 * from  dbarnett.v250_all_sports_programmes_viewed where  cast(viewing_duration as real)/180<viewing_events
delete from dbarnett.v250_all_sports_programmes_viewed where  cast(viewing_duration as real)/180<viewing_events
commit;



/*
--select * from dbarnett.v250_loop_counter02;

select dk_programme_instance_dim
,sum(viewing_duration) as tot_dur

from dbarnett.v250_all_sports_programmes_viewed
group by dk_programme_instance_dim
order by tot_dur desc


select viewing_duration,viewing_events, cast(viewing_duration as real)/180  from dbarnett.v250_all_sports_programmes_viewed 
--where cast(viewing_duration as real)/180>viewing_events

select count(*) from
sk_prod.Vespa_programme_schedule
where genre_description='Sports'

commit;


select genre_description
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
from  sk_prod.vespa_dp_prog_viewed_201301 as a
where  
--panel_id = 12  and capped_full_flag = 0
--and instance_start_date_time_utc < instance_end_date_time_utc and duration>=180
--and 
dk_programme_instance_dim in (441994585,
450289927,
560864466)
group by genre_description

select dk_programme_instance_dim
,programme_instance_name
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
--into #daily_info
from  sk_prod.vespa_dp_prog_viewed_201301 as a
where  service_key in (1301,4002,1701)
and panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc and duration>=180
and cast(instance_start_date_time_utc as date) = '2013-01-08'
group by dk_programme_instance_dim
,programme_instance_name
order by viewing_duration desc



select * from sk_prod.Vespa_programme_schedule 
where dk_programme_instance_dim in (552053854,
557263343)


select * from sk_prod.Vespa_programme_schedule 
where programme_instance_name = 'Bradford City v Aston VillaLive'

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


*/









