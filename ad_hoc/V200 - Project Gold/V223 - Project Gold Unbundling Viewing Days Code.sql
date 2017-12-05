

---Number of Days Accounts Returning Data---

create table dbarnett.v223_daily_viewing_duration
(account_number varchar(20)
,viewing_date date
,viewing_duration int
,viewing_duration_post_5am int
)
;
commit;


CREATE VARIABLE @viewing_days_var_cntr               smallint;
CREATE VARIABLE @viewing_days_month           varchar(6);
CREATE VARIABLE @viewing_days_var_sql                varchar(3000);
SET @viewing_days_var_cntr = 44;    --44= 201308





SET @viewing_days_var_sql = '
insert into dbarnett.v223_daily_viewing_duration
(account_number
,viewing_date 
,viewing_duration 
,viewing_duration_post_5am 
)

select 
a.account_number
,cast(instance_start_date_time_utc as date) as viewing_date
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
,sum( case  when dateformat(instance_start_date_time_utc,''HH'') in (''00'',''01'',''02'',''03'',''04'') then 0 
            when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration_post_5am 
from  sk_prod.vespa_dp_prog_viewed_##^^*^*## as a
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 
group by a.account_number
,viewing_date
;
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

--delete from Project_161_viewing_table;
commit;
while @viewing_days_var_cntr <= 45       --45=Sep 2013
begin
set @viewing_days_month=(select month from dbarnett.viewing_month_order_lookup where month_order=@viewing_days_var_cntr)
    EXECUTE(replace(@viewing_days_var_sql,'##^^*^*##',@viewing_days_month))
--    commit

    set @viewing_days_var_cntr = @viewing_days_var_cntr+1
end;
commit;
--delete from dbarnett.v223_all_sports_programmes_viewed where dk_programme_instance_dim=-1; commit;
---Repeat for Daily Activity between 1st Aug and End Sep (28th when 1st Run)---

--select * from dbarnett.v223_all_sports_programmes_viewed where dk_programme_instance_dim is null
--alter table dbarnett.v223_all_sports_programmes_viewed delete table_date;
--alter table dbarnett.v223_all_sports_programmes_viewed add table_date varchar(10);
--drop  VARIABLE @viewing_days_day       ;
--sp_iqtablesize 'dbarnett.v223_daily_viewing_duration' 

CREATE VARIABLE @viewing_days_day           varchar(8);
--SET @viewing_days_var_cntr = 2;    --2= 1st Aug 2012
SET @viewing_days_var_cntr = 153;    --153= 30th Dec 2012 -- Rerun from part way in due to disconnection

SET @viewing_days_var_sql = '
insert into dbarnett.v223_daily_viewing_duration
(account_number
,viewing_date 
,viewing_duration 
,viewing_duration_post_5am 
)

select 
a.account_number
,cast(viewing_starts as date) as viewing_date
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
,sum( case  when dateformat(viewing_starts,''HH'')  in (''00'',''01'',''02'',''03'',''04'') then 0
        else datediff(second,viewing_starts,viewing_stops) end) as viewing_duration_post_5am 
from  vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## a
--where 
--panel_id = 12 and 
group by a.account_number
,viewing_date

';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;
--select panel_id from vespa_analysts.VESPA_DAILY_AUGS_20130501
--select * into dbarnett.v223_all_sports_programmes_viewed_backup from dbarnett.v223_all_sports_programmes_viewed; commit;
--delete from Project_161_viewing_table;
commit;
while @viewing_days_var_cntr <= 363 --363 = 31st July 2013
begin
set @viewing_days_day=(select replace(cast(Date_ as varchar),'-','') from  Augs_Tables_Dates_Available where rank=@viewing_days_var_cntr)
    EXECUTE(replace(@viewing_days_var_sql,'##^^*^*##',@viewing_days_day))
--    commit

    set @viewing_days_var_cntr = @viewing_days_var_cntr+1
end;
commit;

---Add Index---


CREATE HG INDEX idx1 ON dbarnett.v223_daily_viewing_duration (account_number);
CREATE HG INDEX idx2 ON dbarnett.v223_daily_viewing_duration (viewing_date);

commit;

select account_number
,sum(case when viewing_duration_post_5am >0 then 1 else 0 end) as days_with_viewing
into #count_by_day
from dbarnett.v223_daily_viewing_duration
where viewing_date between '2012-08-01' and '2013-07-31'
group by account_number
;

select days_with_viewing
,count(*) as accounts
from #count_by_day
group by days_with_viewing
order by days_with_viewing
;

select viewing_date
,count(*) as accounts
from dbarnett.v223_daily_viewing_duration
group by viewing_date
order by viewing_date
;
commit;

/*
select viewing_date
,count(*)
from dbarnett.v223_daily_viewing_duration
group by viewing_date
order by viewing_date
;
select *  from  Augs_Tables_Dates_Available

sp_iqtablesize 'dbarnett.v223_daily_viewing_duration'



*/