/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 08 Total Viewing
        
        Analyst: Dan Barnett
        SK Prod: 5

        Create table of all viewing by account
*/------------------------------------------------------------------------------------------------------------------

---Number of Days Accounts Returning Data---
--select count(*) from dbarnett.v250_daily_viewing_duration;
--drop table dbarnett.v250_daily_viewing_duration;
create table dbarnett.v250_daily_viewing_duration
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
SET @viewing_days_var_cntr = 35;    --44= 201308





SET @viewing_days_var_sql = '

insert into dbarnett.v250_daily_viewing_duration
(account_number
,viewing_date 
,viewing_duration 
,viewing_duration_post_5am 
)

select 
a.account_number
,cast(instance_start_date_time_utc as date) as viewing_date
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,sum( case  when dateformat(instance_start_date_time_utc,''HH'') in (''00'',''01'',''02'',''03'',''04'') then 0 
            when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_post_5am 
from  sk_prod.vespa_dp_prog_viewed_##^^*^*## as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc and duration>=180
group by a.account_number
,viewing_date
;
commit;
drop table dbarnett.v250_loop_counter01;
commit;
select @viewing_days_var_cntr into dbarnett.v250_loop_counter01;
commit;
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

--delete from Project_161_viewing_table;
commit;
while @viewing_days_var_cntr <= 46       --45=Sep 2013
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
--sp_iqtablesize 'dbarnett.v250_daily_viewing_duration' 

CREATE VARIABLE @viewing_days_day           varchar(8);
--SET @viewing_days_var_cntr = 2;    --2= 1st Aug 2012
SET @viewing_days_var_cntr = 95;    --153= 30th Dec 2012 -- Rerun from part way in due to disconnection

SET @viewing_days_var_sql = '
insert into dbarnett.v250_daily_viewing_duration
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
;
commit;

drop table dbarnett.v250_loop_counter01;
commit;
select @viewing_days_var_cntr into dbarnett.v250_loop_counter01;
commit;
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;
--select panel_id from vespa_analysts.VESPA_DAILY_AUGS_20130501
--select * into dbarnett.v223_all_sports_programmes_viewed_backup from dbarnett.v223_all_sports_programmes_viewed; commit;
--delete from Project_161_viewing_table;
commit;
while @viewing_days_var_cntr <= 155 --363 = 31st July 2013
begin
set @viewing_days_day=(select replace(cast(Date_ as varchar),'-','') from  Augs_Tables_Dates_Available where rank=@viewing_days_var_cntr)
    EXECUTE(replace(@viewing_days_var_sql,'##^^*^*##',@viewing_days_day))
--    commit

    set @viewing_days_var_cntr = @viewing_days_var_cntr+1
end;
commit;

---Add Index---


CREATE HG INDEX idx1 ON dbarnett.v250_daily_viewing_duration (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_daily_viewing_duration (viewing_date);

commit;

select account_number
,viewing_date
,max(case when viewing_duration_post_5am >0 then 1 else 0 end) as days_with_viewing
into dbarnett.v250_days_viewing_by_account
from dbarnett.v250_daily_viewing_duration
where viewing_date between '2012-11-01' and '2013-10-31'  
group by account_number
,viewing_date
having days_with_viewing>0
;
commit;


select account_number
,sum(days_with_viewing) as total_days_with_viewing
into dbarnett.v250_days_viewed_by_account
from dbarnett.v250_days_viewing_by_account
group by account_number
;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewed_by_account (account_number);

select total_days_with_viewing , count(*) from dbarnett.v250_days_viewed_by_account group by total_days_with_viewing order by total_days_with_viewing


--select * from dbarnett.v250_loop_counter01
--select * from dbarnett.v250_loop_counter01;


commit;
