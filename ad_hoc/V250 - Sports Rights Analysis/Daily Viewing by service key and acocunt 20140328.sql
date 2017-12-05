drop table dbarnett.v250_account_viewing_by_service_key_daily;
--select 1 into dbarnett.v250_loop_counter02;
create table dbarnett.v250_account_viewing_by_service_key_daily
(account_number varchar(20)
,service_key integer
,viewing_duration int
)
;
commit;





CREATE VARIABLE @viewing_days_var_cntr               smallint;
CREATE VARIABLE @viewing_days_month           varchar(6);
CREATE VARIABLE @viewing_days_var_sql                varchar(3000);

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
insert into dbarnett.v250_account_viewing_by_service_key_daily
(account_number
,service_key 
,viewing_duration 
)

select 
a.account_number
,service_key 
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration

from  vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
--where 
--panel_id = 12 and 
group by a.account_number
,service_key
;
commit;

drop table dbarnett.v250_loop_counter02;
commit;
select @viewing_days_var_cntr into dbarnett.v250_loop_counter02;
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