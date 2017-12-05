/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 22b Viewing By Service Key
        
        Analyst: Dan Barnett
        SK Prod: 5

        Create table of all viewing by account and service key
*/------------------------------------------------------------------------------------------------------------------

---Number of Days Accounts Returning Data---
--select count(*) from dbarnett.v250_daily_viewing_duration;
--drop table dbarnett.v250_account_viewing_by_service_key;
create table dbarnett.v250_account_viewing_by_service_key
(account_number varchar(20)
,service_key integer
,viewing_duration int
,sport_viewing_duration int

)
;
commit;


CREATE VARIABLE @viewing_days_var_cntr               smallint;
CREATE VARIABLE @viewing_days_month           varchar(6);
CREATE VARIABLE @viewing_days_var_sql                varchar(3000);
SET @viewing_days_var_cntr = 35;    --44= 201308





SET @viewing_days_var_sql = '

insert into dbarnett.v250_account_viewing_by_service_key
(account_number
,service_key 
,viewing_duration 
,sport_viewing_duration
)

select 
a.account_number
,a.service_key
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,sum(case when genre_description <> ''Sports'' then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end) as sport_viewing_duration
from  sk_prod.vespa_dp_prog_viewed_##^^*^*## as a
where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and (type_of_viewing_event in (''HD Viewing Event'', ''Sky+ time-shifted viewing event'', ''TV Channel Viewing'')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                            or (type_of_viewing_event = ''Other Service Viewing Event'' 
                            and service_type_description in (''NVOD service'',''High Definition TV test service'',''Digital TV channel'')))
                           and capping_end_date_time_utc is not null and duration>=180

group by a.account_number
,a.service_key
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
insert into dbarnett.v250_account_viewing_by_service_key
(account_number
,service_key 
,viewing_duration 
,sport_viewing_duration
)

select 
a.account_number
,service_key 
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
,sum(case when genre_description <> ''Sports'' then 0 else  datediff(second,viewing_starts,viewing_stops) end) as sport_viewing_duration
from  vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
--where 
--panel_id = 12 and 
group by a.account_number
,service_key
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


CREATE HG INDEX idx1 ON dbarnett.v250_account_viewing_by_service_key (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_account_viewing_by_service_key (service_key);

commit;
--select account_number , service_key, count(*) from 
---Create Deduped Table
select * into dbarnett.v250_account_viewing_by_service_key_deduped_old_version from dbarnett.v250_account_viewing_by_service_key_deduped;
commit;

drop table dbarnett.v250_account_viewing_by_service_key_deduped;

select account_number
,service_key
,sum(viewing_duration) as total_viewing_duration
,sum(sport_viewing_duration) as total_sport_viewing_duration
,count(*) as records
into dbarnett.v250_account_viewing_by_service_key_deduped
from dbarnett.v250_account_viewing_by_service_key
group by  account_number
,service_key
;
commit;

drop table dbarnett.v250_account_viewing_by_service_key_deduped_old_version;

grant all on dbarnett.v250_account_viewing_by_service_key_deduped to public;
commit;

drop table dbarnett.v250_account_viewing_by_service_key;
commit;
/*
select account_number
,sum(total_viewing_duration) as viewing_duration
,sum(total_sport_viewing_duration)
--into #account_viewing
from dbarnett.v250_account_viewing_by_service_key_deduped
where account_number in ('621057736251','621377466233','620056878221','620011257461')
group by account_number
;

select account_number
,sum(total_viewing_duration) as viewing_duration
--into #account_viewing
from dbarnett.v250_account_viewing_by_service_key_deduped_old_version
where account_number in ('621057736251','621377466233','620056878221','620011257461')
group by account_number
;
*/




--select top 100 *  from dbarnett.v250_account_viewing_by_service_key_deduped

--select top 100 *  from dbarnett.v250_account_viewing_by_service_key


/*
account_number,seconds_viewed_pay,seconds_viewed_pay_movies,seconds_viewed_pay_sports,seconds_viewed_pay_ent,total_viewing_duration
'621057736251',3028407,669467,131378,2503888,7088010
'621377466233',4118610,0,34148,4084462,6624661
'620056878221',3565586,1030920,250426,2371906,7183169
'620011257461',2868012,150806,23957,2700147,8477710

select * from #account_viewing where account_number='620011257461'
select distinct genre_description from sk_prod.vespa_dp_prog_viewed_201312

*/

---Repeat for 




