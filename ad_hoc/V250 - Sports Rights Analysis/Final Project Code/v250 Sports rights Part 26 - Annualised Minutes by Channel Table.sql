/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 26 Channel Split Annualised Minutes Table Creation
        
        Analyst: Dan Barnett
        SK Prod: 5
        

*/------------------------------------------------------------------------------------------------------------------


---To avoid having to run a large viewing table repeatedly each time a new channel comes in, 
---aim of this is to provide a summary of viewing by account by channel (Annualised Minutes)

--First two parts commented out as run in previous code but include here to show how they were built


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
/*

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

*/



---select * from  Augs_Tables_Dates_Available;

--select count(*) from dbarnett.v250_all_sports_programmes_viewed_new_data;
---Create Empty table to insert all sports programmes viewed--
----select 00 into dbarnett.v250_loop_counter02; 

/*
select account_number into dbarnett.v250_master_account_list_with_weight from dbarnett.v250_master_account_list where account_weight>0;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_master_account_list_with_weight (account_number);
grant all on dbarnett.v250_master_account_list_with_weight to public;
*/

--select sum(account_weight) from dbarnett.v250_master_account_list
--selct count(*) from dbarnett.v250_master_account_list_with_weight;

--drop table dbarnett.v250_account_viewing_by_service_key;
create table dbarnett.v250_account_viewing_by_service_key
(account_number varchar(20)
,service_key bigint
,viewing_duration int
)
;
commit;

--select top 10 service_key from sk_prod.vespa_dp_prog_viewed_201402
--select count(*) from dbarnett.v250_account_viewing_by_service_key;
--select * from dbarnett.viewing_month_order_lookup;
--select 1 into dbarnett.v250_month_counter;
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @month           varchar(6);
CREATE VARIABLE @var_sql                varchar(3000);
SET @var_cntr = 37;    --37= 201301

SET @var_sql = '
insert into dbarnett.v250_account_viewing_by_service_key
(account_number
,service_key 
,viewing_duration 
)

select 
a.account_number
,service_key
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
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
                           and capping_end_date_time_utc is not null 

group by a.account_number
,service_key
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
--select * from dbarnett.viewing_month_order_lookup;
--select * from dbarnett.v223_all_sports_programmes_viewed where dk_programme_instance_dim is null
--alter table dbarnett.v223_all_sports_programmes_viewed delete table_date;
--alter table dbarnett.v223_all_sports_programmes_viewed add table_date varchar(10);
--drop  VARIABLE @day       ;


CREATE VARIABLE @day           varchar(8);
SET @var_cntr = 95;    --95= 1st Nov 2012 - rerun part way through at 108

SET @var_sql = '
insert into dbarnett.v250_account_viewing_by_service_key
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
group by a.account_number
,service_key
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

--select top 100 * from dbarnett.v250_account_viewing_by_service_key;

---Group by Channel---
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_account_viewing_by_service_key (service_key);
commit;
select account_number
,channel_name
,sum(viewing_duration) as total_duration
into dbarnett.v250_account_viewing_by_channel
from dbarnett.v250_account_viewing_by_service_key as a
left outer join  v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
group by account_number
,channel_name
;

--select * from v250_channel_to_service_key_lookup_deduped order by channel_name


commit;
--select top 100 * from v250_channel_to_service_key_lookup_deduped;
--select grouped_channel , min(channel_name),max(channel_name) from v250_channel_to_service_key_lookup_deduped group by grouped_channel order by grouped_channel

--grant all on dbarnett.v250_account_viewing_by_channel to public;

--select channel_name ,sum(total_duration) from dbarnett.v250_account_viewing_by_channel group by channel_name order by channel_name


