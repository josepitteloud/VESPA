/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 24 Viewing By Service Key
        
        Analyst: Dan Barnett
        SK Prod: 5

        Create table of all viewing by account and service key and programme to get details on #programmes viewed 3min+,10min+ 60% (or 1hr for 90min+ prog)
*/------------------------------------------------------------------------------------------------------------------

---Number of Days Accounts Returning Data---
--select count(*) from dbarnett.v250_daily_viewing_duration;
--drop table dbarnett.v250_account_viewing_by_service_key_and_programme_sk;
create table dbarnett.v250_account_viewing_by_service_key_and_programme_sk
(account_number varchar(20)
,service_key integer
,dk_programme_instance_dim bigint
,viewing_duration int
)
;
commit;

--select top 100 dk_programme_instance_dim  from  sk_prod.vespa_dp_prog_viewed_201402;
CREATE VARIABLE @viewing_days_var_cntr               smallint;
CREATE VARIABLE @viewing_days_month           varchar(6);
CREATE VARIABLE @viewing_days_var_sql                varchar(3000);
SET @viewing_days_var_cntr = 44;    --44= 201308 ---Start at 35

SET @viewing_days_var_sql = '

insert into dbarnett.v250_account_viewing_by_service_key_and_programme_sk
(account_number
,service_key 
,dk_programme_instance_dim
,viewing_duration 
)

select 
a.account_number
,a.service_key
,dk_programme_instance_dim
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
                           and capping_end_date_time_utc is not null and duration>=180
group by a.account_number
,a.service_key
,a.dk_programme_instance_dim
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
--select * from dbarnett.v250_loop_counter01;
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

---Put Daily Data into a single table ---
create table dbarnett.v250_account_viewing_by_service_key_and_programme_sk_daily
(account_number varchar(20)
,service_key integer
,dk_programme_instance_dim bigint
,viewing_duration int
)
;
commit;



SET @viewing_days_var_sql = '
insert into dbarnett.v250_account_viewing_by_service_key_and_programme_sk_daily
(account_number
,service_key 
,dk_programme_instance_dim
,viewing_duration 

)

select 
a.account_number
,service_key 
,programme_trans_sk as dk_programme_instance_dim
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
from  vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
--where 
--panel_id = 12 and 
--where right(account_number,4)=''0654''
group by a.account_number
,service_key
,dk_programme_instance_dim
;
commit;

drop table dbarnett.v250_loop_counter01;
commit;
select @viewing_days_var_cntr into dbarnett.v250_loop_counter01;
commit;
';
--select * from dbarnett.v250_loop_counter01
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;
--select panel_id from vespa_analysts.VESPA_DAILY_AUGS_20130501
--select * into dbarnett.v223_all_sports_programmes_viewed_backup from dbarnett.v223_all_sports_programmes_viewed; commit;
--delete from Project_161_viewing_table;
--select * from dbarnett.v250_account_viewing_by_service_key_and_programme_sk;
commit;
while @viewing_days_var_cntr <= 155 --363 = 31st July 2013
begin
set @viewing_days_day=(select replace(cast(Date_ as varchar),'-','') from  Augs_Tables_Dates_Available where rank=@viewing_days_var_cntr)
    EXECUTE(replace(@viewing_days_var_sql,'##^^*^*##',@viewing_days_day))
--    commit

    set @viewing_days_var_cntr = @viewing_days_var_cntr+1
end;
commit;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_account_viewing_by_service_key_and_programme_sk_daily(account_number);
commit;
CREATE HG INDEX idx2 ON dbarnett.v250_account_viewing_by_service_key_and_programme_sk_daily(service_key);
CREATE HG INDEX idx3 ON dbarnett.v250_account_viewing_by_service_key_and_programme_sk_daily(dk_programme_instance_dim);

commit;
---Add Index---

commit;
---Get Summary by Account---
drop table dbarnett.v250_prog_watched_from_daily_data;
select account_number 

,sum(case when pay=1 and viewing_duration>=180 
then 1 else 0 end) as fta_programmes_03min_plus
,sum(case when pay=1 and viewing_duration>=600 
then 1 else 0 end) as fta_programmes_10min_plus
,sum(case when pay=1 and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as fta_programmes_60pc_or_1hr
 

,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=180 
then 1 else 0 end) as sky_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=600 
then 1 else 0 end) as sky_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_pay_basic_programmes_60pc_or_1hr    
         


,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=180 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=600 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6  and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1
when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=3600
 and programme_instance_duration >=5400  
and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK')
 then 1 else 0 end)
as third_party_pay_basic_programmes_60pc_or_1hr    
         

,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=180 
then 1 else 0 end) as sky_movies_programmes_03min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=600 
then 1 else 0 end) as sky_movies_programmes_10min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1  and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_movies_programmes_60pc_or_1hr  



,sum(case when pay=1 
    and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')

and viewing_duration>=180 
then 1 else 0 end) as other_programmes_03min_plus
,sum(case when pay=1  and viewing_duration>=600 
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end) as other_programmes_10min_plus

,sum(case when pay=1  and viewing_duration/cast(programme_instance_duration as real)>=0.6
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')
then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 
and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end)
as other_programmes_60pc_or_1hr  


into dbarnett.v250_prog_watched_from_daily_data
from dbarnett.v250_account_viewing_by_service_key_and_programme_sk_daily as a
--from dbarnett.v250_viewing_by_account_and_channel as a  Changed to use new data created in Part 22b
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
left outer join sk_prod.Vespa_programme_schedule as c
ON a.dk_programme_instance_dim = c.pk_programme_instance_dim
group by account_number
;
commit;

--select top 500 * from dbarnett.v250_prog_watched_from_daily_data

--select  * from  v250_channel_to_service_key_lookup_deduped order by channel_name;

--select  * from  dbarnett.v250_prog_watched_from_201301_data

--select  * from dbarnett.v250_prog_watched_from_daily_data

select * into dbarnett.v250_prog_watched_summary from dbarnett.v250_prog_watched_from_daily_data;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201301_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201302_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201303_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201304_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201305_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201306_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201307_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201308_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201309_data)
;

insert into dbarnett.v250_prog_watched_summary
(select * from dbarnett.v250_prog_watched_from_201310_data)
;

commit;

select account_number
,sum(fta_programmes_03min_plus) as fta_programmes_03min_plus_total
,sum(fta_programmes_10min_plus) as fta_programmes_10min_plus_total
,sum(fta_programmes_60pc_or_1hr) as fta_programmes_60pc_or_1hr_total
,sum(sky_pay_basic_programmes_03min_plus) as sky_pay_basic_programmes_03min_plus_total
,sum(sky_pay_basic_programmes_10min_plus) as sky_pay_basic_programmes_10min_plus_total
,sum(sky_pay_basic_programmes_60pc_or_1hr) as sky_pay_basic_programmes_60pc_or_1hr_total
,sum(third_party_pay_basic_programmes_03min_plus) as third_party_pay_basic_programmes_03min_plus_total
,sum(third_party_pay_basic_programmes_10min_plus) as third_party_pay_basic_programmes_10min_plus_total
,sum(third_party_pay_basic_programmes_60pc_or_1hr) as third_party_pay_basic_programmes_60pc_or_1hr_total
,sum(sky_movies_programmes_03min_plus) as sky_movies_programmes_03min_plus_total
,sum(sky_movies_programmes_10min_plus) as sky_movies_programmes_10min_plus_total
,sum(sky_movies_programmes_60pc_or_1hr) as sky_movies_programmes_60pc_or_1hr_total
,sum(other_programmes_03min_plus) as other_programmes_03min_plus_total
,sum(other_programmes_10min_plus) as other_programmes_10min_plus_total
,sum(other_programmes_60pc_or_1hr) as other_programmes_60pc_or_1hr_total

into dbarnett.v250_prog_watched_summary_final
from dbarnett.v250_prog_watched_summary
group by account_number
;
commit;

---Add back on to profiling table--


alter table dbarnett.v250_Account_profiling add fta_programmes_03min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add fta_programmes_10min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add fta_programmes_60pc_or_1hr_total_annualised real;
alter table dbarnett.v250_Account_profiling add sky_pay_basic_programmes_03min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add sky_pay_basic_programmes_10min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add sky_pay_basic_programmes_60pc_or_1hr_total_annualised real;
alter table dbarnett.v250_Account_profiling add third_party_pay_basic_programmes_03min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add third_party_pay_basic_programmes_10min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add third_party_pay_basic_programmes_60pc_or_1hr_total_annualised real;
alter table dbarnett.v250_Account_profiling add sky_movies_programmes_03min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add sky_movies_programmes_10min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add sky_movies_programmes_60pc_or_1hr_total_annualised real;
alter table dbarnett.v250_Account_profiling add other_programmes_03min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add other_programmes_10min_plus_total_annualised real;
alter table dbarnett.v250_Account_profiling add other_programmes_60pc_or_1hr_total_annualised real;


update dbarnett.v250_Account_profiling
set fta_programmes_03min_plus_total_annualised= cast(fta_programmes_03min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,fta_programmes_10min_plus_total_annualised= cast(fta_programmes_10min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,fta_programmes_60pc_or_1hr_total_annualised= cast(fta_programmes_60pc_or_1hr_total as real) * (365/cast(total_days_with_viewing as real))
,sky_pay_basic_programmes_03min_plus_total_annualised= cast(sky_pay_basic_programmes_03min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,sky_pay_basic_programmes_10min_plus_total_annualised= cast(sky_pay_basic_programmes_10min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,sky_pay_basic_programmes_60pc_or_1hr_total_annualised= cast(sky_pay_basic_programmes_60pc_or_1hr_total as real) * (365/cast(total_days_with_viewing as real))
,third_party_pay_basic_programmes_03min_plus_total_annualised= cast(third_party_pay_basic_programmes_03min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,third_party_pay_basic_programmes_10min_plus_total_annualised= cast(third_party_pay_basic_programmes_10min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,third_party_pay_basic_programmes_60pc_or_1hr_total_annualised= cast(third_party_pay_basic_programmes_60pc_or_1hr_total as real) * (365/cast(total_days_with_viewing as real))
,sky_movies_programmes_03min_plus_total_annualised= cast(sky_movies_programmes_03min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,sky_movies_programmes_10min_plus_total_annualised= cast(sky_movies_programmes_10min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,sky_movies_programmes_60pc_or_1hr_total_annualised= cast(sky_movies_programmes_60pc_or_1hr_total as real) * (365/cast(total_days_with_viewing as real))
,other_programmes_03min_plus_total_annualised= cast(other_programmes_03min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,other_programmes_10min_plus_total_annualised= cast(other_programmes_10min_plus_total as real) * (365/cast(total_days_with_viewing as real))
,other_programmes_60pc_or_1hr_total_annualised= cast(other_programmes_60pc_or_1hr_total as real) * (365/cast(total_days_with_viewing as real))

from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_prog_watched_summary_final as b
on a.account_number =b.account_number

;
commit;
--select top 100 * from dbarnett.v250_prog_watched_summary_final;

--select * from sysobjects where name='v250_Account_profiling'

--select * from sysindexes where name='v250_Account_profiling'



/*
select account_number 
,viewing_duration
,programme_instance_duration
,viewing_duration/cast(programme_instance_duration as real)
,case when pay=1 and viewing_duration>=180 
then 1 else 0 end as fta_programmes_03min_plus
,case when pay=1 and viewing_duration>=600 
then 1 else 0 end as fta_programmes_10min_plus
,case when pay=1 and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end
as fta_programmes_60pc_or_1hr

from dbarnett.v250_account_viewing_by_service_key_and_programme_sk_daily as a
--from dbarnett.v250_viewing_by_account_and_channel as a  Changed to use new data created in Part 22b
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
left outer join sk_prod.Vespa_programme_schedule as c
ON a.dk_programme_instance_dim = c.pk_programme_instance_dim


*/










CREATE HG INDEX idx1 ON dbarnett.v250_account_viewing_by_service_key_and_programme_sk (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_account_viewing_by_service_key_and_programme_sk (service_key);

commit;
--select account_number , service_key, count(*) from 
---Create Deduped Table

--select * into dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped_old_version from dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped;
--commit;

--drop table dbarnett.v250_account_viewing_by_service_key; drop table dbarnett.v250_account_viewing_by_service_key_deduped_old_version; drop table dbarnett.v250_account_viewing_by_service_key_deduped;
 
select account_number
,service_key
,dk_programme_instance_dim
,sum(viewing_duration) as total_viewing_duration
,count(*) as records
into dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped
from dbarnett.v250_account_viewing_by_service_key_and_programme_sk
group by  account_number
,service_key
,dk_programme_instance_dim
;
commit;

select count(*), count(distinct account_number) from dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped
select count(*), count(distinct account_number) from dbarnett.v250_account_viewing_by_service_key_and_programme_sk


drop table dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped_old_version;

grant all on dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped to public;
commit;

drop table dbarnett.v250_account_viewing_by_service_key_and_programme_sk;
commit;
/*
select account_number
,sum(total_viewing_duration) as viewing_duration
,sum(total_sport_viewing_duration)
--into #account_viewing
from dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped
where account_number in ('621057736251','621377466233','620056878221','620011257461')
group by account_number
;

select account_number
,sum(total_viewing_duration) as viewing_duration
--into #account_viewing
from dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped_old_version
where account_number in ('621057736251','621377466233','620056878221','620011257461')
group by account_number
;
*/




--select top 100 *  from dbarnett.v250_account_viewing_by_service_key_and_programme_sk_deduped

--select top 100 *  from dbarnett.v250_account_viewing_by_service_key_and_programme_sk


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




