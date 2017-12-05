if object_id('data_quality_automated_daily_run') is not null drop procedure data_quality_automated_daily_run 
commit

go

create procedure data_quality_automated_daily_run 
(@RunID bigint)
as

declare @creator varchar(255)
declare @table_name varchar(255)
declare @var_sql varchar(8000)
declare @var_sql_final varchar(8000)
declare @broadcast_start_date_key int
declare @local_day_date_prog date
declare @local_year_prog char(4)
declare @local_month_prog char(2)
declare @local_day_date date
declare @local_year char(4)
declare @local_month char(2)
declare @month_year_1 char(6)
declare @month_year_2 char(6)
declare @local_year_month char(6)
declare @local_year_month_prog char(6)
declare @event_start_date_key int


--collect dates for Adsmart SLots process to be run for----
begin

--execute procedure to collect all daily stats for Adsmart reporting

/*
execute logger_add_event @RunID, 3, 'Adsmart DQ Static Checks Started'

select max(broadcast_start_date_key) broadcast_start_date_key into 
#tmp_adsmart_key
from sk_prod.fact_adsmart_slot_instance
 
set @broadcast_start_date_key = (select broadcast_start_date_key from #tmp_adsmart_key)

select distinct local_day_date into #tmp_adsmart_date from sk_prod.viq_date date1
where pk_datehour_dim = @broadcast_start_date_key

set @local_day_date = (select local_day_date from #tmp_adsmart_date)

select distinct local_day_date, local_year_month, local_year_value into #tmp_dqvm from 
sk_prod.viq_date date2
where 
date2.local_day_date between @local_day_date - 2 and @local_day_date - 1

SELECT local_day_date into #temp FROM #tmp_dqvm

------------------------------------------------------------------------------------------

exec logger_create_run 'Data_Quality_Checks', 'Latest Run', @RunID output

--------------------------------------------A02 - Define the loop which will be cycled through to execute the Analytical procedure-------------------------

-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @local_day_date  = local_day_date from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where local_day_date = @local_day_date  -- delete that uid from the temp table

  -- Do something with the uid you have
--  update customers set name = 'Joe Shmoe' where uid = @uid

set @local_year_month = (select local_year_month from #tmp_dqvm
where local_day_date = @local_day_date)

exec data_quality_adsmart_day_processing 'ADSMART_DATA_QUALITY', @RunID, @local_day_date


end


execute logger_add_event @RunID, 3, 'Adsmart DQ Static Checks Ended'
*/
 --collect dates for Linear Programme Viewing process to be run for----

--select from the current view the most recent date with over 12000000 records
--selecting from the dq_prog_viewed_current view, which contains data for the current and previous month.


select max(event_date) event_date into #tmp_prog_max_date
from 
(Select dk_event_start_datehour_dim/100 event_date, count(1) rec_total
from sk_prod.vespa_dp_prog_viewed_current
group by dk_event_start_datehour_dim/100 ) a
where rec_total > 12000000 

--selects the event date into a variable

set @event_start_date_key = (select event_date from #tmp_prog_max_date)

--find the distinct utc date for the event variable above

select distinct utc_day_date into #tmp_viewing_date from sk_prod.viq_date date1
where pk_datehour_dim/100 = @event_start_date_key

---local date - utc_date

set @local_day_date_prog = (select utc_day_date from #tmp_viewing_date)


--select date, month and year from the viq_date table where the local date is -1 to local date

select distinct utc_day_date local_day_date, utc_year_month local_year_month, 
utc_year_value local_year_value into #tmp_dqvm_prog from 
sk_prod.viq_date date2
where 
date2.utc_day_date between @local_day_date_prog - 1 and @local_day_date_prog


--place in temp variable

SELECT local_day_date into #temp_prog FROM #tmp_dqvm_prog


--


while exists (select 1 from #temp_prog)
begin
  set rowcount 1
  select @local_day_date_prog  = local_day_date from #temp_prog -- pull one uid from the temp table
  set rowcount 0
  delete from #temp_prog where local_day_date = @local_day_date_prog  -- delete that uid from the temp table

--get values for date, month and year


set @local_year_month_prog = (select local_year_month from #tmp_dqvm_prog
where local_day_date = @local_day_date_prog)

--execute the programme reporting procedure for each of the days that are valid

exec data_quality_data_processing_month 'null',@RunID,@local_year_month_prog,@local_day_date_prog

end

--programme reporting complete

--log adsmart reporting

execute logger_add_event @RunID, 3, 'Adsmart DQ Daily Checks Started'

--execute adsmart reporting

exec data_quality_BI_checks_reporting

--log adsmart complete

execute logger_add_event @RunID, 3, 'Adsmart DQ Daily Checks Ended'

--collect max dates for Adsmart SLots process to be run for----

end

commit

go

grant execute on data_quality_automated_daily_run to vespa_group_low_security, sk_prodreg, buxceys, kinnairt