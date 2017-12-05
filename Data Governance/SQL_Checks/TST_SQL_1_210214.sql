
create procedure data_quality_automated_run
as

begin
create variable @RunID bigint;

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

exec kinnairt.data_quality_adsmart_day_processing 'ADSMART_DATA_QUALITY', @RunID, @local_day_date


end

 --collect dates for Linear Programme Viewing process to be run for----


select max(event_date) event_date into #tmp_prog_max_date
from 
(Select dk_event_start_datehour_dim/100 event_date, count(1) rec_total
from sk_prod.vespa_dp_prog_viewed_current
group by dk_event_start_datehour_dim/100 ) a
where rec_total > 12000000 

set @event_start_date_key = (select event_date from #tmp_prog_max_date)

select distinct local_day_date into #tmp_viewing_date from sk_prod.viq_date date1
where pk_datehour_dim/100 = @event_start_date_key

set @local_day_date_prog = (select local_day_date from #tmp_viewing_date)

select distinct local_day_date, local_year_month, local_year_value into #tmp_dqvm_prog from 
sk_prod.viq_date date2
where 
date2.local_day_date between @local_day_date_prog - 1 and @local_day_date_prog

SELECT local_day_date into #temp_prog FROM #tmp_dqvm_prog

while exists (select 1 from #temp_prog)
begin
  set rowcount 1
  select @local_day_date_prog  = local_day_date from #temp_prog -- pull one uid from the temp table
  set rowcount 0
  delete from #temp_prog where local_day_date = @local_day_date_prog  -- delete that uid from the temp table


set @local_year_month_prog = (select local_year_month from #tmp_dqvm_prog
where local_day_date = @local_day_date_prog)


exec kinnairt.data_quality_data_processing_month 'null',@RunID,@local_year_month_prog,@local_day_date_prog


end

commit

select min(pk_datehour_dim)min_broadcast_date_hour, max(pk_datehour_dim) max_broadcast_date_hour 
into #tmp_date_hours
from 
sk_prod.viq_date
where local_day_date = @analysis_date_current


drop table data_quality_slots_daily_reporting

create table data_quality_slots_daily_reporting
(dq_sdr_id bigint identity,
date_type varchar(20),
batch_date date,
date_value date,
slots_totals int,
actual_impressions decimal (20,4),
segments_totals int, 
households_totals int,
campaigns_totals int,
load_timestamp datetime)


------
commit


UPDATE          AdSmart
SET             Affluence_group =  case         WHEN H_AFFLUENCE IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN H_AFFLUENCE IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN H_AFFLUENCE IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN H_AFFLUENCE IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN H_AFFLUENCE IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN H_AFFLUENCE IN ('15','16','17')       THEN 'F) High'
                                                WHEN H_AFFLUENCE IN ('18','19')            THEN 'G) Very High' END
;


select top 10 * from sk_prod.vespa_household

select distinct cb_key_household,account_number, affluence_bands
into tst_household_affluence
from sk_prod.vespa_household

commit

select a.cb_key_household, a.h_affluence_v2,
Affluence_group =  case         WHEN a.H_AFFLUENCE_v2 IN ('00','01','02')       THEN 'Very Low'
                                                WHEN a.H_AFFLUENCE_v2 IN ('03','04', '05')      THEN 'Low'
                                                WHEN a.H_AFFLUENCE_v2 IN ('06','07','08')       THEN 'Mid Low'
                                                WHEN a.H_AFFLUENCE_v2 IN ('09','10','11')       THEN 'Mid'
                                                WHEN a.H_AFFLUENCE_v2 IN ('12','13','14')       THEN 'Mid High'
                                                WHEN a.H_AFFLUENCE_v2 IN ('15','16','17')       THEN 'High'
                                                WHEN a.H_AFFLUENCE_v2 IN ('18','19')            THEN 'Very High' END ,
b.cb_key_household cb_key_household_old, b.account_number, b.affluence_bands
into tst_household_affuence_new 
from sk_prodreg.EXPERIAN_CONSUMERVIEW a,
tst_household_affluence b
where a.cb_key_household = b.cb_key_household



drop table tst_adsmart_affluence

select 


select a.cb_key_household, a.h_affluence_v2,
Affluence_group =  case         WHEN a.H_AFFLUENCE_v2 IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN a.H_AFFLUENCE_v2 IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN a.H_AFFLUENCE_v2 IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN a.H_AFFLUENCE_v2 IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN a.H_AFFLUENCE_v2 IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN a.H_AFFLUENCE_v2 IN ('15','16','17')       THEN 'F) High'
                                                WHEN a.H_AFFLUENCE_v2 IN ('18','19')            THEN 'G) Very High' END ,
b.cb_key_household cb_key_household_old, b.account_number, b.h_affluence
into tst_adsmart_affuence_new 
from sk_prodreg.EXPERIAN_CONSUMERVIEW a,
tst_adsmart_affluence b
where a.cb_key_household = b.cb_key_household

commit

select count(distinct account_number) from tst_adsmart_affuence_new 
where upper (substr(affluence_group,4)) !=  upper(h_affluence)