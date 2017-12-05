
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data_Quality_Household_metrics_run
**
**
**  
** Refer also to:
**
**
** Code sections:
**      Part A: A01 - Populate initial table with base and some of key variables
**
** Things done:
**
**
******************************************************************************/


if object_id('data_quality_household_metrics_run') is not null drop procedure data_quality_household_metrics_run
commit

go

create procedure data_quality_household_metrics_run
(@RunID bigint, @batch_date date)
as
begin

declare @metric_short_name varchar(255)
declare @sql_Stmt varchar(8000)

create table #sql_load
(metric_short_name varchar(255),
sql_stmt varchar(8000))

create table #tmp_household_records
(table_name varchar(25),
column_name varchar(100),
text_value varchar(100),
num_recs integer,
percent decimal)

delete from #tmp_household_records

delete from #sql_load
insert into #sql_load
select   'DEMO_HH_'||creator||'_'||tname||'_'||cname||'_percent_check' metric_short_name,
'insert into #tmp_household_records select '''||tname||''','''||cname||''', cast(' ||cname|| ' as varchar(100)), count(1) count,
(1.0 * count(1)/(select count(1) from '||creator||'.'||tname||') * 100)  percent from sk_prod.vespa_household group by '||cname|| '' sql_stmt
from sys.syscolumns 
where lower(tname) = 'vespa_household'
and creator = 'sk_prod'
union all
select   'DEMO_HH_'||creator||'_'||tname||'_'||cname||'_percent_check' metric_short_name,
'insert into #tmp_household_records select '''||tname||''','''||cname||''', cast(' ||cname|| ' as varchar(100)), count(1) count,
(1.0 * count(1)/(select count(1) from '||creator||'.'||tname||') * 100)  percent from sk_prod.adsmart group by '||cname|| ''
from sys.syscolumns 
where lower(tname) = 'adsmart'
and creator = 'sk_prod'
union all
select   'DEMO_HH_'||creator||'_'||tname||'_'||cname||'_percent_check' metric_short_name,
'insert into #tmp_household_records select '''||tname||''','''||cname||''', cast(' ||cname|| ' as varchar(100)), count(1) count,
(1.0 * count(1)/(select count(1) from '||creator||'.'||tname||' where version = 0) * 100)  percent from sk_prod.viq_household where version = 0 group by '||cname|| ''
from sys.syscolumns 
where lower(tname) = 'viq_household'
and creator = 'sk_prod'

delete from #sql_load
where upper(metric_short_name) like '%ACCOUNT_NUMBER%'

delete from #sql_load
where upper(metric_short_name) like '%HOUSEHOLD_KEY%'

delete from #sql_load
where upper(metric_short_name) like '%HOUSEHOLD_ID%'

delete from #sql_load
where upper(metric_short_name) like '%CB_ADDRESS%'

delete from #sql_load
where upper(metric_short_name) like '%CB_KEY%'

delete from #sql_load
where upper(metric_short_name) like '%DELTA%'

delete from #sql_load
where upper(metric_short_name) like '%DATE_FROM%'

delete from #sql_load
where upper(metric_short_name) like '%DATE_TO%'

delete from #sql_load
where upper(metric_short_name) like '%VERSION%'

delete from #sql_load
where upper(metric_short_name) like '%SRC_SYSTEM_ID%'

select metric_short_name into #temp from #sql_load -- you can use a where condition here
------------------------------------------A03 - Loop creation to execute each metric at a time-----------------------------------------
-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @metric_short_name = metric_short_name from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where metric_short_name = @metric_short_name -- delete that uid from the temp table

  -- Do something with the uid you have

set @sql_stmt = (select sql_stmt from #sql_load where metric_short_name = @metric_short_name)

execute (@sql_stmt)

end

insert into data_quality_vespa_repository
(dq_run_id, viewing_data_date, dq_vm_id, metric_result,  metric_tolerance_amber, 
metric_tolerance_red, metric_rag, load_timestamp)
select @RunID,@batch_date, 
b.dq_vm_id, a.percent, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check (a.percent, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) RAG, getdate() from 
(select a.* , 
replace(replace('demo_hh_'||lower(a.table_name)||'_'||lower(a.column_name)||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(a.text_value),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')||'_percent_check' metric_short_name
from #tmp_household_records a) a,
data_quality_vespa_metrics b
where a.metric_short_name = b.metric_short_name

commit

end

grant execute on data_quality_household_metrics_run to vespa_group_low_security, sk_prodreg, buxceys, kinnairt