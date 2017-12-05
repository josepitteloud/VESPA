
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data Quality Metrics Collection UAT
**
** This script has the big procedure which goes through the various metrics that
** we want to report on to check the integrity of the data that is coming in
** from CBI into Olive.  It comprises of basic metric checks in a separate procedure
** and specific metrics that we want to collect for analytical purposes
**
** Refer also to:
**
**  
**      Part A: Daily Live Viewing Household Average Pre Capping
**		A01 - Daily Live Viewing Household Average Pre Capping
**		A02 - Collect Metric Data from Metrics table
**		A03 - Logic to generate Metric Benchmark result
**		A04 - Generate RAG status
**		A05 - insert into Vespa Repository Table
**		A06 - drop Temp Tables
**
**	Part B: Daily recorded Viewing Household Average Pre Capping
**		B01 - Daily recorded Viewing Household Average Pre Capping
**		B02 - Collect Metric Data from Metrics table
**		B03 - Logic to generate Metric Benchmark result
**		B04 - Generate RAG status
**		B05 - insert into Vespa Repository Table
**		B06 - drop Temp Tables
**
**	Part C: Daily Total Viewing Household Pre Capping
**		C01 - Daily Total Viewing Household Pre Capping
**		C02 - Collect Metric Data from Metrics table
**		C03 - Logic to generate Metric Benchmark result
**		C04 - Generate RAG status
**		C05 - insert into Vespa Repository Table
**		C06 - drop Temp Tables
**
**	Part D: Daily Total Viewing Household Post Capping
**		D01 - Daily Total Viewing Household Post Capping
**		D02 - Collect Metric Data from Metrics table
**		D03 - Logic to generate Metric Benchmark result
**		D04 - Generate RAG status
**		D05 - insert into Vespa Repository Table
**		D06 - drop Temp Tables
**
**	Part E: Daily Live Viewing Household Post Capping
**		E01 - Daily Live Viewing Household Post Capping
**		E02 - Collect Metric Data from Metrics table
**		E03 - Logic to generate Metric Benchmark result
**		E04 - Generate RAG status
**		E05 - insert into Vespa Repository Table
**		E06 - drop Temp Tables
**
**	Part F: Daily recorded Viewing Household Post Capping
**		F01 - Daily recorded Viewing Household Post Capping
**		F02 - Collect Metric Data from Metrics table
**		F03 - Logic to generate Metric Benchmark result
**		F04 - Generate RAG status
**		F05 - insert into Vespa Repository Table
**		F06 - drop Temp Tables
**
**	Part G: Daily Live Viewing STB Post Capping
**		H01 - Daily Live Viewing STB Post Capping
**		H02 - Collect Metric Data from Metrics table
**		H03 - Logic to generate Metric Benchmark result
**		H04 - Generate RAG status
**		H05 - insert into Vespa Repository Table
**		H06 - drop Temp Tables
**
**	Part H: Daily Recorded Viewing STB Post Capping
**		H01 - Daily Recorded Viewing STB Pre Capping
**		H02 - Collect Metric Data from Metrics table
**		H03 - Logic to generate Metric Benchmark result
**		H04 - Generate RAG status
**		H05 - insert into Vespa Repository Table
**		H06 - drop Temp Tables
**
**	Part J: Daily Total Viewing STB Pre Capping
**		J01 - Daily Total Viewing STB Pre Capping
**		J02 - Collect Metric Data from Metrics table
**		J03 - Logic to generate Metric Benchmark result
**		J04 - Generate RAG status
**		J05 - insert into Vespa Repository Table
**
**	Part K: Daily Total Viewing STB Post Capping
**		K01 - Daily Total Viewing STB Post Capping
**		K02 - Collect Metric Data from Metrics table
**		K03 - Logic to generate Metric Benchmark result
**		K04 - Generate RAG status
**		K05 - insert into Vespa Repository Table
**		K06 - Drop Temp Tables
**
**	Part L: Daily Live Viewing STB Post Capping
**		L01 - Daily Live Viewing STB Post Capping
**		L02 - Collect Metric Data from Metrics table
**		L03 - Logic to generate Metric Benchmark result
**		L04 - Generate RAG status
**		L05 - insert into Vespa Repository Table
**		L06 - Drop Temp Tables
**
**	Part M: Daily recorded Viewing STB Post Capping
**		M01 - Daily recorded Viewing STB Post Capping
**		M02 - Collect Metric Data from Metrics table
**		M03 - Logic to generate Metric Benchmark result
**		M04 - Generate RAG status
**		M05 - insert into Vespa Repository Table
**		M06 - Drop Temp Tables
**
**	Part N: total count of events per viewing day
**		N01 - total count of events per viewing day
**		N02 - Collect Metric Data from Metrics table
**		N03 - Logic to generate Metric Benchmark result
**		N04 - Generate RAG status
**		N05 - insert into Vespa Repository Table
**		N06 - Drop Temp Tables
**
**	Part P: total count of instances per viewing day
**		P01 - total count of instances per viewing day
**		P02 - Collect Metric Data from Metrics table
**		P03 - Logic to generate Metric Benchmark result
**		P04 - Generate RAG status
**		P05 - insert into Vespa Repository Table
**		P06 - Drop Temp Tables
**
**
**	Part R: average impacts per viewing day 
**		R01 - average impacts per viewing day 
**		R02 - Collect Metric Data from Metrics table
**		R03 - Logic to generate Metric Benchmark result
**		R04 - Generate RAG status
**		R05 - insert into Vespa Repository Table
**		R06 - Drop Temp Tables
**
**	Part S: total impacts per viewing day
**		S01 - total impacts per viewing day 
**		S02 - Collect Metric Data from Metrics table
**		S03 - Logic to generate Metric Benchmark result
**		S04 - Generate RAG status
**		S05 - insert into Vespa Repository Table
**		S06 - Drop Temp Tables
**
**	Part T: Average Events per HH per viewing day
**		T01 - Average Events per HH per viewing day
**		T02 - Collect Metric Data from Metrics table
**		T03 - Logic to generate Metric Benchmark result
**		T04 - Generate RAG status
**		T05 - insert into Vespa Repository Table
**		T06 - Drop Temp Tables
**
**	Part U: Average instances per HH per viewing day
**		U01 - Average Events per HH per viewing day
**		U02 - Collect Metric Data from Metrics table
**		U03 - Logic to generate Metric Benchmark result
**		U04 - Generate RAG status
**		U05 - insert into Vespa Repository Table
**		U06 - Drop Temp Tables
**
**
**	Part V: number of HH dialling back on a Daily Basis
**		V01 - number of HH dialling back on a Daily Basis
**		V02 - Collect Metric Data from Metrics table
**		V03 - Logic to generate Metric Benchmark result
**		V04 - Generate RAG status
**		V05 - insert into Vespa Repository Table
**
**
**	Part W: number of HH not dialling back on a Daily Basis
**		W01 - number of HH not dialling back on a Daily Basis
**		W02 - Collect Metric Data from Metrics table
**		W03 - Logic to generate Metric Benchmark result
**		W04 - Generate RAG status
**		W05 - insert into Vespa Repository Table
**
** Things done:
**
**
******************************************************************************/

-- We've also got a few cases where *everything* ends up with the capped flag set;
-- how are we establishing the correct caps when we have no universe of uncapped
-- to choose from?

if object_id('Data_Quality_Metrics_Collection_UAT') is not null drop procedure Data_Quality_Metrics_Collection_UAT
commit

go

create procedure Data_Quality_Metrics_Collection_UAT
     @load_date       date = NULL     -- Date of daily table caps to cache
    ,@dq_run_id      bigint = NULL   -- Logger ID (so all builds end up in same queue)
    ,@Metric_to_Measure	varchar(100)
as
begin

--test variables in case you are not submitting any of these, though you will probably need
--to as the paricular metric involved will need to be investigated

--set @dq_run_id = 99
--set @load_date = getdate()

-----------------------------------------------daily_viewing_by_account_pre_capping-----------------------------------------------------

----------------------------------------A01 - Daily Live Viewing Household Average Pre Capping------------------------------------------

if @Metric_to_Measure = 'daily_viewing_hh_avg_precapped_live' 

begin


---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------A02 - Collect Metric Data from Metrics table------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table

----------------------------------------A03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,account_number,event_start_date_time_utc,event_end_date_time_utc, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
data_quality_dp_data_audit
where panel_id = 12
and upper(live_recorded) = 'LIVE'
---woo hoo.  Now time to calculate the duration differences in seconds and hours to 3 decimal places
---for each account

select viewing_date
,account_number
,live_recorded
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) / 3600 average_hours
into #uncapped_daily_duration
FROM
#account_event_uncapped
where panel_id = 12
group by viewing_date,account_number,live_recorded,metric_short_name

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking


select dq_vm_id,viewing_date,live_recorded,cast(sum(average_hours)/count(distinct account_number) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #uncapped_daily_comparison
from #uncapped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,live_recorded,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------A04 - Generate RAG status------------------------------------------

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------A05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#uncapped_daily_comparison_final

commit

-----------------------------------------A06 - Drop Temp Tables--------------------------------------------------------------------

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

drop table #uncapped_daily_comparison_final
drop table #uncapped_daily_comparison
drop table #uncapped_daily_duration
drop table #data_metrics_detail
drop table #account_event_uncapped

end

-----------------------------now for recorded pre capping-------------------------------------------

----------------------------------------B01 - Daily recorded Viewing Household Average Pre Capping------------------------------------------


if @Metric_to_Measure = 'daily_viewing_hh_avg_precapped_rec' 

begin

----------------------------------------B02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table

----------------------------------------B03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,account_number,event_start_date_time_utc,event_end_date_time_utc, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
data_quality_dp_data_audit
where panel_id = 12
and upper(live_recorded) = 'RECORDED'
---woo hoo.  Now time to calculate the duration differences in seconds and hours to 3 decimal places
---for each account

select viewing_date
,account_number
,live_recorded
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) / 3600 average_hours
into #uncapped_daily_duration
FROM
#account_event_uncapped
where panel_id = 12
group by viewing_date,account_number,live_recorded,metric_short_name

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking

select dq_vm_id,viewing_date,live_recorded,cast(sum(average_hours)/count(distinct account_number) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #uncapped_daily_comparison
from #uncapped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,live_recorded,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------B04 - Generate RAG status------------------------------------------

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------B05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#uncapped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

-----------------------------------------B06 - Drop Temp Tables--------------------------------------------------------------------

drop table #uncapped_daily_comparison_final
drop table #uncapped_daily_comparison
drop table #uncapped_daily_duration
drop table #data_metrics_detail
drop table #account_event_uncapped

end


--------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------

--3
-----daily_viewing pre capping totals

---

--need to pass this in but hey setting it this week could work. will see

if @Metric_to_Measure = 'daily_viewing_hh_avg_precapped_total' 

--------------------------------------------------C01 - Daily Total Viewing Household Pre Capping--------------------------------

begin


---these should be set as part of the procedure parameters.  Here for testing purposes currently

--set @dq_run_id = 99
--set @load_date = getdate()


---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------C02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--lets get the info needed for the total test from the data audit table

----------------------------------------C03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,account_number,event_start_date_time_utc,event_end_date_time_utc, panel_id,
@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
data_quality_dp_data_audit
where panel_id = 12

--lets get those uncapped viewing times

select viewing_date
,account_number
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) / 3600 average_hours
into #uncapped_daily_duration
FROM
#account_event_uncapped
where panel_id = 12
group by viewing_date,account_number,metric_short_name

--now we have the times lets get the average

select dq_vm_id,viewing_date,cast(sum(average_hours)/count(distinct account_number) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #uncapped_daily_comparison
from #uncapped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

--now we have the average lets sort out that benchmarking

----------------------------------------C04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

--now we have the all we need enter the results in the repository

----------------------------------------C05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#uncapped_daily_comparison_final

commit

--drop those temp tables.  remember you must always drop those temp tables.

-----------------------------------------C06 - Drop Temp Tables--------------------------------------------------------------------


drop table #uncapped_daily_comparison_final
drop table #uncapped_daily_comparison
drop table #uncapped_daily_duration
drop table #data_metrics_detail
drop table #account_event_uncapped

end

-----------------------------------------------daily_viewing_by_account_pre_capping_done-----------------------------------------------------

-----------------------------------------------daily_viewing_by_account_post_capping start-----------------------------------------


--4)


------------------------------------------D01 - Daily Total Viewing Household Post Capping--------------------------------------------


if @Metric_to_Measure = 'daily_viewing_hh_avg_postcapped_total' 

begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------D02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--lets get the info needed for the total test from the data audit table


----------------------------------------D03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,account_number,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
@Metric_to_Measure metric_short_name
into #account_event_capped
from
data_quality_dp_data_audit
where panel_id = 12

--lets get those capped viewing times


select viewing_date
,account_number
    , sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) / 3600 average_hours
,metric_short_name
into #capped_daily_duration
FROM
#account_event_capped
where panel_id = 12
group by viewing_date,account_number,metric_short_name


--now we have the times lets get the average

select dq_vm_id,viewing_date,cast(sum(average_hours)/count(distinct account_number) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #capped_daily_comparison
from #capped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

--now we have the average lets sort out that benchmarking

----------------------------------------D04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

--now we have the all we need enter the results in the repository

----------------------------------------D05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#capped_daily_comparison_final

commit

-----------------------------------------D06 - Drop Temp Tables--------------------------------------------------------------------


---drop those pesky temp tables

drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped

end

------------------------------------------------------------------------------------------

------------------------------------------E01 - Daily Live Viewing Household Post Capping--------------------------------------------


if @Metric_to_Measure = 'daily_viewing_hh_avg_postcapped_live' 

begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------E02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


----------------------------------------E03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,account_number,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_capped
from
data_quality_dp_data_audit
where panel_id = 12
and upper(live_recorded) = 'LIVE'

---woo hoo.  Now time to calculate the duration differences in seconds and hours to 3 decimal places
---for each account

select viewing_date
,account_number
,live_recorded
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) / 3600 average_hours
into #capped_daily_duration
FROM
#account_event_capped
where panel_id = 12
group by viewing_date,account_number,live_recorded,metric_short_name

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking

select dq_vm_id,viewing_date,live_recorded,cast(sum(average_hours)/count(distinct account_number) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #capped_daily_comparison
from #capped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,live_recorded,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------E04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------E05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#capped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

-----------------------------------------E06 - Drop Temp Tables--------------------------------------------------------------------


drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped

end


-----------------------------------------------post_capping_live done--------------------------------------

-----------------------------------------------post_capping_rec start-----------------------------------------


----------------------------------------------------F01 - Daily recorded Viewing Household Post Capping----------------------------------------------------

if @Metric_to_Measure = 'daily_viewing_hh_avg_postcapped_rec' 
begin
---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------F02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table

----------------------------------------F03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,account_number,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_capped
from
data_quality_dp_data_audit
where panel_id = 12
and UPPER(LIVE_RECORDED) = 'RECORDED'


---woo hoo.  Now time to calculate the duration differences in seconds and hours to 3 decimal places
---for each account

select viewing_date
,account_number
,live_recorded
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) / 3600 average_hours
into #capped_daily_duration
FROM
#account_event_capped
where panel_id = 12
group by viewing_date,account_number,live_recorded,metric_short_name

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking

select dq_vm_id,viewing_date,live_recorded,cast(sum(average_hours)/count(distinct account_number) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #capped_daily_comparison
from #capped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,live_recorded,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------F04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------F05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#capped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

-----------------------------------------F06 - Drop Temp Tables--------------------------------------------------------------------


drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped

end


---------------------------------------------------------------------------------------------------------------
----metrics 12 and 13



---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------------------G01 - Daily Live Viewing STB Pre Capping----------------------------------------------------


if @Metric_to_Measure = 'daily_viewing_stb_avg_precapped_live'
begin

----------------------------------------G02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table

----------------------------------------G03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,subscriber_id,event_start_date_time_utc,event_end_date_time_utc, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
data_quality_dp_data_audit
where panel_id = 12
and upper(live_recorded) = 'LIVE'

---woo hoo.  Now time to calculate the duration differences in seconds and hours to 3 decimal places
---for each account

select viewing_date
,subscriber_id
,live_recorded
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) / 3600 average_hours
into #uncapped_daily_duration
FROM
#account_event_uncapped
where panel_id = 12
group by viewing_date,subscriber_id,live_recorded,metric_short_name

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking

select dq_vm_id,viewing_date,live_recorded,cast(sum(average_hours)/count(distinct subscriber_id) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #uncapped_daily_comparison
from #uncapped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,live_recorded,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------G04 - Generate RAG status------------------------------------------



select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------G05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#uncapped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

-----------------------------------------G06 - Drop Temp Tables--------------------------------------------------------------------


drop table #uncapped_daily_comparison_final
drop table #uncapped_daily_comparison
drop table #uncapped_daily_duration
drop table #data_metrics_detail
drop table #account_event_uncapped


end

---------------------------------------------sub_pre_capping live done------------------------------------------


---------------------------------------------------------------------------------------------------------------
----metrics 12 and 13



---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------------------H01 - Daily Recorded Viewing STB Pre Capping----------------------------------------------------


if @Metric_to_Measure = 'daily_viewing_stb_avg_precapped_rec'
begin

----------------------------------------H02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table

----------------------------------------H03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,subscriber_id,event_start_date_time_utc,event_end_date_time_utc, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
data_quality_dp_data_audit
where panel_id = 12
and upper(live_recorded) = 'RECORDED'

---woo hoo.  Now time to calculate the duration differences in seconds and hours to 3 decimal places
---for each account

select viewing_date
,subscriber_id
,live_recorded
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) / 3600 average_hours
into #uncapped_daily_duration
FROM
#account_event_uncapped
where panel_id = 12
group by viewing_date,subscriber_id,live_recorded,metric_short_name

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking

select dq_vm_id,viewing_date,live_recorded,cast(sum(average_hours)/count(distinct subscriber_id) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #uncapped_daily_comparison
from #uncapped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,live_recorded,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------H04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------H05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#uncapped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

-----------------------------------------H06 - Drop Temp Tables--------------------------------------------------------------------


drop table #uncapped_daily_comparison_final
drop table #uncapped_daily_comparison
drop table #uncapped_daily_duration
drop table #data_metrics_detail
drop table #account_event_uncapped


end


-----------------------------------------SUB_ID PRE_CAPPING DONE-------------------------------------


----------------------------------------------------------------------------------------------------------


-----------------------------------------SUB_ID TOTAL VIEWING START-----------------------------------------


----------------------------------------------------J01 - Daily Total Viewing STB Pre Capping----------------------------------------------------


if @Metric_to_Measure = 'daily_viewing_stb_avg_precapped_total'
begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------J02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--lets get the info needed for the total test from the data audit table

----------------------------------------J03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,subscriber_id,event_start_date_time_utc,event_end_date_time_utc, panel_id,
@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
data_quality_dp_data_audit
where panel_id = 12

--lets get those uncapped viewing times

select viewing_date
,subscriber_id
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) / 3600 average_hours
into #uncapped_daily_duration
FROM
#account_event_uncapped
where panel_id = 12
group by viewing_date,subscriber_id,metric_short_name

--now we have the times lets get the average

select dq_vm_id,viewing_date,cast(sum(average_hours)/count(distinct subscriber_id) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #uncapped_daily_comparison
from #uncapped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

--now we have the average lets sort out that benchmarking

----------------------------------------J04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

--now we have the all we need enter the results in the repository

----------------------------------------J05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#uncapped_daily_comparison_final

commit

end

--------------------------------------------sub_id total viewing pre capping done-------------------------------



----------------------------------------------------------------------------------------------------------------

-------------------------------------------------------K01 - Daily Total Viewing STB Post Capping--------------------------------------------

if @Metric_to_Measure = 'daily_viewing_stb_avg_postcapped_total'
begin



---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------K02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--lets get the info needed for the total test from the data audit table

----------------------------------------K03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,subscriber_id,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
@Metric_to_Measure metric_short_name
into #account_event_capped
from
data_quality_dp_data_audit
where panel_id = 12

--lets get those capped viewing times

select viewing_date
,subscriber_id
    , sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) / 3600 average_hours
,metric_short_name
into #capped_daily_duration
FROM
#account_event_capped
where panel_id = 12
group by viewing_date,subscriber_id,metric_short_name


--now we have the times lets get the average

select dq_vm_id,viewing_date,cast(sum(average_hours)/count(distinct subscriber_id) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #capped_daily_comparison
from #capped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

--now we have the average lets sort out that benchmarking

----------------------------------------K04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

--now we have the all we need enter the results in the repository

----------------------------------------K05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#capped_daily_comparison_final

commit


-----------------------------------------K06 - Drop Temp Tables--------------------------------------------------------------------


---drop those pesky temp tables

drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped

end

------------------------------------------------------------------------------------------


-------------------------------------------------------L01 - Daily Live Viewing STB Post Capping--------------------------------------------


if @Metric_to_Measure = 'daily_viewing_stb_avg_postcapped_live'
begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------L02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


----------------------------------------L03 - Logic to generate Metric Benchmark result------------------------------------------

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


select distinct viewing_date,subscriber_id,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_capped
from
data_quality_dp_data_audit
where panel_id = 12
and upper(live_recorded) = 'LIVE'

---woo hoo.  Now time to calculate the duration differences in seconds and hours to 3 decimal places
---for each account

select viewing_date
,subscriber_id
,live_recorded
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) / 3600 average_hours
into #capped_daily_duration
FROM
#account_event_capped
where panel_id = 12
group by viewing_date,subscriber_id,live_recorded,metric_short_name

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking

select dq_vm_id,viewing_date,live_recorded,cast(sum(average_hours)/count(distinct subscriber_id) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #capped_daily_comparison
from #capped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,live_recorded,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------L04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------L05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#capped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

-----------------------------------------L06 - Drop Temp Tables--------------------------------------------------------------------


drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped


end


---------------------------------------------------sub_id live post capping done------------------------------------


---------------------------------------------------sub_id rec post capping start----------------------------


----------------------------------------------------M01 - Daily recorded Viewing STB Post Capping------------------------------

if @Metric_to_Measure = 'daily_viewing_stb_avg_postcapped_rec'
begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

----------------------------------------M02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


----------------------------------------M03 - Logic to generate Metric Benchmark result------------------------------------------


select distinct viewing_date,subscriber_id,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_capped
from
data_quality_dp_data_audit
where panel_id = 12
and upper(live_recorded) = 'RECORDED'

---woo hoo.  Now time to calculate the duration differences in seconds and hours to 3 decimal places
---for each account

select viewing_date
,subscriber_id
,live_recorded
,metric_short_name
    , sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) / 3600 average_hours
into #capped_daily_duration
FROM
#account_event_capped
where panel_id = 12
group by viewing_date,subscriber_id,live_recorded,metric_short_name

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking

select dq_vm_id,viewing_date,live_recorded,cast(sum(average_hours)/count(distinct subscriber_id) as decimal (6,3)) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #capped_daily_comparison
from #capped_daily_duration daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,viewing_date,live_recorded,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------M04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------M05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#capped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions


-----------------------------------------M06 - Drop Temp Tables--------------------------------------------------------------------


drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped


end


-------------------------------------------------------end sub id post capping rec routine--------------------


-------------------------------------------------------start 008 total count of events per vieiwng day

-------------------------------------------------------N01 - total count of events per viewing day--------------------------------------


if @Metric_to_Measure = 'daily_viewing_total_no_of_events'
begin

declare @viewing_date date

set @viewing_date = (select max(viewing_date) from data_quality_dp_data_audit)

----------------------------------------N02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


----------------------------------------N03 - Logic to generate Metric Benchmark result------------------------------------------


------------------------------------------------------------------------
--To get the sum of records from sub query Event_Count
------------------------------------------------------------------------
SELECT sum(Event_Count.Count) as Events_Total, 
CAST(event_start_date_time_utc as date) as Event_Date,
@Metric_to_Measure metric_short_name
into #tmp_total_events
FROM(
------------------------------------------------------------------------
--To get a count of records produced from subscriber_id and event_start_date_time_utc
------------------------------------------------------------------------
  SELECT count(1) over (partition by subscriber_id, event_start_date_time_utc) as Count
    ,subscriber_id, event_start_date_time_utc
  FROM data_quality_dp_data_audit
  WHERE   panel_id IN (12) -- ONLY PANEL 12 REFERENCE
  AND viewing_date = @viewing_date
  GROUP BY subscriber_id, event_start_date_time_utc
) as Event_Count
GROUP BY CAST(event_start_date_time_utc as date)


select dq_vm_id,event_date,Events_Total metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #tmp_total_events_detail
from #tmp_total_events tte, #data_metrics_detail dm_det
where tte.metric_short_name = dm_det.metric_short_name

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!


----------------------------------------N04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #tmp_total_events_detail_final
from #tmp_total_events_detail a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------N05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,event_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_total_events_detail_final

commit

-----------------------------------------N06 - Drop Temp Tables--------------------------------------------------------------------


drop table #tmp_total_events
drop table #tmp_total_events_detail_final
drop table #tmp_total_events_detail



end

----------------------------------------------end total events collection------------------------------------------------



---------------------------------------------start total instances collection------------------------------------


----------------------------------------------P01 - total count of instances per viewing day-------------------------------------

if @Metric_to_Measure = 'daily_viewing_total_no_of_instances'
begin

declare @viewing_date date

set @viewing_date = (select max(viewing_date) from data_quality_dp_data_audit)

--------------------------------------------------------------------------------------


----------------------------------------P02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


----------------------------------------P03 - Logic to generate Metric Benchmark result------------------------------------------


SELECT viewing_date, count(1) no_of_instances,@Metric_to_Measure metric_short_name
into #tmp_total_instances
from data_quality_dp_data_audit
where viewing_date = @viewing_date
group by viewing_date

select dq_vm_id,viewing_date,no_of_instances metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #tmp_total_instances_detail
from #tmp_total_instances tti, #data_metrics_detail dm_det
where tti.metric_short_name = dm_det.metric_short_name

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------P04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #tmp_total_instances_detail_final
from #tmp_total_instances_detail a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------P05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_total_instances_detail_final

commit

-----------------------------------------P06 - Drop Temp Tables--------------------------------------------------------------------


drop table #tmp_total_instances
drop table #tmp_total_instances_detail
drop table #tmp_total_instances_detail_final


end

---------------------------------------------009 total instances per viewing day complete-----------------------------


---------------------------------------------006 average impacts per viewing day start------------------------


---------------------------------------------------R01 - average impacts per viewing day----------------------------------------

if @Metric_to_Measure = 'average_impacts_per_viewing_day'
begin

declare @viewing_date date


--set @viewing_date = (select max(date(impact_day)) from DATA_QUALITY_SLOT_DATA_AUDIT)

set @viewing_date = @load_date

----------------------------------------R02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


----------------------------------------R03 - Logic to generate Metric Benchmark result------------------------------------------


SELECT date(impact_day) viewing_date, sum(case when impacts = 1 then 1 else 0 end) no_of_impacts,
sum(case when household_key > 0 then 1 else 0 end) no_of_households,
@Metric_to_Measure metric_short_name
into #tmp_avg_impacts
from DATA_QUALITY_SLOT_DATA_AUDIT
where date(impact_day) = @viewing_date
group by date(impact_day)

select dq_vm_id,viewing_date,no_of_impacts/no_of_households metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #tmp_avg_impacts_detail
from #tmp_avg_impacts tti, #data_metrics_detail dm_det
where tti.metric_short_name = dm_det.metric_short_name

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------R04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #tmp_avg_impacts_detail_final
from #tmp_avg_impacts_detail a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------R05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_avg_impacts_detail_final

commit

-----------------------------------------R06 - Drop Temp Tables--------------------------------------------------------------------


drop table #tmp_avg_impacts
drop table #tmp_avg_impacts_detail
drop table #tmp_avg_impacts_detail_final


end

-----------------------------------------------------------------------------006 average impacts per hh/ per viewing day finished------------------------------------------------------



-----------------------------------------------------------------------------007 total impacts per viewing day started----------------------------------------------------


-----------------------------------------------------------------------------S01 - total impacts per viewing day ---------------------------------------------------------------------


if @Metric_to_Measure = 'total_impacts_per_viewing_day'
begin

declare @viewing_date date

--set @viewing_date = (select max(date(impact_day)) from DATA_QUALITY_SLOT_DATA_AUDIT)

set @viewing_date = @load_date


----------------------------------------S02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


----------------------------------------S03 - Logic to generate Metric Benchmark result------------------------------------------


SELECT date(impact_day) viewing_date, sum(case when impacts = 1 then 1 else 0 end) no_of_impacts,
@Metric_to_Measure metric_short_name
into #tmp_total_impacts
from DATA_QUALITY_SLOT_DATA_AUDIT
where date(impact_day) = @viewing_date
group by date(impact_day)


select dq_vm_id,viewing_date,no_of_impacts metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #tmp_total_impacts_detail
from #tmp_total_impacts tti, #data_metrics_detail dm_det
where tti.metric_short_name = dm_det.metric_short_name

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------S04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #tmp_total_impacts_detail_final
from #tmp_total_impacts_detail a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------S05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_total_impacts_detail_final

commit

-----------------------------------------S06 - Drop Temp Tables--------------------------------------------------------------------


drop table #tmp_total_impacts
drop table #tmp_total_impacts_detail
drop table #tmp_total_impacts_detail_final


end


-----------------------------------------------------------------------------007 total impacts per viewing day ended----------------------------------------------------


-----------------------------------------------------------------------------010 average events per HH per viewing day started----------------------------------------------------

----------------------------------------------------------------------------T01 - Average Events per HH per viewing day-------------------------------------------------------------


if @Metric_to_Measure = 'average_events_per_hh_per_viewing_day'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)


----------------------------------------T02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


----------------------------------------T03 - Logic to generate Metric Benchmark result------------------------------------------


SELECT sum(Event_Count.Count)/count(distinct account_number) as Events_average, 
CAST(event_start_date_time_utc as date) viewing_date,
@Metric_to_Measure metric_short_name
into #tmp_avg_events
FROM(
------------------------------------------------------------------------
--To get a count of records produced from subscriber_id and event_start_date_time_utc
------------------------------------------------------------------------
  SELECT count(1) over (partition by account_number, event_start_date_time_utc) as Count
    ,account_number, event_start_date_time_utc
  FROM data_quality_dp_data_audit
  WHERE   panel_id IN (12) -- ONLY PANEL 12 REFERENCE
  AND viewing_date = @viewing_date
  GROUP BY account_number, event_start_date_time_utc
) as Event_Count
GROUP BY CAST(event_start_date_time_utc as date),@Metric_to_Measure


select dq_vm_id,viewing_date,Events_average metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #tmp_avg_events_detail
from #tmp_avg_events tti, #data_metrics_detail dm_det
where tti.metric_short_name = dm_det.metric_short_name

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!


----------------------------------------T04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #tmp_avg_events_detail_final
from #tmp_avg_events_detail a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------T05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_avg_events_detail_final

commit

-----------------------------------------T06 - Drop Temp Tables--------------------------------------------------------------------


drop table #tmp_avg_events
drop table #tmp_avg_events_detail
drop table #tmp_avg_events_detail_final

end

-----------------------------------------------------------------------------010 average events per HH per viewing day ended----------------------------------------------------


-----------------------------------------------------------------------------011 average instances per HH per viewing day ended----------------------------------------------------


---------------------------------------------------------------------------U01 - Average instances per HH per viewing day-----------------------------------------------------------------

if @Metric_to_Measure = 'average_instances_per_hh_per_viewing_day'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)

----------------------------------------U02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

----------------------------------------U03 - Logic to generate Metric Benchmark result------------------------------------------



SELECT sum(Instance_Count.Count)/count(distinct account_number) as instances_average, 
@Metric_to_Measure metric_short_name
into #tmp_avg_instances
FROM(
------------------------------------------------------------------------
--To get a count of records produced from subscriber_id and event_start_date_time_utc
------------------------------------------------------------------------
  SELECT count(1) over (partition by account_number, instance_start_date_time_utc) as Count
    ,account_number, instance_start_date_time_utc
  FROM data_quality_dp_data_audit
  WHERE   panel_id IN (12) -- ONLY PANEL 12 REFERENCE
  AND viewing_date = @viewing_date
  GROUP BY account_number, instance_start_date_time_utc
) as Instance_Count
GROUP BY @Metric_to_Measure


select dq_vm_id,@viewing_date,instances_average metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #tmp_avg_instances_detail
from #tmp_avg_instances tti, #data_metrics_detail dm_det
where tti.metric_short_name = dm_det.metric_short_name

---now serious time.  we have our result and benchmark info. Can we now see what our RAG status is please?
---calls the metric_benchmark_check function as it should always be the same calculation and wanted something to
---standardise this.  Now that is planning!!!

----------------------------------------U04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #tmp_avg_instances_detail_final
from #tmp_avg_instances_detail a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------U05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,@viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_avg_instances_detail_final

commit

-----------------------------------------U06 - Drop Temp Tables--------------------------------------------------------------------


drop table #tmp_avg_instances
drop table #tmp_avg_instances_detail
drop table #tmp_avg_instances_detail_final

end



-----------------------------------------------------------------------------011 average instances per HH per viewing day ended----------------------------------------------------


-----------------------------------------------------------------------------014 number of HH dialling back on a Daily Basis---------------------------------------------------------


-----------------------------------------------------------------------------V01 - number of HH dialling back on a Daily Basis--------------------------------------------------------


if @Metric_to_Measure = 'vespa_dp_hh_dialling_back'
begin

declare @viewing_date date


set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)

----------------------------------------V02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


--get me current panel 12 enabled panelists

----------------------------------------V03 - Logic to generate Metric Benchmark result------------------------------------------


select account_number, count(distinct card_subscriber_id) no_of_boxes_panel
into #tmp_panel_members
from sk_prod.VESPA_SUBSCRIBER_STATUS
where result = 'Enabled'
and panel_no = 12
group by account_number


--get me some logs of those events we are looking at

SELECT Account_number 												--HOUSEHOLD ID
,subscriber_id
,CASE
			WHEN CONVERT(INTEGER,dateformat(MIN(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) <23
				THEN CAST(MIN(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)-1
				ELSE
					CAST(min(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)
				END AS 				Log_Date 	-- BASED ON OPS REPORTS DEFINITION "all logs received from 23:00 on day A until 22:59 on next day (A+1) will belong to A"
into #tmp_dp_hh_dialback
FROM data_quality_dp_data_audit
WHERE  	panel_id IN (12) 											-- ONLY PANEL 12 REFERENCE
--	AND 	LOG_RECEIVED_START_DATE_TIME_UTC >= @report_date
	AND 	LOG_RECEIVED_START_DATE_TIME_UTC IS NOT NULL
	AND 	LOG_START_DATE_TIME_UTC IS NOT NULL
	AND		subscriber_id IS NOT NULL
	AND		Account_number IS NOT NULL
GROUP BY Account_number,subscriber_id
HAVING Log_Date IS NOT NULL

---lets tally up the boxes and hh who have dialled back that day

SELECT ACCOUNT_NUMBER, COUNT(DISTINCT SUBSCRIBER_ID) no_of_boxes_events
into #tmp_dp_hh_total
FROM #tmp_dp_hh_dialback
where log_date = @viewing_date
GROUP BY ACCOUNT_NUMBER

---lets compare to see what is  the same and what is different.

select a.account_number, a.no_of_boxes_panel, b.no_of_boxes_events,
@Metric_to_Measure metric_short_name
into #tmp_hh_dp_final
from #tmp_panel_members a, #tmp_dp_hh_total b
where a.account_number = b.account_number
and a.no_of_boxes_panel = b.no_of_boxes_events


select dq_vm_id,@viewing_date viewing_date,count(distinct account_number) metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #tmp_hh_dp_final_detail
from #tmp_hh_dp_final tti, #data_metrics_detail dm_det
where tti.metric_short_name = dm_det.metric_short_name
group by dq_vm_id,@viewing_date ,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red


----------------------------------------V04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #tmp_hh_dp_final_detail_final
from #tmp_hh_dp_final_detail a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------V05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_hh_dp_final_detail_final

commit


end

-----------------------------------------------------------------------------014 number of HH dialling back on a Daily Basis Ended---------------------------------------------------------


-----------------------------------------------------------------------------016 number of HH not dialling back on a Daily Basis Started---------------------------------------------------------

-----------------------------------------------------------------------------W01 - number of HH not dialling back on a Daily Basis-----------------------------------------------------------------

if @Metric_to_Measure = 'vespa_dp_hh_not_dialling_back'
begin


declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)

----------------------------------------W02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--get me current panel 12 enabled panelists

----------------------------------------W03 - Logic to generate Metric Benchmark result------------------------------------------


select account_number, count(distinct card_subscriber_id) no_of_boxes_panel
into #tmp_panel_members
from sk_prod.VESPA_SUBSCRIBER_STATUS
where result = 'Enabled'
and panel_no = 12
group by account_number


--get me some logs of those events we are looking at

SELECT Account_number 												--HOUSEHOLD ID
,subscriber_id
,CASE
			WHEN CONVERT(INTEGER,dateformat(MIN(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) <23
				THEN CAST(MIN(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)-1
				ELSE
					CAST(min(LOG_RECEIVED_START_DATE_TIME_UTC) AS DATE)
				END AS 				Log_Date 	-- BASED ON OPS REPORTS DEFINITION "all logs received from 23:00 on day A until 22:59 on next day (A+1) will belong to A"
into #tmp_dp_hh_dialback
FROM data_quality_dp_data_audit
WHERE  	panel_id IN (12) 											-- ONLY PANEL 12 REFERENCE
--	AND 	LOG_RECEIVED_START_DATE_TIME_UTC >= @report_date
	AND 	LOG_RECEIVED_START_DATE_TIME_UTC IS NOT NULL
	AND 	LOG_START_DATE_TIME_UTC IS NOT NULL
	AND		subscriber_id IS NOT NULL
	AND		Account_number IS NOT NULL
GROUP BY Account_number,subscriber_id
HAVING Log_Date IS NOT NULL

---lets tally up the boxes and hh who have dialled back that day

SELECT ACCOUNT_NUMBER, COUNT(DISTINCT SUBSCRIBER_ID) no_of_boxes_events
into #tmp_dp_hh_total
FROM #tmp_dp_hh_dialback
where log_date = @viewing_date
GROUP BY ACCOUNT_NUMBER

---lets compare to see what is  the same and what is different.

select a.account_number, a.no_of_boxes_panel, b.no_of_boxes_events
into #tmp_hh_dp_final
from #tmp_panel_members a, #tmp_dp_hh_total b
where a.account_number = b.account_number
and a.no_of_boxes_panel != b.no_of_boxes_events

select count(distinct a.account_number) acc_not_exist 
into #tmp_panel_members_nodialback
from #tmp_panel_members a
where not exists
(select 1 from #tmp_dp_hh_total b
where a.account_number = b.account_number) 


select @Metric_to_Measure metric_short_name,
((select count(distinct account_number) from #tmp_hh_dp_final) + (select acc_not_exist from #tmp_panel_members_nodialback)) metric_result
into #tmp_hh_dp_not_dialback_final


select dq_vm_id,@viewing_date viewing_date,tti.metric_result,
dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #tmp_hh_dp_not_dialback_final_detail
from #tmp_hh_dp_not_dialback_final tti, #data_metrics_detail dm_det
where tti.metric_short_name = dm_det.metric_short_name


----------------------------------------W04 - Generate RAG status------------------------------------------


select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #tmp_hh_dp_not_dialback_final_detail_final
from #tmp_hh_dp_not_dialback_final_detail a

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------W05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_hh_dp_not_dialback_final_detail_final

commit


end


-----------------------------------------------------------------------------016 number of HH not dialling back on a Daily Basis Ended---------------------------------------------------------



-----------------------------------------------------------------------------X01 - number of live events received on a Daily Basis-----------------------------------------------------------------

if @Metric_to_Measure = 'mtc_vespa_dp_instances_returned_live'
begin


declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)

----------------------------------------X02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--get me current panel 12 enabled panelists

----------------------------------------X03 - Logic to generate Metric Benchmark result------------------------------------------


select count(1) no_of_live_instances
into #tmp_live_instances
from data_quality_dp_data_audit
where UPPER(live_recorded) = 'LIVE'


select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
no_of_live_instances metric_result
into #tmp_live_instances_final
from #tmp_live_instances

----------------------------------------X04 - Generate RAG status------------------------------------------


select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_live_instances_final_detail
from #tmp_live_instances_final a,  #data_metrics_detail b
where a.metric_short_name = b.metric_short_name


---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------X05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_live_instances_final_detail

commit


-----------------------------------------------------------------x06 - Drop Temp Tables-----------------------------------------------


drop table #data_metrics_detail
drop table #tmp_live_instances
drop table #tmp_live_instances_final
drop table #tmp_live_instances_final_detail


end


-----------------------------------------------------------------------------number of live events received on a Daily Basis End-----------------------------------------------------------------


-----------------------------------------------------------------------------Y01 - number of recorded events received on a Daily Basis-----------------------------------------------------------------



if @Metric_to_Measure = 'mtc_vespa_dp_instances_returned_recorded'
begin


declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)

----------------------------------------Y02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--get me current panel 12 enabled panelists

----------------------------------------Y03 - Logic to generate Metric Benchmark result------------------------------------------


select count(1) no_of_recorded_instances
into #tmp_recorded_instances
from data_quality_dp_data_audit
where UPPER(live_recorded) = 'RECORDED'


select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
no_of_recorded_instances metric_result
into #tmp_recorded_instances_final
from #tmp_recorded_instances

----------------------------------------Y04 - Generate RAG status------------------------------------------

select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_recorded_instances_final_detail
from #tmp_recorded_instances_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------Y05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_recorded_instances_final_detail

commit


-----------------------------------------------------------------Y06 - Drop Temp Tables-----------------------------------------------


drop table #data_metrics_detail
drop table #tmp_recorded_instances
drop table #tmp_recorded_instances_final
drop table #tmp_recorded_instances_final_detail



end


-----------------------------------------------------------------------------Z01 - number of recorded events received on a Daily Basis-----------------------------------------------------------------



if @Metric_to_Measure = 'mtc_vespa_dp_instances_returned_not_live_rec'
begin


declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)

----------------------------------------Z02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--get me current panel 12 enabled panelists

----------------------------------------Z03 - Logic to generate Metric Benchmark result------------------------------------------


select count(1) no_of_recorded_instances
into #tmp_no_live_recorded_instances
from data_quality_dp_data_audit
where UPPER(live_recorded) not in ('RECORDED','LIVE')


select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
no_of_recorded_instances metric_result
into #tmp_no_live_recorded_instances_final
from #tmp_no_live_recorded_instances

----------------------------------------Z04 - Generate RAG status------------------------------------------


select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_no_live_recorded_instances_detail
from #tmp_no_live_recorded_instances_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------Z05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_no_live_recorded_instances_detail

commit


-----------------------------------------------------------------Z06 - Drop Temp Tables-----------------------------------------------


drop table #data_metrics_detail
drop table #tmp_no_live_recorded_instances
drop table #tmp_no_live_recorded_instances_final
drop table #tmp_no_live_recorded_instances_detail



end


-----------------------------------------------------------------------------AA01 - records where duration is negative-----------------------------------------------------------------



if @Metric_to_Measure = 'mtc_vespa_dp_events_duration_negative'
begin


declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)

----------------------------------------AA02 - Collect Metric Data from Metrics table--------------------------------------------------------


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--get me current panel 12 enabled panelists

----------------------------------------AA03 - Logic to generate Metric Benchmark result------------------------------------------


select count(1) no_of_negative_durations
into #tmp_no_of_negative_durations
from data_quality_dp_data_audit
where (duration < 0 or duration is null)


select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
no_of_negative_durations metric_result
into #tmp_no_of_negative_durations_final
from #tmp_no_of_negative_durations


----------------------------------------AA04 - Generate RAG status------------------------------------------



select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_no_of_negative_durations_detail
from #tmp_no_of_negative_durations_final a, #data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------AA05 - insert into Vespa Repository Table------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_no_of_negative_durations_detail

commit


-----------------------------------------------------------------AA06 - Drop Temp Tables-----------------------------------------------


drop table #data_metrics_detail
drop table #tmp_no_of_negative_durations
drop table #tmp_no_of_negative_durations_final
drop table #tmp_no_of_negative_durations_detail



end


------------------------------------------------------------------------------------------------------------------------

----------------------------------------AB01 - CAPPED END TIME > THAN EVENT START--------------------------------------------------------


if @Metric_to_Measure = 'mtc_capped_end_greater_event_start'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)
----------------------------------------AB02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

----------------------------------------AB03 - Logic to generate Metric Benchmark result------------------------------------------

-- Check capped end time is greater thant event start time

select sum(case when capping_end_date_time_utc <= event_start_date_time_utc then 1 else 0 end) as capped_issue
into #tmp_capped_end_greater_event_start
from data_quality_dp_data_audit
where capping_end_date_time_utc is not null

select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
capped_issue metric_result
into #tmp_capped_end_greater_event_start_final
from #tmp_capped_end_greater_event_start

----------------------------------------AB04 - Generate RAG status------------------------------------------

select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_capped_end_greater_event_start_final_detail
from #tmp_capped_end_greater_event_start_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------AB05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_capped_end_greater_event_start_final_detail

commit
-----------------------------------------------------------------AB06 - Drop Temp Tables-----------------------------------------------


drop table #data_metrics_detail
drop table #tmp_capped_end_greater_event_start
drop table #tmp_capped_end_greater_event_start_final
drop table #tmp_capped_end_greater_event_start_final_detail



end


----------------------------------------AD01 - CAPPED EVENT DATE NOT TIME CHK--------------------------------------------------------



if @Metric_to_Measure = 'mtc_tmp_capped_end_time_date_chk'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)
----------------------------------------AD02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

----------------------------------------AD03 - Logic to generate Metric Benchmark result------------------------------------------

-- Check we don't have capped date and not time

select count(1) as capping_chk
into #tmp_capped_end_time_date_chk
from data_quality_dp_data_audit
where (dk_capping_end_datehour_dim > 0 and (dk_capping_end_time_dim <= 0 or dk_capping_end_time_dim is null))
or (dk_capping_end_time_dim > 0 and (dk_capping_end_datehour_dim <= 0 or dk_capping_end_datehour_dim is null))

select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
capping_chk metric_result
into #tmp_capped_end_time_date_chk_final
from #tmp_capped_end_time_date_chk

----------------------------------------AD04 - Generate RAG status------------------------------------------

select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_capped_end_time_date_chk_final_detail
from #tmp_capped_end_time_date_chk_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------AD05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_capped_end_time_date_chk_final_detail

commit
-----------------------------------------------------------------AD06 - Drop Temp Tables-----------------------------------------------


drop table #data_metrics_detail
drop table #tmp_capped_end_time_date_chk
drop table #tmp_capped_end_time_date_chk_final
drop table #tmp_capped_end_time_date_chk_final_detail



end


----------------------------------------AE01 - CAPPED FLAG CONSISTENCY CHK--------------------------------------------------------



if @Metric_to_Measure = 'mtc_capped_flag_consistency_chk'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)
----------------------------------------AE02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

----------------------------------------AE03 - Logic to generate Metric Benchmark result------------------------------------------

-- Check we have values for the full, partial flag available in the fact

select count(1) capping_chk
into #tmp_capped_flag_consistency_chk
from data_quality_dp_data_audit
where capped_full_flag > 0 and capped_partial_flag > 0

select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
capping_chk metric_result
into #tmp_capped_flag_consistency_chk_final
from #tmp_capped_flag_consistency_chk

----------------------------------------AE04 - Generate RAG status------------------------------------------

select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_capped_flag_consistency_chk_final_detail
from #tmp_capped_flag_consistency_chk_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------AE05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_capped_flag_consistency_chk_final_detail

commit
-----------------------------------------------------------------AE06 - Drop Temp Tables-----------------------------------------------


drop table #data_metrics_detail
drop table #tmp_capped_flag_consistency_chk
drop table #tmp_capped_flag_consistency_chk_final
drop table #tmp_capped_flag_consistency_chk_final_detail



end


----------------------------------------AF01 - CAPPED FLAG END TIME CONSISTENCY CHK--------------------------------------------------------



if @Metric_to_Measure = 'mtc_capped_flag_endtime_consistency_chk'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)
----------------------------------------AF02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

----------------------------------------AF03 - Logic to generate Metric Benchmark result------------------------------------------

-- Check capped date/time not null if capped flag is 1

select count(1) capping_chk
into #tmp_capped_flag_endtime_consistency_chk
from data_quality_dp_data_audit
where (capped_full_flag > 0 or capped_partial_flag > 0) and capping_end_date_time_utc is null


select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
capping_chk metric_result
into #tmp_capped_flag_endtime_consistency_chk_final
from #tmp_capped_flag_endtime_consistency_chk

----------------------------------------AF04 - Generate RAG status------------------------------------------

select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_capped_flag_endtime_consistency_chk_final_detail
from #tmp_capped_flag_endtime_consistency_chk_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------AF05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_capped_flag_endtime_consistency_chk_final_detail

commit
-----------------------------------------------------------AF06 - Drop Temp Tables-----------------------------------------------

drop table #data_metrics_detail
drop table #tmp_capped_flag_endtime_consistency_chk
drop table #tmp_capped_flag_endtime_consistency_chk_final
drop table #tmp_capped_flag_endtime_consistency_chk_final_detail

end



----------------------------------------AG01 - BARB START END CONSISTENCY CHK--------------------------------------------------------


-- Check for attributted programmes we have barb start minute and barb end minute
-- Check for cases with barb start but no end minute

if @Metric_to_Measure = 'mtc_barb_start_end_consistency_chk'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)
----------------------------------------AG02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

----------------------------------------AG03 - Logic to generate Metric Benchmark result------------------------------------------

select count(1) barb_chk into #tmp_barb_start_end_consistency_chk
from data_quality_dp_data_audit
where (dk_barb_min_start_datehour_dim > 0 and (DK_BARB_MIN_END_DATEHOUR_DIM in (null,-1) or DK_BARB_MIN_END_TIME_DIM in (null,-1)))
or (dk_barb_min_end_datehour_dim > 0 and (DK_BARB_MIN_start_DATEHOUR_DIM in (null,-1) or DK_BARB_MIN_start_TIME_DIM in (null,-1)))

select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
barb_chk metric_result
into #tmp_barb_start_end_consistency_chk_final
from #tmp_barb_start_end_consistency_chk

----------------------------------------AG04 - Generate RAG status------------------------------------------

select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_barb_start_end_consistency_chk_final_detail
from #tmp_barb_start_end_consistency_chk_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------AG05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_barb_start_end_consistency_chk_final_detail

commit
-----------------------------------------------------------------AG06 - Drop Temp Tables-----------------------------------------------

drop table #data_metrics_detail
drop table #tmp_barb_start_end_consistency_chk
drop table #tmp_barb_start_end_consistency_chk_final
drop table #tmp_barb_start_end_consistency_chk_final_detail

end



----------------------------------------AH01 - BARB MINUTE OVERLAP CONSISTENCY CHK--------------------------------------------------------


--select count(1) from data_quality_dp_data_audit

if @Metric_to_Measure = 'mtc_barb_start_end_overlap_chk'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)
----------------------------------------AH02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

----------------------------------------AH03 - Logic to generate Metric Benchmark result------------------------------------------

select subscriber_id,pk_viewing_prog_instance_fact, instance_start_date_time_utc,live_recorded,
LAG (pk_viewing_prog_instance_fact) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_pk_viewing_prog_instance_fact,
LAG (dk_barb_min_start_time_dim) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_barb_start_time,
LAG (dk_barb_min_end_time_dim) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_barb_end_time,
LAG (dk_barb_min_start_datehour_dim) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_barb_start_date,
LAG (dk_barb_min_end_datehour_dim) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_barb_end_date,
dk_barb_min_start_datehour_dim, dk_barb_min_start_time_dim,
dk_barb_min_end_datehour_dim, dk_barb_min_end_time_dim into #tmp_data_quality_ma_overlap
from data_quality_dp_data_audit
where subscriber_id > 0
and UPPER(live_recorded) = 'LIVE'

select count(1) barb_chk
into #tmp_barb_start_end_overlap_chk
from #tmp_data_quality_ma_overlap
where dk_barb_min_start_datehour_dim = prev_barb_start_date
and dk_barb_min_start_time_dim <= prev_barb_end_time
and dk_barb_min_start_time_dim > 0

select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
barb_chk metric_result
into #tmp_barb_start_end_overlap_chk_final
from #tmp_barb_start_end_overlap_chk

----------------------------------------AH04 - Generate RAG status------------------------------------------

select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_barb_start_end_overlap_chk_final_detail
from #tmp_barb_start_end_overlap_chk_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------AH05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_barb_start_end_overlap_chk_final_detail

commit
-----------------------------------------------------------------AH06 - Drop Temp Tables-----------------------------------------------

drop table #data_metrics_detail
drop table #tmp_data_quality_ma_overlap
drop table #tmp_barb_start_end_overlap_chk
drop table #tmp_barb_start_end_overlap_chk_final
drop table #tmp_barb_start_end_overlap_chk_final_detail

end



---------------------------------------------------------------------------------------------------------------------------------------------------------------------


----------------------------------------AJ01 - BARB START DATE/TIME END DATE/TIME CONSISTENCY CHK--------------------------------------------------------



if @Metric_to_Measure = 'mtc_barb_start_end_time_date_consistency_chk'
begin

declare @viewing_date date

set @viewing_date = (select max(VIEWING_DATE) from DATA_QUALITY_DP_DATA_aUDIT)
----------------------------------------AJ02 - Collect Metric Data from Metrics table--------------------------------------------------------

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

----------------------------------------AJ03 - Logic to generate Metric Benchmark result------------------------------------------



select
(select count(1) dk_dim_start_date from data_quality_dp_data_audit
where dk_barb_min_start_datehour_dim > 0) dk_dim_start_date,
(select count(1) dk_dim_start_time from data_quality_dp_data_audit
where dk_barb_min_start_time_dim > 0) dk_dim_start_time,
(select count(1) dk_dim_end_date from data_quality_dp_data_audit
where dk_barb_min_end_datehour_dim > 0) dk_dim_end_date,
(select count(1) dk_dim_end_time from data_quality_dp_data_audit
where dk_barb_min_end_time_dim > 0) dk_dim_end_time
into #tmp_barb_start_end_time_date_consistency_chk_1

select (dk_dim_start_date - dk_dim_start_time) + (dk_dim_end_date - dk_dim_end_time) barb_chk
into #tmp_barb_start_end_time_date_consistency_chk
from
#tmp_barb_start_end_time_date_consistency_chk_1

select @Metric_to_Measure metric_short_name,@viewing_date viewing_date,
barb_chk metric_result
into #tmp_barb_start_end_time_date_consistency_chk_final
from #tmp_barb_start_end_time_date_consistency_chk

----------------------------------------AJ04 - Generate RAG status------------------------------------------

select a.*,b.dq_vm_id, b.metric_tolerance_amber, b.metric_tolerance_red,
metric_benchmark_check(a.metric_result, b.metric_benchmark, b.metric_tolerance_amber, b.metric_tolerance_red) metric_rag
into #tmp_barb_start_end_time_date_consistency_chk_final_detail
from #tmp_barb_start_end_time_date_consistency_chk_final a,#data_metrics_detail b
where a.metric_short_name = b.metric_short_name

---with our results in hand, lets enter them into the repository and do summit!!

----------------------------------------AJ05 - insert into Vespa Repository Table------------------------------------------

insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @dq_run_id
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#tmp_barb_start_end_time_date_consistency_chk_final_detail

commit
-----------------------------------------------------------------AJ06 - Drop Temp Tables-----------------------------------------------

drop table #data_metrics_detail
drop table #tmp_barb_start_end_time_date_consistency_chk_1
drop table #tmp_barb_start_end_time_date_consistency_chk
drop table #tmp_barb_start_end_time_date_consistency_chk_final
drop table #tmp_barb_start_end_time_date_consistency_chk_final_detail

end


-----------------------------------------------------------------------------


---final end of the entire procedure

end

