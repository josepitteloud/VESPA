if object_id('Data_Quality_Metrics_Collection') is not null then drop procedure Data_Quality_Metrics_Collection;
commit;


-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data Quality Metrics Collection
**
** This script has the big procedure which goes through the various metrics that
** we want to report on to check the integrity of the data that is coming in
** from CBI into Olive.  It comprises of basic metric checks in a separate procedure
** and specific metrics that we want to collect for analytical purposes
**
** Refer also to:
**
**  
**
**
**
** Code sections:
**
**  A) SET UP   
**
**  B) 
**
**              
**
**  C) 
**
**              
**  D) 
**
**
**  E) 
**
**
**  G) 
**
**  J) 
**
**  R) 
**
**
**
**
**
** Things done:
**
**
******************************************************************************/

-- We've also got a few cases where *everything* ends up with the capped flag set;
-- how are we establishing the correct caps when we have no universe of uncapped
-- to choose from?

if object_id('Data_Quality_Metrics_Collection') is not null drop procedure Data_Quality_Metrics_Collection;
commit;

go

create procedure Data_Quality_Metrics_Collection
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

if @Metric_to_Measure = 'daily_viewing_hh_avg_precapped_live' 

begin


---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


select distinct viewing_date,account_number,event_start_date_time_utc,event_end_date_time_utc, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

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
,@load_date
from
#uncapped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

drop table #uncapped_daily_comparison_final
drop table #uncapped_daily_comparison
drop table #uncapped_daily_duration
drop table #data_metrics_detail
drop table #account_event_uncapped

end

-----------------------------now for recorded pre capping-------------------------------------------

if @Metric_to_Measure = 'daily_viewing_hh_avg_precapped_rec' 

begin


select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


select distinct viewing_date,account_number,event_start_date_time_utc,event_end_date_time_utc, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

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
,@load_date
from
#uncapped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

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

begin


---these should be set as part of the procedure parameters.  Here for testing purposes currently

--set @dq_run_id = 99
--set @load_date = getdate()


---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--lets get the info needed for the total test from the data audit table

select distinct viewing_date,account_number,event_start_date_time_utc,event_end_date_time_utc, panel_id,
@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

--now we have the all we need enter the results in the repository

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
,@load_date
from
#uncapped_daily_comparison_final

commit

--drop those temp tables.  remember you must always drop those temp tables.

drop table #uncapped_daily_comparison_final
drop table #uncapped_daily_comparison
drop table #uncapped_daily_duration
drop table #data_metrics_detail
drop table #account_event_uncapped

end

-----------------------------------------------daily_viewing_by_account_pre_capping_done-----------------------------------------------------

-----------------------------------------------daily_viewing_by_account_post_capping start-----------------------------------------


--4)


if @Metric_to_Measure = 'daily_viewing_hh_avg_postcapped_total' 

begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--lets get the info needed for the total test from the data audit table


select distinct viewing_date,account_number,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
@Metric_to_Measure metric_short_name
into #account_event_capped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

--now we have the all we need enter the results in the repository

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
,@load_date
from
#capped_daily_comparison_final

commit


---drop those pesky temp tables

drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped

end

------------------------------------------------------------------------------------------

if @Metric_to_Measure = 'daily_viewing_hh_avg_postcapped_live' 

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


select distinct viewing_date,account_number,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_capped
from
kinnairt.data_quality_dp_data_audit
where panel_id = 12

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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

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
,@load_date
from
#capped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped

end


-----------------------------------------------post_capping_live done--------------------------------------

-----------------------------------------------post_capping_rec start-----------------------------------------

if @Metric_to_Measure = 'daily_viewing_hh_avg_postcapped_rec' 

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


select distinct viewing_date,account_number,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_capped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

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
,@load_date
from
#capped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

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

if @Metric_to_Measure = 'daily_viewing_stb_avg_precapped_live'
begin

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table

select distinct viewing_date,subscriber_id,event_start_date_time_utc,event_end_date_time_utc, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

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
,@load_date
from
#uncapped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

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

if @Metric_to_Measure = 'daily_viewing_stb_avg_precapped_rec'
begin

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table

select distinct viewing_date,subscriber_id,event_start_date_time_utc,event_end_date_time_utc, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

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
,@load_date
from
#uncapped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

drop table #uncapped_daily_comparison_final
drop table #uncapped_daily_comparison
drop table #uncapped_daily_duration
drop table #data_metrics_detail
drop table #account_event_uncapped


end


-----------------------------------------SUB_ID PRE_CAPPING DONE-------------------------------------


----------------------------------------------------------------------------------------------------------


-----------------------------------------SUB_ID TOTAL VIEWING START-----------------------------------------



if @Metric_to_Measure = 'daily_viewing_stb_avg_precapped_total'
begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--lets get the info needed for the total test from the data audit table

select distinct viewing_date,subscriber_id,event_start_date_time_utc,event_end_date_time_utc, panel_id,
@Metric_to_Measure metric_short_name
into #account_event_uncapped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #uncapped_daily_comparison_final
from #uncapped_daily_comparison a

--now we have the all we need enter the results in the repository

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
,@load_date
from
#uncapped_daily_comparison_final

commit



--------------------------------------------sub_id total viewing pre capping done-------------------------------



----------------------------------------------------------------------------------------------------------------

if @Metric_to_Measure = 'daily_viewing_stb_avg_postcapped_total'
begin



---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure

--lets get the info needed for the total test from the data audit table

select distinct viewing_date,subscriber_id,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
@Metric_to_Measure metric_short_name
into #account_event_capped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

--now we have the all we need enter the results in the repository

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
,@load_date
from
#capped_daily_comparison_final

commit


---drop those pesky temp tables

drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped

end

------------------------------------------------------------------------------------------


if @Metric_to_Measure = 'daily_viewing_stb_avg_postcapped_live'
begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


select distinct viewing_date,subscriber_id,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_capped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

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
,@load_date
from
#capped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped


end


---------------------------------------------------sub_id live post capping done------------------------------------


---------------------------------------------------sub_id rec post capping start----------------------------

if @Metric_to_Measure = 'daily_viewing_stb_avg_postcapped_rec'
begin

---lets get the metric details for the particular metrics that we are running here from the
---data_quality_vespa_metrics table

select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
into #data_metrics_detail
from data_quality_vespa_metrics
where metric_short_name = @Metric_to_Measure


---time to do some stuff. go off to the pre-prepared viewing table where we want to derive all of this
---information and get event dates, live_recorded and define metric name for later in the process
---needs grouping so that we only get unique events within the multiple instances on the data prepare
---table


select distinct viewing_date,subscriber_id,event_start_date_time_utc,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time, panel_id,
live_recorded,@Metric_to_Measure metric_short_name
into #account_event_capped
from
kinnairt.data_quality_dp_data_audit
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

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #capped_daily_comparison_final
from #capped_daily_comparison a

---with our results in hand, lets enter them into the repository and do summit!!

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
,@load_date
from
#capped_daily_comparison_final

commit

--drop the temp tables esp as the next process may be slightly lazy and reuse the naming conventions

drop table #capped_daily_comparison_final
drop table #capped_daily_comparison
drop table #capped_daily_duration
drop table #data_metrics_detail
drop table #account_event_capped


end


---final end of the entire procedure

end

