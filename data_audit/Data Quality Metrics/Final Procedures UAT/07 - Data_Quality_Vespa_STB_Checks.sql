-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data_Quality_Vespa_STB_Checks
**
** The proc which runs the STB Checks routine to get the pre/post capping results per stb for the 
** Data Quality Reports
**
**  
** Refer also to:
**
**
** Code sections:
**      Part A: 
**          A01 - Collect Metrics values that you will be collating in this procedure
**
**      Part B:       
**          B01 - collect base information that will be used to calculate the metrics
**          B02 - Calculate the durations pre and post capping for Live and Recorded 
**          B03 - Calculate the durations pre and post capping for Total 
**          B04 - Calculate the averages for STB pre and Post Capping 
**          B05 - Relate the results to each of the individual metrics involved 
**          B06 - join back to metrics table to get the metric ids etc...
**          B07 - push the results for all metrics through the benchmark functionality...
**
**      Part C:
**          C01 - Place the metrics results into the Repository Table
**          
**      Part D:
**          D01 - Drop Temporary Tables
**
**
** Things done:
**
**
******************************************************************************/

if object_id('Data_Quality_Vespa_STB_Checks') is not null drop procedure Data_Quality_Vespa_STB_Checks
commit

go

create procedure Data_Quality_Vespa_STB_Checks
@target_date        date = NULL     -- Date of data analyzed or date process run
    ,@CP2_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

--declare @run_type varchar(40)
declare @creator varchar(40)
declare @column_name varchar(200)
declare @table_name varchar(200)
declare @dq_check_type varchar(200)
declare @dq_check_detail_id int
declare @dq_run_id bigint


EXECUTE logger_add_event @RunID , 3,'Data Quality STB Checks Start for Date '||cast (@target_date as varchar(20))


-------------------------------------------------A01 - Collect Metrics values that you will be collating in this procedure-------------------------------

select * into #data_metrics_detail
from
(select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
from data_quality_vespa_metrics
where metric_short_name like 'daily_viewing_stb_avg_postcapped_primary%'
union all
select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
from data_quality_vespa_metrics
where metric_short_name like 'daily_viewing_stb_avg_postcapped_secondary%'
union all
select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
from data_quality_vespa_metrics
where metric_short_name like 'daily_viewing_stb_avg_precapped_primary%'
union all
select dq_vm_id, metric_short_name, metric_benchmark, metric_tolerance_amber, metric_tolerance_red
from data_quality_vespa_metrics
where metric_short_name like 'daily_viewing_stb_avg_precapped_secondary%') t


---------------------------------------------------------B01 - collect base information that will be used to calculate the metrics-------------------------------------------

select distinct viewing_date,dp.subscriber_id,event_start_date_time_utc,event_end_date_time_utc, panel_id,
vsbv.ps_flag primary_secondary_flag, live_recorded,
coalesce(capping_end_date_time_utc,event_end_date_time_utc) calculated_end_time
into #stb_event_capped_uncapped
from
data_quality_dp_data_audit dp,
vespa_analysts.vespa_single_box_view vsbv
where  dp.subscriber_id = vsbv.subscriber_id
and dp.panel_id = 12


---------------------------------------------------------B02 - Calculate the durations pre and post capping for Live and Recorded ------------------------------------------------------------------

---lets calculate some time results pre and post capping

select viewing_date
,subscriber_id
,live_recorded
--,metric_short_name
,primary_secondary_flag
    , sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) / 3600 average_hours
    , sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) duration_postcapping
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) / 3600 average_hours_postcapping
into #stb_event_capped_uncapped_durations
FROM
#stb_event_capped_uncapped
where panel_id = 12
group by viewing_date,subscriber_id,live_recorded
--,metric_short_name
,primary_secondary_flag

---------------------------------------------------------B03 - Calculate the durations pre and post capping for Total ------------------------------------------------------------------

select viewing_date
,subscriber_id
,'TOTAL' live_recorded
--,metric_short_name
,primary_secondary_flag
    , sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) duration
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,event_end_date_time_utc )) / 3600 average_hours
    , sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) duration_postcapping
, 1.0 * sum(DATEDIFF(ss,event_start_date_time_utc,calculated_end_time )) / 3600 average_hours_postcapping
into #stb_event_capped_uncapped_durations_total
FROM
#stb_event_capped_uncapped
where panel_id = 12
group by viewing_date,subscriber_id,'TOTAL'
--,metric_short_name
,primary_secondary_flag

---even more woo hoo.  Time to collect the metric result and derive our benchmark information so that
---we can see what needs some checking

---------------------------------------------------------B04 - Calculate the averages for STB pre and Post Capping ------------------------------------------------------------------

select * into #stb_duration_results
from
(select viewing_date,live_recorded, primary_secondary_flag, cast(sum(average_hours)/count(distinct subscriber_id) as decimal (6,3)) pre_capped,
cast(sum(average_hours_postcapping)/count(distinct subscriber_id) as decimal (6,3)) post_capped
from #stb_event_capped_uncapped_durations
group by viewing_date,live_recorded, primary_secondary_flag
union all
select viewing_date,live_recorded, primary_secondary_flag, cast(sum(average_hours)/count(distinct subscriber_id) as decimal (6,3)) pre_capped,
cast(sum(average_hours_postcapping)/count(distinct subscriber_id) as decimal (6,3)) post_capped
from #stb_event_capped_uncapped_durations_total
group by viewing_date,live_recorded, primary_secondary_flag) t

---------------------------------------------------------B05 - Relate the results to each of the individual metrics involved ------------------------------------------------------------------


select * into #stb_durations_results_final
from
(select viewing_date,'daily_viewing_stb_avg_precapped_primary_live' metric_short_name, pre_capped metric_result
from #stb_duration_results
where live_recorded = 'LIVE'
AND PRIMARY_SECONDARY_FLAG = 'P'
union all
select viewing_date,'daily_viewing_stb_avg_postcapped_primary_live' metric_short_name, post_capped metric_result
from #stb_duration_results
where live_recorded = 'LIVE'
AND PRIMARY_SECONDARY_FLAG = 'P'
union all
select viewing_date,'daily_viewing_stb_avg_precapped_primary_recorded' metric_short_name, pre_capped metric_result
from #stb_duration_results
where live_recorded = 'RECORDED'
AND PRIMARY_SECONDARY_FLAG = 'P'
union all
select viewing_date,'daily_viewing_stb_avg_postcapped_primary_recorded' metric_short_name, post_capped metric_result
from #stb_duration_results
where live_recorded = 'RECORDED'
AND PRIMARY_SECONDARY_FLAG = 'P'
union all
select viewing_date,'daily_viewing_stb_avg_precapped_secondary_live' metric_short_name, pre_capped metric_result
from #stb_duration_results
where live_recorded = 'LIVE'
AND PRIMARY_SECONDARY_FLAG = 'S'
union all
select viewing_date,'daily_viewing_stb_avg_postcapped_secondary_live' metric_short_name, post_capped metric_result
from #stb_duration_results
where live_recorded = 'LIVE'
AND PRIMARY_SECONDARY_FLAG = 'S'
union all
select viewing_date,'daily_viewing_stb_avg_precapped_secondary_recorded' metric_short_name, pre_capped metric_result
from #stb_duration_results
where live_recorded = 'RECORDED'
AND PRIMARY_SECONDARY_FLAG = 'S'
union all
select viewing_date,'daily_viewing_stb_avg_postcapped_secondary_recorded' metric_short_name, post_capped metric_result
from #stb_duration_results
where live_recorded = 'RECORDED'
AND PRIMARY_SECONDARY_FLAG = 'S'
union all
select viewing_date,'daily_viewing_stb_avg_precapped_secondary_total' metric_short_name, pre_capped metric_result
from #stb_duration_results
where live_recorded = 'TOTAL'
AND PRIMARY_SECONDARY_FLAG = 'S'
union all
select viewing_date,'daily_viewing_stb_avg_postcapped_secondary_total' metric_short_name, post_capped metric_result
from #stb_duration_results
where live_recorded = 'TOTAL'
AND PRIMARY_SECONDARY_FLAG = 'S'
union all
select viewing_date,'daily_viewing_stb_avg_precapped_primary_total' metric_short_name, pre_capped metric_result
from #stb_duration_results
where live_recorded = 'TOTAL'
AND PRIMARY_SECONDARY_FLAG = 'P'
union all
select viewing_date,'daily_viewing_stb_avg_postcapped_primary_total' metric_short_name, post_capped metric_result
from #stb_duration_results
where live_recorded = 'TOTAL'
AND PRIMARY_SECONDARY_FLAG = 'P') t

---------------------------------------------------------B06 - join back to metrics table to get the metric ids etc...-------------------------------------------

select daily_dur.metric_result, daily_dur.viewing_date, dm_det.*
--dq_vm_id,viewing_date,metric_result,dm_det.metric_short_name, dm_det.metric_benchmark, dm_det.metric_tolerance_amber, dm_det.metric_tolerance_red
into #stb_durations_comparison_final
from #stb_durations_results_final daily_dur, #data_metrics_detail dm_det
where daily_dur.metric_short_name = dm_det.metric_short_name

---------------------------------------------------------B07 - push the results for all metrics through the benchmark functionality...-------------------------------------------

select a.*,
metric_benchmark_check(metric_result, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag
into #stb_durations_comparison_final_results
from #stb_durations_comparison_final a

---with our results in hand, lets enter them into the repository and do summit!!

---------------------------------------------------------C01 - Place the metrics results into the Repository Table ------------------------------------------------------------------


insert into data_quality_vespa_repository
(dq_run_id
,viewing_data_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,load_timestamp)
select @CP2_build_ID
,viewing_date
,dq_vm_id
,metric_result
,metric_tolerance_amber
,metric_tolerance_red
,metric_rag
,getdate()
from
#stb_durations_comparison_final_results


---------------------------------------------------------D01 - Drop Temp Tables ------------------------------------------------------------------

EXECUTE logger_add_event @RunID , 3,'Data Quality STB Checks End for Date '||cast (@target_date as varchar(20))


drop table #stb_durations_comparison_final_results
drop table #stb_duration_results
drop table #stb_durations_comparison_final
drop table #data_metrics_detail
drop table #stb_event_capped_uncapped_durations_total
drop table #stb_event_capped_uncapped_durations
drop table #stb_event_capped_uncapped

end

go

grant execute on Data_Quality_Vespa_STB_Checks to vespa_group_low_security, sk_prodreg