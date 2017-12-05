/*###############################################################################
# Created on:   31/07/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Enablement - process execution wrapper script
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - all procedures and related tables
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 31/07/2013  SBE   Initial version
# 26/11/2013  SBE   Threading mechanism implemented
# 13/01/2014  SBE   Wrapper script updated with relevant parameters and changes to
#                   procedures
# 21/02/2014  SBE   Wrapper script streamlined & removed reference to "bednaszs"
#                   schema
#
###############################################################################*/


  -- Work out relevant scaling date for the period (to be published)
select
      adjusted_event_start_date_vespa as Scaling_Date,
      count(*) as PROD_Records,
      count(distinct Account_Number) as PROD_Accounts,
      sum(calculated_scaling_weight) as PROD_Sum_Of_Weights,
      max(c.vespa_panel) as VA_Accounts,
      max(c.sum_of_weights) as VA_Sum_Of_Weights
  from sk_prod.viq_viewing_data_scaling a,
       VESPA_Shared.Aggr_Period_Dim b,
       vespa_analysts.sc2_Metrics c
 where a.adjusted_event_start_date_vespa between date(b.Period_Start) and date(b.Period_End)
   and b.Period_Key = 9
   and c.scaling_date = a.adjusted_event_start_date_vespa
 group by Scaling_Date
 order by Scaling_Date;


  -- ###############################################################################
create variable @varPeriodKey smallint;
create variable @varUsername varchar(30);
set @varPeriodKey = 12;
set @varUsername  = user_name();


  -- ##### Create schedule #####
commit;
execute VAggr_0_Schedule @varPeriodKey, '', null;                                           -- [Period key], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_0_Schedule_v01', 4;


  -- ##### Account attributes & raw viewing data #####
commit;
execute VAggr_1_Account_Attributes @varPeriodKey, '2013-12-01', 'PROD', '', null;           -- [Period key], [Day for scaling weight], [Scaling weights source (PROD or VA)], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_1_Acc_Attr_v01', 4;

commit;
execute VAggr_2_Viewing_Data @varPeriodKey, null, 'vespa_dp_prog_viewed_201312', '', null;  -- [Period key], [Manual start date], [Source table name], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_2_Vw_Data_v01', 4;


  -- ##### Base aggregations #####
commit;
execute VAggr_3_Base_Aggr_Scheduling @varPeriodKey, @varUsername, '', null;                 -- [Period key], [Schema], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_3_Bs_Schdl_v01', 4;


commit;
execute VAggr_3_Base_Aggr @varPeriodKey, @varUsername, 1, '', null;                         -- [Period key], [Destination schema], [Thread Id], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_3_Base_v01', 4;

commit;
execute VAggr_3_Base_Aggr @varPeriodKey, @varUsername, 2, '', null;                         -- [Period key], [Destination schema], [Thread Id], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_3_Base_v01', 4;

commit;
execute VAggr_3_Base_Aggr @varPeriodKey, @varUsername, 3, '', null;                         -- [Period key], [Destination schema], [Thread Id], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_3_Base_v01', 4;

commit;
execute VAggr_3_Base_Aggr @varPeriodKey, @varUsername, 4, '', null;                         -- [Period key], [Destination schema], [Thread Id], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_3_Base_v01', 4;


commit;
execute VAggr_3_Base_Aggr_Assembly @varPeriodKey, @varUsername, 'VESPA_Shared', '', null;   -- [Period key], [Source schema], [Destination schema], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_3_Bs_Asmbl_v01', 4;


  -- ##### Get summary to check if all aggregations and groupings have been calculated correctly #####
select
      a.Aggregation_Key,
      b.Period_Key,
      a.Run_Processed_Flag,
      a.Grouping_Run,
      a.Grouping_Processed_Flag,
      count(distinct Metric_Group_Key) as Bin_Groups,
      count(*) as Cnt,
      min(Metric_Group_Key) as Min_Metric_Group_Key,
      max(Metric_Group_Key) as Max_Metric_Group_Key
  from VAggr_Meta_Run_Schedule a left join vespa_shared.aggr_fact b
        on a.aggregation_key = b.aggregation_key
       and a.period_key = b.period_key
 where a.period_key = @varPeriodKey
 group by a.Aggregation_Key, a.Run_Processed_Flag, a.Grouping_Run, a.Grouping_Processed_Flag, b.period_key
 order by a.Grouping_Run, a.Aggregation_Key;


  -- ##### Derived aggregations,grouping & customer aggregations #####
commit;
execute VAggr_4_Derived_Aggr @varPeriodKey, 'VESPA_Shared', '', null;                       -- [Period key], [Destination schema], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_4_Derived_v01', 4;

commit;
execute VAggr_5_Grouping @varPeriodKey, 'VESPA_Shared', '', null;                           -- [Period key], [Destination schema], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_5_Grouping_v01', 4;

commit;
execute VAggr_6_Custom_Aggr @varPeriodKey, 'VESPA_Shared', '', null;                        -- [Period key], [Destination schema], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_6_Custom_v01', 4;


  -- ##### Publishing #####
commit;
execute VAggr_7_Publishing @varPeriodKey, 1, 1, 1, '', null;                                -- [Period key], [Publish raw], [Publish LL], [Publish HL], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_7_Publish_v01', 4;


  -- ##### Cleaning up #####
commit;
execute VAggr_8_Cleanup @varPeriodKey, 1, '', null;                                         -- [Period key], [Drop working data], [Process identifier], [Logger ID]
execute logger_get_latest_job_events 'VAggr_8_Cleanup_v01', 4;




  -- ##############################################################################################################
  -- ##############################################################################################################



















