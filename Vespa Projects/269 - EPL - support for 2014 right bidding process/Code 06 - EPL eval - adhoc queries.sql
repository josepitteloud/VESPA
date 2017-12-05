/*###############################################################################
# Created on:   26/03/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - adhoc queries
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 26/03/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Average metric values for Sports subs
select
      a.Metric,
      a.Category,
      sum(a.Calculated_SOC) as Total_SOC,
      count(*) as Accounts_Num,
      avg(a.Calculated_SOC) as Average_SOC
  from EPL_03_SOCs_Summaries a,
       EPL_04_Profiling_Variables b
 where a.Account_Number = b.Account_number
   and a.Period = b.Period
   and a.Period = 1
   and b.Prem_Sports > 0
 group by a.Metric, a.Category;


select
      a.Metric,
      a.Category,
      sum(a.Calculated_SOV) as Total_SOV,
      count(*) as Accounts_Num,
      avg(a.Calculated_SOV) as Average_SOV
  from EPL_03_SOVs a,
       EPL_04_Profiling_Variables b
 where a.Account_Number = b.Account_number
   and a.Period = b.Period
   and a.Period = 1
   and b.Prem_Sports > 0
 group by a.Metric, a.Category;



  -- ##############################################################################################################
  -- ##############################################################################################################

















