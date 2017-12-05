/*

supernode - historical calling churn, file 2014-03-12_TA Model Development - ETL v3.0.str

*/

select *
into #HistoricalCallingBehaviour1
from
#TA_calls_output t3
full outer join
#PAT_calls_output t5
on t3.account_number=t5.account_number
full outer join
#IC_calls_output t7
on t5.account_number=t7.account_number


select
account_number
,case when IC_Calls_Last_Year is null then 0 else IC_Calls_Last_Year end
,case when IC_Calls_Last_3_Months is null then 0 else IC_Calls_Last_3_Months end
,case when IC_Calls_Last_6_Months is null then 0 else IC_Calls_Last_6_Months end
,case when IC_Calls_Last_9_Months is null then 0 else IC_Calls_Last_9_Months end
,case when IC_Calls_Last_2_Years is null then 0 else IC_Calls_Last_2_Years end
,Date_of_Last_IC_Call
,case when PAT_Calls_Last_Year is null then 0 else PAT_Calls_Last_Year end
,case when PAT_Calls_Last_3_Months is null then 0 else PAT_Calls_Last_3_Months end
,case when PAT_Calls_Last_6_Months is null then 0 else PAT_Calls_Last_6_Months end
,case when PAT_Calls_Last_9_Months is null then 0 else PAT_Calls_Last_9_Months end
,case when PAT_Calls_Last_2_Years is null then 0 else PAT_Calls_Last_2_Years end
,Date_of_Last_PAT_Call
,case when TA_Calls_Last_Year is null then 0 else TA_Calls_Last_Year end
,case when TA_Calls_Last_3_Months is null then 0 else TA_Calls_Last_3_Months end
,case when TA_Calls_Last_6_Months is null then 0 else TA_Calls_Last_6_Months end
,case when TA_Calls_Last_9_Months is null then 0 else TA_Calls_Last_9_Months end
,case when TA_Calls_Last_2_Years is null then 0 else TA_Calls_Last_2_Years end
,Date_of_Last_TA_Call
into #HistoricalCallingBehaviour_output
from #HistoricalCallingBehaviour1

-- output to stream is table #HistoricalCallingChurn_output
