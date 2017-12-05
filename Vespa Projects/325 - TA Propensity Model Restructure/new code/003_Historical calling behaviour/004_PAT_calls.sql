/*

purpose:
to develop code for PAT, stripping SQL code out from spss

supernode - churn_history, file 2014-03-12_TA Model Development - ETL v3.0.str

*/



-- first branch
select *
into #PAT_calls1
from #PAT_calls_in_output
where
event_dt > [2_Years_Prior] and event_dt =< Snapshot_Date

select
account_number
,max(event_dt)
into #PAT_calls2
from #PAT_calls1
group by account_number

select
account_number
,event_dt_Max as Date_of_Last_PAT_Call
into #PAT_calls3
from #PAT_calls2
-- output of first branch is #PAT_calls3


-- second branch
select *
into #PAT_calls4
from #PAT_calls_in_output
where
event_dt > [2_Years_Prior] and event_dt =< Snapshot_Date

select account_number
,count() as PAT_Calls_Last_2_Years
into #PAT_calls5
from #PAT_calls4
-- output of second branch is #PAT_calls5


-- third branch
select *
into #PAT_calls6
from #PAT_calls_in_output
where
event_dt > [1_Year_Prior] and event_dt =< Snapshot_Date

select account_number
,count() as PAT_Calls_Last_Year
into #PAT_calls7
from #PAT_calls6
-- output of third branch is #PAT_calls7



-- fourth branch
select *
into #PAT_calls8
from #PAT_calls_in_output
where
event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date


select account_number
,count() as PAT_Calls_Last_9_Months
into #PAT_calls9
from #PAT_calls8
-- output of fourth branch is #PAT_calls9


-- fifth branch
select *
into #PAT_calls10
from #PAT_calls_in_output
where
event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date


select account_number
,count() as PAT_Calls_Last_6_Months
into #PAT_calls11
from #PAT_calls10
-- output of fifth branch is #PAT_calls11


-- sixth branch
select *
into #PAT_calls12
from #PAT_calls_in_output
where
event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date

select account_number
,count() as PAT_Calls_Last_3_Months
into #PAT_calls13
from #PAT_calls12
-- output of sixth branch is #PAT_calls13


-- Leo: to complete we do the final merge
select *
into #PAT_calls_output
from
#PAT_calls3 t3
full outer join
#PAT_calls5 t5
on t3.account_number=t5.account_number
full outer join
#PAT_calls7 t7
on t5.account_number=t7.account_number
full outer join
#PAT_calls9 t9
on t7.account_number=t9.account_number
full outer join
#PAT_calls11 t11
on t9.account_number=t11.account_number
full outer join
#PAT_calls11 t13
on t11.account_number=t13.account_number


-- output to stream is table #PAT_calls_output
