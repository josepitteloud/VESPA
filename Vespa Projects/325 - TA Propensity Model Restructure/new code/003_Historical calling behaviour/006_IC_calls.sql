/*

purpose:
to develop code for IC, stripping SQL code out from spss

supernode - churn_history, file 2014-03-12_TA Model Development - ETL v3.0.str

*/



-- first branch
select *
into #IC_calls1
from #IC_calls_in_output
where
event_dt > [2_Years_Prior] and event_dt =< Snapshot_Date

select
account_number
,max(event_dt)
into #IC_calls2
from #IC_calls1
group by account_number

select
account_number
,event_dt_Max as Date_of_Last_IC_Call
into #IC_calls3
from #IC_calls2
-- output of first branch is #IC_calls3


-- second branch
select *
into #IC_calls4
from #IC_calls_in_output
where
event_dt > [2_Years_Prior] and event_dt =< Snapshot_Date

select account_number
,count() as IC_Calls_Last_2_Years
into #IC_calls5
from #IC_calls4
-- output of second branch is #IC_calls5


-- third branch
select *
into #IC_calls6
from #IC_calls_in_output
where
event_dt > [1_Year_Prior] and event_dt =< Snapshot_Date

select account_number
,count() as IC_Calls_Last_Year
into #IC_calls7
from #IC_calls6
-- output of third branch is #IC_calls7



-- fourth branch
select *
into #IC_calls8
from #IC_calls_in_output
where
event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date


select account_number
,count() as IC_Calls_Last_9_Months
into #IC_calls9
from #IC_calls8
-- output of fourth branch is #IC_calls9


-- fifth branch
select *
into #IC_calls10
from #IC_calls_in_output
where
event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date


select account_number
,count() as IC_Calls_Last_6_Months
into #IC_calls11
from #IC_calls10
-- output of fifth branch is #IC_calls11


-- sixth branch
select *
into #IC_calls12
from #IC_calls_in_output
where
event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date

select account_number
,count() as IC_Calls_Last_3_Months
into #IC_calls13
from #IC_calls12
-- output of sixth branch is #IC_calls13


-- Leo: to complete we do the final merge
select *
into #IC_calls_output
from
#IC_calls3 t3
full outer join
#IC_calls5 t5
on t3.account_number=t5.account_number
full outer join
#IC_calls7 t7
on t5.account_number=t7.account_number
full outer join
#IC_calls9 t9
on t7.account_number=t9.account_number
full outer join
#IC_calls11 t11
on t9.account_number=t11.account_number
full outer join
#IC_calls11 t13
on t11.account_number=t13.account_number


-- output to stream is table #IC_calls_output
