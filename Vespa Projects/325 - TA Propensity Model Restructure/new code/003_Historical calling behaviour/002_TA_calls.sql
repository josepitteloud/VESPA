/*

purpose:
to develop code for TA, stripping SQL code out from spss

supernode - churn_history, file 2014-03-12_TA Model Development - ETL v3.0.str

*/



-- first branch
select *
into #TA_calls1
from #TA_calls_in_output
where
event_dt > [2_Years_Prior] and event_dt =< Snapshot_Date

select
account_number
,max(event_dt)
into #TA_calls2
from #TA_calls1
group by account_number

select
account_number
,event_dt_Max as Date_of_Last_TA_Call
into #TA_calls3
from #TA_calls2
-- output of first branch is #TA_calls3


-- second branch
select *
into #TA_calls4
from #TA_calls_in_output
where
event_dt > [2_Years_Prior] and event_dt =< Snapshot_Date

select account_number
,count() as TA_Calls_Last_2_Years
into #TA_calls5
from #TA_calls4
-- output of second branch is #TA_calls5


-- third branch
select *
into #TA_calls6
from #TA_calls_in_output
where
event_dt > '1_Year_Prior' and event_dt =< Snapshot_Date

select account_number
,count() as TA_Calls_Last_Year
into #TA_calls7
from #TA_calls6
-- output of third branch is #TA_calls7



-- fourth branch
select *
into #TA_calls8
from #TA_calls_in_output
where
event_dt > '9_Months_Prior' and event_dt <= Snapshot_Date


select account_number
,count() as TA_Calls_Last_9_Months
into #TA_calls9
from #TA_calls8
-- output of fourth branch is #TA_calls9


-- fifth branch
select *
into #TA_calls10
from #TA_calls_in_output
where
event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date


select account_number
,count() as TA_Calls_Last_6_Months
into #TA_calls11
from #TA_calls10
-- output of fifth branch is #TA_calls11


-- sixth branch
select *
into #TA_calls12
from #TA_calls_in_output
where
event_dt > '3_Months_Prior' and event_dt <= Snapshot_Date

select account_number
,count() as TA_Calls_Last_3_Months
into #TA_calls13
from #TA_calls12
-- output of sixth branch is #TA_calls13


-- Leo: to complete we do the final merge
select *
into #TA_calls_output
from
#TA_calls3 t3
full outer join
#TA_calls5 t5
on t3.account_number=t5.account_number
full outer join
#TA_calls7 t7
on t5.account_number=t7.account_number
full outer join
#TA_calls9 t9
on t7.account_number=t9.account_number
full outer join
#TA_calls11 t11
on t9.account_number=t11.account_number
full outer join
#TA_calls11 t13
on t11.account_number=t13.account_number


-- output to stream is table #TA_calls_output
