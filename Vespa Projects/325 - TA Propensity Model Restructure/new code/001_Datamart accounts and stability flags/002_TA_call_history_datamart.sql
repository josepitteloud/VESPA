/*

purpose:
to develop code for TA, stripping SQL code out from spss

supernode - churn_history, file 2014-03-12_TA Model Development - ETL v3.0.str

*/

create or replace variable @Reference integer;

set @Reference=201308
;

create or replace variable @Sample_1_EndString varchar(4)
;

set @Sample_1_EndString='27'

;
select
account_number
,event_dt
,TypeOfEvent
into #TA_call_history1
from yarlagaddar.View_CUST_CALLS_HIST
where TypeOfEvent = 'TA'
;


create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;
-- select
select *
into #TA_call_history2
from #TA_call_history1
where account_number like @likestring -- insert the variable

;

select *,@Reference as Reference
into #TA_call_history3
from #TA_call_history2
;

select *
into #TA_call_history4
from
#TA_call_history3 his3
inner join
SourceDates sd
on his3.Reference=sd.Reference
;

select *
into #TA_call_history_output
from #TA_call_history4
where
event_dt <= Snapshot_Date and event_dt >[2_Years_Prior]



