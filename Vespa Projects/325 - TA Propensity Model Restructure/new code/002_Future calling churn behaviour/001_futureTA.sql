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
--select
select
account_number
,event_dt
,TypeOfEvent
into #futureTA1
from yarlagaddar.View_CUST_CALLS_HIST
where TypeOfEvent = 'TA'
;



create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;
-- reference
select *
into #futureTA2
from #futureTA1
where account_number like @likestring -- insert the variable

;
--merge
select *,@Reference as Reference
into #futureTA3
from #futureTA2
;

--select
select *
into #futureTA4
from
#futureTA3 fut3
inner join
SourceDates sd
on fut3.Reference=sd.Reference
;


select *
into #futureTA5
from #futureTA4
where
event_dt > Snapshot_Date and event_dt <= [3_Months_Future]


-- restructure
select *
, case when type_of_event='TA' then event_dt else cast(NULL as date) as type_of_event_TA_event_dt
into #futureTA6
from #futureTA5

--aggregate
select account_number
,max(type_of_event_TA_event_dt) as type_of_event_TA_event_dt_Max
group by account_number
into #futureTA7
from #futureTA6

--flag
select account_number
,type_of_event_TA_event_dt_Max
,case when type_of_event_TA_event_dt_Max is not NULL then 1 else 0 as type_of_event_TA_event_dt_Max_flag
into #futureTA8
from #futureTA7

-- filter
select
account_number
,effective_from_dt_max
,effective_from_dt_max
,TypeOfEvent_TA_event_dt_Max
,TypeOfEvent_TA_event_dt_Max_flag
into #futureTA_output
from #futureTA8



