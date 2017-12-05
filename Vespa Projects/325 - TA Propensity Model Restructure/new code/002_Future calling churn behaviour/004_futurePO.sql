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
,effective_from_dt
,effective_to_dt
,TypeOfEvent
into #futurePO1
 from yarlagaddar.View_CUST_CHURN_HIST
where TypeofEvent = 'PO'
;

create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;
-- select
select *
into #futurePO2
from #futurePO1
where account_number like @likestring -- insert the variable

;

select *,@Reference as Reference
into #futurePO3
from #futurePO2
;


select *
into #futurePO4
from
#futurePO3 hist3
inner join
SourceDates  sd
on hist3.Reference=sd.Reference

;

select *
into #futurePO5
from #futurePO4
effective_from_dt > Snapshot_Date and effective_from_dt <= [3_Months_Future]


-- restructure
select *
, case when type_of_event='PO' then event_dt else cast(NULL as date) as type_of_event_PO_event_dt
into #futurePO6
from #futurePO5

--aggregate
select account_number
,max(type_of_event_PO_event_dt) as type_of_event_PO_event_dt_Max
group by account_number
into #futurePO7
from #futurePO6

--flag
select account_number
,type_of_event_PO_event_dt_Max
,case when type_of_event_PO_event_dt_Max is not NULL then 1 else 0 as type_of_event_PO_event_dt_Max_flag
into #futurePO8
from #futurePO7

-- filter
select
account_number
,TypeOfEvent_PO_event_dt_Max_flag as PO_In_Next_4_Months_Flag
into #futurePO_output
from #futurePO8

