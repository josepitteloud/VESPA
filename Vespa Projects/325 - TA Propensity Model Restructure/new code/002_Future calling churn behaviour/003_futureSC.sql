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
into #futureSC1
 from yarlagaddar.View_CUST_CHURN_HIST
where TypeofEvent = 'SC'
;

create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;
-- select
select *
into #futureSC2
from #futureSC1
where account_number like @likestring -- insert the variable

;

select *,@Reference as Reference
into #futureSC3
from #futureSC2
;


select *
into #futureSC4
from
#futureSC3 hist3
inner join
SourceDates  sd
on hist3.Reference=sd.Reference

;

select *
into #futureSC5
from #futureSC4
effective_from_dt > Snapshot_Date and effective_from_dt <= [3_Months_Future]


-- restructure
select *
, case when type_of_event='SC' then event_dt else cast(NULL as date) as type_of_event_SC_event_dt
into #futureSC6
from #futureSC5

--aggregate
select account_number
,max(type_of_event_SC_event_dt) as type_of_event_SC_event_dt_Max
group by account_number
into #futureSC7
from #futureSC6

--flag
select account_number
,type_of_event_SC_event_dt_Max
,case when type_of_event_SC_event_dt_Max is not NULL then 1 else 0 as type_of_event_SC_event_dt_Max_flag
into #futureSC8
from #futureSC7

-- filter
select
account_number
,TypeOfEvent_SC_event_dt_Max_flag as SC_In_Next_4_Months_Flag
into #futureSC_output
from #futureSC8

