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
into #futureAB1
 from yarlagaddar.View_CUST_CHURN_HIST
where TypeofEvent = 'AB'
;

create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;
-- select
select *
into #futureAB2
from #futureAB1
where account_number like @likestring -- insert the variable

;

select *,@Reference as Reference
into #futureAB3
from #futureAB2
;


select *
into #futureAB4
from
#futureAB3 hist3
inner join
SourceDates  sd
on hist3.Reference=sd.Reference

;

select *
into #futureAB5
from #futureAB4
effective_from_dt > Snapshot_Date and effective_from_dt <= [3_Months_Future]


-- restructure
select *
, case when type_of_event='AB' then event_dt else cast(NULL as date) as type_of_event_AB_event_dt
into #futureAB6
from #futureAB5

--aggregate
select account_number
,max(type_of_event_AB_event_dt) as type_of_event_AB_event_dt_Max
group by account_number
into #futureAB7
from #futureAB6

--flag
select account_number
,type_of_event_AB_event_dt_Max
,case when type_of_event_AB_event_dt_Max is not NULL then 1 else 0 as type_of_event_AB_event_dt_Max_flag
into #futureAB8
from #futureAB7

-- filter
select
account_number
,TypeOfEvent_AB_event_dt_Max_flag as AB_in_NEXT_3_months_flag
into #futureAB_output
from #futureAB8

