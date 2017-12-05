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
into #futureTA_4_6_m1
from yarlagaddar.View_CUST_CALLS_HIST
where TypeOfEvent = 'TA'
;



create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;
-- select
select *
into #futureTA_4_6_m2
from #futureTA_4_6_m1
where account_number like @likestring -- insert the variable

;

select *,@Reference as Reference
into #futureTA_4_6_m3
from #futureTA_4_6_m2
;


select *
into #futureTA_4_6_m4
from
#futureTA_4_6_m3 fut3
inner join
SourceDates sd
on fut3.Reference=sd.Reference
;


select *
into #futureTA_4_6_m5
from #futureTA_4_6_m4
where
event_dt > '3_Months_Future' and event_dt <= [6_Months_Future]

-- restructure
select *
, case when type_of_event='TA' then event_dt else cast(NULL as date) as type_of_event_TA_event_dt
into #futureTA_4_6_m6
from #futureTA_4_6_m5

--aggregate
select account_number
,max(type_of_event_TA_event_dt) as type_of_event_TA_event_dt_Max
group by account_number
into #futureTA_4_6_m7
from #futureTA_4_6_m6

--flag
select account_number
,type_of_event_TA_event_dt_Max
,case when type_of_event_TA_event_dt_Max is not NULL then 1 else 0 as type_of_event_TA_event_dt_Max_flag
into #futureTA_4_6_m8
from #futureTA_4_6_m7

-- filter -- Leoooo changed from TA_in_3-6_Months_Flag to TA_in_3_6_Months_Flag
select
account_number
,TypeOfEvent_TA_event_dt_Max_flag as TA_in_3_6_Months_Flag
into #futureTA_4_6_m_output
from #futureTA_4_6_m8



