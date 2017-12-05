/*

purpose:
to develop code for IC, stripping SQL code out from spss

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
into #IC_calls_in1
from yarlagaddar.View_CUST_CALLS_HIST
where TypeOfEvent = 'IC'
;


create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;

select *,@Reference as Reference
into #IC_calls_in3
from #IC_calls_in2

;
-- select
select *
into #IC_calls_in2
from #IC_calls_in1
where account_number like @likestring -- insert the variable

;
select *
into #IC_calls_in_output
from
#IC_calls_in3 fut3
inner join
SourceDates sd
on fut3.Reference=sd.Reference
;


