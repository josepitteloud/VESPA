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
,status_code
into #table_churn_history1
 from yarlagaddar.View_CUST_CHURN_HIST

;

create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;
-- select
select *
into #table_churn_history2
from #table_churn_history1
where account_number like @likestring -- insert the variable

;

select *,@Reference as Reference
into #table_churn_history3
from #table_churn_history2
;


select *
into #table_churn_history4
from
#table_churn_history3 hist3
inner join
SourceDates  sd
on hist3.Reference=sd.Reference

;

select *
into #table_churn_history_output
from #table_churn_history4
effective_from_dt <= Snapshot_Date and effective_from_dt >[2_Years_Prior]





