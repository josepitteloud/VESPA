--Need to distinguish if we have an empty box is it because they do not have a rule or are they not in sky anymore.

/*

Look at customer value segments 'Bedding In' and 'Unstable' over time a period of 1 year

*/
-- Compile historical data
select *
into cuscan_segments_history
from (
        select *, '2013-04-11' as cb_data_date from atrisk_results          union all
        select *, '2013-01-11' as cb_data_date from atrisk_results_20130111 union all
        select *, '2012-10-11' as cb_data_date from atrisk_results_20121011
) t;
--Create segment combining cuscan type and rule into cuscan_segment
alter table cuscan_segments_history add cuscan_segment varchar(40);
update      cuscan_segments_history
        set cuscan_segment =
        (
                case
                        when cuscan_type IS NULL and rule = 0
                        then 'Not in churn segments'
                        else cuscan_type||' '||rule
                end
        );

--Check
select top 20 * from cuscan_segments_history

-- Identify value segments by quarter over the last year
select account_number
        ,coalesce(max(Apr2013),'Not in Sky') as April_2013
        ,coalesce(max(Jan2013),'Not in Sky') as January_2013
        ,coalesce(max(Oct2012),'Not in Sky') as October_2012
into cuscan_segments_quarters
from (
select account_number
        ,case when cb_data_date = '2013-04-11' then cuscan_segment end as Apr2013
        ,case when cb_data_date = '2013-01-11' then cuscan_segment end as Jan2013
        ,case when cb_data_date = '2012-10-11' then cuscan_segment end as Oct2012
from cuscan_segments_history
--where month > '2012-03-01' -- remove data from Feb 2012
) t
group by account_number;

-- Have a look
select top 20 * from cuscan_segments_quarters

-- Aggregate # of customers by their value segment changes over time
select   "April 2013"
        ,"January 2013"
        ,"October 2012"
        ,count(*) as 'Number_Customers'
from (
select account_number
        ,case when   April_2013 IS NULL then 'Not in Sky' else   April_2013 end as 'April 2013'
        ,case when January_2013 IS NULL then 'Not in Sky' else January_2013 end as 'January 2013'
        ,case when October_2012 IS NULL then 'Not in Sky' else October_2012 end as 'October 2012'
from cuscan_segments_quarters
) t
group by "April 2013"
        ,"January 2013"
        ,"October 2012"
order by Number_Customers desc;

create hg index ind_acc on cuscan_segments_history(account_number);
create lf index ind_dat on cuscan_segments_history(cb_data_date);

--Do a full join on date and account number with account_segment_lagged to
--include every possible account and date
-- Get the raw data first
--Any account/date combination which is now null is assumed not to be in Sky
select   acc.account_number
        ,dat.cb_data_date
        ,t2.value_seg_updated
        ,t2.churn_rate
        ,t2.colour
        ,coalesce(t2.cuscan_segment , 'Not in Sky') as cuscan_segment
into cuscan_segments_transition_temp
from
(select distinct account_number from cuscan_segments_history) acc
full join
(select distinct cb_data_date from cuscan_segments_history) dat
on 1=1
left join
(select * from cuscan_segments_history) t2
on acc.account_number = t2.account_number
and dat.cb_data_date = t2.cb_data_date;
--29600586 Row(s) affected
select top 20 * from cuscan_segments_transition_temp;

--Create table showing every account and date as well as the lagged cuscan segment
--Drop rows where date equals '2012-10-11' as we do not require previous segment
select *
into account_segment_lagged
from
(
        select   account_number
                ,cb_data_date
                ,value_seg_updated
                ,churn_rate
                ,colour
                ,lag(cuscan_segment) over (partition by account_number
                                             order by cb_data_date) as prev_cuscan_seg
                ,cuscan_segment as cuscan_segment
        from cuscan_segments_transition_temp
) t
where cb_data_date > '2012-10-11';
--19733724 Row(s) affected
select top 20 * from account_segment_lagged;

--Find group of cuscan segments that have more than 100
select   coalesce(prev_cuscan_seg,'Not in Sky') as prev_cuscan_seg
        ,coalesce(cuscan_segment ,'Not in Sky') as cuscan_segment
        ,count(*) as number_customers
into     cuscan_segment_lagged_count
from     account_segment_lagged
group by prev_cuscan_seg,cuscan_segment
having count(*) >= 100
order by number_customers desc
;
--384 Row(s) affected

--Check
select * from cuscan_segment_lagged_count;

create lf index acc_pseg on account_segment_lagged(prev_cuscan_seg);
create lf index acc_cseg on account_segment_lagged(cuscan_segment);
create lf index lag_pseg on cuscan_segment_lagged_count(prev_cuscan_seg);
create lf index lag_cseg on cuscan_segment_lagged_count(cuscan_segment);

-- Look at value changes in totals
select   t2.prev_cuscan_seg as 'Previous Segment'
        ,t2.cuscan_segment  as 'Current Segment'
--         coalesce(t2.prev_cuscan_seg,'Not in Churn Segments') as 'Previous Segment'
--         ,coalesce(t2.cuscan_segment ,'Not in Churn Segments') as 'Current Segment'
        ,cb_data_date as 'Date'
        ,churn_rate
        ,colour
        ,value_seg_updated
        ,count(*) as 'Number Customers'
into cuscan_segments_transition
from account_segment_lagged t1
right join
( select * from cuscan_segment_lagged_count ) t2
on  t2.prev_cuscan_seg = t1.prev_cuscan_seg
and t2.cuscan_segment  = t1.cuscan_segment
--where dat.cb_data_date > '2012-10-11' --not needed as we do not require previous segment for Oct 2012
group by t2.prev_cuscan_seg
        ,t2.cuscan_segment
--          coalesce(t2.prev_cuscan_seg,'Not in Churn Segments')
--         ,coalesce(t2.cuscan_segment ,'Not in Churn Segments')
        ,cb_data_date
        ,churn_rate
        ,colour
        ,value_seg_updated
order by 3,4 desc;
--1141 Row(s) affected

--Check
select * from cuscan_segments_transition;







--Do a full join on date and account number with account_segment_lagged to
--include every possible account and date
create hg index lag_acc on cuscan_segment_lagged(account_number);
create lf index lag_dat on cuscan_segment_lagged(cb_data_date);
-- Get the raw data first
--Any account/date combination which is now null is assumed not to be in Sky
select   acc.account_number
        ,dat.cb_data_date
        ,t2.value_seg_updated
        ,t2.churn_rate
        ,t2.colour
        ,coalesce(t2.cuscan_segment , 'Not in Sky') as cuscan_segment
into cuscan_segments_transition_temp
from
(select distinct account_number from cuscan_segments_history) acc
full join
(select distinct cb_data_date from cuscan_segments_history where cb_data_date > '2012-10-11') dat
on 1=1
left join
(select * from cuscan_segments_history) t2
on acc.account_number = t2.account_number
and dat.cb_data_date = t2.cb_data_date;


select top 20 * from cuscan_segments_transition_temp
select top 20 * from account_segment_lagged where account_number = '200000235644'
-- select   cuscan_segment
--         ,count(*) as c
-- from account_segment_lagged
-- group by cuscan_segment
-- order by cuscan_segment
--
--
-- select   prev_cuscan_seg
--         ,cuscan_segment
--         ,count(*) as c
-- from account_segment_lagged
-- where cuscan_segment = 'Not in Sky'
-- group by   prev_cuscan_seg
--         ,cuscan_segment
-- order by   c desc

--Find group of cuscan segments that have more than 100
select   coalesce(prev_cuscan_seg,'Not in Sky') as prev_cuscan_seg
        ,coalesce(cuscan_segment ,'Not in Sky') as cuscan_segment
        ,count(*) as number_customers
into     cuscan_segment_lagged_count
from     account_segment_lagged
group by prev_cuscan_seg,cuscan_segment
having count(*) >= 100
order by number_customers desc
;


create lf index ind_pseg on cuscan_segments_transition_temp(prev_cuscan_seg);
create lf index ind_cseg on cuscan_segments_transition_temp(cuscan_segment);
create lf index lag_pseg on cuscan_segment_lagged_count(prev_cuscan_seg);
create lf index lag_cseg on cuscan_segment_lagged_count(cuscan_segment);

-- Look at value changes in totals
select   t2.prev_cuscan_seg as 'Previous Segment'
        ,t2.cuscan_segment  as 'Current Segment'
--         coalesce(t2.prev_cuscan_seg,'Not in Churn Segments') as 'Previous Segment'
--         ,coalesce(t2.cuscan_segment ,'Not in Churn Segments') as 'Current Segment'
        ,cb_data_date as 'Date'
        ,churn_rate
        ,colour
        ,value_seg_updated
        ,count(*) as 'Number Customers'
into cuscan_segments_transition
from cuscan_segments_transition_temp t1
right join
( select * from cuscan_segment_lagged_count ) t2
on  t2.prev_cuscan_seg = t1.prev_cuscan_seg
and tw.cuscan_segment  = t1.cuscan_segment
--where dat.cb_data_date > '2012-10-11' --not needed as we do not require previous segment for Oct 2012
group by t2.prev_cuscan_seg
        ,t2.cuscan_segment
--          coalesce(t2.prev_cuscan_seg,'Not in Churn Segments')
--         ,coalesce(t2.cuscan_segment ,'Not in Churn Segments')
        ,cb_data_date
        ,churn_rate
        ,colour
        ,value_seg_updated
order by 3,4 desc;








































-- Aggregate information now
select   coalesce(prev_cuscan_seg,'Not in Sky') as 'Previous Segment'
        ,coalesce(cuscan_segment,'Not in Sky') as 'Current Segment'
        ,cb_data_date as 'Date'
        ,churn_rate
        ,colour
        ,value_seg_updated
        ,count(*) as 'Number Customers'
into cuscan_segments_transition
from (
select  *
from cuscan_segments_transition_temp
) t
group by coalesce(prev_cuscan_seg,'Not in Sky')
        ,coalesce(cuscan_segment ,'Not in Sky')
        ,cb_data_date
        ,churn_rate
        ,colour
        ,value_seg_updated
order by 3,4 desc



select * from cuscan_segment_lagged_count
where cuscan_segment = 'Long Tenure     12'
order by prev_cuscan_seg, cuscan_segment

select   account_number
        ,cb_data_date
        ,churn_rate
        ,colour
        ,value_seg_updated
        ,t.prev_cuscan_seg
        ,t.cuscan_segment
into    cuscan_segment_lagged
from    account_segment_lagged t
right join
( select * from cuscan_segment_lagged_count ) t2
on      t.prev_cuscan_seg = t2.prev_cuscan_seg
and     t.cuscan_segment  = t2.cuscan_segment;


-- select   prev_cuscan_seg
--         ,cuscan_segment
--         ,count(*) as number_customers
-- from     cuscan_segment_lagged
-- group by prev_cuscan_seg
--         ,cuscan_segment
-- order by number_customers
-- 
-- select top 20 * from cuscan_segment_lagged
-- where prev_cuscan_seg = 'Long Tenure     12'
--
-- create hg index lag_acc on cuscan_segment_lagged(account_number);
-- create lf index lag_dat on cuscan_segment_lagged(cb_data_date);
--
-- select   coalesce(prev_cuscan_seg,'Not in Sky') as 'Previous Segment'
--         ,coalesce(cuscan_segment ,'Not in Sky') as 'Current Segment'
--         ,cb_data_date as 'Date'
--         ,churn_rate
--         ,colour
--         ,value_seg_updated
--         ,count(*) as 'Number Customers'
-- into    cuscan_segments_transition2
-- from    cuscan_segment_lagged
-- group by coalesce(prev_cuscan_seg,'Not in Sky')
--         ,coalesce(cuscan_segment ,'Not in Sky')
--         ,cb_data_date
--         ,churn_rate
--         ,colour
--         ,value_seg_updated
-- -- 1054 Row(s) affected
-- 
-- select   coalesce(prev_cuscan_seg,'Not in Sky') as 'Previous Segment'
--         ,coalesce(cuscan_segment ,'Not in Sky') as 'Current Segment'
--         ,cb_data_date as 'Date'
--         ,count(*) as 'Number Customers'
-- from    cuscan_segment_lagged
-- group by coalesce(prev_cuscan_seg,'Not in Sky')
--         ,coalesce(cuscan_segment ,'Not in Sky')
--         ,cb_data_date
-- order by coalesce(prev_cuscan_seg,'Not in Sky')
--         ,coalesce(cuscan_segment ,'Not in Sky')
-- 
-- 
-- --Check what's there
-- select * from cuscan_segments_transition2
--


-- Look at value changes in totals
select   coalesce(t2.prev_cuscan_seg,'Not in Churn Segments') as 'Previous Segment'
        ,coalesce(t2.cuscan_segment ,'Not in Churn Segments') as 'Current Segment'
        ,dat.cb_data_date as 'Date'
        ,churn_rate
        ,colour
        ,value_seg_updated
        ,count(*) as 'Number Customers'
into cuscan_segments_transition
from
(select distinct account_number from cuscan_segments_history) acc
full join
(select distinct cb_data_date   from cuscan_segments_history) dat
on 1=1
left join
( select * from cuscan_segment_lagged ) t2
on  acc.account_number = t2.account_number
and dat.cb_data_date   = t2.cb_data_date
where dat.cb_data_date > '2012-10-11' --not needed as we do not require previous segment for Oct 2012
group by coalesce(t2.prev_cuscan_seg,'Not in Churn Segments')
        ,coalesce(t2.cuscan_segment ,'Not in Churn Segments')
        ,dat.cb_data_date
        ,churn_rate
        ,colour
        ,value_seg_updated
order by 3,4 desc;
-- 1055 Row(s) affected

--Check what's there
select * from cuscan_segments_transition


--Not too sure if the following commands are required
--------------------------------------
-- Look at value changes in totals
--------------------------------------

-- Get the raw data first
select   acc.account_number
        ,dat.cb_data_date
        ,t2.value_seg_updated
        ,t2.churn_rate
        ,t2.colour
        ,t2.prev_cuscan_seg
        ,t2.cuscan_segment
into cuscan_segments_transition_temp
from
(select distinct account_number from cuscan_segments_history) acc
full join
(select distinct cb_data_date from cuscan_segments_history) dat
on 1=1
left join
(select * from cuscan_segments_history) t2
on acc.account_number = t2.account_number
and dat.cb_data_date = t2.cb_data_date;
-- 29600586 Row(s) affected

select top 20 * from cuscan_segments_transition_temp

-- Aggregate information now
select   coalesce(prev_cuscan_seg,'Not in Sky') as 'Previous Segment'
        ,coalesce(cuscan_segment,'Not in Sky') as 'Current Segment'
        ,cb_data_date as 'Date'
        ,churn_rate
        ,colour
        ,value_seg_updated
        ,count(*) as 'Number Customers'
into cuscan_segments_transition
from (
select  *
from cuscan_segments_transition_temp
) t
where    cb_data_date > '2012-10-11'
group by coalesce(prev_cuscan_seg,'Not in Sky')
        ,coalesce(cuscan_segment ,'Not in Sky')
        ,cb_data_date
        ,churn_rate
        ,colour
        ,value_seg_updated
order by 3,4 desc
-- 1334  Row(s) affected

-- create hg index ind_acc on cuscan_segments_history(account_number);
-- -- create lf index ind_dat on cuscan_segments_history(cb_data_date);
-- -- --Take raw data and create table with current and previous segment for each account
-- select   account_number
--         ,cb_data_date as 'Date'
--         ,value_seg_updated
--         ,churn_rate
--         ,colour
--         ,coalesce(prev_cuscan_seg,'Not in Sky') as 'Previous Segment'
--         ,coalesce(cuscan_segment ,'Not in Sky') as 'Current Segment'
-- into account_segments_transition
-- from (
-- select   account_number
--         ,cb_data_date
--         ,value_seg_updated
--         ,churn_rate
--         ,colour
--         ,lag(cuscan_segment) over (partition by account_number
--                                 order by cb_data_date) as prev_cuscan_seg
--         ,cuscan_segment
-- from cuscan_segments_transition_temp
-- ) t
-- where    cb_data_date > '2012-10-11'
-- group by account_number
--         ,cb_data_date
--         ,value_seg_updated
--         ,churn_rate
--         ,colour
--         ,coalesce(prev_cuscan_seg,'Not in Sky')
--         ,coalesce(cuscan_segment ,'Not in Sky')
-- order by account_number
-- -- 19733724 Row(s) affected
-- 
select * from cuscan_segments_transition;
select top 20 * from cuscan_segments_transition_temp;
select top 20 * from account_segments_transition;

-- Candidate accounts for inclusion in panel
select account_number
from vespa_analysts.waterfall_base
where knockout_level >= 9999

