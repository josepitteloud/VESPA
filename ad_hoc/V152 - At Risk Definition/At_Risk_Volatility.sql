/*

Look at customer value segments 'Bedding In' and 'Unstable' over time a period of 1 year

*/

-- Compile historical data
select *
into value_segments_history
from (
select * from sk_prod.VALUE_SEGMENTS_DATA_20120203 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20120307 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20120404 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20120503 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20120606 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20120703 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20120802 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20120904 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20120930 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20121029 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20121202 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20121231 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20130129 union all
select * from sk_prod.VALUE_SEGMENTS_DATA_20130304
) t

-- Identify value segments by quarter over the last year
select account_number
        ,max(Mar2012) as March_2012
        ,max(Jun2012) as June_2012
        ,max(Sep2012) as September_2012
        ,max(Dec2012) as December_2012
        ,max(Mar2013) as March_2013
into value_segments_quarters
from (
select account_number
        ,case 
        when cb_data_date = '2012-03-07' then case
                                                when value_seg in ('Bedding In','Unstable') 
                                                then 'Low value'
                                                else 'Medium/High value'
                                               end
        else '' 
        end as Mar2012
        ,case 
        when cb_data_date = '2012-06-06' then case 
                                                when value_seg in ('Bedding In','Unstable') 
                                                then 'Low value'
                                                else 'Medium/High value'
                                               end
        else '' 
        end as Jun2012
        ,case 
        when cb_data_date = '2012-09-04' then case 
                                                when value_seg in ('Bedding In','Unstable') 
                                                then 'Low value'
                                                else 'Medium/High value'
                                               end
        else '' 
        end as Sep2012
        ,case 
        when cb_data_date = '2012-12-02' then case 
                                                when value_seg in ('Bedding In','Unstable') 
                                                then 'Low value'
                                                else 'Medium/High value'
                                               end
        else '' 
        end as Dec2012
        ,case 
        when cb_data_date = '2013-03-04' then case 
                                                when value_seg in ('Bedding In','Unstable') 
                                                then 'Low value'
                                                else 'Medium/High value'
                                               end
        else '' 
        end as Mar2013
from value_segments_history
where cb_data_date > '2012-03-01' -- remove data from Feb 2012
) t
group by account_number

-- Have a look
select * from value_segments_quarters

-- Aggregate # of customers by their value segment changes over time
select "March 2012"
        ,"June 2012"
        ,"September 2012"
        ,"December 2012"
        ,"March 2013"
        ,count(*) as 'Number_Customers'
from (        
select account_number
        ,case when March_2012 = '' then 'Not with Sky' else March_2012 end as 'March 2012'
        ,case when June_2012 = '' then 'Not with Sky' else June_2012 end as 'June 2012'
        ,case when September_2012 = '' then 'Not with Sky' else September_2012 end as 'September 2012'
        ,case when December_2012 = '' then 'Not with Sky' else December_2012 end as 'December 2012'
        ,case when March_2013 = '' then 'Not with Sky' else March_2013 end as 'March 2013'
from value_segments_quarters
) t
group by "March 2012"
        ,"June 2012"
        ,"September 2012"
        ,"December 2012"
        ,"March 2013"
order by Number_Customers desc

-- Look at value changes in totals
select coalesce(t2.prev_value_seg,'Not in Sky') as 'Previous Segment'
        ,coalesce(t2.value_seg,'Not in Sky') as 'Current Segment'
        ,dat.cb_data_date as 'Date'
        ,count(*) as 'Number Customers'
into value_segments_transition
from 
(select distinct account_number from value_segments_history) acc
full join
(select distinct cb_data_date from value_segments_history) dat
on 1=1
left join
(
select account_number
        ,cb_data_date
        ,lag(value_seg) over (partition by account_number
                                order by cb_data_date) as prev_value_seg
        ,value_seg 
from value_segments_history
) t2
on acc.account_number = t2.account_number
and dat.cb_data_date = t2.cb_data_date
where dat.cb_data_date > '2012-03-01'
group by coalesce(t2.prev_value_seg,'Not in Sky')
        ,coalesce(t2.value_seg,'Not in Sky')
        ,dat.cb_data_date
order by 3,4 desc
-- 511

--------------------------------------
-- Look at value changes in totals
--------------------------------------

-- Get the raw data first
select acc.account_number
        ,dat.cb_data_date
        ,t2.value_seg
into value_segments_transition_temp
from 
(select distinct account_number from value_segments_history) acc
full join
(select distinct cb_data_date from value_segments_history) dat
on 1=1
left join
(select * from value_segments_history) t2
on acc.account_number = t2.account_number
and dat.cb_data_date = t2.cb_data_date
-- 160239617 row(s) affected

-- Aggregate information now
select coalesce(prev_value_seg,'Not in Sky') as 'Previous Segment'
        ,coalesce(value_seg,'Not in Sky') as 'Current Segment'
        ,cb_data_date as 'Date'
        ,count(*) as 'Number Customers'
into value_segments_transition
from (
select account_number
        ,cb_data_date
        ,lag(value_seg) over (partition by account_number
                                order by cb_data_date) as prev_value_seg
        ,value_seg 
from value_segments_transition_temp
) t
where cb_data_date > '2012-03-01'
group by coalesce(prev_value_seg,'Not in Sky')
        ,coalesce(value_seg,'Not in Sky')
        ,cb_data_date
order by 3,4 desc
-- 597

select * from value_segments_transition

-- Candidate accounts for inclusion in panel
select account_number
from vespa_analysts.waterfall_base
where knockout_level >= 9999

