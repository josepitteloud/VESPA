------------------------------
-- Tenure of Drop-off Home [2]
------------------------------

select  dropers.dropoff_woy as week_of_year
        ,intervals.control_cell
        ,dropers.dropoff_reason
        ,datediff(week,viq.scaling_enablement_dt,dropers.dropoff_date)  as recency
        ,count(distinct viq.account_number) as volume
into RSMB_Dropoff_Homes
from    (
            -- Sample of DP Responders in October
            select  scaling.adjusted_event_start_date_vespa   as viq_date
                    ,scaling.account_number
                    ,panel_hist.scaling_enablement_dt
            from    sk_prod.VIQ_VIEWING_DATA_SCALING    as scaling
                    inner join  (   
                                select  account_number
                                        ,min(reporting_starts)  as scaling_enablement_dt
                                from    vespa_analysts.sc2_intervals
                                group by  account_number
                                )   as panel_hist
                    on  scaling.account_number = panel_hist.account_number
            where   scaling.adjusted_event_start_date_vespa between '2013-10-01' and '2013-10-31'
        )   as viq
        inner join  igonorp.sc_interval     as intervals
        on  viq.account_number = intervals.account_number
        inner join  (
                        -- LIST OF DROPPERS...
                        select  cast(lookup.writeback_datetime as date)    as dropoff_date
                                ,calendar.theweek    as dropoff_woy
                                ,lookup.cell_name   as dropoff_reason
                                ,cust.account_number
                        from    sk_prod.campaign_history_lookup_cust        as lookup
                                inner join sk_prod.CAMPAIGN_HISTORY_CUST    as cust
                                on  lookup.cell_id = cust.cell_id
                                inner join  (
                                                select  distinct
                                                        utc_day_date        as thedate
                                                        ,utc_week_in_year   as theweek
                                                from    sk_prod.VESPA_CALENDAR 
                                                where   utc_day_date between '2013-10-01' and '2013-10-31'
                                            )   as calendar
                                on  cast(lookup.writeback_datetime as date) = calendar.thedate
                        where   upper(lookup.campaign_name) like '%VESPA%DISABLEMENT%'
                        and     cast(lookup.writeback_datetime as date) between '2013-10-01' and '2013-10-31'
                        and     lookup.cell_name not in ('AnytimePlusEnablements & TransfersToPanel12'
                                                        ,'Panel_6_7_moving_to_12')
                    )   as dropers
        on  viq.account_number = dropers.account_number
where   viq.viq_date between intervals.interval_starts and intervals.interval_ends
group   by  week_of_year
            ,intervals.control_cell
            ,dropers.dropoff_reason
            ,recency

select top 100 * from RSMB_Dropoff_Homes where volume>1

select week_of_year,sum(volume)
from RSMB_Dropoff_Homes
group by week_of_year
order by week_of_year

select recency,sum(volume)
from RSMB_Dropoff_Homes
group by recency
order by recency

select dropoff_reason,sum(volume)
from RSMB_Dropoff_Homes
group by dropoff_reason
order by dropoff_reason

-- OUTPUT
select * from RSMB_Dropoff_Homes;
OUTPUT TO 'G:\RTCI\Sky Projects\Vespa\Measurements and Algorithms\RSMB Audit\Phase II\RSMB_Dropoff_Homes_v2_20140106.csv' 
FORMAT ASCII 
DELIMITED BY ',' 
QUOTE '';
-- 54868 rows written

-------------------------------
-- Monthly Panel Continuity [3]
-------------------------------

-- Export to Netezza
-- Tenure from 28 Feb
select   viq.account_number
        ,case 
            when max(dropers.dropoff_date) is null
            then datediff(week,'2013-02-28',today())
            else datediff(week,'2013-02-28',max(dropers.dropoff_date))
        end as tenure_weeks
into   RSMB_TENURE_FROM_28FEB_LOOKUP
from   sk_prod.VIQ_VIEWING_DATA_SCALING viq
left join  (
                        -- LIST OF DROPPERS...
                        select  cast(lookup.writeback_datetime as date)    as dropoff_date
                                ,cust.account_number
                        from    sk_prod.campaign_history_lookup_cust        as lookup
                                inner join sk_prod.CAMPAIGN_HISTORY_CUST    as cust
                                on  lookup.cell_id = cust.cell_id
                        where   upper(lookup.campaign_name) like '%VESPA%DISABLEMENT%'
                        and     cast(lookup.writeback_datetime as date) > '2013-02-28'
                        and     lookup.cell_name not in ('AnytimePlusEnablements & TransfersToPanel12'
                                                        ,'Panel_6_7_moving_to_12')
                    )   as dropers
on  viq.account_number = dropers.account_number
where viq.adjusted_event_Start_date_vespa between '2013-02-01' and '2013-02-28'
group by viq.account_number
-- 460698 row(s) affected

select tenure_weeks,count(*)
from RSMB_TENURE_FROM_28FEB_LOOKUP
group by tenure_weeks
order by tenure_weeks 

select *
from RSMB_TENURE_FROM_28FEB_LOOKUP;
OUTPUT TO 'G:\RTCI\Sky Projects\Vespa\Measurements and Algorithms\RSMB Audit\Phase II\RSMB_Tenure_From_28Feb_20140106.csv' 
FORMAT ASCII 
DELIMITED BY ',' 
QUOTE '';
-- 423086 rows written

---------------------------
-- Daily Response Rates [1]
---------------------------

select *
--into  RSMB_Daily_Response_Rates
from (
select viq.adjusted_event_start_date_vespa as thedate
        ,interval.control_cell
        ,sum(case when thedate between interval.interval_starts and interval.interval_ends then 1 else 0 end) as vol_response
        ,cast(sum(case 
                    when thedate between interval.interval_starts and interval.interval_ends 
                    then calculated_scaling_weight 
                    else 0 
                  end) as int) as target_sky_base
from    sk_prod.VIQ_VIEWING_DATA_SCALING    as viq
        inner join igonorp.sc_interval      as interval
        on  viq.account_number = interval.account_number
where   viq.adjusted_event_Start_date_vespa between '2013-10-01' and '2013-10-31'
group   by  thedate
            ,interval.control_cell
) t
where vol_response > 0;
OUTPUT TO 'G:\RTCI\Sky Projects\Vespa\Measurements and Algorithms\RSMB Audit\Phase II\RSMB_Daily_Response_Rates_v2_20140106.csv' 
FORMAT ASCII 
DELIMITED BY ',' 
QUOTE '';
-- 536439 row(s) affected

select top 100 *  from RSMB_Daily_Response_Rates 
select thedate,sum(target_sky_base) from  RSMB_Daily_Response_Rates group by thedate order by 1