--Overview
--Boxes with requests for enablement as of 30 days ago
--Confirmed activated boxes as of 30 days ago
--Boxes returning data at least once within 30 days
--Boxes returning data every day for last 30 days
select  panel
        ,'Boxes with requests for enablement as of 30 days ago' as context
        ,count(distinct subscriber_ID)  as n_boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   dateadd(day, 30, selection_date) <= (select cast((case when datepart(weekday,today()) = 7 then today() else dateadd(day,((datepart(weekday,today()))-8),today()) end) as date))
and     panel in ('VESPA','VESPA11')
and     status_vespa <> 'Disabled'
group   by  panel
union
select  panel
        ,'Confirmed activated boxes as of 30 days ago' as context
        ,count(distinct subscriber_ID)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   dateadd(day, 30, selection_date) <= (select cast((case when datepart(weekday,today()) = 7 then today() else dateadd(day,((datepart(weekday,today()))-8),today()) end) as date))
and     panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
group   by  panel
union
select  panel
        ,'Boxes returning data at least once within 30 days' as context
        ,count (distinct subscriber_ID) as boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
and     num_logs_sent_30d > 0
group   by  panel
union
select  panel
        ,'Boxes returning data every day for last 30 days' as context
        ,count (subscriber_ID) as boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
and     returned_data_30d = 1
group   by  panel
union

--Boxes with requests for enablement as of 7 days ago
--Confirmed activated boxes as of 7 days ago
--Boxes returning data at least once within 7 days
--Boxes returning data every day for last 7 days
select  panel
        ,'Boxes with requests for enablement as of 7 days ago' as context
        ,count(distinct subscriber_ID)  as n_boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   dateadd(day, 7, selection_date) <= weekending
and     panel in ('VESPA','VESPA11')
and     status_vespa <> 'Disabled'
group   by  panel
union
select  panel
        ,'Confirmed activated boxes as of 7 days ago' as context
        ,count(distinct subscriber_ID)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   dateadd(day, 7, selection_date) <= weekending
and     panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
group   by  panel
union
select  panel
        ,'Boxes returning data at least once within 7 days' as context
        ,count (distinct subscriber_ID) as boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
and     num_logs_sent_7d > 0
group   by  panel
union
select  panel
        ,'Boxes returning data every day for last 7 days' as context
        ,count (subscriber_ID) as boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
and     returned_data_7d = 1
group   by  panel

-----------------------------------------------------------------------------------------------

--30 day total logs
--Number of logs for primary box reporting over 30 days
--Number of logs for secondary box reporting over 30 days
select  panel
        ,num_logs_sent_30d
        ,ps_flag
        ,count(subscriber_id)   as n_boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
group   by  panel
            ,num_logs_sent_30d
            ,ps_flag
			
--------------------------------------------------------------------------------------------------

--same stuff for 7 days
select  panel
        ,num_logs_sent_7d
        ,ps_flag
        ,count(subscriber_id)   as n_boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
group   by  panel
            ,num_logs_sent_7d
            ,ps_flag
			
--------------------------------------------------------------------------------------------------

/*
--Number of logs for P&S box reporting over 30 days
select num_logs_sent_30d, count (card_subscriber_ID), ps_flag from vespa_analysts.SIG_SINGLE_BOX_VIEW
where ps_flag = 'P' or ps_flag = 'S'
and num_logs_sent_30d is not null
group by ps_flag, num_logs_sent_30d
order by ps_flag, num_logs_sent_30d
*/

--30 day intervals
--Number of continuous days for secondary box reporting over 30 days
--Number of continuous days for secondary box reporting over 30 days
select  panel
        ,Continued_trans_30d
        ,ps_flag
        ,count(subscriber_ID)   as n_boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
group   by panel
            ,Continued_trans_30d
            ,ps_flag
-----------------------------------------------------------------------------------------------------
			
-- Same stuff for 7 days
select  panel
        ,Continued_trans_7d
        ,ps_flag
        ,count(subscriber_ID)   as n_boxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
group   by panel
            ,Continued_trans_7d
            ,ps_flag
			
-----------------------------------------------------------------------------------------------------
			
			
--Dialback Insights
--Number of logs per box model reporting over 30 days
select  panel
		,box_model
        ,case   when num_logs_sent_30d = 0               then '1) 0'
                when num_logs_sent_30d between 1  and 5  then '2) 1-5'
                when num_logs_sent_30d between 6  and 10 then '3) 6-10'
                when num_logs_sent_30d between 11 and 15 then '4) 11-15'
                when num_logs_sent_30d between 16 and 20 then '5) 16-20'
                when num_logs_sent_30d between 21 and 25 then '6) 21-25'
                when num_logs_sent_30d between 26 and 30 then '7) 26-30'
                else NULL
        end     as Frequency
       ,count (card_subscriber_ID) as nboxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa in ('Enabled','DisableRequested','DisablePending')
group   by  panel
			,box_model
            ,Frequency

-----------------------------------------------------------------------------------------------------

--Number of logs per TV region reporting over 30 days
select  sbv.panel
        ,isba_tv_region AS TV_region
        ,case   when num_logs_sent_30d = 0               then '1) 0'
                when num_logs_sent_30d between 1  and 5  then '2) 1-5'
                when num_logs_sent_30d between 6  and 10 then '3) 6-10'
                when num_logs_sent_30d between 11 and 15 then '4) 11-15'
                when num_logs_sent_30d between 16 and 20 then '5) 16-20'
                when num_logs_sent_30d between 21 and 25 then '6) 21-25'
                when num_logs_sent_30d between 26 and 30 then '7) 26-30'
                else NULL
        end as Frequency
       ,count (sbv.card_subscriber_ID) nboxes
from    vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
        INNER JOIN /*sk_prod.*/CUST_SINGLE_ACCOUNT_VIEW AS sav   
        ON sav.account_number = sbv.account_number
where   sbv.panel in ('VESPA','VESPA11')
and     sbv.status_vespa in ('Enabled','DisableRequested','DisablePending')
group   by  sbv.panel
            ,sbv.box_model
            ,TV_region
            ,Frequency
		
-----------------------------------------------------------------------------------------------------		
			
-- REPORTING TIME (TAB)
declare @todt       date
        ,@log_from  integer
        ,@log_to    integer

select  @todt   =	case    when datepart(weekday,today()) = 7 then today()
						    else (today() - datepart(weekday,today()))
                    end
set @log_from   =   convert(integer,dateformat(dateadd(day, -29, @todt),'yyyymmddhh'))	-- YYYYMMDD00
set @log_to     =   convert(integer,dateformat(@todt,'yyyymmdd')+'23')	                -- YYYYMMDD23


select  panel_id
        ,datepart(hh,log_received_start_date_time_utc)  as thehours
        ,service_instance_type
        ,count(distinct subscriber_id||' '||log_received_start_date_time_utc) as logs_returned
from    /*sk_prod.*/vespa_dp_prog_viewed_current
WHERE   subscriber_id > 0
AND     dk_log_received_datehour_dim between @log_from and @log_to
group   by  panel_id
            ,thehours
            ,service_instance_type




/* [S] MAIN SLICER */ 

select  subscriber_id
                ,case when dateadd(day, 30, selection_date) <= (select cast((case when datepart(weekday,today()) = 7 then today() else dateadd(day,((datepart(weekday,today()))-8),today()) end) as date)) then 1 else 0 end    as e_request_30
                ,case when dateadd(day, 7, selection_date) <= (select cast((case when datepart(weekday,today()) = 7 then today() else dateadd(day,((datepart(weekday,today()))-8),today()) end) as date)) then 1 else 0 end     as e_request_7
                ,case when dateadd(day, 30, enablement_date) <= (select cast((case when datepart(weekday,today()) = 7 then today() else dateadd(day,((datepart(weekday,today()))-8),today()) end) as date)) then 1 else 0 end    as c_request_30
                ,case when dateadd(day, 7, enablement_date) <= (select cast((case when datepart(weekday,today()) = 7 then today() else dateadd(day,((datepart(weekday,today()))-8),today()) end) as date)) then 1 else 0 end     as c_request_7
                ,status_vespa
                ,ps_flag
                ,case when num_logs_sent_30d > 30 then 30 else num_logs_sent_30d end as num_logs_sent_30d_cap
                ,case when num_logs_sent_7d > 7 then 7 else num_logs_sent_7d end as num_logs_sent_7d_cap
                ,case when continued_trans_30d > 30 then 30 else continued_trans_30d end as continued_trans_30d_cap
                ,case when continued_trans_7d > 7 then 7 else continued_trans_7d end as continued_trans_7d_cap
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel = 'VESPA'
and             status_vespa = 'Enabled'



/* [S] REPORTING TIME SLICER */ -- (COMPOSED) --source: [1]

select  time_of_day
        ,sum(case when box_rank = 'P' then log_count else 0 end) as [Primary]
        ,sum(case when box_rank = 'S' then log_count else 0 end) as Secondary
from    (
            select  base.ps_flag            as box_rank
                    ,summary.log_date
                    ,summary.hour_received  as time_of_day
                    ,count(1)               as log_count
            from    (
                        select  distinct 
                                subscriber_id
                                ,ps_flag
                        from    vespa_analysts.SIG_SINGLE_BOX_VIEW
                        where   panel in ('VESPA','VESPA11')
                        and     status_vespa = 'Enabled'
                    )   as base
                    inner join vespa_Dialback_log_daily_summary as summary
                    on  base.subscriber_id = summary.subscriber_id
            group   by  box_rank
                        ,summary.log_date
                        ,time_of_day
        )   as stage
group   by  time_of_day
order   by      time_of_day


/*      [S] EVENT COUNT SLICER */ -- (COMPOSED) --source: [1]

select  event_count_bracket
        ,sum(case when box_rank = 'P' then log_count else 0 end) as [Primary]
        ,sum(case when box_rank = 'S' then log_count else 0 end) as Secondary
from    (
            select  base.ps_flag            as box_rank
                    ,case   when (summary.log_event_count / 20) * 20 > 600 then 600
                            else (summary.log_event_count / 20) * 20 
                    end     as event_count_bracket
                    ,count(1)               as log_count
            from    (
                        select  distinct 
                                subscriber_id
                                ,ps_flag
                        from    vespa_analysts.SIG_SINGLE_BOX_VIEW
                        where   panel in ('VESPA','VESPA11')
                        and     status_vespa = 'Enabled'
                    )   as base
                    inner join vespa_Dialback_log_daily_summary as summary
                    on  base.subscriber_id = summary.subscriber_id
            group   by  box_rank
                        ,event_count_bracket
        )   as stage
group   by  event_count_bracket
order   by      event_count_bracket


/* [S] DIALBACK FREQUENCY BY BOX MODEL */

select  box_model
       ,case when (num_logs_sent_30d = 0 or num_logs_sent_30d is null)  then '1) 0'
             when num_logs_sent_30d between 1  and 5                    then '2) 1-5'
             when num_logs_sent_30d between 6  and 10                   then '3) 6-10'
             when num_logs_sent_30d between 11 and 15                   then '4) 11-15'
             when num_logs_sent_30d between 16 and 20                   then '5) 16-20'
             when num_logs_sent_30d between 21 and 25                   then '6) 21-25'
             when num_logs_sent_30d >=26                                then '7) 26-30'
        else                                                            null
        end as Frequency
       ,count (card_subscriber_ID) as hits
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel in ('VESPA','VESPA11')
and     status_vespa = 'Enabled'
and     box_model is not null
group   by      box_model
                        ,Frequency
order   by      box_model
                        ,Frequency


/* [S] DIALBACK FREQUENCY BY TV REGION */

select  sav.isba_tv_region AS TV_region
       ,case when (SBV.num_logs_sent_30d = 0 or SBV.num_logs_sent_30d is null)  then '1) 0'
             when SBV.num_logs_sent_30d between 1  and 5                        then '2) 1-5'
             when SBV.num_logs_sent_30d between 6  and 10                       then '3) 6-10'
             when SBV.num_logs_sent_30d between 11 and 15                       then '4) 11-15'
             when SBV.num_logs_sent_30d between 16 and 20                       then '5) 16-20'
             when SBV.num_logs_sent_30d between 21 and 25                       then '6) 21-25'
             when SBV.num_logs_sent_30d >= 26                                   then '7) 26-30'
        else NULL
        end as Frequency
       ,count (sbv.card_subscriber_ID)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
        INNER JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW AS sav   
        ON  sav.account_number = sbv.account_number
WHERE   sbv.panel in ('VESPA','VESPA11')
and     sbv.status_vespa = 'Enabled'
group   by  isba_tv_region
            ,Frequency
order   by  isba_tv_region
            ,Frequency

-- Source: [1]

declare @latest_full_date       date
declare @event_from_date    integer
declare @event_to_date      integer
declare @bst_start                      date
declare @gmt_start                      date

execute vespa_analysts.Regulars_Get_report_end_date @latest_full_date output

set @event_from_date    = convert(integer,dateformat(dateadd(day, -30, @latest_full_date),'yyyymmddhh'))        -- YYYYMMDD00
set @event_to_date      = convert(integer,dateformat(dateadd(day,1,@latest_full_date),'yyyymmdd')+'23')         -- YYYYMMDD23

set @bst_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-03-31') -1),datepart(year, today()) || '-03-31')  -- to get last Sunday in March
set @gmt_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-10-31') -1),datepart(year, today()) || '-10-31')  -- to get last Sunday in October

IF today()  >= @bst_start and today() < @gmt_start
Begin
        insert  into vespa_Dialback_log_collection_dump (
                                                                                                                 subscriber_id
                                                                                                                ,stb_log_creation_date
                                                                                                                ,doc_creation_date_from_9am
                                                                                                                ,first_event_mark
                                                                                                                ,last_event_mark
                                                                                                                ,log_event_count
                                                                                                                ,hour_received
                                                                                                                ,panel_id
                                                                                                        )
        select  subscriber_id
                        ,dateadd(hour,1, LOG_START_DATE_TIME_UTC)
                        ,case   when convert(integer,dateformat(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)),'hh')) <23 then cast(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)) as date)-1
                                        else cast(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)) as date)
                        end     as doc_creation_date_from_9am
                        ,min(dateadd(hour,1, EVENT_START_DATE_TIME_UTC))
                        ,max(dateadd(hour,1, EVENT_END_DATE_TIME_UTC))
                        ,count(1)
                        ,datepart(hh, min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)))
                        ,min(panel_id)
        from    sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
        where   panel_id                                                        in (4,11,12)
        and     dk_event_start_datehour_dim             between @event_from_date and @event_to_date
        and     LOG_RECEIVED_START_DATE_TIME_UTC        is not null
        and     LOG_START_DATE_TIME_UTC                         is not null
        and     subscriber_id                                           is not null
        group   by      subscriber_id
                                ,LOG_START_DATE_TIME_UTC
        having  doc_creation_date_from_9am is not null
End

ELSE
Begin
        insert  into vespa_Dialback_log_collection_dump (
                                                                                                                 subscriber_id
                                                                                                                ,stb_log_creation_date
                                                                                                                ,doc_creation_date_from_9am
                                                                                                                ,first_event_mark
                                                                                                                ,last_event_mark
                                                                                                                ,log_event_count
                                                                                                                ,hour_received
                                                                                                                ,panel_id
                                                                                                        )
        select  subscriber_id
            ,LOG_START_DATE_TIME_UTC
            ,case       when convert(integer,dateformat(min(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) <23 then cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)-1
                    else cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)
            end         as doc_creation_date_from_9am
            ,min(EVENT_START_DATE_TIME_UTC)
            ,max(EVENT_END_DATE_TIME_UTC)
            ,count(1)
            ,datepart(hh, min(LOG_RECEIVED_START_DATE_TIME_UTC))
            ,min(panel_id)
        from    sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
        where   panel_id                                                        in (4,11,12)
    and     dk_event_start_datehour_dim                 between @event_from_date and @event_to_date
    and     LOG_RECEIVED_START_DATE_TIME_UTC    is not null
    and     LOG_START_DATE_TIME_UTC                     is not null
    and     subscriber_id                                               is not null
        group   by      subscriber_id
                                ,LOG_START_DATE_TIME_UTC
        having  doc_creation_date_from_9am is not null
End

create index panel_id on vespa_Dialback_log_collection_dump (panel_id)
commit

--execute citeam.logger_add_event @DBR_logging_ID, 3, 'C01: Data loaded'
commit

truncate table vespa_Dialback_log_daily_summary
commit

insert  into vespa_Dialback_log_daily_summary
select  subscriber_id
                ,convert(date, doc_creation_date_from_9am) as log_date
                ,count(distinct doc_creation_date_from_9am) -- we never check that this isn't 1?
                ,min(first_event_mark)
                ,max(last_event_mark)
                ,sum(log_event_count) -- we're still not doing anything with these coverage numbers?
                ,min(hour_received)
from    vespa_Dialback_log_collection_dump
where   panel_id in (4,12,11) -- split out phone DP from BB panel
group   by      subscriber_id, log_date -- 19,180,935 row(s) inserted

commit

delete from vespa_Dialback_log_daily_summary
where dateadd(day, 30, log_date) <= @latest_full_date
or log_date > @latest_full_date

commit
