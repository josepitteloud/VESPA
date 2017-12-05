


-- Panel Size:
select  case    when panel in ('VESPA','VESPA11')       then 'DP'
                when panel in ('ALT5','ALT6','ALT7')    then 'AP'
        end     as  The_panel
        ,sum(case   when panel in ('VESPA','ALT6','ALT7')   then 1 else 0 end)  as PSTN
        ,sum(case   when panel in ('ALT5','VESPA11')        then 1 else 0 end)  as BB
from    (     
            select  distinct 
                    account_number
                    ,panel
            from    vespa_analysts.vespa_single_box_view
            where   status_vespa = 'Enabled'
        )   as sbv
group   by  the_panel




-- Typical nightly return????

-- 2...

declare @from_dt    date
declare @to_dt      date

select  @to_dt = max(weekending) from sig_single_Account_view
set @from_dt = @to_dt-34


select  scaling.weekending
        ,sbvh.panel_id
        ,count(distinct scaling.account_number) as naccounts_returning
from    (
            select  distinct
                    adjusted_event_Start_Date_vespa as thedate
                    ,case   when datepart(weekday,thedate) = 7 then thedate
                            else cast(dateadd(day,(7-datepart(weekday,thedate)),thedate) as date)
                    end     as weekending
                    ,account_number
            from    sk_prod.VIQ_VIEWING_DATA_SCALING
            where   adjusted_event_Start_Date_vespa between @from_dt and @to_dt
        )   as scaling
        inner join vespa_analysts.vespa_sbv_hist_qualitycheck   as sbvh
        on  scaling.account_number  = sbvh.account_number
        and scaling.weekending      = sbvh.weekending
group   by  scaling.weekending
            ,sbvh.panel_id

-- Once in last week/month
-- Every day in last week/month
select  case    when ssav.panel in ('VESPA','VESPA11')       then 'DP'
                when ssav.panel in ('ALT5','ALT6','ALT7')    then 'AP'
        end     as  The_panel
        ,sum(case   when ssav.panel in ('VESPA','ALT6','ALT7')  and ssbv.returned_something_7d>0    then 1 else 0 end)  as PSTN_once_lw
        ,sum(case   when ssav.panel in ('ALT5','VESPA11')       and ssbv.returned_something_7d>0    then 1 else 0 end)  as BB_once_lw
        ,sum(case   when ssav.panel in ('VESPA','ALT6','ALT7')  and ssbv.returned_something_30d>0   then 1 else 0 end)  as PSTN_once_lm
        ,sum(case   when ssav.panel in ('ALT5','VESPA11')       and ssbv.returned_something_30d>0   then 1 else 0 end)  as BB_once_lm

        ,sum(case   when ssav.panel in ('VESPA','ALT6','ALT7')  and ssav.return_data_7d>0    then 1 else 0 end)  as PSTN_all_lw
        ,sum(case   when ssav.panel in ('ALT5','VESPA11')       and ssav.return_data_7d>0    then 1 else 0 end)  as BB_all_lw
        ,sum(case   when ssav.panel in ('VESPA','ALT6','ALT7')  and ssav.return_data_30d>0   then 1 else 0 end)  as PSTN_all_lm
        ,sum(case   when ssav.panel in ('ALT5','VESPA11')       and ssav.return_data_30d>0   then 1 else 0 end)  as BB_all_lm

from    sig_single_Account_view         as ssav
        inner join  (
                        select  account_number
                                ,min(num_logs_sent_7d)  as returned_something_7d
                                ,min(num_logs_sent_30d) as returned_something_30d
                        from    sig_single_box_view
                        where   status_vespa = 'Enabled'
                        group   by  account_number
                    )   as ssbv
        on  ssav.account_number = ssbv.account_number
where   ssav.status_vespa = 'Enabled'
group   by  the_panel



declare @from_dt    date
declare @to_dt      date

select  @to_dt = max(weekending) from vespa_analysts.vespa_sbv_hist_qualitycheck
set @from_dt = @to_dt-34


select  panel
        ,dialformat
        ,sum(case   when period = 'LW' and dialbacks <= 7   then 1 else 0 end)  as once_lw
        ,sum(case   when period = 'LM' and dialbacks <= 28  then 1 else 0 end)  as once_lm
        ,sum(case   when period = 'LW' and dialbacks = 7    then 1 else 0 end)  as full_lw
        ,sum(case   when period = 'LM' and dialbacks = 28   then 1 else 0 end)  as full_lm
from    (
            select  scaling.period
                    ,case   when sbvh.panel_id in (11,12)   then 'DP'
                            when sbvh.panel_id in (5,6,7)   then 'AP'
                    end     as panel
                    ,case   when sbvh.panel_id in (11,5)    then 'BB'
                            when sbvh.panel_id in (12,6,7)  then 'PSTN'
                    end     as dialformat
                    ,scaling.account_number
                    ,sum(weekcalls) as dialbacks
            from    (
                        select  case   when datepart(weekday,adjusted_event_Start_Date_vespa) = 7 then adjusted_event_Start_Date_vespa
                                        else cast(dateadd(day,(7-datepart(weekday,adjusted_event_Start_Date_vespa)),adjusted_event_Start_Date_vespa) as date)
                                end     as weekending
                                ,case   when weekending = @to_dt then 'LW'
                                        else 'LM'
                                end     as period
                                ,account_number
                                ,count(distinct adjusted_event_Start_Date_vespa) as weekcalls
                        from    sk_prod.VIQ_VIEWING_DATA_SCALING
                        where   adjusted_event_Start_Date_vespa between @from_dt and @to_dt
                        group   by  weekending
                                    ,account_number
                    )   as scaling
                    inner join  (
                                    select  distinct
                                            weekending
                                            ,account_number
                                            ,panel_id
                                    from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                    where   weekending between @from_dt and @to_dt
                                )   as sbvh
                    on  scaling.account_number  = sbvh.account_number
                    and scaling.weekending      = sbvh.weekending
            group   by  scaling.period
                        ,panel
                        ,dialformat
                        ,scaling.account_number
        )   as step1
group   by  panel
            ,dialformat

-- Panel performance


select  thepanel
        ,thedecile
        ,avg(new_rq)                    as theavg
        ,count(distinct account_number) as naccounts
from    (
            select  thepanel
                    ,ntile(10) over (
                                        partition by    thepanel
                                        order by        new_rq
                                    )   as thedecile
                    ,new_rq
                    ,account_number
            from    (   
                        select  account_number
                                ,case   when panel = 'VESPA'            then 'DP PSTN'
                                        when panel = 'VESPA11'          then 'DP BB'
                                        when panel in ('ALT6','ALT7')   then 'AP PSTN'
                                        when panel = 'ALT5'             then 'AP BB'
                                end     as thepanel            
                                ,avg(case when reporting_quality > 1 then 1 else reporting_quality end)as new_rq
                        from    sig_single_box_view
                        where   status_vespa = 'Enabled'
                        and     reporting_quality is not null
                        group   by  account_number
                                    ,thepanel
                    )   as base
        )   as slicer
group   by  thepanel
            ,thedecile
			
			
-- 3)...
			
