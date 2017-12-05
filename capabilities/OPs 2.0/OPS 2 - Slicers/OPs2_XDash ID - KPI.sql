--Personalised Services Consent LAST WEEK!!!
select	count(distinct account_number)
from 	vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where 	viewing_consent_flag = 'Y'

--DP Accounts Enabled LAST WEEK!!!
select	count(distinct account_number)
from 	vespa_analysts.SIG_SINGLE_BOX_VIEW
where 	status_vespa = 'Enabled'
and 	panel = 'VESPA'

--DP Accounts Returning Data LAST WEEK!!!
select  count(distinct acview.account_number)
from    (
            -- counting dialling boxes... (account level)
            select  perf.dt
                    ,boxview.account_number
                    ,count(distinct perf.subscriber_id) as dialling_b
            from    vespa_analysts.panel_data                       as perf
                    inner join vespa_analysts.sig_single_box_view   as boxview
                    on  perf.subscriber_id = boxview.subscriber_id
                    and boxview.panel in ('VESPA','VESPA11')
                    and boxview.status_vespa = 'Enabled'
            where   perf.panel in (12,11)
            and     perf.data_received = 1
            and     perf.dt between (
                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                        else (today() - datepart(weekday,today()))-6
                                                end
                                    )
                                    and (
                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                            else (today() - datepart(weekday,today()))
                                                    end
                                        )
            group   by  perf.dt 
                        ,boxview.account_number -- 505071
        )   as dialback
        inner join  vespa_analysts.sig_single_account_view as acview
        on  dialback.account_number = acview.account_number
where   dialback.dialling_b >= acview.num_boxes

--DP Avg. Rep. Quality of Accs. Returning Data LAST WEEK!!!
select  avg(rq)
from    (
            -- counting dialling boxes... (account level)
            select  perf.dt
                    ,boxview.account_number
                    ,count(distinct perf.subscriber_id) as dialling_b
                    ,avg(case when boxview.reporting_quality > 1 then 1 else boxview.reporting_quality end )    as rq
            from    vespa_analysts.panel_data                       as perf
                    inner join vespa_analysts.sig_single_box_view   as boxview
                    on  perf.subscriber_id = boxview.subscriber_id
                    and boxview.panel in ('VESPA','VESPA11')
                    and boxview.status_vespa = 'Enabled'
            where   perf.panel in (12,11)
            and     perf.data_received = 1
            and     perf.dt between (
                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                        else (today() - datepart(weekday,today()))-6
                                                end
                                    )
                                    and (
                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                            else (today() - datepart(weekday,today()))
                                                    end
                                        )
            group   by  perf.dt
                        ,boxview.account_number -- 505071
        )   as dialback
        inner join  vespa_analysts.sig_single_account_view as acview
        on  dialback.account_number = acview.account_number
where   dialback.dialling_b >= acview.num_boxes

--DP Avg. Rep. Quality of Accs. Not Returning Data LAST WEEK!!!
select  avg(rq)
from    (
            -- counting dialling boxes... (account level)
            select  perf.dt
                    ,boxview.account_number
                    ,count(distinct perf.subscriber_id) as dialling_b
                    ,avg(case when boxview.reporting_quality > 1 then 1 else boxview.reporting_quality end )    as rq
            from    vespa_analysts.panel_data                       as perf
                    inner join vespa_analysts.sig_single_box_view   as boxview
                    on  perf.subscriber_id = boxview.subscriber_id
                    and boxview.panel in ('VESPA','VESPA11')
                    and boxview.status_vespa = 'Enabled'
            where   perf.panel in (12,11)
            and     perf.data_received = 1
            and     perf.dt between (
                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                        else (today() - datepart(weekday,today()))-6
                                                end
                                    )
                                    and (
                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                            else (today() - datepart(weekday,today()))
                                                    end
                                        )
            group   by  perf.dt
                        ,boxview.account_number -- 505071
        )   as dialback
        inner join  vespa_analysts.sig_single_account_view as acview
        on  dialback.account_number = acview.account_number
where   dialback.dialling_b < acview.num_boxes

--DP - Avg. Reporting Quality of the Panel LAST WEEK!!!
select  avg (reporting_quality)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel = 'VESPA'
and     status_vespa = 'Enabled'

--DP - Accounts Returning Data Reliably LAST WEEK!!!
select  count(distinct account_number)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   reporting_quality >=0.9
and     panel = 'VESPA'
and     status_vespa = 'Enabled'

--DP Adsmartable Households Reporting LAST WEEK!!!
select  count(distinct account_number)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   reporting_quality > 0
and     panel = 'VESPA'
and     status_vespa = 'Enabled'
and     Adsmart_flag = 1

--AP Accounts Enabled LAST WEEK!!!
select  count(distinct account_number)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   status_vespa = 'Enabled'
and     panel is not null

--AP Accounts Returning Data LAST WEEK!!!
select  count(distinct acview.account_number)
from    (
            -- counting dialling boxes... (account level)
            select  perf.dt
                    ,boxview.account_number
                    ,count(distinct perf.subscriber_id) as dialling_b
            from    vespa_analysts.panel_data                       as perf
                    inner join vespa_analysts.sig_single_box_view   as boxview
                    on  perf.subscriber_id = boxview.subscriber_id
                    and boxview.panel is not null
                    and boxview.status_vespa = 'Enabled'
            where   perf.panel is not null
            and     perf.data_received = 1
            and     perf.dt between (
                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                        else (today() - datepart(weekday,today()))-6
                                                end
                                    )
                                    and (
                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                            else (today() - datepart(weekday,today()))
                                                    end
                                        )
            group   by  perf.dt 
                        ,boxview.account_number -- 505071
        )   as dialback
        inner join  vespa_analysts.sig_single_account_view as acview
        on  dialback.account_number = acview.account_number
where   dialback.dialling_b >= acview.num_boxes


--AP - Accounts Returning Data  >= 50% LAST WEEK!!!
select  count(distinct acview.account_number)
from    (
            -- counting dialling boxes... (account level)
            select  perf.dt
                    ,boxview.account_number
                    ,count(distinct perf.subscriber_id) as dialling_b
            from    vespa_analysts.panel_data                       as perf
                    inner join vespa_analysts.sig_single_box_view   as boxview -- 955377
                    on  perf.subscriber_id = boxview.subscriber_id
                    and boxview.panel is not null
                    and boxview.status_vespa = 'Enabled'
                    and boxview.reporting_quality >= 0.5
            where   perf.panel is not null
            and     perf.data_received = 1
            and     perf.dt between (
                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                        else (today() - datepart(weekday,today()))-6
                                                end
                                    )
                                    and (
                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                            else (today() - datepart(weekday,today()))
                                                    end
                                        )
            group   by  perf.dt 
                        ,boxview.account_number -- 505071
        )   as dialback
        inner join  vespa_analysts.sig_single_account_view as acview
        on  dialback.account_number = acview.account_number
where   dialback.dialling_b >= acview.num_boxes


--AP Avg. Rep. Quality of Accs. Returning Data LAST WEEK!!!
select  avg(rq)
from    (
            -- counting dialling boxes... (account level)
            select  perf.dt
                    ,boxview.account_number
                    ,count(distinct perf.subscriber_id) as dialling_b
                    ,avg(case when boxview.reporting_quality > 1 then 1 else boxview.reporting_quality end )    as rq
            from    vespa_analysts.panel_data                       as perf
                    inner join vespa_analysts.sig_single_box_view   as boxview
                    on  perf.subscriber_id = boxview.subscriber_id
                    and boxview.panel is not null
                    and boxview.status_vespa = 'Enabled'
            where   perf.panel is not null
            and     perf.data_received = 1
            and     perf.dt between (
                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                        else (today() - datepart(weekday,today()))-6
                                                end
                                    )
                                    and (
                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                            else (today() - datepart(weekday,today()))
                                                    end
                                        )
            group   by  perf.dt
                        ,boxview.account_number -- 505071
        )   as dialback
        inner join  vespa_analysts.sig_single_account_view as acview
        on  dialback.account_number = acview.account_number
where   dialback.dialling_b >= acview.num_boxes


--AP Avg. Rep. Quality of Accs. Not Returning Data LAST WEEK!!!
select  avg(rq)
from    (
            -- counting dialling boxes... (account level)
            select  perf.dt
                    ,boxview.account_number
                    ,count(distinct perf.subscriber_id) as dialling_b
                    ,avg(case when boxview.reporting_quality > 1 then 1 else boxview.reporting_quality end )    as rq
            from    vespa_analysts.panel_data                       as perf
                    inner join vespa_analysts.sig_single_box_view   as boxview
                    on  perf.subscriber_id = boxview.subscriber_id
                    and boxview.panel is not null
                    and boxview.status_vespa = 'Enabled'
            where   perf.panel is not null
            and     perf.data_received = 1
            and     perf.dt between (
                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                        else (today() - datepart(weekday,today()))-6
                                                end
                                    )
                                    and (
                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                            else (today() - datepart(weekday,today()))
                                                    end
                                        )
            group   by  perf.dt
                        ,boxview.account_number -- 505071
        )   as dialback
        inner join  vespa_analysts.sig_single_account_view as acview
        on  dialback.account_number = acview.account_number
where   dialback.dialling_b < acview.num_boxes


--AP Avg. Rep. Quality of the Panel LAST WEEK!!!
select  avg (reporting_quality)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   panel is not null
and     status_vespa = 'Enabled'

--AP - Accounts Returning Data Reliably LAST WEEK!!!
select  count(distinct account_number)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW
where   reporting_quality >=0.9
and     panel is not null
and     status_vespa = 'Enabled'

--AP - TA Call Coverage from Accounts Enabled LAST WEEK!!!
select  round((cast((sum(case when enabled = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as tacoverage
from    (
            select  case    when sbv.panel is not null and SBV.status_vespa = 'Enabled' then 1 else 0 end   as enabled
                    ,round(sum(round(ta.TA_Propensity,2)),0)                                                as sumprop
            from    limac.VESPA_TA_CALLERS_201307_SCORED_19Nov      as ta
                    left join vespa_analysts.SIG_SINGLE_BOX_VIEW    as sbv
                    on  ta.account_number = sbv.account_number
            group   by  enabled
        )   as base

--AP - TA Call Coverage from Accounts Returning Data LAST WEEK!!!
select  round((cast((sum(case when enabled = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as tacoverage
from    (
            select  case    when sbv.panel is not null and SBV.status_vespa = 'Enabled' then 1 else 0 end   as enabled
                    ,round(sum(round(ta.TA_Propensity,2)),0)                                                as sumprop
            from    limac.VESPA_TA_CALLERS_201307_SCORED_19Nov      as ta
                    left join vespa_analysts.SIG_SINGLE_BOX_VIEW    as sbv
                    on  ta.account_number = sbv.account_number
            where   ta.account_number in    (
                                                select  distinct dialback.account_number
                                                from    (
                                                            -- counting dialling boxes... (account level)
                                                            select  perf.dt
                                                                    ,boxview.account_number
                                                                    ,count(distinct perf.subscriber_id) as dialling_b
                                                            from    vespa_analysts.panel_data                       as perf
                                                                    inner join vespa_analysts.sig_single_box_view   as boxview
                                                                    on  perf.subscriber_id = boxview.subscriber_id
                                                                    and boxview.panel in ('VESPA','VESPA11')
                                                                    and boxview.status_vespa = 'Enabled'
                                                            where   perf.panel in (12,11)
                                                            and     perf.data_received = 1
                                                            and     perf.dt between (
                                                                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                                                                        else (today() - datepart(weekday,today()))-6
                                                                                                end
                                                                                    )
                                                                                    and (
                                                                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                                                                            else (today() - datepart(weekday,today()))
                                                                                                    end
                                                                                        )
                                                            group   by  perf.dt
                                                                        ,boxview.account_number -- 505071
                                                        )   as dialback
                                                        inner join  vespa_analysts.sig_single_account_view as acview
                                                        on  dialback.account_number = acview.account_number
                                                where   dialback.dialling_b >= acview.num_boxes
                                            )
            group   by  enabled
        )   as base

--AP - TA Call Coverage from Accs. Ret. Data >= 50% LAST WEEK!!!
select  round((cast((sum(case when enabled = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as tacoverage
from    (
            select  case    when sbv.panel is not null and SBV.status_vespa = 'Enabled' then 1 else 0 end   as enabled
                    ,round(sum(round(ta.TA_Propensity,2)),0)                                                as sumprop
            from    limac.VESPA_TA_CALLERS_201307_SCORED_19Nov      as ta
                    left join vespa_analysts.SIG_SINGLE_BOX_VIEW    as sbv
                    on  ta.account_number = sbv.account_number
                    and sbv.reporting_quality >= 0.5
            where   ta.account_number in    (
                                                select  distinct dialback.account_number
                                                from    (
                                                            -- counting dialling boxes... (account level)
                                                            select  perf.dt
                                                                    ,boxview.account_number
                                                                    ,count(distinct perf.subscriber_id) as dialling_b
                                                            from    vespa_analysts.panel_data                       as perf
                                                                    inner join vespa_analysts.sig_single_box_view   as boxview
                                                                    on  perf.subscriber_id = boxview.subscriber_id
                                                                    and boxview.panel in ('VESPA','VESPA11')
                                                                    and boxview.status_vespa = 'Enabled'
                                                            where   perf.panel in (12,11)
                                                            and     perf.data_received = 1
                                                            and     perf.dt between (
                                                                                        select  case    when datepart(weekday,today()) = 7 then today()-6
                                                                                                        else (today() - datepart(weekday,today()))-6
                                                                                                end
                                                                                    )
                                                                                    and (
                                                                                            select  case    when datepart(weekday,today()) = 7 then today()
                                                                                                            else (today() - datepart(weekday,today()))
                                                                                                    end
                                                                                        )
                                                            group   by  perf.dt
                                                                        ,boxview.account_number -- 505071
                                                        )   as dialback
                                                        inner join  vespa_analysts.sig_single_account_view as acview
                                                        on  dialback.account_number = acview.account_number
                                                where   dialback.dialling_b >= acview.num_boxes
                                            )
            group   by  enabled
        )   as base

--VP - Accounts Enabled LAST WEEK!!!
select  count(distinct SBV.account_number)
from    vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
        inner join    vespa_analysts.vespa_broadcast_reporting_vp_map as VP
        on sbv.account_number = vp.account_number
where   sbv.status_vespa = 'Enabled'
and     vp.vp1 = 1

--VP - Accounts Returning Data
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join    vespa_analysts.vespa_broadcast_reporting_vp_map as VP
on sbv.account_number = vp.account_number
where status_vespa = 'Enabled'
and   vp1 = 1
and Num_logs_sent_7d >0

--Sky UK Base
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW

--VESPA Scaled
select sum(weight_)
       from (
                 select distinct SAV.account_number, weight as Weight_
                 from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
                 inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
                 on SBV.account_number = SAV.account_number
                        where SBV.panel = 'VESPA'
                        and SBV.status_vespa = 'Enabled'
            ) as n

--DP - Effective Sample Size
select power(sum(weight_),2)/sum(power(weight_,2))
       from (
                 select distinct SAV.account_number, weight as Weight_
                 from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
                 inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
                 on SBV.account_number = SAV.account_number
                        where SBV.panel = 'VESPA'
                        and SBV.status_vespa = 'Enabled'
                        and Num_logs_sent_7d >0
            ) as n

--Sky UK Base - Adsmartable HH (One Box & Adsmartable)
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where Num_adsmartable_boxes = 1
and Num_boxes = 1

--Sky UK Base - Adsmartable HH (Multibox Not All Adsmartable)
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where Num_adsmartable_boxes < Num_boxes
and Num_adsmartable_boxes > 0
and Num_boxes > 1

--Sky UK Base - Adsmartable HH (Multibox All Adsmartable)
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where Num_adsmartable_boxes = Num_boxes
and Num_boxes > 1

--Sky UK Base - Not Adsmartable Households
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where Num_adsmartable_boxes = 0




