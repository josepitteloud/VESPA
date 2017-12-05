--Personalised Services Consent
select count(account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where viewing_consent_flag = 'Y'

/*
select count (account_number),viewing_consent_flag
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
group by viewing_consent_flag
*/

--DP Accounts Enabled
select count (distinct account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where status_vespa = 'Enabled'
and panel = 'VESPA'

--DP Accounts Returning Data
select count (distinct account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where Num_logs_sent_7d > 0
and panel = 'VESPA'
and status_vespa = 'Enabled'

--DP Accounts Returning Data
select count (distinct account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where Num_logs_sent_7d > 0
and panel = 'VESPA'
and status_vespa = 'Enabled'

--DP Panel Representation
select sum(weight)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SBV.account_number = SAV.account_number
where SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'

--AP Accounts Enabled
select count (distinct account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where status_vespa = 'Enabled'
and panel is not null

--AP Accounts Returning Data
select count (distinct account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where Num_logs_sent_7d > 0
and panel is not null
status_vespa = 'Enabled'

--AP Avg. Rep. Quality of Accs. Returning Data
select avg (reporting_quality)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where Num_logs_sent_7d > 0
and panel is not null
and status_vespa = 'Enabled'

--AP - TA Call Coverage from Accounts Enabled
select  round((cast((sum(case when enabled = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as tacoverage
                from    (
                                                                select  case when sbv.panel is not null and SBV.status_vespa = 'Enabled' then 1 else 0 end   as enabled
                                                                                ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                                                from    limac.VESPA_TA_CALLERS_201307_SCORED_19Nov  as ta
                                                                left join   vespa_analysts.SIG_SINGLE_BOX_VIEW   as sbv
                                                                on  ta.account_number = sbv.account_number
                                                                group   by  enabled
                )   as n


--AP - TA Call Coverage from Accounts Returning Data
select  round((cast((sum(case when enabled = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as tacoverage
                from    (
                                                                select  case when sbv.panel is not null and SBV.status_vespa = 'Enabled' and Num_logs_sent_7d > 0 then 1 else 0 end   as enabled
                                                                                ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                                                from    limac.VESPA_TA_CALLERS_201307_SCORED_19Nov  as ta
                                                                left join   vespa_analysts.SIG_SINGLE_BOX_VIEW   as sbv
                                                                on  ta.account_number = sbv.account_number
                                                                group   by  enabled
                )   as n
                
--VP - Accounts Returning Data
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join    vespa_analysts.vespa_broadcast_reporting_vp_map as VP
on sbv.account_number = vp.account_number
where status_vespa = 'Enabled'
and   vp1 = 1
and Num_logs_sent_7d > 0
