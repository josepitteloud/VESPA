 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

	This script holds the views designed to validate outputs from the OPs 2.0 Platform
	this is just a one-time-run script, the results are on an excel sheet located at:
	G:\RTCI\Sky Projects\Vespa\Measurements and Algorithms\Operational Reports\2 - Projects\OPS 2.0\Validation\

**Sections:

--------------------------------------------------------------------------------------------------------------
*/

declare @profiling_thursday date

execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output


-- 1
select  'SBV'   as source
        ,panel
        ,count(distinct account_number) as n_accounts
        ,count(distinct subscriber_id)  as n_boxes
        ,avg(reporting_quality)         as avg_rq
from    vespa_analysts.vespa_single_box_view
where   status_vespa = 'Enabled'
group   by  source
            ,panel
union   all
select  'OPS2.0'   as source
        ,sav.panel
        ,count(distinct sbv.account_number) as n_accounts
        ,count(distinct sbv.subscriber_id)  as n_boxes
        ,avg(sbv.reporting_quality)         as avg_rq
from    sig_single_box_view as sbv
        inner join sig_single_account_view as sav
        on  sbv.account_number = sav.account_number
where   sav.status_vespa = 'Enabled'
group   by  source
            ,sav.panel


-- 2
declare @profiling_thursday date

execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output

select  'SBV'   as source
        ,panel
        ,reporting_performance
        ,count(distinct account_number) as hits
from    (
            select  account_number
                    ,panel
                    ,case   when datediff(day, max(Enablement_date), @profiling_thursday) < 15      then 'Recently enabled'
                            when min(logs_returned_in_30d) = 1                                      then 'Acceptable'
                            when min(logs_returned_in_30d) >= 27 or min(reporting_quality) >= 0.9   then 'Acceptable'
                            when max(logs_returned_in_30d) = 0                                      then 'Zero reporting'
                                                                                                    else 'Unreliable'
                    end     as Reporting_performance
            from    vespa_analysts.vespa_single_box_view
            where   status_vespa = 'Enabled'
            group   by  account_number
                        ,panel
        )   as base
group   by  source
            ,panel
            ,reporting_performance
union   all
select  'OPs 2.0'   as source
        ,panel
        ,Reporting_performance
        ,count(distinct account_number) as hits
from    sig_single_account_view
where   status_vespa = 'Enabled'
group   by  source
            ,panel
            ,reporting_performance
			
			
			
-- 3

select  'SBV'   as source
        ,box_type_subs as Box_Type
        ,sum(case when panel in ( 'VESPA','VESPA11') then 1 else 0 end) as Vespa_DP
from    vespa_analysts.vespa_single_box_view
where   status_vespa = 'Enabled'
group   by  source
            ,box_type_subs
union   all
select  'OPs 2.0'   as source
        ,box_type_subs as Box_Type
        ,sum(case when panel in ( 'VESPA','VESPA11') then 1 else 0 end) as Vespa_DP
from    sig_single_account_view
where   status_vespa = 'Enabled'
group   by  source
            ,box_type_subs
			
			
-- 4
select  'SBV'   as source
        ,panel
        ,avg(logs_returned_in_30d)  as avg_month_return
from    vespa_analysts.vespa_single_box_view
group   by  source
            ,panel
union   all
select  'OPs 2.0'   as source
        ,panel
        ,avg(num_logs_sent_30d)     as avg_month_return
from    sig_single_box_view
where   status_vespa = 'Enabled'
group   by  source
            ,panel
			
			
-- 5 vol of actives accounts

select  'SAV'   as source
        ,count(distinct account_number) as Active_accounts
from	sk_prod.CUST_SINGLE_ACCOUNT_VIEW
where   CUST_ACTIVE_DTV = 1
and     pty_country_code = 'GBR'
union   all
select  'OPs 2.0'   as source
        ,count(distinct account_number) as Active_accounts
from    sig_single_account_view

-- 6 vol of active boxes for all active accounts

select  'CSTB'	as source		
		,count(distinct c.si_external_identifier)   as active_boxes
from    (
            select  account_number
                    ,service_instance_id
            from    (
                        Select  account_number
                                ,service_instance_id
                                ,currency_code
                                ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
                        from    sk_prod.CUST_SET_TOP_BOX
                    )   as base
            where   active_flag = 1
            and     currency_code = 'GBP'
        )   as  tlbxtemp
        inner join  (
                        select  distinct account_number
                        from	sk_prod.CUST_SINGLE_ACCOUNT_VIEW
                    	where   CUST_ACTIVE_DTV = 1
                    	and     pty_country_code = 'GBR'
                    )   as b
        on  tlbxtemp.account_number         = b.account_number
        inner join  sk_prod.cust_service_instance   as c
        on  tlbxtemp.service_instance_id    = c.src_system_id
union
select  'OPs 2.0'
        ,count(distinct card_subscriber_id)
from    sig_single_box_view


-- 7 Scaling SOW and checking volume of scaling sample

select  'OPs 2.0'   as source
        ,sum(weight)    as sow
        ,sum(case when weight is not null then 1 else 0 end) as scaling_sample
from    sig_single_account_view
union
select  'SC2'               as source
        ,sum_of_weights     as sow
        ,vespa_panel        as scaling_sample
from    vespa_analysts.sc2_metrics
where   scaling_date = (select max(scaling_date) from vespa_analysts.sc2_metrics)


-- 8 Traffic lights Comparison

select	'OPs 2.0'   as source
		,sequencer ||'- '||trim(variable_name)    as [Variable Name]
		,sum(case when panel = 'DP' then imbalance_rating else 0 end) 	as DP_Imbalance
		,sum(case when panel = 'AP' then imbalance_rating else 0 end)	as AP_Imbalance
from 	vespa_traffic_lights_hist
where 	weekending = ( select max(weekending) from vespa_traffic_lights_hist)
group 	by	variable_name
union   
select  'OPs'   as source
		,sequencer ||'- '||trim(variable_name)    as [Variable Name]
		,min(case when panel = 'DP' then imbalance_rating end) as DP_Imbalance
		,min(case when panel = 'AP' then imbalance_rating end) as AP_Imbalance
from    vespa_analysts.vespa_PanMan_09_traffic_lights
group   by  variable_name


/*------------------
WORHT PUTTING IN PLACE
--------------------*/
select  panel
        ,count(Distinct subscriber_id)  as nboxes
        ,avg(rq)                        as avgrq
        ,avg(logs_ret_30)               as avglogs
        ,stddev(logs_ret_30)            as devlogs
from    rq_checks
group   by  panel

select  panel
        ,logs_ret_30
        ,count(1)   as hits
from    rq_checks
group   by  panel
            ,logs_ret_30