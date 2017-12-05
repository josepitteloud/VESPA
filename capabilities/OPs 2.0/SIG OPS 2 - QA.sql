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

	This is the script that contains the QA checks for the OPs 2.0 weekly build.

**Sections:

	S01 - Composition Fundamental Checks
	S02 - Panel Volumes 
	S03 - Vespa Status Volumes
	S04 - Primary / Secondary Box Flag Comparison
	S05 - Accounts Returning Data Over 30 Days
	S06 - Boxes Returning Data Over 30 Days
	S07 - Boxes Returning Data Over 30 Days as per Olive
	S08 - Effective Sample Size

--------------------------------------------------------------------------------------------------------------
*/


---------------------------------------
-- S01 - Composition Fundamental Checks
---------------------------------------
/*
	the values for the "Measure" column intend to explain what the check is about
*/

select  *
from    (
            select  'SSAV'                          as source
                    ,'Accounts on multiple panels'  as Measure
                    ,count(1)                       as hits
            from    (
                        select  account_number
                                ,count(distinct panel) as hits
                        from    sig_single_account_view
                        group   by  account_number
                        having  hits > 1
                    )   as base
            union   
            select  'SSBV'                      as source
                    ,'Boxes on multiple panels' as Measure
                    ,count(1)                   as hits
            from    (
                        select  subscriber_id
                                ,count(distinct panel) as hits
                        from    sig_single_box_view
                        group   by  subscriber_id
                        having  hits > 1
                    )   as base
            union   
            select  'SSBV'                          as source
                    ,'Boxes on multiple Accounts'   as Measure
                    ,count(1)                       as hits
            from    (
                        select  subscriber_id
                                ,count(distinct account_number) as hits
                        from    sig_single_box_view
                        group   by  subscriber_id
                        having  hits > 1
                    )   as base
            union   
            select  'SSAV'                          as source
                    ,'Accounts Duplicated in SSAV'  as Measure
                    ,nrows - naccounts              as hits
            from    (
                        select  count(1)                        as nrows
                                ,count(distinct account_number) as naccounts
                        from    sig_single_account_view
						where	account_number is not null
                    )   as base
            union   
			select  'SSBV'                      as source
                    ,'Boxes Duplicated in SSBV' as Measure
                    ,nrows - nboxes             as hits
            from    (
                        select  count(1)                        as nrows
                                ,count(distinct subscriber_id)  as nboxes
                        from    sig_single_box_view
                        where   subscriber_id is not null
                    )   as base
			union			
			select  'SSAV'  as source
					,'Account Number left as NULL'  as Measure
					,hits
			from    (
						select  count(1)    as hits
						from    sig_single_account_view
						where   account_number is null
					)   as base
			union
			select  'SSBV'  as source
					,'Subscriber ID left as NULL'   as Measure
					,hits
			from    (
						select  count(1)    as hits
						from    sig_single_box_view
						where   subscriber_id is null
					)   as base
        )   as super
order   by hits desc



----------------------
-- S02 - Panel Volumes
----------------------

select  'Accounts Enabled (SSAV)'       as Level
        ,coalesce(panel,'Remanent')     as [Vespa Panel]
        ,count(distinct account_number) as naccounts
from    sig_single_account_view
where   status_vespa = 'Enabled'
group   by   [Vespa Panel]
union
select  'Box Enabled (SSBV)'            as Level
        ,coalesce(panel,'Remanent')     as [Vespa Panel]
        ,count(distinct subscriber_id)  as naccounts
from    sig_single_box_view
where   status_vespa = 'Enabled'
group   by   [Vespa Panel]
union
select  'Accounts Distribution (SSAV)'      as Level
        ,coalesce(panel,'Outside Vespa')    as [Vespa Panel]
        ,count(distinct account_number)     as naccounts
from    sig_single_account_view
group   by   [Vespa Panel]
union
select  'Box Distribution (SSBV)'         	as Level
        ,coalesce(panel,'Outside Vespa')	as [Vespa Panel]
        ,count(distinct subscriber_id)      as naccounts
from    sig_single_box_view
group   by   [Vespa Panel]


-----------------------------
-- S03 - Vespa Status Volumes
-----------------------------

select  'SSAV'                          as source
        ,coalesce(panel,'Remanent')     as [Vespa Panel]
        ,status_vespa                   as [Panel status]
        ,count(distinct account_number) as Accounts
from    sig_single_account_view
group   by  [Vespa Panel]
            ,[Panel status]
union
select  'SSBV'  as source
        ,coalesce(panel,'Remanent')     as [Vespa Panel]
        ,status_vespa                   as [Panel status]
        ,count(distinct subscriber_id)  as Boxes
from    sig_single_box_view
group   by  [Vespa Panel]
            ,[Panel status]
			
			
			
------------------------------------------------
-- S04 - Primary / Secondary Box Flag Comparison
------------------------------------------------

select  case    when ps_olive = 'S' and ps_vespa = 'P'  then 'Sources Conflicts'
                when ps_olive = 'P' and ps_vespa = 'S'  then 'Sources Conflicts'
                when ps_olive = 'P' and ps_vespa = 'P'  then 'Sources Agree'
                when ps_olive = 'S' and ps_vespa = 'S'  then 'Sources Agree'
                when ps_olive = 'U' and ps_vespa = 'P'  then 'Derived from Vespa'
                when ps_olive = 'U' and ps_vespa = 'S'  then 'Derived from Vespa'
                when ps_olive = 'P' and ps_vespa = 'U'  then 'Derived from Olive'
                when ps_olive = 'S' and ps_vespa = 'U'  then 'Derived from Olive'
                else 'Outside Vespa'
        end     as [P/S Derivation]
        ,count(distinct subscriber_id)  as boxes
from    sig_single_box_view
group   by  [P/S Derivation]


---------------------------------------------
-- S05 - Accounts Returning Data Over 30 Days
---------------------------------------------
/*
	Is important to understand the concept of an account returning data...
	We say an account returned data on a given day, if and only if, on that day
	we collected data from all of the boxes associated with that account, case contrary
	it will not be counted in this KPI...
*/

select  panel
        ,num_ac_returned_30d
        ,count(distinct account_number) as naccounts
from    sig_single_account_view
where   panel is not null
group   by   panel
            ,num_ac_returned_30d
			
			
			
------------------------------------------
-- S06 - Boxes Returning Data Over 30 Days
------------------------------------------

select  panel
        ,num_logs_sent_30d
        ,count(1)   as hits
from    sig_single_box_view
where   panel is not null
and     num_logs_sent_30d is not null
group   by  panel
            ,num_logs_sent_30d
			
			
-------------------------------------------------------
-- S07 - Boxes Returning Data Over 30 Days as per Olive
-------------------------------------------------------

declare @from_dt			date
declare	@to_dt				date
declare @event_from_date 	integer
declare @event_to_date      integer
declare @profiling_day		date

select @profiling_day 	= max(cb_data_date) from sk_prod.cust_single_account_view
	
set @to_dt 				= @profiling_day 																-- YYYY-MM-DD
set @from_dt 			= @profiling_day -29															-- YYYY-MM-DD
set @event_from_date    = convert(integer,dateformat(dateadd(day, -29, @profiling_day),'yyyymmddhh'))	-- YYYYMMDD00
set @event_to_date      = convert(integer,dateformat(@profiling_day,'yyyymmdd')+'23')	                -- YYYYMMDD23


select  *
from    (
            select  thepanel
                    ,dials
                    ,count(1)   as hits
            from    (
                        select  case    when panel_id = 11  then 'VESPA11'
                                        when panel_id = 12  then 'VESPA'
                                end     as thepanel
                                ,subscriber_id
                                ,count(distinct( convert(date, dateadd(hh, -6, log_received_start_date_time_utc)))) as dials
                        from   	sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
                        where  	panel_id in (12,11)
                        and     dk_event_start_datehour_dim between @event_from_date and @event_to_date
                        and     LOG_RECEIVED_START_DATE_TIME_UTC is not null
                        and     LOG_START_DATE_TIME_UTC is not null
                        and     subscriber_id > 0 -- to avoid nulls and -1...
                        group   by  thepanel
                                    ,subscriber_id
                    )   as base
            group   by  thepanel
                        ,dials
            union
            select  thepanel
                    ,dials
                    ,count(1)   as hits
            from    (
                        select  case    when panel = 6  then 'ALT6'
                                        when panel = 7  then 'ALT7'
                                        when panel = 5  then 'ALT5'
                                end     as thepanel
                                ,subscriber_id
                                ,count(Distinct dt) as dials
                        from    vespa_analysts.panel_data
                        where   panel in (5,6,7)
                        and     dt between @from_dt and @to_dt
                        and     data_received   = 1
                        group   by  thepanel
                                    ,subscriber_id
                    )   as base
            group   by  thepanel
                        ,dials
        )   as base			
		
		
------------------------------
-- S08 - Effective Sample Size
------------------------------

select  'VESPA Weights'                             as source
        ,sum(weight)                                as [Sum of Weights]
        ,power(sum(weight),2)/sum(power(weight,2))  as ESS
from    sig_single_account_view
union
select  'VIQ Weights'                                       as source
        ,sum(viq_weight)                                    as [Sum of Weights]
        ,power(sum(viq_weight),2)/sum(power(viq_weight,2))  as ESS
from    sig_single_account_view

