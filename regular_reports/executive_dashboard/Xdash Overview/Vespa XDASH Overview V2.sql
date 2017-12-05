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
**Stakeholder:                          Gavin Meggs
**Due Date:                             17/10/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:
	
	A Lighter version for Xdash focused on panel performance based on dialling platform...
	
	Below names for each of the Sections were also used in the excel report since all of these
	selects are slicers to make up the report...

**Modules:

S01: OPs XDASH Overview
        S01.0 - Accounts Enabled
        S01.1 - Accounts Returning Data
		S01.2 - Accounts Typically Returning Data
		S01.3 - Scaling Reporting Quality Distribution Graph

**Stats:

	-- running time: 10 min approx...
	
--------------------------------------------------------------------------------------------------------------
*/


---------------------------
-- S01.0 - Accounts Enabled
---------------------------

select  case    when thepanel in ('VESPA','ALT6','ALT7')        then 'PSTN'
                when thepanel in ('ALT5','VESPA11')             then 'Broadband'
        end     as  Dial_Format
        ,sum(case   when thepanel in ('VESPA','VESPA11')        then 1 else 0 end)  as Daily
        ,sum(case   when thepanel in ('ALT5','ALT6','ALT7')     then 1 else 0 end)  as Alternate
from    (
            select  distinct
                    account_number
                    ,panel as thepanel
            from    angeld.sig_single_account_view
            where   status_vespa = 'Enabled'
        )   as sbv
group   by  Dial_Format
order by Dial_Format desc


----------------------------------
-- S01.1 - Accounts Returning Data
----------------------------------

select  case    when panel in ('VESPA','ALT6','ALT7')   then 'PSTN'
                when panel in ('ALT5','VESPA11')        then 'Broadband'
        end     as  Dial_Format
        ,case   when panel in ('ALT5','ALT6','ALT7')    then 'Alternate'
                when panel in ('VESPA','VESPA11')       then 'Dialy'
        end     as over_panel
        ,case   when dial_format = 'PSTN' and over_panel = 'Alternate'  then    avg(o30d)
                else sum(o30d)
        end     as o30d
        ,case   when dial_format = 'PSTN' and over_panel = 'Alternate'  then    avg(o7d)
                else sum(o7d)
        end     as o7d
        ,case   when dial_format = 'PSTN' and over_panel = 'Alternate'  then    avg(f30d)
                else sum(f30d)
        end     as f30d
        ,case   when dial_format = 'PSTN' and over_panel = 'Alternate'  then    avg(f7d)
                else sum(f7d)
        end     as f7d
from    (
            select  panel
                    ,sum(case when num_ac_returned_30d > 0 then 1 else 0 end)   as o30d
                    ,sum(case when num_ac_returned_7d > 0 then 1 else 0 end)    as o7d
                    ,sum(ac_full_returned_30d)  as f30d
                    ,sum(ac_full_returned_7d)   as f7d
            from    sig_single_account_view
            where   panel is not null
            and     status_vespa = 'Enabled'
            group   by  panel
        )   as base
group   by  dial_format
            ,over_panel


--------------------------------------------
-- S01.2 - Accounts Typically Returning Data
--------------------------------------------

	declare @todt	date	

	-- calculating the date for last Saturday... which is the end of the week for our time frame...
	-- this is the mark from where we analyse the performance of boxes and accounts back into 30 days...
	select  @todt =	case	when datepart(weekday,today()) = 7 then today()
							else (today() - datepart(weekday,today()))
					end
    
	-- a list of accounts and how many boxes each has...
	select  panel		
			,account_number
			,count(distinct subscriber_id) 	as num_boxes
			,min(enablement_date)			as enablement_date
	into	#acview
	from    m04_t1_panel_sample_stage0
	where	panel is not null
	group   by  panel
				,account_number
	
	commit
	create hg index hg1 on #acview(account_number)
	create lf index lf1 on #acview(panel)
	commit
	
	-- counting for each day on the past 30 days the number of boxes that dialed
	-- for every single account...
	select  perf.dt
			,boxview.account_number
			,count(distinct perf.subscriber_id) as dialling_b
	into	#panel_data
	from    vespa_analysts.panel_data               as perf
			inner join  m04_t1_panel_sample_stage0	as boxview
			on  perf.subscriber_id = boxview.subscriber_id
			and boxview.panel is not null
			and boxview.status_vespa = 'Enabled'
			and	boxview.panel in ('ALT5','ALT6','ALT7')
	where   perf.panel is not null
	and     perf.data_received = 1
	and     perf.dt between @todt-29 and @todt
	group   by  perf.dt 
				,boxview.account_number
	
	commit
	create date index date1 on #panel_data(dt)
	create hg index hg1 	on #panel_data(account_number)
	commit


	select  case    when panel in ('VESPA','ALT6','ALT7')        then 'PSTN'
                    when panel in ('ALT5','VESPA11')             then 'Broadband'
            end     as  Dial_Format
            ,avg(case   when panel in ('VESPA','VESPA11')        then scaling_candidates else null end)  as Daily
            ,avg    (
                        case    when panel = 'ALT5'                                         then scaling_candidates
                                when panel in ('ALT6','ALT7') and scaling_candidates <> 0   then scaling_candidates 
                                                                                            else null 
                        end
                    )   as Alternate
    from    (
                select  acview.panel
                        ,panel_data.dt
                        ,count(distinct (
                                            case    when acview.panel = 'ALT6' and datepart(day,panel_data.dt)%2 >0 then panel_data.account_number
                                                    when acview.panel = 'ALT7' and datepart(day,panel_data.dt)%2 =0 then panel_data.account_number
                                                    when acview.panel = 'ALT5'                                      then panel_data.account_number
                                                                                                                    else null
                                            end
                                        ))  as scaling_candidates
                from    #acview as acview
                	    inner join  #panel_data as panel_data
                		on  acview.account_number   = panel_data.account_number
                where   panel_data.dialling_b >= acview.num_boxes
                group   by  acview.panel
                            ,panel_data.dt
                union   
                select  ssav.panel
                        ,viq.adjusted_event_start_Date_vespa    as dt
                		,count(distinct viq.account_number)     as scaling_candidates
                from    sk_prod.VIQ_VIEWING_DATA_SCALING    as viq
                        inner join sig_single_account_view  as ssav
                        on  viq.account_number  = ssav.account_number
                        and ssav.panel in ('VESPA','VESPA11')
                where   adjusted_event_start_Date_vespa between @todt-29 and @todt
                and     ssav.panel is not null
                group   by  ssav.panel
                            ,dt
            )   as base
    group   by  dial_format

	
-------------------------------------------------------
-- S01.3 - Scaling Reporting Quality Distribution Graph
-------------------------------------------------------

select  thepanel
        ,thedecile
        ,avg(new_rq)                    as theavg
        ,count(distinct account_number) as nboxes
from    (
            select  case    when panel = 'VESPA'            then 'DP PSTN'
                            when panel = 'VESPA11'          then 'DP BB'
                            when panel in ('ALT6','ALT7')   then 'AP PSTN'
                            when panel = 'ALT5'             then 'AP BB'
                    end     as thepanel
                    ,account_number
                    ,case when scaling_reporting_quality > 1 then 1 else scaling_reporting_quality end as new_rq
                    ,ntile(10) over (
                                        partition by    panel
                                        order by        new_rq
                                    )   as thedecile
            from    sig_single_account_view
            where   status_vespa = 'Enabled'
            and     scaling_reporting_quality is not null
        )   as base
group   by  thepanel
            ,thedecile