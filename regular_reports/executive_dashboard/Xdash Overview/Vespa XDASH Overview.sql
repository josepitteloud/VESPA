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
**Due Date:                             13/06/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:
	
	A Lighter version for Xdash focused on panel performance based on dialling platform...

**Modules:

M01: OPs XDASH Overview
        M01.0 - Initialising environment
        M01.1 - Building Base for Panel Performance
		M01.2 - Producing Analysis for Panel Balance (scaling variables)
		M01.3 - Setting Privileges for base tables

**Stats:

	-- running time: 10 min approx...
	
--------------------------------------------------------------------------------------------------------------
*/

create or replace procedure ops_xdash_overview
	@weekending	date	= null
as 
begin

-----------------------------------
-- M01.0 - Initialising environment
-----------------------------------

	MESSAGE now() ||' | Beginig  OPS_XDASH_OVERVIEW' TO CLIENT
	
	MESSAGE now() ||' | ETA: [90 Minutes] Run APROX' TO CLIENT
	
	MESSAGE now() ||' | Beginig  M01.0 - Initialising environment' TO CLIENT

-- Initialising base tables for slicing
					
	MESSAGE now() ||' | @ M01.0: Sampling Accounts VIQ-Scaled in past Month' TO CLIENT
	
	/*
		Sampling for everyone in the DP who has been scaled in the past month
		as per this report we are interested on representing the "account returning data"
		concept based on scaling (an account will be part of the scaling sample of a day if on that 
		day that account returned data from all of the boxes)...
	*/
	if object_id('vespa_analysts.scaling_s1') is not null
		drop table scaling_s1
		
	commit
		
	select  case   when datepart(weekday,thedate) = 7 then thedate
					else cast(dateadd(day,(7-datepart(weekday,thedate)),thedate) as date)
			end     as weekending
			,adjusted_event_start_date_vespa as thedate
			,account_number
	into    scaling_s1
	from    sk_prod.VIQ_VIEWING_DATA_SCALING
	where	adjusted_event_start_date_vespa between @weekending-29 and @weekending
	/* where   adjusted_event_start_date_vespa >=  (
													select  max(adjusted_event_start_date_vespa)-30
													from    sk_prod.VIQ_VIEWING_DATA_SCALING
												) */
	commit
	create hg index hg1 on scaling_s1(account_number)
	create date index d1 on scaling_s1(thedate)
	create date index d2 on scaling_s1(weekending)
	commit

	MESSAGE now() ||' | @ M01.0: Sampling Accounts VIQ-Scaled in past Month DONE' TO CLIENT
	
	
	MESSAGE now() ||' | @ M01.0: Determining Accounts RQs' TO CLIENT
	
	/*
		To go in line with the new requirement to generate the RQ based on the frequency
		at which an account picks up a weight in the last 30 days, we are now creating
		below table to store the RQ for each account as per what describe above...
	*/

	if object_id('vespa_analysts.ac_rq_lookup') is not null
		drop table ac_rq_lookup
		
	commit

	select  base.account_number
			,count(distinct base.thedate)                                           as hits
			,round((cast(hits as float) / cast(max(timeframe.length_) as float)),2) as RQs
	into 	ac_rq_lookup
	from    scaling_s1  as base
			inner join  (
							select  datediff(day,min(thedate),max(thedate)) as length_
							from    scaling_s1
						)   as timeframe
			on  1 = 1 --> this makes true the relation between the table for all rows
	group   by  base.account_number

	commit
	create hg index hg1 on ac_rq_lookup(Account_number)
	commit

	MESSAGE now() ||' | @ M01.0: Determining Accounts RQs DONE' TO CLIENT	
	
	MESSAGE now() ||' | @ M01.0: Sampling History of Accounts enabled in Vespa' TO CLIENT
	
	/*
		Sampling for everyone in the Vespa panel who was enabled at an specific week (week defined from last Sunday
		until last Saturday)... bringing the table from box level up to account level as that is the context of
		the Xdash overview...
	*/

	if object_id('vespa_analysts.sbvh') is not null
		drop table sbvh

	commit

	select  distinct
			weekending
			,account_number
			,panel_id
	into    sbvh
	from    vespa_analysts.vespa_sbv_hist_qualitycheck
	where	weekending between @weekending-27 and @weekending
	/* where   weekending >=   (
								select  max(adjusted_event_start_date_vespa)-30
								from    sk_prod.VIQ_VIEWING_DATA_SCALING
							) */
	commit
	create hg index hg1 on sbvh(account_number)
	create date index d1 on sbvh(weekending)
	commit

	MESSAGE now() ||' | @ M01.0: Sampling History of Accounts enabled in Vespa DONE' TO CLIENT
	
	MESSAGE now() ||' | @ M01.0: Identifying Panel assigments through history' TO CLIENT
	
	/*
		Intercepting above two tables to be able to identify which accounts below to which panel
		at a given week... hence been able to do any count broken down as needed	
	*/

	if object_id('vespa_analysts.base1') is not null
		drop table base1
		
	commit

	select  s1.*
			,sbvh.panel_id
	into    base1
	from    scaling_s1  as s1
			inner join  sbvh
			on  s1.weekending       = sbvh.weekending
			and s1.account_number   = sbvh.account_number
	commit
	create hg index hg1 on base1(account_number)
	create date index d1 on base1(weekending)
	create date index d2 on base1(thedate)
	commit

	drop table scaling_s1
	drop table sbvh
	commit

	MESSAGE now() ||' | @ M01.0: Identifying Panel assigments through history DONE' TO CLIENT
	
	MESSAGE now() ||' | @ M01.0: Counting historically number of boxes owed by accounts' TO CLIENT
	
	/*
		Sampling all boxes enabled in the Vespa panel on the last month
		to be able to then check further on how many boxes each account has
		and compare that against how many of them are actually dialling on
		a given day to satisfy the returning data definition...
	*/

	if object_id('vespa_analysts.sbvh_boxlevel') is not null
		drop table sbvh_boxlevel
		
	commit	

	select  distinct
			weekending
			,account_number
			,subscriber_id
			,panel_id
	into    sbvh_boxlevel
	from    vespa_analysts.vespa_sbv_hist_qualitycheck
	where	weekending between @weekending-27 and @weekending
	/* where   weekending >=   (
								select  max(adjusted_event_start_date_vespa)-30
								from    sk_prod.VIQ_VIEWING_DATA_SCALING
							) */

	commit
	create hg index hg1 on sbvh_boxlevel(account_number)
	create hg index hg2 on sbvh_boxlevel(subscriber_id)
	create date index d1 on sbvh_boxlevel(weekending)
	commit
	
	MESSAGE now() ||' | @ M01.0: Counting historically number of boxes owed by accounts DONE' TO CLIENT
	
	MESSAGE now() ||' | @ M01.0: Sampling Accounts dialbacks over a month' TO CLIENT

	/*
		Sampling from the panel data
	*/

	if object_id('vespa_analysts.dialsample') is not null
		drop table dialsample
		
	commit

	select  subscriber_id
			,dt
	into    dialsample
	from    vespa_analysts.panel_data
	where	dt between @weekending-29 and @weekending
	/* where   dt >=   (
						select  max(adjusted_event_start_date_vespa)-30
						from    sk_prod.VIQ_VIEWING_DATA_SCALING
					) */
	and     data_received = 1

	commit
	create hg index hg1 on dialsample(subscriber_id)
	create date index d1 on dialsample(dt)
	commit

	MESSAGE now() ||' | @ M01.0: Sampling Accounts dialbacks over a month DONE' TO CLIENT
	
	MESSAGE now() ||' | @ M01.0: Summarising Dialback interaction at account level (ETA: 20 mins APROX)' TO CLIENT
	
	if object_id('vespa_analysts.dialsample_aclevel') is not null
		drop table dialsample_aclevel
		
	commit

	select  dial.dt
			,sbvh.account_number
			,count(Distinct dial.subscriber_id) as dialling_boxes
	into    dialsample_aclevel
	from    dialsample              as dial
			left join sbvh_boxlevel as sbvh
			on  dial.subscriber_id = sbvh.subscriber_id
	group   by  dial.dt
				,sbvh.account_number

	commit
	create hg index hg1 on dialsample_aclevel(account_number)
	create date index d1 on dialsample_aclevel(dt)
	commit

	MESSAGE now() ||' | @ M01.0: Summarising Dialback interaction at account level DONE' TO CLIENT
	
	MESSAGE now() ||' | @ M01.0: Detecting accounts returning data' TO CLIENT

	if object_id('vespa_analysts.returning_accounts') is not null
		drop table returning_accounts
		
	commit

	select  a.dt    as thedate
			,a.account_number
			,case   when datepart(weekday,thedate) = 7 then thedate
					else cast(dateadd(day,(7-datepart(weekday,thedate)),thedate) as date)
			end     as weekending
	into    returning_accounts
	from    dialsample_aclevel  as A
			inner join  (
							select  account_number
									,count(distinct subscriber_id)  as total_boxes
							from    sbvh_boxlevel
							group   by  account_number
						)   as B
			on  a.account_number = b.account_number
	where   a.dialling_boxes >= b.total_boxes

	commit
	create hg index hg1 on returning_accounts(account_number)
	create date index d1 on returning_accounts(weekending)
	create date index d2 on returning_accounts(thedate)
	commit

	MESSAGE now() ||' | @ M01.0: Detecting accounts returning data DONE' TO CLIENT
	MESSAGE now() ||' | Beginig  M01.0 - Initialising environment DONE' TO CLIENT

----------------------------------------------
-- M01.1 - Building Base for Panel Performance
----------------------------------------------

	MESSAGE now() ||' | Beginig  M01.1 - Building Base for Panel Performance' TO CLIENT

	MESSAGE now() ||' | @ M01.1: Measuring AP interaction' TO CLIENT
	
	-- AP Full week/month	
	declare @themaxdt    date

	select  @themaxdt = max(thedate)-6 from returning_accounts

	if object_id('vespa_analysts.apfullweekmonth') is not null
		drop table apfullweekmonth
		
	commit
			
	select	b.thepanel
			,b.dialway
			,sum	(
						case	when thepanel = 'AP' and dialway = 'PSTN' and a.full7d>=4	then 1
								when a.full7d>=7											then 1
								else 0
						end
					)	as fulllastweek
			,sum    (
						case   when thepanel = 'AP' and dialway = 'PSTN' and full30d>=15    then 1
							   when full30d>=24                                           	then 1
						end
					)   as fulllastmonth
	into    apfullweekmonth
	from    (
				select  account_number
						,sum(case   when thedate >= @themaxdt then 1 else 0 end) as full7d
						,count(distinct thedate) as full30d
				from    returning_accounts
				group   by  account_number
			)   as A
			inner join  (
							select  distinct
									account_number
									,case    when panel_id in (11,12)    then 'DP'
											when panel_id in (5,6,7)    then 'AP'
									end     as thepanel
									,case   when panel_id in (12,6,7)   then 'PSTN'
											when panel_id in (5,11)     then 'BB'
									end     as dialway
							from    sbvh_boxlevel
						)   as b
			on  a.account_number    = b.account_number
			and b.thepanel          = 'AP'
	group   by  b.thepanel
				,b.dialway -- Sly
						
	commit
			
	MESSAGE now() ||' | @ M01.1: Measuring AP interaction DONE' TO CLIENT
	
	MESSAGE now() ||' | @ M01.1: Measuring DP interaction' TO CLIENT
						
	-- DP once and full week/month	
	select  @themaxdt = max(thedate)-6 from base1

	if object_id('vespa_analysts.dpfullweekmonth') is not null
		drop table dpfullweekmonth
		
	commit
						
	select  panel_id
			,count(distinct account_number)                     as once30d
			,sum(case when full7d >0 then 1 else 0 end)         as once7d
			,sum(case when full7d >=7 then 1 else 0 end)        as fulldial7d
			,sum(case when overalldial >=27 then 1 else 0 end)  as fulldial30d
	into    dpfullweekmonth
	from    (
				select  panel_id
						,account_number
						,count(distinct thedate) as overalldial
						,sum(case when thedate >= @themaxdt then 1 else 0 end) as full7d
				from    base1
				where   panel_id in (11,12)
				group   by  panel_id
							,account_number
			)   as base
	group   by   panel_id -- Sly

	commit

	MESSAGE now() ||' | @ M01.1: Measuring DP interaction DONE' TO CLIENT
	MESSAGE now() ||' | Beginig  M01.1 - Building Base for Panel Performance DONE' TO CLIENT
	
-------------------------------------------------------------------
-- M01.2 - Producing Analysis for Panel Balance (scaling variables)
-------------------------------------------------------------------

	MESSAGE now() ||' | Beginig  M01.2 - Producing Analysis for Panel Balance (scaling variables)' TO CLIENT
	
	execute xdash_ov_variable_metrics @weekending
	
	MESSAGE now() ||' | Beginig  M01.2 - Producing Analysis for Panel Balance (scaling variables) DONE' TO CLIENT
	

---------------------------------------------
-- M01.3 - Setting Privileges for base tables
---------------------------------------------

	MESSAGE now() ||' | Beginig  M01.3 - Setting Privileges for base tables' TO CLIENT
	
	grant select on base1				to vespa_group_low_security
	grant select on sbvh_boxlevel		to vespa_group_low_security	 
	grant select on dialsample			to vespa_group_low_security	
	grant select on dialsample_aclevel	to vespa_group_low_security	
	grant select on returning_accounts	to vespa_group_low_security	
	grant select on apfullweekmonth		to vespa_group_low_security	
	grant select on dpfullweekmonth		to vespa_group_low_security	
	grant select on ac_rq_lookup		to vespa_group_low_security
	commit
	
	MESSAGE now() ||' | Beginig  M01.3 - Setting Privileges for base tables DONE' TO CLIENT
	
	MESSAGE now() ||' | OPS_XDASH_OVERVIEW DONE' TO CLIENT
	 
end;

commit;
grant execute on ops_xdash_overview to vespa_group_low_security;

commit;








