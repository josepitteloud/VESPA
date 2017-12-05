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
**Project Name:							RSMB Support
**Analysts:                             Angel Donnarumma / Patrick Igonor
**Lead(s):                              Jose Loureda / Claudio Lima
**Stakeholder:                          RSMB
**Due Date:                             06/12/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:
	
	Extracting from Netezza the scaling segments to be in line with the weights used in Olive on VIQ table
	(These weights coming from CBI)

**Sections:

1	Response Rates			Daily Response Rates
2	Panel Continuity		Tenure of Drop-off Home
3	Panel Continuity		Monthly Panel Continuity
4	Response Rates			Monthly Response Rates
5	Panel Composition		Panel Composition
6	Panel Continuity		Reporting Continuity
--------------------------------------------------------------------------------------------------------------
*/

---------------------------
-- Daily Response Rates [1]
---------------------------

select  viq.adjusted_event_start_date_vespa as thedate
        ,interval.control_cell
        ,sum(case when thedate between interval.interval_starts and interval.interval_ends then 1 else 0 end) as vol_response
from    sk_prod.VIQ_VIEWING_DATA_SCALING    as viq
        inner join igonorp.sc_interval      as interval
        on  viq.account_number = interval.account_number
where   viq.adjusted_event_Start_date_vespa between '2013-10-01' and '2013-10-31'
group   by  thedate
            ,interval.control_cell
			
			
------------------------------
-- Tenure of Drop-off Home [2]
------------------------------

select  dropers.woy as week_of_year
        ,intervals.control_cell
        ,dropers.dropoff_reason
        ,(week_of_year - viq.pseudo_Start)  as recency
        ,count(distinct viq.account_number) as volume
--into    check2
from    (
            -- Sample of DP Responders in October
            select  scaling.adjusted_event_start_date_vespa   as viq_date
                    ,scaling.account_number
                    ,panel_hist.pseudo_start
            from    sk_prod.VIQ_VIEWING_DATA_SCALING    as scaling
                    inner join  (   
                                    select  account_number
                                            ,min(calendar.utc_week_in_year) as pseudo_start
                                    from    vespa_analysts.vespa_sbv_hist_qualitycheck as A
                                            inner join	(
															select  distinct
																	utc_day_date        as thedate
																	,utc_week_in_year   as theweek
															from    sk_prod.VESPA_CALENDAR 
															--where   utc_day_date between '2013-10-01' and '2013-10-31'
														)	as calendar
                                            on  A.weekending = calendar.utc_day_date
                                    group   by  account_number
                                )   as panel_hist
                    on  scaling.account_number = panel_hist.account_number
            where   scaling.adjusted_event_start_date_vespa between '2013-10-01' and '2013-10-31'
        )   as viq
        inner join  igonorp.sc_interval     as intervals
        on  viq.account_number = intervals.account_number
        inner join  (
                        -- LIST OF DROPPERS...
                        select  calendar.theweek    as woy
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
                        where   upper(lookup.campaign_name) like 'VESPA_DISABLEMENT_WEEKLY_%'
                        and     cast(lookup.writeback_datetime as date) between '2013-10-01' and '2013-10-31'
                        and     lookup.cell_name not in ('AnytimePlusEnablements & TransfersToPanel12')
                    )   as dropers
        on  viq.account_number = dropers.account_number
where   viq.viq_date between intervals.interval_starts and intervals.interval_ends
group   by  week_of_year
            ,intervals.control_cell
            ,dropers.dropoff_reason
            ,recency
			

			
-------------------------------
-- Monthly Panel Continuity [3]
-------------------------------

-- Sample 1: with RQ >= 0.9

select	control_cell
		,dp_frequency
		,count(distinct account_number) as hh_volume
from	(		
			select	scaling.account_number -- UNIQUE AT ACCOUNT LEVEL...
					,lookup.CONTROL_CELL
					,round((count(distinct scaling.event_Start_date)/ 28.00),2)	as rq_february
					,count(distinct scaling.event_Start_date) rq
					,min(tenure.dp_frequency) as dp_frequency
			from	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY	as scaling
					left join TEMP..RSMB2_CONTROL_CELL_LOOKUP		as lookup
					on	scaling.HH_COMPOSITION 			= lookup.HH_COMPOSITION
					and	scaling.TV_REGION 				= lookup.TV_REGION
					and	scaling.DTV_PACKAGE 			= lookup.DTV_PACKAGE
					and	scaling.BOX_TYPE 				= lookup.BOX_TYPE
					and	scaling.TENURE 					= lookup.TENURE
					and	scaling.SCALING_UNIVERSE_KEY	= lookup.SCALING_UNIVERSE_KEY
					left join temp..RSMB2_TENURE_LOOKUP			as tenure
					on	scaling.account_number = tenure.ACCOUNT_NUMBER
			where	scaling.event_Start_date between '2013-02-01 00:00:00' and '2013-02-28 00:00:00'
			group	by	scaling.account_number
						,lookup.CONTROL_CELL
		)	as thebase
where	dp_frequency is not null
and rq_february >= 0.9
group	by	control_cell
			,dp_frequency
			
-- Sample 2: all accounts

select	control_cell
		,dp_frequency
		,count(distinct account_number) as hh_volume
from	(		
			select	scaling.account_number -- UNIQUE AT ACCOUNT LEVEL...
					,lookup.CONTROL_CELL
					,round((count(distinct scaling.event_Start_date)/ 28.00),2)	as rq_february
					,count(distinct scaling.event_Start_date) rq
					,min(tenure.dp_frequency) as dp_frequency
			from	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY	as scaling
					left join TEMP..RSMB2_CONTROL_CELL_LOOKUP		as lookup
					on	scaling.HH_COMPOSITION 			= lookup.HH_COMPOSITION
					and	scaling.TV_REGION 				= lookup.TV_REGION
					and	scaling.DTV_PACKAGE 			= lookup.DTV_PACKAGE
					and	scaling.BOX_TYPE 				= lookup.BOX_TYPE
					and	scaling.TENURE 					= lookup.TENURE
					and	scaling.SCALING_UNIVERSE_KEY	= lookup.SCALING_UNIVERSE_KEY
					left join temp..RSMB2_TENURE_LOOKUP			as tenure
					on	scaling.account_number = tenure.ACCOUNT_NUMBER
			where	scaling.event_Start_date ='2013-02-28 00:00:00'
			group	by	scaling.account_number
						,lookup.CONTROL_CELL
		)	as thebase
where	dp_frequency is not null
group	by	control_cell
			,dp_frequency
			

-----------------------------
-- Monthly Response Rates [4]
-----------------------------

select	A.themonth
		,B.control_cell		
		,A.responsive		as response_rates
		,C.target			as sky_target
--into	RSMB2_MONTHLY_RESPONSE_RATES
from	z_3_beta3								as A
		inner join rsmb2_control_cell_lookup	as B
		on	A.HH_COMPOSITION	= B.HH_COMPOSITION
		and	A.TV_REGION			= B.TV_REGION
		and	A.DTV_PACKAGE		= B.DTV_PACKAGE
		and	A.BOX_TYPE			= B.BOX_TYPE
		and	A.TENURE			= B.TENURE
		and	A.UNIVERSE			= B.SCALING_UNIVERSE_KEY
		left join z_target_lookup				as C
		on	A.HH_COMPOSITION	= C.HH_COMPOSITION
		and	A.TV_REGION			= C.TV_REGION
		and	A.DTV_PACKAGE		= C.DTV_PACKAGE
		and	A.BOX_TYPE			= C.BOX_TYPE
		and	A.TENURE			= C.TENURE
		and	A.UNIVERSE			= C.UNIVERSE
		

		

------------------------
-- Panel Composition [5]
------------------------


-- THURSDAYS...
select	rsmb.thursday
		,lookup.control_cell
		,rsmb.sky_base
		,rsmb.vespa_panel
		,rsmb.responsive
into	RSMB2_PANEL_COMPOSITION_THURSDAYS
from 	z_5_thu									as rsmb
		inner join RSMB2_CONTROL_CELL_LOOKUP 	as lookup
		on	rsmb.HH_COMPOSITION	= lookup.HH_COMPOSITION
		and	rsmb.TV_REGION		= lookup.TV_REGION
		and	rsmb.DTV_PACKAGE	= lookup.DTV_PACKAGE
		and	rsmb.BOX_TYPE		= lookup.BOX_TYPE
		and	rsmb.TENURE			= lookup.TENURE
		and	rsmb.UNIVERSE		= lookup.SCALING_UNIVERSE_KEY

		
-- SATURDAY...
select	rsmb.saturday
		,lookup.control_cell
		,rsmb.sky_base
		,rsmb.vespa_panel
		,rsmb.responsive
into	RSMB2_PANEL_COMPOSITION_SATURDAYS
from 	z_5_satu								as rsmb
		inner join RSMB2_CONTROL_CELL_LOOKUP 	as lookup
		on	rsmb.HH_COMPOSITION	= lookup.HH_COMPOSITION
		and	rsmb.TV_REGION		= lookup.TV_REGION
		and	rsmb.DTV_PACKAGE	= lookup.DTV_PACKAGE
		and	rsmb.BOX_TYPE		= lookup.BOX_TYPE
		and	rsmb.TENURE			= lookup.TENURE
		and	rsmb.UNIVERSE		= lookup.SCALING_UNIVERSE_KEY


		
---------------------------
-- Reporting Continuity [6]
---------------------------

-- SAMPLE A...
select	control_cell
		--,sum(case when frequency is null then 1 else 0 end) as issue
		--,sum(case when frequency = 0 then 1 else 0 end) as freq_0
		,sum(case when frequency = 1 then 1 else 0 end) as freq_1
		,sum(case when frequency = 2 then 1 else 0 end) as freq_2
		,sum(case when frequency = 3 then 1 else 0 end) as freq_3
		,sum(case when frequency = 4 then 1 else 0 end) as freq_4
		,sum(case when frequency = 5 then 1 else 0 end) as freq_5
		,sum(case when frequency = 6 then 1 else 0 end) as freq_6
		,sum(case when frequency = 7 then 1 else 0 end) as freq_7
		,sum(case when frequency = 8 then 1 else 0 end) as freq_8
		,sum(case when frequency = 9 then 1 else 0 end) as freq_9
		,sum(case when frequency = 10 then 1 else 0 end) as freq_10
		,sum(case when frequency = 11 then 1 else 0 end) as freq_11
		,sum(case when frequency = 12 then 1 else 0 end) as freq_12
into	RSMB2_REPORTING_CONTINUITY_OVERALL
from	(
			-- Linking base with lookup to simplify view using control cells ids...
			select	lookup.CONTROL_CELL
					,base.account_number
					,count(distinct base.themonth)	as frequency
			from	z_6_rc_base									as base
					left join	temp..RSMB2_CONTROL_CELL_LOOKUP	as lookup
					on	base.hh_composition 		= lookup.hh_composition
					and	base.tv_region 				= lookup.tv_region
					and	base.dtv_package 			= lookup.dtv_package
					and	base.box_type 				= lookup.box_type
					and	base.tenure					= lookup.tenure
					and	base.scaling_universe_key	= lookup.scaling_universe_key
			group	by	1,2
		)	as rc
group	by	1
order	by	1

-- SAMPLE B...
-- SAMPLE B...
select	control_cell
		--,sum(case when frequency is null then 1 else 0 end) as issue
		--,sum(case when frequency = 0 then 1 else 0 end) as freq_0
		,sum(case when frequency = 1 then 1 else 0 end) as freq_1
		,sum(case when frequency = 2 then 1 else 0 end) as freq_2
		,sum(case when frequency = 3 then 1 else 0 end) as freq_3
		,sum(case when frequency = 4 then 1 else 0 end) as freq_4
		,sum(case when frequency = 5 then 1 else 0 end) as freq_5
		,sum(case when frequency = 6 then 1 else 0 end) as freq_6
		,sum(case when frequency = 7 then 1 else 0 end) as freq_7
		,sum(case when frequency = 8 then 1 else 0 end) as freq_8
		,sum(case when frequency = 9 then 1 else 0 end) as freq_9
		,sum(case when frequency = 10 then 1 else 0 end) as freq_10
		,sum(case when frequency = 11 then 1 else 0 end) as freq_11
		,sum(case when frequency = 12 then 1 else 0 end) as freq_12
into	RSMB2_REPORTING_CONTINUITY_ALLYEAR
from	(
			-- Linking base with lookup to simplify view using control cells ids...
			select	lookup.CONTROL_CELL
					,base.account_number
					,count(distinct base.themonth)	as frequency
			from	z_6_rc_base									as base
					inner join	(
									-- sampling for accounts overlaping in Jan and Dec...
									select	distinct base1.account_number -- 216,098
									from	(
												select	distinct A.account_number -- 358649
												from	dis_reference..FINAL_SCALING_HOUSEHOLD_HISTORY	as A
														inner join	z_6_rc_base as b
														on a.account_number = b.account_number
												where	event_start_date = '2013-01-01 00:00:00'
											)	as base1
											inner join	(
															select	distinct A.account_number -- 384509
															from	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_JUL15_DEC2013_OBS	as A
																	inner join	z_6_rc_base as b
																	on a.account_number = b.account_number
															where	event_start_date = '2013-12-31 00:00:00'
														)	as base2
											on	base1.account_number = base2.account_number
								)	as allyear_sample
					on	base.account_number = allyear_sample.account_number
					left join	temp..RSMB2_CONTROL_CELL_LOOKUP	as lookup
					on	base.hh_composition 		= lookup.hh_composition
					and	base.tv_region 				= lookup.tv_region
					and	base.dtv_package 			= lookup.dtv_package
					and	base.box_type 				= lookup.box_type
					and	base.tenure					= lookup.tenure
					and	base.scaling_universe_key	= lookup.scaling_universe_key
			group	by	1,2
		)	as rc
group	by	1
order	by	1


-- SAMPLE C...
select	control_cell
		--,sum(case when frequency is null then 1 else 0 end) as issue
		--,sum(case when frequency = 0 then 1 else 0 end) as freq_0
		,sum(case when frequency = 1 then 1 else 0 end) as freq_1
		,sum(case when frequency = 2 then 1 else 0 end) as freq_2
		,sum(case when frequency = 3 then 1 else 0 end) as freq_3
		,sum(case when frequency = 4 then 1 else 0 end) as freq_4
		,sum(case when frequency = 5 then 1 else 0 end) as freq_5
		,sum(case when frequency = 6 then 1 else 0 end) as freq_6
		,sum(case when frequency = 7 then 1 else 0 end) as freq_7
		,sum(case when frequency = 8 then 1 else 0 end) as freq_8
		,sum(case when frequency = 9 then 1 else 0 end) as freq_9
		,sum(case when frequency = 10 then 1 else 0 end) as freq_10
		,sum(case when frequency = 11 then 1 else 0 end) as freq_11
		,sum(case when frequency = 12 then 1 else 0 end) as freq_12
from	(
			-- Linking base with lookup to simplify view using control cells ids...
			select	lookup.CONTROL_CELL
					,base.account_number
					,count(distinct base.themonth)	as frequency
			from	z_6_rc_base									as base
					inner join	(
									-- sampling for accounts existing on Jan...
									select	distinct A.account_number -- 358649
									from	dis_reference..FINAL_SCALING_HOUSEHOLD_HISTORY	as A
											inner join	z_6_rc_base as b
											on a.account_number = b.account_number
									where	event_start_date = '2013-01-01 00:00:00'
								)	as trakcing_sample
					on	base.account_number = trakcing_sample.account_number
					left join	temp..RSMB2_CONTROL_CELL_LOOKUP	as lookup
					on	base.hh_composition 		= lookup.hh_composition
					and	base.tv_region 				= lookup.tv_region
					and	base.dtv_package 			= lookup.dtv_package
					and	base.box_type 				= lookup.box_type
					and	base.tenure					= lookup.tenure
					and	base.scaling_universe_key	= lookup.scaling_universe_key
			group	by	1,2
		)	as rc
group	by	1
order	by	1
