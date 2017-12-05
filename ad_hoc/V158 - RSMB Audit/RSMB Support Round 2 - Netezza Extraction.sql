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
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          RSMB
**Due Date:                             06/12/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:
	
	Extracting from Netezza the scaling segments to be in line with the weights used in Olive on VIQ table
	(These weights coming from CBI)
	
--------------------------------------------------------------------------------------------------------------
*/

/*
----------------------------------------------------------
-- SYNTAX TO EXPORT THE QUERY INTO A CSV FORMAT LOCALLY...
----------------------------------------------------------

CREATE EXTERNAL TABLE 'C:\\_Playpen_\\2012-10-24 VIQ CBI UAT\\SBE CBI UAT Input 1.csv'
USING
(
	DELIMITER ','
	Y2BASE 2000
	ENCODING 'internal'
	REMOTESOURCE 'ODBC'
	ESCAPECHAR '\'
)
AS

*/

-- NETEZZA SCALING SEGMENT LOOKUP (SCALING MAP)
select	*
		,ROW_NUMBER() OVER 	(
								ORDER BY	hh_composition
											,tv_region
											,dtv_package
											,box_type
											,tenure
											,scaling_universe_key
							)	 as rowid_
from	(
			select	distinct
					hh_composition
					,tv_region
					,dtv_package
					,box_type
					,tenure
					,scaling_universe_key
			from	DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
		)	as thebase
		
		
-- NETEZZA INTERVALS GENERATION (igonorp.sc_interval)
select	control_cell
		,account_number
		,interval_starts
		,interval_ends
from	(
			-- 3) ...
			select	control_cell
					,account_number
					,event_start_date	as interval_starts
					,case	when (thestart is not null and theend is not null)	then event_start_date
							else	(
										min(event_start_date)	over	(
																			partition by	control_cell
																							,account_number
																			order by		interval_starts
																			rows between	1 following and 1 following
																		)	
									)
					end		as interval_ends
					,thestart
			from	(
						-- 2) Removing days between intervals so we can then check the leading day per account in control cells (DEFINING INTERVALS)...
						select	control_cell
								,account_number
								,event_start_date
								,thestart
								,theend
						from	(
									-- 1) joining AC interaction activity with segment lookup + logic for spotting intervals...
									select	lookup.control_cell
											,scaling.ACCOUNT_NUMBER
											,min(scaling.event_Start_Date) over	(
																					partition by	lookup.control_Cell
																									,scaling.account_number
																					order by		scaling.event_Start_date
																					rows between	1 preceding and 1 preceding
																				)	as thelag
											,scaling.EVENT_START_DATE
											,min(scaling.event_Start_Date) over	(
																					partition by	lookup.control_Cell
																									,scaling.account_number
																					order by		scaling.event_Start_date
																					rows between	1 following and 1 following
																				)	as thelead
											,extract(epoch from scaling.EVENT_START_DATE-thelag) 	as thedifflag
											,extract(epoch from thelead-scaling.EVENT_START_DATE) as thedifflead
											,case when	(
															thelag is null
															or
															thedifflag > 1
														)	then 'S' else null end as thestart
											,case when	(
															thedifflead > 1
															or
															thelead is null
														)	then 'E' else null end as theend
									from 	DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY as scaling
											inner join	(
															-- 0) Generating the Scaling segment Lookup table with static segment ids (MAP)
															select	*
																	,ROW_NUMBER() OVER 	(
																							ORDER BY	hh_composition
																										,tv_region
																										,dtv_package
																										,box_type
																										,tenure
																										,scaling_universe_key
																						)	 as control_cell
															from	(
																		select	distinct
																				hh_composition
																				,tv_region
																				,dtv_package
																				,box_type
																				,tenure
																				,scaling_universe_key
																		from	DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
																	)	as thebase
														)	as lookup
											on	scaling.hh_composition			= lookup.hh_composition
											and scaling.tv_region				= lookup.tv_region
											and	scaling.dtv_package				= lookup.dtv_package
											and	scaling.box_type				= lookup.box_type
											and	scaling.tenure					= lookup.tenure
											and	scaling.scaling_universe_key	= lookup.scaling_universe_key
									where	scaling.event_start_date between '2013-07-15 00:00:00' and '2013-07-22 00:00:00'
									and		scaling.ACCOUNT_NUMBER in ('621460585881','630150844193')
									limit 	200
								)	as thebase0
						where	(thestart is not null or theend is not null)
					)	as thebase1
		)	as thebase2
where	thestart is not null


-- NETEZZA EXTRACT FOR THE ENABLEMENT DATE OF DP ACCOUNTS (CURRENTLY CATERING FOR FEBRUARY)
select	account_number
		,max(date_from) as enabled_dt
from	mds..BILLING_CUSTOMER_ACCOUNT_DIM
where	date_from < '2013-03-01 00:00:00'
and		CURRENCY_CODE = 'GBP'
and		VIEWING_PANEL_ID = 12
group	by	account_number


-- FROM PROD: SAMPLING ACCOUNTS INVOLVED IN SCALING BETWEEN AUGUST AND DECEMBER (z_3_beta3)
/* 
	This considers accounts participating actively in the panel at least 90% of the days, review iteratively each
	month
*/

select	base.themonth
		,base.hh_composition
		,base.tv_region
		,base.dtv_package
		,base.box_type
		,base.tenure
		,base.scaling_universe_key
		,count(distinct base.account_number)		 as	everyone
		,sum(case when freq >=0.9 then 1 else 0 end) as responsive
from	(
			select	distinct
					hh_composition
					,tv_region
					,dtv_package
					,box_type
					,tenure
					,scaling_universe_key
					,account_number
					,date_part('month',event_start_date)	as themonth
			from 	DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY
			where	event_start_date between '2013-09-01' and '2013-12-31'
		)	as base
		inner join	(
						-- Calculating the frequency at which accounts interacted with the panel
						select	account_number
								,date_part('month',event_start_date)	as themonth
								,count(distinct event_start_date) 	as tenure_
								,cast(tenure_ as float) / 30.0 as freq
						from 	DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY
						where	event_start_date between '2013-09-01' and '2013-12-31'
						group	by	account_number
									,themonth
					)	as verif_list
		on	base.account_number	= verif_list.account_number
		and	base.themonth		= verif_list.themonth
group	by	base.themonth
			,base.hh_composition
			,base.tv_region
			,base.dtv_package
			,base.box_type
			,base.tenure
			,base.scaling_universe_key

			
/*
	DUE TO AN ISSUE WITH SEPTEMBER WE HAD TO USE THE SAME APPROACH BUT WITH AN SPECIFIC TABLE ON THE RSMB SERVER
	(z_3_beta_sept)
	
	
	select	base.themonth
			,base.hh_composition
			,base.tv_region
			,base.dtv_package
			,base.box_type
			,base.tenure
			,base.scaling_universe_key
			,count(distinct base.account_number)		 as	everyone
			,sum(case when freq >=0.9 then 1 else 0 end) as responsive
	INTO	z_3_beta_sept
	from	(
				select	distinct
						hh_composition
						,tv_region
						,dtv_package
						,box_type
						,tenure
						,scaling_universe_key
						,account_number
						,date_part('month',event_start_date)	as themonth
				from 	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_SEP2013_OBS
				--where	event_start_date between '2013-09-01' and '2013-12-31'
			)	as base
			inner join	(
							-- Calculating the frequency at which accounts interacted with the panel
							select	account_number
									,date_part('month',event_start_date)	as themonth
									,count(distinct event_start_date) 	as tenure_
									,cast(tenure_ as float) / 30.0 as freq
							from 	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_SEP2013_OBS
							--where	event_start_date between '2013-09-01' and '2013-12-31'
							group	by	account_number
										,themonth
						)	as verif_list
			on	base.account_number	= verif_list.account_number
			and	base.themonth		= verif_list.themonth
	group	by	base.themonth
				,base.hh_composition
				,base.tv_region
				,base.dtv_package
				,base.box_type
				,base.tenure
				,base.scaling_universe_key
*/

			
-- FROM PROD: EXTRACTING THE AVERAGE WEIGHT ASSIGNED TO EACH SCALING SEGMENT ON EACH MONTH TO DERIVE TARGETS (z_3_targets2)

select	date_part('month',event_start_date)	as themonth
		,HH_COMPOSITION
		,TV_REGION
		,DTV_PACKAGE
		,BOX_TYPE
		,TENURE
		,SCALING_UNIVERSE_KEY
		,avg (weight_scaled_value)	as weight
from 	DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY
where	event_start_date between '2013-09-01' and '2013-12-31'
group	by	1,2,3,4,5,6,7


-- FROM PROD: EXTRACTING THE SKY BASE TARGETS FOR EACH SCALING SEGMENT FOR MONTHLY RESPONSE RATE REPORT

select	hh_composition
		,tv_region
		,dtv_package
		,box_type
		,tenure
		,universe
		,count(1)							as panelists
		,round((panelists * min(theweight)),0) 	as thetarget
from	(
			--	doing this to get rid of visibility over viewing events, compacting everything up to account level...
			select	account_number	
					,hh_composition
					,tv_region
					,dtv_package
					,box_type
					,tenure
					,scaling_universe_key			as universe
					,min(weight_scaled_value)		as theweight
			from	DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY
			where	event_start_date = '2013-10-31' --  this date used as is the median date of the sample...
			group	by	1,2,3,4,5,6,7
		) 	as thebase
group	by	hh_composition
			,tv_region
			,dtv_package
			,box_type
			,tenure
			,universe
			
-- FROM PROD / OLD PROD / VESPA PROD BOX: assembling a full year of scaling for Reporting Continuity (z_6_rc_base)

/*
	All extracts where aggregated monthly...
	This is sampling for all accounts which were scaled at least 90% of days of a month...
	Iteratively done for all months of 2013...
	
	Using below tables on the RSMB server:
	
	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY
	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_APR2013_OBS
	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_MAY2013_OBS
	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_JUN2013_OBS
	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_JUL2013_OBS
	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY_JUL15_DEC2013_OBS (where event_start_date >= '2013-08-01')

*/

select	base.*
into	z_6_rc_base
from	(
			-- simplifying to accounts in control cells per month...
			select	distinct
					hh_composition
					,tv_region
					,dtv_package
					,box_type
					,tenure
					,scaling_universe_key
					,account_number
					,date_part('month',event_start_date)	as themonth
			from 	DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY
		)	as base
		inner join	(
						-- Calculating the frequency at which accounts interacted with the panel per month...
						select	account_number
								,date_part('month',event_start_date)	as themonth
								,count(distinct event_start_date) 	as tenure_
								,case	when themonth = 2 then	28.0
										else 30.0
								end		as thefactor
								,cast(tenure_ as float) / thefactor as freq
						from 	DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY
						group	by	account_number
									,themonth
					)	as verif_list
		on	base.account_number	= verif_list.account_number
		and	base.themonth		= verif_list.themonth
where	verif_list.freq >=0.9;
commit;