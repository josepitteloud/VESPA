/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ 
             ?$$$,      I$$$ $$$$. $$$$  $$$= 
              $$$$$$$$= I$$$$$$$    $$$$.$$$  
                  :$$$$~I$$$ $$$$    $$$$$$   
               ,.   $$$+I$$$  $$$$    $$$$=   
              $$$$$$$$$ I$$$   $$$$   .$$$    
                                      $$$     
                                     $$$      
                                    $$$?

            CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							PRODUCTS ANALYTICS 	(PA)
**Done by:                             	Angel Donnarumma	(angel.donnarumma@sky.uk)
**Stakeholder:                          Daniel Chronnell

**Business Brief:

        Studying overall Q's UI performance
		
**Considerations:

		-> This has to be done for Gateways only
		-> Time frame is now March 2017 (used to be Oct-Nov 2016)
		
**Sections:

		A - Data Evaluation
			1 - Checking on volume of data over March
			
		B - Data Preparation
			2 - Generate Quartiles
				
		C - Data Analysis (Queries)
			3 - Analysing Home Page Performance for Quartiles
			4 - Distribution of Converted Journeys
			
**Running Time:

???

--------------------------------------------------------------------------------------------------------------

*/

--------------------------------------------
-- 1 - Checking on volume of data over March
--------------------------------------------

select	date_
		,count(1) as njourneys
from	ref_home_Start_
where	date_ between '2017-03-01' and '2017-03-31'
group	by	date_

-- 12, 13, 25 of march 2017 are not in good shape for analysis... will have to exclude them from the month


-------------------------
-- 2 - Generate Quartiles
-------------------------
/*
	Create 4 new quartiles based on their propensity to download OD content (based on share of all converted 
	actions that are Downloads).  
	Potentially Deciles given likely similarities
*/

-- drop table z_qtiles;commit;

with	base as	(
					select	base.dk_serial_number
							--,count(distinct base.date_)	as ndays
							,count(distinct	(
												case	when base.dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then base.date_
														else null
												end
											))	as nconv_days -- [X]
							,count(distinct base.date_||'-'||base.dk_Serial_number||'-'||base.gn_lvl2_session_grain)	as njourneys-- [X]
							,count(distinct	(
												case	when base.dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_Session_grain
														else null
												end
											))	as nconv_journeys-- [X]
							,sum(case when base.dk_action_id = 02400 then 1 else 0 end)	as ndownloads-- [X]
					from	z_pa_events_fact_			as base
							inner join	ref_home_start_	as home_ref
							on	base.date_				= home_ref.date_
							and	base.dk_Serial_number	= home_ref.dk_serial_number
							and	base.gn_lvl2_session_grain	= home_ref.target
					where	base.date_ between '2017-03-01' and '2017-03-31' 			-- only 17 days in march at the moment...
					and		base.date_ not in ( '2017-03-12','2017-03-13','2017-03-25') -- Days with data volume issues
					and		base.gn_lvl2_session in	(
														'Catch Up'
														,'My Q'
														,'Top Picks'
														,'Sky Box Sets'
														,'Sky Movies'
														,'Sky Store'
														,'Sports'
														,'Kids'
														,'Music'
													)
					and		base.stb_type in ('Silver','Q')
					group	by	base.dk_serial_number
				)
		,ref_act as	(
						select	dk_serial_number
								,count(distinct date_)	as ndays
						from	z_pa_events_fact_
						where	date_ between '2017-03-01' and '2017-03-31' 			-- only 17 days in march at the moment...
						and		date_ not in ( '2017-03-12','2017-03-13','2017-03-25')	-- Days with data volume issues
						and		stb_type in ('Silver','Q')
						group	by	dk_serial_number
					)
select	base.dk_serial_number
		,base.ndownloads
		,cast(base.nconv_days as float) / cast(ref_act.ndays as float)		as p_ndays_conv
		,cast(base.nconv_journeys as float) / cast(base.njourneys as float)	as conv_rate
		,ntile(4) over	(
							order by	base.ndownloads	desc -- downloading N titles
										,p_ndays_conv	desc -- visiting On Demand areas quite frequently 
										--,conv_rate		desc -- The ones with great propensity to convert
						)	as qtiles
		,ref_act.ndays														as ndays_Active
		,case	when base.nconv_journeys > 0 then cast(base.ndownloads as float) / cast(base.nconv_journeys as float)	
				else null
		end		as downloading_ratio
into	z_qtiles
from	base
		inner join ref_act
		on	base.dk_serial_number	= ref_act.dk_serial_number;

commit;


----------------------------------------------------
-- 3 - Analysing Home Page Performance for Quartiles
----------------------------------------------------

with	ref_ses_conv as	(
							-- Measuring for converted/exploring sessions
							-- this is used to measure Time_To... used by our current capping criteria
							select	date_
									,dk_serial_number
									,session_grain
									,min(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end)	as x
							from	z_pa_events_fact_
							where	session = 'Home'
							and		dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							and		date_ between '2017-03-01' and '2017-03-31' 			-- only 17 days in march at the moment...
							and		date_ not in ( '2017-03-12','2017-03-13','2017-03-25')	-- Days with data volume issues
							and		stb_type in ('Silver','Q')
							group	by	date_
										,dk_serial_number
										,session_grain
						)
		,ref_ses as		(
							-- Identifying Home Sessions we are considering valid for analysis
							select	date_
									,dk_serial_number
									,session_grain
									,case when time_to_conv is not null then 1 else 0 end	as conv_flag
									,coalesce(time_to_conv,time_spent)						as time_to_
							from	(
										select	base.date_
												,base.dk_serial_number
												,base.session_grain
												,sum(base.ss_elapsed_next_action)																						as time_spent
												,sum(case when ref.session_grain is not null and base.index_ < ref.x then base.ss_elapsed_next_action else null end)	as time_to_conv
										from	z_pa_events_fact_		as base
												inner join z_qtiles		as ntiles
												on	base.dk_Serial_number	= ntiles.dk_Serial_number
												left join ref_ses_conv	as ref
												on	base.date_				= ref.date_
												and	base.dk_serial_number	= ref.dk_serial_number
												and	base.session_grain		= ref.session_grain
										where	base.date_ between '2017-03-01' and '2017-03-31' 			-- only 17 days in march at the moment...
										and		base.date_ not in ( '2017-03-12','2017-03-13','2017-03-25') -- Days with data volume issues
										and		base.session = 'Home'
										and		base.stb_type in ('Silver','Q')
										group	by	base.date_
													,base.dk_serial_number
													,base.session_grain
									)	as final_ 
							where	(	
										(conv_flag = 1 and time_to_conv between 0 and 600) -- 95% of all converted journeys
										or
										(conv_flag = 0 and time_spent between 0 and 1000) -- 90% of all exploratory journeys
									)
						)
		,base as		(
							-- extracting the base data for:
							-- All valid Home Sessions
							-- and from those, isolating journeys that started from the Home Page only
							select	a.*
									,ntiles.qtiles
							from	z_pa_events_fact_	as a
									inner join ref_ses			as b
									on	a.date_					= b.date_
									and	a.dk_serial_number		= b.dk_serial_number
									and	a.session_grain			= b.session_grain
									inner join ref_home_start_	as ref_home
									on	a.date_					= ref_home.date_
									and	a.dk_Serial_number		= ref_home.dk_serial_number
									and a.gn_lvl2_session_grain	= ref_home.target
									inner join z_qtiles			as ntiles
									on	a.dk_serial_number	= ntiles.dk_Serial_number
							where	a.date_ between '2017-03-01' and '2017-03-31' 				-- only 17 days in march at the moment...
							and		a.date_ not in ( '2017-03-12','2017-03-13','2017-03-25')	-- Days with data volume issues
							and		a.stb_type in ('Silver','Q')
						)
		,ref_conv as	(
							-- Flagging exact point of conversions for journeys at SLM lvl that did so
							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	base
							where	dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
		,base_size as	(
							-- counting the total number of STBs we are looking at in this exercise
							select	qtiles
									,count(distinct dk_serial_number) as nactive_boxes
							from	base
							group	by	qtiles
						)
select	base.qtiles
		,base.gn_lvl2_session
		,max(base_size.nactive_boxes)																											as active_boxes
		,count(distinct base.dk_serial_number)																									as reach
		,count(distinct ref_conv.dk_serial_number)																								as conv_reach
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain) 												as njourneys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl2_session_grain)									as nconv_journeys
		,sum(base.ss_elapsed_next_action)																										as time_spent
		,sum(case when ref_conv.gn_lvl2_session_grain is not null and base.index_ < ref_conv.x then base.ss_elapsed_next_action else null end)	as time_to_conv
		--,sum(case when base.dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)										as nconv_actions
		,sum(case when base.dk_action_id = 02400 then 1 else 0 end)																				as ndownloads
		,sum(case when base.dk_action_id = 03000 then 1 else 0 end)																				as nplaybacks
		,sum(case when base.dk_action_id in (02000,02010,02002,02005) then 1 else 0 end)														as nbookings
from	base
		left join ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		inner join base_size
		on base.qtiles	= base_size.qtiles
group	by	base.qtiles
			,base.gn_lvl2_session
			
			
-----------------------------------------
-- 4 - Distribution of Converted Journeys
-----------------------------------------

with	ref_ses_conv as	(
							-- Measuring for converted/exploring sessions
							-- this is used to measure Time_To... used by our current capping criteria
							select	date_
									,dk_serial_number
									,session_grain
									,min(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end)	as x
							from	z_pa_events_fact_
							where	session = 'Home'
							and		dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							and		date_ between '2017-03-01' and '2017-03-31' 			-- only 17 days in march at the moment...
							and		date_ not in ( '2017-03-12','2017-03-13','2017-03-25')	-- Days with data volume issues
							and		stb_type in ('Silver','Q')
							group	by	date_
										,dk_serial_number
										,session_grain
						)
		,ref_ses as		(
							-- Identifying Home Sessions we are considering valid for analysis
							select	date_
									,dk_serial_number
									,session_grain
									,case when time_to_conv is not null then 1 else 0 end	as conv_flag
									,coalesce(time_to_conv,time_spent)						as time_to_
							from	(
										select	base.date_
												,base.dk_serial_number
												,base.session_grain
												,sum(base.ss_elapsed_next_action)																						as time_spent
												,sum(case when ref.session_grain is not null and base.index_ < ref.x then base.ss_elapsed_next_action else null end)	as time_to_conv
										from	z_pa_events_fact_		as base
												inner join z_qtiles		as ntiles
												on	base.dk_Serial_number	= ntiles.dk_Serial_number
												left join ref_ses_conv	as ref
												on	base.date_				= ref.date_
												and	base.dk_serial_number	= ref.dk_serial_number
												and	base.session_grain		= ref.session_grain
										where	base.date_ between '2017-03-01' and '2017-03-31' 			-- only 17 days in march at the moment...
										and		base.date_ not in ( '2017-03-12','2017-03-13','2017-03-25') -- Days with data volume issues
										and		base.session = 'Home'
										and		base.stb_type in ('Silver','Q')
										group	by	base.date_
													,base.dk_serial_number
													,base.session_grain
									)	as final_ 
							where	(	
										(conv_flag = 1 and time_to_conv between 0 and 600) -- 95% of all converted journeys
										or
										(conv_flag = 0 and time_spent between 0 and 1000) -- 90% of all exploratory journeys
									)
						)
		,base as		(
							-- extracting the base data for:
							-- All valid Home Sessions
							-- and from those, isolating journeys that started from the Home Page only
							select	a.*
									,ntiles.qtiles
							from	z_pa_events_fact_	as a
									inner join ref_ses	as b
									on	a.date_					= b.date_
									and	a.dk_serial_number		= b.dk_serial_number
									and	a.session_grain			= b.session_grain
									inner join ref_home_start_	as ref_home
									on	a.date_					= ref_home.date_
									and	a.dk_Serial_number		= ref_home.dk_serial_number
									and a.gn_lvl2_session_grain	= ref_home.target
									inner join z_qtiles			as ntiles
									on	a.dk_serial_number	= ntiles.dk_Serial_number
							where	a.date_ between '2017-03-01' and '2017-03-31' 				-- only 17 days in march at the moment...
							and		a.date_ not in ( '2017-03-12','2017-03-13','2017-03-25')	-- Days with data volume issues
							and		a.stb_type in ('Silver','Q')
						)
		,ref_conv as	(
							-- Flagging exact point of conversions for journeys at SLM lvl that did so
							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	base
							where	dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
select	qtiles
		,gn_lvl2_session
		,time_to_conv
		,count(1)	as njourneys
		,sum(njourneys)	over (partition by qtiles)	as prop
from	(
			select	base.qtiles
					,base.date_
					,base.dk_serial_number
					,base.gn_lvl2_session
					,base.gn_lvl2_session_grain
					,sum(case when ref_conv.gn_lvl2_session_grain is not null and base.index_ < ref_conv.x then base.ss_elapsed_next_action else null end)	as time_to_conv
					,cast((time_to_conv/10)-.5 as numeric(15,0))*10 																						as time_bucket
			from	base
					inner join ref_conv
					on	base.date_					= ref_conv.date_
					and	base.dk_serial_number		= ref_conv.dk_serial_number
					and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
			group	by	base.qtiles
						,base.date_
						,base.dk_serial_number
						,base.gn_lvl2_session
						,base.gn_lvl2_session_grain
		)	as final_
group	by	qtiles
			,gn_lvl2_session
			,time_to_conv