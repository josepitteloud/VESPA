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
**Project Name:							PRODUCTS ANALYTICS (PA)
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Stakeholder:                          Product Team
**Due Date:                             05/02/2016
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        Pool of scripts written to verify PA's logics functioning, meant to work as part of a QA suit
		to check the health of PRODUCTS ANALYTICS

**Sections:

		A - Verifying Sessions configuration
			A0 - Foundations
				A01 - Global Sessions
			A1 - Home Session Level 1
			A2 - Home Session Level 2
				A21 - All Actions associated with a lvl2 session
				A22 - All sessions are the expected type of sessions
			A3 - Home Session Level 3
			A4 - Home Sessions delivered through HDFS [THIS IS WHAT MATTERS]
			

**Notes:

	These scripts are (so far up to 20160219) instantiated in Tableau on individual datasets.
	Tableau reference link: (Comming soon...)
	
--------------------------------------------------------------------------------------------------------------

*/

-------------------
-- A0 - Foundations
-------------------

/*-------------------------------------------------------
	A01 - Global Sessions

	ACCEPTANCE CRITERIA: 
		-- the success rate should not be more than a 0%
*/-------------------------------------------------------

select	dk_date
		,count(1) as nrows
		,sum(Case when global_session_id = '' then 1 else 0 end) as with_null_sessions
		,cast(with_null_sessions as float) / cast(nrows as float)	as success_rate
from	pa_events_Fact
group	by	dk_Date
order	by	dk_Date desc

--> In case of bugs: DIVE DEEP:

-- what hours are affected?
select	dk_datehour
		,count(1) as nrows
		,sum(Case when global_session_id = '' then 1 else 0 end) as with_null_sessions
		,cast(with_null_sessions as float) / cast(nrows as float)	as success_rate
from	pa_events_Fact
group	by	dk_Datehour
order	by	dk_Datehour desc

-- Global Session assignment ratio
select	stb_type
		,gs_assigned
		,count(1) as nstbs
		,round((cast(nstbs as float)/ cast((sum(nstbs) over(partition by 1)) as float)),3) as theprop
		,round((cast(nstbs as float)/ cast((sum(nstbs) over(partition by stb_type)) as float)),3) as theprop_x_type
from	(
			select	dk_Serial_number
					,sum(success_rate)	as abs_success
					,count(distinct dk_datehour)	as ndays
					,round((cast(abs_success as float)/cast(ndays as float)),2) as gs_assigned
					,case	when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Silver'
							when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Box'
							when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
					end		as stb_type
			from	(
						select	dk_datehour
								,dk_Serial_number
								,count(1) as nrows
								,sum(Case when global_session_id = '' then 1 else 0 end) as with_null_sessions
								,cast(with_null_sessions as float) / cast(nrows as float)	as success_rate
						from	pa_events_Fact
						--where	dk_datehour >= 2016040515
						group	by	dk_Datehour
									,dk_Serial_number
						order	by	dk_Datehour desc
					)	as base
			group	by	 1
		)	as base2
--where	gs_assigned = 1
group	by	stb_type
			,gs_assigned


-- how much would we dropping by just pikcing records from STBs that are assigned with
-- global session in 100%

select	1,dk_Datehour
		,count(1) as hits
from	pa_Events_fact
--where	dk_Datehour >= 2016040515
group	by	dk_Datehour
union
select	2,dk_Datehour
		,count(1) as hits
from	pa_events_Fact
--where	dk_Datehour>= 2016040515
and		dk_serial_number in	(
								select	distinct
										dk_serial_number
								from	(
											select	dk_Serial_number
													,sum(success_rate)	as abs_success
													,count(distinct dk_datehour)	as ndays
													,round((cast(abs_success as float)/cast(ndays as float)),2) as reporting_quality
													,case	when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Silver'
															when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Box'
															when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
													end		as stb_type
											from	(
														select	dk_datehour
																,dk_Serial_number
																,count(1) as nrows
																,sum(Case when global_session_id = '' then 1 else 0 end) as with_null_sessions
																,cast(with_null_sessions as float) / cast(nrows as float)	as success_rate
														from	pa_events_Fact
														where	dk_datehour >= 2016040515
														group	by	dk_Datehour
																	,dk_Serial_number
														order	by	dk_Datehour desc
													)	as base
											group	by	 1
										)	as base2
								where	reporting_quality = 1
							)
group	by	dk_Datehour

----------------------------
-- A1 - Home Session Level 1
----------------------------
/*
	checking at the Home session rules
	
	a - there should be no home session assigned for actions id 01001 and 01002
	b - all global nav actions (01400) with destination either 'Fullscreen' or 'Stand By In' should have a home session id assigned
	c - there should be no home session assigned for actions with a ref value of either 'Fullscreen','zapper' or 'miniguide'
	
*/

select	dk_date
		,count(1)	as x
		,count(distinct home_session_id)	as y
		,count(distinct (case when dk_action_id in (01001,01002) and home_session_id <>'' then home_session_id else null end)) as a
		,(cast(a as float)/cast(y as float))	as bug_ratio_a
		,sum(case when dk_action_id = 01400 and (lower(dk_current) like '%fullscreen%' or lower(dk_current)like '%stand%by%') and home_session_id ='' then 1 else 0 end) as b
		,cast(b as float)/ sum(case when dk_action_id = 01400 then 1 else 0 end) as bug_ratio_b
		,count(distinct (case when (lower(dk_referrer_id) like '%fullscreen%' or lower(dk_referrer_id) like '%zapper%' or lower(dk_referrer_id) like '%miniguide%') then home_session_id else null end)) as c
		,(cast(c as float)/cast(y as float))	as bug_ratio_c
from	pa_events_fact
where	dk_date >= 20160216
group	by	dk_date


----------------------------
-- A2 - Home session Level 2
----------------------------

/*--------------------------------------------------
	A21 - All Actions associated with a lvl2 session

	ACCEPTANCE CRITERIA: 
		-- the success rate should be at least 98%
*/--------------------------------------------------

select	dk_date
		,count(1) as nrows
		,count(dk_Action_id) as nrows_with_actions
		,sum(case when home_session_lvl2_id<>'' then 1 else 0 end)	as nrows_in_lvl2
		,round((cast(nrows_in_lvl2 as float)/cast(nrows_with_actions as float)),2) as success_rate
from	pa_events_fact
group	by	dk_Date
order	by	dk_Date desc

/*------------------------------------------------------
	A22 - All sessions are the expected type of sessions
	
		ACCEPTANCE CRITERIA:
		
			-- success_Rating = 100%
*/------------------------------------------------------

select	count(1) as actions_starts
		,round(
					cast(actions_starts as float)/
					sum(
							case	when home_session_lvl2_id = benchmark then 1
									when home_session_lvl2_id = 'Stand By' and benchmark is null then 1
									else 0
							end
						)	
					,2
				)	as success_Rating
from	(
			select	base.*	
					,coalesce(screen.screen_parent,screen.session_type) as benchmark
			from	(
						select	global_session_id||'-'||home_session_lvl2_id||'-'||home_session_lvl2_grain as target
								,dk_action_id
								,dense_rank()over(partition by target order by timems) as therank
								,case when dk_previous like '0%' and therank = 1 then dk_referrer_id else dk_previous end as thelinkage
								,home_session_lvl2_id
						from	pa_events_fact
						where	home_session_lvl2_id <> '' 
						and 	dk_serial_number like '32C0010480008190A'
						and		dk_date = 20160122
					)	as base
					left join pa_screen_dim as screen
					on	base.thelinkage	= screen.pk_screen_id
			where	base.therank = 1
		)	as thetest

/*	VISUALS
	Aim: Actions taking place in unexpected areas in the UI (Fails at this stage are very likely to be due sessions logic)
*/

with	base as		(
						select	index_
								,dk_asset_id
								,(TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp	as dt
								,extract(epoch from	dt -	(
																min(dt) over	(
																					partition by	dk_serial_number
																					order by		index_
																					rows between	1 preceding and 1 preceding
																				)
															))
								as ss_elapsed_next_action
								,dk_serial_number
								,dk_action_id
								,acts.action_name
								,dk_previous
								,dk_current
								,dk_referrer_id
								,dk_trigger_id
								,last_value(stage1 ignore nulls) over	(
																			partition by	dk_serial_number
																			order by 		index_
																			rows between	200 preceding and current row
																		)							as gn_lvl3_session_grain
								,substr(gn_lvl3_session_grain,1,instr(gn_lvl3_session_grain,'-')-1)	as gn_lvl3_session
								,last_value(stage2 ignore nulls) over	(
																			partition by	dk_serial_number
																			order by 		index_
																			rows between	200 preceding and current row
																		)							as gn_lvl2_session_grain
								,substr(gn_lvl2_session_grain,1,instr(gn_lvl2_session_grain,'-')-1) as gn_lvl2_session
						from	z_gslvl23_step_withtime				as obs
								left join pa_action_dim	as acts
								on	obs.dk_action_id	= acts.pk_action_id
								inner join pa_time_dim as thetime
								on	obs.dk_time			= thetime.pk_time_dim
								inner join pa_date_dim as thedate
								on	obs.dk_Date			= thedate.date_pk
						order	by	index_
					)
		,totstb as	(
						select	date(dt)	as date_
								,count(distinct dk_serial_number) as total_stb 
						from 	base
						group	by	date_
					)
		,totact as	(
						select	dk_Action_id 
								,count(distinct dk_serial_number) as nstb_per_actions
						from 	base
						group	by	dk_Action_id
					)
select	step1.dk_action_id
		,step1.action_name
		,step1.gn_lvl2_session
		,step1.date_
		,count(distinct step1.dk_serial_number||'-'||step1.gn_lvl2_session_grain) 			as nsessions
		,((nsessions / sum(nsessions)over(partition by step1.dk_action_id,step1.date_)))	as prop_sessions
		,count(1) 																			as act_freq
		,act_freq / sum(act_freq)over(partition by step1.gn_lvl2_session,step1.date_)		as prop_act_freq
		,avg(step1.ss_elapsed_next_action)													as avg_ss_4_next_Action
		,stddev(step1.ss_elapsed_next_action)												as stddev_ss_4_next_Action
		,min(step1.ss_elapsed_next_action)													as min_ss_4_next_Action
		,max(step1.ss_elapsed_next_action)													as max_ss_4_next_Action
		,count(distinct step1.dk_serial_number) 											as nboxes
		,cast (nboxes as float) / cast(max(totstb.total_stb) as float)						as prop_nboxes
		,totact.nstb_per_actions
		,max(totstb.total_stb)																as total_stbs
from	(
			select	dk_action_id
					,action_name
					,date(dt)				as date_
					,dk_serial_number
					,gn_lvl2_session_grain
					,gn_lvl2_session
					,gn_lvl3_session_grain
					,gn_lvl3_session
					,ss_elapsed_next_action
			from	base
		)	as step1
		inner join	totact
		on	step1.dk_Action_id	= totact.dk_Action_id
		inner join	totstb
		on step1.date_			= totstb.date_
group	by	step1.dk_action_id
			,step1.action_name
			,step1.gn_lvl2_session
			,step1.date_
			,totact.nstb_per_actions
			

----------------------------
-- A3 - Home Session Level 3
----------------------------

with	base as		(
						select	index_
								,dk_asset_id
								,(TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp	as dt
								,extract(epoch from	dt -	(
																min(dt) over	(
																					partition by	dk_serial_number
																					order by		index_
																					rows between	1 preceding and 1 preceding
																				)
															))
								as ss_elapsed_next_action
								,dk_serial_number
								,dk_action_id
								,acts.action_name
								,dk_previous
								,dk_current
								,dk_referrer_id
								,dk_trigger_id
								,last_value(stage1 ignore nulls) over	(
																			partition by	dk_serial_number
																			order by 		index_
																			rows between	200 preceding and current row
																		)							as gn_lvl3_session_grain
								,substr(gn_lvl3_session_grain,1,instr(gn_lvl3_session_grain,'-')-1)	as gn_lvl3_session
								,last_value(stage2 ignore nulls) over	(
																			partition by	dk_serial_number
																			order by 		index_
																			rows between	200 preceding and current row
																		)							as gn_lvl2_session_grain
								,substr(gn_lvl2_session_grain,1,instr(gn_lvl2_session_grain,'-')-1) as gn_lvl2_session
						from	z_gslvl23_step_withtime				as obs
								left join pa_action_dim	as acts
								on	obs.dk_action_id	= acts.pk_action_id
								inner join pa_time_dim as thetime
								on	obs.dk_time			= thetime.pk_time_dim
								inner join pa_date_dim as thedate
								on	obs.dk_Date			= thedate.date_pk
						order	by	index_
					)
		,totstb as	(
						select	date(dt)	as date_
								,count(distinct dk_serial_number) as total_stb 
						from 	base
						group	by	date_
					)
		,totact as	(
						select	dk_Action_id 
								,count(distinct dk_serial_number) as nstb_per_actions
						from 	base
						group	by	dk_Action_id
					)
select	step1.dk_action_id
		,step1.action_name
		,step1.gn_lvl3_session
		,step1.date_
		,count(distinct step1.dk_serial_number||'-'||step1.gn_lvl3_session_grain) 			as nsessions
		,((nsessions / sum(nsessions)over(partition by step1.dk_action_id,step1.date_)))	as prop_sessions
		,count(1) 																			as act_freq
		,act_freq / sum(act_freq)over(partition by step1.gn_lvl3_session,step1.date_)		as prop_act_freq
		,avg(step1.ss_elapsed_next_action)													as avg_ss_4_next_Action
		,stddev(step1.ss_elapsed_next_action)												as stddev_ss_4_next_Action
		,min(step1.ss_elapsed_next_action)													as min_ss_4_next_Action
		,max(step1.ss_elapsed_next_action)													as max_ss_4_next_Action
		,count(distinct step1.dk_serial_number) 											as nboxes
		,cast (nboxes as float) / cast(max(totstb.total_stb) as float)						as prop_nboxes
		,totact.nstb_per_actions
		,max(totstb.total_stb)																as total_stbs
from	(
			select	dk_action_id
					,action_name
					,date(dt)				as date_
					,dk_serial_number
					,gn_lvl2_session_grain
					,gn_lvl2_session
					,gn_lvl3_session_grain
					,gn_lvl3_session
					,ss_elapsed_next_action
			from	base
		)	as step1
		inner join	totact
		on	step1.dk_Action_id	= totact.dk_Action_id
		inner join	totstb
		on step1.date_			= totstb.date_
group	by	step1.dk_action_id
			,step1.action_name
			,step1.gn_lvl3_session
			,step1.date_
			,totact.nstb_per_actions
			
			
-- WRITE ABOUT THIS THING...
			
select	1,date_
		,gn_lvl2_session
		,sum(nboxes)
		,sum(nsessions)
from	z_pa_cube_hslvl2_sessions
where	date_ between '2016-03-06' and '2016-03-08'
and		gn_lvl2_session in ('Top Picks','Home','Recordings')
group	by	date_
			,gn_lvl2_session
union
select	2,date(dt) as thedate
		,gn_lvl2_session
		,count(distinct dk_serial_number) as nboxes
		,count(distinct dk_serial_number||'-'||gn_lvl2_session_grain) as nsessions
from	z_checking
where	date(dt) between '2016-03-06' and '2016-03-08'
and		gn_lvl2_session in ('Top Picks','Home','Recordings')
group	by	thedate
			,gn_lvl2_session
			
			
-- WRITE ABOUT THIS THING...

select	1,date_
		,dk_action_id
		,gn_lvl2_session
		,sum(nsessions)	as tot_sessions
from	z_pa_cube_hslvl2_actions
where	date_ between '2016-02-12' and '2016-02-13'
and		dk_action_id in (02400,03000)
and		gn_lvl2_session in ('Top Picks','My Q','Home')
group	by	date_
			,dk_action_id
			,gn_lvl2_session
union
select	2,date(dt) as date_
		,dk_action_id
		,gn_lvl2_session
		,count(distinct date(dt)||'-'||dk_serial_number||'-'||gn_lvl2_session_grain) as tot_sessions
from 	z_checking 
where	date(dt) between '2016-02-12' and '2016-02-13'
and		gn_lvl2_session in ('Top Picks','My Q','Home')
and		dk_action_id in (02400,03000)
group	by	date_
			,dk_action_id
			,gn_lvl2_session
			
			
	


-- STBs View, how many sessions are drafted for each STBs in the past days
select	the_avg_prop_gs
		,count(1) as freq
from	(
			select	dk_serial_number
					,round(avg(prop_gs),0) as the_avg_prop_gs
			from	(
						select	dk_date
								,dk_serial_number
								,count(1) as nrows
								,sum(case when length(global_session_id)>1 then 1 else 0 end) 		as n_gs
								,round(cast(n_gs as float)/ cast(nrows as float),4) *100 			as prop_gs
								,sum(Case when home_session_id <> '' then 1 else 0 end)				as n_hs
								,round(cast(n_hs as float)/cast(nrows as float),4) *100				as prop_hs
								,sum(case when length(HOME_SESSION_LVL2_ID)>0 then 1 else 0 end) 	as n_hs2
								,round(cast(n_hs2 as float)/cast(nrows as float),4) *100  			as prop_hs2
								,sum(case when length(HOME_SESSION_LVL3_ID)>0 then 1 else 0 end)	as n_hs3
								,round(cast(n_hs3 as float)/cast(nrows as float),4) *100			as prop_hs3
						from	pa_events_Fact
						where	dk_date >= 20160415
						--and		(global_session_id is null or global_session_id = '' )
						group	by	dk_date
									,dk_serial_number
						order	by	dk_date desc
					)	as base
			group	by	dk_serial_number
		)	as base2
group	by	1


-- Checking at why certain actions on unexpected sessions (03000,02400,01001,01002)
select	dk_referrer_id
		,count(1) as freq
		,count(distinct dk_serial_number ) as nboxes
		,cast(nboxes as float)/127179 as prop_boxes
from	pa_events_Fact
where	dk_Action_id = 01001
and		home_session_lvl3_id = 'Fullscreen'
and		dk_Date >= 20160416
group	by	dk_referrer_id
order	by	freq desc


--------------------------------------------
-- A4 - Home Sessions delivered through HDFS
--------------------------------------------			

-- Integrity checks (assignment of Home Sessions)
select	dk_date
		,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as stb_type
		,count(1) as nrows
		,sum(case when length(global_session_id)>1 then 1 else 0 end) 			as n_gs
		,round(cast(n_gs as float)/ cast(nrows as float),4) *100 				as prop_gs
		,sum(Case when home_session_id <> '' then 1 else 0 end)					as n_hs
		,round(cast(n_hs as float)/cast(nrows as float),4) *100					as prop_hs
		,sum(case when length(trim(HOME_SESSION_LVL2_ID))>0 then 1 else 0 end) 	as n_hs2
		,round(cast(n_hs2 as float)/cast(n_gs as float),4) *100  				as prop_hs2
		,sum(case when length(trim(HOME_SESSION_LVL3_ID))>0 then 1 else 0 end)	as n_hs3
		,round(cast(n_hs3 as float)/cast(n_gs as float),4) *100					as prop_hs3
		,sum(case when account_number='' then 1 else 0 end) 					as n_noaccount
		,cast(n_noaccount as float)/cast(nrows as float)						as prop_rows_noaccount
		,count(distinct dk_serial_number) 										as nboxes
		,count(distinct account_number)											as naccounts
from	pa_events_Fact
where	dk_date = 20160508
group	by	dk_date
			,stb_type
order	by	dk_date desc
			,stb_type
			
			
select	case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as stb_type
		,home_session_lvl2_id
		,count(distinct dk_date||'-'||dk_serial_number||'-'||home_session_lvl2_id||'-'||home_session_lvl2_grain) as nsessions
		,case when length(trim(HOME_SESSION_LVL2_ID))>0  then 1 else 0 end as x_flag
from	pa_events_Fact
where	dk_date = 20160508
group	by	stb_type
			,home_session_lvl2_id

select	case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as stb_type
		,home_session_lvl3_id
		,count(distinct dk_date||'-'||dk_serial_number||'-'||home_session_lvl3_id||'-'||home_session_lvl3_grain) as nsessions
		,case when length(trim(HOME_SESSION_LVL3_ID))>0  then 1 else 0 end as x_flag
		,count(1) as nrows 		
from	pa_events_Fact
where	dk_date = 20160507
group	by	stb_type
			,home_session_lvl3_id