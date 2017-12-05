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
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Product Team
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        The table in question here (z_backdoor_facts) is the one holding the right output for Home Sessions.
		
		Home sessions is something that should be coming directly from hadoop but since the hourly batch job was failing to finish within the expected time, we had to delegate Home Sessions to Netezza.
		
		This scenario will change after the hourly batch job that treats PA JSONs gets sparkyfied (IDS - Tech / Big Data team working on this)
		
**Sections:

		A - Drafting Home Sessions
			A00 - Initialisation
				A01 - Find Starting points for all sessions
				A02 - Bag actions into their relevant sessions
			
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/

-----------------------
-- A00 - Initialisation
-----------------------
SET CATALOG ETHAN_PA_PROD;

-- PRE-REQUISIT:

/*
	if below query doesn't equals 1 then the rest should not be executed
*/
/*
select
		now()
	,	b.proc_date
	,	cast(count(distinct a.dk_datehour) as float) / cast(24 as float)	chk
from
				ETHAN_PA_PROD..pa_events_fact	a
	right join	(
					select	to_char(max(date(date_))+1,'YYYYMMDD')	as	proc_date
					from	ETHAN_PA_PROD..z_backdoor_facts
				)								b	on	a.DK_DATE	=	b.proc_date
group by
		now()
	,	b.proc_date
;
*/
-- END OF PRE-REQUISIT

truncate table z_backdoor_step_0;
truncate table z_backdoor_step_1;

commit;


----------------------------------------------
-- A01 - Find Starting points for all sessions
----------------------------------------------

insert	into z_backdoor_step_0
select
		index_
	,	dk_asset_id
	,	global_session_id
	,	dk_date
	,	dk_time
	,	dk_serial_number
	,	case	substr(dk_serial_number,3,1)
			when 'B' then 'Sky Q Silver'
			when 'C' then 'Sky Q Box'
			when 'D' then 'Sky Q Mini'
		end		as	stb_type
	,	dk_action_id
	,	dk_previous
	,	dk_current
	,	dk_referrer_id
	,	dk_trigger_id
	,	asset_uuid
	,	stage1
	,	remote_type
	,	case
			when	TLM =	(
								max(TLM) over	( 
													partition by	dk_serial_number
													order by 		index_ 
													rows between 	1 preceding and 1 preceding
												)
							)	then	null
			else						TLM||'-'|| dense_rank() over	(
																			partition by	dk_date
																							,dk_serial_number
																							,TLM
																			order by 		index_
																		)
		end		as	stage2	--	this highlights whether there has been a change in TLM. If so, Stage2 will show the new SLM name PLUS the degeneracy - i.e. how many additional transactions to come are attributed to the same SLM. Otherwise, this remains NULL.
--into	z_backdoor_step_0
from	(

			-- step 3 - Carving the sessions (level 3)
			
			/*
				here what we are aiming to achieve is to bag together all actions into a single and relevant
				session, at each level (3 in this case).
				
				Hence stripping (making nulls) then associate sessions values for any actions right below
				the session start point.
			*/
			select
					index_
				,	dk_asset_id
				,	global_session_id
				,	dk_date
				,	dk_time
				,	dk_serial_number
				,	dk_action_id
				,	dk_previous
				,	dk_current
				,	dk_referrer_id
				,	dk_trigger_id
				,	asset_uuid
				,	SLM	--	z_pa_screen_dim_v2.session_type
				,	remote_type
				,	case
						when	SLM =	(
											max(SLM) over	( 
																partition by	dk_serial_number
																order by 		index_ 
																rows between 	1 preceding and 1 preceding
															)
										)	then	null
						else						SLM||'-'|| dense_rank() over	(
																						partition by
																								dk_date
																							,	dk_serial_number
																							,	SLM
																						order by	index_
																					)
					end	as	stage1	--	this highlights whether there has been a change in SLM. If so, Stage1 will show the new SLM name PLUS the degeneracy - i.e. how many additional transactions to come are attributed to the same SLM. Otherwise, this remains NULL.
				,	screen_parent	--	z_pa_screen_dim_v2.session_type
				,	case
						when	stage1 is not null	then	(
																case	when screen_parent is null then substr(stage1,1,instr(stage1,'-')-1)
																		else screen_parent 
																end
															) 
						else								null 
					end	as	TLM	-- i.e. if there has been a change in SLM, then set TLM using z_pa_screen_dim_v2.session_type, or in its absence, simply the SLM name itself
			from	(
			
						-- step 2 - Identifying sessions starts
						
						/*
							by linking Step 1 output with pa_session_config, we then lookup (through thelinkage field)
							what are the URIs we have parametrised to make up the sessions (and the relevant session's metadata)
						*/
						select
								index_
							,	dk_asset_id
							,	a.global_session_id
							,	b.session_type	as	SLM
							,	a.thelinkage
							,	a.dk_action_id
							,	theprevious		as	dk_previous
							,	a.DK_current 	-- destination
							,	a.DK_REFERRER_ID
							,	a.dk_trigger_id
							,	a.asset_uuid
							,	a.dk_serial_number
							,	a.dk_date
							,	a.dk_time
							,	b.screen_parent
							,	a.remote_type
						from
											(
									
												-- Step 1 -> Sampling & preparing the data for merging with the Session Config table
												/*
													Sampling: 	
													
													in principle, given that this is just a POC we don't need the whole data
													but instead one day of a lab box which we knows has the latest SI build installed
																
													Preparing the data:
													
													given that we compiled for every action, its preceding (dk_previous) and its
													succeeding (dk_current) actions done by the user. Every interactive action (that is,
													any dk_action_id <> 01400) we state that the dk_previous is always = dk_referrer_id
													Doing so means that such dk_previous = the place where the action took place
													the field below named thelinkage should be a redundant validation for above.
													
												*/
												select
														row_number() over	(order by timems)	as	index_
													,	dk_asset_id
													,	timems
													,	dk_date
													,	dk_time
													,	dk_serial_number
													,	dk_action_id
													,	global_session_id
													,	case
															when dk_previous in ('01400','N/A')	then	dk_referrer_id	--	GLobal navigation
															else										dk_previous
														end										as	theprevious
													,	case
															when	(
																		dk_referrer_id	like 'guide://ondemand/asset/EVOD%' or
																		dk_previous	like 'guide://ondemand/asset/EVOD%'
																	)														then	'Top Picks'		--	map EVOD referrer IDs to Top Picks (covers VOD on Top Picks only)
															when (dk_referrer_id is not null and dk_referrer_id <> 'N/A')	then	dk_referrer_id	--	accept dk_referrer_id if it is not null
															when dk_action_id in ('01001','01002') 							then	'Mini guide'	--	these actions (dismiss mini guide, mini guide browse channel) are synonymous with the Mini guide
															when dk_previous like '0%'										then	dk_referrer_id 	--	capture a bug where Json message doesn't contain ref field (rarely seen now)
															when instr(dk_previous,'"',1) > 0 								then	translate(substr (dk_previous,instr(dk_previous,'"',1)),'"','')	-- trim all before " and remove "
															else																	dk_previous		--	default to dk_previous (orig) since this is planned to replace dk_referrer_id (ref) in the future
														end										as	thelinkage	-- join onto z_pa_screen_dim_v2 using this
													,	dk_current
													,	dk_trigger_id
													,	dk_referrer_id
													,	asset_uuid
													,	remote_type
												from	pa_events_fact as a
												where	dk_date =	(
																		select	to_char (date (min(x)+1), 'YYYYMMDD') as proc_date
																		from	(
																					select	max(date(date_)) as x from z_backdoor_facts
																				)	as base
																	)
											)					as	a
								left join	z_pa_screen_dim_v2	as	b -- pa_screen_dim
																		on	a.thelinkage	=	b.PK_SCREEN_ID
																		and b.pk_screen_id	not like '0%'
					)	as base
			group by
					index_
				,	dk_asset_id
				,	global_session_id
				,	dk_date
				,	dk_time
				,	dk_serial_number
				,	dk_action_id
				,	dk_previous
				,	dk_current
				,	dk_trigger_id
				,	dk_referrer_id
				,	asset_uuid
				,	SLM
				,	screen_parent
				,	remote_type
		)	as base2
;
commit;


-------------------------------------------------
-- A02 - Bag actions into their relevant sessions
-------------------------------------------------

/*
	- Propagating sessions tags (both levels 2 and 3)
	
	once we have carved the sessions and identified where each of them start and ends
	we simply propagate the correspondent tags (sessions names and ids) to the 
	associated actions (rows)
*/

insert	into z_backdoor_step_1
select
		index_
	,	dk_asset_id
	,	(TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp												as dt
	,	date((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp)										as date_
	,	datetime(to_char((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp,'YYYY-MM-DD HH24:00:00'))	as datehour_
	,	case
			when cast(datehour_ as time) between '00:00:00.000' and '05:59:59.000' then 'night'
			when cast(datehour_ as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
			when cast(datehour_ as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
			when cast(datehour_ as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
			when cast(datehour_ as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
			when cast(datehour_ as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
			when cast(datehour_ as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
		end		as part_of_day
	,	extract(epoch from	dt -	(
										min(dt) over	(	-- Simply gets dt for the preceding action
															partition by	dk_serial_number
															order by		index_
															rows between	1 preceding and 1 preceding
														)
									))
		as ss_elapsed_next_action	-- time difference between current action and preceding action in seconds
	,	dk_serial_number
	,	stb_type
	,	dk_action_id
	,	acts.action_name
	,	dk_previous
	,	dk_current
	,	dk_referrer_id
	,	dk_trigger_id
	,	asset_uuid
	,	remote_type
	,	last_value(stage1 ignore nulls) over	(
													partition by	dk_serial_number
													order by 		index_
													rows between	200 preceding and current row
												)							as gn_lvl3_session_grain	--	Find current - or if null, most recent - stage1/SLM name plus grain/degeneracy
	,	substr(gn_lvl3_session_grain,1,instr(gn_lvl3_session_grain,'-')-1)	as gn_lvl3_session			--	As above but without grain/degeneracy
	,	last_value(stage2 ignore nulls) over	(
													partition by	dk_serial_number
													order by 		index_
													rows between	200 preceding and current row
												)							as gn_lvl2_session_grain	--	Find current - or if null, most recent - stage2/TLM name plus grain/degeneracy
	,	substr(gn_lvl2_session_grain,1,instr(gn_lvl2_session_grain,'-')-1) as gn_lvl2_session			--	As above but without grain/degeneracy
--into	z_backdoor_step_1
from
				z_backdoor_step_0		as	obs
	left join	pa_action_dim	as	acts		on	obs.dk_action_id	=	acts.pk_action_id
	inner join	pa_time_dim		as	thetime		on	obs.dk_time			=	thetime.pk_time_dim
	inner join	pa_date_dim		as	thedate		on	obs.dk_Date			=	thedate.date_pk
order	by	index_;

commit;


insert	into z_backdoor_facts
select	*
from	z_backdoor_step_1;

commit;

---------------------
-- A03 - Housekeeping
---------------------
truncate table z_backdoor_step_0;
truncate table z_backdoor_step_1;

commit;