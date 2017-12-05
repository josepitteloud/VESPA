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

        ADHoc metrics for Top Picks but quite similar to what we have. Here we are checking at how many journeys throughout the month into this feature, how many of those actually converted and how many unique visitors are we having in this area.
		
		Done for April, May and June 
		
		The sample must be 10k STBs already active before the month commence (1 sample per month) and STBs must have had at least 10 days of active reporting
		
**Sections:

		A - Calculating Top Picks Measures
			A00 - Sampling
				A01 - Find Starting points for all sessions
				A02 - Bag actions into their relevant sessions
				A03 - Generating output for Tableau Data source
				A99 - Housekeeping
			
			
**Running Time:

45 Min

--------------------------------------------------------------------------------------------------------------

*/


truncate table z_pa_adhoc_toppicks_sample_0;commit;
truncate table z_pa_adhoc_toppicks_sample_1;commit;
truncate table z_pa_adhoc_toppicks_step_0;commit;
--truncate table z_pa_adhoc_toppicks_base;commit;  --> OUTPUT [DO NOT CLEAR]

commit;

------------------
-- A00 -  Sampling 
------------------

insert	into z_pa_adhoc_toppicks_sample_0
select	dk_serial_number
--into	z_pa_adhoc_toppicks_sample_0
from	(		
			select	*
					,random() 	as x
			from	(
						select	distinct
								dk_serial_number
						from	pa_events_Fact
						where	dk_date = 20160531
					)	as base
		)	as base2
order	by	x	desc
limit	15000;

commit;

insert	into z_pa_adhoc_toppicks_sample_1
select	dk_serial_number
--into 	z_pa_adhoc_toppicks_sample_1
from	(
			select	dk_serial_number
					,count(distinct dk_date) as freq
			from	pa_events_fact
			where	dk_date between 20160601 and 20160630
			group	by	dk_serial_number
		)	as base
where	freq > 9
limit	10000;

commit;


select	count(1)
		,count(distinct dk_serial_number)
from	z_pa_adhoc_toppicks_sample_1

----------------------------------------------
-- A01 - Find Starting points for all sessions
----------------------------------------------

insert	into z_pa_adhoc_toppicks_step_0
select	index_
		,dk_asset_id
		,global_session_id
		,dk_date
		,dk_time
		,dk_serial_number
		,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as stb_type
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_trigger_id
		,asset_uuid
		,stage1
		,remote_type
		,case	when	TLM = (max(TLM) over	( 
													partition by	dk_serial_number
													order by 		index_ 
													rows between 	1 preceding and 1 preceding
												)
							) then null
				else TLM||'-'|| dense_rank() over	(
														partition by	dk_date
																		,dk_serial_number
																		,TLM
														order by 		index_
													)
		end		as stage2
--into	z_pa_adhoc_toppicks_step_0
from	(

			-- step 3 - Carving the sessions (level 3)
			
			/*
				here what we are aiming to achieve is to bag together all actions into a single and relevant
				session, at each level (3 in this case).
				
				Hence stripping (making nulls) then associate sessions values for any actions right below
				the session start point.
			*/
			select	index_
					,dk_asset_id
					,global_session_id
					,dk_date
					,dk_time
					,dk_serial_number
					,dk_action_id
					,dk_previous
					,dk_current
					,dk_referrer_id
					,dk_trigger_id
					,asset_uuid
					,SLM
					,remote_type
					,case	when SLM = (max(SLM) over	( 
																	partition by	dk_serial_number
																	order by 		index_ 
																	rows between 	1 preceding and 1 preceding
																)
											) then null
							else SLM||'-'|| dense_rank() over	(
																		partition by	dk_date
																						,dk_serial_number
																						,SLM
																		order by 		index_
																	)
					end		stage1
					,screen_parent
					,case 	when	stage1 is not null	then	(
																	case	when screen_parent is null then substr(stage1,1,instr(stage1,'-')-1)
																			else screen_parent 
																	end
																) 
							else null 
					end		as TLM
			from	(
			
						-- step 2 - Identifying sessions starts
						
						/*
							by linking Step 1 output with pa_session_config, we then lookup (through thelinkage field)
							what are the URIs we have parametrised to make up the sessions (and the relevant session's metadata)
						*/
						select	index_
								,dk_asset_id
								,a.global_session_id
								,b.session_type	as SLM
								,a.thelinkage
								,a.dk_action_id
								,theprevious	as dk_previous
								,a.DK_current 	-- destination
								,a.DK_REFERRER_ID
								,a.dk_trigger_id
								,a.asset_uuid
								,a.dk_serial_number
								,a.dk_date
								,a.dk_time
								,b.screen_parent
								,a.remote_type
						from	(
						
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
									select	row_number() over	(order by a.timems)	as index_
											,a.dk_asset_id
											,a.timems
											,a.dk_date
											,a.dk_time
											,a.dk_serial_number
											,a.dk_action_id
											,a.global_session_id
											,case	when a.dk_previous in ('01400','N/A') then	dk_referrer_id 
													else a.dk_previous
											end		as theprevious
											,case	when (a.dk_referrer_id is not null and a.dk_referrer_id <> 'N/A')	then a.dk_referrer_id
													when a.dk_action_id in ('01001','01002') 							then 'Mini guide'
													-- below one, to capture cases for Jsons without ref field as a bug
													when a.dk_previous like '0%'										then	dk_referrer_id 
													when instr(a.dk_previous,'"',1) > 0 								then translate(substr (dk_previous,instr(dk_previous,'"',1)),'"','')
													else a.dk_previous
											end		as thelinkage
											,a.dk_current
											,a.dk_trigger_id
											,a.dk_referrer_id
											,a.asset_uuid
											,a.remote_type
									from	pa_events_fact as a
											inner join z_pa_adhoc_toppicks_sample_1 as ref
											on	a.dk_serial_number = ref.dk_serial_number
									where	a.dk_date between 20160601 and 20160630
								)	as a
								left join z_pa_screen_dim_v2 as b -- pa_screen_dim
								on	a.thelinkage = b.PK_SCREEN_ID
								and b.pk_screen_id not like '0%'
					)	as base
			group	by	index_
						,dk_asset_id
						,global_session_id
						,dk_date
						,dk_time
						,dk_serial_number
						,dk_action_id
						,dk_previous
						,dk_current
						,dk_trigger_id
						,dk_referrer_id
						,asset_uuid
						,SLM
						,screen_parent
						,remote_type
		)	as base2;
		
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

insert	into z_pa_adhoc_toppicks_base
select	index_
		,dk_asset_id
		,(TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp												as dt
		,date((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp)										as date_
		,datetime(to_char((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp,'YYYY-MM-DD HH24:00:00'))	as datehour_
		,case	when cast(datehour_ as time) between '00:00:00.000' and '05:59:59.000' then 'night'
				when cast(datehour_ as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
				when cast(datehour_ as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
				when cast(datehour_ as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
				when cast(datehour_ as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
				when cast(datehour_ as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
				when cast(datehour_ as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
		end		as part_of_day
		,extract(epoch from	dt -	(
										min(dt) over	(
															partition by	dk_serial_number
															order by		index_
															rows between	1 preceding and 1 preceding
														)
									))
		as ss_elapsed_next_action
		,dk_serial_number
		,stb_type
		,dk_action_id
		,acts.action_name
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_trigger_id
		,asset_uuid
		,remote_type
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
--into	z_pa_adhoc_toppicks_base
from	z_pa_adhoc_toppicks_step_0	as obs
		left join pa_action_dim		as acts
		on	obs.dk_action_id	= acts.pk_action_id
		inner join pa_time_dim 		as thetime
		on	obs.dk_time			= thetime.pk_time_dim
		inner join pa_date_dim 		as thedate
		on	obs.dk_Date			= thedate.date_pk
order	by	index_;

commit;

--------------------------------------------------
-- A03 - Generating output for Tableau Data source
--------------------------------------------------

select	--to_char (date (date_), 'YYYYMMDD') as the_date
		date_
		,dk_serial_number
		,case when gn_lvl2_session = 'Top Picks' then gn_lvl2_session else 'Other Sessions' end	as Q_sessions
		,count(distinct	(
							case 	when length(gn_lvl2_session) <2 then null 
									else date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain
							end
						))	as tot_journeys
		,count	(distinct
					(
						case	when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain
								else null 
						end
					)
				)	as tot_actioned_journey
		,count(1)	as n_acctions
from	z_pa_adhoc_toppicks_base
group	by	date_
			,dk_serial_number
			,Q_sessions
			
			
---------------------
-- A99 - Housekeeping
---------------------

truncate table z_pa_adhoc_toppicks_sample_0;commit;
truncate table z_pa_adhoc_toppicks_sample_1;commit;
truncate table z_pa_adhoc_toppicks_step_0;commit;
--truncate table z_pa_adhoc_toppicks_base;commit;  --> OUTPUT [DO NOT CLEAR]

commit;