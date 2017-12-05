/* WORKING ON PLOTING PA INTO SANKEY */

select	uri_ref_previous.the_index	as x
		,uri_ref_current.the_index	as y
		,expol.instance
		,((expol.instance-25.00)/4.00) as sigmoid_p
		,case	when 	expol.instance = 1 and base.journey_lvl > 1 then (sigmoid_p + ((-6+(6*base.journey_lvl))*2))+0.01
				else	sigmoid_p + ((-6+(6*base.journey_lvl))*2)
		end		as stretch
		,y+((x-y)*(1/(1+EXP(1)^+sigmoid_p))) as the_curve
		,base.*
from	z_journey_cube	as base
		cross join	table(system..ROWEXPAND(49)) as expol
		inner join	(
						select	pos
								,dense_rank() over	(
														order by pos
													)	as the_index
						from	(
									select	distinct dk_previous	as pos
									from	z_journey_cube
									union
									select	distinct dk_current		as pos
									from	z_journey_cube
								)	as dic
					)	as uri_ref_previous
		on	base.dk_previous	= uri_ref_previous.pos
		inner join	(
						select	pos
								,dense_rank() over	(
														order by pos
													)	as the_index
						from	(
									select	distinct dk_previous	as pos
									from	z_journey_cube
									union
									select	distinct dk_current		as pos
									from	z_journey_cube
								)	as dic
					)	as uri_ref_current
		on	base.dk_current		= uri_ref_current.pos
order	by	x,y,instance


/* HERE IS, WITH PA DATA, WHAT WE NEED TO TRANSFORM INTO A SANKEY LOOKING LIKE DIAGRAM FOR JOURNEY ANALYSIS*/
truncate table z_journey_cube;

commit;

insert	into z_journey_cube
select	thedate
		,gn_lvl3_session
		,journey_lvl
		,dk_previous
		,dk_current
		,count(1) 												as freq
		,count(distinct dk_serial_number) 						as nboxes
		,sum(time_to_action)									as tot_ss_to_action
		,cast(tot_ss_to_action as float)/cast(freq as float)	as avg_ss_to_action
		,cast(tot_ss_to_action as float)/cast(nboxes as float)	as avg_sstoaction_x_stb
--into	z_journey_cube
from	(
			select	date(dt) 				as thedate
					,dk_serial_number
					,gn_lvl3_session
					,gn_lvl3_session_grain
					,index_
					,dk_action_id
					,ss_elapsed_next_action	as time_to_action
					,dense_rank() over	(
											partition by	date(dt)
															,dk_serial_number
															,gn_lvl3_session
															,gn_lvl3_session_grain
											order by		index_
										)	as journey_lvl
					,dk_previous
					,dk_current
			from	z_hslvl23_obs -- this table is the output from home session logic lvl 3
			where	date(dt) >= '2016-01-15'
			and		dk_serial_number = '32D00004800136512'
		)	as step1
where	journey_lvl <=7
group	by	thedate
			,gn_lvl3_session
			,journey_lvl
			,dk_previous
			,dk_current
order	by	thedate
			,gn_lvl3_session
			,journey_lvl;
commit;

/* BELOW IS THE FIRST ROUND I DID IN NETEZZA TO UNDERSTAND HOW TO DO THE SANKEY DIAGRAM */
-- STEP 3: Calculating "stretch" and Designing the "curve" to be displayed in tableau for each observation
select	stage1.pos1
		,stage1.pos2
		,stage1.freq
		,stage1.total_acts
		,stage1.ratio
		,pos_map_x.the_index	as x
		,pos_map_y.the_index	as y
		,stage1.lvl
		,stage1.instance
		,((stage1.instance-25.00)/4.00) as T
		,case	when 	stage1.instance = 1 and stage1.lvl > 1 then (t + ((-6+(6*lvl))*2))+0.01
				else	t + ((-6+(6*lvl))*2)
		end		as stretch
		,X+((Y-X)*(1/(1+EXP(1)^-T)))	as curve
from	(
			-- STEP 2: expanding 49 times each observation in order to plot lines in Tableau
			select	pos1
					,pos2
					,freq
					,total_acts
					,ratio
					,lvl
					,instance
			from	(
						-- STEP 1: getting data for all levels I'll be plotting in Tableau
						select	dk_previous	as pos1
								,dk_current	as pos2
								,count(1)	as freq
								,sum(freq) over	(
													partition by dk_previous
												)	as total_acts
								,freq/total_acts	as ratio
								,count(distinct dk_serial_number)	as n_stbs
								,1	as lvl
						from	pa_events_fact	as a
						where	dk_date = 20150918
						/* and		dk_previous in	(
													'guide://home'
													,'guide://fullscreen'
													,'guide://settings-direct/bluetoothremote'
												) */
						group	by	dk_previous
									,dk_current
						union
						select	dk_current	as pos1
								,dk_next	as pos2
								,count(1)	as freq
								,sum(freq) over	(
													partition by dk_current
												)	as total_acts
								,freq/total_acts	as ratio
								,count(distinct dk_serial_number)	as n_stbs
								,2	as lvl
						from	pa_events_fact	as a
						where	dk_date = 20150918
						/* and		dk_current in	(
													'guide://home'
													,'guide://standby'
													,'guide://fullscreen'
												) */
						group	by	dk_current
									,dk_next
					)	as base
					cross join	table(system..ROWEXPAND(49)) as expol
		)	as stage1
		inner join	(
						select	pos
								,dense_rank() over	(
														order by pos
													)	as the_index
						from	(
									select	distinct
											dk_previous	as pos
									from	pa_events_fact -- 82
									where	dk_date = 20150918
									union
									select	distinct
											dk_current
									from	pa_events_fact -- 139
									where	dk_date = 20150918
									union
									select	distinct
											dk_next
									from	pa_events_fact
									where	dk_date = 20150918
								)	as base
					)	as pos_map_x
		on	stage1.pos1	= pos_map_x.pos
		inner join	(
						select	cast(pos as varchar(42))	as pos
								,dense_rank() over	(
														order by pos
													)	as the_index
						from	(
									select	distinct
											dk_previous	as pos
									from	pa_events_fact -- 82
									where	dk_date = 20150918
									union
									select	distinct
											dk_current
									from	pa_events_fact -- 139
									where	dk_date = 20150918
									union
									select	distinct
											dk_next
									from	pa_events_fact
									where	dk_date = 20150918
								)	as base
					)	as pos_map_y
		on	stage1.pos2	= pos_map_y.pos
order	by	pos1
			,pos2
			,lvl
			,t