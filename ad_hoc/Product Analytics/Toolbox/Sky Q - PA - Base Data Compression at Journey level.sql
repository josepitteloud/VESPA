
----------------------------------------
-- Base Data Compression - Journey Level
----------------------------------------

with	ref_conv as	(
						-- Identifying indexes of conversion/playback...
						select	date_
								,dk_Serial_number
								,gn_lvl2_Session_Grain
								,min(index_)																as x_
								,min(case when dk_action_id in (00001,03000) then index_  else null end)	as x_play
						from	z_pa_events_fact_YYYYMM
						and		dk_action_id in (02400,00001,03000,02000,02010,02002,02005)
						group	by	date_
									,dk_Serial_number
									,gn_lvl2_Session_Grain
					)
select	g.date_
		,g.dk_serial_number
		,g.stb_type
		,g.session
		,g.session_grain
		,g.gn_lvl2_session
		,g.gn_lvl2_session_grain
		
		-- Time
		,min(g.dt)						as start_
		,min(g.timems)					as start_time
		,min(g.index_)					as start_index
		,max(g.dt)						as end_
		,max(g.timems)					as end_time
		,max(g.index_)					as end_index
		,sum(g.ss_elapsed_next_action)	as time_spent
		
		-- Activity
		,count(1)																		as nactions
		,sum(case when g.dk_action_id = 01400 then 1 else 0 end)						as n_gnavs
		,sum(case when g.dk_action_id = 02400 then 1 else 0 end)						as n_downloads
		,sum(case when g.dk_action_id = 03000 then 1 else 0 end)						as n_playbacks
		,sum(case when g.dk_action_id = 00001 then 1 else 0 end)						as n_tunings
		,sum(case when g.dk_action_id in (02000,02010,02002,02005) then 1 else 0 end)	as n_bookings
		,sum(case when g.dk_action_id = 04002 then 1 else 0 end)						as n_launches
		
		-- Effectiveness
		,sum(case when ref_conv.x_ is not null and g.index_ <= ref_conv.x_ then g.ss_elapsed_next_action else null end)			as time_to_conv
		,sum(case when ref_conv.x_play is not null and g.index_ <= ref_conv.x_play then g.ss_elapsed_next_action else null end)	as time_to_playback
		
into	z_pa_journeys_YYYYMM -- it is suggested to store this in a table named as following but clean after project is done...
from	z_pa_events_fact_YYYYMM	as g
		left join ref_conv
		on	g.date_					= ref_conv.date_
		and	g.dk_Serial_number		= ref_conv.dk_Serial_number
		and	g.gn_lvl2_Session_Grain	= ref_conv.gn_lvl2_Session_Grain
group	by	g.date_
			,g.dk_serial_number
			,g.stb_type
			,g.session
			,g.session_grain
			,g.gn_lvl2_session
			,g.gn_lvl2_session_grain