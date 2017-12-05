-----------------------
-- To be ran on Netezza
-----------------------
truncate table z_globalnav_lvl1_step1;

commit;

insert	into z_globalnav_lvl1_step1
select	index_
		,dk_asset_id
		,coalesce (b.SCREEN_PARENT,b.screen_name) as sessions
		,a.thelinkage
		,a.dk_action_id
		,a.asset_uuid
		,theprevious	as dk_previous
		,a.DK_current 	-- destination
		,a.DK_REFERRER_ID
		,a.dk_trigger_id
		,dk_serial_number
		,dk_date
		,remote_type
--into	z_globalnav_lvl1_step1
from	(
			select	row_number() over	(order by timems)	as index_
					,dk_asset_id
					,timems
					,dk_date
					,a.dk_serial_number
					,dk_action_id
					,asset_uuid
					,case	when dk_previous in ('01400','N/A') then	dk_referrer_id 
							else dk_previous
					end		as theprevious
					,case	when (dk_referrer_id is not null and dk_referrer_id <> 'N/A') then dk_referrer_id
							when dk_previous like '0%'	then	dk_referrer_id -- to capture cases for Jsons without ref field as a bug
							else dk_previous
					end		as thelinkage
					,dk_current
					,dk_trigger_id
					,dk_referrer_id
					,remote_type
			from	pa_events_fact as a
					inner join z_pa_focus_group as c
					on	a.dk_serial_number	= c.dk_serial_number
--					inner join pa_trial_dim 	as c
--					on	a.dk_serial_number = c.pk_serial_number
--					and	c.dk_utype in (3,4)
			where	dk_date >= 20160301
		)	as a
		left join pa_screen_dim as b
		on	a.thelinkage = b.PK_SCREEN_ID
		and b.pk_screen_id not like '0%'
		and	b.SESSION_TYPE is not null;
		
		
commit;

---------------------------------------------
-- slicer to construct Tableau's data extract
---------------------------------------------

select	index_
		,asset_uuid
		,dk_date
		,dk_serial_number
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_trigger_id
		,box_type
		,remote_type
		,last_value(stage1 ignore nulls) over	(
													partition by	dk_serial_number
													order by 		index_
													rows between	500 preceding and current row
												)					as gn_session_grain
		,substr(gn_session_grain,1,instr(gn_session_grain,'-')-1) 	as gn_session
from	(
			select	index_
					,asset_uuid
					,dk_date
					,dk_serial_number
					,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
							when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
							when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
					end		as box_type
					,dk_action_id
					,dk_previous
					,dk_current
					,dk_referrer_id
					,dk_trigger_id
					,sessions
					,remote_type
					,case	when sessions = (max(sessions) over	( 
																	partition by	dk_serial_number
																	order by 		index_ 
																	rows between 	1 preceding and 1 preceding
																)
											) then null
							else sessions||'-'|| dense_rank() over	(
																		partition by	dk_date
																						,dk_serial_number
																						,sessions
																		order by 		index_
																	)
					end		stage1
			from	z_globalnav_lvl1_step1
			group	by	index_
						,asset_uuid
						,dk_date
						,dk_serial_number
						,dk_action_id
						,dk_previous
						,dk_current
						,dk_trigger_id
						,dk_referrer_id
						,sessions
						,remote_type
		)	as base2