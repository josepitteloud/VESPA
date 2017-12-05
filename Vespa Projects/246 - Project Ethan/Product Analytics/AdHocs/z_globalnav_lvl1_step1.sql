/*
	The Actual Script to refresh the table
*/


truncate table z_pa_focus_group; commit;

insert	into z_pa_focus_group
select	stb_type
		,substr(serial,1,16) as dk_serial_number
from	(
			select	*
					,dense_rank() over	(
											partition by	stb_type
											order by		x
										)	as therank
			from	(
						select	distinct
								substr(dk_serial_number,3,1) 		as stb_type
								,dk_serial_number as serial
								,random() as x
						from	(
									select	distinct
											substr(dk_serial_number,3,1)	as stb_type
											,dk_serial_number
									from	ethan_pa_prod..pa_events_fact
									where	dk_date >= 20160303
								)	as step0
					)	as step1
		)	as step2
where	(
			(stb_type = 'B' and therank <= (cast(2000 as float)*round(0.454541,2))) -- Falcons
			or (stb_type='D' and therank <= (cast(2000 as float)*round(0.535745,2))) -- MRs
			or (stb_type='C' and therank <= (cast(2000 as float)*round(0.009714,2))) -- Xwings
		);

commit;


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
					on	substr(a.dk_serial_number,1,16)	= c.dk_serial_number
--					inner join pa_trial_dim 	as c
--					on	a.dk_serial_number = c.pk_serial_number
--					and	c.dk_utype in (3,4)
			where	dk_date >= 20160301
		)	as a
		left join Z_PA_SCREEN_DIM_V2 as b -- pa_screen_dim
		on	a.thelinkage = b.PK_SCREEN_ID
		and b.pk_screen_id not like '0%'
		and	b.SESSION_TYPE is not null;
		
		
commit;


------------------------
-- PREPARING Z ASSET DIM
------------------------

truncate table z_pa_asset_dim;commit;

insert	into z_pa_asset_dim
select	programme_uuid
		,programme_name
		,programme_duration_seconds
		,programme_genre
--into	z_pa_asset_dim
from	(
			select	*
					,dense_rank() over	(
											partition by	programme_uuid
											order by		theflag	desc
															,programme_duration_seconds	desc
															,programme_genre
															,programme_name
										)	as thefilter
			from	(
						select	*
								,max(programme_name) over	(
																partition by	programme_uuid 
																order by		programme_uuid 
																rows between 	1 following and 1 following
															)	as x
								,max(programme_duration_seconds) over	(
																			partition by	programme_uuid 
																			order by		programme_uuid 
																			rows between 	1 following and 1 following
																		)	as y 
								,max(programme_genre) over	(
																partition by	programme_uuid 
																order by		programme_uuid 
																rows between 	1 following and 1 following
															)	as z
								,case 	when y is null then 1
										else	(
													case	when programme_duration_seconds - y > 0 then 2
															else 0
													end
												)
								end		as theflag
						from	(
									select	programme_uuid
											,programme_name
											,programme_duration_seconds
											,programme_genre
											,count(1) as freq
											,dense_rank() over	(
																	partition by	programme_uuid
																	order by		freq desc
																)	as therank
									from	(
												select	distinct
														base.programme_uuid
														,base.programme_name
														,base.programme_duration_seconds
														,base.programme_genre
												from	pa_asset_dim as base
														inner join	(
																		select	programme_uuid
																				,max(broadcast_start_datetime) as thedate
																		from	pa_asset_dim
																		where	length(programme_uuid)>20
																		group	by	programme_uuid
																	)	as ref
														on	base.programme_uuid				= ref.programme_uuid
														and	base.broadcast_start_datetime	= ref.thedate
											)	as final
									group	by	programme_uuid
												,programme_name
												,programme_duration_seconds
												,programme_genre
									) as x
					)	as y
		)	as z
where	thefilter = 1;
commit;

-------------------------------
-- YOU CAN TRUST THIS IF BELOW:
-------------------------------

-- QA: Below should be always 100%
select	round((cast(count(distinct programme_uuid) as float)/ cast(count(1) as float)),2)*100 as matching_ratio
from	z_pa_asset_dim