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