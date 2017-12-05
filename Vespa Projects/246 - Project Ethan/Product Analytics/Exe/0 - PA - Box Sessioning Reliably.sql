truncate table z_pa_reliable_boxes; commit;


insert	into z_pa_reliable_boxes
select	dk_serial_number
		,round(avg(prop_gs),0) as the_avg_prop_gs
--into	z_pa_reliable_boxes
from	(
			select	dk_date
					,dk_serial_number
					,count(1) as nrows
					,sum(case when length(global_session_id)>1 and length(HOME_SESSION_LVL2_ID)>0 then 1 else 0 end) 		as n_gs
					,round(cast(n_gs as float)/ cast(nrows as float),4) *100 			as prop_gs
--					,sum(Case when home_session_id <> '' then 1 else 0 end)				as n_hs
--					,round(cast(n_hs as float)/cast(nrows as float),4) *100				as prop_hs
--					,sum(case when length(HOME_SESSION_LVL2_ID)>0 then 1 else 0 end) 	as n_hs2
--					,round(cast(n_hs2 as float)/cast(nrows as float),4) *100  			as prop_hs2
--					,sum(case when length(HOME_SESSION_LVL3_ID)>0 then 1 else 0 end)	as n_hs3
--					,round(cast(n_hs3 as float)/cast(nrows as float),4) *100			as prop_hs3
			from	pa_events_Fact
			where	dk_date >= 20160415
			--and		(global_session_id is null or global_session_id = '' )
			group	by	dk_date
						,dk_serial_number
			order	by	dk_date desc
		)	as base
group	by	dk_serial_number
having	the_avg_prop_gs >=70;

commit;