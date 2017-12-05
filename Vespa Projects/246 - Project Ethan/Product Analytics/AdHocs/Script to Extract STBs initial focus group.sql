/*
	The Actual Script to refresh the table
*/


truncate table z_pa_focus_group; commit;

insert	into z_pa_focus_group
select	stb_type
		,dk_serial_number
from	(
			select	*
					,dense_rank() over	(
											partition by	stb_type
											order by		x
										)	as therank
			from	(
						select	distinct
								substr(dk_serial_number,3,1) 		as stb_type
								,dk_serial_number
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

/*
	On the recent version we are using 30K STBs which are the full population existing on 20160307
*/

select	distinct
		dk_serial_number
into	z_pa_focus_group
from	pa_events_fact
where	dk_date = 20160307;commit;

/*
	we set an arbitrary target of STBs of circa 2000 to replicate Ethan's STB demographics
	(Falcons, Xwings and MRs)
*/

select	'base',*
		,nboxes/sum(nboxes) over (partition by 1) as prop_
from	(
			select	substr(dk_serial_number,3,1) 		as stb_type
					,count(distinct dk_serial_number)	as nboxes
			from	ethan_pa_prod..pa_events_fact
			where	dk_date = 20160303
			group	by	stb_type
		)	as base
union all
select	'sample',*
		,nboxes/sum(nboxes) over (partition by 1) as prop_
from	(		
			select	stb_type
					,count(distinct dk_serial_number) as nboxes
			from	(
						select	*
								,dense_rank() over	(
														partition by	stb_type
														order by		x
													)	as therank
						from	(
									select	distinct
											substr(dk_serial_number,3,1) 		as stb_type
											,dk_serial_number
											,random() as x
									from	(
												select	distinct
														substr(dk_serial_number,3,1)	as stb_type
														,dk_serial_number
												from	ethan_pa_prod..pa_events_fact
												where	dk_date = 20160303
											)	as step0
								)	as step1
					)	as step2
			where	(
						(stb_type = 'B' and therank <= (cast(2000 as float)*round(0.454541,2))) -- Falcons
						or (stb_type='D' and therank <= (cast(2000 as float)*round(0.535745,2))) -- MRs
						or (stb_type='C' and therank <= (cast(2000 as float)*round(0.009714,2))) -- Xwings
					)
			group	by	stb_type
		)	as checks