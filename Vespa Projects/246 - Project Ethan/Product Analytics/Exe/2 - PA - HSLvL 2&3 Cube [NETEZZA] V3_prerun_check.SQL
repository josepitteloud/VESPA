/*
drop table ETHAN_PA_PROD..PA_cube_check_2;
create table ETHAN_PA_PROD..PA_cube_check_2(
									chk_dt		datetime
								,	proc_date	date
								,	chk			float
							)
;
*/
SET CATALOG ETHAN_PA_PROD;
insert into ETHAN_PA_PROD..PA_cube_check_2
select
			now()	chk_dt
		,	b.proc_date
		,	cast(count(distinct a.datehour_) as float) / cast(24 as float)	chk
from
				ETHAN_PA_PROD..z_pa_events_fact	a
	right join	(
					select	min(x)+1	as proc_date
					from	(
								select	max(date(datehour_)) as x from ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N union
								select	max(date(datehour_)) as x from ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N union
								select	max(date(datehour_)) as x from ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N union
								select	max(date(datehour_)) as x from ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N
							)	as base
				)					b	on	b.proc_date		=	a.DATE_
group by	
		chk_dt
	,	b.proc_date
;


select	a.*
from
				ETHAN_PA_PROD..PA_cube_check_2	a
	inner join	(
					select	max(chk_dt) max_dt
					from	ETHAN_PA_PROD..PA_cube_check_2
				)								b	on	a.chk_dt	=	b.max_dt
;

