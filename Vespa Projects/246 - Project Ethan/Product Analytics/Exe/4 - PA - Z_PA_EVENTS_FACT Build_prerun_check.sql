/*
drop table ETHAN_PA_PROD..PA_cube_check_1;
create table ETHAN_PA_PROD..PA_cube_check_1(
									chk_dt		datetime
								,	proc_date	date
								,	chk			float
							)
;
*/
SET CATALOG ETHAN_PA_PROD;
insert into ETHAN_PA_PROD..PA_cube_check_1
select
		now()	chk_dt
	,	b.proc_date
	,	cast(count(distinct a.dk_datehour) as float) / cast(24 as float)	chk
from
				ETHAN_PA_PROD..pa_events_fact	a
	right join	(
					select	to_char(max(date(date_))+1,'YYYYMMDD')	as	proc_date
					from	ETHAN_PA_PROD..z_pa_events_fact
				)								b	on	a.DK_DATE	=	b.proc_date
group by
		chk_dt
	,	b.proc_date
;

select	a.*
from
				ETHAN_PA_PROD..PA_cube_check_1	a
	inner join	(
					select	max(chk_dt) max_dt
					from	ETHAN_PA_PROD..PA_cube_check_1
				)								b	on	a.chk_dt	=	b.max_dt
;

