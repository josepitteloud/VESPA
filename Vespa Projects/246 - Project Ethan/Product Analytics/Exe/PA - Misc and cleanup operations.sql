-- Latest data check
select
		date_
	,	a.datehour_
	,	count(1)
from
				ETHAN_PA_PROD..z_pa_events_fact	a
where	date_	=	(
						select	max(date_)	dt
						from	ETHAN_PA_PROD..z_pa_events_fact
					)
group by
		date_
	,	a.datehour_
order by
		date_
	,	a.datehour_
;



/* -- For cleanup operation

delete from ETHAN_PA_PROD..z_pa_events_fact	where	date_ >= '2016-09-01';

delete from ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N	where date(datehour_) >= '2016-09-01';
delete from ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N	where date(datehour_) >= '2016-09-01';
delete from ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N	where date(datehour_) >= '2016-09-01';
delete from ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N	where date(datehour_) >= '2016-09-01';

*/


-- Prep for reprocessing
SELECT	*
INTO	ETHAN_PA_PROD..Z_PA_EVENTS_FACT_REPROC
FROM	ETHAN_PA_PROD..Z_PA_EVENTS_FACT
WHERE	1<>1
;

SELECT	*
INTO	ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N_REPROC
FROM	ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N
WHERE	1<>1
;

SELECT	*
INTO	ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N_REPROC
FROM	ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N
WHERE	1<>1
;

SELECT	*
INTO	ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N_REPROC
FROM	ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N
WHERE	1<>1
;

SELECT	*
INTO	ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N_REPROC
FROM	ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N
WHERE	1<>1
;


select * from ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N_REPROC limit 10;
select * from ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N_REPROC limit 10;
select * from ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N_REPROC limit 10;
select * from ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N_REPROC limit 10;




-- Check of Top Picks journeys
select
		date(datehour_)	dt
	,	count(1)
	,	sum(tot_journeys)
-- from	z_pa_cube_hslvl2_Sessions_N
from	z_pa_cube_hslvl2_Sessions_N_REPROC
where
		gn_lvl2_session	=	'Top Picks'
--	and	datehour_	>	'2016-07-01'
group by	dt
order by	dt
;
