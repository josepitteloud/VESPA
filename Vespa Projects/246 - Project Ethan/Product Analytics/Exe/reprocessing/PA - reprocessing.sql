
-- Prep for reprocessing
/*
select * into ETHAN_PA_PROD..Z_PA_EVENTS_FACT_REPROC from ETHAN_PA_PROD..Z_PA_EVENTS_FACT where 1 <> 1 limit 1;
select * into ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N_REPROC from ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N where 1 <> 1 limit 1;
select * into ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N_REPROC from ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N where 1 <> 1 limit 1;
select * into ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N_REPROC from ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N where 1 <> 1 limit 1;
select * into ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N_REPROC from ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N where 1 <> 1 limit 1;
select * into ETHAN_PA_PROD..z_pa_stb_tenure_reproc from ETHAN_PA_PROD..z_pa_stb_tenure where 1 <> 1 limit 1;
*/

truncate table ETHAN_PA_PROD..Z_PA_EVENTS_FACT_REPROC;
truncate table ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N_REPROC;
truncate table ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N_REPROC;
truncate table ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N_REPROC;
truncate table ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N_REPROC;
truncate table ETHAN_PA_PROD..z_pa_stb_tenure_reproc;

INSERT INTO	ETHAN_PA_PROD..Z_PA_EVENTS_FACT_REPROC
SELECT	*
FROM	ETHAN_PA_PROD..Z_PA_EVENTS_FACT
WHERE	date(date_)	=	'2016-07-14'
;

INSERT INTO	ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N_REPROC
SELECT	*
FROM	ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N
WHERE	date(datehour_)	=	'2016-07-14'
;

INSERT INTO	ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N_REPROC
SELECT	*
FROM	ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N
WHERE	date(datehour_)	=	'2016-07-14'
;

INSERT INTO	ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N_REPROC
SELECT	*
FROM	ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N
WHERE	date(datehour_)	=	'2016-07-14'
;

INSERT INTO	ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N_REPROC
SELECT	*
FROM	ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N
WHERE	date(datehour_)	=	'2016-07-14'
;

insert into z_pa_stb_tenure_reproc
select	*
from	z_pa_stb_tenure
where	pa_start_dt	<	'2016-07-15'
;

/* Check data
select * from ETHAN_PA_PROD..Z_PA_EVENTS_FACT_REPROC limit 10;
select * from ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N_REPROC limit 10;
select * from ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N_REPROC limit 10;
select * from ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N_REPROC limit 10;
select * from ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N_REPROC limit 10;
select * from ETHAN_PA_PROD..z_pa_stb_tenure_reproc limit 10;
*/


--	Compare old and reprocessed Top Picks journeys
select
		a.dt
	,	a.gn_lvl2_session
	,	a.num			as	old_count
	,	a.num_journeys	as	old_num_journeys
	,	b.num			as	new_count
	,	b.num_journeys	as	new_num_journeys
from
				(
					select
							date(datehour_)	dt
						,	gn_lvl2_session
						,	count(1)	num
						,	sum(tot_journeys)	num_journeys
					from	z_pa_cube_hslvl2_Sessions_N
					-- where	gn_lvl2_session	=	'Top Picks'
					group by
							dt
						,	gn_lvl2_session
				)	a
	left join	(
					select
							date(datehour_)	dt
						,	gn_lvl2_session
						,	count(1)	num
						,	sum(tot_journeys)	num_journeys
					from	z_pa_cube_hslvl2_Sessions_N_REPROC
					-- where	gn_lvl2_session	=	'Top Picks'
					group by
							dt
						,	gn_lvl2_session
				)	b	on	a.dt				=	b.dt
						and	a.gn_lvl2_session	=	b.gn_lvl2_session
order by
		a.dt
	,	a.gn_lvl2_session
;



/*
-- Push back into production tables
select * into ETHAN_PA_PROD..Z_PA_EVENTS_FACT_reproc_BACKUP from ETHAN_PA_PROD..Z_PA_EVENTS_FACT where date(date_)	between '2016-07-15' and '2016-09-04';
select * into ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N_reproc_BACKUP from ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N WHERE	date(datehour_)	between '2016-07-15' and '2016-09-04';
select * into ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N_reproc_BACKUP from ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N WHERE	date(datehour_)	between '2016-07-15' and '2016-09-04';
select * into ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N_reproc_BACKUP from ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N WHERE	date(datehour_)	between '2016-07-15' and '2016-09-04';
select * into ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N_reproc_BACKUP from ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N WHERE	date(datehour_)	between '2016-07-15' and '2016-09-04';
select * into ETHAN_PA_PROD..z_pa_stb_tenure_reproc_BACKUP from ETHAN_PA_PROD..z_pa_stb_tenure where	pa_start_dt	between '2016-07-15' and '2016-09-04';

delete from ETHAN_PA_PROD..Z_PA_EVENTS_FACT where date(date_)	>=	'2016-07-15';
delete from ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N WHERE	date(datehour_)	>=	'2016-07-15';
delete from ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N WHERE	date(datehour_)	>=	'2016-07-15';
delete from ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N WHERE	date(datehour_)	>=	'2016-07-15';
delete from ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N WHERE	date(datehour_)	>=	'2016-07-15';
delete from ETHAN_PA_PROD..z_pa_stb_tenure where	pa_start_dt	>=	'2016-07-15';

insert into ETHAN_PA_PROD..Z_PA_EVENTS_FACT select * from  ETHAN_PA_PROD..Z_PA_EVENTS_FACT_REPROC	where date(date_)	>= '2016-07-15';
insert into ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N select * from  ETHAN_PA_PROD..z_pa_cube_hslvl2_Interaction_N_REPROC	WHERE	date(datehour_)	>= '2016-07-15';
insert into ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N select * from  ETHAN_PA_PROD..z_pa_cube_hslvl2_Sessions_N_REPROC	WHERE	date(datehour_)	>= '2016-07-15';
insert into ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N select * from  ETHAN_PA_PROD..z_pa_cube_hslvl3_Interaction_N_REPROC	WHERE	date(datehour_)	>= '2016-07-15';
insert into ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N select * from  ETHAN_PA_PROD..z_pa_cube_hslvl3_Sessions_N_REPROC	WHERE	date(datehour_)	>= '2016-07-15';
insert into ETHAN_PA_PROD..z_pa_stb_tenure select * from  ETHAN_PA_PROD..z_pa_stb_tenure_REPROC	where	pa_start_dt	>= '2016-07-15';

*/
