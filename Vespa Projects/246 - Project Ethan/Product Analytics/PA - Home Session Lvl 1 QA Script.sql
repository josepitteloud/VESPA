
/*
	checking at the Home session rules
	
	a - there should be no home session assigned for actions id 01001 and 01002
	b - all global nav actions (01400) with destination either 'Fullscreen' or 'Stand By In' should have a home session id assigned
	c - there should be no home session assigned for actions with a ref value of either 'Fullscreen','zapper' or 'miniguide'
	
*/

select	dk_date
		,count(1)	as x
		,count(distinct home_session_id)	as y
		,count(distinct (case when dk_action_id in (01001,01002) and home_session_id <>'' then home_session_id else null end)) as a
		,(cast(a as float)/cast(y as float)) * 100	as bug_ratio_a
		,sum(case when dk_action_id = 01400 and (lower(dk_current) like '%fullscreen%' or lower(dk_current)like '%stand%by%') and home_session_id ='' then 1 else 0 end) as b
		,count(distinct (case when (lower(dk_referrer_id) like '%fullscreen%' or lower(dk_referrer_id) like '%zapper%' or lower(dk_referrer_id) like '%miniguide%') then home_session_id else null end)) as c
		,(cast(c as float)/cast(y as float)) * 100	as bug_ratio_c
from	pa_events_fact
where	dk_date >= 20160216
group	by	dk_date



/*-----------------------------------------------------------------------------------------------------------
	C would amend easyly by making sure that we evaluate the Home session logic after mapping the URIs to the
	screen names as for what we set on PA_SCREEN_DIM
*/-----------------------------------------------------------------------------------------------------------

select	distinct dk_referrer_id
from	pa_events_fact
where	dk_date >= 20160216
and		(lower(dk_referrer_id) like '%fullscreen%' or lower(dk_referrer_id) like '%zapper%' or lower(dk_referrer_id) like '%miniguide%')
and		home_session_id <> ''



select	base.*,a.*
from	(
			select	distinct dk_referrer_id
			from	pa_events_fact
			where	dk_date >= 20160216
			and		(lower(dk_referrer_id) like '%fullscreen%' or lower(dk_referrer_id) like '%zapper%' or lower(dk_referrer_id) like '%miniguide%')
			and		home_session_id <> ''
		)	as base
		left join pa_screen_dim as a
		on	lower(base.dk_referrer_id) = lower(a.pk_screen_id)