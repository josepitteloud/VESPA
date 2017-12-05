/* 
	This aims to see what sessions were created for all URIs with the '"' character in place 
	The assumption here is that querying for these URIs we will get all On-Demand sessions
*/
select	home_session_lvl3_id
		,count(1) as volume
		,count(distinct dk_serial_number||'-'||home_session_lvl3_grain) as nsessions
from	pa_events_fact
where	dk_datehour = 2016042422
and		instr(dk_previous,'"')>0
group	by	home_session_lvl3_id
order	by	home_session_lvl3_id


/* 
	This query serves to search for online video sessions to check all URIs captured within 
*/
select	distinct
		dk_serial_number
		,trim(home_session_lvl3_id)||'-'||trim(home_session_lvl3_grain)	as thesession
from	pa_events_fact
where	dk_datehour = 2016042422
and		home_session_lvl3_id = 'Online Videos'
limit	100

/* 
	I use this query to then summon above cases
*/
select	dk_serial_number
		,dk_date
		,timems
		,trim(home_session_lvl3_id)||'-'||trim(home_session_lvl3_grain)	as thesession
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
from	pa_events_fact
where	dk_datehour = 2016042422
and		trim(home_session_lvl3_id) = 'Online Videos'
and		home_session_lvl3_grain in (15,12)
and		dk_serial_number = '32B0560480019603'
order	by	timems


/* 
	And if you want to know all On-Demand URIs assumed by the '"' Character contained in the target date/hour 
*/
select	distinct dk_previous
from	pa_events_fact
where	dk_datehour = 2016042422
and		instr(dk_previous,'"')>0