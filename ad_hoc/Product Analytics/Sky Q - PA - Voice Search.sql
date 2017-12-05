/*
-------------
-- Foundation
-------------

	the base for anything that is done in this script is z_backdoor_facts
	
	This table was created by re running Z_PA_EVENTS_FACT script, considering the new addition to Z_PA_SCREEN_DIM_V2 for voice search URI (This is already in place). For example, by simply re-running Z_PA_EVENTS_FACT logic on any date within z_backdoor_facts you will right away achieve the same result.
	
	this was done to carve voice search journeys based on the relevant URI
	
	The following 3 querys (slicers) are making up the aggregations to what given to Aamer
	
	Output file location:
	G:\RTCI\Sky Projects\Vespa\Products\Analysis - Excel\Sky Q - PA - Voice Search Proxy.xlsx

*/

-- I Effectiveness
select	case 	when date_ between '2016-11-23' and '2016-11-29'	then 'Week 1'
				else 'Week 2'
		end		as weeks
		,gn_lvl2_Session
		,count(distinct date_||'-'||dk_Serial_number||'-'||gn_lvl2_Session_Grain) as njourneys
		,count(distinct (case when dk_Action_id = 01605 then date_||'-'||dk_Serial_number||'-'||gn_lvl2_session_grain else null end)) as njourneys_searched
		,count(distinct (Case when dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then date_||'-'||dk_Serial_number||'-'||gn_lvl2_session_grain else null end)) as nconv_journeys
from	z_backdoor_facts
where	gn_lvl2_Session = 'Voice Search'
and		date_>= '2016-11-23'
group	by	weeks
			,gn_lvl2_Session

-- II Reach
select	case 	when date_ between '2016-11-23' and '2016-11-29'	then 'Week 1'
				else 'Week 2'
		end		as weeks
		,count(distinct dk_Serial_number) as npop
		,count(distinct (case when gn_lvl2_Session = 'Voice Search' then dk_SErial_number else null end)) as nvs_reach
		,count(distinct (case when gn_lvl2_Session = 'Voice Search' and dk_Action_id = 01605 then dk_Serial_number else null end)) as nvs_reach_Searching
		,count(distinct (Case when gn_lvl2_Session = 'Voice Search' and dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then dk_Serial_number else null end)) as nvs_reach_acting
from	z_backdoor_facts
where	date_ >= '2016-11-23'
group	by	weeks

-- III:
-- how frequent (%of active days) are people using the voice search feature...
select	case	when engagement_ratio between 0 and 0.09 then '0-9'
				when engagement_ratio between 0.1 and 0.19 then '10-19'
				when engagement_ratio between 0.2 and 0.29 then '20-29'
				when engagement_ratio between 0.3 and 0.39 then '30-39'
				when engagement_ratio between 0.4 and 0.49 then '40-49'
				when engagement_ratio between 0.5 and 0.59 then '50-59'
				when engagement_ratio between 0.6 and 0.69 then '60-69'
				when engagement_ratio between 0.7 and 0.79 then '70-79'
				when engagement_ratio between 0.8 and 0.89 then '80-89'
				when engagement_ratio between 0.9 and 0.99 then '90-99'
				when engagement_ratio = 1 then '100'
		end		engagement_buckets
		,count(distinct dk_serial_number) as nboxes
from	(
			-- for everyone with a fair timespan of activity (at least 15 days) and who has searched something at least once over a timeframe...
			select	dk_Serial_number
					,count(distinct date_) as nactive_days
					,count(distinct (case when dk_action_id = 01605 and gn_lvl2_session = 'Voice Search' then date_ else null end)) as ndays_voicesearching
					,cast(ndays_voicesearching as float) / cast(nactive_days as float)	as engagement_ratio
			from	z_backdoor_facts
			where	date_ >= '2016-11-13'
			group	by	dk_serial_number
			having	nactive_days >=3 -- change this to 15 when rolling it out...
			and		ndays_voicesearching > 0
		)	as base
group	by	engagement_buckets