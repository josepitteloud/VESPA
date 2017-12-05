
-- z_s1_YYYYMMDD
select 	*
		,case when step_0 == x then null else concat(step_0,'-',string(the_id)) end as step_1
from	(
			SELECT	*
					,date(HitTime)	as thedate
					,case	when lower(screen) like '/guide%'											then 'TV Guide'
							when lower(screen) like '/search%' 											then 'Search'
							when 	(
										lower(screen) like '/kids%' 	or 
										lower(screen) like 'kids%' 		or
										lower(screen) like ' /kids%'	or
										lower(screen) like '% /kids/%'
									)	then 'Kids'
							when 	(
										lower(screen) like 'top picks%'		or
										lower(screen) like '/top picks%'	or
										lower(screen) like '/top picks%'	or
										lower(screen) like '/whats_new%'	or
										lower(screen) like '%/top picks/%'
									)	then 'Top Picks'
							when	hits.eventInfo.eventCategory = 'HomePageLinkJump' and -- Top Picks complement to capture it from Home Tiles
									(
										hits.eventInfo.eventAction CONTAINS 'row=3;column=1' or
										hits.eventInfo.eventAction CONTAINS 'row=3;column=3' or
										hits.eventInfo.eventAction CONTAINS 'row=3;column=4' or
										hits.eventInfo.eventAction CONTAINS 'row=4;column=4' or
										hits.eventInfo.eventAction CONTAINS 'row=3;column=5' or
										hits.eventInfo.eventAction CONTAINS 'row=3;column=6'
									)	then 'Top Picks'
							when	(
										lower(screen) like 'sky box sets%'	or
										lower(screen) like '%/sky box sets/%'
									)	then 'Sky Box Sets'
							when	(
										lower(screen) like '/planner'	or 
										lower(screen) like '/planner/%'
									)	then 'Recordings'
							when	(
										lower(screen) like 'sky cinema%'	or 
										lower(screen) like '% /sky cinema%'
									)	then 'Sky Cinema'
							when	(
										lower(screen) like 'sky store%'				or 
										lower(screen)like '% /sky store%' 			or
										lower(screen) like '/sky store%'			or 
										lower(screen) like '/anytime/sky store%'
									)	then 'Sky Store'
							when	(
										lower(screen) like '/sports%'			or 
										lower(screen) like '% /sports%' 		or
										lower(screen) like '/homepage/Sports/%'
									)	then 'Sports'
							when	(
										lower(screen) like 'catch up tv/%'		or 
										lower(screen) like '%/catch up tv/%'
									)	then 'Catch Up TV'
							when lower(screen) like '/homepage%'										then 'Home'
							when lower(screen) like '%/banner%'											then 'Mini Guide'
							when lower(screen) like '/tv/live%' 										then 'Fullscreen'
							else null
					end 	as step_0
					,max(step_0) over	( 
											partition by	VCID
											order by 		hitTime
															,hits.hitNumber 
											rows between 	1 preceding and 1 preceding
										)	as x
					,dense_rank() over	(
											partition by	thedate
																  ,VCID
																  ,step_0
											order by 		hitTime
																,hits.hitNumber 
										)	as the_id
					,max(HitTime) over(partition by vcid order by sessionid,hits.hitNumber rows between 1 following and 1 following) as next_timestamp
			FROM	FLATTEN	(
								(
									SELECT	MAX(IF(hits.customDimensions.index=3, hits.customDimensions.value, NULL)) WITHIN HITS AS VCID
											,visitId AS SessionId
											,hits.hitNumber
											,TIMESTAMP(INTEGER(visitStartTime*1000000 + hits.time*1000)) AS HitTime
											,hits.appInfo.screenName AS Screen
											,hits.eventInfo.eventCategory
											,hits.eventInfo.eventAction
											,hits.eventInfo.eventLabel AS EventLabel
								  FROM		TABLE_DATE_RANGE([78413818.ga_sessions_],TIMESTAMP('2016-08-14'),TIMESTAMP('2016-08-14'))
								)
								,VCID
							)
			where  	hitTime between timestamp('2016-08-13 00:00:00') and timestamp('2016-08-13 23:59:59')
			--and 	VCID = '613125947'
			order 	by	hitTime
						,hits.hitNumber 
		) 	as base

-- z_s2_YYYYMMDD
with ref_ as  (
                select    VCID
                          ,thedate
                          ,Step_1
                          ,SessionId      as from_s
                          ,hits_hitNumber as from_n
                          ,max(sessionid) over ( partition by thedate,vcid order by sessionid, hits_hitnumber rows between 1 following and 1 following ) as to_s
                          ,coalesce	(
										max(hits_hitnumber) over ( partition by thedate,vcid order by sessionid, hits_hitnumber rows between 1 following and 1 following )-1
										,9000000
									) 	as to_n
                from      Q_PA_Stage.z_step1
                where     step_1 is not null
                ORDER BY  sessionId
                          ,hits_hitNumber
              )
select  base.thedate
        ,base.VCID 															as viewing_card
        ,base.SessionID
        ,base.hits_hitNumber 												as Actions_sequence
        ,base.HitTime 														as Timestamp_
        ,base.Screen
        ,base.hits_eventInfo_eventCategory  								as Action_Category
        ,base.hits_eventInfo_eventAction    								as Action
        ,ref_.step_1 														as Sky_Plus_session_grain
		,base.EventLabel
		,base.next_timestamp
from    Q_PA_Stage.z_step1 as base
        Left JOIN ref_
        on  base.VCID  = ref_.VCID
        and base.thedate  = ref_.thedate
        and base.SessionID between ref_.from_s and ref_.to_s
        and base.hits_hitNumber between ref_.from_n and ref_.to_n
order   by  base.sessionId
            ,base.hits_hitNumber
 
 
 -- z_s3_YYYYMMDD
 select  thedate
        ,viewing_card
        ,sessionid
        ,actions_sequence
        ,timestamp_
        ,floor((timestamp(next_timestamp) - timestamp(timestamp_))/1000000) 	as secs_to_next_action
        ,screen
        ,action_category
        ,action
        ,eventlabel
        ,Sky_Plus_session_grain
        ,substr(Sky_Plus_session_grain,1,instr(Sky_Plus_session_grain,'-')-1) as Sky_plus_session
from    Q_PA_Stage.z_s2 
 
-- Final query
select  left(Sky_Plus_session_grain,instr(Sky_Plus_session_grain,'-')-1)	as Sky_plus_session
        ,count(distinct concat(string(thedate),string(viewing_card),string(sky_plus_session_grain))) as njourneys
        ,count(distinct (
                          case  when  (
                                        lower(screen) like '/tv/live/%' or
                                        lower(screen) like '/playback/%' or
                                        lower(eventlabel) like '%not_booked%'
                                      ) then concat(string(thedate),string(viewing_card),string(sky_plus_session_grain))
                                else null
                          end
                        ))  as conversion_flag
        ,count(distinct viewing_card) as reach
from    TABLE_QUERY(Q_PA_Stage,'table_id contains "z_plus_events_"')
group   by  Sky_plus_session


-- Done
lower(screen) like '/tv/live%'
and
lower(screen) not like '%banner%'
= 'Fullscreen'


-- Done
/guide% 				= 'TV Guide'


-- Done
/search% 				= 'Search'


-- Done
/kids%					= 'Kids'
kids%					= 'Kids'
 /kids%					= 'Kids'
% /kids/%				= 'Kids'


-- Done
top picks%				= 'Top Picks'
/top picks%				= 'Top Picks'
 /top picks%			= 'Top Picks'
%/top picks/%			= 'Top Picks'


-- Done
sky box sets%			= 'Sky Box Sets'
%/sky box sets/%		= 'Sky Box Sets'


-- Done
/planner & /planner/%	= 'Recordings'

-- Done
sky store%				= 'Sky Store'
% /sky store%			= 'Sky Store'
/sky store%				= 'Sky Store'
/anytime/sky store%		= 'Sky Store'

-- Done
/homepage%				= 'Home'


-- Done
/sports%				= 'Sports'
% /sports%				= '% /sports%'
/homepage/Sports/%		= '/homepage/Sports/%'

-- to verify on all of the aboves:

select  hits.appInfo.screenName
        ,case when lower(hits.appInfo.screenName) like '/kids%'    then 1
              when lower(hits.appInfo.screenName) like 'kids%'      then 1 
              when lower(hits.appInfo.screenName) like ' /kids%'    then 1
              when lower(hits.appInfo.screenName) like '% /kids/%'  then 1
              else 0
        end   as the_flag
        ,count(1) as hits
FROM    TABLE_DATE_RANGE([78413818.ga_sessions_],TIMESTAMP('2016-08-01'),TIMESTAMP('2016-09-12'))
where   lower(hits.appInfo.screenName) like '%kids%'
group   by  1,2
order   by  hits desc


Above dependant on software version... ojo

-- Identifying actions...

Action category = 	Asset Action is the set of actinos done over an asset and then for the event action which 
					is part of meta data for action category the values or the event labels tell what actially happened 

-----

1) use Tom query and snap one date
2) create the lower(screen)dim table
3) work on above date and join it with lower(screen)dim table (same logic for Home session)

!!! success means home sessions sliced (tuning the logic might be required by adjusting the actual lower(screen)names to be what they really are meant to be)

# Follow up step is to understand how to identify actions within sessions
