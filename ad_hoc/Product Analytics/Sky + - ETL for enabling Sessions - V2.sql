/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ 
             ?$$$,      I$$$ $$$$. $$$$  $$$= 
              $$$$$$$$= I$$$$$$$    $$$$.$$$  
                  :$$$$~I$$$ $$$$    $$$$$$   
               ,.   $$$+I$$$  $$$$    $$$$=   
              $$$$$$$$$ I$$$   $$$$   .$$$    
                                      $$$     
                                     $$$      
                                    $$$?

            CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							PRODUCTS HOLISTIC DASHBOARD
**Done By:                             	Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Products Team
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        The goal of this logic is to use Sky + data related to users interactions with the STB in the similar fashion as what capture for Sky Q Product Analytics
		In order to enable us analysing Sky + Journeys into areas of interest and having a comparative base against Q
		
**Sections:

		A - Creating Base layer for Journey analysis.			
			A01 - Step 1, Identifying Starting point of Sessions (z_s1_YYYYMMDD)
			A02 - Step 2, Encapsulating set of actions under relevant sessions (z_s2_YYYYMMDD)
			A03 - Final, Minor key ETLs (z_final_YYYYMMDD)

**Note:

	the following token:
	
		-- < PARAMETER
		
	Identifies the table names I manually have to input to process one specific day worth of data
--------------------------------------------------------------------------------------------------------------

*/

-----------------------------------------------------------------------
-- A01 - Step 1, Identifying Starting point of Sessions (z_s1_YYYYMMDD)
-----------------------------------------------------------------------

/*
	This piece (in order) Extracts all data available for any given day (works one day at a time) and then scans all screens name through a set of wild-cards to identify what screens are to be considered the starts of any sessions 
	
	
	NOTE: SQL Legacy is ticked to run this step
*/

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
									SELECT	MAX(IF(hits.customDimensions.index=3, hits.customDimensions.value, NULL)) WITHIN HITS	AS VCID
											,visitId 																				AS SessionId
											,hits.hitNumber
											,TIMESTAMP(INTEGER(visitStartTime*1000000 + hits.time*1000))							AS HitTime
											,hits.appInfo.screenName 																AS Screen
											,hits.eventInfo.eventCategory
											,hits.eventInfo.eventAction
											,hits.eventInfo.eventLabel 																AS EventLabel
											,hits.appInfo.appVersion 																AS software_version
								  FROM		TABLE_DATE_RANGE([78413818.ga_sessions_],TIMESTAMP('2016-08-14'),TIMESTAMP('2016-08-14')) -- < PARAMETER
								)
								,VCID
							)
			where  	hitTime between timestamp('2016-08-14 00:00:00') and timestamp('2016-08-14 23:59:59') -- < PARAMETER
		) 	as base


-------------------------------------------------------------------------------------
-- A02 - Step 2, Encapsulating set of actions under relevant sessions (z_s2_YYYYMMDD)
-------------------------------------------------------------------------------------

/*
	Once we have identify the starting points of each session (on each day for every Viewing Card), we then look into what is the ending point of them all.
	
	This is done by the simple criteria of the start of session that starts after another will be considered the end of the former on-going session and so on. For example:
	
	Fullscreen-1	action 1
	Planner-1		action 10
	
	then that means that all actions until action # 9 will be part of the Fullscreen session 1 :
	
	Fullscreen-1	starts on action 1 ends on action 9
	Planner-1		starts on action 10 until ...
	
	
	NOTE: SQL Legacy is NOT ticked to run this step
	
*/


-- save this as Z_REF_YYYYMMDD...
select  *
		,coalesce	(
						min(from_) over (
											PARTITION BY  thedate
														  ,vcid
											ORDER BY      from_
											rows between  1 following and 1 following
										)-1
						,999999999999999999
					)   as to_
from	(
			select	vcid
					,thedate
					,step_1
					,cast(concat(cast(min(sessionid) as string),substr(concat('00000000',cast(min(hits_hitnumber) as string)),-8)) as int64)    as from_
			from  	TempStorage.z_s1_YYYYMMDD -- < PARAMETER
			group   by	vcid
						,thedate
						,step_1
		)   as base


-- Then the following...
select  base.thedate
        ,base.VCID 															as viewing_card
        ,base.SessionID
        ,base.hits_hitNumber 												as Actions_sequence
        ,base.HitTime 														as Timestamp_
        ,base.Screen
        ,base.hits_eventInfo_eventCategory  								as Action_Category
        ,base.hits_eventInfo_eventAction    								as Action
        ,ref_.step_1 														as Sky_Plus_session_grain
        ,base.step_1
		,base.EventLabel
		,base.software_version
		,base.next_timestamp
from    TempStorage.z_s1_YYYYMMDD as base -- < CHANGE HERE
        Left JOIN Z_REF_YYYYMMDD as ref_
        on  base.VCID  = ref_.VCID
        and base.thedate  = ref_.thedate
		and	cast(concat(cast(base.sessionid as string),substr(concat('00000000',cast(base.hits_hitnumber as string)),-8)) as int64) between ref_.from_ and ref_.to_
 


-------------------------------------------------
 -- A03 - Final, Minor key ETLs (z_final_YYYYMMDD)
 --------------------------------------------------
 
 /*
	This step is only here to calculate the seconds elapsed between the current action and the next one. which is equivalent to say how much time passed until another action was made.
	and to create the sky_plus_session variable which is a macro identifier of sessions for easier data manipulation/referencing
	
	NOTE: SQL Legacy is ticked to run this step
	
 */
 
 select  thedate
        ,viewing_card
		,software_version
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
from    Q_PA_Stage.z_s2_YYYYMMDD -- < PARAMETER