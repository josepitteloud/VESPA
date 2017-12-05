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
**Project Name:							PRODUCTS ANALYTICS (PA)
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Stakeholder:                          Product Team
**Due Date:                             05/02/2016
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

		We previously had 'Recently Viewed' as a Lvl 3 area, but decided to incorporate this with Mini Guide as the data had not been validated.

		We'd now like to get visibility of when Recently Viewed was invoked and actioned, and separate it as an individual Lvl 3 area.

		What we are after:

		+ How many times the user invoked it
		+ How many times they dismissed
		+ How many times they action on it (always a channel tune)

		Key Measures:

		# ratios of dismissals
		# ratios of engagement	

**Sections:
	
	A - Understanding what available for Recently Viewed
	B - Reach
	C - Engagement (N Days)
		
--------------------------------------------------------------------------------------------------------------

*/

-------------------------------------------------------
-- A - Understanding what available for Recently Viewed
-------------------------------------------------------

select	hits
		,count(distinct session) as n_sessions
from	(
			select	date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain as session
					,count(1) as hits
			from	z_pa_events_fact
			where	dk_referrer_id in ('mostRecent','MostRecent','Recently Viewed','recentlyViewed')
			and		date_ between '2016-09-01' and '2016-09-30'
			group	by	session
		)	as base
group	by	hits

-- How many actions do we see on each journey in which RECENTLYVIEWED is present

select	n_actions
		,count(Distinct the_session) as n_sessions
from	(
			select	base.date_||'-'||base.dk_serial_number||'-'||base.session as the_session
					,count(base.dk_Action_id) as n_actions
			from	(
						select	distinct
								date_
								,dk_serial_number
								,gn_lvl2_session_grain as session
								,dk_action_id
								,index_
						from	z_pa_events_fact	as base
						where	date_ between '2016-09-01' and '2016-09-30'
					)	as base
					inner join	(
									-- This identifies sessions in which recently Viewed has been present
									select	date_
											,dk_serial_number
											,gn_lvl2_session_grain as session
									from	z_pa_events_fact
									where	dk_referrer_id in ('mostRecent','MostRecent','Recently Viewed','recentlyViewed')
									and		date_ between '2016-09-01' and '2016-09-30'
									group	by	datE_
												,dk_serial_number
												,gn_lvl2_session_grain
								)	as ref
					on	base.date_				=ref.date_
					and	base.dk_serial_number	=ref.dk_serial_number
					and	base.session			=ref.session
			group	by	the_session
		)	as step1
group	by	n_actions


-- on those sessions of lenght = 1 what are the actions taking place there (my assumption is that I should see, amongst others, dismisses)


select	step1.dk_action_id
		,count(distinct step1.date_||'-'||step1.dk_serial_number||'-'||step1.session) as n_Sessions
from	(
			select	date_
					,dk_serial_number
					,gn_lvl2_session_grain as session
					,dk_action_id
					,index_
			from	z_pa_events_fact	as base
			where	date_ between '2016-09-01' and '2016-09-30'
		)	as step1
		inner join	(
						-- all RECENTLYVIEWED sessions of 1 action length...
						select	base.date_
								,base.dk_serial_number
								,base.session as the_session
								,count(base.dk_Action_id) as y
						from	(
									select	distinct
											date_
											,dk_serial_number
											,gn_lvl2_session_grain as session
											,dk_action_id
											,index_
									from	z_pa_events_fact	as base
									where	date_ between '2016-09-01' and '2016-09-30'
								)	as base
								inner join	(
												-- This identifies sessions in which recently Viewed has been present
												select	date_
														,dk_serial_number
														,gn_lvl2_session_grain as session
												from	z_pa_events_fact
												where	dk_referrer_id in ('mostRecent','MostRecent','Recently Viewed','recentlyViewed')
												and		date_ between '2016-09-01' and '2016-09-30'
												group	by	datE_
															,dk_serial_number
															,gn_lvl2_session_grain
											)	as ref
								on	base.date_				=ref.date_
								and	base.dk_serial_number	=ref.dk_serial_number
								and	base.session			=ref.session
						group	by	base.date_
									,base.dk_serial_number
									,the_session
						having	y =1
					)	as the_ref
		on	step1.date_				=the_ref.date_
		and	step1.dk_serial_number	=the_ref.dk_serial_number
		and	step1.session			=the_ref.the_session
group	by	step1.dk_action_id


/*-------------------------------------------------------------------------------------------------------------------------------------
	So because we don't have a clear view of when the journey begins and or end... not sure yet on whether dismissals are been captured
	as well as opening the Recently Viewed Miniguide...
	
	The current KPIs we can deliver on are:
	
	Reach 
	Freq
	Engagement (n Days)
	
	All of the above ONLY for when an interaction took place... 
	
*/-------------------------------------------------------------------------------------------------------------------------------------


------------
-- B - Reach
------------


select	extract(month from date_)	as the_month
		,case	substr(dk_serial_number,3,1)
				when 'B' then 'Sky Q Silver'
				when 'C' then 'Sky Q Box'
				when 'D' then 'Sky Q Mini'
		end		as the_stb_type
		,count(distinct dk_Serial_number) as tot_active_pop
		,count(distinct (case when dk_referrer_id in ('mostRecent','MostRecent','Recently Viewed','recentlyViewed') then dk_serial_number else null end)) as reach
from	z_pa_events_fact
where	date_ >= '2016-07-01'
group	by	the_month
			,the_stb_type


			
--------------------------
-- C - Engagement (N Days)
--------------------------


select	the_month
		,the_stb_type
		,ndays
		,count(distinct dk_serial_number)	as reach
from	(
			select	extract(month from date_)	as the_month
					,case	substr(dk_serial_number,3,1)
							when 'B' then 'Sky Q Silver'
							when 'C' then 'Sky Q Box'
							when 'D' then 'Sky Q Mini'
					end		as the_stb_type
					,dk_serial_number
					,count(distinct date_)	as ndays
			from	z_pa_events_fact
			where	dk_referrer_id in ('mostRecent','MostRecent','Recently Viewed','recentlyViewed')
			and		date_ >= '2016-07-01'
			group	by	the_month
						,the_stb_type
						,dk_serial_number
		)	as base
group	by	the_month
			,the_stb_type
			,ndays