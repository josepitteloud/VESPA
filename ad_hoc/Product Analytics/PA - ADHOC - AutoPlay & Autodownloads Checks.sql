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
**Project Name:							PA - ADHOC - AutoPlay & Autodownloads Checks
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Products Team
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        We are measuring here the level of engagement Sky Q population is having around both Auto-Playback and Auto-Download features
		
**Sections:

		A - Drafting Auto-Download & Auto-Playback Measures
			A00 - Pre-requisites
			A01 - Generating Metrics
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/

-----------------------
-- A00 - Pre-requisites
-----------------------

/*

	Here I was understanding how to isolate playbacks and downloads from our pool of data that were automatically triggered from the manual ones
	
	select	dk_Action_id
			,dk_referrer_id
			,count(1) as hits
	from 	z_pa_events_fact 
	where	dk_action_id in (03000,02400)
	and		(lower(dk_referrer_id) not like '%guide://interim%'
			and
			lower(dk_referrer_id) not like '%guide://ondemand%'
			and
			lower(dk_referrer_id) not like '%guide://thumbnailinterim%'
			)
	group	by	dk_Action_id
				,dk_referrer_id


	select	dk_Action_id
			,dk_referrer_id
			,count(1) as his
	from	z_pa_events_Fact
	where	dk_referrer_id in ('autoplay','SERIES_DOWNLOAD_SERVICE')
	group	by	dk_Action_id
				,dk_referrer_id
*/
			
		
---------------------------
-- A01 - Generating Metrics
---------------------------		
select	base.year_
		,base.week_number
		,base.Q_Sessions
		,base.Referrer
		,base.dk_Action_id
		,max(ref.n_boxes_weekly) 	as weekly_n_boxes
		,max(ref.n_boxes_dp_weekly)	as weekly_n_boxes_acting
		,max(hits_)		as hits
		,max(n_boxes_)	as n_boxes
		,max(n_dates_)	as n_dates
from	(		
			select	extract(year from date(date_))	as year_
					,extract(week from date(date_))	as week_number
					,case 	when dk_action_id = 03000 and dk_referrer_id in ('referrer','autoplay')	then 'Fullscreen' 
							when dk_action_id = 03000 and dk_referrer_id = 'guide://series'			then 'Recordings'
							else gn_lvl2_session 
					end 	as Q_Sessions
					,case 	when dk_action_id = 03000 and dk_referrer_id = 'autoplay' 								then 'Auto-Playback'
							when dk_action_id = 02400 and dk_referrer_id in ('autoplay','SERIES_DOWNLOAD_SERVICE')	then 'Auto-Download'
							else 'Other Areas'
					end		as Referrer
					,dk_Action_id
					,count(1) 							as hits_
					,count(distinct dk_serial_number) 	as n_boxes_
					,count(distinct date_)				as n_dates_
			from	z_pa_events_Fact	as base
			where	dk_action_id in (03000,02400)
			and		date_ >= '2016-07-21 00:00:00'
			group	by	year_
						,week_number
						,Q_Sessions
						,Referrer
						,dk_Action_id
		)	as base
		inner join	(
						select	extract(year from date(date_))																	as year_
								,extract(week from date(date_))																	as week_number
								,count(distinct dk_serial_number) 																as n_boxes_weekly
								,count(distinct (case when dk_action_id in (03000,02400) then dk_serial_number else null end))	as n_boxes_dp_weekly
						from	z_pa_events_fact
						group	by	year_
									,week_number
					)	as ref
		on	base.year_ 			= ref.year_
		and	base.week_number	= ref.week_number
group	by	base.year_
			,base.week_number
			,base.Q_Sessions
			,base.Referrer
			,base.dk_Action_id
			
			
			
--------------- FURTHER CHECKS

 -- DECILES ROUND 1
select	year_
		,week_number
		,dk_action_id
		,deciles
		,min(hits)				as the_start
		,max(hits) 				as the_end
		,count(distinct hits)	as the_range
		,sum(nboxes) 			as tot_stb_per_bucket
		,sum(tot_stb_per_bucket) over	(
											partition by	year_
															,week_number
															,dk_action_id
										)	as tot_stbs
		,cast(tot_stb_per_bucket as float)/cast(tot_stbs as float)	as the_prop
from	(
			select	year_
					,week_number
					,dk_action_id
					,hits
					,count(distinct dk_serial_number) 	as nboxes
					,ntile(20) over	(
										partition by	year_
														,week_number
														,dk_action_id 
										order by 		hits 
									)	as deciles
			from	(
						select	extract(year from date(date_))	as year_
								,extract(week from date(date_))	as week_number
								,dk_serial_number
								,dk_action_id
								,count(1) as hits
						from	z_pa_events_Fact
						where	dk_referrer_id in ('autoplay','SERIES_DOWNLOAD_SERVICE')
						and		date_ >= '2016-07-21 00:00:00'
						and		dk_action_id in (02400,03000)
						and		gn_lvl2_session in	(
														'Catch Up'
														,'Fullscreen'
														,'Kids'
														,'Mini Guide'
														,'Music'
														,'My Q'
														,'Online Videos'
														,'Recordings'
														,'Search'
														,'Sky Box Sets'
														,'Sky Movies'
														,'Sky Store'
														,'Sports'
														,'Top Picks'
														,'TV Guide'
													)
						group	by	year_
									,week_number
									,dk_serial_number
									,dk_action_id
					)	as base
			group	by	year_
						,week_number
						,dk_action_id
						,hits
		)	as base1
group	by	year_
			,week_number
			,dk_action_id
			,deciles