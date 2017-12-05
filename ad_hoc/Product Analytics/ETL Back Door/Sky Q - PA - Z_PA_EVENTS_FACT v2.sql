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
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Product Team
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        A new Z_PA_EVENTS_FACT
		
**VERSION: 
		
		2.1
		
**Sections:

		A - Data Preparation
			
**Running Time:

???

--------------------------------------------------------------------------------------------------------------

*/



-----------------------
-- A - Data Preparation
-----------------------

-- Parallelism
/* 
select	dk_asset_id
		,timems
		,dk_date
		,dk_time
		,dk_serial_number
		,dk_action_id
		,global_session_id
		,dk_previous
		,dk_current
		,dk_trigger_id
		,dk_referrer_id
		,asset_uuid
		,remote_type
		,trial
		,dk_channel_id
		,app_name
into	ground_20170711
from	pa_events_fact as a
where	dk_date = 20170711; -- > Parameter (Specific)
commit;
 */

--drop table z_pseudo;commit;
truncate table z_pseudo;commit;

insert	into z_pseudo
select	index_
		,dk_serial_number
		,case	substr(dk_serial_number,3,1) 
				when 'B' then 'Silver'
				when 'C' then 'Q'
				when 'D' then 'MR'
		end		as stb_type
		,timems
		,(TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp												as dt
		,date((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp)										as date_
		,datetime(to_char((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp,'YYYY-MM-DD HH24:00:00'))	as datehour_
		,case	when cast(datehour_ as time) between '00:00:00.000' and '05:59:59.000' then 'night'
				when cast(datehour_ as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
				when cast(datehour_ as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
				when cast(datehour_ as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
				when cast(datehour_ as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
				when cast(datehour_ as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
				when cast(datehour_ as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
		end		as part_of_day
		,extract(epoch from	dt -	( min(dt) over	(	-- Simply gets dt for the preceding action
														partition by	dk_serial_number
														order by		index_
														rows between	1 preceding and 1 preceding
													)
									))	as ss_elapsed_next_action
		,last_value(x3 ignore nulls) over	(	
												partition by	dk_date
																,dk_serial_number
												order by		index_
												rows between	200 preceding and current row
											)	as gn_lvl3_session_grain
		,last_value(x2 ignore nulls) over	(	
												partition by	dk_date
																,dk_serial_number
												order by		index_
												rows between	200 preceding and current row
											)	as gn_lvl3_session
		,last_value(y3 ignore nulls) over	(	
												partition by	dk_date
																,dk_serial_number
												order by		index_
												rows between	200 preceding and current row
											)	as gn_lvl2_session_grain
		,last_value(y2 ignore nulls) over	(	
												partition by	dk_date
																,dk_serial_number
												order by		index_
												rows between	200 preceding and current row
											)	as gn_lvl2_session
		,dk_action_id
		,dk_trigger_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_asset_id
		,asset_uuid
		,dk_channel_id
		,app_name
		,remote_type
		,trial
--into	z_pseudo
from	(
			select	x2||'-'||dense_rank() over	( partition by dk_date,dk_serial_number,x2 order by index_)	as x3
					,y2||'-'||dense_rank() over	( partition by dk_date,dk_serial_number,y2 order by index_)	as y3
					,*
			--into	step2
			from	(
						select	max(x) over	(
													partition by	dk_date
																	,dk_serial_number
													order by		index_
													rows between	1 preceding and 1 preceding
												)	as x1
								,case when x<>x1 then x else null end as x2
								,max(y) over	(
													partition by	dk_date
																	,dk_serial_number
													order by		index_
													rows between	1 preceding and 1 preceding
												)	as y1
								,case when y<>y1 then y else null end as y2
								,*
						from	(
									select	ref_screen.session_type
											,last_value(ref_screen.session_type ignore nulls) over	(
																										partition by	base.dk_date
																														,base.dk_serial_number
																										order by		base.index_
																										rows between	200 preceding and current row
																									)	as x
											,coalesce(ref_screen.SCREEN_PARENT,ref_screen.session_type) as the_screen_parent
											,last_value(the_screen_parent ignore nulls) over		(
																										partition by	base.dk_date
																														,base.dk_serial_number
																										order by		base.index_
																										rows between	200 preceding and current row
																									)	as y
											,base.*
									from	(
												select	row_number() over	(order by timems)	as	index_
														,dk_asset_id
														,timems
														,dk_date
														,dk_time
														,dk_serial_number
														,dk_action_id
														,global_session_id
														,case	when dk_previous in ('01400','N/A')	then	dk_referrer_id	--	GLobal navigation
																else										dk_previous
														end		as	theprevious
														,case	when	(
																			dk_referrer_id	like 'guide://ondemand/asset/EVOD%' or
																			dk_previous	like 'guide://ondemand/asset/EVOD%'
																		)														then	'Top Picks'		--	map EVOD referrer IDs to Top Picks (covers VOD on Top Picks only)
																when (dk_referrer_id is not null and dk_referrer_id <> 'N/A')	then	dk_referrer_id	--	accept dk_referrer_id if it is not null
																when dk_action_id in ('01001','01002') 							then	'Mini guide'	--	these actions (dismiss mini guide, mini guide browse channel) are synonymous with the Mini guide
																when dk_previous like '0%'										then	dk_referrer_id 	--	capture a bug where Json message doesn't contain ref field (rarely seen now)
																when instr(dk_previous,'"',1) > 0 								then	translate(substr (dk_previous,instr(dk_previous,'"',1)),'"','')	-- trim all before " and remove "
																else																	dk_previous		--	default to dk_previous (orig) since this is planned to replace dk_referrer_id (ref) in the future
														end		as	thelinkage	-- join onto z_pa_screen_dim_v2 using this
														,dk_previous
														,dk_current
														,dk_trigger_id
														,dk_referrer_id
														,asset_uuid
														,remote_type
														,trial
														,dk_channel_id
														,app_name
												from	pa_events_fact as a
												--from	ground_YYYYMMDD as a
												--where	dk_date = 20170309 -- > Parameter (Specific)
												where	dk_date =	(
																		select	cast(to_char(max(date_)+1,'YYYYMMDD') as integer)	as dk_Date
																		from	z_pa_events_fact_YYYYMM
																		--where	date_ between '2016-10-01' and '2016-10-31' --> Parameter (Extra Precision)
																	)	--> Paremeter (Dynamic)
											)	as base
											left join z_pa_screen_dim_v2	as	ref_screen -- pa_screen_dim
											on	base.thelinkage	=	ref_screen.PK_SCREEN_ID
											and	ref_screen.pk_screen_id	not like '0%'
								)	as step1
					)	as step2
		)	as step3
		inner join	pa_time_dim						as thetime
		on	step3.dk_time	= thetime.pk_time_dim
		inner join	pa_date_dim						as thedate
		on	step3.dk_Date	= thedate.date_pk
--order	by index_
;

commit;




--drop table z_pa_events_fact_YYYYMM;commit;
--truncate table z_pa_events_fact_YYYYMM;commit;

insert	into z_pa_events_fact_YYYYMM
select	index_
		,dk_serial_number
		,stb_type
		,timems
		,dt
		,date_
		,datehour_
		,part_of_day
		,ss_elapsed_next_action
		,y0 as session
		,last_value(w0 ignore nulls) over	(
												partition by	date_
																,dk_serial_number
												order by		index_
												rows between	200 preceding and current row
											)	as Session_grain -- as w
		,gn_lvl2_session
		,gn_lvl2_session_grain
		,gn_lvl3_session
		,gn_lvl3_session_grain
		,dk_action_id
		,dk_trigger_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_asset_id
		,asset_uuid
		,dk_channel_id
		,app_name
		,remote_type
		,trial
--into	z_pa_events_fact_YYYYMM
from	(
			select	*
					,x1||'-'||dense_rank() over (partition by date_,dk_serial_number,x1 order by index_) as w0
			from	(
						select	*
								,max(y0)  over	(
													partition by 	date_
																	,dk_serial_number
													order by		index_
													rows between	1 preceding and 1 preceding
												)	as z
								,case	when (z is null or z<>y0)	then x
										else null
								end		x1
						from	(
									select	*
											,case	when gn_lvl2_session in ('Home','Fullscreen')	then gn_lvl2_session 
													when gn_lvl2_session in (
																				'TV Guide'
																				,'Catch Up'
																				,'Recordings'
																				,'My Q'
																				,'Top Picks'
																				,'Sky Box Sets'
																				,'Sky Movies'
																				,'Sky Cinema'
																				,'Sky Store'
																				,'Sports'
																				,'Kids'
																				,'Music'
																				,'Online Videos'
																				,'Search'
																			)						then 'Home'
													when dk_action_id = 00003 						then 'Stand By Out'
													when dk_action_id = 00004						then 'Reboot'
													else null 
											end		as x
											,last_value(x ignore nulls) over	(
																					partition by	date_
																									,dk_serial_number
																					order by 		index_
																					rows between 	200 preceding and current row
																				) 	as  y0
									from	z_pseudo
								)	as base
					)	as	step_1
		)	as	step2;
commit;