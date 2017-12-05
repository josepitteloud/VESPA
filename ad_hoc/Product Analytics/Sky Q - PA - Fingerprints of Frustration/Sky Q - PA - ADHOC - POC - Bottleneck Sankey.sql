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
**Project Name:							FINGERPRINTS FOR FURSTRATION
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Daniel Chronnel
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        We are after comprehending what converted / void journeys navigation pattern is so we can:
		
			1) where are users mainly spending their time in the UI (e.g. back and forth between TLMs and SLMS? scrolling around catalogues?)
			2) Define Conversion target for TLMs (Highly related to personalisation)
			3) Define Navigation time spent per TLM (Highly related to personalisation)
		
**Sections:

		A - ETL (Data Understanding, Validation and Preparation)
			0 - Preparing the Data (Base data ETLs)
			1 - Preparing Dimensions (Dims ETLs)
			2 - Sankey Logic assemble
		
		B - Analysis (More Data Understanding)
			3 - Journeys Intra Stats
			
		APPENDINX
			
**Running Time:

??

--------------------------------------------------------------------------------------------------------------

*/


/*
	Here I'm preparing the data to start the sankey diagram to x ray journeys, find common behaviour and colour that to show where is that time spent
*/



------------------------------------------------------

------------------------------------------
-- 0 - Preparing the Data (Base data ETLs)
------------------------------------------
truncate table z_journey_globnav;commit;

insert	into z_journey_globnav
with	step1 as	(
						select	date_ 				as thedate
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,index_
								,dk_action_id
								,min(dk_action_id) over	(
															partition by	date_
																			,dk_serial_number
																			,gn_lvl2_session
																			,gn_lvl2_session_grain
															order by		index_
															rows between 	1 following and 1 following
														)	as next_action
								,ss_before_next_action	as time_to_action
								,dt
								,dk_previous
								,dk_current
								,dk_referrer_id
								,dk_trigger_id
						from	z_working_sample -- this table is the output from home session logic lvl 2
						--where	date_ = '2016-10-17'
						--and		dk_serial_number = '32B0590488326722'
					)
		,ref as 	(
						select	date_
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,max(case when dk_action_id in(02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
						from	z_working_sample
						--where	date_ = '2016-10-17 00:00:00'
						--and		dk_serial_number = '32B0590488326722'
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session
									,gn_lvl2_session_grain
					)
select	step1.*
		,dense_rank() over	(
								partition by	step1.thedate
												,step1.dk_serial_number
												,step1.gn_lvl2_session
												,step1.gn_lvl2_session_grain
								order by		step1.index_
							)	as journey_lvl
		,ref.conv_flag
--into	z_journey_globnav
from	step1
		inner join ref
		on	step1.thedate 				= ref.date_
		and	step1.dk_serial_number		= ref.dk_serial_number
		and	step1.gn_lvl2_session_grain	= ref.gn_lvl2_session_grain
where	step1.dk_action_id = 01400;
commit;


truncate table z_journey_cube; commit;

insert	into z_journey_cube
select	thedate
		,conv_flag
		,gn_lvl2_session
		,journey_lvl
		,dk_action_id
		,next_action
		,dk_previous
		,dk_current
		,count(1) 												as freq
		,count(distinct dk_serial_number) 						as nboxes
		,sum(time_to_action)									as tot_ss_to_action
		,cast(tot_ss_to_action as float)/cast(freq as float)	as avg_ss_to_action
		,cast(tot_ss_to_action as float)/cast(nboxes as float)	as avg_sstoaction_x_stb
from	z_journey_globnav	as step2
group	by	thedate
			,conv_flag
			,gn_lvl2_session
			,journey_lvl
			,dk_action_id
			,next_action
			,dk_previous
			,dk_current;
commit;

---------------------------------------
-- 1 - Preparing Dimensions (Dims ETLs)
---------------------------------------

-- UPGRADE BELOW WITH...
/*
select	'/Sky Cinema/Movie Genres/Action'	as y -->>>> I'm currently misclassifying these URIs as SLMs when they are really SSLMs...
		,length(y) - length(translate(y,'/','')) as z
		,case 	when substr(y,1,1) = '/' and (length(y) - length(translate(y,'/','')))>2	then 'SSLM' 
				when substr(y,1,1) = '/' and (length(y) - length(translate(y,'/','')))>1	then 'SLM'
				else y
		end		as x
*/

truncate table z_journey_uri_dim;commit;

insert	into z_journey_uri_dim
select	base.*
		,case	when instr(the_uri,'"',1) > 0 								then	translate(substr (the_uri,instr(the_uri,'"',1)),'"','')	-- trim all before " and remove "
				when	(
							the_uri	like 'guide://ondemand/asset/EVOD%' or
							the_uri	like 'guide://ondemand/asset/EVOD%'
						)													then	'Top Picks'	--	map EVOD referrer IDs to Top Picks (covers VOD on Top Picks only)
				when (the_uri is not null and the_uri <> 'N/A')				then	the_uri		--	accept dk_referrer_id if it is not null
				else														the_uri				--	default to dk_previous (orig) since this is planned to replace dk_referrer_id (ref) in the future
		end		as	thelinkage	-- join onto z_pa_screen_dim_v2 using this
		,coalesce(ref.screen_name,thelinkage) as y
		,case	when y in ('Home','Fullscreen')									then 0
				when ref.session_type is not null and ref.screen_parent is null	then 1
				when base.the_uri like '%/classification%'						then 2
				when base.the_uri like '%/asset%'								then 3
				else															99
		end		as screen_levels
		,row_number() over	( order by screen_levels, y) as scale
--into	z_journey_uri_dim
from	(
			select	distinct
					dk_previous as the_uri
			from	z_journey_cube
			union
			select	distinct
					dk_current  as the_uri
			from	z_journey_cube
		)	as base
		left join z_pa_screen_dim_v2 as ref
		on	base.the_uri = ref.PK_SCREEN_ID;
commit;

----------------------------
-- 2 - Sankey Logic assemble
----------------------------

truncate table	z_journey_plot;commit;

insert	into z_journey_plot
select	base.conv_flag
		,uri_orig.scale									as orig
		,uri_orig.y										as orig_name
		,uri_dest.scale									as dest
		,uri_dest.y										as dest_name
		,expol.instance
		,((expol.instance-25.00)/4.00) 					as sigmoid_p
		,case	when 	expol.instance = 1 and base.journey_lvl > 1 then (sigmoid_p + ((-6+(6*base.journey_lvl))*2))+0.001
				else	sigmoid_p + ((-6+(6*base.journey_lvl))*2)
		end		as stretch
		,dest+((orig-dest)*(1/(1+EXP(1)^+sigmoid_p)))	as the_curve
		,base.thedate
		,base.gn_lvl2_session
		,base.journey_lvl
		,base.dk_action_id
		,base.next_action
		,base.dk_previous
		,base.dk_current
		,base.freq
		,base.nboxes
		,base.tot_ss_to_action
		,base.avg_ss_to_action
		,base.avg_sstoaction_x_stb
		,uri_orig.screen_levels
--into	z_journey_plot
from	z_journey_cube								as base
		cross join	table(system..ROWEXPAND(49))	as expol
		inner join	z_journey_uri_dim				as uri_orig
		on	base.dk_previous	= uri_orig.the_uri
		inner join	z_journey_uri_dim				as uri_dest
		on	base.dk_current		= uri_dest.the_uri
order	by	base.conv_flag
			,orig
			,dest
			,instance;
commit;

---------------------------
-- 3 - Journeys Intra Stats
---------------------------

------------------------------------------------------

------------
-- APPENDINX
------------

-- recreating the table containing the journeys I've sent to DanC originally...
/*
select	ref.x
		,x.*
into	z_working_sample
from	(
			select	a.index_
					,a.date_
					,dt
					,case 	extract(dow from date(a.date_))
							when 1 then 'we'
							when 2 then 'wd'
							when 3 then 'wd'
							when 4 then 'wd'
							when 5 then 'wd'
							when 6 then 'wd'
							when 7 then 'we'
					end		week_part
					,extract(epoch from (min(dt) over (partition by a.date_,a.dk_serial_number order by a.index_ rows between 1 following and 1 following))- dt) as ss_before_next_action
					,a.dk_serial_number
					,gn_lvl2_session
					,gn_lvl2_session_grain
					,dk_Action_id
					,dk_referrer_id
					,dk_previous
					,dk_current
					,dk_trigger_id
			from	z_pa_events_fact as a
					inner join	(
									select	distinct
											date_
											,dk_Serial_number
									from	z_journeys
								)	as b
					on	a.date_ = b.date_
					and	a.dk_Serial_number = b.dk_Serial_number
		)	as x
		inner join	z_journeys as ref
		on	x.date_ = ref.date_
		and	x.dk_Serial_number = ref.dk_Serial_number
		and	x.gn_lvl2_session_grain = ref.gn_lvl2_Session_grain
commit
*/