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
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Products Team
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:


		
**Sections:

	A - Drafting Sky Q Journey foundational measures			
		A00 - Initialisation
		A01 - Creating Grains for Analysis (gn_lvl2_session_grain)
		A02 - Preparing data for Clustering
		A03 - Preparing data for analysing multiple TLMs 
			
**Iterations:

	V2:
		•	In Place. Trimming for only actions done by users (excluding automated actions).
		•	In Place. Trimming converted journeys to Seconds to Conversion (instead of looking into their whole length).
		•	In Progress. Plotting Quadrants.
		•	In Progress. Identifying a way to explain the optimum point.
		•	In Progress. Visuals for above allowing to compare TLMs.



--------------------------------------------------------------------------------------------------------------

*/


-----------------------
-- A00 - Initialisation
-----------------------

-- let's define what is legitimate to use and what do we treat as a bug in PA... (this is capping)

-------------------------------------------------------------
-- A01 - Creating Grains for Analysis (gn_lvl2_session_grain)
-------------------------------------------------------------
--drop table 	z_checks;commit;
truncate table	z_checks;commit;

insert	into into	z_checks
with	base as	(
					-- Isolating for the data/journeys that are required and no more...
					select	base_.index_
							,base_.date_
							,base_.dt
							,base_.ss_elapsed_next_action
							,base_.dk_serial_number
							,base_.gn_lvl2_session
							,base_.gn_lvl2_session_grain
							,base_.dk_Action_id
							,base_.dk_referrer_id
							,base_.dk_previous
							,base_.dk_current
							,base_.dk_trigger_id
							,base_.remote_type
					from	z_pa_events_fact	as base_
							inner join	(
											-- >>> CAPPING <<< Current Criteria (only journeys lasting up to 600 secs)...
											select	date_
													,dk_serial_number
													,gn_lvl2_session_grain
													,sum(ss_elapsed_next_action)	as journey_length_s
											from	z_pa_events_fact
											where	date_ between '2016-10-17' and '2016-10-23' --> Parameter
											and		gn_lvl2_session in	(
																			'TV Guide'
																			,'Catch Up'
																			,'Recordings'
																			,'My Q'
																			,'Top Picks'
																			,'Sky Box Sets'
																			,'Sky Movies'
																			,'Sky Store'
																			,'Sports'
																			,'Kids'
																			,'Music'
																			,'Online Videos'
																		)
											group	by	date_
														,dk_serial_number
														,gn_lvl2_session_grain
											having	journey_length_s <= 600
										)	as ref
							on	base_.date_					= ref.date_
							and	base_.dk_serial_number		= ref.dk_serial_number
							and	base_.gn_lvl2_session_grain	= ref.gn_lvl2_session_grain
				)
		,ref_conv as	(
							-- From above, identifying point of conversions...
							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	base
							group	by	date_
									,dk_serial_number
									,gn_lvl2_session_grain
							having	x is not null
						)
-- Describing Journeys (macro level KPIs)...
select	base.date_
		,case	substr(base.dk_serial_number,3,1) 
				when 'B' then 'Sky Q Silver'
				when 'C' then 'Sky Q Box'
				when 'D' then 'Sky Q Mini'
				else substr(base.dk_serial_number,3,1)
		end		as the_stb_type
		,base.gn_lvl2_session
		,base.date_||'-'||base.dk_Serial_number||'-'||base.gn_lvl2_session_grain							as grain
		,max(case when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as converted
		,count(1)																							as length_clicks
		,sum(base.ss_elapsed_next_action)																	as length_ss
		,sum(case when base.index_ <= ref_conv.x then base.ss_elapsed_next_action else null end)			as length_to_conv_ss
--into	z_checks
from	base
		left join	ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_Serial_number 		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
where	base.dk_trigger_id not in (' ','reboot-','system-') 	-- Isolating for concious actions performed by users... 'timeOut-' is still desirable as is the user consciously inactivity period
group	by	base.date_
			,the_stb_type
			,base.gn_lvl2_session
			,grain;
commit;



-- Verifying Journeys distribution through Length (Seconds)...


-- [1] For full length...
-- [CHECK VISUALISED IN TABLELAU]
select	converted
		,floor (cast(length_ss as float)/10) * 10	as journeys_length
		,count(1) as njourneys
from	z_checks
group	by	converted
			,journeys_length
			
			
-- [2] Now Considering time to conversion (converted journeys up to time to conversion and unconverted journeys for full length)
-- [CHECK VISUALISED IN TABLELAU]
select	converted
		,floor (cast(coalesce(length_to_conv_ss,length_ss) as float)/10) * 10	as journeys_length_2
		,count(1) as njourneys
from	z_checks
group	by	converted
			,journeys_length_2

-- [3] Now considering same context as [2], but broken down by TLMs (MIND AT THIS POINT IS NOT HOME PAGE PERFORMANCE)...
-- [CHECK VISUALISED IN TABLELAU]
select	gn_lvl2_session	as sky_q_tlms
		,converted
		,floor (cast(coalesce(length_to_conv_ss,length_ss) as float)/10) * 10	as journeys_length_2
		,count(1) as njourneys
from	z_checks
group	by	sky_q_tlms
			,converted
			,journeys_length_2




/* -- 1) identify where the 90% of the population is (filtering on tales, first round against outliers which are treated actually as bugs)...
drop table z_cut;commit;

select	week_part
		,gn_lvl2_session
		,converted
		,max(length_ss) as first_cut -- this is the max length we will admit for each session in accordance with the 0.9 rule...
into	z_cut
from	(
			select	*
					,round	(
								sum(the_prop) over	(
														partition by	week_part
																		,gn_lvl2_session
																		,converted 
														order by 		length_ss 
														rows between 	unbounded preceding and current row 
													) 	
								,3
							)	as x
			from	(
						select	week_part
								,gn_lvl2_session
								,converted
								,length_ss
								,count(distinct grain) 																			as nobs
								,cast(nobs as float) / cast((sum(nobs) over(partition by week_part,gn_lvl2_session,converted)) as float)	as the_prop
						from	z_checks
						where	length_ss is not null
						group	by	week_part
									,gn_lvl2_session
									,converted
									,length_ss
					)	as base
		)	as step1
where	x <=0.9 --< Arbitrary Cut
group	by	week_part
			,gn_lvl2_session
			,converted;
commit; */

/* -- 2) what is the AVG behaviour for Converted (both on Steps and Time)...

drop table z_the_avg;commit;

select	base.week_part
		,base.gn_lvl2_session
		,base.converted
		,avg(coalesce(base.length_to_conv_ss,base.length_ss))	as avg_time_spent
into	z_the_avg
from 	z_checks 			as base
		inner join z_cut	as ref
		on	base.week_part  		= ref.week_part
		and	base.gn_lvl2_session	= ref.gn_lvl2_session
		and	base.converted 			= ref.converted
where	base.length_ss <= ref.first_cut
group	by	base.week_part
			,base.gn_lvl2_session
			,base.converted;
			
commit; */

/* -- 3) Comparing the AVG against the Median...

drop table z_the_median;commit;

with 	step1 as	(
						select	base.WEEK_PART
								,base.gn_lvl2_session
								,base.converted
								--,base.LENGTH_SS
								,coalesce(base.length_to_conv_ss, base.LENGTH_SS) as time_spent
								,row_number() over	(
														partition by	base.week_part
																		,base.GN_LVL2_SESSION
																		,base.converted
														order by		time_spent
													)	as	index_
						from 	z_checks as base
								inner join z_cut as ref
								on	base.week_part  		= ref.week_part
								and	base.gn_lvl2_session	= ref.gn_lvl2_session
								and	base.converted 			= ref.converted
						where	base.length_ss <= ref.first_cut
					)
		,ref as 	(
						select	week_part
								,gn_lvl2_session
								,converted
								,max(index_)/2	as mid_index
						from	step1
						group	by	week_part
									,gn_lvl2_session
									,converted
					)
select	step1.week_part
		,step1.gn_lvl2_session
		,step1.converted
		,step1.time_spent as the_median
into	z_the_median
from	step1
		inner join ref
		on	step1.week_part 		= ref.week_part
		and	step1.gn_lvl2_session	= ref.gn_lvl2_session
		and step1.converted 		= ref.converted
		and	step1.index_ 			= ref.mid_index;
		
commit; */
					
/*
	After this we can do predictive analytics
	check out:

		https://www.coursera.org/learn/business-analytics-decision-making/lecture/pGFT5/4-cluster-analysis-with-excel	
*/



----------------------------
-- A04 - Extracts in Tableau
----------------------------

-- Extract 1
select	base.week_part
		,base.GN_LVL2_SESSION
		,base.CONVERTED
		,coalesce(base.length_to_conv_ss,base.LENGTH_SS) as time_spent
		,count(distinct base.GRAIN) as nrows
from 	z_checks as base
		inner join z_cut as ref
		on	base.week_part  		= ref.week_part
		and	base.gn_lvl2_session	= ref.gn_lvl2_session
		and	base.converted 			= ref.converted
where	base.length_ss <= ref.first_cut
group	by	base.week_part
			,base.GN_LVL2_SESSION
			,base.CONVERTED
			,time_spent


-- Extract 2
select	ground.week_part
		,ground.GN_LVL2_SESSION
		,ground.CONVERTED
		,ground.time_spent
		--,c.the_max_length
		,ground.x
		,case	when ground.time_spent = a.the_median 			then 'Median'
				when ground.time_spent = round(b.avg_time_spent,0)	then 'Avg'
				else null
		end		stat
from	(
			select	*
					,round	(
								sum(the_prop) over	(
														partition by	week_part
																		,gn_lvl2_session
																		,converted
														order by 		time_spent
														rows between 	unbounded preceding and current row
													)
								,3
							)	as x
			from	(
						select	base.week_part
								,base.GN_LVL2_SESSION
								,base.CONVERTED
								,coalesce(base.length_to_conv_ss,base.LENGTH_SS) 	as time_spent
								,count(distinct base.GRAIN) 						as ngrains
								,cast(ngrains as float)/cast((sum(ngrains)over(partition by base.week_part,base.GN_LVL2_SESSION,base.CONVERTED)) as float) as the_prop
						from 	z_checks as base
								inner join z_cut as ref
								on	base.week_part  		= ref.week_part
								and	base.gn_lvl2_session	= ref.gn_lvl2_session
								and	base.converted 			= ref.converted
						where	base.length_ss <= ref.first_cut
						group	by	base.week_part
									,base.GN_LVL2_SESSION
									,base.CONVERTED
									,time_spent
					)	as step1
		)	as ground
		inner join	z_the_median	as a
		on	ground.week_part 		= a.week_part
		and	ground.gn_lvl2_session 	= a.gn_lvl2_session
		and ground.converted 		= a.converted
		inner join	z_the_avg		as b
		on	ground.week_part 		= b.week_part
		and	ground.gn_lvl2_session 	= b.gn_lvl2_session
		and ground.converted 		= b.converted
		--inner join	(
						--select	base.week_part
								--,base.gn_lvl2_session
								--,base.converted
								--,max(coalesce(length_to_conv_ss,length_ss)	as the_max_length
						--from 	z_checks as base
								--inner join z_cut as ref
								--on	base.week_part  		= ref.week_part
								--and	base.gn_lvl2_session	= ref.gn_lvl2_session
								--and	base.converted 			= ref.converted
						--where	base.length_ss <= ref.first_cut
						--group	by	base.week_part
									--,base.gn_lvl2_session
									--,base.converted
					--)	as c
		--on	ground.week_part 		= c.week_part
		--and	ground.gn_lvl2_session 	= c.gn_lvl2_session
		--and ground.converted 		= c.converted
where	(
			ground.time_spent = a.the_median
			or
			ground.time_spent = round(b.avg_time_spent,0)
		)



/*
	interesting query
	
	This one allows to see with how much of the records we end up working with and how much of the tail we are cutting off...
	
*/


select	week_part
		,gn_lvl2_session
		,converted
		,count(1) as z
		,sum(Case when x<=0.9 then 1 else 0 end) as w
		,cast(w as float) / cast(z as float) as final_
from	(
			select	*
					,round	(
								sum(the_prop) over	(
														partition by	week_part
																		,gn_lvl2_session
																		,converted 
														order by 		length_ss 
														rows between 	unbounded preceding and current row 
													) 	
								,3
							)	as x
			from	(
						select	week_part
								,gn_lvl2_session
								,converted
								,length_ss
								,count(distinct grain) 																			as nobs
								,cast(nobs as float) / cast((sum(nobs) over(partition by week_part,gn_lvl2_session,converted)) as float)	as the_prop
						from	z_checks
						where	length_ss is not null
						group	by	week_part
									,gn_lvl2_session
									,converted
									,length_ss
					)	as base
		)	as step1
group	by	week_part
			,gn_lvl2_session
			,converted
			
---------------------------
-- A99 getting a sample out
---------------------------

/*
	getting journeys both converted and exited that lasted between 100 to 170 seconds 
	(the converted ones considering only time to conversion rather their full length)
*/


create table z_journeys (x integer,date_ date, dk_serial_number varchar(17), gn_lvl2_Session_Grain varchar(20) ); commit;
truncate table z_journeys;commit;


insert into z_journeys values (0,'2016-10-20','32D0010487213927','Sky Movies-2');commit;
insert into z_journeys values (0,'2016-10-20','32D0030487357182','Sky Movies-6');commit;
insert into z_journeys values (0,'2016-10-18','32B0610488359317','Sky Movies-2');commit;
insert into z_journeys values (0,'2016-10-22','32B0580488176373','Sky Movies-6');commit;
insert into z_journeys values (0,'2016-10-18','32B0560488036711','Sky Movies-3');commit;
insert into z_journeys values (0,'2016-10-20','32B0570488071467','Sky Movies-8');commit;
insert into z_journeys values (0,'2016-10-21','32B0570488146643','Sky Movies-2');commit;
insert into z_journeys values (0,'2016-10-23','32D0030487352706','Sky Movies-1');commit;
insert into z_journeys values (0,'2016-10-22','32B0580488185708','Sky Movies-7');commit;
insert into z_journeys values (0,'2016-10-23','32B0570488075547','Sky Movies-9');commit;
insert into z_journeys values (0,'2016-10-19','32D0010487192000','Sky Movies-1');commit;
insert into z_journeys values (0,'2016-10-22','32D0010487148862','Sky Movies-3');commit;
insert into z_journeys values (0,'2016-10-23','32B0570488134752','Sky Movies-4');commit;
insert into z_journeys values (0,'2016-10-21','32B0580488217164','Sky Movies-6');commit;
insert into z_journeys values (0,'2016-10-22','32B0590488323232','Sky Movies-3');commit;
insert into z_journeys values (0,'2016-10-18','32B0560488042384','Sky Movies-1');commit;
insert into z_journeys values (0,'2016-10-23','32B0560488008318','Sky Movies-4');commit;
insert into z_journeys values (0,'2016-10-22','32B0560488035575','Sky Movies-4');commit;
insert into z_journeys values (0,'2016-10-18','32B0620488499413','Sky Movies-2');commit;
insert into z_journeys values (0,'2016-10-23','32B0580488220996','Sky Movies-4');commit;
insert into z_journeys values (1,'2016-10-18','32B0570488090213','Sky Movies-10');commit;
insert into z_journeys values (1,'2016-10-22','32B0570488100999','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-18','32D0010487040380','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-20','32B0610488411168','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-19','32B0580488178752','Sky Movies-3');commit;
insert into z_journeys values (1,'2016-10-22','32B0570488146260','Sky Movies-2');commit;
insert into z_journeys values (1,'2016-10-21','32D0030487467277','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-23','32B0570488093277','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-22','32B0570488070825','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-18','32B0580488175984','Sky Movies-4');commit;
insert into z_journeys values (1,'2016-10-17','32B0590488326722','Sky Movies-6');commit;
insert into z_journeys values (1,'2016-10-17','32B0570488123226','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-23','32B0590488322106','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-19','32B0570488132458','Sky Movies-4');commit;
insert into z_journeys values (1,'2016-10-22','32B0560488018398','Sky Movies-3');commit;
insert into z_journeys values (1,'2016-10-22','32D0010487107744','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-21','32D0010487146224','Sky Movies-9');commit;
insert into z_journeys values (1,'2016-10-21','32B0620488427452','Sky Movies-9');commit;
insert into z_journeys values (1,'2016-10-17','32D0030487471759','Sky Movies-1');commit;
insert into z_journeys values (1,'2016-10-22','32B0570488094137','Sky Movies-3');commit;

truncate table	z_working_sample;commit;

insert	into z_working_sample
select	ref.x
		,x.*
--into	z_working_sample
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
order	by	x.date_
			,x.dk_Serial_number
			,x.index_
