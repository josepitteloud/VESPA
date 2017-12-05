 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	                                                          
**Business Brief:

	This Script goal is to generate metrics to compare the performance of the process vs. BARB 

**Sections:
	
	M12: Validation Process 
			M12.0 - Initialising Environment
			M12.1 - Slicing for weighted duration (Skyview)
			
		
--------------------------------------------------------------------------------------------------------------
*/
	
-----------------------------------	
-- M12.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m12_validation
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M12.0 - Initialising Environment' TO CLIENT
		   
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building Stage1 table from M07' TO CLIENT
	
	if object_id('stage1') is not null
		drop table stage1

	commit
					
	select  'VESPA'                     as source
			,date(m07.event_start_utc)  as scaling_date
			,ska.channel_name
			,ska.channel_pack
			,trim(m07.session_daypart)  as daypart
			,m07.event_id
			,m07.overlap_batch
			,coalesce(m07.chunk_duration_seg,m07.event_duration_seg) as duration_seg
			,m07.account_number
	into    stage1
	from    V289_M07_dp_data                        as m07 --  11158541
			inner join  v289_m06_dp_raw_data        as m06 -- <-- we need this guy to get the service_key to then get the channel name
			on  m07.event_id    = m06.pk_viewing_prog_instance_fact
			left join vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES    as ska
			on	m06.service_key = ska.service_key
			and m07.event_start_utc between ska.EFFECTIVE_FROM and ska.EFFECTIVE_TO

	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Stage1 Table DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building Overlaps_Side table' TO CLIENT
	
	if object_id('Overlaps_side') is not null
		drop table Overlaps_side

	commit

	select  s1.*
			,m10.hh_person_number
			,m10.person_ageband as age
			,case   when m10.person_gender = 'F' then 'Female'
					when m10.person_gender = 'M' then 'Male'
					else 'Undefined'
			end     as gender
			,m11.weight
	into    Overlaps_side -- 222862 row(s) affected
	from    stage1 as s1
			inner join V289_M10_session_individuals as m10 -- <-- we need this table to get the persons ids to get the scaling weights later on
			on  s1.event_id    		= m10.event_id
			and	s1.account_number	= m10.account_number
			and s1.overlap_batch   	= m10.overlap_batch 
			inner join  (
							select  distinct
									account_number
									,hh_person_number
									,scaling_date
									,scaling_weighting  as weight
							from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
						)   AS M11
			on  m10.account_number      = m11.account_number
			and m10.hh_person_number    = m11.hh_person_number
			and s1.scaling_date         = m11.scaling_date 

	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: Overlaps_Side table DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building No_Overlaps_Side table' TO CLIENT
	
	if object_id('no_Overlaps_side') is not null
		drop table no_Overlaps_side

	commit

	select  s1.*
			,m10.hh_person_number
			,m10.person_ageband as age
			,case   when m10.person_gender = 'F' then 'Female'
					when m10.person_gender = 'M' then 'Male'
					else 'Undefined'
			end     as gender
			,m11.weight
	into    no_Overlaps_side -- 9037422 row(s) affected
	from    stage1 as s1
			inner join V289_M10_session_individuals as m10 -- <-- we need this table to get the persons ids to get the scaling weights later on
			on  s1.event_id    		= m10.event_id
			and	s1.account_number	= m10.account_number
			and m10.overlap_batch is null
			inner join  (
							select  distinct
									account_number
									,hh_person_number
									,scaling_date
									,scaling_weighting  as weight
							from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
						)   AS M11
			on  m10.account_number      = m11.account_number
			and m10.hh_person_number    = m11.hh_person_number
			and s1.scaling_date         = m11.scaling_date 

	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: No_Overlaps_Side table DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building v289_m12_dailychecks_base table' TO CLIENT
	
	if	object_id('v289_m12_dailychecks_base') is not null
		drop table v289_m12_dailychecks_base
		
	commit

	select	*
	into	v289_m12_dailychecks_base
	from	(
				select	*
				from 	Overlaps_side
				union
				select	*
				from	no_Overlaps_side
			)	as base
			
	commit

	create hg index hg1 	on v289_m12_dailychecks_base(account_number)
	create hg index hg2 	on v289_m12_dailychecks_base(event_id)
	create hg index hg3 	on v289_m12_dailychecks_base(channel_name)
	create lf index lf1 	on v289_m12_dailychecks_base(hh_person_number)
	create lf index lf2 	on v289_m12_dailychecks_base(channel_pack)
	create lf index lf3 	on v289_m12_dailychecks_base(daypart)
	create date index dt1	on v289_m12_dailychecks_base(scaling_date)
	commit

	grant select on v289_m12_dailychecks_base to vespa_group_low_security
	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: v289_m12_dailychecks_base table DONE' TO CLIENT
	
	drop table Overlaps_side
	drop table no_Overlaps_side
	drop table stage1
	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: Creating table v289_S12_v_weighted_duration_skyview' TO CLIENT
	
	if object_id('v289_S12_weighted_duration_skyview') is not null
		drop table v289_S12_weighted_duration_skyview
		
	commit
		
	select	*
	into	v289_S12_weighted_duration_skyview
	from	(
				select  source                                                                         
						,scaling_date
						,age
						,trim(gender)   					as gender
						,daypart
						,account_number						as household
						,hh_person_number					as person
						,min(weight)                        as ukbase
						,sum(duration_seg)/60.00            as duration_mins
						,(sum(duration_seg)*ukbase)/60.00   as duration_weighted_mins
				from    v289_m12_dailychecks_base
				group   by  source
							,scaling_date
							,age
							,gender
							,daypart
							,account_number
							,hh_person_number
				UNION ALL 
				select	*
				from	(
							SELECT	'BARB'                                      as source 
									,DATE(v.start_time_of_session)	        as scaling_date
									,trim(v.ageband) 				            as age
									,trim(v.sex) 					            as gender
									,trim(v.session_daypart)		            as daypart
									,cast(v.household_number as varchar(12))    as household
									,v.person_number				            as person
									,min(v.processing_weight)		            as ukbase
									,sum(v.progwatch_duration)  	as duration_min
									,sum(v.progscaled_duration) 	as duration_weighted_min
							FROM 	skybarb_fullview as v
									LEFT JOIN	vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES	AS mp	
									ON	mp.service_key = v.service_key
									AND DATE(broadcast_start_date_time_local) BETWEEN mp.EFFECTIVE_FROM AND mp.EFFECTIVE_TO
							GROUP BY	source
										,scaling_date
										,age
										,gender
										,daypart
										,v.household_number
										,v.person_number
						)	barb
				where	scaling_date = (select max(cast(event_start_date_time_utc as date)) from v289_M06_dp_raw_data)
			)	as final
			
	create hg index hg1 	on v289_S12_weighted_duration_skyview(household)
	create lf index lf1 	on v289_S12_weighted_duration_skyview(person)
	create lf index lf3 	on v289_S12_weighted_duration_skyview(daypart)
	create date index dt1	on v289_S12_weighted_duration_skyview(scaling_date)
	commit
	
	grant select on v289_S12_weighted_duration_skyview to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: v289_S12_v_weighted_duration_skyview table DONE' TO CLIENT
	
	drop table v289_m12_dailychecks_base
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Initialising Environment DONE' TO CLIENT
	
-------------------------------------------------	
-- M12.1 - Slicing for weighted duration (Skyview)
-------------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Begining M12.1 - Slicing for weighted duration (Skyview)' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_avgminwatched_x_genderage' TO CLIENT
	
	if object_id('v289_s12_avgminwatched_x_genderage') is not null
		drop view v289_s12_avgminwatched_x_genderage
	
	commit
		
	create view v289_s12_avgminwatched_x_genderage as
	    select  scaling_date
			,source
			,case when source = 'BARB' and age = '0-19' then 'Undefined' else gender end as gender
			,age
            ,count(distinct individuals)                        as sample
            ,sum(weights)                                       as weighted_sample
			,sum(minutes_watched)			                    as total_mins_watched
			,sum(minutes_watched_scaled)	                    as total_mins_scaled_watched
			,avg(minutes_watched)/60.00                         as avg_hh_watched
			,sum(minutes_watched_scaled)/weighted_sample/60.00  as avg_hh_watched_scaled
	from    (
				select  scaling_date
						,source
						,age
						,gender
						,household||'-'||person         as individuals
						,min(ukbase)                    as weights
						,sum(duration_mins)             as minutes_watched
						,sum(duration_weighted_mins)    as minutes_watched_scaled
				from    v289_S12_weighted_duration_skyview
				group   by  scaling_date
							,source
							,age
							,gender
							,individuals
			)   as base
	group   by  scaling_date
				,source
				,gender
				,age

	grant select on v289_s12_avgminwatched_x_genderage to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_avgminwatched_x_genderage DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_sovminwatched_x_dimensions' TO CLIENT
	
	if object_id('v289_s12_sovminwatched_x_dimensions') is not null
		drop view v289_s12_sovminwatched_x_dimensions
	
	commit
	
	create view v289_s12_sovminwatched_x_dimensions as
	select  source
			,scaling_date
			,age
			,case 	when source = 'BARB' and age = '0-19' then 'Undefined'
					else gender
			end		as gender
			,daypart
			,sum(duration_mins)             as minutes_watched
			,sum(duration_weighted_mins)    as minutes_weighted_watched
            ,count(Distinct cast((household||'-'||person) as varchar(30)))  as sample
            ,sum(ukbase)                                                    as weighted_sample
			,avg(duration_mins)				as avg_min_watched
	from    v289_S12_weighted_duration_skyview
	group   by  source
				,scaling_date
				,age
				,gender
				,daypart

	grant select on v289_s12_sovminwatched_x_dimensions to vespa_group_low_security
	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_sovminwatched_x_dimensions DONE' TO CLIENT
	
	
-- V289_s12_v_hhsize_distribution

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_hhsize_distribution' TO CLIENT

	if object_id('V289_s12_v_hhsize_distribution') is not null	
		drop view V289_s12_v_hhsize_distribution
		
	commit

	create view V289_s12_v_hhsize_distribution as
	select  'VESPA' 		    as source
			,hhsize
			,count(1)   		as	hits
			,sum(hhweighted)	as	ukbase
	from    (
				select  m07.account_number
						,min(m08.household_size)		as	hhsize
						,sum(m11.scaling_weighting)	    as hhweighted
				from    (
							select  distinct
									account_number
							from    v289_m07_dp_data
						)   as 	m07
						inner join 	V289_M08_SKY_HH_composition as 	m08		
						on  m07.account_number = m08.account_number
						and	m08.person_head = '1'
						inner join	(
										select  distinct
												account_number
												,hh_person_number
												,scaling_date
												,scaling_weighting
										from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
									)   as	m11		
					on  m07.account_number      = m11.account_number
					and	m11.HH_person_number    = m08.HH_person_number
					and	m11.scaling_date        = (select max(date(event_start_utc)) from	v289_m07_dp_data)
					group   by  m07.account_number
				)   as base
	group   by  source
				,hhsize
	union   
	select  'BARB'  as source
			,hhsize
			,count(1)           as hits
			,sum(hhweighted)    as ukbase
	from    (
				select  house_id
						,count(1) as hhsize
						,sum(weight.processing_weight)/10  as hhweighted
				from    skybarb	as	skybarb
						left join barb_weights as weight
						on  skybarb.house_id    = weight.household_number
						and skybarb.person      = weight.person_number
						and skybarb.head        = 1
				group   by  house_id
			)   as base
	group   by  source
				,hhsize
	
	
	grant select on V289_s12_v_hhsize_distribution to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_hhsize_distribution DONE' TO CLIENT
	
	
-- V289_s12_v_genderage_distribution

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_genderage_distribution' TO CLIENT

	if object_id('V289_s12_v_genderage_distribution') is not null	
		drop view V289_s12_v_genderage_distribution
		
	commit

	create view V289_s12_v_genderage_distribution as
	select  'VESPA'                             as source
			,trim(m08.person_ageband)	        as ageband
			,case   when m08.person_gender = 'F' then 'Female'
					when m08.person_gender = 'M' then 'Male'
					else 'Undefined'
			end     as genre
			,count(1)   as hits
            ,cast(sum(m11.weight) as integer)   as sow
	from    V289_M08_SKY_HH_composition as m08
			inner join  (
							select  distinct
									account_number
							from    v289_m07_dp_data
						)   as m07
			on  m08.account_number = m07.account_number
            inner join	(
						    select  distinct
							        account_number
									,hh_person_number
									,scaling_date
									,scaling_weighting  as weight
							from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
						)   as	m11		
			on  m08.account_number      = m11.account_number
			and	m08.HH_person_number    = m11.HH_person_number
			and	m11.scaling_date        = (select max(date(event_start_utc)) from	v289_m07_dp_data)
	group   by  source
				,ageband
				,genre
	union
	select  'BARB'                                                  as source
			,case   when age between 1 and 17	then '0-19'
					when age between 20 and 24 	then '20-24'
					when age between 25 and 34 	then '25-34'
					when age between 35 and 44 	then '35-44'
					when age between 45 and 64 	then '45-64'
					when age > 65              	then '65+'
					else 'Undefined'  
			end     as ageband
			,trim(sex)	                                            as sex_
			,count(1)                                               as hits
            ,cast((sum(weight.processing_weight)/10) as integer)    as hhweighted
	from    skybarb	as	skybarb
            left join barb_weights as weight
			on  skybarb.house_id    = weight.household_number
			and skybarb.person      = weight.person_number
	group   by  source
				,ageband
				,sex_

	
	grant select on V289_s12_v_genderage_distribution to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_genderage_distribution DONE' TO CLIENT
	

-- v289_s12_overall_consumption

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_overall_consumption' TO CLIENT

	if object_id('v289_s12_overall_consumption') is not null	
		drop view v289_s12_overall_consumption
		
	commit

	create view v289_s12_overall_consumption as
	select  source
			,scaling_date
			,count(distinct individual)                         as sample
			,sum(weight)                                        as scaled_sample
			,sum(minutes_watched)                               as source_mins_watched
			,sum(minutes_watched_scaled)                        as source_scaled_mins_watched
			,avg(minutes_watched)/60.00                         as avg_mins_watched
			,source_scaled_mins_watched / scaled_sample / 60.00 as avg_scaled_mins_watched
	from    (   
				select  source
						,scaling_date
						,household||'-'||person         as individual
						,min(ukbase) as weight
						,sum(duration_mins)             as minutes_watched
						,sum(duration_weighted_mins)    as minutes_watched_scaled
				from    v289_S12_weighted_duration_skyview
				group   by  source
							,scaling_date
							,individual
			)   as base
	group   by  source
				,scaling_date
				
	grant select on v289_s12_overall_consumption to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_overall_consumption DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Slicing for weighted duration (Skyview) DONE' TO CLIENT
	
end;


commit;
grant execute on v289_m12_validation to vespa_group_low_security;
commit;