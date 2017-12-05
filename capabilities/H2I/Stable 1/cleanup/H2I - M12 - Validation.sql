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
			M12.2 - Returning Results
		
--------------------------------------------------------------------------------------------------------------
*/
	
-----------------------------------	
-- M12.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m12_validation
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M12.0 - Initialising Environment' TO CLIENT
		   
	declare @proc_date 	date
	declare @query		varchar(5000)
		   
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building Stage1 table from M07' TO CLIENT
	
	
					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('stage1')
                        and     tabletype = 'TABLE')
	
		drop table stage1

	commit
					
	select  'VESPA'                     as source
			,date(m07.event_start_utc)  as scaling_date
			,m06.service_key
			,ska.channel_name
			,ska.channel_pack
			,trim(m07.session_daypart)  as daypart
			,m07.event_id
			,coalesce(m07.chunk_start,m07.event_start_utc)  as session_start
            ,coalesce(m07.chunk_end,m07.event_end_utc)      as session_end
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building Overlaps_Side table' TO CLIENT
	
	
					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('Overlaps_side')
                        and     tabletype = 'TABLE')
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building No_Overlaps_Side table' TO CLIENT
	
	
					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('no_Overlaps_side')
                        and     tabletype = 'TABLE')
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Building v289_m12_dailychecks_base table' TO CLIENT
	
	
					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_m12_dailychecks_base')
                        and     tabletype = 'TABLE')
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
	create dttm index dttm1 on v289_m12_dailychecks_base(session_start)
	create dttm index dttm2 on v289_m12_dailychecks_base(session_end)
	commit

	grant select on v289_m12_dailychecks_base to vespa_group_low_security
	commit

	select	@proc_date = max(scaling_date) from v289_m12_dailychecks_base
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: v289_m12_dailychecks_base table DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	
	drop table Overlaps_side
	drop table no_Overlaps_side
	drop table stage1
	commit

	MESSAGE cast(now() as timestamp)||' | @ M12.0: Creating table v289_S12_v_weighted_duration_skyview' TO CLIENT
					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_S12_weighted_duration_skyview')
                        and     tabletype = 'TABLE')
		drop table v289_S12_weighted_duration_skyview
		
	commit
		
	select	*
	into	v289_S12_weighted_duration_skyview
	from	(
				select  'H2I'                                           as source
						,@proc_date                                   	as scaling_date
						,m08.person_ageband                             as age
						,case m08.person_gender when 'F' then 'Female'
												when 'M' then 'Male'
												else 'Undefined'
						end                                             as gender
						,m12.daypart
						,m11.account_number					            as household
						,m11.hh_person_number				            as person
						,min(m11.scaling_weighting)                     as ukbase
						,case when min(m12.hh_person_number) is not null then min(m11.scaling_weighting) else null end	as viewersbase
						,sum(m12.duration_seg)/60.00                    as duration_mins
						,(sum(m12.duration_seg)*ukbase)/60.00           as duration_weighted_mins
				from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING  as m11
						inner join V289_M08_SKY_HH_composition  as m08
						on  m11.account_number      = m08.account_number
						and m11.hh_person_number    = m08.hh_person_number
						left join v289_m12_dailychecks_base     as m12
						on  m11.account_number      = m12.account_number
						and m11.hh_person_number    = m12.hh_person_number
				group   by  source
							,scaling_date
							,age
							,gender
							,m12.daypart
							,household
							,person
				UNION ALL 
				select	*
				from	(
							SELECT	'BARB'                                          					as source 
									,@proc_date                                           				as scaling_date
									,case   when base.age between 1 and 19  then '0-19'
											when base.age between 20 and 24 then '20-24'
											when base.age between 25 and 34 then '25-34'
											when base.age between 35 and 44 then '35-44'
											when base.age between 45 and 64 then '45-64'
											when base.age >= 65             then '65+'  
									end                                                     			as age
									,case when age = '0-19' then 'Undefined' else trim(base.sex) end	as gender
									,trim(v.session_daypart)		                        			as daypart
									,cast(base.house_id as varchar(12))                     			as household
									,base.person				                            			as person
									,min(weights.processing_weight)		                    			as ukbase
									,min(v.processing_weight)											as viewersbase
									,sum(v.progwatch_duration)  	                        			as duration_mins
									,sum(v.progscaled_duration) 	                        			as duration_weighted_mins
							FROM 	skybarb                         as base
									inner join  barb_weights        as weights
									on  base.house_id   = weights.household_number
									and base.person     = weights.person_number
									left join   skybarb_fullview    as v
									on  base.house_id   = v.household_number
									and base.person     = v.person_number
									and date(v.start_time_of_Session) = @proc_date
									LEFT JOIN	vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES	AS mp	
									ON	mp.service_key = v.service_key
									AND DATE(broadcast_start_date_time_local) BETWEEN mp.EFFECTIVE_FROM AND mp.EFFECTIVE_TO
							GROUP   BY	source
										,scaling_date
										,age
										,gender
										,daypart
										,household
										,person
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Creating table v289_S12_freqreach' TO CLIENT
	
	
	
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Creating table v289_S12_freqreach DONE' TO CLIENT
		
	MESSAGE cast(now() as timestamp)||' | @ M12.0: Initialising Environment DONE' TO CLIENT
	
-------------------------------------------------	
-- M12.1 - Slicing for weighted duration (Skyview)
-------------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Begining M12.1 - Slicing for weighted duration (Skyview)' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_avgminwatched_x_genderage' TO CLIENT
	
					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_s12_avgminwatched_x_genderage')
                        and     tabletype = 'VIEW')
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_sovminwatched_x_dimensions' TO CLIENT
	
	
					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_s12_sovminwatched_x_dimensions')
                        and     tabletype = 'VIEW')
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	
-- V289_s12_v_hhsize_distribution

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_hhsize_distribution' TO CLIENT

					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_s12_v_hhsize_distribution')
                        and     tabletype = 'VIEW')
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	
-- V289_s12_v_genderage_distribution

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View V289_s12_v_genderage_distribution' TO CLIENT

					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_s12_v_genderage_distribution')
                        and     tabletype = 'VIEW')
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT

-- v289_s12_overall_consumption

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_overall_consumption' TO CLIENT

					if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_s12_overall_consumption')
                        and     tabletype = 'VIEW')
		drop view v289_s12_overall_consumption
		
	commit

	create view v289_s12_overall_consumption as
	select  source
			,scaling_date
			,count(distinct individual)                         				as sample
			,sum(weight)                                        				as scaled_sample
            ,sum(vweight)                                                       as viewers_scaled_sample
			,sum(minutes_watched)                               				as source_mins_watched
			,sum(minutes_watched_scaled)                        				as source_scaled_mins_watched
			,(cast(source_mins_watched as float)/cast(sample as float))/60.0    as avg_mins_watched
			,source_scaled_mins_watched / scaled_sample / 60.00 				as avg_scaled_mins_watched
	from    (   
				select  source
						,scaling_date
						,household||'-'||person         as individual
						,min(ukbase) 					as weight
                        ,min(viewersbase)   			as vweight
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
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_freq_and_reach' TO CLIENT
	
	create or replace view v289_s12_freq_and_reach as
	select  source
			,scaling_date
			,channel_name
			,daypart
			,gender
			,age
			,cast((sum(frequency*weight_reach) over ( partition by channel_name)) as float)/cast(r_cn as float) 						as f_CN
			,sum(case when reach_CNp_level = 1 then weight_reach else 0 end) over ( partition by channel_name) 							as r_CN
			,sum(dur_min_scaled) over ( partition by channel_name) 																        as tmws_CN
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,daypart)) as float)/cast(r_CNd as float) 				as f_CNd
			,sum(case when reach_CNdP_level = 1 then weight_reach else 0 end) over ( partition by channel_name,daypart) 				as r_CNd
			,sum(dur_min_scaled) over ( partition by channel_name,daypart) 														        as tmws_CNd
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,gender)) as float)/cast(r_CNg as float) 				as f_CNg
			,sum(case when reach_CNgP_level = 1 then weight_reach else 0 end) over ( partition by channel_name,gender) 					as r_CNg
			,sum(dur_min_scaled) over ( partition by channel_name,gender) 														        as tmws_CNg
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,age)) as float)/cast(r_CNa as float)					as f_CNa
			,sum(case when reach_CNaP_level = 1 then weight_reach else 0 end) over ( partition by channel_name,age) 					as r_CNa
			,sum(dur_min_scaled) over ( partition by channel_name,age)															        as tmws_CNa
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,age,gender)) as float)/cast(r_CNaG as float)			as f_CNaG
			,sum(case when reach_CNaGp_level = 1 then weight_reach else 0 end) over ( partition by channel_name,age,gender) 			as r_CNaG
			,sum(dur_min_scaled) over ( partition by channel_name,age,gender)													        as tmws_CNaG
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,daypart,age)) as float)/cast(r_CNdA as float)			as f_CNdA
			,sum(case when reach_CNdAp_level = 1 then weight_reach else 0 end) over ( partition by channel_name,daypart,age) 			as r_CNdA
			,sum(dur_min_scaled) over ( partition by channel_name,daypart,age)													        as tmws_CNdA
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,daypart,age,gender)) as float)/cast(r_CNdAg as float)	as f_CNdAg
			,sum(case when reach_CNdAgP_level = 1 then weight_reach else 0 end) over ( partition by channel_name,daypart,age,gender)	as r_CNdAg
			,sum(dur_min_scaled) over ( partition by channel_name,daypart,age,gender)											        as tmws_CNdAg
	from    (
                select  source
                        ,scaling_date
                		,coalesce(channel_name,'Unknown')       as channel_name
                		,trim(daypart)							as daypart
                		,account_number||'-'||hh_person_number  as person
                		,age
                		,gender
                		,count(distinct event_id)   as frequency
                		,min(weight)                as weight_reach
                        ,sum(duration_seg)/60.0     as dur_min
                        ,dur_min*weight_reach       as dur_min_scaled
                		,row_number() over ( partition by person order by person)                                   as reach_P_level
                		,row_number() over ( partition by channel_name,person order by person)                      as reach_CNp_level
                		,row_number() over ( partition by channel_name,daypart,person order by person)              as reach_CNdP_level
                		,row_number() over ( partition by channel_name,gender,person order by person)               as reach_CNgp_level
                		,row_number() over ( partition by channel_name,age,person order by person)                  as reach_CNaP_level
                		,row_number() over ( partition by channel_name,age,gender,person order by person)           as reach_CNaGp_level
                		,row_number() over ( partition by channel_name,daypart,age,person order by person)          as reach_CNdAp_level
                		,row_number() over ( partition by channel_name,daypart,age,gender,person order by person)   as reach_CNdAgP_level
                from    v289_m12_dailychecks_base
                group   by  source
                            ,scaling_date
                			,channel_name
                			,daypart
                			,person
                			,age
                			,gender
        )	as vespaside
	union   all
	select  source
			,scaling_date
			,channel_name
			,daypart
			,gender
			,age
			,cast((sum(frequency*weight_reach) over ( partition by channel_name)) as float)/cast(r_cn as float) 						as f_CN
			,sum(case when reach_CNp_level = 1 then weight_reach else 0 end) over ( partition by channel_name) 							as r_CN
			,sum(dur_min_scaled) over ( partition by channel_name) 																        as tmws_CN
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,daypart)) as float)/cast(r_CNd as float) 				as f_CNd
			,sum(case when reach_CNdP_level = 1 then weight_reach else 0 end) over ( partition by channel_name,daypart) 				as r_CNd
			,sum(dur_min_scaled) over ( partition by channel_name,daypart) 														        as tmws_CNd
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,gender)) as float)/cast(r_CNg as float) 				as f_CNg
			,sum(case when reach_CNgP_level = 1 then weight_reach else 0 end) over ( partition by channel_name,gender) 					as r_CNg
			,sum(dur_min_scaled) over ( partition by channel_name,gender) 														        as tmws_CNg
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,age)) as float)/cast(r_CNa as float)					as f_CNa
			,sum(case when reach_CNaP_level = 1 then weight_reach else 0 end) over ( partition by channel_name,age) 					as r_CNa
			,sum(dur_min_scaled) over ( partition by channel_name,age)															        as tmws_CNa
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,age,gender)) as float)/cast(r_CNaG as float)			as f_CNaG
			,sum(case when reach_CNaGp_level = 1 then weight_reach else 0 end) over ( partition by channel_name,age,gender) 			as r_CNaG
			,sum(dur_min_scaled) over ( partition by channel_name,age,gender)													        as tmws_CNaG
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,daypart,age)) as float)/cast(r_CNdA as float)			as f_CNdA
			,sum(case when reach_CNdAp_level = 1 then weight_reach else 0 end) over ( partition by channel_name,daypart,age) 			as r_CNdA
			,sum(dur_min_scaled) over ( partition by channel_name,daypart,age)													        as tmws_CNdA
			,cast((sum(frequency*weight_reach) over ( partition by channel_name,daypart,age,gender)) as float)/cast(r_CNdAg as float)	as f_CNdAg
			,sum(case when reach_CNdAgP_level = 1 then weight_reach else 0 end) over ( partition by channel_name,daypart,age,gender)	as r_CNdAg
			,sum(dur_min_scaled) over ( partition by channel_name,daypart,age,gender)											        as tmws_CNdAg
	from    (
                select  'BARB'                                  as source
						,date(start_time_of_session)            as scaling_date
						,coalesce(channel_name,'Unknown')       as channel_name
						,trim(session_daypart)                  as daypart
						,household_number||'-'||person_number   as person
						,ageband                                as age
						,sex                                    as gender
						,count(distinct event_id)             as frequency
						,min(processing_weight)                 as weight_reach
                        ,sum(progscaled_duration)               as dur_min_scaled
						,row_number() over ( partition by person order by person)                                   as reach_P_level
						,row_number() over ( partition by channel_name,person order by person)                      as reach_CNp_level
						,row_number() over ( partition by channel_name,daypart,person order by person)              as reach_CNdP_level
						,row_number() over ( partition by channel_name,gender,person order by person)               as reach_CNgp_level
						,row_number() over ( partition by channel_name,age,person order by person)                  as reach_CNaP_level
						,row_number() over ( partition by channel_name,age,gender,person order by person)           as reach_CNaGp_level
						,row_number() over ( partition by channel_name,daypart,age,person order by person)          as reach_CNdAp_level
						,row_number() over ( partition by channel_name,daypart,age,gender,person order by person)   as reach_CNdAgP_level
				from    skybarb_fullview
				group   by  source
							,scaling_date
							,channel_name
							,daypart
							,person
							,age
							,gender
        )	as barbside
	
	commit
	grant select on v289_s12_freq_and_reach to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_freq_and_reach DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Slicing for weighted duration (Skyview) DONE' TO CLIENT
	
	
	/*MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating Table v289_m12_MBM' TO CLIENT
		
	-- Generating a table with every minute of the processing date...
	if object_id('time_dimension') is not null
		drop table time_dimension
		
	commit
    create table time_dimension ( dt_hhmm timestamp )
    commit

    insert  into time_dimension
	select  distinct
			cast((@proc_date||' '||substring(cast(local_time_minute as varchar(8)),1,5)||':00') as timestamp)    as dt_hhmm
	from    viq_time
	
	commit
	create unique index u1 on time_dimension(dt_hhmm)
	commit
	
	
	if object_id('v289_m12_MBM') is not null
		drop table v289_m12_MBM

	commit

	select  m12.source
			,m12.scaling_date
			--,coalesce(m12.channel_name,'Unknown')                           as channel_name
			,m12.daypart
			,m12.age
			,m12.gender
			,mbm.dt_hhmm													as minute
			,count(distinct m12.account_number||'-'||m12.hh_person_number)  as people
			,sum(weight)                                                    as reach
	into    v289_m12_MBM
	from    v289_m12_dailychecks_base   as m12
			inner join  time_dimension	as mbm
			on  mbm.dt_hhmm between m12.session_start and m12.session_end
	group   by  m12.source
				,m12.scaling_date
				--,m12.channel_name
				,m12.daypart
				,m12.age
				,m12.gender
				,mbm.dt_hhmm

	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating Table v289_m12_MBM Stage 1 DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	
	-- bringing the table up from programme level to channel level to avoid double counts 
	-- on individuals
	select  distinct
			date(start_time_of_session) as scaling_date
			,household_number
			,person_number
			,processing_weight
			,trim(sex)                        as gender
			,trim(ageband)                    as age
			,event_id
			,start_time_of_session
			,end_time_of_session
			,channel_name
			,trim(session_daypart)      as daypart
	into	#v289_m12_aux
	from    skybarb_fullview
	where   date(start_time_of_session) = @proc_date

	commit
	create dttm index dttm1 on #v289_m12_aux(start_time_of_session)
	create dttm index dttm2 on #v289_m12_aux(end_time_of_session)
	commit
	
	insert  into v289_m12_mbm
	select  'BARB'                                                          as source
			,barb.scaling_date
			--,coalesce(barb.channel_name,'Unknown')                          as channel_name
			,barb.daypart
            ,barb.age
			,case when barb.age = '0-19' then 'Undefined' else barb.gender end	as gender
			,mbm.dt_hhmm                                                    as minute
			,count(distinct barb.household_number||'-'||barb.person_number) as people
			,sum(barb.processing_weight)                                    as reach
	from    #v289_m12_aux   			as barb
			inner join  time_dimension	as mbm
			on  mbm.dt_hhmm between barb.start_time_of_session and barb.end_time_of_session
	group   by  source
				,barb.scaling_date
				--,barb.channel_name
				,barb.daypart
				,barb.age
				,barb.gender
				,minute

	commit
	drop table #v289_m12_aux
	grant select on v289_m12_mbm to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating Table v289_m12_MBM DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	*/
	---------------------------------------
	-- Session Size Distribution comparison
	---------------------------------------

	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_m12_piv_distributions' TO CLIENT
	
	create or replace view v289_m12_piv_distributions as
	-- BARB
	select  'BARB'  as source
			,base.thedate
			,houses.hhsize
			,viewers.viewers_size
			,base.session_size
			,count(1)   as hits
	from    (
				-- identifying volume of session size
				select  date(start_time_of_session)	as thedate
						,household_number
						,session_id
						,count(Distinct person_number)  as session_size
				from    skybarb_fullview
				group   by  thedate
							,household_number
							,session_id
			)   as base
			inner join  (
							-- identifying the hh viewers size (useful dimention)
							select  household_number
									,count(distinct person_number)  as viewers_size
							from    skybarb_fullview
							group   by  household_number
						)   as viewers
			on  base.household_number   = viewers.household_number
			inner join  (
							-- identifying the hh size (useful dimention)
							select  base.house_id
									,count(distinct base.person)    as hhsize
							from    skybarb as base
									inner join  (
													select  distinct
															household_number
													from    skybarb_fullview
												)   as viewing
									on  base.house_id   = viewing.household_number
							group   by  base.house_id
						)   as houses
			on  base.household_number   = houses.house_id
	group   by  source
				,base.thedate
				,viewers.viewers_size
				,houses.hhsize
				,base.session_size
	union   all
	-- H2I
	select  'VESPA' as source
			,date(m07.event_start_utc)	as thedate
			,m07.hhsize
			,viewers.viewer_hhsize
			,m07.session_size
			,count(1)   as hits
	from    v289_m07_dp_data    as m07
			inner join  (
							select  distinct
									account_number
									,viewer_hhsize
							from    V289_M08_SKY_HH_composition
						)   as viewers
			on  m07.account_number  = viewers.account_number
	group   by  source		
				,thedate
				,m07.hhsize
				,M07.session_size
				,viewers.viewer_hhsize
	
	commit
	grant select on v289_m12_piv_distributions to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_m12_piv_distributions DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_m12_m07_hhchecks' TO CLIENT
	
	create or replace view v289_m12_m07_hhchecks as
	select  m07.*
			,m10.hh_person_number
			,m10.person_gender
			,m10.person_ageband
			,m11.weight
	from    (
				select  date(event_Start_utc) as thedate
						,account_number
						,hhsize
						,event_id
						,overlap_batch
						,session_daypart
						,programme_genre
						,channel_pack
						,coalesce(chunk_start,event_start_utc) as session_start
						,coalesce(chunk_end,event_end_utc) as session_end
						,coalesce(chunk_duration_seg,event_duration_seg) as session_duration
						,datediff(ss,session_start,session_end) as pseudo_duration
						,case when session_duration <> pseudo_duration then 1 else 0 end as flag
						,session_size
				from    v289_m07_dp_data
			)   as m07
				inner join V289_M10_session_individuals as m10
				on  m07.account_number = m10.account_number
				and m07.event_id = m10.event_id
				and coalesce(m07.overlap_batch,0) = coalesce(m10.overlap_batch,0)
				inner join  (
								select  distinct
										account_number
										,hh_person_number
										,scaling_date
										,scaling_weighting as weight
								from    V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
							)   as M11
				on  m07.account_number = m11.account_number
				and m10.hh_person_number = m11.hh_person_number
				and m07.thedate = m11.scaling_date
		order   by  m07.account_number asc
					,m07.session_start asc
					,m07.overlap_batch asc
	
	commit
	grant select on v289_m12_m07_hhchecks to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_m12_m07_hhchecks DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M12.1: Creating View v289_s12_overall_consumption_hhlevel DONE' TO CLIENT
	
	set @query =	'create or replace view v289_s12_overall_consumption_hhlevel as '||
					'select  ''H2I''									 as source '||
							',thedate as scaling_date '||
							',count(distinct account_number)             as sample '||
							',sum(theweight)                             as Scaled_Sample '||
							',sum(tsw)/60.0                              as tmw_tot '||
							',sum(tsws)/60.0                             as tmws_tot '||
							',cast(tmw_tot as float)/cast(sample as float)	as tmw_avg '||
							',cast(tmws_tot as float)/cast(Scaled_Sample as float) as tmws_avg '||
							',(tmw_avg/60.00)                          as thw_avg '||
							',(tmws_avg/60.00)                         as thws_avg '||
					'from    ( '||
								'select  date(m07.event_Start_utc)                                       as thedate '||
										',m07.account_number '||
										',sum(coalesce(m07.chunk_duration_seg,m07.event_duration_seg))   as tsw '||
										',max(w.scaling_weighting)                                       as theweight '||
										',tsw*theweight                                                  as tsws '||
								'from    v289_m07_dp_data    as m07 '||
										'inner join V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING w '||
										'on m07.account_number = w.account_number '||
										'inner join  ( '||
														'select  distinct '||
																'account_number '||
																',hh_person_number '||
														'from    V289_M08_SKY_HH_composition '||
														'where   person_head = ''1'' '||
													')   as comp '||
										'on w.account_number = comp.account_number '||
										'and w.hh_person_number = comp.hh_person_number '||
								'group   by  thedate '||
											',m07.account_number '||
							')   as base '||
					'group   by  thedate '||
					'union	all '||
					'select  ''BARB''									 as source '||
							',thedate '||
							',count(distinct household_number)           as sample '||
							',sum(hhweight)                              as sow '||
							',sum(tmw)                                   as tmw_tot '||
							',sum(tmws)                                  as tmws_tot '||
							',cast(tmw_tot as float)/cast(sample as float) as tmw_avg '||
							',cast(tmws_tot as float)/cast(sow as float) as tmws_avg '||
							',(tmw_avg/60.00)                            as thw_avg '||
							',(tmws_avg/60.00)                           as thws_avg '||
					'from    ( '||
								'select  thedate '||
										',household_number '||
										',hhweight '||
										',sum(s_dur)     as tmw '||
										',tmw*hhweight   as tmws '||
								'from    ( '||
											'select  '''||@proc_date||'''					as thedate '||
													',base.house_id					as household_number '||
													',v.session_id '||
													',sum(progwatch_duration)   		as s_dur '||
													',max(weights.processing_weight)	as hhweight '||
											'FROM 	skybarb                         as base '||
													'inner join  barb_weights        as weights '||
													'on  base.house_id   = weights.household_number '||
													'and base.person     = weights.person_number '||
													'left join   skybarb_fullview    as v '||
													'on  base.house_id   = v.household_number '||
													'and base.person     = v.person_number '||
													'and date(v.start_time_of_Session) = '''||@proc_date||''' '||
											'where	base.head = 1 '||
											'group   by  thedate '||
														',household_number '||
														',session_id '||
										')   as base1 '||
								'group   by  thedate '||
											',household_number '||
											',hhweight '||
							')   as base '||
					'group   by  thedate '
	
	execute(@query)
	
	commit
	grant select on v289_s12_overall_consumption_hhlevel to vespa_group_low_security
	commit
	----------------------------
	-- M12.2 - Returning Results
	----------------------------
	
	MESSAGE cast(now() as timestamp)||' | M12 Finished' TO CLIENT
	
end;


commit;
grant execute on v289_m12_validation to vespa_group_low_security;
commit;