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

	http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight Collation Documents/01 Analysis Requests/V289 - Skyview Futures
                                                                        
**Business Brief:

	This brief module serves to produce a high-level summary of the H2I algorithm outputs, giving average
	viewing hours per individual across the gender-age classes, and average hours per household to comparison
	against an equivalent metric as derived from the dp_prog_viewing_* tables.
	
	The main procedure can be executed to target particular table names (backups for e.g.), where in the 
	absence of input parameters will automatically attempt to analyse the standard tables:
		V289_M10_session_individuals
		V289_M07_dp_data
	within the user's own schema.
	
	Syntax:
		execute V289_H2I_summarise_avg_viewing;
		execute V289_H2I_summarise_avg_viewing 
				'thompsonja.V289_M10_session_individuals_backup_50pc_01'
			,	'thompsonja.V289_M07_dp_data_backup_50pc_01'
		;
		

**Module:
	
	M10b: Process Manager
			M10b.0 - Create simple function to test for data existence within target tables
			M10b.1 - Main procedure to calculate average viewing hours per individual and household

	
--------------------------------------------------------------------------------------------------------------
*/



----------------------------------------------------------------------------------
-- M10b.0 - Create simple function to test for data existence within target tables
----------------------------------------------------------------------------------
create or replace function V289_H2I_check_M10_validation_tables(
        @table_session_individuals   varchar(255)   =   'V289_M10_session_individuals'
    ,   @table_dp_data               varchar(255)   =   'V289_M07_dp_data'
    )
returns bit
as  begin

    declare @result bit

    execute(
    +   '   select  '
    +   '       @result =   case    '
    +   '                       when    '
    +   '                               (exists (select top 1 1 from ' + @table_session_individuals + '))   '
    +   '                           and (exists (select top 1 1 from ' + @table_dp_data + '))   '
    +   '                           then 1  '
    +   '                           else 0  '
    +   '                   end ')

    return @result

end
;
commit;

grant execute on V289_H2I_check_M10_validation_tables to vespa_group_low_security;
commit;


------------------------------------------------------------------------------------------
-- M10b.1 - Main procedure to calculate average viewing hours per individual and household
------------------------------------------------------------------------------------------
create or replace procedure V289_H2I_summarise_avg_viewing
        @table_session_individuals   varchar(255)   =   'V289_M10_session_individuals'
    ,   @table_dp_data               varchar(255)   =   'V289_M07_dp_data'
as  begin


	-- Check for data before proceeding
	
	message cast(now() as timestamp) || ' | M10b - Checking for M07 and M10 output tables' to client
	
	if  V289_H2I_check_M10_validation_tables(
				@table_session_individuals
			,   @table_dp_data
			)   =   1
		begin


			-- Create temporary views on the input tables
			
			message cast(now() as timestamp) || ' | M10b - Creating temporary views' to client
			
			execute ('create or replace view tmp_view_m10 as select * from ' + @table_session_individuals)
			execute ('create or replace view tmp_view_m07 as select * from ' + @table_dp_data)


			
			-- Prepare base table (still no aggregations as yet - just pulling together all of the relevant data on an event/individual level)
			select
					a.person_gender
				,   a.person_ageband
				,   a.account_number
				,   a.hh_person_number
				,   a.account_number + '-' + cast(a.hh_person_number as varchar)    as  person_id
				,	c.scaling_weighting
				,   a.event_id
				,	trim(b.session_daypart)											as	daypart
				,   case
						when    a.overlap_batch is null then b.event_start_utc
						else    b.chunk_start
					end     														as  viewing_start_dt
				,   case
						when    a.overlap_batch is null then b.event_end_utc
						else    b.chunk_end
					end     														as  viewing_end_dt
				,   datediff(second,viewing_start_dt,viewing_end_dt)        		as  viewing_seconds
			into	#tmp
			from
							tmp_view_m10   											as  a
				inner join  tmp_view_m07                							as  b   on  a.account_number = b.account_number
																							and a.event_id = b.event_id
																							and case when a.overlap_batch is null then 0 else a.overlap_batch end = case when b.overlap_batch is null then 0 else b.overlap_batch end
				inner join	V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING					as	c	on	a.account_number = c.account_number
																							and	a.hh_person_number = c.hh_person_number
																							and	c.build_date = (select	max(build_date) from	V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING)
            -- where a.overlap_batch is null
			
			
			-- Vespa viewing per individual (aggregated over the entire day of data)
			
			message cast(now() as timestamp) || ' | M10b - Calculating average viewing per individual gender-age class' to client

			select
					person_gender
				,   person_ageband
				,   count()                         							as  individuals
				,   count(distinct account_number)  							as  households
				,   sum(viewing_hours)              							as  total_viewing_hours
				,   avg(viewing_hours)              							as  average_viewing_hours
				,	sum(scaling_weighting)										as	individuals_scaled
				,	sum(viewing_hours_scaled)									as	total_viewing_hours_scaled
				,   total_viewing_hours_scaled / individuals_scaled				as	average_viewing_hours_scaled
				,	(select count(distinct account_number) from tmp_view_m10)	as	OLAP_unique_households
			from
				(	-- aggregated over individuals to get their respective viewing total and scaled total consumption
					select
							person_gender
						,   person_ageband
						,   person_id
						,	scaling_weighting
						,   account_number
						,   sum(viewing_seconds) / 3600.0  		as  viewing_hours
						,	viewing_hours * scaling_weighting	as	viewing_hours_scaled
					from	#tmp	as  t0
					group by
							person_gender
						,   person_ageband
						,   person_id
						,	scaling_weighting
						,   account_number
				)   as  t1
			group by
					person_gender
				,   person_ageband
			order by
					person_gender
				,   person_ageband


			
			
			message cast(now() as timestamp) || ' | M10b - Calculating average viewing per individual gender-age class and daypart' to client

			select
					person_gender
				,   person_ageband
				,	daypart
				,   count()                         							as  individuals
				,   count(distinct account_number)  							as  households
				,   sum(viewing_hours)              							as  total_viewing_hours
				,   avg(viewing_hours)              							as  average_viewing_hours
				,	sum(scaling_weighting)										as	individuals_scaled
				,	sum(viewing_hours_scaled)									as	total_viewing_hours_scaled
				,   total_viewing_hours_scaled / individuals_scaled				as	average_viewing_hours_scaled
				,	(select count(distinct account_number) from tmp_view_m10)	as	OLAP_unique_households
			from
				(	-- aggregated over individuals to get their respective viewing total and scaled total consumption per daypart
					select
							person_gender
						,   person_ageband
						,	daypart
						,   person_id
						,	scaling_weighting
						,   account_number
						,   sum(viewing_seconds) / 3600.0  		as  viewing_hours
						,	viewing_hours * scaling_weighting	as	viewing_hours_scaled
					from	#tmp	as  t0
					group by
							person_gender
						,   person_ageband
						,	daypart
						,   person_id
						,	scaling_weighting
						,   account_number
				)   as  t1
			group by
					person_gender
				,   person_ageband
				,	daypart
			order by
					person_gender
				,   person_ageband
				,	daypart
			
			
			-- Count distinct accounts
			
			message cast(now() as timestamp) || ' | M10b - Counting distinct accounts' to client
			
			select  count(distinct account_number)  as  unique_accounts
			from    tmp_view_m10

			
			
			
			
			-- Viewing per household - dedupe individuals per event
			
			message cast(now() as timestamp) || ' | M10b - Calculating average viewing per household' to client
			
			select
					count()                     as  rows
				,   count(distinct account_number)  as  accounts
				,   sum(viewing_hours)          as  total_hours
				,   avg(viewing_hours)          as  avg_hours_per_hh
				,   min(viewing_hours)          as  min_hours_per_hh
				,   max(viewing_hours)          as  max_hours_per_hh
				,   stddev(viewing_hours)       as  std_hours_per_hh
			from
				(
					select
							account_number
						,   sum(viewing_hours)  as  viewing_hours
					from
						(
							select
									account_number
								,   sum(viewing_seconds / 3600.0)   as  viewing_hours
							from
								(
									select
											a.account_number
										,   a.event_id
										,   datediff(second,b.event_start_utc,b.event_end_utc)      as  viewing_seconds
									from
													tmp_view_m10        as  a
										inner join  tmp_view_m07      	as  b   on  a.account_number = b.account_number
																				and a.event_id = b.event_id
																				and case when a.overlap_batch is null then 0 else a.overlap_batch end = case when b.overlap_batch is null then 0 else b.overlap_batch end
									where a.overlap_batch is null
									group by
											a.account_number
										,   a.event_id
										,   viewing_seconds
								)   as  t0
							group by account_number
							union all
							select
									account_number
								,   sum(viewing_seconds / 3600.0)   as  viewing_hours
							from
								(
									select
											a.account_number
										,   a.overlap_batch
										,   datediff(second,b.chunk_start,b.chunk_end)              as  viewing_seconds
									from
													tmp_view_m10        as  a
										inner join  tmp_view_m07       	as  b   on  a.account_number = b.account_number
																				and a.event_id = b.event_id
																				and case when a.overlap_batch is null then 0 else a.overlap_batch end = case when b.overlap_batch is null then 0 else b.overlap_batch end
									where a.overlap_batch is not null
									group by
											a.account_number
										,   a.overlap_batch
										,   viewing_seconds
								)   as  t0
							group by account_number
						)   as  t1
					group by account_number
				)   as  t2

		end -- if begin
	else
		message cast(now() as timestamp) || ' | M10b - Required input tables not detected. Exiting.' to client


end -- begin procedure
;
commit;

grant execute on V289_H2I_summarise_avg_viewing to vespa_group_low_security;
commit;



/*

execute V289_H2I_summarise_avg_viewing;

execute V289_H2I_summarise_avg_viewing 'tanghoi.V289_M10_session_individuals' , 'tanghoi.V289_M07_dp_data';

*/