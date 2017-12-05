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

	This Module is to prepare the extracted BARB data into a more suitable data structure for analysis...

**Module:
	
	M04: Barb Data Preparation
			M04.0 - Initialising Environment
			M04.1 - Preparing transient tables
			M04.2 - Final BARB Data Preparation
			M04.3 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M04.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m04_barb_data_preparation
	@processing_date date = null
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M04.0 - Initialising Environment' TO CLIENT
	
	
    declare @a int
	
    select	@a = count(1)
    from	barb_weights
    
	if @a > 0
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M04.0: Initialising Environment DONE' TO CLIENT
		
-------------------------------------
-- M04.1 - Preparing transient tables
-------------------------------------

		MESSAGE cast(now() as timestamp)||' | Begining M04.1 - Preparing transient tables' TO CLIENT
		
		/*
			Extracting the sample of panellists from Barb with Sky as the base for any analysis for the project
			at this stage we are only interested on the household demographic (hh size, sex and age of people withing the hH)
		*/

		if object_id('skybarb') is not null
			drop table skybarb
			
		commit
		
		select  demo.household_number										as house_id
				,demo.person_number											as person
				,datepart(year,today())-datepart(year,demo.date_of_birth) 	as age
				,case   when demo.sex_code = 1 then 'Male'
						when demo.sex_code = 2 then 'Female'
						else 'Unknown'
				end     as sex
				,case   when demo.household_status in (4,2)  then 1
						else 0
				end     as head
		into	skybarb
		from    BARB_INDV_PANELMEM_DET  as demo
				inner join  (
								select  distinct household_number
								from    BARB_PANEL_DEMOGR_TV_CHAR
								where   @processing_date between date_valid_from and date_valid_to
								and     reception_capability_code_1 = 2
							)   as barb_sky_panelists
				on  demo.household_number   = barb_sky_panelists.household_number
		where   @processing_date between demo.date_valid_from and demo.date_valid_to

		commit
		
		create hg index hg1	on skybarb(house_id)
		create lf index lf1	on skybarb(person)
		commit
		
		grant select on skybarb to vespa_group_low_security
		commit

		MESSAGE cast(now() as timestamp)||' | @ M04.1: Preparing transient tables DONE' TO CLIENT
		
--------------------------------------
-- M04.2 - Final BARB Data Preparation
--------------------------------------
		
		MESSAGE cast(now() as timestamp)||' | Begining M04.2 - Final BARB Data Preparation' TO CLIENT
		
		
		/*
			Now constructing a table to be able to check minutes watched across all households based on Barb (weighted to show UK):
			Channel pack, household size, programme genre and the part of the day where these actions happened (breakfast, lunch, etc...)
		*/

		if object_id('skybarb_fullview') is not null
			drop table skybarb_fullview

		commit

		select  mega.*
				,z.sex
				,case   when z.age between 1 and 19		then '0-19'
						when z.age between 20 and 24 	then '20-24'
						when z.age between 25 and 34 	then '25-34'
						when z.age between 35 and 44 	then '35-44'
						when z.age between 45 and 64 	then '45-64'
						when z.age >= 65              	then '65+'  
				end     as ageband
		into    skybarb_fullview
		from    (
					select  ska.service_key
							,barbskyhhsize.thesize	as hhsize
							,base.*
					from    (
								-- multiple aggregations to derive part of the day where the viewing session took place
								-- and a workaround to get the minutes watched per each person in the household multiplied
								-- by their relevant weights to show the minutes watched by UK (as per barb scaling exercise)...
								select  viewing.household_number
										,viewing.programme_name
										,local_start_time_of_session	as start_time_of_session
										,local_end_time_of_session		as end_time_of_session
										,local_tv_instance_start_date_time	as instance_start
										,local_tv_instance_end_date_time	as instance_end
										,duration_of_session
										,db1_station_code
										,case when local_start_time_of_recording is null then local_start_time_of_session else local_start_time_of_recording end as session_start_date_time								  -- This field was to link to VPS for programme data
										,case when local_start_time_of_recording is null then local_end_time_of_session else dateadd(mi, Duration_of_session, local_start_time_of_recording) end as session_end_date_time -- This field was to link to VPS for programme data
										,case   when cast(local_start_time_of_session as time) between '00:00:00.000' and '05:59:00.000' then 'night'
												when cast(local_start_time_of_session as time) between '06:00:00.000' and '08:59:00.000' then 'breakfast'
												when cast(local_start_time_of_session as time) between '09:00:00.000' and '11:59:00.000' then 'morning'
												when cast(local_start_time_of_session as time) between '12:00:00.000' and '14:59:00.000' then 'lunch'
												when cast(local_start_time_of_session as time) between '15:00:00.000' and '17:59:00.000' then 'early prime'
												when cast(local_start_time_of_session as time) between '18:00:00.000' and '20:59:00.000' then 'prime'
												when cast(local_start_time_of_session as time) between '21:00:00.000' and '23:59:00.000' then 'late night'
										end     as session_daypart
										,viewing.channel_pack
										,viewing.genre_description	as programme_genre
										,weights.person_number
										,weights.processing_weight	as processing_weight
										,case when person_1_viewing   = 1 and person_number = 1   then processing_weight*duration_of_session else 0 end as person_1
										,case when person_2_viewing   = 1 and person_number = 2   then processing_weight*duration_of_session else 0 end as person_2
										,case when person_3_viewing   = 1 and person_number = 3   then processing_weight*duration_of_session else 0 end as person_3
										,case when person_4_viewing   = 1 and person_number = 4   then processing_weight*duration_of_session else 0 end as person_4
										,case when person_5_viewing   = 1 and person_number = 5   then processing_weight*duration_of_session else 0 end as person_5
										,case when person_6_viewing   = 1 and person_number = 6   then processing_weight*duration_of_session else 0 end as person_6
										,case when person_7_viewing   = 1 and person_number = 7   then processing_weight*duration_of_session else 0 end as person_7
										,case when person_8_viewing   = 1 and person_number = 8   then processing_weight*duration_of_session else 0 end as person_8
										,case when person_9_viewing   = 1 and person_number = 9   then processing_weight*duration_of_session else 0 end as person_9
										,case when person_10_viewing  = 1 and person_number = 10  then processing_weight*duration_of_session else 0 end as person_10
										,case when person_11_viewing  = 1 and person_number = 11  then processing_weight*duration_of_session else 0 end as person_11
										,case when person_12_viewing  = 1 and person_number = 12  then processing_weight*duration_of_session else 0 end as person_12
										,case when person_13_viewing  = 1 and person_number = 13  then processing_weight*duration_of_session else 0 end as person_13
										,case when person_14_viewing  = 1 and person_number = 14  then processing_weight*duration_of_session else 0 end as person_14
										,case when person_15_viewing  = 1 and person_number = 15  then processing_weight*duration_of_session else 0 end as person_15
										,case when person_16_viewing  = 1 and person_number = 16  then processing_weight*duration_of_session else 0 end as person_16
										,person_1+person_2+person_3+person_4+person_5+person_6+person_7+person_8+person_9+person_10+person_11+person_12+person_13+person_14+person_15+person_16 as theflag
										--,case when  session_start_date_time >= local_tv_instance_start_date_time then session_start_date_time else local_tv_instance_start_date_time end as x
										--,case when  local_tv_instance_end_date_time <= session_end_date_time then local_tv_instance_end_date_time else session_end_date_time end as y
										,datediff(minute,instance_start,instance_end)	        as progwatch_duration
										,progwatch_duration * processing_weight as progscaled_duration
                                        ,broadcast_start_date_time_local
                                        ,broadcast_end_date_time_local
								from    ripolile.latest_barb_viewing_table  as viewing
										inner join  barb_weights			as weights
										on  viewing.household_number    = weights.household_number
								where   viewing.sky_stb_holder_hh = 'Y'
								and		cast(viewing.local_start_time_of_session as date) between @processing_date-29 and @processing_date
							)   as base
							inner join	(
											-- fixing barb sample to only barb panellists with Sky (table from prior step)
											select  house_id
													,max(person) as thesize
											from    skybarb
											group   by  house_id
										)   as barbskyhhsize
							on	base.household_number	= barbskyhhsize.house_id
							inner join  (
											-- mapping the db1 station code to the actual service key to find meta data for service key
											-- done on the join after this one...
											select  db1_station_code, service_key
											from    thompsonja.BARB_Channel_Map
											where   main_sk = 'Y'
										)   as map
							on  base.db1_station_code   = map.db1_station_code
							inner join  (
											-- getting metadata for service key
											select  service_key
													,channel_genre
													,channel_pack
											from    vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
											where   activex = 'Y'
										)   as ska
							on  map.service_key         = ska.service_key
					where   base.theflag > 0
				)   as mega
				inner join  skybarb as z
				on  mega.household_number   = z.house_id
				and mega.person_number      = z.person


		commit

		create hg index hg1 on skybarb_fullview     (service_key)
		create hg index hg2 on skybarb_fullview     (household_number)
		create lf index lf1 on skybarb_fullview     (channel_pack)
		create lf index lf2 on skybarb_fullview     (programme_genre)
		create dttm index dt1 on skybarb_fullview   (start_time_of_session)
		create dttm index dt2 on skybarb_fullview   (end_time_of_session)
		create dttm index dt3 on skybarb_fullview   (session_start_date_time)
		create dttm index dt4 on skybarb_fullview   (session_end_date_time)
		commit

		grant select on skybarb_fullview to vespa_group_low_security
		commit
				
		MESSAGE cast(now() as timestamp)||' | @ M04.1: Final BARB Data Preparation DONE' TO CLIENT
	
	
	end
	
	else
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M04.0: Missing Data on base tables for Data Preparation Stage!!!' TO CLIENT
		
	end

	
----------------------------
-- M04.3 - Returning Results	
----------------------------

	MESSAGE cast(now() as timestamp)||' | M04 Finished' TO CLIENT	
	
end;

commit;
grant execute on v289_m04_barb_data_preparation to vespa_group_low_security;
commit;