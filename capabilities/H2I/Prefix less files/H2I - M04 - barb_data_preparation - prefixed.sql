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
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M04.0 - Initialising Environment' TO CLIENT
	
	
    declare @a int
    declare @b int
    
    select	@a = count(1)
    from	BARB_PVF04_Individual_Member_Details
    
    select	@b = count(1)
	from	Barb_skytvs

	if @a > 0 and @b > 0
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

		select  members.*
		into    skybarb
		from    (
					-- defining the household demographic as needed for the most recent file loaded into barb schema...
					select  household_number										as house_id
							,person_number											as person
							,datepart(year,today())-datepart(year,date_of_birth) 	as age
							,case   when sex_code = 1 then 'Male'
									when sex_code = 2 then 'Female'
									else 'Unknown'
							end     as sex
							,case   when household_status in (4,2)  then 1
									else 0
							end     as head
					from    BARB_PVF04_Individual_Member_Details
					where   date_valid_for_db1 = (select max(date_valid_for_db1) from BARB_PVF04_Individual_Member_Details)
				)   as members
				inner join  (
								-- this join here is to fix our sample to only those barb panellists with Sky providers...
								select  distinct 
										household_number
								from    Barb_skytvs
								where   reception_capability_code1 = 2
							)   as skytvs
				on  members.house_id    = skytvs.household_number

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
					select  sch.genre_description as programme_genre
							,ska.channel_pack
							,ska.service_key
							,sch.programme_name
							,sch.broadcast_start_date_time_local
							,sch.broadcast_end_date_time_local
							,barbskyhhsize.thesize	as hhsize
							,base.*
							,case when  base.session_start_date_time >= sch.broadcast_start_date_time_local then base.session_start_date_time else sch.broadcast_start_date_time_local end as x
							,case when  broadcast_end_date_time_local <= base.session_end_date_time then broadcast_end_date_time_local else base.session_end_date_time end as y
							,datediff(minute,x,y)	as progwatch_duration
							,progwatch_duration * base.processing_weight as progscaled_duration
					from    (
								-- multiple aggregations to derive part of the day where the viewing session took place
								-- and a workaround to get the minutes watched per each person in the household multiplied
								-- by their relevant weights to show the minutes watched by UK (as per barb scaling exercise)...
								select  a.household_number
										,a.start_time_of_session
										,a.end_time_of_session
										,a.duration_of_session
										,a.db1_station_code
										,case when start_time_of_recording is null then start_time_of_session else start_time_of_recording end as session_start_date_time
										,case when start_time_of_recording is null then end_time_of_session else dateadd(mi, Duration_of_session, start_time_of_recording) end as session_end_date_time -- -1 because of minute attribution
										,case   when cast(start_time_of_session as time) between '00:00:00.000' and '05:59:00.000' then 'night'
												when cast(start_time_of_session as time) between '06:00:00.000' and '08:59:00.000' then 'breakfast'
												when cast(start_time_of_session as time) between '09:00:00.000' and '11:59:00.000' then 'morning'
												when cast(start_time_of_session as time) between '12:00:00.000' and '14:59:00.000' then 'lunch'
												when cast(start_time_of_session as time) between '15:00:00.000' and '17:59:00.000' then 'early prime'
												when cast(start_time_of_session as time) between '18:00:00.000' and '20:59:00.000' then 'prime'
												when cast(start_time_of_session as time) between '21:00:00.000' and '23:59:00.000' then 'late night'
										end     as session_daypart
										,b.person_number
										,b.processing_weight/10 as processing_weight
										,case when a.person_1_viewing   = 1 and person_number = 1   then b.processing_weight*a.duration_of_session else 0 end as person_1
										,case when a.person_2_viewing   = 1 and person_number = 2   then b.processing_weight*a.duration_of_session else 0 end as person_2
										,case when a.person_3_viewing   = 1 and person_number = 3   then b.processing_weight*a.duration_of_session else 0 end as person_3
										,case when a.person_4_viewing   = 1 and person_number = 4   then b.processing_weight*a.duration_of_session else 0 end as person_4
										,case when a.person_5_viewing   = 1 and person_number = 5   then b.processing_weight*a.duration_of_session else 0 end as person_5
										,case when a.person_6_viewing   = 1 and person_number = 6   then b.processing_weight*a.duration_of_session else 0 end as person_6
										,case when a.person_7_viewing   = 1 and person_number = 7   then b.processing_weight*a.duration_of_session else 0 end as person_7
										,case when a.person_8_viewing   = 1 and person_number = 8   then b.processing_weight*a.duration_of_session else 0 end as person_8
										,case when a.person_9_viewing   = 1 and person_number = 9   then b.processing_weight*a.duration_of_session else 0 end as person_9
										,case when a.person_10_viewing  = 1 and person_number = 10  then b.processing_weight*a.duration_of_session else 0 end as person_10
										,case when a.person_11_viewing  = 1 and person_number = 11  then b.processing_weight*a.duration_of_session else 0 end as person_11
										,case when a.person_12_viewing  = 1 and person_number = 12  then b.processing_weight*a.duration_of_session else 0 end as person_12
										,case when a.person_13_viewing  = 1 and person_number = 13  then b.processing_weight*a.duration_of_session else 0 end as person_13
										,case when a.person_14_viewing  = 1 and person_number = 14  then b.processing_weight*a.duration_of_session else 0 end as person_14
										,case when a.person_15_viewing  = 1 and person_number = 15  then b.processing_weight*a.duration_of_session else 0 end as person_15
										,case when a.person_16_viewing  = 1 and person_number = 16  then b.processing_weight*a.duration_of_session else 0 end as person_16
										,person_1+person_2+person_3+person_4+person_5+person_6+person_7+person_8+person_9+person_10+person_11+person_12+person_13+person_14+person_15+person_16 as theflag
								from    barb_rawview            as a
										inner join barb_weights as b
										on  a.household_number      = b.household_number
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
											from    CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_V
											where   activex = 'Y'
										)   as ska
							on  map.service_key         = ska.service_key
							inner join  (
											-- incorporating all programmes watched throughout the session
											select  service_key
													,broadcast_start_date_time_local
													,broadcast_end_date_time_local
													,genre_description
													,programme_name
											from    VESPA_PROGRAMME_SCHEDULE_V
											where   broadcast_start_date_time_utc >= '2013-01-01 00:00:00.000' 
											and     broadcast_start_date_time_utc < '2014-01-01 00:00:00.000'
										)   as sch
							on  ska.service_key                 = sch.service_key
							and sch.broadcast_start_date_time_local   <= base.session_end_date_time
							and sch.broadcast_end_date_time_local     > base.session_start_date_time
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