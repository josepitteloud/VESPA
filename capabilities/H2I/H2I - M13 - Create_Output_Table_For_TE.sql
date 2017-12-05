
/*
create table V289_M13_individual_viewing_working_table (
        event_id bigint
        ,overlap_batch int
        ,account_number varchar(20)
        ,subscriber_id numeric(10)
        ,service_key int
        ,event_start_date_time timestamp
        ,event_end_date_time timestamp
        ,person_1 smallint
        ,person_2 smallint
        ,person_3 smallint
        ,person_4 smallint
        ,person_5 smallint
        ,person_6 smallint
        ,person_7 smallint
        ,person_8 smallint
        ,person_9 smallint
        ,person_10 smallint
        ,person_11 smallint
        ,person_12 smallint
        ,person_13 smallint
        ,person_14 smallint
        ,person_15 smallint
        ,person_16 smallint
)
;
create hg index hg1 on V289_M13_individual_viewing_working_table(event_id);
create hg index hg2 on V289_M13_individual_viewing_working_table(overlap_batch);




create table V289_M13_individual_viewing (
        VIEWING_DEVICE_HASH             bigint -- subscriber_id
        ,ACCOUNT_NUMBER_HASH            int
        ,STB_BROADCAST_START_TIME       timestamp
        ,STB_BROADCAST_END_TIME         timestamp
        ,STB_EVENT_START_TIME           timestamp
        ,TIMESHIFT                      int
        ,service_key                    int
        ,Platform_flag                  int
        ,Original_Service_key           int
        ,AdSmart_flag                   int
        ,DTH_VIEWING_EVENT_ID           bigint
        ,person_1                       smallint
        ,person_2                       smallint
        ,person_3                       smallint
        ,person_4                       smallint
        ,person_5                       smallint
        ,person_6                       smallint
        ,person_7                       smallint
        ,person_8                       smallint
        ,person_9                       smallint
        ,person_10                      smallint
        ,person_11                      smallint
        ,person_12                      smallint
        ,person_13                      smallint
        ,person_14                      smallint
        ,person_15                      smallint
        ,person_16                      smallint
)
;

create table V289_M13_individual_details (
        activity_date                   date
        ,account_number_hash            int
        ,person_number                  int
        ,ind_scaling_weight             double
        ,gender                         int
        ,age_band                       int
        ,head_of_hhd                    int
)
;
*/






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
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                                                                ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                                                                ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                                                              ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin

**Business Brief:

        This Module produces the final individual level viewing table that will be sent to Techedge

**Module:

        M06: DP Data Extraction
                        M06.0 - Initialising Environment
                        M06.1 - Composing Table Name
                        M06.2 - Data Extraction
                        M06.3 - Trimming Sample
                        M06.4 - Returning Results

--------------------------------------------------------------------------------------------------------------
*/





---------------------------------








-----------------------------------
-- M13.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_M13_Create_Final_Output_Tables
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M13.0 - Initialising Environment' TO CLIENT

	-- create variable @person_loop int
	-- create variable @sql_text varchar(1000)

	declare @person_loop int
	declare @sql_text varchar(1000)


	MESSAGE cast(now() as timestamp)||' | @ M13.0: Initialising Environment DONE' TO CLIENT




	-----------------------------------
	-- M13.1 - Transpose Individuals to Columns
	-----------------------------------

	truncate table V289_M13_individual_viewing_working_table
	commit

	-- Populate the working viewing table with a single copy of each viewing event and overlap batches where relevant
	insert into     V289_M13_individual_viewing_working_table
	select          dp.event_id
					,dp.overlap_batch
					,dp.account_number
					,dp.subscriber_id
					,dp_raw.service_key
					,dp.event_Start_utc
					,dp.event_end_utc
					,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	from            
					v289_M06_dp_raw_data 	as	dp_raw
		inner join 	V289_M07_dp_data 		as	dp 		on	dp_raw.pk_viewing_prog_instance_fact = dp.event_id
	commit


	-- Loop through each possible person (max 16) in a hhd and add their viewing

	set @person_loop = 1
	commit

	while @person_loop <= 16
	begin

			-- update events with person where overlap_batch match (i.e. overlap_batch is not null)
			set @sql_text =                    'update         V289_M13_individual_viewing_working_table m13 '
			set @sql_text = @sql_text ||       'set            person_' || @person_loop || ' = 1 '
			set @sql_text = @sql_text ||       'from           V289_M10_session_individuals m10 '
			set @sql_text = @sql_text ||       'where          m13.event_id = m10.event_id '
			set @sql_text = @sql_text ||       'and            m13.overlap_batch = m10.overlap_batch '
			set @sql_text = @sql_text ||       'and            m10.hh_person_number = ' || @person_loop
			commit

			execute (@sql_text)
			commit

			-- now update when overlap_batch is null (i.e. the event is not overlapping another in same hhd which is most of them)
			set @sql_text =                    'update         V289_M13_individual_viewing_working_table m13 '
			set @sql_text = @sql_text ||       'set            person_' || @person_loop || ' = 1 '
			set @sql_text = @sql_text ||       'from           V289_M10_session_individuals m10 '
			set @sql_text = @sql_text ||       'where          m13.event_id = m10.event_id '
			set @sql_text = @sql_text ||       'and            m13.overlap_batch is null '
			set @sql_text = @sql_text ||       'and            m10.overlap_batch is null '
			set @sql_text = @sql_text ||       'and            m10.hh_person_number = ' || @person_loop
			commit

			execute (@sql_text)
			commit

			set @person_loop = @person_loop + 1
			commit
	end




	-----------------------------------
	-- M13.2 - Final Viewing Output Table
	-----------------------------------


	-- This will need re-working to make sure we get the right data
	-- Also needs to be MA version for start/end times. Have cheated here for now

	truncate table V289_M13_individual_viewing
	commit

	insert into     V289_M13_individual_viewing
	select          0 -- haven't got table with VIEWING_DEVICE_HASH so put zero for now
					,demi.account_number_hash
					,dateround(mi, event_start_date_time, 1) -- as H2I applied to live only this is same as broadcast start time. Should be MA rather than rounded
					,dateround(mi, event_end_date_time, 1) -- as H2I applied to live only this is same as broadcast end time. Should be MA rather than rounded
					,event_start_date_time
					,0 --  as H2I applied to live only this is zero
					,service_key -- should be SD version
					,0 -- SD/HD need to capture this
					,service_key
					,1
					,event_id -- change this from the programme instance fact to dth viewing event id
					,person_1
					,person_2
					,person_3
					,person_4
					,person_5
					,person_6
					,person_7
					,person_8
					,person_9
					,person_10
					,person_11
					,person_12
					,person_13
					,person_14
					,person_15
					,person_16
	from            V289_M13_individual_viewing_working_table m13
					inner join DEMI on m13.account_number = demi.account_number -- for account_number_hash THIS WILL NEED TO CHANGE
	where           person_1 + person_2 + person_3 + person_4 + person_5 + person_6 + person_7 + person_8 + person_9 + person_10
																	+ person_11 + person_12 + person_13 + person_14 + person_15 + person_16 > 0
	commit



	-----------------------------------
	-- M13.3 - Final Individual Table
	-----------------------------------


	truncate table V289_M13_individual_details
	commit

	insert into	V289_M13_individual_details	(
													account_number_hash
												,	person_number
												,	ind_scaling_weight
												,	gender
												,	age_band
												,	head_of_hhd
											)
	select          
			demi.account_number_hash
		,	hh.hh_person_number
		,	w.scaling_weighting
		,	case
				when hh.person_gender = 'M' then 1
				when hh.person_gender = 'F' then 2
				else 99
			end		as	gender
		,	case
				when hh.person_age between 1 and 19 then 1
				when hh.person_age between 20 and 24 then 2
				when hh.person_age between 25 and 34 then 3
				when hh.person_age between 35 and 44 then 4
				when hh.person_age between 45 and 64 then 5
				when hh.person_age >= 65 then 6
				else 99
			end		as	age_band
		,	hh.person_head
	from
					V289_M08_sky_hh_composition 			as	hh
		inner join 	V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING 	as	w 	on	hh.account_number	=	w.account_number
																	and	hh.HH_person_number	=	w.HH_person_number
		inner join 	DEMI 											on	hh.account_number	=	demi.account_number


/* Where did this stuff come from?
		   account_number              varchar(20)     not null
							,HH_person_number           tinyint         not null
							,scaling_date               date            not null        -- date on which scaling is applied
							,scaling_weighting          float           not null
							,build_date
	---------------------------------------------------------------------------------------
*/

end -- procedure
;




