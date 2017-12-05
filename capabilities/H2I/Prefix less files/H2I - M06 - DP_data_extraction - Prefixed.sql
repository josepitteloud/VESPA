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

	This Module goal is to generate the probability matrices from BARB data to be used for identifying
	the most likely candidate(s) of been watching TV at a given event...

**Module:
	
	M06: DP Data Extraction
			M06.0 - Initialising Environment
			M06.1 - Composing Table Name 
			M06.2 - Data Extraction
			M06.3 - Trimming Sample
			M06.4 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M06.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m06_DP_data_extraction
	@event_date date = null
	,@sample_proportion smallint = 100
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M06.0 - Initialising Environment' TO CLIENT
	

	declare @dp_tname 	varchar(50)
	declare @query		varchar(3000)
	declare @from_dt	integer
	declare @to_dt		integer
	
	set @dp_tname = 'SK_PROD.VESPA_DP_PROG_VIEWED_'
	select  @from_dt 	= cast((dateformat(@Event_date,'YYYYMMDD')||'00') as integer)
	select  @to_dt 		= cast((dateformat(@Event_date,'YYYYMMDD')||'23') as integer)
	
	if @Event_date is null
	begin
		MESSAGE cast(now() as timestamp)||' | @ M06.0: You need to provide a Date for extraction !!!' TO CLIENT
	end
	else
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M06.0: Initialising Environment DONE' TO CLIENT
-------------------------------
-- M06.1 - Composing Table Name
-------------------------------

		MESSAGE cast(now() as timestamp)||' | Begining M06.1 - Composing Table Name' TO CLIENT

		set @dp_tname = @dp_tname||datepart(year,@Event_date)||right(('00'||cast(datepart(month,@event_date) as varchar(2))),2) 

		MESSAGE cast(now() as timestamp)||' | @ M06.1: Composing Table Name DONE: '||@dp_tname  TO CLIENT
		
--------------------------
-- M06.2 - Data Extraction
--------------------------

		MESSAGE cast(now() as timestamp)||' | Begining M06.2 - Data Extraction' TO CLIENT

		if object_id('v289_M06_dp_raw_data') is not null
			truncate table v289_M06_dp_raw_data
			
		commit

		set @query =    'insert  into v289_M06_dp_raw_data  ('||
                                                            'pk_viewing_prog_instance_fact'||
                                                            ',dk_event_start_datehour_dim'||
															',dk_event_end_datehour_dim'||
                                                            ',dk_broadcast_start_Datehour_dim'||
                                                            ',dk_instance_start_datehour_dim'||
                                                            ',dk_viewing_event_dim'||
                                                            ',duration'||
                                                            ',genre_description'||
                                                            ',service_key'||
                                                            ',cb_key_household'||
                                                            ',event_start_date_time_utc'||
                                                            ',event_end_date_time_utc'||
                                                            ',account_number'||
                                                            ',subscriber_id'||
                                                            ',service_instance_id'||
															',programme_name'||
															',capping_end_Date_time_utc'||
															',broadcast_start_date_time_utc'||
															',broadcast_end_date_time_utc'||
															',instance_start_date_time_utc'||
															',instance_end_date_time_utc'||
                                                        ') '||
                        'select  pk_viewing_prog_instance_fact'||
                                ',dk_event_start_datehour_dim'||
								',dk_event_end_datehour_dim'||
                                ',dk_broadcast_start_Datehour_dim'||
                                ',dk_instance_start_datehour_dim'||
                                ',dk_viewing_event_dim'||
                                ',duration'||
                                ',case when genre_description in (''Undefined'',''Unknown'') then ''Unknown'' else genre_description end'||
                                ',service_key'||
                                ',c.household_key'||
                                ',event_start_date_time_utc'||
                                ',event_end_date_time_utc'||
                                ',a.account_number'||
                                ',subscriber_id'||
                                ',service_instance_id'||
								',programme_name'||
								',capping_end_Date_time_utc'||
								',broadcast_start_date_time_utc'||
								',broadcast_end_date_time_utc'||
								',instance_start_date_time_utc'||
								',instance_end_date_time_utc'||
                        ' from    V289_viewing_data_view as a '
								||'inner join  (
													select  account_number
															,min(cb_key_household)  as household_key
													from    V289_M08_SKY_HH_composition 
													group   by  account_number
												)   as c 
								on a.account_number = c.account_number '||
						
						'where dk_event_start_datehour_dim between '||@from_dt||' and '||@to_dt
						
						
		execute (@query)
		
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M06.2: Data Extraction DONE ROWS;'||@@rowcount  TO CLIENT
			
	
--------------------------
-- M06.3 - Trimming Sample
--------------------------
		
		if @sample_proportion < 100
		begin
				
			MESSAGE cast(now() as timestamp)||' | Begining M06.3 - Trimming Sample' TO CLIENT
			
			select  account_number
					,cast(account_number as float)          as random
			into	#aclist
			from    v289_M06_dp_raw_data
			group   by   account_number

			commit

			update  #aclist
			set     random  = rand(cast(account_number as float)+datepart(us, getdate()))

			commit

			select  distinct account_number
			into    #sample
			from    (
						select  *
								,row_number() over( order by random) as therow
						from    #aclist
					)   as base
			where   therow <=   (
									select  (count(1)*@sample_proportion)/100
									from    #aclist
								)

			commit
			
			delete  v289_M06_dp_raw_data
			where   account_number not in   (
												select  distinct
														account_number
												from    #sample
											)
											
			commit
		
			MESSAGE cast(now() as timestamp)||' | @ M06.3: Trimming Sample DONE' TO CLIENT
		
		end	
		
	
----------------------------
-- M06.4 - Returning Results
----------------------------

	end

	MESSAGE cast(now() as timestamp)||' | M06 Finished' TO CLIENT

end;

commit;
grant execute on v289_m06_DP_data_extraction to vespa_group_low_security;
commit;