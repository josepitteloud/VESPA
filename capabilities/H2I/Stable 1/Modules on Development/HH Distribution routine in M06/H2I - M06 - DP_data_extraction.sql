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


		MESSAGE cast(now() as timestamp)||' | Resetting panel_flag = 1 to all panel accounts' TO CLIENT

			UPDATE V289_M08_SKY_HH_composition
			SET panel_flag = 1 
			FROM V289_M08_SKY_HH_composition as a 
			JOIN (SELECT DISTINCT account_number FROM VIQ_VIEWING_DATA_SCALING  WHERE adjusted_event_start_date_vespa = @event_date )   as viq ON a.account_number    = viq.account_number
			
			
			
		----------------------------
		-- M00.x - Adjusting VESPA hh distribution to fit BARB
		----------------------------
				MESSAGE cast(now() as timestamp)||' | @ M04.3: Adjusting VESPA HH distribution according to BARB' TO CLIENT
			
				DECLARE @tot  INT
						
				SELECT  1 as id
						,  ind
						, CAST(count(house_id) AS FLOAT)    AS hits
						, SUM(weight) 						AS t_weight
						, sum(hits) OVER (ORDER BY id)      AS total
						, sum(t_weight) OVER (ORDER BY id)  AS total_weighted
						, prop = hits / total
						, prop_w = t_weight / total_weighted
						, CAST (0 AS INT)                   AS vespa_hit
						, CAST (0 AS INT)                   AS min_count
						, CAST (0 AS INT)                   AS rebase
				INTO #perb
				FROM    (SELECT house_id, count(DISTINCT skybarb.person) ind, max(weights.processing_weight)  weight
						 FROM skybarb 
						 JOIN 	barb_weights AS weights ON skybarb.house_id = weights.household_number
														AND skybarb.person = weights.person_number
						 GROUP BY house_id) as f
				JOIN    (SELECT DISTINCT household_number FROM skybarb_fullview WHERE DATE (start_time_of_Session) = @event_date ) as b  ON b.household_number = f.house_id
				GROUP bY ind

				SELECT @tot = MAX(ind) 
				FROM #perb
				
				UPDATE V289_M08_SKY_HH_composition
				SET panel_flag = 0 
				WHERE household_size > @tot
				
				SELECT a.account_number
					, min(household_size) hh_size
					, min(row_id)	roww
					, RAND(datepart(us,now()) + roww) AS rnd
				INTO #hVespa
				FROM V289_M08_SKY_HH_composition as a
				GROUP BY a.account_number

				SELECT *, rankk = rank() OVER (partition BY hh_size ORDER BY rnd)
				INTO #tVespa
				FROM #hVespa

				SELECT
					hh_size
					, count(account_number) hhs
				INTO #perv
				FROM #hVespa as g
				GROUP BY hh_size

				UPDATE #perb
				SET vespa_hit = hhs,
					min_count = hhs/prop
				FROM #perb as a
				JOIN #perv AS b ON a.ind = b.hh_size

				SELECT @tot = MIN(min_count)
				FROM #perb

				UPDATE #perb
				SET rebase = prop * @tot
				COMMIT

				DELETE FROM #tVespa
				FROM #tVespa    AS a
				JOIN #perb      AS b ON b.ind = a.hh_size  AND rankk > rebase
				COMMIT
				
				UPDATE V289_M08_SKY_HH_composition
				SET panel_flag = 0 
				WHERE account_number NOT IN (SELECT account_number FROM #tVespa)
			---------------------------------------------------------------------------------------------------
			---------------------------------------------------------------------------------------------------			


--------------------------
-- M06.2 - Trimming Sample
--------------------------
		
		if @sample_proportion < 100
		begin
				
			MESSAGE cast(now() as timestamp)||' | Begining M06.2 - Trimming Sample' TO CLIENT
			
			COMMIT
			
			select  account_number
					,cast(account_number as float)          as random
			into	#aclist
			from    V289_M08_SKY_HH_composition
			WHERE panel_flag = 1 
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
			
			UPDATE V289_M08_SKY_HH_composition
			SET PANEL_FLAG = 0 
			WHERE account_number NOT IN (SELECT account_number FROM #sample)
		
			MESSAGE cast(now() as timestamp)||' | @ M06.2: Trimming Sample DONE: '||@@rowcount TO CLIENT
		
		end	





--------------------------
-- M06.3 - Data Extraction
--------------------------

		MESSAGE cast(now() as timestamp)||' | Begining M06.3 - Data Extraction' TO CLIENT

                if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_M06_dp_raw_data')
                        and     tabletype = 'TABLE')		
			truncate table v289_M06_dp_raw_data
			
		commit

                if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_m06_pseudo')
                        and     tabletype = 'TABLE')		
		 -- verifying our step table is free for re-usage
			drop table v289_m06_pseudo
		
		commit


		-- Dedupe household keys per account from our sample
		select
				account_number
			,	min(cb_key_household)  as household_key
		into	#account_household_keys
		from    V289_M08_SKY_HH_composition 
		WHERE PANEL_FLAG = 1 
		group by	account_number
		commit
		
		create unique hg index idx1 on #account_household_keys(account_number)
		commit


		-- Now pull in data for the sample from the viewing tables
		set @query =    /*'insert  into v289_M06_dp_raw_data  ('||
                                                            'pk_viewing_prog_instance_fact'||
															',dth_event_id'||
                                                            ',dk_event_start_datehour_dim'||
															',dk_event_end_datehour_dim'||
                                                            ',dk_broadcast_start_Datehour_dim'||
                                                            ',dk_instance_start_datehour_dim'||
                                                            --',dk_viewing_event_dim'||
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
															',dk_barb_min_start_datehour_dim'||
															',dk_barb_min_start_time_dim'||
															',dk_barb_min_end_datehour_dim'||
															',dk_barb_min_end_time_dim'||
															',barb_min_start_date_time_utc'||
															',barb_min_end_date_time_utc'||
															') '||*/
                        'select  pk_viewing_prog_instance_fact'||
								',viewing_event_id'||
								',dk_event_start_datehour_dim'||
								',dk_event_end_datehour_dim'||
                                ',dk_broadcast_start_Datehour_dim'||
                                ',dk_instance_start_datehour_dim'||
                                --',dk_viewing_event_dim'||
                                ',duration'||
                                ',case when genre_description in (''Undefined'',''Unknown'') then ''Unknown'' else genre_description end as genre_description'||
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
								',dk_barb_min_start_datehour_dim'||
								',dk_barb_min_start_time_dim'||
								',dk_barb_min_end_datehour_dim'||
								',dk_barb_min_end_time_dim'||
								',barb_min_start_date_time_utc'||
								',barb_min_end_date_time_utc'||
								',live_recorded'||
						' into	v289_m06_pseudo'||
                        ' from    '||@dp_tname||' as a '
								||'inner join  #account_household_keys   as c 
								on a.account_number = c.account_number '||
						
						'where '||
							'dk_event_start_datehour_dim between '||@from_dt||' and '||@to_dt||
							-- ' and dk_barb_min_start_datehour_dim  <>  -1'||
							-- ' and dk_barb_min_start_time_dim      <>  -1'||
							' and service_key is not null'
						
						
		execute (@query)
		commit

		-- Add indices to working table
		create hg index key1 on v289_m06_pseudo(pk_viewing_prog_instance_fact)
		create hg index hg0 on v289_m06_pseudo(viewing_event_id)
		create hg index hg1 on v289_m06_pseudo(dk_event_start_datehour_dim)
		create hg index hg2 on v289_m06_pseudo(dk_broadcast_start_datehour_dim)
		create hg index hg3 on v289_m06_pseudo(dk_instance_start_datehour_dim)
		--create hg index hg4 on v289_m06_pseudo(dk_viewing_event_dim)
		create hg index hg5 on v289_m06_pseudo(service_key)
		create hg index hg6 on v289_m06_pseudo(account_number)
		create hg index hg7 on v289_m06_pseudo(subscriber_id)
		create hg index hg8 on v289_m06_pseudo(programme_name)
		create hg index hg9 on v289_m06_pseudo(dk_barb_min_start_datehour_dim)
		create hg index hg10 on v289_m06_pseudo(dk_barb_min_start_time_dim)
		create hg index hg11 on v289_m06_pseudo(dk_barb_min_end_datehour_dim)
		create hg index hg12 on v289_m06_pseudo(dk_barb_min_end_time_dim)
		create hg index hg13 on v289_m06_pseudo(barb_min_start_date_time_utc)
		create hg index hg14 on v289_m06_pseudo(barb_min_end_date_time_utc)
		create lf index lf1 on v289_m06_pseudo(genre_description)
		create lf index lf2 on v289_m06_pseudo(live_recorded)
		create dttm index dttm1 on v289_m06_pseudo(barb_min_start_date_time_utc)
		create dttm index dttm2 on v289_m06_pseudo(barb_min_end_date_time_utc)
		commit


		-- Clean up
		drop table #account_household_keys
		commit
		
		
		MESSAGE cast(now() as timestamp)||' | @ M06.3: Data Extraction !!!!!SHIELD AGAINST DUPLICATED PKS!!!!!!' TO CLIENT
		/*
			AD: 12-02-2015:
			This is a redundancy check we add as we saw data issues before where the PK of the Viewing tables is duplicated
			(haha sweet irony), but yeah it happens... so we shield against this few number of cases that were making the project
			crash at this stage
		*/
		select  pk_viewing_prog_instance_fact
		into    #templist
		from    v289_m06_pseudo -- 27261399
		group   by  pk_viewing_prog_instance_fact
		having  count(1) > 1
		commit
		
		create unique hg index idx1 on #templist(pk_viewing_prog_instance_fact)
		commit
		

		delete from v289_m06_pseudo as a
		from    #templist   as b
		where   a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact

		commit
		drop table #templist
		commit
		MESSAGE cast(now() as timestamp)||' | @ M06.3: Data Extraction !!!!!SHIELD AGAINST DUPLICATED PKS!!!!!! DONE. ROWS DELETED:'||@@rowcount  TO CLIENT
		
		insert  into v289_M06_dp_raw_data  (
												pk_viewing_prog_instance_fact
												,dth_event_id
												,dk_event_start_datehour_dim
												,dk_event_end_datehour_dim
												,dk_broadcast_start_Datehour_dim
												,dk_instance_start_datehour_dim
												--',dk_viewing_event_dim
												,duration
												,genre_description
												,service_key
												,cb_key_household
												,event_start_date_time_utc
												,event_end_date_time_utc
												,account_number
												,subscriber_id
												,service_instance_id
												,programme_name
												,capping_end_Date_time_utc
												,broadcast_start_date_time_utc
												,broadcast_end_date_time_utc
												,instance_start_date_time_utc
												,instance_end_date_time_utc
												,dk_barb_min_start_datehour_dim
												,dk_barb_min_start_time_dim
												,dk_barb_min_end_datehour_dim
												,dk_barb_min_end_time_dim
												,barb_min_start_date_time_utc -- STB broadcast (already MA) - check this
												,barb_min_end_date_time_utc
												,live_recorded
						)
		select	*
		from	v289_m06_pseudo
		
		MESSAGE cast(now() as timestamp)||' | @ M06.3: Data Extraction DONE ROWS:'||@@rowcount  TO CLIENT
		
		commit
		drop table v289_m06_pseudo
		commit




		
		
----------------------------
-- M06.4 - Returning Results
----------------------------

	end	--	if @Event_date is null {...}, else...

	MESSAGE cast(now() as timestamp)||' | M06 Finished' TO CLIENT

end;

commit;
grant execute on v289_m06_DP_data_extraction to vespa_group_low_security;
commit;