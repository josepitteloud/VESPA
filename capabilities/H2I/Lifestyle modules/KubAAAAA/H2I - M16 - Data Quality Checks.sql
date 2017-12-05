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
**Project Name:                         Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                        ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                        ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
										,Christopher Spencer	(Christopher.Spencer2@bskyb.com)
**Stakeholder:                          SkyIQ
                                        ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin

**Business Brief:

		Even before the project begins to run we need revise the data resource and guarantee the integrity of the data
		we will processing, this module is for that!
		
**Section:

        M16: Data Quality Checks
		
            M16.0 - Initialising Environment
			M16.1 - Checking KPIs through Metrics of Central Tendency
            M16.2 - Measuring Barb Integrity
            M16.3 - Measuring Vespa Integrity
			M16.4 - Are we good to go?
            M16.5 - Returning Results

--------------------------------------------------------------------------------------------------------------
*/

create or replace procedure v289_m16_data_quality_checks
	@proc_date 		date = null
	,@good_to_go	bit	output
as begin
----------------------------------
--M16.0 - Initialising Environment
----------------------------------

	declare @target	decimal(18,3)
	declare @query varchar(5000)
	declare @from_dt	integer
	declare @to_dt		integer
	declare @aux_dt     date

    set     @aux_dt     = @proc_date-14
	select  @from_dt 	= cast((dateformat(@proc_date-14,'YYYYMMDD')||'00') as integer)
	select  @to_dt 		= cast((dateformat(@proc_date,'YYYYMMDD')||'23') as integer)
	

	MESSAGE cast(now() as timestamp)||' | Begining M16.0 - Initialising Environment' TO CLIENT


	-- preparing v289_m16_barb_Check1
	
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_barb_Check1' TO CLIENT
	IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('v289_m16_barb_Check1'))		drop table v289_m16_barb_Check1
			
	commit
	
	select  thedate
			,count(1)    																							as nrows
			,sum(case when sky_stb_viewing = 'Y' then 1 else 0 end) 												as n_sky_viewing
			,sum(case when viewing_platform = 4 then 1 else 0 end)  												as n_digisat_viewing
			,count(distinct household_number)                       												as n_viewerhouseholds
			,sum(case when sky_stb_viewing = 'Y' and viewing_platform = 4 then sum_barb_inst_dur else null end)     as tot_min_watch_non_scaled
			,sum(case when sky_stb_viewing = 'Y' and viewing_platform = 4 then sum_barb_inst_dur_s else null end)	as tot_min_watch_scaled
			,sum(case when dur_min_v1<>dur_min_v2 then 1 else 0 end)/tot_min_watch_non_scaled    					as bug_on_durationofsession
			,sum(case when dur_min_v1<>dur_min_v3 then 1 else 0 end)/tot_min_watch_non_scaled    					as bug_on_barbinstanceduration
	into    v289_m16_barb_Check1
	from    (
				select  cast(local_start_time_of_session as date)                   as thedate
						,household_number
						,set_number
						,sky_stb_viewing
						,viewing_platform
						,local_start_time_of_session
						,local_end_time_of_session
						,max(duration_of_session)                                   as dur_min_v1
						,min(duration_of_session)                                   as dur_min_v2
						,sum(barb_instance_duration)                                as dur_min_v3
						,sum(barb_instance_duration*total_people_viewing)           as sum_barb_inst_dur
						,sum(barb_instance_duration*weighted_total_people_viewing)  as sum_barb_inst_dur_s
				--from    barb_daily_ind_prog_viewed
				from	ripolile.barb_daily_ind_prog_viewed_output
				where   sky_stb_holder_hh = 'Y'
				and     panel_or_guest_flag = 'Panel'
				and     cast(local_start_time_of_session as date) between @proc_date-29 and @proc_date
				group   by  thedate
							,household_number
							,set_number
							,sky_stb_viewing
							,viewing_platform
							,local_start_time_of_session
							,local_end_time_of_session
			)   as base
	group   by  thedate
	order   by  thedate desc

	commit
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_barb_Check1 DONE' TO CLIENT
	
	-- Preparing v289_m16_barb_checks2
	
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_barb_Check2' TO CLIENT
	IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('v289_m16_barb_Check2'))		drop table v289_m16_barb_Check2

		
	commit
	
	select  b.thedate
			,count(distinct household_number) as nhouseholds
			,count(distinct household_number||'-'||person_number) as  people
			,people - count(1)  as cardinality_sample
			,nhouseholds - sum(case when household_status in (4,2)  then 1 else 0 end) as cardinality_hoh
	into    v289_m16_barb_check2
	from    BARB_INDV_PANELMEM_DET  as a
			inner join  (
							select  cast((@proc_date-row_num+1) as date) as thedate
							FROM	sa_rowgenerator( 1, 15)
						)   as b
			on  b.thedate between date_valid_from and date_valid_to
	and     person_membership_status = 0
	group   by  b.thedate
	order   by  b.thedate desc

	commit
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_barb_Check2 DONE' TO CLIENT
	
	-- Preparing v289_m16_barb_checks3

	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_barb_Check3' TO CLIENT
	IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('v289_m16_barb_Check3'))		drop table v289_m16_barb_Check3

		
	commit
	
	select  thedate
			,count(distinct household_number)   as n_hh
			,sum(digital_hh)                    as n_digital
	into    v289_m16_barb_check3
	from    (
				select  skycap.thedate
						,whole.household_number
						,min(digital_satellite) as digital_hh
				from    BARB_PANEL_DEMOGR_TV_CHAR   as whole
						inner join  (
										select  distinct 
												timeframe.thedate
												,tv.household_number
										from    BARB_PANEL_DEMOGR_TV_CHAR   as tv
												inner join  (
																select  cast((@proc_date-row_num+1) as date) as thedate
																FROM	sa_rowgenerator( 1, 15)
															)   as timeframe
												on  timeframe.thedate between tv.date_valid_from and tv.date_valid_to
										and 	(
													reception_capability_code_1=2
													or reception_capability_code_2=2
													or reception_capability_code_3=2
													or reception_capability_code_4=2
													or reception_capability_code_5=2
													or reception_capability_code_6=2
													or reception_capability_code_7=2
													or reception_capability_code_8=2
													or reception_capability_code_9=2
													or reception_capability_code_10=2
												)
									)   as skycap
						on  whole.household_number  = skycap.household_number
						and skycap.thedate between whole.date_valid_from and whole.date_valid_to
				group	by	skycap.thedate
							,whole.household_number
			)   as base
	group   by  thedate            
	order   by  thedate desc

	commit
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_barb_Check3 DONE' TO CLIENT
	
	-- Preparing v289_m16_barb_check4

	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_barb_Check4' TO CLIENT
	IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('v289_m16_barb_Check4'))		drop table v289_m16_barb_Check4
			
	commit
	
	select  date_of_activity                                        as thedate
			,count(Distinct household_number||'-'||person_number)   as sample
			,sum(processing_weight/10)                              as sow
	into    v289_m16_barb_check4
	from    BARB_PANEL_MEM_RESP_WGHT
	where   date_of_activity between @proc_date-29 and @proc_date
	and     reporting_panel_code = 50
	group   by  thedate

	commit
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_barb_Check4 DONE' TO CLIENT
	
	-- preparing v289_m16_h2i_check1
	
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_h2i_check1' TO CLIENT
	IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('v289_m16_h2i_check1'))		drop table v289_m16_h2i_check1
			
	commit
	
	while 1=1
	begin

		set @query =    @query||'union '||
						'select  cast(cast((dk_event_start_datehour_dim/100) as varchar) as date) as thedate '||
								',count(1) as nrows '||
								',sum(case when dk_capping_end_datehour_dim <0 then 1 else 0 end)   as ncapped '||
								',sum(case when genre_description is null then 1 else 0 end)             as null_genres '||
						'from    SK_PROD.VESPA_DP_PROG_VIEWED_'||datepart(year,@aux_dt)||right(('00'||cast(datepart(month,@aux_dt) as varchar(2))),2)||' '||
						'where  dk_event_start_datehour_dim between '||@from_dt||' and '||@to_dt||' '||
						'group   by  thedate '
		if  datepart(year,@proc_date)||right(('00'||cast(datepart(month,@proc_date) as varchar(2))),2)
			<>
			datepart(year,@aux_dt)||right(('00'||cast(datepart(month,@aux_dt) as varchar(2))),2)
		begin
		
			MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_h2i_check1 - Period covering two different months' TO CLIENT
			set @aux_dt = @proc_date
			
		end
		else
		begin
			break
		end
	end

	set @query = substring(@query,7)

	execute ('select * into v289_m16_h2i_check1 from ( '||@query||' ) as base')
	commit
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_h2i_check1 DONE' TO CLIENT
	
	-- preparing v289_m16_h2i_check2
	
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_h2i_check2' TO CLIENT
	IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('v289_m16_h2i_check2'))		drop table v289_m16_h2i_check2

		
	commit
	
	select  adjusted_event_start_date_vespa as thedate
			,count(distinct account_number) as sample
	into    v289_m16_h2i_check2
	from    VIQ_VIEWING_DATA_SCALING
	where   adjusted_event_start_date_vespa between @proc_date-14 and @proc_date
	group   by  thedate

	commit
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Preparing v289_m16_h2i_check2 DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M16.0: Initialising Environment DONE' TO CLIENT

------------------------------------------------------------
-- M16.1 - Checking KPIs through Metrics of Central Tendency
------------------------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M16.1 - Checking KPIs through Metrics of Central Tendency' TO CLIENT

	while exists    (
						select  first test_result
						from    v289_m16_dq_mct_checks
						where   test_result = 'Pending' --> Any Tasks Pending ???
					)
	begin
		
		select  @query =    'select @target = '||target_field||' '||
							'from   '||target_table||' '||
							'where  thedate = '''||@proc_date||''''
		from    v289_m16_dq_mct_checks
		where   sequencer = (
								select  min(sequencer)
								from    v289_m16_dq_mct_checks
								where   test_result = 'Pending'
							)

		execute (@query)
		
		select  @query =    'update v289_m16_dq_mct_checks '||
							'set    processing_date = '''||@proc_date||''' '||
									',actual_value   = '||@target||' '||
									',test_result    = theresult '||
							'from    ( '||
										'select  avg('||target_field||') as x '||
												',case when x = 0 then 1 else x end as y '||
												',case when ((avg('||target_field||')-stddev('||target_field||'))/y)>'||tolerance||' then (case when (avg('||target_field||')-stddev('||target_field||'))<='||@target||' then ''Passed'' else ''Fail'' end) else ''High Dispercity'' end as theresult '||
										'from    '||target_table||' '||
									')  as base '||
							'where  sequencer = '||sequencer
		from    v289_m16_dq_mct_checks
		where   sequencer = (
								select  min(sequencer)
								from    v289_m16_dq_mct_checks
								where   test_result = 'Pending'
							)
		message @query to client
		execute (@query)
		commit

	end

	MESSAGE cast(now() as timestamp)||' | @ M16.1: Checking KPIs through Metrics of Central Tendency DONE' TO CLIENT

----------------------------------
--M16.2 - Measuring Barb Integrity
----------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M16.2 - Measuring Barb Integrity' TO CLIENT

	TRUNCATE TABLE v289_m16_dq_fact_checks
	
	insert  into v289_m16_dq_fact_checks    (
												source
												,test_context
												,processing_date
												,actual_value
												,test_result
											)
	select	'BARB'
			,'Barb Channel Map has observations'
			,@proc_date
			,count(1)                                        		as value_
			,case when value_ > 0 then 'Passed' else 'Failed' end   as result
	from    BARB_Channel_Map_v
	union
	select	'BARB'
			,'Barb Channel Map has Main SK'
			,@proc_date
			,sum(case when main_sk = 'Y' then 1 else 0 end)         as value_
			,case when value_ > 0 then 'Passed' else 'Failed' end   as result
	from    BARB_Channel_Map_v
	union
	select	'BARB'
			,'Barb Channel Map minimum level of SK association'
			,@proc_date
			,min(db1_count)                                         as value_
			,case when value_ > 0 then 'Passed' else 'Failed' end   as result
	from    BARB_Channel_Map_v

	commit


	MESSAGE cast(now() as timestamp)||' | @ M16.2: Measuring Barb Integrity DONE' TO CLIENT

-----------------------------------
--M16.3 - Measuring Vespa Integrity
-----------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M16.3 - Measuring Vespa Integrity' TO CLIENT
	
	insert  into v289_m16_dq_fact_checks    (
												source
												,test_context
												,processing_date
												,actual_value
												,test_result
											)
	select	'H2I'
			,'Full Scaling Sample Contained in Viewing Table'
			,@proc_date
			,count(distinct viq.account_number)																					as value_
			,case when count(distinct viq.account_number) = count(distinct sav.account_number) then 'Passed' else 'Failed' end	as results
	from    VIQ_VIEWING_DATA_SCALING				as viq
			inner join cust_single_account_view		as sav
			on	viq.account_number	= sav.account_number
	where	adjusted_event_start_date_vespa = @proc_date
	union
	select  'H2I'
			,'Accounts with NO cb_key_household values in Experian'
			,@proc_date
			,sum(case when exp.cb_key_household is null then 1 else 0 end)														        as value_
			,case when cast(value_ as float)/cast((count(distinct viq.account_number))as float)*100 <6 then 'Passed' else 'Failed' end  as results
	from    VIQ_VIEWING_DATA_SCALING			as viq
			inner join cust_single_account_view as sav
			on  viq.account_number  = sav.account_number
			left join experian_consumerview     as exp
			on	sav.cb_key_household = exp.cb_key_household
	where	adjusted_event_start_date_vespa = @proc_date
	union
	select  'H2I'
			,'No Duplicated Service Key in CM for active records'
			,@proc_date
			,count(distinct service_key)					                as value_
			,case when count(1) = value_ then 'Passed' else 'Failed' end    as results
	from    vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
	where   activex = 'Y'
	and     service_key >999 and service_key <65535
	union
	select  'H2I'
			,'Active Service Keys found'
			,@proc_date
			,count(1)									            as value_
			,case when count(1) > 0 then 'Passed' else 'Failed' end as results
	from    vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
	union
	select  'H2I'
			,'No Null cb_key_household in Experian'
			,@proc_date
			,count(1)									            as value_
			,case when count(1) = 0 then 'Passed' else 'Failed' end as results
	from    experian_consumerview
	where   cb_key_household is null
	union
	select  'H2I'
			,'Volume of null address (1) in Experian'
			,@proc_date
			,sum(case when CB_address_line_1 is not null then 1 else 0 end)						                as value_
			,case when cast(value_ as float) / cast(count(1) as float) >= 0.9 then 'Passed' else 'Failed' end   as results
	from    experian_consumerview
	union
	select  'H2I'
			,'Volume of Head of Households in PLAYPEN'
			,@proc_date
			,sum(cast(p_head_of_household as integer))              as value_
			,case when value_ > 0 then 'Passed' else 'Failed' end   as results
	from    PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD
	union
	select  'H2I'
			,'All exp_cb_key_db_person found in Experian are also in PLAYPEN'
            ,@proc_date
			,count(distinct playpen.exp_cb_key_db_person)                                                   as value_
			,case when value_ = count(distinct exp.exp_cb_key_db_person) then 'Passed' else 'Failed' end    as results
	from    PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD   as playpen
			left join experian_consumerview            as exp
			on  exp.exp_cb_key_db_person    = playpen.exp_cb_key_db_person
	
	MESSAGE cast(now() as timestamp)||' | @ M16.3 - Measuring Vespa Integrity DONE' TO CLIENT

----------------------------
--M16.4 - Are we good to go?
----------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M16.4 - Are we good to go?' TO CLIENT
	
	select  @good_to_go = min(theflag)
	from    (
				select case when count(1) = sum(case when test_result = 'Passed' then 1 else 0 end) then 1 else 0 end  as theflag
				from    v289_m16_dq_mct_checks
				union
				select  case when count(1) = sum(case when test_result = 'Passed' then 1 else 0 end) then 1 else 0 end  as theflag
				from    v289_m16_dq_fact_checks
			)   as base
	
	MESSAGE cast(now() as timestamp)||' | @ M16.4 - Are we good to go? DONE' TO CLIENT

---------------------------
--M16.5 - Returning Results
---------------------------

	drop table v289_m16_barb_check1
	drop table v289_m16_barb_check2
	drop table v289_m16_barb_check3
	drop table v289_m16_barb_check4
	drop table v289_m16_h2i_check1
	drop table v289_m16_h2i_check2
	commit

MESSAGE cast(now() as timestamp)||' | M16 Finished' TO CLIENT   

end;

commit;
grant execute on v289_m16_data_quality_checks to vespa_group_low_security;
commit;