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

-----------------------------------------------------------------------------------

**Project Name:                         Capping Calibration Automation
**Analysts:                             Leonardo Ripoli  (Leonardo.Ripoli@sky.uk)
                                        Jonathan Green   (Jonathan.Green2@sky.uk)
                                        
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@sky.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FCapping%20Calibration%20Automation

**Business Brief:

Capping Calibration Automation ultimately aims to deliver improved Vespa viewing consumption through monthly alignment to BARB reference data. 
The Capping algorithm was developed in order to truncate the length of viewing events where there is no actual audience present (e.g. TV set off, but STB on and registering viewing).

Up to this point, the parameters and thresholds that feed into the Capping algorithm have been largely static since the time of the original algorithm development by IQ and ultimate implementation within EDM. 
Although a recent capping calibration exercise addressed exactly the issue realigning Vespa viewing to BARB, this was a highly manual process that required much resource to construct and perform the analyses and assessment. 
Capping Calibration Automation will adopt those calculations and fundamental approach, but delivered as a self-contained and automated process that calculates the best set of capping thresholds and parameters in IQ/Olive 
for ingest within into the EDM production environment

This project will also aim to increase the precision over which the Capping thresholds and parameters operate. For example, the current parameters are defined at the day-part level, 
each of which spans a number of hours in the day. The intention is to explore the possibility of redefining the parameters at the hourly level in order to give greater control over the alignment process against BARB. 
In theory, there should be little to no adjustment required to the actual flow of the Capping algorithm since the thresholds and parameters are contained in an external lookup table rather than being hard-coded in the SQL.




**Module:                               V306_CP2_M03_Capping_Stage2_phase2

create AUG tables - CUSTOM

*/

create or replace procedure V306_CP2_M03_Capping_Stage2_phase2
            @capping_date date
            ,@sample_size  tinyint
            ,@VESPA_table_name       varchar(150)
			,@VESPA_table_name_lag       varchar(150)
			,@lag_days	smallint
as begin

        -- ########################################################################
        -- #### Capping State 2 - create AUG tables - CUSTOM                   ####
        -- ########################################################################
        -- Change months according to range of run
		
		
		---??? cs
		--- changes required here and elsewhere for midinght boundary fix.
		--- first need to add LastWeek flag to various event files (work out which)
		--- also need a new Vespa table name file to capture events from the previous week (might not be the same table name)
		--- then run an additional piece of code to add last weeks hour 23 events

        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2'
        COMMIT

        --------------------------------------------------------------
        -- Initialise
        --------------------------------------------------------------
        
        declare @QA_catcher   integer   commit

        declare @sql_   varchar(5000)   commit
        
		declare @tx_perc	real commit
		
		declare @gmt_start     date                     -- To capture when the clocks go back in Autumn
        commit 
		declare @bst_start     date                     -- To capture when the clocks go forward in Spring
        commit 

        set @bst_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-03-31') -1),datepart(year, today()) || '-03-31')  -- to get last Sunday in March
        commit 
        set @gmt_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-10-31') -1),datepart(year, today()) || '-10-31')  -- to get last Sunday in October
        commit 

        
        --------------------------------------------------------------
        -- Select sample of accounts
        --------------------------------------------------------------

        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : take sample selection @ '  ||      @sample_size || '%'
        COMMIT


        --------------------------------------------------------------
        -- Identify non-duplicated VPIF keys
        --------------------------------------------------------------
        
        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Identify and retain non-duplicated VPIF keys'
        COMMIT


        -- Clear tables if necessary
        execute DROP_LOCAL_TABLE 'CP2_unique_vpif_keys'
        commit
		
        
        -- Get non-duplicated VPIF keys for the date of interest
        set     @sql_   =       '
                select
					pk_viewing_prog_instance_fact
                    ,account_number
                    ,cast(99 as smallint) as tx_day
                    ,cast(NULL as bigint)            as      dth_viewing_event_id
                    ,cast(0 as tinyint)                  as      pre_standby_event_flag --updated in the next stage
                    ,count() as cnt
					,case 
					when  datepart (hour,min(case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end)) 
					in (22,23)   then cast(1 as smallint)  
					else  cast(0 as smallint)  end  as lag_event_flag
                into CP2_unique_vpif_keys
                from
                ' || @VESPA_table_name  || ' as dpp
                where
                    date (case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						  else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end)
						  =''' || @capping_date || '''
                    and  panel_id in (11,12)
                    and  type_of_viewing_event is not null
                    and  type_of_viewing_event <> ''Non viewing event''
                group by
                    pk_viewing_prog_instance_fact
                    ,account_number
                having cnt = 1
                commit
                '
        commit
        
        execute(@sql_)
		commit

        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : CP2_unique_vpif_keys completed'
        COMMIT
		
        execute DROP_LOCAL_TABLE 'CP2_unique_vpif_keys_lag'
        commit
 
--- find the events from x days previously for the hours close to midnight
		
        set     @sql_   =       '
                select
					pk_viewing_prog_instance_fact
                    ,account_number
                    ,cast(99 as smallint) as tx_day
					,cast(NULL as bigint)            as      dth_viewing_event_id
                    ,cast(0 as tinyint)                  as      pre_standby_event_flag --updated in the next stage
                    ,count() as cnt
					,cast(2 as smallint)  as lag_event_flag
                into CP2_unique_vpif_keys_lag
                from
                ' || @VESPA_table_name_lag  || ' as dpp
                where
                    date (case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						  else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end)=''' || dateadd(dd,@lag_days,@capping_date) || '''
					and  datepart (hour,case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end) in (22,23)
                    and  panel_id in (11,12)
                    and  type_of_viewing_event is not null
                    and  type_of_viewing_event <> ''Non viewing event''
                group by
                    pk_viewing_prog_instance_fact
                    ,account_number
                having cnt = 1
                commit
                '
        commit		

        execute(@sql_)
		commit
		
		execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : CP2_unique_vpif_keys_lag completed'
        COMMIT
		
/*
        TX definition required - (log received date - event start date + 1)? Include TX+0 if there are any.
        TX+1 -> ntiling process
        TX+2/3 -> capping applied -- is this actually needed? how many get scaling weights? - shouldn't expect many
        MbM - still isolate to scaled accounts for reporting/TE
*/
        

        commit
        
        create unique hg index uhg on CP2_unique_vpif_keys(pk_viewing_prog_instance_fact)
        commit
        create hg index idx1 on CP2_unique_vpif_keys(account_number)
        commit
        create lf index idx2 on CP2_unique_vpif_keys(tx_day)
        commit


        create unique hg index uhg on CP2_unique_vpif_keys_lag(pk_viewing_prog_instance_fact)
        commit
        create hg index idx1 on CP2_unique_vpif_keys_lag(account_number)
        commit
        create lf index idx2 on CP2_unique_vpif_keys_lag(tx_day)
        commit


        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Non-duplicated VPIF instances identified.'
        COMMIT

        
        
        -- Calculate TX day for each instance/event. Do this separately from the initial VPIF deduping so as not to re-introduce duplicates at that level.

        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Calculate TX Day for each instance'
        COMMIT

        set     @sql_   =       '
                update  CP2_unique_vpif_keys
                set tx_day = datediff (day,dpp.event_start_date_time_utc,dpp.netezza_audit_timestamp)
                from
                CP2_unique_vpif_keys as a
                inner join ' || @VESPA_table_name  || ' as dpp
                on  dpp.pk_viewing_prog_instance_fact = a.pk_viewing_prog_instance_fact
                and  dpp.account_number = a.account_number
				and                     date (case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						  else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end)
						  =''' || @capping_date || '''
--                and  date (dpp.event_start_date_time_utc)=''' || @capping_date || '''
                and  dpp.panel_id in (11,12)
                and  dpp.type_of_viewing_event is not null
                and  dpp.type_of_viewing_event <> ''Non viewing event''
                commit
                '
        commit
                                                        
        execute(@sql_)
        commit
        

        set     @sql_   =       '
                update  CP2_unique_vpif_keys_lag
                set tx_day = datediff (day,dpp.event_start_date_time_utc,dpp.netezza_audit_timestamp)
                from
                CP2_unique_vpif_keys_lag as a
                inner join ' || @VESPA_table_name_lag  || ' as dpp
                on  dpp.pk_viewing_prog_instance_fact = a.pk_viewing_prog_instance_fact
                and  dpp.account_number = a.account_number
--                and  date (dpp.event_start_date_time_utc)=''' || dateadd(dd,@lag_days,@capping_date) || '''
--				and datepart (hour,dpp.event_start_date_time_utc) in (22,23)
				and date (case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						  else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end)=''' || dateadd(dd,@lag_days,@capping_date) || '''
					and  datepart (hour,case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end) in (22,23)
                and  dpp.panel_id in (11,12)
                and  dpp.type_of_viewing_event is not null
                and  dpp.type_of_viewing_event <> ''Non viewing event''
                commit
                '
        commit
                                                        
        execute(@sql_)
        commit
        
-- add together and dedupe again (could get duplicates via the adding process)		


-- INSERT INTO CP2_unique_vpif_keys SELECT * FROM CP2_unique_vpif_keys_lag

/* --- can use this as a means of deduping when amended

        if object_id('Capping2_tmp_View_dupe_Culling_1') is not null drop table Capping2_tmp_View_dupe_Culling_1
        commit --; --^^ to be removed

        -- First off: Kick out the duplicates out that come in from the weird day wrapping stuff
        select
                        subscriber_id
                ,       adjusted_event_start_time
                ,       X_Viewing_Start_Time
                ,       min(ID_Key)                             as Min_ID_Key
        into    Capping2_tmp_View_dupe_Culling_1
        from    Capping2_01_Viewing_Records
        group by
                        subscriber_id
                ,       adjusted_event_start_time
                ,       X_Viewing_Start_Time
        commit --;-- ^^ originally a commit

        create unique index idx1 on Capping2_tmp_View_dupe_Culling_1 (Min_ID_Key)
        commit --;-- ^^ originally a commit

                        -- Delete records with non-existing ID_Key in the deduped table
        delete from     Capping2_01_Viewing_Records
        from
                                        Capping2_01_Viewing_Records                     a
                left join       Capping2_tmp_View_dupe_Culling_1        b       on      a.ID_Key        =       b.Min_ID_Key
        where b.Min_ID_Key is null
        commit --;-- ^^ originally a commit

*/


		
		-- First need to do check on numbers to be deleted - if it is less than 20% say then do it, if it is more then do not delete (something has gone wrong during reprocessing)
		
        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Retain only those events up to TX+3'
        COMMIT

		set @tx_perc=100*(select count(*) from CP2_unique_vpif_keys where tx_day  between 0 and 3 group by 1)/ (select count(*) from CP2_unique_vpif_keys group by 1)
		
		execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : tx_perc is ' ||      @tx_perc 
		
		if @tx_perc < 80 
        begin
                update  CP2_unique_vpif_keys
                set tx_day = 0
                from
                CP2_unique_vpif_keys as a
                execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : tx_day disabled for filtering'
                commit
        end
        
     
		set @tx_perc=100*(select count(*) from CP2_unique_vpif_keys_lag where tx_day  between 0 and 3 group by 1)/ (select count(*) from CP2_unique_vpif_keys_lag group by 1)
		
		execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : tx_perc for lagged events is ' ||      @tx_perc 
		
		if @tx_perc < 80  
		begin
                update  CP2_unique_vpif_keys_lag
                set tx_day = 0
                from
                CP2_unique_vpif_keys_lag as a
                execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : tx_day disabled for filtering (lag events)'
                commit
        end		
		
		-- Retain only those events up to TX+3
        delete from CP2_unique_vpif_keys
        where tx_day not between 0 and 3
        commit

		delete from CP2_unique_vpif_keys_lag
        where tx_day not between 0 and 3
        commit


        -----------------------------------------------------------------------------------------------------------------------------------------------------
        -- Now also apply sample trimming. This was previously performed by joining into the above, but we now need to factoring in unscaled accounts as well
        -----------------------------------------------------------------------------------------------------------------------------------------------------

        execute DROP_LOCAL_TABLE 'CP2_unscaled_accounts'
        commit
        
        select
            a.account_number
			,rand(number()) as rand_num
        into CP2_unscaled_accounts
        from
            CP2_unique_vpif_keys  a
            left join
			CP2_accounts b 
			on a.account_number = b.account_number
        where b.account_number is null
        group by a.account_number        -- shouldn't need this, but deduping just in case!
        commit
        


        -- Apply sample trimming on unscaled accounts (the scaled version was performed earlier in V306_CP2_M03_Capping_Stage2_phase1)
        -- For now, this sample can change between iterations, whereas the scaled account sample is fixed per capping date.
        delete from CP2_unscaled_accounts
        where rand_num > (@sample_size/ 100.0)
        commit
        

        delete from CP2_unique_vpif_keys
        from
        CP2_unique_vpif_keys a
        left join (
        select  account_number
        from CP2_accounts
        union all
        select  account_number
        from    CP2_unscaled_accounts
        ) b on a.account_number = b.account_number
        where a.account_number is null
        commit

        execute DROP_LOCAL_TABLE 'CP2_unscaled_accounts_lag'
        commit
        
        select
            a.account_number
			,rand(number()) as rand_num
        into CP2_unscaled_accounts_lag
        from
            CP2_unique_vpif_keys_lag  a
            left join
			CP2_accounts_lag b 
			on a.account_number = b.account_number
        where b.account_number is null  
        group by a.account_number        -- shouldn't need this, but deduping just in case!
        commit



        -- Apply sample trimming on unscaled accounts (the scaled version was performed earlier in V306_CP2_M03_Capping_Stage2_phase1)
        -- For now, this sample can change between iterations, whereas the scaled account sample is fixed per capping date.
        delete from CP2_unscaled_accounts_lag
        where rand_num > (@sample_size/ 100.0)
        commit
        

        delete from CP2_unique_vpif_keys_lag
        from
        CP2_unique_vpif_keys_lag a
        left join (
        select  account_number
        from CP2_accounts_lag
        union all
        select  account_number
        from    CP2_unscaled_accounts_lag
        ) b on a.account_number = b.account_number
        where a.account_number is null
        
        
  
        ---*********************************************************************************************---
        ---*********************************************************************************************---
        --   THIS SECTION USES AN
        --           E X T E R N A L L Y   C R E A T E D   T A B L E - if it exists
        --           - - - - - - - - - -                to set the pre-stand-by event flags
        ---*********************************************************************************************---
        ---*********************************************************************************************---
      
        
        
        if exists  (
            select 1
              from sysobjects
             where [name] = 'Capping_NZ_Extract_Standby_dth_viewing_event_id_tmp'
               and uid = user_id()
               and upper([type]) = 'U'
        )
            BEGIN
                UPDATE CP2_unique_vpif_keys         uvk
                   SET uvk.pre_standby_event_flag = standby.pre_standby_event_flag --case when standby.pk_viewing_programme_instance_fact IS NOT NULL then cast(1 as bit) else cast(0 as bit) end
                  FROM Capping_NZ_Extract_Standby_dth_viewing_event_id_tmp as standby  --               <--- EXTERNAL (and manually) built table [contains records that only lead to a standby event] change to flag in viewing table when available
                 WHERE uvk.pk_viewing_prog_instance_fact = standby.pk_viewing_programme_instance_fact
                commit

                execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : PreStandby events updated<'||@@rowcount||'>'
                COMMIT
                UPDATE CP2_unique_vpif_keys_lag         uvk
                   SET uvk.pre_standby_event_flag = standby.pre_standby_event_flag --case when standby.pk_viewing_programme_instance_fact IS NOT NULL then cast(1 as bit) else cast(0 as bit) end
                  FROM Capping_NZ_Extract_Standby_dth_viewing_event_id_tmp as standby  --               <--- EXTERNAL (and manually) built table [contains records that only lead to a standby event] change to flag in viewing table when available
                 WHERE uvk.pk_viewing_prog_instance_fact = standby.pk_viewing_programme_instance_fact
                commit

                execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : PreStandby events updated - lags<'||@@rowcount||'>'
                COMMIT				
            END
        else     
            BEGIN
                execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : PreStandby events table not available. No pre-standby events'
                COMMIT 
            END


        ----------------------------------------------------------------------------
        -- Create view to the appropriate monthly viewing table for the capping date
        ----------------------------------------------------------------------------
                
        set @sql_   =  '
                create or replace view  Capping2_00_Raw_Uncapped_Events as
                select  dpp.*
				,keys.tx_day
                ,keys.pre_standby_event_flag
				,keys.lag_event_flag
				,case  when (service_key=65535 or service_key < 1000)     	then ''PullVOD''
                    when service_key between 4094 and 4098            		then ''PushVOD''
                    when --type_of_viewing_event = Sky+ time-shifted viewing event
                            live_recorded = ''RECORDED'' and time_in_seconds_since_recording<=3600    then ''VOSDAL_1hr''
                    when live_recorded = ''RECORDED'' and time_in_seconds_since_recording between 3601 and 86400        then ''VOSDAL_1to24hr''
                    when live_recorded = ''RECORDED'' and time_in_seconds_since_recording > 86400                       then ''Playback''     -- (Recorded and Pull VOD - Recorded_Ntile)
                    when live_recorded = ''LIVE''                                                                       then  ''Live''        -- (uses THRESHOLD_NTILEor/then	THRESHOLD_NONTILE)
                    else ''Other'' --doubt if this will ever trigger
                end  as View_Type  --added to split playback events into Vosdal types, playback, & live
                from
                ' || @VESPA_table_name  || '  as dpp
                inner join CP2_unique_vpif_keys as keys 
				on dpp.pk_viewing_prog_instance_fact = keys.pk_viewing_prog_instance_fact
                where
                date (case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						  else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end)=''' || @capping_date || '''
                and panel_id in (11,12)
                and type_of_viewing_event is not null
                and type_of_viewing_event <> ''Non viewing event''
				union all 
				  select  dpp.*
				,keys.tx_day
				,keys.pre_standby_event_flag
				,keys.lag_event_flag
				,case  when (service_key=65535 or service_key < 1000)     	then ''PullVOD''
                    when service_key between 4094 and 4098            		then ''PushVOD''
                    when --type_of_viewing_event = Sky+ time-shifted viewing event
                            live_recorded = ''RECORDED'' and time_in_seconds_since_recording<=3600    then ''VOSDAL_1hr''
                    when live_recorded = ''RECORDED'' and time_in_seconds_since_recording between 3601 and 86400        then ''VOSDAL_1to24hr''
                    when live_recorded = ''RECORDED'' and time_in_seconds_since_recording > 86400                       then ''Playback''     -- (Recorded and Pull VOD - Recorded_Ntile)
                    when live_recorded = ''LIVE''                                                                       then  ''Live''        -- (uses THRESHOLD_NTILEor/then	THRESHOLD_NONTILE)
                    else ''Other'' --doubt if this will ever trigger
                end  as View_Type  --added to split playback events into Vosdal types, playback, & live
                from
                ' || @VESPA_table_name_lag  || '  as dpp
                inner join CP2_unique_vpif_keys_lag as keys 
				on dpp.pk_viewing_prog_instance_fact = keys.pk_viewing_prog_instance_fact
                where
               date (case
                          when dpp.EVENT_START_DATE_TIME_UTC between  dateadd(hh, 1, convert(datetime, '''||@bst_start||''')) and dateadd(hh, 2, convert(datetime, '''||@gmt_start||''')) then dateadd(hour, 1, dpp.EVENT_START_DATE_TIME_UTC) 
						  else dpp.EVENT_START_DATE_TIME_UTC --  in GMT
                          end)=''' || dateadd(dd,@lag_days,@capping_date) || '''
                and panel_id in (11,12)
                and type_of_viewing_event is not null
                and type_of_viewing_event <> ''Non viewing event''
				
				
                commit
        '
        commit
        
        execute (@sql_)
        commit
        
        
        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Working view created : Capping2_00_Raw_Uncapped_Events'
        COMMIT
        

/*
commit                                                          
execute logger_create_run 'Capping2.x CUSTOM', 'Weekly capping run', @varBuildId output

select @QA_catcher = count(1)
from CP2_duplicated_keys

execute logger_add_event @varBuildId, 3, 'Number of duplicated keys: ', coalesce(@QA_catcher, -1)
*/

end;
commit;

grant execute on V306_CP2_M03_Capping_Stage2_phase2 to vespa_group_low_security;
commit;

