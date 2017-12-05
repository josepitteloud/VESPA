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
                                                                                ,       @sample_size  tinyint
                                                                                ,       @VESPA_table_name       varchar(150)
as begin

        -- ########################################################################
        -- #### Capping State 2 - create AUG tables - CUSTOM                   ####
        -- ########################################################################
        -- Change months according to range of run

        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2'
        COMMIT

        --------------------------------------------------------------
        -- Initialise
        --------------------------------------------------------------

        declare @QA_catcher   integer   commit

        declare @sql_   varchar(5000)   commit

        --------------------------------------------------------------
        -- Select sample of accounts
        --------------------------------------------------------------

        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : take sample selection @ '  ||      @sample_size || '%'
        COMMIT


        --------------------------------------------------------------
        -- Identify non-duplicated VPIF keys
        --------------------------------------------------------------

        -- Clear table if necessary
        execute DROP_LOCAL_TABLE 'CP2_unique_vpif_keys'
        commit

        -- Get non-duplicated VPIF keys for the date of interest
        set     @sql_   =       '
                select
                                pk_viewing_prog_instance_fact
                        ,       count() as      cnt
                into    CP2_unique_vpif_keys
                from
                                                ' || @VESPA_table_name  || '    as      dpp
--                      inner join      CP2_accounts                    as      acc             on      dpp.account_number      =       acc.account_number
--                                                                                                                      and     acc.reference_date = ''' || @capping_date || '''
               where
                                --(
                                                (
                                                        dk_event_start_datehour_dim     between cast(dateformat(dateadd(day,0,''' || @capping_date || '''), ''yyyymmdd00'') as int)
                                                                                                                and             cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
                                                )
                                       -- and      (
                                       --                                  dk_event_end_datehour_dim       between cast(dateformat(dateadd(day,0,''' || @capping_date || '''), ''yyyymmdd00'') as int)
                                       --                                                                         and  cast(dateformat(dateadd(day,1,''' || @capping_date || '''), ''yyyymmdd01'') as int)
                                        --        )
                                --)
/*--Original all that start or end on x
                                (
                                                (
                                                        dk_event_start_datehour_dim     between cast(dateformat(''' || @capping_date || ''', ''yyyymmdd00'') as int)
                                                                                                                and             cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
                                                )
                                        or      (
                                                        dk_event_end_datehour_dim       between cast(dateformat(''' || @capping_date || ''', ''yyyymmdd00'') as int)
                                                                                                                and             cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
                                                )
                                )
*/
                        and     panel_id                                                        in      (11,12)
                        and     type_of_viewing_event                           is not null
                        and     type_of_viewing_event                           <>      ''Non viewing event''
                        -- and where end_time < 2am of @capping_date+1 -- v1 (extend second OR condition to between... and @capping_date+1 0100)
                        -- and end_time > midnight of @capping_date+1 -- v2 (equivalent to removing second OR condition on dk_event_end_datehour_dim)
                        -- all events starting on @capping_date-7 -- v3
                group by        pk_viewing_prog_instance_fact
                having  cnt     =       1
                commit
                '
        commit
        
        execute(@sql_)
        commit
        
        create unique hg index uhg on CP2_unique_vpif_keys(pk_viewing_prog_instance_fact)
        commit


        execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Non-duplicated VPIF instances identified.'
        COMMIT







        ---------------------------------------------------------------------------------------------------
        -- Create table that combines the non-duplicated VPIF keys and a flag signifying pre-standby events
        ---------------------------------------------------------------------------------------------------

        -- Clear table if necessary
        execute DROP_LOCAL_TABLE 'pre_standby_event_flag_tmp'

        set @sql_ = '
                select dpp.pk_viewing_prog_instance_fact,
                       --dpp.dk_viewing_event_dim,
                       null as dth_viewing_event_id,
                       cast(0 as bit) pre_standby_event_flag --updated in the next stage
                  into pre_standby_event_flag_tmp
                  from ' || @VESPA_table_name  || '             as      dpp
                         inner join  CP2_unique_vpif_keys       as      keys
                    on dpp.pk_viewing_prog_instance_fact = keys.pk_viewing_prog_instance_fact
                commit
                    '
         execute (@sql_)
        --28949632 Row(s) affected



        ---*********************************************************************************************---
        ---*********************************************************************************************---
        --   THIS SECTION USES AN
        --           E X T E R N A L L Y   C R E A T E D   T A B L E
        --           - - - - - - - - - -                to set the pre-stand-by event flags
        ---*********************************************************************************************---
        ---*********************************************************************************************---

        UPDATE pre_standby_event_flag_tmp         psef
           SET psef.pre_standby_event_flag = 1 --case when standby.pk_viewing_programme_instance_fact IS NOT NULL then cast(1 as bit) else cast(0 as bit) end
          FROM Capping_NZ_Extract_Standby_dth_viewing_event_id_tmp as standby  --               <--- EXTERNAL (and manually) built table [contains records that only lead to a standby event]
         WHERE psef.pk_viewing_prog_instance_fact = standby.pk_viewing_programme_instance_fact
        commit


        ----------------------------------------------------------------------------
        -- Create view to the appropriate monthly viewing table for the capping date
        ----------------------------------------------------------------------------

        set @sql_       =       '
                create or replace view  Capping2_00_Raw_Uncapped_Events as
                select  dpp.*,
                        keys.pre_standby_event_flag,   --flag added to identify events that preceed standby events
                        case  when (service_key=65535 or service_key < 1000)                                                    then 'PullVOD'
                              when service_key between 4094 and 4098                                                            then 'PushVOD'
                              when --type_of_viewing_event = 'Sky+ time-shifted viewing event'
                                live_recorded = 'RECORDED' and time_in_seconds_since_recording<=3600                            then 'VOSDAL_1hr'
                              when live_recorded = 'RECORDED' and time_in_seconds_since_recording between 3601 and 86400        then 'VOSDAL_1to24hr'
                              when live_recorded = 'RECORDED' and time_in_seconds_since_recording > 86400                       then 'Playback'
                              when live_recorded = 'LIVE'                                                                       then  'Live'
                              else 'Other' --doubt if this will ever trigger
                         end  as View_Type             --added to split playback events into Vosdal types, playback, & live
                from    ' || @VESPA_table_name  || '                    as      dpp
                          inner join      pre_standby_event_flag_tmp    as      keys
                  on    dpp.pk_viewing_prog_instance_fact = keys.pk_viewing_prog_instance_fact

--                        LEFT OUTER JOIN
--        Capping_NZ_Extract_Standby_dth_viewing_event_id_tmp as standby                                          -- table extracted and built from Netezza export
--         on standby.dth_viewing_event_id = dpp.dk_viewing_event_dim
/*                where
                                (
                                                (
                                                        dk_event_start_datehour_dim     between cast(dateformat(''' || @capping_date || ''', ''yyyymmdd00'') as int)
                                                                                                                and             cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
                                                )
                                        or      (
                                                        dk_event_end_datehour_dim       between cast(dateformat(''' || @capping_date || ''', ''yyyymmdd00'') as int)
                                                                                                                and             cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
                                                )
                                )
*/
                  where
                               -- (
                                                (
                                                        dk_event_start_datehour_dim     between cast(dateformat(dateadd(day,0,''' || @capping_date || '''), ''yyyymmdd00'') as int)
                                                                                                                and             cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
                                                )
                   -- and standby.standby_lead_event_action = ''Standby In'' -- <--- dont really need this restriction but lets add it just for safety (incase source data includes other types of event)
                                       -- and      (
                                        --                                 dk_event_end_datehour_dim       between cast(dateformat(dateadd(day,0,''' || @capping_date || '''), ''yyyymmdd00'') as int)
                                        --                                                                        and  cast(dateformat(dateadd(day,1,''' || @capping_date || '''), ''yyyymmdd01'') as int)
                                        --        )
                                --)
                    and     panel_id                                                        in      (11,12)
                    and     type_of_viewing_event                           is not null
                    and     type_of_viewing_event                           <>      ''Non viewing event''
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

