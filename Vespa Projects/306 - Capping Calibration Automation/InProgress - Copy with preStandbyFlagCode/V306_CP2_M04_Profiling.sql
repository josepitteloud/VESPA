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




**Module:                               V306_CP2_M04_Profiling

-- We don't need to profile each box on each different day, we're just going to profile
-- once a week (or something like that) at the beginning of the build week and use that
-- for the whole week. This way isn't going to be super robust against race conditions,
-- but the scheduler is fairly robust against two things running the same proc at the
-- same time. Still, we can also throw the build date onto the metadata table to ensure
-- things don't get desynchronised.

*/

-------------------------------------------------------------------------------------------------
-- J - WEEKLY PROFILING BULD OF BOX METADATA
-------------------------------------------------------------------------------------------------

create or replace procedure V306_CP2_M04_Profiling
                                                                                @profiling_thursday     date = NULL
as begin

        execute M00_2_output_to_logger '@ M04 : V306_CP2_M04_Profiling'
        COMMIT


        declare @QA_catcher             integer
        commit 

        -- Note that we've started the build:
        execute M00_2_output_to_logger  ' New week: Profiling boxes as of ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') ||'.'
        commit --;-- ^^ originally a commit

        -------------------------------------------------------------------------------------------------
        -- J01) CLEARING OLD STUFF OUT OF THE TABLE, REPOPULATING
        -------------------------------------------------------------------------------------------------
        execute M00_2_output_to_logger '@ M04 : Update CP2_relevant_boxes'
        COMMIT
        
        -- For the dev build we're using '2012-01-26' but now it's a proc we want to be able to fire
        -- in dates of our own choosing.
        --set @profiling_thursday = '2012-01-26'

        truncate table CP2_box_lookup
        commit --
        -- Yeah, the trick now is that no single loop will contain all the boxes we want to
        -- process, because we're only caching one day worth of caps at once. So we need to go
        -- over the daily tables again and pull out all the account numbers that we care about.

        -- We'd use temporary tables for these two guys, except that they get populated via
        -- some dynamic SQL, so temporary tables would fail (being inside a separate execution
        -- scope, sadface)
        truncate table CP2_relevant_boxes
        commit --


        declare @scanning_day               date
        commit

        set @scanning_day = dateadd(day, -1, @profiling_thursday)
        commit

        insert into     CP2_relevant_boxes
        select
                        account_number
                ,       subscriber_id
                ,       service_instance_id
        from    Capping2_00_Raw_Uncapped_Events
        where
                        /*event_start_date_time_utc     >=      @scanning_day
                and     event_start_date_time_utc       <=      dateadd(day, 1, @scanning_day) -- we could replace this with the variable @profiling_thursday
                and*/panel_id                                  in (11,12)
                and  account_number                            is not null
                and  subscriber_id                             is not null
                and  pre_standby_event_flag                    =  0  --remove any event that precedes a standby event
        group by
                        account_number
                ,       subscriber_id
                ,       service_instance_id
        commit
        
        execute M00_2_output_to_logger  ' Days processed: ' || dateformat(@scanning_day, 'dd/mm/yyyy') || '-' || dateformat(dateadd(day, 1, @scanning_day), 'dd/mm/yyyy')
        commit


        -- We also need to populate the CP2_box_lookup table:
        execute M00_2_output_to_logger '@ M04 : Update CP2_box_lookup'
        COMMIT
        
        insert into     CP2_box_lookup  (
                                                                                subscriber_id
                                                                        ,       account_number
                                                                        ,       service_instance_id
                                                                )
        select
                        subscriber_id
                ,       min(account_number)
                ,       min(service_instance_id)
        from    CP2_relevant_boxes
        where
                        subscriber_id   is not null -- dunno if there are any, but we need to check
                and     subscriber_id   <>      -1
                and     account_number  is not null
        group by        subscriber_id
        commit

        -- Maybe have some QA somewhere checking for duplication between account number / subscriber
        -- id / service instance id? the min(.) method is kind of ugly

        set @QA_catcher = -1
        commit

        select @QA_catcher = count(1)
        from CP2_box_lookup
        commit 
        
        execute M00_2_output_to_logger  ' J01: Complete! (Box lookup built) ' || coalesce(@QA_catcher, -1)
        commit 

        -------------------------------------------------------------------------------------------------
        -- J02) PRIMARY & SECONDARY BOX FLAGS
        -------------------------------------------------------------------------------------------------

        execute M00_2_output_to_logger '@ M04 : Determine Primary and Secondary STB flags'
        COMMIT
        
        execute DROP_LOCAL_TABLE 'CP2_deduplicated_accounts'
        commit  
        
        
        -- For pulling stuff out of the customer database: we would join on service instance ID,
        -- except that it's not indexed in cust_subs_hist. So instead we pull out everything for
        -- these accounts, and then join back on service instance ID later.
        select
                        account_number
                ,       1       as Dummy
        into    CP2_deduplicated_accounts
        from    CP2_relevant_boxes
        group by
                        account_number
                ,       Dummy
        commit
        
        create unique index fake_pk on CP2_deduplicated_accounts (account_number)
        commit 

        -- OK, now we can go get get P/S flgs:
        execute DROP_LOCAL_TABLE 'all_PS_flags'
        commit  

        select
                        csh.service_instance_id
                ,       case    csh.subscription_sub_type
                                when    'DTV Primary Viewing'           then    'P'
                                when    'DTV Extra Subscription'        then    'S'
                        end                             as PS_flag
        into    all_PS_flags
        from
                                        CP2_deduplicated_accounts       as      da
                inner join      cust_subs_hist                          as      csh             on      da.account_number                       =       csh.account_number
                                                                                                                        and     csh.SUBSCRIPTION_SUB_TYPE       in      (
                                                                                                                                                                                                                'DTV Primary Viewing'
                                                                                                                                                                                                        ,       'DTV Extra Subscription'
                                                                                                                                                                                                )
                                                                                                                        and     csh.status_code                         in      ('AC','AB','PC')
                                                                                                                        and     csh.effective_from_dt           <=      @profiling_thursday
                                                                                                                        and     csh.effective_to_dt                     >       @profiling_thursday
        group by
                        csh.service_instance_id
                ,       PS_flag
        commit 

        -- ^^ This guy, on the test build (300k distinct accounts) took 8 minutes. That's managable.


        -- OK, so building P/S off what's active on the Thursday could cause issues with
        -- recent activators not having subscriptions which give them flags, but I'm okay
        -- with there being a few 'U' entries for recent joiners to Sky for the first week
        -- they're on the Vespa panel. It's not about recently joining Vespa, it's about
        -- recently joining Sky, so it shouldn't be much of an issue at all.

        -- Index *should* be unique, but might not be if there are conflicts in Olive. So,
        -- more QA, check that these are actually unique.
        create index idx1 on all_PS_flags (service_instance_id)
        commit 

        update  CP2_box_lookup
        set     CP2_box_lookup.PS_flag  =       apsf.PS_flag
        from
                                        CP2_box_lookup
                inner join      all_PS_flags    as      apsf    on      CP2_box_lookup.service_instance_id      =       apsf.service_instance_id
        commit
        
        ---------------------------------------
        -- Clean up and finish
        ---------------------------------------
        execute DROP_LOCAL_TABLE 'CP2_deduplicated_accounts'
        commit  
        
        execute DROP_LOCAL_TABLE 'all_PS_flags'
        commit  
        -- Need some QA on the these numbers, including warning about guys still flagged
        -- as 'U', but the process all seems okay.

        set @QA_catcher = -1
        commit

        select @QA_catcher = count(1)
        from CP2_box_lookup
        where PS_flag in ('P', 'S')
        commit 

        execute M00_2_output_to_logger  ' J02: Complete! (Derive P/S per box) ' || coalesce(@QA_catcher, -1)
        commit 

end;
commit;

grant execute on V306_CP2_M02_Capping_Stage1 to vespa_group_low_security;
commit;
