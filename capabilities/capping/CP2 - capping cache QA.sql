/******************************************************************************
**
** Project Vespa: Capping 2 - Automatic QA / Unit Tests
**
** Here are all the things we check in an automated manner whenever we refresh
** the Capping 2 cache with new data. 
**
** Refer to the main build script "CP2 - make capping cache.sql" for more
** details on the build and outstanding dev actions, or otherwise, at:
**
**  http://rtci/Vespa1/Capping.aspx
**
**
** Code sections:
**
**              Q01 - Dynamic naming, essential field population
**              Q02 - Duplication testing
**              Q03 - Out of bounds errors
**              Q04 - ??
**              Q05 - BARB allocation testing
**              Q06 - ??
**
** (Starting at section Q to follow on from secions aded to the same logger
** build run by the cap cache process.)
**
******************************************************************************/

-- So this guy wants to end up a procedure because we'll kick it off at the end
-- of each Capping cache, and the results will end up in... the logger or something.

/****************** What sort of QA are we going to be doing? ******************/

-- One thing at least on each of the QA table groups we're tracking...

if object_id('CP2_QA_on_cap_build') is not null
   drop procedure CP2_QA_on_cap_build;

create procedure CP2_QA_on_cap_build
    @target_date        date = NULL     -- Date of daily table caps to cache
    ,@CP2_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

    -- Needed for logging, of course:
    DECLARE @QA_catcher             integer

    EXECUTE citeam.logger_add_event @CP2_build_ID, 3, 'Section Q: Unit testing on caps for ' || convert(varchar(10),@target_date,123)
        
    /****************** Q01 - CHECKING DYNAMIC NAMING OF THINGS ******************/

    -- We'll also check the connection to the daily table too, because it's there.
    
    
    
    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from ...

    
    -- So we're also going to check that the really important fields are set...
    
    set @QA_catcher = -1
    
    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where viewing_starts is null
    
    commit

    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q01j: Viewing start time missing!', coalesce(@QA_catcher, -1)

    commit
    
    set @QA_catcher = -1
    
    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where viewing_stops is null
    
    commit

    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q01k: Viewing end time missing!', coalesce(@QA_catcher, -1)

    commit
    
    
    --wait for delay '00:00:02' - if therre's some problem with the logger getting it tables locked and thing like that, this delay might help... might have to put that in a lot of places like this then, eh?
    
    EXECUTE citeam.logger_add_event @CP2_build_ID, 3, 'Q01: Complete! (Dynamic naming)', coalesce(@QA_catcher, -1)
    commit


    /****************** Q02 - CHECKING DUPLICATION OF VIEWING DATA ******************/

    -- Okay, so for all the viewing data, there should be certain elements which are
    -- determined by the subscriber_id + Adjusted_Event_Start_Time combination:
    set @QA_catcher = -1
    
    select @QA_catcher = count(1) from (
    select subscriber_id, Adjusted_Event_Start_Time
        ,count(distinct account_number)             as an_dupes
        ,count(distinct X_Type_Of_Viewing_Event )   as type_dupes
        ,count(distinct X_Adjusted_Event_End_Time ) as endtime_dupes
        ,count(distinct X_Event_Duration)           as dur_dupes
        ,count(distinct event_start_hour)           as hour_dupes
        ,count(distinct event_start_day)            as day_dupes
        ,count(distinct Live)                       as live_dupes
    from CP2_viewing_records
    group by subscriber_id, Adjusted_Event_Start_Time
    having
        an_dupes        > 1 or
        type_dupes      > 1 or
        endtime_dupes   > 1 or
        dur_dupes       > 1 or
        hour_dupes      > 1 or
        day_dupes       > 1 or
        live_dupes      > 1
    ) as t

    -- If this gives you any results, that's bad, means we've got inconsistencies
    -- upstream, and that's no good.
    commit
    
    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q02a: Duplicates on ADJUSTED_EVENT_START_TIME!', coalesce(@QA_catcher, -1)
    
    commit

    -- Okay, also, we've noticed this strange style of duplicate, and traced it back to being
    -- able to see the same thing in the raw data, so now we're just reporting the weirdness.

    set @QA_catcher = -1
    
    select @QA_catcher = count(1) from (
        select subscriber_id, viewing_starts
        from CP2_capped_data_holding_pen
        group by subscriber_id, viewing_starts
        having count(1) > 1
    ) as t

    -- Um, we do have a few hits here. Oh well. See if they continue to track through... we'll
    -- be doing full rebuilds through the recalibration anyway, so we'll be able to track it then.
    
    commit
    
    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q02b: Duplicates on VIEWING_STARTS!', coalesce(@QA_catcher, -1)
    
    commit

    -- What else are we testing then? Other duplicates to consider?
    

    EXECUTE citeam.logger_add_event @CP2_build_ID, 3, 'Q02: Complete! (Viewing data dupes)'
    commit

    /****************** Q03 - OUT OF BOUNDS ERRORS ON VIEWING VS EVENTS ******************/
    
    -- So all the viewing times should be contained within the capped event times, and the
    -- capped event times should be contained within the uncapped event time for the same
    -- item. So let's check those things.
    
    set @QA_catcher = -1
    
    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where capped_event_end_time > x_adjusted_event_end_time
    
    commit

    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q03a: Capping extends viewing!', coalesce(@QA_catcher, -1)

    commit
    
    set @QA_catcher = -1
    
    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where viewing_starts < adjusted_event_start_time
    
    commit

    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q03b: Viewing starts strictly before event!', coalesce(@QA_catcher, -1)

    commit
    
    set @QA_catcher = -1
    
    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where viewing_starts > coalesce (capped_event_end_time, X_Adjusted_Event_End_Time)
    
    commit

    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q03c: Viewing starts after event ends!', coalesce(@QA_catcher, -1)

    commit
    
    set @QA_catcher = -1
    
    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where viewing_stops < adjusted_event_start_time
    
    commit

    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q03d: Viewing ends before event starts!', coalesce(@QA_catcher, -1)

    commit
    
    set @QA_catcher = -1
    
    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where viewing_stops > coalesce (capped_event_end_time, X_Adjusted_Event_End_Time)
    
    commit

    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q03e: Viewing ends strictly after event!', coalesce(@QA_catcher, -1)

    commit
    
    -- Checking that the view ends are after the view starts
    set @QA_catcher = -1
    
    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where datediff(ss, viewing_starts, viewing_stops) < 5 -- This "5" is also checking that small capped events are also removed
    
    -- Getting a bunch of hits for cases where capping ends the viewing event moments after a new
    -- programme starts. That's not entirely unexpected, but it still should be treated... probably?
    -- Because otherwise it leads to cases where a minute might be (BARB) allocated to a channel
    -- based on behaviour strictly outside of that minute, which is weird.
    
    commit

    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @SBV_build_ID, 2, 'Q03f: Small or negative viewing duration!', coalesce(@QA_catcher, -1)

    commit
    
    EXECUTE citeam.logger_add_event @CP2_build_ID, 3, 'Q03: Complete! (OOB errors)'
    commit
    
    /****************** Q04 - ?? ******************/



    /****************** Q05 - VALIDATING BARB MINUTE PROCESSING ******************/

    
    
    
    EXECUTE citeam.logger_add_event @CP2_build_ID, 3, 'Q03: Complete! (BARB minute - NYIP)'
    commit
        
    /****************** Q06 - CHECKING ?? ******************/

    


end;
commit;
go
