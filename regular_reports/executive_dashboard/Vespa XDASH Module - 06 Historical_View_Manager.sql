/*
--------------------------------------------------------------------------------------------------------------
**Project Name:                     Vespa Executive Dashboard
**Analysts:                         Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                          Jose Loureda
**Stakeholder:                      Vespa Directors / Managers.
**Due Date:                         22/02/2013
**Project Code (Insight Collation):
**Sharepoint Folder:                http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fRegular%20reports
                                    %2fMeta%2fExecutive%20Dashboard&FolderCTID=&View={95B15B22-959B-4B62-809A-AD43E02001BD}
                                    
**Business Brief:

Module responsible for Assembling the historical view of account returning data and their reporting quality for each Sky week...

**Module's Sections:

M06: Historical View Manager
        M06.0 - Initialising environment
        M06.1 - Deriving Metric(s)
        M06.2 - QAing results
        M06.3 - Returning results

--------------------------------------------------------------------------------------------------------------
*/

--------------------------------------
/* M06.0 - Initialising environment */
--------------------------------------
if object_id ('vespa_xdash_m06_histviewman')is not null
    drop procedure vespa_xdash_m06_histviewman

commit
go

create procedure vespa_xdash_m06_histviewman
    @log_id bigint = null
as begin

    declare @histfrom       date
    declare @histto         date
    declare @resultfrom     date
    declare @resultto       date
    declare @toolbox_dp     varchar(2)
    declare @toolbox_ap     varchar(2)
    declare @toolbox_dpn    varchar(3)
    declare @toolbox_tn     varchar(50)
    declare @module_id      varchar(3)
    declare @counter        integer
    declare @flag           tinyint

    set @toolbox_dp     = 'dp'
    set @toolbox_ap     = 'ap'
    set @toolbox_dpn    = 'dpn'
    set @module_id      = 'M06'
    set @flag           = 0

    create table #xdash_hist_trans_stage(
        thedate_        date
        ,account_number varchar(20)
        ,panel_id       tinyint
        ,box_count      tinyint
        ,expected_boxes tinyint
        ,weekending     date
    )
    --------------------------------
    /* M06.1 - Deriving Metric(s) */
    --------------------------------

    --loop to keep the table up-to-date

    while @flag = 0
    begin

                --1) Derive the dates that are missing in the hist, needed for this run...

                select  @histfrom = max(weekending) + 1
                from    vespa_analysts.vespa_xdash_hist_acinteractionkpi

                if @histfrom is null
                                begin
                                        -- Calculating last sunday 5 weeks ago to build up the history we need...
                                        select @histfrom = cast((today() - 37) as date) - (datepart(weekday,cast((today() - 37) as date)) - 1)
                                end

                select  @histto = min(thedate)
                from    (
                                        select  'dp' as viewingtable
                                                        ,cast(cast((max(dk_event_start_datehour_dim)/100) as varchar(8))as date) as thedate
                                        from    sk_prod.vespa_dp_prog_viewed_current
                                        /*union
                                        select  'ap'        as viewingtable
                                                        ,max(dt)    as thedate
                                        from    vespa_analysts.panel_data*/ -- cortb commented out as not sure why is needed 11/09/2014
                                )       as base


                if (datediff(day,@histfrom,@histto) >= 6)   -- Is there enough data for the full week to construct the history?...
                                begin
                                                
                                                delete from #xdash_hist_trans_stage

                                                set @histto = @histfrom + 6 -- Forcing the ToDate to be next saturday of FromDate...

                                                execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : building hist from', @histfrom
                                                execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : building hist to', @histto

                                                --Selecting Sample from Daily Panel Viewing Events...
                                                execute vespa_analysts.vespa_toolbox_acreturndata @histfrom,@histto,@toolbox_dp,@toolbox_tn output
                                                                -- select @toolbox_tn --> table name is 'toolbox_t_acreturndata'...

                                                insert  into #xdash_hist_trans_stage
                                                select  *
                                                                ,null -- as weekending...
                                                from    vespa_analysts.toolbox_t_acreturndata

                                                commit

                                                --Selecting Sample from Daily Panel Non Viewing Events...
                                                /*execute vespa_analysts.vespa_toolbox_acreturndata @histfrom,@histto,@toolbox_dpn,@toolbox_tn output

                                                insert  into #xdash_hist_trans_stage
                                                select  *
                                                                                ,null -- as weekending...
                                                from    vespa_analysts.toolbox_t_acreturndata

                                                commit*/ -- Commenting this out we are no long recieving non-viewing event data for daily panel

                                                --Selecting Sample from Alter Panels Viewing Events...
                                                execute vespa_analysts.vespa_toolbox_acreturndata @histfrom,@histto,@toolbox_ap,@toolbox_tn output

                                                insert  into #xdash_hist_trans_stage
                                                select  *
                                                                ,null -- as weekending...
                                                from    vespa_analysts.toolbox_t_acreturndata

                                                commit -- 15 min estimated run Up to here...

                                                -- Need to deduplicate account number population as I'm pulling from daily panel viewed and daily panel non viewed...

                                                select  *
                                                into    #xdash_hist_trans_stage_2
                                                from    (
                                                                        select  *
                                                                                        ,rank() over    (   
                                                                                                                                partition by    thedate_
                                                                                                                                                                ,account_number
                                                                                                                                order by        box_count       asc
                                                                                                                        ) as ranking
                                                                        from    (
                                                                                                select  distinct *
                                                                                                from    #xdash_hist_trans_stage -- filtering out duplicated records...
                                                                                        )       as base                           -- Then flaging those duplicated by back population...
                                                                                ) as X
                                                where   ranking = 1                                     -- And finally, getting unique records from the sample...

                                                
                                                --2) For those missing dates, Derive weekending

                                                update  #xdash_hist_trans_stage_2
                                                set     weekending =    case
                                                                                                                when datepart(weekday,thedate_) = 7
                                                                                                                then thedate_
                                                                                                                else (thedate_ + ( 7 - datepart(weekday,thedate_))) -- forcing to pick up the saturday parting from thedate...
                                                                                                end
                                                where   weekending is null

                                                commit


---------------------------
/* M06.2 - QAing results */
---------------------------

                --comming soon...

-------------------------------
/* M06.3 - Returning results */
-------------------------------

                                                --3) For those missing dates, update xdash hist table with new derivations
                                                --  (Get reliability metrics from SBV hist table (This one is gonna be new))...

                                                insert  into vespa_analysts.vespa_xdash_hist_acinteractionkpi
                                                select  hist.weekending
                                                                                ,hist.account_number
                                                                                ,hist.panel_id
                                                                                ,round(avg(hist.box_count),0)
                                                                                ,round(avg(hist.expected_boxes),0)
                                                                                ,min(qacheck.reporting_quality)
                                                from    #xdash_hist_trans_stage_2 as hist
                                                                                left join vespa_analysts.vespa_sbv_hist_qualitycheck as qacheck
                                                                                on  hist.account_number = qacheck.account_number
                                                                                and hist.weekending     = qacheck.weekending
                                                group   by  hist.weekending
                                                                                                ,hist.account_number
                                                                                                ,hist.panel_id
                                                                                
                                                commit

                                                set @counter = -1

                                                select  @counter = count(1)
                                                from    #xdash_hist_trans_stage_2

                                                execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : New AC into hist', @counter

                                                drop table #xdash_hist_trans_stage_2

                                                commit
                                                
                                end

                else
                                begin
                                                drop table #xdash_hist_trans_stage
                                                set @flag = 1
                                end

    end

commit

end;

commit;



