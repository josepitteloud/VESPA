/*
--------------------------------------------------------------------------------------------------------------
**Project Name:                                         Vespa Executive Dashboard
**Analysts:                                                     Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                                                      Jose Loureda
**Stakeholder:                                          Vespa Directors / Managers.
**Due Date:                                                     22/02/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                            http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fRegular%20reports
                                                                        %2fMeta%2fExecutive%20Dashboard&FolderCTID=&View={95B15B22-959B-4B62-809A-AD43E02001BD}
                                                                        
**Business Brief:

Module responsible for managing the assamblage of derivations into the output tables for future extractions from VBA...

**Module's Sections:

M04: XDash Output Manager
                M04.0 - Initialising environment
                M04.1 - Assembling output
                M04.2 - QAing results
                M04.3 - Returning results

--------------------------------------------------------------------------------------------------------------
*/

--------------------------------------
/* M04.0 - Initialising environment */
--------------------------------------

if object_id('vespa_xdash_m04_outputmanager') is not null
        drop procedure vespa_xdash_m04_outputmanager;

commit;
go

create procedure vespa_xdash_m04_outputmanager
        @task           varchar(15) = null
        ,@log_id        bigint = null
as begin

        declare @histviewman    varchar(15)
        declare @adhocs                 varchar(15)
        declare @trafficlights  varchar(15)
        declare @dpdreturn              varchar(15)
        declare @vespabalance   varchar(15)
        declare @resultto               date
        declare @resultfrom             date
        declare @module_id              varchar(3)
        declare @exe_status             integer
        declare @counter                integer
        
        set @histviewman        = 'histviewman'
        set @adhocs                     = 'adhocs'
        set @trafficlights      = 'trafficlights'
        set @dpdreturn          = 'dpdreturn'
        set @vespabalance       = 'vespabalance'
        set @module_id          = 'M04'
        
        
-------------------------------
/* M04.1 - Assembling output */
-------------------------------

-- To derive the history of accounts transmitting data back to us we need to:
-- 1) Derive the period missing based on max date in history table vs today
-- 2) once we have the dates, we trigger the toolbox 01 (Deriving accounts returning data for a date range)
-- 3) Dump that result into our hist table, and that's it... (I guess)

-- assamble the history, which will be also used by the adhoc metrics...

        if @task = @histviewman
                begin
                        
                        execute @exe_status = vespa_analysts.vespa_xdash_m06_histviewman @log_id

                        execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M06', @exe_status
                        
                        select  @resultto = max(weekending)
                        from    vespa_analysts.vespa_xdash_hist_acinteractionkpi

                        set @resultfrom = @resultto -30

                        insert  into vespa_analysts.vespa_xdash_o2_histviewman
                        select  *
                        from    vespa_analysts.vespa_xdash_hist_acinteractionkpi
                        where   weekending between @resultfrom and @resultto
                        
                        commit 
                        
                        set @counter = -1
                        
                        select  @counter = count(1)
                        from    vespa_analysts.vespa_xdash_o2_histviewman
                        
                        execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : Hist Sample size', @counter
                        
                end
                
                
        -- get the adhoc metrics and present them in the right shape...
        if @task = @adhocs
                begin
                        
                        execute @exe_status = vespa_analysts.vespa_sp_xdash_m05_adhoxmetrics
                        
                        execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M05', @exe_status
                        
                        update  vespa_analysts.vespa_xdash_o1_adhocmetrics
                        set             lastweek        = metric.valueweek
                                        ,lastmonth      = metric.valuemonth
                        from    vespa_analysts.vespa_xdash_o1_adhocmetrics                             as adhoc
                                        inner join      vespa_analysts.vespa_xdash_stage_adhocmetrics  as metric
                                        on adhoc.metric = metric.metricref
                                        
                        commit
                        
                        set @counter = -1
                        
                        select  @counter = count(1)
                        from    vespa_analysts.vespa_xdash_o1_adhocmetrics
                        where   (lastweek       is null or lastweek     <= 0)
                        or              (lastmonth      is null or lastmonth    <=0)
                        
                        execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : Figs on 0 or null', @counter
                        
                end             

        -- Extracting Traffic lights...
        if @task = @trafficlights
                begin

                        insert  into vespa_analysts.vespa_xdash_o3_trafficlights
                        select  *
                        from    (vespa_analysts.vespa_sp_xdash_m07_panelbalanceextract())
                        
                        commit
                        
                        insert  into vespa_analysts.vespa_xdash_o6_vespaPanelBalanceHist
                        select  *
                        from    (vespa_analysts.vespa_sp_xdash_m10_vespaPanelBalanceHist())
                        
                        commit
                        
                        -- M07 QA...
                        set @counter = -1
                        
                        select  @counter = count(1)
                        from    vespa_analysts.vespa_xdash_o3_trafficlights
                        
                        execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : traffic lights derived', @counter

                        -- M10 QA...
                        set @counter = -1
                        
                        select  @counter = count(1)
                        from    vespa_analysts.vespa_xdash_o6_vespaPanelBalanceHist
                        
                        execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : traffic lights hist. caught', @counter            
                        
                end
        
        -- Extract the Daily Panel Return history (based on 5 weeks) to display trend of figures...
        if @task = @dpdreturn
                begin

                        insert  into vespa_analysts.vespa_xdash_o4_dpdreturnextract
                        select  *
                        from    (vespa_analysts.vespa_sp_xdash_m08_dpdreturnextract())
                        
                        commit
                        
                        if (
								select  count(1)
								from    vespa_analysts.vespa_xdash_o4_dpdreturnextract
							) = 5
						begin
							execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : dpd 5 weeks in place'
						end
                        else
						begin
							execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : dpd history incomplete'
						end
                        
                end
        
        -- Extract latest Scaling results to display how balanced the Vespa Panel is...
        if @task = @vespabalance
                begin
						/*cortb (22-04-2014) commented out as it was causing errors
                        insert  into vespa_analysts.vespa_xdash_o5_vespaPanelBalance
                        select  *
                        from    (vespa_analysts.vespa_sp_xdash_m09_vespaPanelBalance())
						*/
						
                        execute vespa_analysts.vespa_sp_xdash_m09_vespaPanelBalance
						
                        commit
                        
                        if (
								select  count(1)
								from    vespa_analysts.vespa_xdash_o5_vespaPanelBalance
							) = 5
						begin
							execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : dpBalance 5 weeks in place'
						end
                        else
						begin
							execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : dpBalance history incomplete'
						end
                                
                end
---------------------------
/* M04.2 - QAing results */
---------------------------
-- commig soon...
-------------------------------
/* M04.3 - Returning results */
-------------------------------
-- NYIP...
end;

commit;

