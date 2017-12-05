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

Module responsible for deriving Adhoc metrics coming from various sources precalculated on previous processes...

**Module's Sections:

M05: Ad-Hoc Metrics
                M05.0 - Initialising environment
                M05.1 - Deriving Metric(s)
                M05.2 - QAing results
                M05.3 - Returning results

--------------------------------------------------------------------------------------------------------------
*/


---------------------------------------
/* M05.0 - Initialising environment  */
---------------------------------------

if object_id ('vespa_sp_xdash_m05_adhoxmetrics') is not null
        drop procedure vespa_sp_xdash_m05_adhoxmetrics

commit
go

create procedure vespa_sp_xdash_m05_adhoxmetrics
as
begin

        -- Declaring local constants...
        Declare @dp12   tinyint
        Declare @dp11   tinyint
        Declare @alt5   tinyint
        Declare @alt6   tinyint
        Declare @alt7   tinyint

        set @dp12   = 12
                set @dp11       = 11
                set @alt5   = 5
        set @alt6   = 6
        set @alt7   = 7

        -- Declaring local variables...
        declare @maxdate date

        if object_id('vespa_analysts.vespa_xdash_stage_adhocmetrics') is not null
                drop table vespa_analysts.vespa_xdash_stage_adhocmetrics
                
        commit

        -- creating a transition table to temporarly store metrics...
        create table vespa_analysts.vespa_xdash_stage_adhocmetrics(
                metricref       varchar(20)
                ,valueweek      decimal
                ,valuemonth     decimal
        )

        commit


        --------------------------------
        /* M05.1 - Deriving Metric(s) */
        --------------------------------

        -- Deriving : Vespa Consent

        -- Last Week:

        -- Feeding from Opdash which already calculates it by flagging 
        -- currently active accounts that have given sky viewing consent...

        insert into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'dp_vespa_consent'
                        ,*
        from(
                        select  sum(viewing_allowed) as value
                        from    vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts
                ) as metric

        commit

        -- Last Month:

        -- Feeding from the historical archive for RTM figures derived and maintained in OPDASH

        select  top 4   
                        value
                        ,weekending
        into    #tempshelf
        from    vespa_analysts.vespa_OpDash_hist_optout 
        where   context = 'number'
                and     lower(rtm) = 'all'
        and     weekending <> ( select  max(weekending) 
                                                        from    vespa_analysts.vespa_OpDash_hist_optout
                                                        where   context = 'number')
        order   by  weekending desc

        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                                                select  avg(value) as value
                                                                from    #tempshelf
                                                        )
        where   metricref = 'dp_vespa_consent'

        commit

        drop table #tempshelf

        commit
        
        -----------------------------------------------------------
        
        /* 
        Deriving :      - Vespa Consent Percentage 
        */
        
        declare @weekending date

        select  @weekending = max(weekending)
        from    vespa_analysts.vespa_OpDash_hist_optout
        
        -- Last Week:
        
        insert into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'dp_vespa_consent_per'
                                ,value as value
                from    vespa_analysts.vespa_OpDash_hist_optout
                where   weekending = @weekending
                and     context = 'percentage'
                and     lower(rtm)='all'
        
        commit
        
        
        -- Last Month:
        
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                                                select  avg(value) as value
                                                                                                                                from    vespa_analysts.vespa_OpDash_hist_optout
                                                                                                                                where   weekending >=   @weekending - 30
                                                                                                                                and     weekending <    @weekending
                                                                                                                                and     context = 'percentage'
                                                                                                                                and     lower(rtm)='all'
                                                        )
        where   metricref = 'dp_vespa_consent_per'
        
        commit
        
        
        ---------------------------------------------------------------------------------

        /* 
        Deriving:               - Daily Panel Accounts Enabled 
                                        - Daily Panel Accounts Returning Data Reliably
                                        - All Accounts Enabled 
                                        - All Accounts Returning Data Reliably 
        */
        -- This guys are derived from the same data source, so why not put them all in the same section of the module...

                
        /*      cortb included the metrics below (06-02-2014) to replace the
                        - Daily Panels enabled accounts & accounts reporting reliably
                        - All Panels enabled accounts & accounts reporting reliably
                        as they used to be pulled from PanMan - which was based on scaling */

 
        -- Last Week:  DP accounts enabled
        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman

        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'dp_ac_enabled'
                ,count(distinct account_number)
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending = @maxdate
        and     panel_id in (@dp12,@dp11)

        -- Last Month:
        select  weekending, count(distinct account_number) as accounts
        into    #monthvalue
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending      >= (@maxdate - 28)
        and     weekending      < @maxdate
        and     panel_id in (@dp12,@dp11)
        group   by weekending

        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set     valuemonth =    (
                                                                        select avg(accounts)
                                                                        from #monthvalue
                                                                )
        where   metricref ='dp_ac_enabled'
        drop table #monthvalue

        commit

        -- Last Week:  AllP accounts enabled
        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman

        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'all_ac_enabled'
                ,count(distinct account_number)
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending = @maxdate
        and     panel_id is not null

        -- Last Month:
        select  weekending, count(distinct account_number) as accounts
                into    #monthvalue
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending      >= (@maxdate - 28)
        and     weekending      < @maxdate
        and     panel_id is not null
        group   by      weekending

        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set     valuemonth =    (
                                                                        select avg(accounts)
                                                                        from #monthvalue
                                                                )
        where   metricref ='all_ac_enabled'
        drop table #monthvalue

        commit

                ---------------------------------------------------------------------------------------
        --  DP accounts reporting reliably
        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'dp_ac_reliable'
                ,count(distinct account_number)
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   reporting_quality > 0.9
        and     weekending = @maxdate
        and     panel_id in (@dp12,@dp11)

        -- Last Month:
        select  weekending, count(distinct account_number) as accounts
                into    #monthvalue
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending      >= (@maxdate - 28)
        and     weekending      < @maxdate
        and     panel_id in (@dp12,@dp11)
        and     reporting_quality > 0.9
        group by weekending

        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                        select avg(accounts)
                                        from #monthvalue
                                        )
        where   metricref ='dp_ac_reliable'
        drop table #monthvalue

        commit

        -- AllP accounts reporting reliably
        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'all_ac_reliable'
                ,count(distinct account_number)
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   reporting_quality > 0.9
        and     weekending = @maxdate
        and     panel_id is not null

        -- Last Month:
        select  weekending, count(distinct account_number) as accounts
                into    #monthvalue
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending      >= (@maxdate - 28)
        and     weekending      < @maxdate
        and     panel_id is not null
        and     reporting_quality > 0.9
        group by weekending

        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                        select avg(accounts)
                                        from #monthvalue
                                        )
        where   metricref ='all_ac_reliable'
        drop table #monthvalue

        commit
                
        ---------------------------------------------------------------------------------       

        /* 
        Deriving:    - Daily Panel Accounts Returning Data
        */

        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman
        where   panel_id in (@dp12,@dp11)

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'dp_ac_returning'
                        ,count(1)
        from    vespa_analysts.vespa_xdash_o2_histviewman
        where   weekending = @maxdate
        and             panel_id in (@dp12,@dp11)
                                                        
        commit

        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set     valuemonth =    (
                                                                        select  coalesce(avg(hits),0)
                                                                        from    (
                                                                                                select  weekending
                                                                                                                                ,count(1)       as hits
                                                                                                from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                                                where   weekending      >= @maxdate - 28
                                                                                                and             weekending      < @maxdate
                                                                                                and             panel_id        in (@dp12,@dp11)
                                                                                                group   by      weekending
                                                                                        )       as base
                                                                ) 
        where   metricref ='dp_ac_returning'

        commit
        ---------------------------------------------------------------------------------       

        /*
        Deriving:       - All Accounts Returning Data
        */


        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman


        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'all_ac_returning'
                ,count(1)
        from    vespa_analysts.vespa_xdash_o2_histviewman
        where   weekending = @maxdate
                                                        
        commit

        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                                                        select  coalesce(avg(hits),0)
                                                                        from    (
                                                                                                select  weekending
                                                                                                                                ,count(1)       as hits
                                                                                                from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                                                where   weekending      >= @maxdate - 28
                                                                                                and             weekending      < @maxdate
                                                                                                group   by      weekending
                                                                                        )       as base
                                                                ) 
        where   metricref ='all_ac_returning'

        commit


        ---------------------------------------------------------------------------------
        /*
        Deriving:       - Daily panel Average Reporting Quality of Accounts Returning Data
        */

        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman
        where   panel_id in (@dp12,@dp11)

        -- Last Week:

        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'dp_avg_ac_ret_ok'
                                ,coalesce(avg(reporting_quality),0)
        from    vespa_analysts.vespa_xdash_o2_histviewman
        where   weekending = @maxdate
        and     panel_id in (@dp12,@dp11)
        --and     reporting_quality > 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)

        commit

        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set     valuemonth =    (
                                                                        select  coalesce(avg(reporting_quality),0)
                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                        where   weekending >= @maxdate - 28
                                                                        and     weekending      < @maxdate
                                                                        and     account_number in       (       
                                                                                                                                        select  distinct
                                                                                                                                                        account_number
                                                                                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                                                                                        where   weekending = @maxdate
                                                                                                                                        and     panel_id   in (@dp12,@dp11)
                                                                                                                                )
                                                                                --and     reporting_quality > 0 -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
                                                                )
        where   metricref = 'dp_avg_ac_ret_ok'

        commit

        ---------------------------------------------------------------------------------
        /*
        Deriving:       - Daily Panel Average Reporting Quality of Accounts Not Returning Data
        */

        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman

        select  all_                    as weekending
                ,avg(reporting_quality) as repqual
        into    #tempshelf
        from    (
                                        select  everyone.account_number
                                                        ,everyone.weekending        as all_
                                                        ,reporting.weekending       as rep
                                                        ,everyone.reporting_quality
                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck as everyone -- This holds all enabled accounts...
                                                        left join   (
                                                                                        select  distinct 
                                                                                                        Z.account_number
                                                                                                        ,Z.weekending
                                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman as Z
                                                                                                        inner join  (
                                                                                                                                        select  distinct account_number, panel_id
                                                                                                                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                                                                                                                        where   panel_id in (@dp12,@dp11)
                                                                                                                                )       as X
                                                                                                        on  Z.account_number = X.account_number
                                                                                                        and Z.panel_id = X.panel_id -- This is to get all accounts reporting and that are Enabled...
                                                                                )       as reporting
                                                        on  everyone.account_number = reporting.account_number
                                                        and everyone.weekending     = reporting.weekending
                                        where   everyone.weekending between (@maxdate-30) and @maxdate
                                        and     panel_id in (@dp12,@dp11)
                                ) as base
        where   rep is null --reporting_quality > 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
        group   by  all_
        order   by  all_ desc


        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'dp_avg_ac_ret_notok'
                                ,repqual
        from    #tempshelf
        where   weekending = @maxdate
        
        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set     valuemonth =    (
                                                                        select  avg(repqual)
                                                                        from    #tempshelf
                                                                        where   weekending < @maxdate
                                                                )
        where   metricref = 'dp_avg_ac_ret_notok'
        
        drop table #tempshelf
        
        ---------------------------------------------------------------------------------
        /*
        Deriving:   - All Average Reporting Quality of Accounts Returnind Data
        */


        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman

        -- Last Week:

        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'all_avg_ac_ret_ok'
                ,coalesce(avg(case when reporting_quality > 1 then 1 else reporting_quality end),0) -- the coalesce case should only act when no weekending matching with maxdate...
        from    vespa_analysts.vespa_xdash_o2_histviewman
        where   weekending = @maxdate
        --and     reporting_quality <> 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
        
        commit

        -- Last Month:

        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                                                        select  coalesce(avg(case when reporting_quality > 1 then 1 else reporting_quality end),0)
                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                        where   weekending >= @maxdate - 28
                                                                        and             weekending < @maxdate
                                                                        and     account_number in       (       
                                                                                                                                        select  distinct
                                                                                                                                                        account_number
                                                                                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                                                                                        where   weekending = @maxdate
                                                                                                                                )
                                                                        -- and     reporting_quality <> 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
                                                                )
        where   metricref = 'all_avg_ac_ret_ok'

        

    ---------------------------------------------------------------------------------
        /*
        Deriving:       - All Average Reporting Quality of Accounts Not Returning Data
        */
        
        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_xdash_o2_histviewman

        select  all_                                                                                                                    as weekending
                ,avg(case when reporting_quality > 1 then 1 else reporting_quality end) as repqual
        into    #tempshelf
        from    (
                                        select  everyone.account_number
                                                        ,everyone.weekending        as all_
                                                        ,reporting.weekending       as rep
                                                        ,everyone.reporting_quality
                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck as everyone -- This holds all enabled accounts...
                                                        left join   (
                                                                                        select  distinct 
                                                                                                        Z.account_number
                                                                                                        ,Z.weekending
                                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman as Z
                                                                                                        inner join  (
                                                                                                                                        select  distinct account_number, panel_id
                                                                                                                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                                                                                                                )       as X
                                                                                                        on  Z.account_number = X.account_number
                                                                                                        and Z.panel_id = X.panel_id -- This is to get all accounts returning and that are Enabled...
                                                                                ) as reporting
                                                        on  everyone.account_number = reporting.account_number
                                                        and everyone.weekending     = reporting.weekending
                                        where   everyone.weekending             between (@maxdate - 30) and @maxdate
                                )       as base
        where   -- reporting_quality > 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
                                rep is null
        group   by  all_
        order   by  all_ desc
        
        -- Last Week:
        insert  vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'all_avg_ac_ret_notok'
                        ,repqual
        from    #tempshelf
        where   weekending = @maxdate

        
        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                                                select  avg(repqual)
                                                                from    #tempshelf
                                                                where   weekending < @maxdate
                                                        )
        where   metricref = 'all_avg_ac_ret_notok'
        
        drop table #tempshelf
        
        -------------------------------------------------------------------------------------------------
        /*
        Deriving:       - Daily Panel Average Reporting Quality
        */
        
        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        
        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'dp_avg_ac_return'
                ,avg(reporting_quality)
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending = @maxdate
        --  and     reporting_quality > 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
        and     panel_id in (@dp12,@dp11)
        
        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set     valuemonth =    (
                                                                        select  avg(reporting_quality)
                                                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                                                        where   weekending >= (@maxdate - 30)
                                                                        and     weekending < @maxdate
                                                                        --  and     reporting_quality > 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
                                                                        and     panel_id in (@dp12,@dp11)
                                                                )
        where   metricref = 'dp_avg_ac_return'


        -------------------------------------------------------------------------------------------------
        /*
        Deriving:       - All Panel Average Reporting Quality
        */

        select  @maxdate = max(weekending)
        from    vespa_analysts.vespa_sbv_hist_qualitycheck

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select  'all_avg_ac_return'
                        ,avg(case when reporting_quality > 1 then 1 else reporting_quality end)
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending = @maxdate
        -- and     reporting_quality > 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
        
        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                                                select  avg(case when reporting_quality > 1 then 1 else reporting_quality end)
                                                                from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                                                where   weekending >= (@maxdate - 30)
                                                                and     weekending < @maxdate
                                                                -- and     reporting_quality > 0  -- cortb removed to re-balance the averages as reporting quality was over 100% (21/11/2013)
                                                        )
        where   metricref = 'all_avg_ac_return'                                         
        
                
                -------------------------------------------------------------------------------------------------
        /*
        Deriving:       - Daily Panel Average Effective Sample Size (ESS)
        */
                declare @aux date
                -- declare @weekending date  -- cortb commented out as it is above (21/11/2013)

                select @aux = max(adjusted_event_Start_date_vespa) from sk_prod.viq_viewing_data_scaling

                select   @weekending =  case    when datepart(weekday,@aux) = 7 then @aux
                                                                                else (@aux - datepart(weekday,@aux))
                                                                end

                select  weekending
                                ,avg(ess.theess) as ess
                                ,count(1) as hits
                into    #tempshelf
                from    (
                                        select  adjusted_event_Start_date_vespa as scaling_day
                                                        ,case when datepart(weekday,scaling_day) = 7 then scaling_day else (scaling_day + (7-datepart(weekday,scaling_day))) end as weekending
                                                        ,power(sum(calculated_scaling_weight),2)/sum(power(calculated_scaling_weight,2)) as theess
                                        from    sk_prod.viq_viewing_data_scaling
                                        where   adjusted_event_Start_date_vespa between @weekending-34 and @weekending
                                        group   by  scaling_day
                                )   as ess
                group   by  weekending
                
                -- Last Week:
                insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
                select  'dp_ess'
                                ,ess
                from    #tempshelf
                where   weekending = (select max(weekending) from #tempshelf)
                
                -- Last Month:
                update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valuemonth =    (
                                                                        select  avg(ess)
                                                                        from    #tempshelf
                                                                        where   weekending < (select max(weekending) from #tempshelf)
                                                                )
                where   metricref = 'dp_ess'
                
                commit

        drop table #tempshelf

        commit
                
                        -------------------------------------------------------------------------------------------------
        /*
        Deriving:       - Adsmartable HouseHolds Metrics  (cortb added this section 06/11/2013)
        */

        -- Adsmartable households with one box and that box is adsmartable
        select  @maxdate = max(weekending)
        from vespa_analysts.vespa_opdash_16_adsm_history_4Xdash

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select 'adsm_hhs_1box'
                        ,adsm_hhs_1box
        from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
        where   weekending = @maxdate

        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valueMonth = (
                                                                select  avg(adsm_hhs_1box)
                                                                from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
                                                                where   weekending >= (@maxdate - 30)
                                                                and     weekending < @maxdate
                                                        )
        where metricref = 'adsm_hhs_1box'


        -- Adsmartable households with more than one box and at least one is adsmartable but not all
        select  @maxdate = max(weekending)
        from vespa_analysts.vespa_opdash_16_adsm_history_4Xdash

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select 'adsm_hhs_mt1box'
                        ,adsm_hhs_morethan1box_1box_adsm
        from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
        where   weekending = @maxdate

        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valueMonth = (
                                                                select  avg(adsm_hhs_morethan1box_1box_adsm)
                                                                from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
                                                                where   weekending >= (@maxdate - 30)
                                                                and     weekending < @maxdate
                                                        )
        where metricref = 'adsm_hhs_mt1box'


        -- Adsmartable households with more than one box and all are adsmartable
        select  @maxdate = max(weekending)
        from vespa_analysts.vespa_opdash_16_adsm_history_4Xdash

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select 'adsm_hhs_all_adsm'
                        ,adsm_hhs_all_adsm
        from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
        where   weekending = @maxdate

        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valueMonth = (
                                                                select  avg(adsm_hhs_all_adsm)
                                                                from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
                                                                where   weekending >= (@maxdate - 30)
                                                                and     weekending < @maxdate
                                                        )
        where metricref = 'adsm_hhs_all_adsm'


        -- Non Adsmartable households 
        select  @maxdate = max(weekending)
        from vespa_analysts.vespa_opdash_16_adsm_history_4Xdash

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select 'non_adsm_hhs'
                        ,non_adsm_hhs
        from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
        where   weekending = @maxdate

        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valueMonth = (
                                                                select  avg(non_adsm_hhs)
                                                                from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
                                                                where   weekending >= (@maxdate - 30)
                                                                and     weekending < @maxdate
                                                        )
        where metricref = 'non_adsm_hhs'
                
                
                -- Adsmartable households with reporting boxes
        select  @maxdate = max(weekending)
        from vespa_analysts.vespa_opdash_16_adsm_history_4Xdash

        -- Last Week:
        insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
        select 'adsm_hhs_reporting'
                        ,adsm_hhs_reporting
        from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
        where   weekending = @maxdate

        -- Last Month:
        update  vespa_analysts.vespa_xdash_stage_adhocmetrics
        set             valueMonth = (
                                                                select  avg(adsm_hhs_reporting)
                                                                from    vespa_analysts.vespa_opdash_16_adsm_history_4Xdash
                                                                where   weekending >= (@maxdate - 30)
                                                                and     weekending < @maxdate
                                                        )
        where metricref = 'adsm_hhs_reporting'
                
                
                                -------------------------------------------------------------------------------------------------
        /*
        Deriving:       - Virtual Panel metrics (constructed by angeld 2013/12/02)
        */ 
                
                -- VP Accounts enabled...
                -- last week 
                insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
                select  'vp_ac_enabled'
                                                ,count(distinct account_number)
                from    vespa_analysts.vespa_single_box_view
                where   status_vespa = 'Enabled'
                and     account_number in   (
                                                                                select  distinct account_number -- 521603
                                                                                from    vespa_analysts.vespa_broadcast_reporting_vp_map
                                                                                where   vp1 = 1
                                                                        )

                -- last Month
                update  vespa_analysts.vespa_xdash_stage_adhocmetrics
                set                     valueMonth = (
                                                                                                                                select  cast((avg(hits)) as integer) as thevalue
                                                                                                                                from    (
                                                                                                                                                        select  weekending
                                                                                                                                                                        ,count(distinct account_number) as hits
                                                                                                                                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                                                                                                                                        where   account_number in   (
                                                                                                                                                                                                                        select  distinct account_number -- 521603
                                                                                                                                                                                                                        from    vespa_analysts.vespa_broadcast_reporting_vp_map
                                                                                                                                                                                                                        where   vp1 = 1
                                                                                                                                                                                                                )
                                                                                                                                                        group   by  weekending
                                                                                                                                                )   as thebase
                                                                         )
                where metricref = 'vp_ac_enabled'

                                                                                                                                                
                -- VP Accounts returning data 
                -- last week 
                insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
                select  'vp_ac_returning'
                                                ,count(distinct account_number) -- 392555
                from    vespa_analysts.vespa_xdash_o2_histviewman
                where   account_number in   (
                                                                                select  distinct account_number -- 521603
                                                                                from    vespa_analysts.vespa_broadcast_reporting_vp_map
                                                                                where   vp1 = 1
                                                                        )
                and     weekending = (select max(weekending) from vespa_analysts.vespa_xdash_o2_histviewman)

                -- last month... 
                update  vespa_analysts.vespa_xdash_stage_adhocmetrics
                set                     valueMonth = (
                                                                                                                                select  cast(round((avg(hits)),0) as integer) 
                                                                                                                                from    (
                                                                                                                                                        select  weekending
                                                                                                                                                                        ,count(distinct account_number) as hits-- 392555
                                                                                                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                                                                                                        where   account_number in   (
                                                                                                                                                                                                                        select  distinct account_number -- 521603
                                                                                                                                                                                                                        from    vespa_analysts.vespa_broadcast_reporting_vp_map
                                                                                                                                                                                                                        where   vp1 = 1
                                                                                                                                                                                                                )
                                                                                                                                                        and     weekending < (select max(weekending) from vespa_analysts.vespa_xdash_o2_histviewman)
                                                                                                                                                        group   by  weekending
                                                                                                                                                )   as thebase
                                                                         )
                where metricref = 'vp_ac_returning'

                
                                -------------------------------------------------------------------------------------------------
        /*
        Deriving:       - TA Call Coverage metrics (constructed by angeld 2013/12/02)
        */ 
                
                -- TA Accounts enabled
                -- last week
                insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
                select  'all_tacc_ac_enabled'
                                                ,round((cast((sum(case when enabled = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as tacoverage
                from    (
                                                                select  case when sbv.panel is not null then 1 else 0 end   as enabled
                                                                                ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                                                from    vespa_analysts.SkyBase_TA_scores  as ta
                                                                left join   (
                                                                                                -- Vespa Panel Accounts enabled
                                                                                                select  account_number
                                                                                                                ,panel_id        as panel
                                                                                                from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                                                                                where   weekending = (select Max(weekending) from vespa_analysts.vespa_sbv_hist_qualitycheck)
                                                                                        )   as sbv
                                                                on  ta.account_number = sbv.account_number
                                                                group   by  enabled
                )   as n

                -- last month
                declare @totalprop decimal (10,2)

                select  @totalprop = sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores
                
                update  vespa_analysts.vespa_xdash_stage_adhocmetrics
                set                     valueMonth = (
                                                                                                                                select  round((avg(percen)),4) as tacoverage
                                                                                                                                from    (
                                                                                                                                                        select  sbv.weekending  
                                                                                                                                                                        ,case when sbv.panel is not null then 1 else 0 end   as enabled
                                                                                                                                                                        ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                                                                                                                                                        ,sum(round(ta.TA_Propensity,2)) / @totalprop        as percen
                                                                                                                                                        from    vespa_analysts.SkyBase_TA_scores  as ta
                                                                                                                                                        left join   (
                                                                                                                                                                                        -- Vespa Panel Accounts enabled
                                                                                                                                                                                        select  weekending
                                                                                                                                                                                                        ,account_number
                                                                                                                                                                                                        ,panel_id        as panel
                                                                                                                                                                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                                                                                                                                                                        where   weekending >= (select max(weekending)-28 from vespa_analysts.vespa_sbv_hist_qualitycheck)
                                                                                                                                                                                        and     weekending < (select max(weekending) from vespa_analysts.vespa_sbv_hist_qualitycheck)
                                                                                                                                                                                )   as sbv
                                                                                                                                                        on  ta.account_number = sbv.account_number
                                                                                                                                                        where   sbv.weekending is not null
                                                                                                                                                        group   by  weekending
                                                                                                                                                                                ,enabled
                                                                                                                                                )   as thebase
                                                                        )
                where metricref = 'all_tacc_ac_enabled'

                
                -- TA Accounts returning data 
                -- last week
                insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
                select 'all_tacc_ac_return'
                                                ,round((cast((sum(case when returned = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as tacoverage
                from    (
                                                                select  case when sbv.panel is not null then 1 else 0 end   as returned
                                                                                ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                                                from    vespa_analysts.SkyBase_TA_scores  as ta
                                                                left join   (
                                                                                                -- Vespa Panel Accounts Returning data
                                                                                                select  account_number
                                                                                                                ,panel_id        as panel
                                                                                                from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                                                where   weekending = (select Max(weekending) from vespa_analysts.vespa_xdash_o2_histviewman)
                                                                                        )   as sbv
                                                                on  ta.account_number = sbv.account_number
                                                                group   by  returned
        )   as n

                -- last Month
                select  @totalprop = sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores

                update  vespa_analysts.vespa_xdash_stage_adhocmetrics
                set                     valueMonth = (
                                                                                                                                select  round((avg(percen)),4) as tacoverage
                                                                                                                                from    (
                                                                                                                                                        select  sbv.weekending  
                                                                                                                                                                        ,case when sbv.panel is not null then 1 else 0 end   as returned
                                                                                                                                                                        ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                                                                                                                                                        ,sum(round(ta.TA_Propensity,2)) / @totalprop        as percen
                                                                                                                                                        from    vespa_analysts.SkyBase_TA_scores  as ta
                                                                                                                                                        left join   (
                                                                                                                                                                                        -- Vespa Panel Accounts Returning data
                                                                                                                                                                                        select  weekending
                                                                                                                                                                                                        ,account_number
                                                                                                                                                                                                        ,panel_id        as panel
                                                                                                                                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman
                                                                                                                                                                                        where   weekending < (select max(weekending) from vespa_analysts.vespa_xdash_o2_histviewman)
                                                                                                                                                                                )   as sbv
                                                                                                                                                        on  ta.account_number = sbv.account_number
                                                                                                                                                        where   sbv.weekending is not null
                                                                                                                                                        group   by  weekending
                                                                                                                                                                                ,returned
                                                                                                                                                )   as thebase
                                                                        )
                where metricref = 'all_tacc_ac_return'
                
                commit
                
                
                -------------------------------------------------------------------------------------------------
        /*
        Deriving: TA Call Coverage from accounts with RQ>=0.5
        */
                
                -- LAST WEEK
                insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
                select 'all_tacc_ac_ret_ge50'
                                ,round((cast((sum(case when returned = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as tacoverage
                from    (
                                        select  case when sbv.panel is not null then 1 else 0 end   as returned
                                                        ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                        from    vespa_analysts.SkyBase_TA_scores  as ta
                                        left join   (
                                                                        -- Vespa Panel Accounts Returning data
                                                                        select  hist.account_number
                                                                                        ,hist.panel_id        as panel
                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman               as hist
                                                                                        inner join vespa_analysts.vespa_sbv_hist_qualitycheck   as rq
                                                                                        on  hist.weekending = rq.weekending
                                                                                        and hist.account_number = rq.account_number
                                                                        where   hist.weekending = (select Max(weekending) from vespa_analysts.vespa_xdash_o2_histviewman)
                                                                        and     rq.reporting_quality >=0.5
                                                                )   as sbv
                                        on  ta.account_number = sbv.account_number
                                        group   by  returned
                                )   as n
                                
                commit
                
                -- LAST MONTH
                update  vespa_analysts.vespa_xdash_stage_adhocmetrics
                set                     valueMonth = (
                                                                                select  round((avg(percen)),4) as tacoverage
                                                                                from    (
                                                                                                        select  sbv.weekending  
                                                                                                                        ,case when sbv.panel is not null then 1 else 0 end   as returned
                                                                                                                        ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                                                                                                        ,sum(round(ta.TA_Propensity,2)) / @totalprop        as percen
                                                                                                        from    vespa_analysts.SkyBase_TA_scores  as ta
                                                                                                                        left join   (
                                                                                                                                                        -- Vespa Panel Accounts Returning data
                                                                                                                                                        select  hist.weekending
                                                                                                                                                                        ,hist.account_number
                                                                                                                                                                        ,hist.panel_id        as panel
                                                                                                                                                        from    vespa_analysts.vespa_xdash_o2_histviewman   as hist
                                                                                                                                                                        inner join vespa_analysts.vespa_sbv_hist_qualitycheck   as rq
                                                                                                                                                                        on  hist.weekending = rq.weekending
                                                                                                                                                                        and hist.account_number = rq.account_number
                                                                                                                                                        where   hist.weekending < (select max(weekending) from vespa_analysts.vespa_xdash_o2_histviewman)
                                                                                                                                                        and     rq.reporting_quality >=0.5
                                                                                                                                                )   as sbv
                                                                                                                                        on  ta.account_number = sbv.account_number
                                                                                                        where   sbv.weekending is not null
                                                                                                        group   by  weekending
                                                                                                                                ,returned
                                                                                                )   as thebase
                                                                        )
                where metricref = 'all_tacc_ac_ret_ge50'
                
                commit
                
                
                -------------------------------------------------------------------------------------------------
        /*
        Deriving: Volume of all panels accounts returning data with RQ>=0.5
        */
                
                -- LAST WEEK
                
                select  @maxdate = max(weekending)
                from    vespa_analysts.vespa_xdash_o2_histviewman

                insert  into vespa_analysts.vespa_xdash_stage_adhocmetrics(metricref,valueweek)
                select  'all_ac_ret_ge50'
                                ,count(distinct hist.account_number)
                from    vespa_analysts.vespa_xdash_o2_histviewman   as hist
                                inner join vespa_analysts.vespa_sbv_hist_qualitycheck   as rq
                                on  hist.weekending = rq.weekending
                                and hist.account_number = rq.account_number
                where   hist.weekending = @maxdate
                and     rq.reporting_quality >=0.5

                commit

                -- LAST MONTH
                update  vespa_analysts.vespa_xdash_stage_adhocmetrics
                set                     valueMonth = (
                                                                                select  coalesce(avg(hits),0)
                                                                                from    (
                                                                                                                select  hist.weekending
                                                                                                                                ,count(1)       as hits
                                                                                                                from    vespa_analysts.vespa_xdash_o2_histviewman   as hist
                                                                                                                                inner join vespa_analysts.vespa_sbv_hist_qualitycheck   as rq
                                                                                                                                on  hist.weekending = rq.weekending
                                                                                                                                and hist.account_number = rq.account_number
                                                                                                                where   hist.weekending < @maxdate
                                                                                                                and     rq.reporting_quality >=0.5
                                                                                                                group   by  hist.weekending
                                                                                                ) as base
                                                                        )
                where metricref = 'all_ac_ret_ge50'
                
                commit
---------------------------
/* M05.2 - QAing results */
---------------------------
-- Comming soon...
-------------------------------
/* M05.3 - Returning results */
-------------------------------
        commit


end;

--Giving permissions to admin team.
        grant select on vespa_analysts.vespa_xdash_stage_adhocmetrics to vespa_group_low_security, vespa_analysts_admin_group, cortb;

        commit;

commit;
