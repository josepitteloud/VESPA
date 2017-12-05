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
**Project Name:                                                 OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Gavin Meggs
**Due Date:                             13/06/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:
        
        A Lighter version for Xdash focused on panel performance based on dialling platform...

**Modules:

M01: OPs XDASH Overview
        M01.0 - Initialising environment
        M01.1 - Building Base for Panel Performance
                M01.2 - Producing Analysis for Panel Balance (scaling variables)
                M01.3 - Setting Privileges for base tables

**Stats:

        -- running time: 10 min approx...
        
--------------------------------------------------------------------------------------------------------------
*/

create or replace procedure ops_xdash_overview
        @weekending     date    = null
as 
begin

-----------------------------------
-- M01.0 - Initialising environment
-----------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  OPS_XDASH_OVERVIEW'
        
        execute xdash_ov_output_to_logger ' | ETA: [90 Minutes] Run APROX'
        
        execute xdash_ov_output_to_logger ' | Beginig  M01.0 - Initialising environment'

-- Initialising base tables for slicing
                                        
        execute xdash_ov_output_to_logger ' | @ M01.0: Sampling Accounts VIQ-Scaled in past Month'
        
        /*
                Sampling for everyone in the DP who has been scaled in the past month
                as per this report we are interested on representing the "account returning data"
                concept based on scaling (an account will be part of the scaling sample of a day if on that 
                day that account returned data from all of the boxes)...
        */
        if object_id('scaling_s1') is not null
                drop table scaling_s1
                
        commit
                
        select  case   when datepart(weekday,thedate) = 7 then thedate
                                        else cast(dateadd(day,(7-datepart(weekday,thedate)),thedate) as date)
                        end     as weekending
                        ,adjusted_event_start_date_vespa as thedate
                        ,account_number
        into    scaling_s1
        from    /*sk_prod.*/VIQ_VIEWING_DATA_SCALING
        where   adjusted_event_start_date_vespa between @weekending-29 and @weekending
        /* where   adjusted_event_start_date_vespa >=  (
                                                                                                        select  max(adjusted_event_start_date_vespa)-30
                                                                                                        from    /*sk_prod.*/VIQ_VIEWING_DATA_SCALING
                                                                                                ) */
        commit
        create hg index hg1 on scaling_s1(account_number)
        create date index d1 on scaling_s1(thedate)
        create date index d2 on scaling_s1(weekending)
        commit

        execute xdash_ov_output_to_logger ' | @ M01.0: Sampling Accounts VIQ-Scaled in past Month DONE'
        
        
        execute xdash_ov_output_to_logger ' | @ M01.0: Determining Accounts RQs'
        
        /*
                To go in line with the new requirement to generate the RQ based on the frequency
                at which an account picks up a weight in the last 30 days, we are now creating
                below table to store the RQ for each account as per what describe above...
        */

        if object_id('ac_rq_lookup') is not null
                drop table ac_rq_lookup
                
        commit

        select  base.account_number
                        ,count(distinct base.thedate)                                           as hits
                        ,round((cast(hits as float) / cast(max(timeframe.length_) as float)),2) as RQs
        into    ac_rq_lookup
        from    scaling_s1  as base
                        inner join  (
                                                        select  datediff(day,min(thedate),max(thedate)) as length_
                                                        from    scaling_s1
                                                )   as timeframe
                        on  1 = 1 --> this makes true the relation between the table for all rows
        group   by  base.account_number

        commit
        create hg index hg1 on ac_rq_lookup(Account_number)
        commit

        execute xdash_ov_output_to_logger ' | @ M01.0: Determining Accounts RQs DONE'   
        
        execute xdash_ov_output_to_logger ' | @ M01.0: Sampling History of Accounts enabled in Vespa'
        
        /*
                Sampling for everyone in the Vespa panel who was enabled at an specific week (week defined from last Sunday
                until last Saturday)... bringing the table from box level up to account level as that is the context of
                the Xdash overview...
        */

        if object_id('sbvh') is not null
                drop table sbvh

        commit

        select  distinct
                        weekending
                        ,account_number
                        ,panel_id
        into    sbvh
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending between @weekending-27 and @weekending
        /* where   weekending >=   (
                                                                select  max(adjusted_event_start_date_vespa)-30
                                                                from    /*sk_prod.*/VIQ_VIEWING_DATA_SCALING
                                                        ) */
        commit
        create hg index hg1 on sbvh(account_number)
        create date index d1 on sbvh(weekending)
        commit

        execute xdash_ov_output_to_logger ' | @ M01.0: Sampling History of Accounts enabled in Vespa DONE'
        
        execute xdash_ov_output_to_logger ' | @ M01.0: Identifying Panel assigments through history'
        
        /*
                Intercepting above two tables to be able to identify which accounts below to which panel
                at a given week... hence been able to do any count broken down as needed
        */

        if object_id('base1') is not null
                drop table base1
                
        commit

        select  s1.*
                        ,sbvh.panel_id
        into    base1
        from    scaling_s1  as s1
                        inner join  sbvh
                        on  s1.weekending       = sbvh.weekending
                        and s1.account_number   = sbvh.account_number
        commit
        create hg index hg1 on base1(account_number)
        create date index d1 on base1(weekending)
        create date index d2 on base1(thedate)
        commit

        drop table scaling_s1
        drop table sbvh
        commit

        execute xdash_ov_output_to_logger ' | @ M01.0: Identifying Panel assigments through history DONE'
        
        execute xdash_ov_output_to_logger ' | @ M01.0: Counting historically number of boxes owed by accounts'
        
        /*
                Sampling all boxes enabled in the Vespa panel on the last month
                to be able to then check further on how many boxes each account has
                and compare that against how many of them are actually dialling on
                a given day to satisfy the returning data definition...
        */

        if object_id('sbvh_boxlevel') is not null
                drop table sbvh_boxlevel
                
        commit  

        select  distinct
                        weekending
                        ,account_number
                        ,subscriber_id
                        ,panel_id
        into    sbvh_boxlevel
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending between @weekending-27 and @weekending
        /* where   weekending >=   (
                                                                select  max(adjusted_event_start_date_vespa)-30
                                                                from    /*sk_prod.*/VIQ_VIEWING_DATA_SCALING
                                                        ) */

        commit
        create hg index hg1 on sbvh_boxlevel(account_number)
        create hg index hg2 on sbvh_boxlevel(subscriber_id)
        create date index d1 on sbvh_boxlevel(weekending)
        commit
        
        execute xdash_ov_output_to_logger ' | @ M01.0: Counting historically number of boxes owed by accounts DONE'
        
        execute xdash_ov_output_to_logger ' | @ M01.0: Sampling Accounts dialbacks over a month'

        /*
                Sampling from the panel data
        */

        if object_id('dialsample') is not null
                drop table dialsample
                
        commit

        select  subscriber_id
                        ,dt
        into    dialsample
        from    vespa_analysts.panel_data
        where   dt between @weekending-29 and @weekending
        /* where   dt >=   (
                                                select  max(adjusted_event_start_date_vespa)-30
                                                from    /*sk_prod.*/VIQ_VIEWING_DATA_SCALING
                                        ) */
        and     data_received = 1

        commit
        create hg index hg1 on dialsample(subscriber_id)
        create date index d1 on dialsample(dt)
        commit

        execute xdash_ov_output_to_logger ' | @ M01.0: Sampling Accounts dialbacks over a month DONE'
        
        execute xdash_ov_output_to_logger ' | @ M01.0: Summarising Dialback interaction at account level (ETA: 20 mins APROX)'
        
        if object_id('dialsample_aclevel') is not null
                drop table dialsample_aclevel
                
        commit

        select  dial.dt
                        ,sbvh.account_number
                        ,count(Distinct dial.subscriber_id) as dialling_boxes
        into    dialsample_aclevel
        from    dialsample              as dial
                        left join sbvh_boxlevel as sbvh
                        on  dial.subscriber_id = sbvh.subscriber_id
        group   by  dial.dt
                                ,sbvh.account_number

        commit
        create hg index hg1 on dialsample_aclevel(account_number)
        create date index d1 on dialsample_aclevel(dt)
        commit

        execute xdash_ov_output_to_logger ' | @ M01.0: Summarising Dialback interaction at account level DONE'
        
        execute xdash_ov_output_to_logger ' | @ M01.0: Detecting accounts returning data'

        if object_id('returning_accounts') is not null
                drop table returning_accounts
                
        commit

        select  a.dt    as thedate
                        ,a.account_number
                        ,case   when datepart(weekday,thedate) = 7 then thedate
                                        else cast(dateadd(day,(7-datepart(weekday,thedate)),thedate) as date)
                        end     as weekending
        into    returning_accounts
        from    dialsample_aclevel  as A
                        inner join  (
                                                        select  account_number
                                                                        ,count(distinct subscriber_id)  as total_boxes
                                                        from    sbvh_boxlevel
                                                        group   by  account_number
                                                )   as B
                        on  a.account_number = b.account_number
        where   a.dialling_boxes >= b.total_boxes

        commit
        create hg index hg1 on returning_accounts(account_number)
        create date index d1 on returning_accounts(weekending)
        create date index d2 on returning_accounts(thedate)
        commit

        execute xdash_ov_output_to_logger ' | @ M01.0: Detecting accounts returning data DONE'
        execute xdash_ov_output_to_logger ' | Beginig  M01.0 - Initialising environment DONE'

----------------------------------------------
-- M01.1 - Building Base for Panel Performance
----------------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  M01.1 - Building Base for Panel Performance'

        execute xdash_ov_output_to_logger ' | @ M01.1: Measuring AP interaction'
        
        -- AP Full week/month   
        declare @themaxdt    date

        select  @themaxdt = max(thedate)-6 from returning_accounts

        if object_id('apfullweekmonth') is not null
                drop table apfullweekmonth
                
        commit
                        
        select  b.thepanel
                        ,b.dialway
                        ,sum    (
                                                case    when thepanel = 'AP' and dialway = 'PSTN' and a.full7d>=4       then 1
                                                                when a.full7d>=7                                                                                        then 1
                                                                else 0
                                                end
                                        )       as fulllastweek
                        ,sum    (
                                                case   when thepanel = 'AP' and dialway = 'PSTN' and full30d>=15    then 1
                                                           when full30d>=24                                             then 1
                                                end
                                        )   as fulllastmonth
        into    apfullweekmonth
        from    (
                                select  account_number
                                                ,sum(case   when thedate >= @themaxdt then 1 else 0 end) as full7d
                                                ,count(distinct thedate) as full30d
                                from    returning_accounts
                                group   by  account_number
                        )   as A
                        inner join  (
                                                        select  distinct
                                                                        account_number
                                                                        ,case    when panel_id in (11,12)    then 'DP'
                                                                                        when panel_id in (5,6,7)    then 'AP'
                                                                        end     as thepanel
                                                                        ,case   when panel_id in (12,6,7)   then 'PSTN'
                                                                                        when panel_id in (5,11)     then 'BB'
                                                                        end     as dialway
                                                        from    sbvh_boxlevel
                                                )   as b
                        on  a.account_number    = b.account_number
                        and b.thepanel          = 'AP'
        group   by  b.thepanel
                                ,b.dialway -- Sly
                                                
        commit
                        
        execute xdash_ov_output_to_logger ' | @ M01.1: Measuring AP interaction DONE'
        
        execute xdash_ov_output_to_logger ' | @ M01.1: Measuring DP interaction'
                                                
        -- DP once and full week/month  
        select  @themaxdt = max(thedate)-6 from base1

        if object_id('dpfullweekmonth') is not null
                drop table dpfullweekmonth
                
        commit
                                                
        select  panel_id
                        ,count(distinct account_number)                     as once30d
                        ,sum(case when full7d >0 then 1 else 0 end)         as once7d
                        ,sum(case when full7d >=7 then 1 else 0 end)        as fulldial7d
                        ,sum(case when overalldial >=27 then 1 else 0 end)  as fulldial30d
        into    dpfullweekmonth
        from    (
                                select  panel_id
                                                ,account_number
                                                ,count(distinct thedate) as overalldial
                                                ,sum(case when thedate >= @themaxdt then 1 else 0 end) as full7d
                                from    base1
                                where   panel_id in (11,12)
                                group   by  panel_id
                                                        ,account_number
                        )   as base
        group   by   panel_id -- Sly

        commit

        execute xdash_ov_output_to_logger ' | @ M01.1: Measuring DP interaction DONE'
        execute xdash_ov_output_to_logger ' | Beginig  M01.1 - Building Base for Panel Performance DONE'
        
-------------------------------------------------------------------
-- M01.2 - Producing Analysis for Panel Balance (scaling variables)
-------------------------------------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  M01.2 - Producing Analysis for Panel Balance (scaling variables)'
        
        execute xdash_ov_variable_metrics @weekending
        
        execute xdash_ov_output_to_logger ' | Beginig  M01.2 - Producing Analysis for Panel Balance (scaling variables) DONE'
        

---------------------------------------------
-- M01.3 - Setting Privileges for base tables
---------------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  M01.3 - Setting Privileges for base tables'
        
        grant select on base1                           to vespa_group_low_security
        grant select on sbvh_boxlevel           to vespa_group_low_security
        grant select on dialsample                      to vespa_group_low_security
        grant select on dialsample_aclevel      to vespa_group_low_security
        grant select on returning_accounts      to vespa_group_low_security
        grant select on apfullweekmonth         to vespa_group_low_security
        grant select on dpfullweekmonth         to vespa_group_low_security
        grant select on ac_rq_lookup            to vespa_group_low_security
        commit
        
        execute xdash_ov_output_to_logger ' | Beginig  M01.3 - Setting Privileges for base tables DONE'
        
        execute xdash_ov_output_to_logger ' | OPS_XDASH_OVERVIEW DONE'
         
end;

commit;
grant execute on ops_xdash_overview to vespa_group_low_security;

commit;








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

**Project Name:                                         XDash Overview Variables
**Analysts:                                                     Berwyn Cort        (berwyn.cort@skyiq.co.uk)
**Lead(s):
**Stakeholder:                                          SIG and Executive Team


**Business Brief:

        A method to use 241 - Panel Balancing 2.0 created by Jon Green and list of accounts with the number
        of boxes they have and are returning built by Angel Donnarumma with other tables to pull together variables
        based on Scaling 3.0 to show Sky base, Panel base, average Scaling Weight Reportin Quality and formulae to
        produce Panel Balancing indices.

**Sections:

                A00: Initialising the environment

        A: POPULATE PANEL BALANCE TABLES FOR VARIABLE DATA

                A01: Run stored procedure
                     The SP is in the following location in the repository (...GIT\Vespa\Vespa Projects\241 - Panel Balancing 2.0\panbal_segments).


        B: REFRESH DAILY PANEL ACCOUNT POPULATION WITH RETURNING BOXES

                B01: Accounts and thier boxes
                B02: Number of boxes dialled against thier account in the past 30 days

        C: POPULATE VARIABLES METRICS TABLE

                C01: Initialisation
                C02: Set the temp tables
                C03: Set the high level variables
                C04: Construct the variable table, repeating per variable

--------------------------------------------------------------------------------------------------------------------------------------------
USEFUL NOTE:    first the PanBal_segmentation SP is run to get the latest snapshot of variables against accounts and
                segment_id then the population is refreshed with their box details.  Then variables are populated with
                Sky and Panel base for index calculations.  Queries then pull together the metrics and repeat for every variable.
--------------------------------------------------------------------------------------------------------------------------------------------

*/

------------------------------------
-- A00: Initialising the environment
------------------------------------

create or replace procedure xdash_ov_variable_metrics
        @weekending     date    = null
as begin

        execute xdash_ov_output_to_logger ' | Beginig  XDASH_OV_VARIABLE_METRICS'
        
        execute xdash_ov_output_to_logger ' | Beginig  A00: Initialising the environment'

        delete from xdash_overview_variables
    commit

        execute xdash_ov_output_to_logger ' | Beginig  A00: Initialising the environment DONE'
        
------------------------------------------
-- A01: Run Panel Balance stored procedure
------------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  A01: Run Panel Balance stored procedure (ETA: 30 mins APROX)'

        -- To populate tables for variable data
    execute /*vespa_analysts.*/V306_M03_PanBal_Segments_adapted @weekending

        execute xdash_ov_output_to_logger ' | Beginig  A01: Run Panel Balance stored procedure DONE'

-----------------------------------------------------
-- B01: Account population with their number of boxes
-----------------------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  B01: Account population with thier number of boxes'

        -- declare and inititialise variables
        
        execute xdash_ov_output_to_logger ' | ONLY TO FIT RSMB AUDIT!!!!!'
        
        if object_id('acview') is not null
                drop table acview
                
        commit
        
        -- a list of accounts and how many boxes each has...
        select  panel_id        as panel        
                        ,account_number
                        ,count(distinct subscriber_id)  as num_boxes
        into    acview
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   panel is not null
        and             weekending = @weekending
        group   by  panel
                                ,account_number

        commit
        create hg index hg1 on acview(account_number)
        create lf index lf1 on acview(panel)
        commit

        execute xdash_ov_output_to_logger ' | Beginig  B01: Account population with thier number of boxes DONE'
        
-----------------------------------------------------
-- B02: Boxes dialled per account in the past 30 days
-----------------------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  B02: Boxes dialled per account in the past 30 days'
        
        execute xdash_ov_output_to_logger ' | OOP!!!!!'
        
        -- counting for each day on the past 30 days the number of boxes that dialed
        -- for every single account...

        if object_id('panel_data_summary') is not null
                drop table panel_data_summary
        
        commit
        
    select  perf.dt
                        ,boxview.account_number
                        ,count(distinct perf.subscriber_id) as dialling_b
        into    panel_data_summary
        from    (
                                select  dt
                                                ,subscriber_id
                                from    vespa_analysts.panel_data
                                where   data_received = 1
                                and             dt between @weekending-29 and @weekending
                                and             panel in (5,6,7)
                        )       as perf
                        inner join  vespa_analysts.vespa_sbv_hist_qualitycheck  as boxview
                        on  perf.subscriber_id = boxview.subscriber_id
                        and     boxview.panel_id in (5,6,7)
                        and     boxview.weekending = @weekending
        group   by  perf.dt 
                                ,boxview.account_number

        commit
        create date index date1 on panel_data_summary(dt)
        create hg index hg1     on panel_data_summary(account_number)
        grant select on panel_data_summary to vespa_group_low_security
        commit 

        execute xdash_ov_output_to_logger ' | Beginig  B02: Boxes dialled per account in the past 30 days DONE'
        
----------------------
-- C01: Initialisation
----------------------

        execute xdash_ov_output_to_logger ' | Beginig  C01: Initialisation'

        -- declare the variables
        declare @Sky_Base decimal(8,1)
                        ,@Panel_Base decimal(8,1)
                        ,@todtMinus1 date
                        ,@todtMinus2 date
                        ,@todtMinus3 date
                        ,@HLAvgReturners integer
                        ,@HLBalanceIndex decimal(16,6)
                        ,@var_count      tinyint
                        ,@thevariable    varchar(20)

        -- Populate the variables
        select @Sky_Base   =    count(distinct account_number)
                                                        from    PanBal_segment_snapshots

        select @Panel_Base =    count(distinct account_number)
                                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck
                                                        where   weekending = @weekending
                                                        and             panel_id in (11,12)

        select  @todtMinus1 = @weekending - 7
        select  @todtMinus2 = @todtMinus1 - 7
        select  @todtMinus3 = @todtMinus2 - 7

        select  @var_count = min(id) from panbal_variables

        execute xdash_ov_output_to_logger ' | Beginig  C01: Initialisation DONE'
        
---------------------------------------------------
-- C02: Set the temporary tables most commonly used
---------------------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  C02: Set the temporary tables most commonly used'
        
        execute xdash_ov_output_to_logger ' | Changed to VIQ!!!!!'

        -- Scaling Weight Reporting Quality population
        /* select  case when dial.dt <= @todt       and dial.dt > @todtMinus1   then @todt
                                        when dial.dt <= @todtMinus1 and dial.dt > @todtMinus2   then @todtMinus1
                                        when dial.dt <= @todtMinus2 and dial.dt > @todtMinus3   then @todtMinus2
                                        when dial.dt <= @todtMinus3 and dial.dt > @todtMinus3-7 then @todtMinus3
                        end     as dt
                        ,acview.account_number
        into    #SWRQ
        from    #panel_data         as dial
                        inner join #acview  as acview                    --*
                        on  dial.account_number = acview.account_number
                        and dial.dialling_b >= acview.num_boxes -- This is the condition that flags whether an account returned data or not
        where   dt > @todtMinus3-7 */
        
        select  case    when adjusted_event_start_date_vespa <= @weekending       and adjusted_event_start_date_vespa > @weekending-7   then @weekending
                                        when adjusted_event_start_date_vespa <= @weekending-7     and adjusted_event_start_date_vespa > @weekending-14  then @weekending-7
                                        when adjusted_event_start_date_vespa <= @weekending-14    and adjusted_event_start_date_vespa > @weekending-21  then @weekending-14
                                        when adjusted_event_start_date_vespa <= @weekending-21    and adjusted_event_start_date_vespa > @weekending-28  then @weekending-21
                        end     as dt
                        ,account_number
        into    #SWRQ
        from    /*sk_prod.*/VIQ_VIEWING_DATA_SCALING
        where   adjusted_event_start_date_vespa between @weekending-29 and @weekending
        
        commit
        create hg index idx_accountnumber on #SWRQ(account_number)
        commit

        Message now()||' | Building SWRQ DONE'

        -- Vespa panel households population
        select  account_number
        into    #PanBaseHH
        from    vespa_analysts.vespa_sbv_hist_qualitycheck
        where   weekending = @weekending
        and             panel_id in (11,12)

        commit
        create hg index idx_accountnumber on #PanBaseHH(account_number)
        commit

        Message now()||' | Building PanBaseHH DONE'
        execute xdash_ov_output_to_logger ' | Beginig  C02: Set the temporary tables most commonly used DONE'
        
------------------------------------
-- C03: Set the high level variables
------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  C03: Set the high level variables'

        -- Variable to get average returning households at high level.
        select  @HLAvgReturners = avg(AvgRet)
        from    (
                                select  swrq.dt
                                                ,count(swrq.account_number) / 7 as AvgRet
                                from    PanBal_segment_snapshots as pss
                                                left join #SWRQ as swrq
                                                on swrq.account_number = pss.account_number
                                group   by swrq.dt
                        )       as HLReturners

        execute xdash_ov_output_to_logger ' | Beginig  C03: Set the high level variables DONE'
                        
------------------------------------------------------------
-- C04: Construct the variable table, repeating per variable
------------------------------------------------------------

        execute xdash_ov_output_to_logger ' | Beginig  C04: Construct the variable table, repeating per variable'

        while @var_count <= (select max(id) from panbal_variables)
        begin

                select  @thevariable = aggregation_variable from panbal_variables where id = @var_count

                execute xdash_ov_output_to_logger ' | @ C04: Looping for '|| @thevariable 
                
                -- Variable to get the the Balance Index at high level.
                select  @HLBalanceIndex = min(Balance_Index)
                from(
                                select  psl.value
                                                ,count(pss.account_number) as Sky_Base_Households
                                                ,count(distinct pbhh.account_number) as Panel_base_Households
                                                ,sqrt(
                                                                avg(
                                                                                (
                                                                                        (
                                                                                                (Panel_base_Households * @Sky_Base
                                                                                                                                                                / Sky_Base_Households
                                                                                                                                                                                                                / @Panel_Base
                                                                                                ) * 100
                                                                                        ) - 100
                                                                                ) * (
                                                                                                (
                                                                                                        (Panel_base_Households * @Sky_Base
                                                                                                                                                                        / Sky_Base_Households
                                                                                                                                                                                                                        / @Panel_Base
                                                                                                        ) * 100
                                                                                                ) - 100
                                                                                        )
                                                                   ) over(partition by Part)
                                                          ) as Balance_Index --formula to get high level balance index
                                                ,'Partition' as Part
                                from    PanBal_segment_snapshots as pss
                                                inner join PanBal_segments_lookup_normalised as psl             --*
                                                on  pss.segment_id = psl.segment_id
                                                and psl.aggregation_variable = @var_count
                                                left join #PanBaseHH as pbhh
                                                on  pbhh.account_number = pss.account_number
                                group   by  psl.value
                        )HLBalanceIndex
                
                Message now()||' | C04: Calculating Balance Index at high level DONE'

                -- To get low level sub variable metrics
                select  psl.value
                                ,count(pss.account_number) as Sky_Base_Households
                                ,count(distinct pbhh.account_number) as Panel_base_Households
                                ,cast(0 as integer) Returning_Households
                                ,(
                                        (Panel_base_Households * @Sky_Base
                                                                                                / Sky_Base_Households
                                                                                                                                                / @Panel_Base
                                        ) * 100 
                                )-100   as Balance_Index
                into    #TempLowLevel
                from    PanBal_segment_snapshots as pss
                                inner join PanBal_segments_lookup_normalised as psl             --*
                                on      pss.segment_id = psl.segment_id
                                and     psl.aggregation_variable = @var_count
                                left join #PanBaseHH as pbhh
                                on      pbhh.account_number = pss.account_number
                group   by      psl.value
                
                Message now()||' | C04: Calculating Low level sub variable metrics DONE'

                -- To get average returning households at low level sub variables
                select  value
                                ,cast(avg(AvgRet) as integer)   as AvgReturners
                into    #TempAvgRetLL
                from    (
                                        select  psl.value
                                                        ,count(swrq.account_number) / 7 AvgRet
                                        from    PanBal_segment_snapshots                                                as pss
                                                        inner join PanBal_segments_lookup_normalised    as psl
                                                        on      pss.segment_id = psl.segment_id
                                                        and     psl.aggregation_variable = @var_count
                                                        left join #SWRQ                                                                 as swrq
                                                        on      swrq.account_number = pss.account_number
                                        group   by      psl.value
                                                                ,swrq.dt
                                        having  AvgRet > 0
                                )       as LLAvgReturners
                group   by      value
                
                Message now()||' | C04: Calculating average returning households at low level sub variables DONE'

                --To update low level average Returning households.
                update  #TempLowLevel as tll
                set     tll.Returning_Households = tarll.AvgReturners
                from    #TempAvgRetLL as tarll
                where   tll.value = tarll.value
                and     tarll.AvgReturners is not null
                
                Message now()||' | C04: Calculating update low level average Returning households DONE'

                -- To get variables with Sky base, Panel base at high level joined with low level split per sub variable with Balance indices
                
                insert into xdash_overview_variables    (  
                                                                                                        aggregation_variable
                                                                                                        ,categories
                                                                                                        ,sky_base_households
                                                                                                        ,panel_base_households
                                                                                                        ,avg_returning_households
                                                                                                        ,balance_index
                                                                                                )
                select  right(('0'||@var_count),2)||' - '||@thevariable as aggregation_variable
                                ,'Summary'                                                                              as categories
                                ,count(pss.account_number)                                              as Sky_Base_Households
                                ,count(distinct pbhh.account_number)                    as Panel_base_Households
                                ,@HLAvgReturners                                                                as Returning_Households
                                ,@HLBalanceIndex                                                                as Balance_Index
                from    PanBal_segment_snapshots        as pss
                                left join #PanBaseHH            as pbhh
                                on pbhh.account_number = pss.account_number
                UNION
                select  right(('0'||@var_count),2)||' - '||@thevariable as aggregation_variable
                                ,coalesce(lk.friendlyname,ll.value) as category
                                ,ll.Sky_Base_Households
                                ,ll.Panel_base_Households
                                ,ll.Returning_Households
                                ,ll.Balance_Index
                from    #TempLowLevel   as ll
                                left join category_lookup as lk
                                on  lower(lk.category_techname) = lower(ll.value)
                                and lk.aggregation_index        = @var_count
                where   trim(value) <> ''
                commit

                Message now()||' | C04: Saving Overview Variables DONE'

                drop table #TempLowLevel
                drop table #TempAvgRetLL
                commit
                
                set @var_count = @var_count + 1

                Message now()||' | Looping through '|| @thevariable ||' DONE'    

        end

        grant select  on xdash_overview_variables  to vespa_group_low_security
        commit
        
        execute xdash_ov_output_to_logger ' | Beginig  C04: Construct the variable table, repeating per variable DONE'
        execute xdash_ov_output_to_logger ' | XDASH_OV_VARIABLE_METRICS DONE'
        commit
        
end;
commit;

grant execute on xdash_ov_variable_metrics to vespa_group_low_security;
commit;
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

**Project Name:                         Panel Balancing
**Analysts:                             Jon Green   (Jonathan.Green@skyiq.co.uk)
                                        Leonardo Ripoli
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@skyiq.co.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FPanel%20Balancing

**Business Brief:

Panel Balancing is a regular exercise to ensure that the viewing panel is as
representative of the wider Sky customer base as possible as the latter evolves over
time. Balancing is also crucial in ensuring that key KPI and contractual obligations
of account coverage are maintained.




**Module:                              M03_Panbal_Segments

This module categorises each account on the sky base against each of the balancing variables. A segment is created for each combination that at least one account matches.
A lookup table is created to find the segment ID for each account, and a lookup table is created with the segment details for each segment ID.

the balancing variables are:

adsmbl     (adsmartable)
region
hhcomp     (household composition)
tenure     (Sky tenure)
package    (Sky TV package)
mr         (multiscreen)
hd     
pvr    
valseg     (value segment)
mosaic 
fss        (financial segment)
onnet      (located in an OnNet area)
skygo  
st         (Sky Talk)
bb         (Broadband)
bb_capable


*/

-------------------------------------------------------------------------------------- [STEP 1]

create or replace procedure V306_M03_PanBal_Segments_adapted
        @weekending     date    = null
 as begin

            create table #PanBal_weekly_sample (account_number    varchar(30)
                  ,cb_key_household  bigint
                  ,cb_key_individual bigint
                  ,adsmbl            varchar(30) default 'Non-Adsmartable'
                  ,region            varchar(40)
                  ,hhcomp            varchar(30)
                  ,tenure            varchar(30)
                  ,package           varchar(30)
                  ,mr                bit         default 0
                  ,hd                bit         default 0
                  ,pvr               bit         default 0
                  ,valseg            varchar(30)
                  ,mosaic            varchar(30)
                  ,fss               varchar(30)
                  ,onnet             bit         default 0
                  ,skygo             bit         default 0
                  ,st                bit         default 0
                  ,bb                bit         default 0
                  ,bb_capable        varchar(8)  default 'No Panel'
                   )
            create unique hg index idx1 on #PanBal_weekly_sample(account_number)
            create        lf index idx2 on #PanBal_weekly_sample(region)
            create        lf index idx3 on #PanBal_weekly_sample(hhcomp)
            create        lf index idx4 on #PanBal_weekly_sample(tenure)
            create        lf index idx5 on #PanBal_weekly_sample(package)
            create        lf index idx6 on #PanBal_weekly_sample(valseg)
            create        lf index idx7 on #PanBal_weekly_sample(mosaic)
            create        lf index idx8 on #PanBal_weekly_sample(fss)

            create table #PanBal_segments_lookup(
                   segment_id        bigint identity primary key
                  ,adsmbl            varchar(30)   default 'Non-Adsmartable'
                  ,region            varchar(40)
                  ,hhcomp            varchar(30)   default 'U'
                  ,tenure            varchar(30)
                  ,package           varchar(30)
                  ,mr                bit           default 0
                  ,hd                bit           default 0
                  ,pvr               bit           default 0
                  ,valseg            varchar(30)   default 'Unknown'
                  ,mosaic            varchar(30)   default 'U'
                  ,fss               varchar(30)   default 'U'
                  ,onnet             bit           default 0
                  ,skygo             bit           default 0
                  ,st                bit           default 0
                  ,bb                bit           default 0
                  ,bb_capable        varchar(8)    default 'No Panel'
                  ,panel_accounts    decimal(10,2) default 0
                  ,base_accounts     int           default 0
                   )
            create lf index lfads on #PanBal_segments_lookup(adsmbl)
            create lf index lfreg on #PanBal_segments_lookup(region)
            create lf index lfhhc on #PanBal_segments_lookup(hhcomp)
            create lf index lften on #PanBal_segments_lookup(tenure)
            create lf index lfpac on #PanBal_segments_lookup(package)
            create lf index lfval on #PanBal_segments_lookup(valseg)
            create lf index lfmos on #PanBal_segments_lookup(mosaic)
            create lf index lffss on #PanBal_segments_lookup(fss)
            create lf index lfbbc on #PanBal_segments_lookup(bb_capable)

            create table #panbal_segments_lookup_unnormalised(
                   segment_id bigint --identity
                  ,v1         varchar(30)   default 'Non-Adsmartable'
                  ,v2         varchar(40)
                  ,v3         varchar(30)   default 'U'
                  ,v4         varchar(30)
                  ,v5         varchar(30)
                  ,v6         bit           default 0
                  ,v7         bit           default 0
                  ,v8         bit           default 0
                  ,v9         varchar(30)   default 'Unknown'
                  ,v10        varchar(30)   default 'U'
                  ,v11        varchar(30)   default 'U'
                  ,v12        bit           default 0
                  ,v13        bit           default 0
                  ,v14        bit           default 0
                  ,v15        bit           default 0
                  ,v16        varchar(8)    default 'No Panel'
                   )

            commit
            create hg index hgseg on #panbal_segments_lookup_unnormalised(segment_id)
            create lf index lfv1 on #panbal_segments_lookup_unnormalised(v1)
            create lf index lfv2 on #panbal_segments_lookup_unnormalised(v2)
            create lf index lfv3 on #panbal_segments_lookup_unnormalised(v3)
            create lf index lfv4 on #panbal_segments_lookup_unnormalised(v4)
            create lf index lfv5 on #panbal_segments_lookup_unnormalised(v5)
            create lf index lfv9 on #panbal_segments_lookup_unnormalised(v9)
            create lf index lfv10 on #panbal_segments_lookup_unnormalised(v10)
            create lf index lfv11 on #panbal_segments_lookup_unnormalised(v11)
            create lf index lfv16 on #panbal_segments_lookup_unnormalised(v16)

           create table #matches(
                  segment_id bigint
                  )

           declare @counter bigint

          truncate table panbal_variables
            insert into panbal_variables(
                   id
                  ,aggregation_variable
                   )
            select 1
                 ,'Adsmartable'
             union
            select 2
                  ,'Region'
             union
            select 3
                  ,'HH Composition'
             union
            select 4
                  ,'Tenure'
             union
            select 5
                  ,'Package'
             union
            select 6
                  ,'Multi-Room'
             union
            select 7
                  ,'HD'
             union
            select 8
                  ,'PVR'
             union
            select 9
                  ,'Value Segment'
             union
            select 10
                  ,'Mosaic'
             union
            select 11
                  ,'Financial Stress'
             union
            select 12
                  ,'On-Net'
             union
            select 13
                  ,'SkyGo'
             union
            select 14
                  ,'Sky-Talk'
             union
            select 15
                  ,'Broadband'
--             union
--            select 16
--                  ,'bb_capable'

                        declare @profiling_thursday date
                        --execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
            set @profiling_thursday = @weekending - 2                           -- but we want a Thursday

                   /**************** L01: ESTABLISH POPULATION ****************/
                -- Captures all active accounts in cust_subs_hist
            SELECT account_number
                  ,cb_key_household
                  ,cb_key_individual
                  ,current_short_description
                  ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
                  ,convert(bit, 0)  AS uk_standard_account
                  ,convert(VARCHAR(20), NULL) AS isba_tv_region
              INTO #weekly_sample
              FROM /*sk_prod.*/cust_subs_hist as csh
             WHERE subscription_sub_type IN ('DTV Primary Viewing')
               AND status_code IN ('AC','AB','PC')
               AND effective_from_dt    <= @profiling_thursday
               AND effective_to_dt      > @profiling_thursday
               AND EFFECTIVE_FROM_DT    IS NOT NULL
               AND cb_key_household     > 0
               AND cb_key_household     IS NOT NULL
               AND cb_key_individual    IS NOT NULL
               AND service_instance_id  IS NOT NULL

                -- De-dupes accounts
            COMMIT
            DELETE FROM #weekly_sample WHERE rank > 1

            COMMIT
            CREATE UNIQUE hg INDEX uhacc ON #weekly_sample (account_number)
            CREATE        lf INDEX lfcur ON #weekly_sample (current_short_description)

                -- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
            UPDATE #weekly_sample
               SET uk_standard_account = CASE WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
                                              ELSE 0
                                         END
                  ,isba_tv_region      = b.isba_tv_region
                  ,cb_key_individual   = b.cb_key_individual
              FROM #weekly_sample AS a
                   inner join /*sk_prod.*/cust_single_account_view AS b ON a.account_number = b.account_number

            COMMIT
            DELETE FROM #weekly_sample WHERE uk_standard_account = 0


                /**************** L02: ASSIGN VARIABLES ****************/
                -- Since "h_household_composition" & "p_head_of_household" are in two separate tables, an intemidiary table is created
                -- so both variables are available for ranking function in the next step
            SELECT cv.cb_key_household
                  ,cv.cb_key_family
                  ,cv.cb_key_individual
                  ,min(cv.cb_row_id)               as cb_row_id
                  ,max(cv.h_household_composition) as h_household_composition
                  ,max(pp.p_head_of_household)     as p_head_of_household
                  ,max(h_mosaic_uk_group)          as mosaic
                  ,max(h_fss_v3_group)             as fss
              INTO #cv_pp
              FROM /*sk_prod.*/EXPERIAN_CONSUMERVIEW cv,
                   /*sk_prod.*/PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
             WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
               AND cv.cb_key_individual is not null
          GROUP BY cv.cb_key_household
                  ,cv.cb_key_family
                  ,cv.cb_key_individual

            COMMIT
            CREATE LF INDEX idx1 on #cv_pp(p_head_of_household)
            CREATE HG INDEX idx2 on #cv_pp(cb_key_family)
            CREATE HG INDEX idx3 on #cv_pp(cb_key_individual)

            SELECT cb_key_individual
                  ,cb_row_id
                  ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
                  ,rank() over(partition by cb_key_individual ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_ind
                  ,h_household_composition
                  ,mosaic
                  ,fss
              INTO #cv_keys
              FROM #cv_pp
             WHERE cb_key_individual IS not NULL
               AND cb_key_individual <> 0

            commit
            DELETE FROM #cv_keys WHERE rank_fam != 1 AND rank_ind != 1

            commit
            CREATE INDEX index_ac on #cv_keys (cb_key_individual)

                -- Populate Package & ISBA TV Region
            INSERT INTO #PanBal_weekly_sample (
                   account_number
                  ,cb_key_household
                  ,cb_key_individual
                  ,package
            )
            SELECT fbp.account_number
                  ,fbp.cb_key_household
                  ,fbp.cb_key_individual
                  ,CASE WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN               'Top Tier'
                        WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN               'Dual Sports'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN               'Dual Movies'
                        WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN               'Single Sports'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN               'Single Movies'
                        WHEN cel.prem_sports > 0 OR  cel.prem_movies > 0 THEN               'Other Premiums'
                        WHEN kids = 1 or music = 1 or news_events = 1 or knowledge = 1 then 'Basic - Ent Extra'
                        ELSE                                                                'Basic - Ent'
                   END
              FROM #weekly_sample AS fbp
                   left join /*sk_prod.*/cust_entitlement_lookup AS cel ON fbp.current_short_description = cel.short_description
             WHERE fbp.cb_key_household IS NOT NULL
               AND fbp.cb_key_individual IS NOT NULL

            commit
              drop table #weekly_sample

                -- Experian variables
            UPDATE #PanBal_weekly_sample as sws
               SET sws.hhcomp = case when cv.h_household_composition in ('00', '01', '02', '03', '09', '10')         then 'A'
                                     when cv.h_household_composition in ('04', '05')                                 then 'B'
                                     when cv.h_household_composition in ('06', '07', '08', '11')                     then 'C'
                                     else                                                                                 'D'
                                end
                  ,fss    = cv.fss
                  ,mosaic = cv.mosaic
              FROM #cv_keys AS cv
             where sws.cb_key_individual = cv.cb_key_individual

                -- coalesce didn't work, so...
            UPDATE #PanBal_weekly_sample as sws set hhcomp ='U' where hhcomp is null
            UPDATE #PanBal_weekly_sample as sws set mosaic ='U' where mosaic is null
            UPDATE #PanBal_weekly_sample as sws set fss ='U'    where fss is null

              drop table #cv_keys

                -- Tenure
            UPDATE #PanBal_weekly_sample as bas
               SET bas.tenure = CASE WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  304  THEN 'A) 0-10 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'B) 10-24 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3652 THEN 'B) 2-10 Years'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) >  3652 THEN 'C) 10 Years+'
                                     ELSE 'D) Unknown'
                                END
                  ,bas.region = sav.isba_tv_region
              FROM /*sk_prod.*/cust_single_account_view sav
             WHERE bas.account_number = sav.account_number

            COMMIT

                -- MR, HD, PVR
            SELECT account_number
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
                  ,1 AS pvr
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
              INTO #scaling_box_level_viewing
              FROM /*sk_prod.*/cust_subs_hist AS csh
             WHERE effective_FROM_dt <= @profiling_thursday
               AND effective_to_dt    > @profiling_thursday
               AND status_code IN  ('AC','AB','PC')
               AND SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing', 'DTV Sky+', 'DTV Extra Subscription', 'DTV HD')
          GROUP BY account_number

            commit

            update #PanBal_weekly_sample as bas
               set bas.hd = blv.hd
                  ,bas.mr = blv.mr
                  ,bas.pvr = blv.pvr
              from #scaling_box_level_viewing as blv
             where bas.account_number = blv.account_number

            update #PanBal_weekly_sample as bas
               set valseg = coalesce(seg.value_seg, 'Unknown')
              from /*sk_prod.*/VALUE_SEGMENTS_DATA as seg
             where bas.account_number = seg.account_number

                -- coalesce didn't work again, so...
            update #PanBal_weekly_sample as bas
               set valseg = 'Unknown' where valseg is null

            update #PanBal_weekly_sample as bas
               set skygo = 1
              from /*sk_prod.*/SKY_PLAYER_USAGE_DETAIL as spu
             where bas.account_number = spu.account_number
               and activity_dt >= '2011-08-18'
                -- this query takes 10 mins

                -- The OnNet goes by postcode, so...
            select account_number
                  ,min(cb_address_postcode) as postcode
                  ,convert(bit, 0) as onnet
              into #onnet_patch
              from /*sk_prod.*/cust_single_account_view
             where cust_active_dtv = 1
          group by account_number

            update #onnet_patch
               set postcode = upper(REPLACE(postcode,' ',''))

            commit
            create unique hg index idx1 on #onnet_patch (account_number)
            create        index joinsy  on #onnet_patch (postcode)

                -- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes
            SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
              INTO #bpe
              FROM /*sk_prod.*/BROADBAND_POSTCODE_EXCHANGE
          GROUP BY postcode

            update #bpe
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on #bpe (postcode)

                -- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
            SELECT postcode as postcode, MAX(exchange_id) as exchID
              INTO #p2e
              FROM /*sk_prod.*/BB_POSTCODE_TO_EXCHANGE
          GROUP BY postcode

            update #p2e
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on #p2e (postcode)

                -- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
            SELECT COALESCE(#p2e.postcode, #bpe.postcode) AS postcode
                  ,COALESCE(#p2e.exchID, #bpe.exchID) as exchange_id
                  ,'OFFNET' as exchange
              INTO #onnet_lookup
              FROM #bpe FULL JOIN #p2e ON #bpe.postcode = #p2e.postcode

            commit
            create unique index fake_pk on #onnet_lookup (postcode)

                -- 4) Update with latest Easynet exchange information
            UPDATE #onnet_lookup
               SET exchange = 'ONNET'
              FROM #onnet_lookup AS base
                   INNER JOIN /*sk_prod.*/easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
             WHERE easy.exchange_status = 'ONNET'

                -- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
                --   spaces removed so your table will either need to have a similar filed or use a REPLACE
                --   function in the join
            UPDATE #onnet_patch
               SET onnet = CASE WHEN tgt.exchange = 'ONNET'
                                THEN 1
                                ELSE 0
                           END
              FROM #onnet_patch AS bas
                   INNER JOIN #onnet_lookup AS tgt on bas.postcode = tgt.postcode

            commit

            update #PanBal_weekly_sample as bas
               set bas.onnet = onn.onnet
              from #onnet_patch as onn
             where bas.account_number = onn.account_number

            update #PanBal_weekly_sample as bas
               set bb = 1
              from /*sk_prod.*/cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'Broadband DSL Line'
               and status_code in ('AC', 'AB', 'PC', 'CF', 'PT')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            update #PanBal_weekly_sample as bas
               set st = 1
              from /*sk_prod.*/cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'SKY TALK SELECT'
               and status_code in ('A', 'FBP', 'PC', 'RI')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            select account_number
              into #noconsent
              from /*sk_prod.*/cust_single_account_view as sav
             where cust_viewing_data_capture_allowed <> 'Y'
          group by account_number

            select account_number
              into #adsmbl
              from /*sk_prod.*/cust_set_top_box
             where active_box_flag = 'Y'
               and (x_pvr_type in ('PVR5', 'PVR6') and x_manufacturer not in ('Samsung'))
           group by account_number

            commit
            create unique hg index idx1 on #adsmbl(account_number)
            create unique hg index idx1 on #noconsent(account_number)

            update #PanBal_weekly_sample as bas
               set adsmbl = case when con.account_number is null then 'Adsmartable consent'
                                                                 else 'Adsmartable non-consent'
                            end
              from #adsmbl as ads
                   left join #noconsent as con on con.account_number = ads.account_number
             where bas.account_number = ads.account_number

--            update #PanBal_weekly_sample as sam
--               set bb_capable = l20_darwin
--              from waterfall_base as wat
--             where sam.account_number = wat.account_number
--               and l07_prod_latest_dtv        = 1
--               and l08_country                = 1
--               and l10_surname                = 1
--               and l11_standard_accounts      = 1
--               and l24_last_callback_dt       = 1

                -- count boxes for every account
            select distinct (ccs.account_number)
                  ,count(distinct card_subscriber_id) as boxes
              into #sky_box_count
              from /*sk_prod.*/CUST_CARD_SUBSCRIBER_LINK as ccs
                   inner join /*sk_prod.*/cust_single_account_view as sav on ccs.account_number = sav.account_number
             where effective_to_dt = '9999-09-09'
               and cust_active_dtv = 1
          group by ccs.account_number

            insert into #panbal_segments_lookup(
                   adsmbl
                  ,region
                  ,hhcomp
                  ,tenure
                  ,package
                  ,mr
                  ,hd
                  ,pvr
                  ,valseg
                  ,mosaic
                  ,fss
                  ,onnet
                  ,skygo
                  ,st
                  ,bb
                  ,bb_capable
                  ,base_accounts
            )
            select adsmbl
                  ,region
                  ,hhcomp
                  ,tenure
                  ,package
                  ,mr
                  ,hd
                  ,pvr
                  ,valseg
                  ,mosaic
                  ,fss
                  ,onnet
                  ,skygo
                  ,st
                  ,bb
                  ,bb_capable
                  ,sum(boxes)
              from #PanBal_weekly_sample as sam
                   inner join #sky_box_count as sbc on sam.account_number = sbc.account_number
          group by adsmbl
                  ,region
                  ,hhcomp
                  ,tenure
                  ,package
                  ,mr
                  ,hd
                  ,pvr
                  ,valseg
                  ,mosaic
                  ,fss
                  ,onnet
                  ,skygo
                  ,st
                  ,bb
                  ,bb_capable

            commit

                -- need to unnormalise the normalised table, so we can find the combinations that don't exist
               --set temporary option identity_insert = ''
               --set temporary option identity_insert = '#panbal_segments_lookup_unnormalised'

            insert into #panbal_segments_lookup_unnormalised(
                   segment_id
                  ,v1
                  ,v2
                  ,v3
                  ,v4
                  ,v5
                  ,v6
                  ,v7
                  ,v8
                  ,v9
                  ,v10
                  ,v11
                  ,v12
                  ,v13
                  ,v14
                  ,v15
                  ,v16
                   )
            select segment_id
                  ,max(case when aggregation_variable = 1 then value else null end)
                  ,max(case when aggregation_variable = 2 then value else null end)
                  ,max(case when aggregation_variable = 3 then value else null end)
                  ,max(case when aggregation_variable = 4 then value else null end)
                  ,max(case when aggregation_variable = 5 then value else null end)
                  ,max(case when aggregation_variable = 6 then value else null end)
                  ,max(case when aggregation_variable = 7 then value else null end)
                  ,max(case when aggregation_variable = 8 then value else null end)
                  ,max(case when aggregation_variable = 9 then value else null end)
                  ,max(case when aggregation_variable = 10 then value else null end)
                  ,max(case when aggregation_variable = 11 then value else null end)
                  ,max(case when aggregation_variable = 12 then value else null end)
                  ,max(case when aggregation_variable = 13 then value else null end)
                  ,max(case when aggregation_variable = 14 then value else null end)
                  ,max(case when aggregation_variable = 15 then value else null end)
                  ,max(case when aggregation_variable = 16 then value else null end)
              from panbal_segments_lookup_normalised -- [STATIC TABLE]
          group by segment_id

               set temporary option identity_insert = ''
               set temporary option identity_insert = '#panbal_segments_lookup'

                -- update with segment id from unnormalised table
                -- db space issue, so have to do this query a bit at a time
               set @counter = 0
             while @counter < (select max(segment_id) from #panbal_segments_lookup_unnormalised) begin
                      update #panbal_segments_lookup as lkp
                         set segment_id = unn.segment_id
                        from #panbal_segments_lookup_unnormalised as unn
                       where v1 = adsmbl
                         and v2 = region
                         and v3 = hhcomp
                         and v4 = tenure
                         and v5 = package
                         and v6 = mr
                         and v7 = hd
                         and v8 = pvr
                         and v9 = valseg
                         and v10 = mosaic
                         and v11 = fss
                         and v12 = onnet
                         and v13 = skygo
                         and v14 = st
                         and v15 = bb
                         and v16 = bb_capable
                         and lkp.segment_id between @counter and @counter + 100000

                         set @counter = @counter +100000
               end

          truncate table PanBal_segment_snapshots
            insert into PanBal_segment_snapshots(account_number
                                                ,segment_id)
            select sam.account_number
                  ,segment_id
              from #PanBal_weekly_sample               as sam
                   inner join #PanBal_segments_lookup as lkp on sam.adsmbl     = lkp.adsmbl
                                                            and sam.region     = lkp.region
                                                            and sam.hhcomp     = lkp.hhcomp
                                                            and sam.tenure     = lkp.tenure
                                                            and sam.package    = lkp.package
                                                            and sam.mr         = lkp.mr
                                                            and sam.hd         = lkp.hd
                                                            and sam.pvr        = lkp.pvr
                                                            and sam.valseg     = lkp.valseg
                                                            and sam.mosaic     = lkp.mosaic
                                                            and sam.fss        = lkp.fss
                                                            and sam.onnet      = lkp.onnet
                                                            and sam.skygo      = lkp.skygo
                                                            and sam.st         = lkp.st
                                                            and sam.bb         = lkp.bb
                                                            and sam.bb_capable = lkp.bb_capable

                -- find the new segments
            insert into #matches
            select bas.segment_id
              from #panbal_segments_lookup as bas
                   inner join #panbal_segments_lookup_unnormalised as unn on v1 = adsmbl
                                                                        and v2 = region
                                                                        and v3 = hhcomp
                                                                        and v4 = tenure
                                                                        and v5 = package
                                                                        and v6 = mr
                                                                        and v7 = hd
                                                                        and v8 = pvr
                                                                        and v9 = valseg
                                                                        and v10 = mosaic
                                                                        and v11 = fss
                                                                        and v12 = onnet
                                                                        and v13 = skygo
                                                                        and v14 = st
                                                                        and v15 = bb
                                                                        and v16 = bb_capable

                -- normalise for new segments
                                
                        --truncate table panbal_segments_lookup_normalised
                        --commit
                        
            insert into panbal_segments_lookup_normalised(
                   segment_id
                  ,aggregation_variable
                  ,value
                   )
            select bas.segment_id
                  ,1
                  ,cast(adsmbl as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,2
                  ,region
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,3
                  ,hhcomp
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,4
                  ,tenure
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,5
                  ,package
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,6
                  ,cast(mr as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,7
                  ,cast(hd as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,8
                  ,cast(pvr as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,9
                  ,valseg
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,10
                  ,mosaic
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,11
                  ,fss
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,12
                  ,cast(onnet as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,13
                  ,cast(skygo as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,14
                  ,cast(st as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,15
                  ,cast(bb as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,16
                  ,cast(bb_capable as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null

                        commit
                         
            update panbal_segments_lookup_normalised
               set curr = 0

            update panbal_segments_lookup_normalised as bas
               set curr = 1
              from panbal_segment_snapshots as snp
             where bas.segment_id = snp.segment_id

     end --V306_M03_PanBal_Segments
 commit

grant execute on V306_M03_PanBal_Segments_adapted to vespa_group_low_security
commit


-------------------------------------------------------------------------------------- [STEP 2]


if object_id('panbal_segments_lookup_normalised') is not null drop table panbal_segments_lookup_normalised

create table panbal_segments_lookup_normalised(
        segment_id              bigint
        ,aggregation_variable   tinyint
        ,value                  varchar(40)
        ,curr                   bit default 0
)

insert  into panbal_segments_lookup_normalised
select  *
from    vespa_analysts.panbal_segments_lookup_normalised
commit

create hg index hg1 on panbal_segments_lookup_normalised(segment_id)
create hg index hg2 on panbal_segments_lookup_normalised(aggregation_variable)
create hg index hg3 on panbal_segments_lookup_normalised(value)
grant select on panbal_segments_lookup_normalised to vespa_group_low_security
commit


if object_id('PanBal_segment_snapshots') is not null drop table PanBal_segment_snapshots

create table PanBal_segment_snapshots(
           account_number varchar(30)
          ,segment_id     int
)

grant select on PanBal_segment_snapshots to vespa_group_low_security
create unique hg index uhacc on PanBal_segment_snapshots(account_number)
commit

if object_id('panbal_variables') is not null drop table panbal_variables
            
create table panbal_variables(
           id                                   int
          ,aggregation_variable varchar(30)
)

insert  into  panbal_variables
select  *
from    vespa_analysts.panbal_variables
commit

grant select on panbal_variables to vespa_group_low_security
create lf index lfid1 on panbal_variables(id)
commit
