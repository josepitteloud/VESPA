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
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@skyiq.co.uk)
**Stakeholder:                          Vespa
**Due Date:                             n/a
**Project Code (Insight Collation):     V241
**SharePoint Folder:                    TBA

**Business Brief:

Panel Balancing is a regular exercise to ensure that the viewing panel is as
representative of the wider Sky customer as possible as the latter evolves over
time. Balancing is also crucial in ensuring that key KPI and contractual obligations
of account coverage are maintained.

This script comprises and ad hoc analysis to address the TA calls coverage KPIs by
highlighting additional accounts that may be enabled onto the BB alt day panel


**Modules:





**Stats:



**Change log:
22/05/2013 Author : Hoi Yu Tang, hoi_yu.tang@skyiq.co.uk



-----------------------------------------------------------------------------------

*/




-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 1. This first query simply overlays onto the proposed movements additional information such as source/destination panels, return expectations, at-risk accounts etc.
-- This query is only really to get an initial view of the data.
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
select
    a.movement
    , b.panel
    , case
            when c.knockout_level_PSTN = 9999   then    'PSTN'
            when c.knockout_level_BB = 9999     then    'BB'
            else null
      end                                       as  destination_panel_type
    , count()                                   as  accounts
    , sum(b.boxes)                              as  boxes
    , round(sum(b.boxes * b.rq),0)              as  returning_boxes_by_rq
    , round(sum(b.boxes * b.cbck_rate),0)       as  returning_boxes_by_cbck_rate
    , round(sum(b.TA_propensity),0)             as  at_risk_accounts
from
                    -- greenj.panbal_amends_20140514           as  a
                    panbal_amends                           as  a
    left join       vespa_analysts.panbal_SAV               as  b       on  a.account_number = b.account_number
    left join       vespa_analysts.waterfall_base           as  c       on  a.account_number = c.account_number
group by
    a.movement
    , b.panel
    , destination_panel_type
order by
    a.movement
    , b.panel
    , destination_panel_type
;

/*
movement    panel   destination_panel_type  accounts    boxes   returning_boxes_by_rq   returning_boxes_by_cbck_rate    at_risk_accounts
Account to add to panels 6/7, eventually for panel 12   NULL    NULL    7254    NULL    NULL    NULL    NULL
Account to add to panels 6/7, eventually for panel 12   NULL    BB  95284   98134   98134   96382   5177
Account to add to panels 6/7, eventually for panel 12   NULL    PSTN    31462   61794   61794   55124   2805
Account to remove from panel 12 NULL    NULL    49  NULL    NULL    NULL    NULL
Account to remove from panel 12 11  NULL    3451    3504    1150    2810    332
Account to remove from panel 12 12  NULL    11500   17106   393 9585    960
*/



----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 2. Starting from all eligible and current panellists, overlay proposed balancing movements and estimate additional alt-day (BB) panellists required to achieve TA coverage KPIs
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- First, estimate the number of TA callers on the Sky base
create variable @TA_callers_SkyBase int;
select @TA_callers_SkyBase = round(sum(TA_propensity),0)
from
                    vespa_analysts.Skybase_TA_scores        as  a
    inner join      sk_prod.cust_single_account_view        as  SAV     on  a.account_number = SAV.account_number
                                                                        and cust_active_dtv = 1
;
select @TA_callers_SkyBase; --1075470
set @TA_callers_SkyBase = 1075470;
select @TA_callers_SkyBase;



-- Combine all data from panbal_SAV, panbal_amends, BB knockout level from the Waterfall, and add ranking to data return-TA-call-propensity
drop table #tmp1;
select
        t1.*
    ,   rank() over (partition by panel, proposed_panellist, knockout_level_BB order by TA_propensity * rq_cbck_hybrid_rate desc) as TA_skew_order
into #tmp1
from (          -- Start here! First subsquery to pull in
    select
            a.*
        ,   b.movement
        ,   c.knockout_level_BB
        ,   case
                when a.rq is not null then a.rq
                else a.cbck_rate
            end     as  rq_cbck_hybrid_rate
        ,   case
                when (a.panel is not null and b.account_number is null)                     then 1  -- current panellist that is not being moved
                when b.movement = 'Account to remove from panel'                            then 0  -- disable panellist
                when b.movement = 'Account to add to panels 6/7, eventually for panel 12'   then 1  -- add to daily panel from base
                when b.movement = 'Account to add to Panel 6 or 7 as segment backup'        then 2  -- add to alt day panel from base
                else 0
            end     as  proposed_panellist
    from
                    vespa_analysts.panbal_SAV           as  a
    left join       panbal_amends                       as  b       on  a.account_number = b.account_number
    left join       vespa_analysts.waterfall_base       as  c       on  a.account_number = c.account_number
    )   as  t1
;

create unique hg index tmp1_u_idx_1 on #tmp1(account_number);

select top 20 * from #tmp1;
select
    proposed_panellist
    ,   count()
from #tmp1
group by proposed_panellist
;

select
        movement
    ,   count()
from panbal_amends
group by movement
;
/*
movement    count()
Account to add to Panel 6 or 7 as segment backup    339764
Account to add to panels 6/7, eventually for panel 12   113000
*/








-- Starting from panel balancing results (greenj.panbal_amends_20140514), shore up the TA accounts coverage by adding the N most at-risk/good returners
drop table #tmp2;
select
        panel                                                       as current_panel
    ,   proposed_panellist
    ,   case
            when (
                        proposed_panellist = 0
                and     current_panel is null
                and     knockout_level_BB = 9999
                and     TA_skew_order between 1 and 300000 -- Adjust this last number as necessary to manually test whether KPIs are reached
                )   then 1
            else 0
        end                                                         as  TA_panellist
    ,   count()                                                     as  accounts
    ,   round(sum(
                    case
                        when rq >= 0.5 or (rq is null and cbck_rate >= 0.5) then 1
                        else 0
                    end
        ),0)                                                        as  accounts_returning
    ,   round(sum(rq_cbck_hybrid_rate),0)                           as  accounts_returning2
    ,   sum(boxes)                                                  as  total_boxes
    ,   round(sum(boxes * rq),0)                                    as  expected_returners_by_rq
    ,   round(sum(boxes * cbck_rate),0)                             as  expected_returners_by_cbck_rate
    ,   round(sum(TA_propensity),0)                                 as  at_risk_accounts_enabled
    ,   100.0 * at_risk_accounts_enabled / @TA_callers_SkyBase      as at_risk_accounts_enabled_coverage
    ,   round(sum(TA_propensity * rq_cbck_hybrid_rate),0)           as  at_risk_accounts_returning
    ,   100.0 * at_risk_accounts_returning / @TA_callers_SkyBase    as at_risk_accounts_returning_coverage
into #tmp2
from #tmp1
where proposed_panellist in (1,2) or TA_panellist = 1 -- select only those who'll end up on the final proposed panel
group by
    panel
    , proposed_panellist
    , TA_panellist
order by
    panel
    , proposed_panellist
    , TA_panellist
;
/*
select * from #tmp2;
current_panel   proposed_panellist  TA_panellist    accounts    accounts_returning  accounts_returning2 total_boxes expected_returners_by_rq    expected_returners_by_cbck_rate at_risk_accounts_enabled    at_risk_accounts_enabled_coverage   at_risk_accounts_returning  at_risk_accounts_returning_coverage
NULL    0   1   250000  250000  250000  250313  250313  221486  82391   7.66092964006434    82391   7.66092964006434
NULL    1   0   126742  126742  126742  159928  159928  151506  7982    .742187136786707282 7982    .742187136786707282
5   1   0   1133811 798753  730156  1464138 665359  915126  135862  12.6328024026704594 87967   8.17940063414135218
6   1   0   191934  28761   56716   243996  46167   115310  18924   1.75960277832017601 5498    .51121835104651922
7   1   0   226308  107884  78236   277645  66834   146336  21840   2.03074004853691876 7368    .685095818572345138
11  1   0   257184  233079  225920  261797  207933  242420  35980   3.34551405432043672 31521   2.93090462774414897
12  1   0   457288  400807  389113  631046  471440  536693  49677   4.61909676699489415 41166   3.82772183324500024

*/


-- Select the proposed alt day panel additions (combined post-balancing and at-risk skewing)
drop table #tmp3;
select
        account_number
    ,   cast(null as tinyint)   as  destination
into #tmp3
from panbal_amends
where movement = 'Account to add to Panel 6 or 7 as segment backup'
union
select
        account_number
    ,   cast(5 as tinyint)      as  destination
from #tmp1
where
            proposed_panellist = 0
    and     panel is null
    and     knockout_level_BB = 9999
    and     TA_skew_order between 1 and 300000 -- Adjust this last number as necessary to manually test whether KPIs are reached
;

select count() from #tmp3; --640035
select top 20 * from #tmp3;



-- Prepare data for export into vespa_analysts.panel_movements_log for campaign building
drop table #tmp4;
select
        acc.account_number
    ,   CSL.card_subscriber_id
    ,   null                            as  source
    ,   acc.destination
    ,   PAV.rq
    ,   PAV.cbck_rate                   as  CA_callback_rate
    ,   cast(CBK.Cbk_Day as tinyint)    as  CA_callback_day
    ,   case
            when    CA_callback_day >= 16   then    dateadd(dd,CA_callback_day,'2014-06-01') - 1
            when    CA_callback_day < 16    then    dateadd(dd,CA_callback_day,'2014-07-01') - 1
            end                         as  requested_enablement_dt
    ,   'Panel Balancing alt.'          as  requested_movement_type
    ,   'KQ'                            as  requested_enablement_route
    ,   'Unknown'                       as  last_ca_callback_route
into #tmp4
from
                #tmp3                                           as  acc
    inner join  sk_prod.cust_card_subscriber_link               as  CSL     on  acc.account_number = CSL.account_number
                                                                            and CSL.current_flag = 'Y'
                                                                            and CSL.effective_to_dt = '9999-09-09'
    inner join  vespa_analysts.panbal_SAV                       as  PAV     on  acc.account_number = PAV.account_number
    left join   vespa_analysts.waterfall_scms_callback_data     as  CBK     on  acc.account_number = CBK.account_number
                                                                            and cast(CSL.card_subscriber_id as int) = CBK.subscriber_id
group by
        acc.account_number
    ,   card_subscriber_id
    ,   source
    ,   destination
    ,   rq
    ,   CA_callback_rate
    ,   CA_callback_day
    ,   requested_enablement_dt
    ,   requested_movement_type
    ,   requested_enablement_route
    ,   last_ca_callback_route
;

create hg index tmp4_idx_1 on #tmp4(account_number);

/* Some checks on #tmp4...
select top 20 * from #tmp4;

select
    count()
    , count(distinct account_number)
    , count(distinct card_subscriber_id)
from #tmp4;
-- 646602   640025  646602

*/


-- Get current timestamp for movements log
create variable @now_dt datetime;
select @now_dt = now();
select @now_dt;

/*
-- Now perform the export proper
insert into vespa_analysts.panel_movements_log(
        account_number
    ,   card_subscriber_id
    ,   source
    ,   destination
    ,   rq
    ,   CA_callback_rate
    ,   CA_callback_day
    ,   requested_enablement_dt
    ,   requested_movement_type
    ,   requested_enablement_route
    ,   last_ca_callback_route
    ,   request_created_dt
    )
select
        account_number
    ,   card_subscriber_id
    ,   source
    ,   destination
    ,   rq
    ,   CA_callback_rate
    ,   datepart(day,min(requested_enablement_dt) over (partition by account_number))   as  CA_callback_day
    ,   requested_enablement_dt
    ,   requested_movement_type
    ,   requested_enablement_route
    ,   last_ca_callback_route
    ,   @now_dt     as  request_created_dt
from #tmp4
;
*/

-- Quick check on the exported movements
select
        request_created_dt
    ,   count()
from vespa_analysts.panel_movements_log
group by request_created_dt
order by request_Created_dt desc
;

-- Estimate on daily enablements
select count(distinct account_number), count() as acc ,acc / 28.0 from sk_prod.panel_movements_log where request_created_dt = '2014-06-09 16:09:59.194939';

