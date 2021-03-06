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

This script provides the panel balancing calculations with a singular source of "input"
data, which can be re-referenced in order to reproduce results, allow for retrospective
testing and QA.


**Modules:

    1. Initialise
    2. Add current panellists and Waterfall accounts
    3. Add CA callback rate to pool of accounts
    4. Add box count
    5. Add segment ID, previously generated by the panbal_segments procedure
    6. Include TA propensity scores
    7. Add flag for prospective BB panellists
    8. Finish




**Stats:

    Running time: ~100s


**Change log:
24/12/2013 Author : Hoi Yu Tang, hoi_yu.tang@skyiq.co.uk
03/01/2014 Rev. 1 : Hoi Yu Tang.
                    Added dynamic creation of table name with date reference.
03/03/2014 Rev. 2 : Hoi Yu Tang.
                    Refactor for better speed and tidy up for seamless execution. Removed pre-reconciliation hacks.
30/04/2014 Rev. 3 : Hoi Yu Tang.
                    Defer writing to a permament table in vespa_anslysts until the end of the analysis.
                    Updated for new Waterfall PSTN/BB split, new active box flag from cust_set_top_box, TA propensity from vespa_analysts table.


-----------------------------------------------------------------------------------

*/


----------------
-- 1. Initialise
----------------

      -- Define new table name - we'll need this at the very end
  create variable @SAV_name varchar(72);
  select @SAV_name = ('panbal_SAV' + dateformat(now(*), '_yyyymmdd'));
  select @SAV_name;


/* -- Create table in vespa_analysts schema if necessary

call dba.sp_drop_table('vespa_analysts','panbal_SAV');
call dba.sp_create_table(
    'vespa_analysts',
    'panbal_SAV',
    '
        account_number      varchar(30)     default null
    ,   segment_id          int             default null
    ,   boxes               tinyint         default null
    ,   cbck_rate           double          default null
    ,   rq                  double          default null
    ,   viq_rq              double          default null
    ,   panel               tinyint         default null
    ,   TA_propensity       double          default null
    ,   bb_panel            bit             default 0
    '
    )
;
create unique hg index panbal_SAV_u_idx_1 on vespa_analysts.panbal_SAV(account_number);

*/


      -- Create table structure
if object_id('#temp_panbal_SAV') is not null drop table #temp_panbal_SAV;

  create table #temp_panbal_SAV(
         account_number      varchar(30)     default null
        ,segment_id          int             default null
        ,boxes               tinyint         default null
        ,cbck_rate           double          default null
        ,rq                  double          default null
        ,viq_rq              double          default null
        ,panel               tinyint         default null
        ,TA_propensity       double          default null
        ,bb_panel            bit             default 0
        )
;

      -- Add index to account_number
  commit;
  create unique hg index temp_panbal_SAV_idx_1 on #temp_panbal_SAV(account_number);




----------------------------------------------------------------------------------------------------
-- 2. Add current panellists and Waterfall accounts
-- Pre-requisite : run Waterfall_procedure first to udpate Waterfall tables on vespa_analysts schema
----------------------------------------------------------------------------------------------------

      -- Get latest date available in panel_data
if object_id('@max_dt') is not null drop variable @max_dt;
  create variable @max_dt date;
  select @max_dt = max(dt)
--    from vespa_analysts.panel_data
    from netezza_data
;

      -- Get all active panel subscribers
  select account_number
        ,card_subscriber_id
        ,panel_no
        ,cast(null as date) as min_dt
        ,null as rq
    into #temp_panel_subs
    from sk_prod.vespa_subscriber_status
   where result = 'Enabled'
     and panel_no in (5, 6, 7, 11, 12)
;

  create        hg index temp_panel_subs_idx_1   on #temp_panel_subs(account_number);
  create unique hg index temp_panel_subs_u_idx_2 on #temp_panel_subs(card_subscriber_id);


      -- Calculate first instance of data return per subscriber and append to the above (filter to current panel only)
if object_id('#temp_min_dt') is not null drop table #temp_min_dt;

--  select subscriber_id
  select box as subscriber_id
        ,min(dt)       as min_dt
    into #temp_min_dt
--    from vespa_analysts.panel_data   as VAP
    from netezza_data   as VAP
--         inner join #temp_panel_subs as SUB on VAP.subscriber_id = cast(SUB.card_subscriber_id as int)
         inner join #temp_panel_subs as SUB on VAP.box = cast(SUB.card_subscriber_id as int)
                                           and VAP.panel         = SUB.panel_no
--   where data_received = 1
group by subscriber_id
;

  commit;
  create unique hg index temp_min_dt_u_idx_1 on #temp_min_dt(subscriber_id);

  update #temp_panel_subs as PAC
     set PAC.min_dt = MDT.min_dt
    from #temp_min_dt   as MDT
   where cast(PAC.card_subscriber_id as int) = MDT.subscriber_id
;

      -- Now calculate the RQ, first at subscriber level, followed by aggregating by account level
if object_id('@rq_window') is not null drop variable @rq_window;
  create variable @rq_window int;
     set @rq_window = 15;

if object_id('#temp_rq') is not null drop table #temp_rq;
  select account_number
        ,min(rq) as rq
    into #temp_rq
    from   (select PA.account_number
                  ,PD.subscriber_id
                  ,sum(1.0 * PD.data_received / (case when (@max_dt - PA.min_dt + 1) < @rq_window then (@max_dt - PA.min_dt + 1) else @rq_window end)) as rq
              from vespa_analysts.panel_data   as PD
                   inner join #temp_panel_subs as PA on convert(int, PA.card_subscriber_id) = PD.subscriber_id
                                                       and dt between (@max_dt - @rq_window + 1) and @max_dt
                                                       and PD.data_received = 1
             where @max_dt > min_dt
          group by PA.account_number
                  ,PD.subscriber_id
          ) as t
group by account_number
;

  insert into #temp_panbal_SAV(
         account_number
        ,rq
        ,panel
        )
  select VSS.account_number
        ,TRQ.rq
        ,VSS.panel_no
    from sk_prod.vespa_subscriber_status     as VSS
         left join #temp_rq                  as TRQ  on VSS.account_number = TRQ.account_number
   where VSS.result = 'Enabled'
     and VSS.panel_no in (5,6,7,11,12)
group by VSS.account_number
        ,TRQ.rq
        ,VSS.panel_no
;

if object_id('#temp_rq') is not null drop table #temp_rq;

      -- Add Waterfall accounts
  insert into #temp_panbal_SAV(account_number)
  select WBA.account_number
    from vespa_analysts.waterfall_base as WBA
         left join #temp_panbal_SAV    as PAV  on      PAV.account_number = WBA.account_number
   where (WBA.knockout_level_PSTN = 9999 or WBA.knockout_level_BB = 9999)
     and PAV.account_number is null
;




---------------------------------------------------------------------------------
-- 3. Add CA callback rate to pool of accounts (modify for BB-connected accounts)
---------------------------------------------------------------------------------

-- previou version
-- update #temp_panbal_SAV as PAV
-- set PAV.cbck_rate = t.cbck_rate
-- from (
--     select
--         account_number
--         , sum(cast(expected_cbcks as double) - cast(missing_cbcks as double)) / sum(cast(expected_cbcks as double)) as cbck_rate
--     from vespa_analysts.waterfall_scms_callback_data
--     where expected_cbcks > 0
--     group by account_number
--     ) as t
-- where PAV.account_number = t.account_number
-- ;


-- Initial calculation of CA callback rate
  update #temp_panbal_SAV as sav
     set cbck_rate = on_time/(expected_cbcks * 1.0)
    from vespa_analysts.waterfall_scms_callback_data as cbk
   where expected_cbcks > 0
     and sav.account_number = cbk.account_number
;


-- Flag all possible candidates for BB panels from Waterfall
  update #temp_panbal_SAV as sav
     set bb_panel = 1
    from vespa_analysts.waterfall_base as wat
   where sav.account_number = wat.account_number
     and knockout_level_bb = 9999
;

-- Flag current BB-panellists as BB-candidates as well (since the Waterfall discounts these by definition)
  update #temp_panbal_SAV as sav
     set bb_panel = 1
   where panel in (5, 11)
;


-- Determine the most recent content download date for each account from the pool
  select apd.account_number
        ,max(last_modified_dt) as max_dt
    into #dl
    from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS as apd
         inner join #temp_panbal_SAV         as sav on apd.account_number = sav.account_number
   where last_modified_dt <= @max_dt
group by apd.account_number
;

-- Modify the CA callback rate of BB-connected panellists and candidates using their recent DL history
  update #temp_panbal_SAV as sav
     set cbck_rate = case when #dl.account_number is null           then 0
                          when datediff(day, max_dt, today()) > 180 then 0
                          else cbck_rate * ((180 - datediff(day, max_dt, today())) / 180.0)
                     end
    from #dl
   where sav.account_number = #dl.account_number
     and bb_panel = 1
;




-------------------
-- 4. Add box count
-------------------

if object_id('#temp_box_count') is not null drop table #temp_box_count;

  select account_number
        ,count() as boxes
    into #temp_box_count
    from sk_prod.cust_set_top_box
   where x_active_box_flag_new = 'Y'
group by account_number
;

  update #temp_panbal_SAV as PAV
     set PAV.boxes = BXC.boxes
    from #temp_box_count as BXC
   where PAV.account_number = BXC.account_number
;

if object_id('#temp_box_count') is not null drop table #temp_box_count;




---------------------------------------------------------------------------
-- 5. Add segment ID, previously generated by the panbal_segments procedure
---------------------------------------------------------------------------

  update #temp_panbal_SAV          as PAV
     set PAV.segment_id = PSS.segment_id
    from greenj.panbal_segment_snapshots       as PSS
   where PAV.account_number = PSS.account_number
;

      -- Remove accounts without a valid segment assignment
  delete from #temp_panbal_SAV
   where segment_id is null
;




----------------------------------
-- 6. Include TA propensity scores
----------------------------------

  update #temp_panbal_SAV                 as PAV
     set PAV.TA_propensity = TAS.TA_propensity
    from vespa_analysts.SkyBase_TA_scores as TAS
   where PAV.account_number = TAS.account_number
;




--------------------------------------------------------------------------------------------
-- 7. Calculate the account-level RQ based on the production VIQ scaling tables
--  This is a more applicable measure of data return and may
--  supersede the conventional RQ as calculated from the vespa_analysts.panel_data table
--------------------------------------------------------------------------------------------

      -- Get most recent date available
if object_id('@viq_max_dt') is not null drop variable @viq_max_dt;
  create variable @viq_max_dt datetime;
  select @viq_max_dt = max(adjusted_event_start_date_vespa) from sk_prod.VIQ_viewing_data_scaling
  where adjusted_event_start_date_vespa <= @max_dt;
  select @viq_max_dt;

      -- Define RQ calculation window
     set @rq_window = 15;

      -- Calculate the RQ. Note: since the ETL and Netezza filtering removes a fair number of accounts, we shall not use the VIQ table to estimate a date of recent panel additions
if object_id('@#temp_viq_rq') is not null drop table #temp_viq_rq;
  select PAV.account_number
        ,1.0 * count() / @rq_window  as viq_rq
    into #temp_viq_rq
    from sk_prod.VIQ_viewing_data_scaling as VIQ
         inner join #temp_panbal_SAV      as PAV on VIQ.account_number = PAV.account_number  -- change this to point at the temp table when finalising the code
                                                and PAV.panel in (11, 12)
   where VIQ.adjusted_event_start_date_vespa between (@viq_max_dt - @rq_window + 1) and @viq_max_dt
group by PAV.account_number
;

  create unique hg index temp_viq_rq_u_idx_1 on #temp_viq_rq(account_number);

      -- Finally, insert into panbal_SAV table
  update #temp_panbal_SAV  as PAV
     set PAV.viq_rq = VIQ.viq_rq
    from #temp_viq_rq as VIQ
   where PAV.account_number = VIQ.account_number
;




-------------
-- XX. Finish
-------------

/*
-- Final checks...
select top 20 * from #temp_panbal_SAV;
select count() from #temp_panbal_SAV;


-- Save snapshot in local schema and add index
execute immediate with result set on ('select * into ' + @SAV_name + ' from #temp_panbal_SAV');
execute immediate with result set on ('create unique hg index ' + @SAV_name + '_u_idx_1 on ' + @SAV_name + '(account_number)');
select @SAV_name;
execute immediate with result set on ('select top 20 * from ' + @SAV_name);

-- Truncate and update panbal_SAV on vespa_analysts with the latest calculations if happy
truncate table vespa_analysts.panbal_SAV;
insert into vespa_analysts.panbal_SAV
select *
from #temp_panbal_SAV
;

*/




