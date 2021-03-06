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




**Module:                              M04_PanBal_SAV

This module provides the panel balancing calculations with a singular source of "input"
data, which can be re-referenced in order to reproduce results, allow for retrospective
testing and QA.

**Sections:

    1. Initialise
    2. Add current panellists and Waterfall accounts
    3. Add CA callback rate to pool of accounts
    4. Add box count
    5. Add segment ID, previously generated by the panbal_segments procedure
    6. Include TA propensity scores
    7. Add flag for prospective BB panellists
    8. Add virtual panel flags
    9. Finish
*/


  create or replace procedure V306_M04_PanBal_SAV
         @r_type bit
        ,@today  date
        ,@country bit -- 0 = uk, 1 = ROI
  as begin

                   ----------------
                   -- 1. Initialise
                   ----------------

            declare @rq_window int
                set @rq_window = 7

                 -- Get most recent date available
                 if object_id('@viq_max_dt') is not null drop variable @viq_max_dt

                 -- Define new table name - we'll need this at the very end
            declare @SAV_name varchar(72)
            declare @viq_max_dt datetime
            declare @pan_max_dt date




            select @SAV_name = ('panbal_SAV' + dateformat(now(*), '_yyyymmdd'))

                -- Create table structure
            create table temp_panbal_SAV(
                   account_number      varchar(30)     default null
                  ,segment_id          int             default null
                  ,boxes               tinyint         default null
                  ,cbck_rate           double          default 1 null
                  ,rq                  double          default 1 null
                  ,true_rq             double          default 0 null
                  ,panel               tinyint         default null
                  ,TA_propensity       double          default null
                  ,bb_panel            bit             default 0
                  ,vp1                 bit             default 0
                  ,vp2                 bit             default 0
                  )

                -- Add index to account_number
            commit
            create hg index hgacc on temp_panbal_SAV(account_number)


                   ---------------------------------------------------------------------------
                   -- 2. Add current panellists and Waterfall accounts
                   -- Pre-requisite : run Waterfall_procedure first to udpate Waterfall tables
                   ---------------------------------------------------------------------------

            insert into temp_panbal_SAV(
                   account_number
                  ,panel
                  )
            select account_number
                  ,panel_no
              from vespa_subscriber_status
             where result = 'Enabled'
               and panel_no in (5, 6, 11, 12)
          group by account_number
                  ,panel_no

            select account_number
                  ,count() as cow
              into temp_dupes
              from temp_panbal_SAV
          group by account_number
            having cow > 1

            commit
            create unique hg index uhacc on temp_dupes(account_number)

            delete
              from temp_panbal_SAV
             where account_number in (
                      select account_number
                        from temp_dupes
                                     )

                -- Add Waterfall accounts
                if @country = 0 begin
                      insert into temp_panbal_SAV(account_number)
                      select WBA.account_number
                        from waterfall_base as WBA
                             left join temp_panbal_SAV as PAV on PAV.account_number = WBA.account_number
                       where (WBA.knockout_level_PSTN = 9999 or WBA.knockout_level_BB = 9999)
                         and PAV.account_number is null
               end
              else begin
                      insert into temp_panbal_SAV(account_number)
                      select WBA.account_number
                        from waterfall_base as WBA
                             left join temp_panbal_SAV as PAV on PAV.account_number = WBA.account_number
                       where (WBA.knockout_level_ROI = 9999)
                         and PAV.account_number is null
               end

                   ---------------------------------------------------------------------------------
                   -- 3. Add CA callback rate to pool of accounts (modify for BB-connected accounts)
                   ---------------------------------------------------------------------------------

                -- Initial calculation of CA callback rate
            update temp_panbal_SAV as sav
               set cbck_rate = on_time/(expected_cbcks * 1.0)
              from vespa_analysts.waterfall_scms_callback_data as cbk
             where expected_cbcks > 0
               and sav.account_number = cbk.account_number

                -- Flag all possible candidates for BB panels from Waterfall
            update temp_panbal_SAV as sav
               set bb_panel = 1
              from waterfall_base as wat
             where sav.account_number = wat.account_number
               and knockout_level_bb = 9999

                -- Flag current BB-panellists as BB-candidates as well (since the Waterfall discounts these by definition)
            update temp_panbal_SAV as sav
               set bb_panel = 1
             where panel in (5, 11)

                -- Determine the most recent content download date for each account from the pool

            create table temp_dl_temp(
                   account_number varchar(30)
                  ,max_dt         date
                  )

            insert into temp_dl_temp(
                   account_number
                  ,max_dt
                  )
            select apd.account_number
                  ,max(last_modified_dt) as max_dt
              from CUST_ANYTIME_PLUS_DOWNLOADS as apd
                   inner join temp_panbal_SAV         as sav on apd.account_number = sav.account_number
          group by apd.account_number

                -- Modify the CA callback rate of BB-connected panellists and candidates using their recent DL history
            update temp_panbal_SAV as sav
               set cbck_rate = case when tdl.account_number is null           then 0
                                    when datediff(day, max_dt, today()) > 180 then 0
                                    when cbck_rate is null then ((180 - datediff(day, max_dt, today())) / 180.0)
                                    else cbck_rate * ((180 - datediff(day, max_dt, today())) / 180.0)
                               end
              from temp_dl_temp as tdl
             where sav.account_number = tdl.account_number
               and bb_panel = 1


                   -------------------
                   -- 4. Add box count
                   -------------------

            select account_number
                  ,count() as boxes
              into temp_box_count
              from cust_set_top_box
             where x_active_box_flag_new = 'Y'
          group by account_number

            update temp_panbal_SAV as PAV
               set PAV.boxes = BXC.boxes
              from temp_box_count as BXC
             where PAV.account_number = BXC.account_number


                   ---------------------------------------------------------------------------
                   -- 5. Add segment ID, previously generated by the panbal_segments procedure
                   ---------------------------------------------------------------------------

            update temp_panbal_SAV          as PAV
               set PAV.segment_id = PSS.segment_id
              from panbal_segment_snapshots  as PSS
             where PAV.account_number = PSS.account_number

                -- Remove accounts without a valid segment assignment
            delete from temp_panbal_SAV
             where segment_id is null


                   ----------------------------------
                   -- 6. Include TA propensity scores
                   ----------------------------------

            update temp_panbal_SAV                 as PAV
               set PAV.TA_propensity = TAS.TA_propensity
              from vespa_analysts.SkyBase_TA_scores as TAS
             where PAV.account_number = TAS.account_number


                   --------------------------------------------------------------------------------------------
                   -- 7. Calculate the account-level RQ based on the production VIQ scaling tables
                   -- This is a more applicable measure of data return and may
                   -- supersede the conventional RQ as calculated from the vespa_analysts.panel_data table
                   --------------------------------------------------------------------------------------------

            select @viq_max_dt = max(adjusted_event_start_date_vespa) from VIQ_viewing_data_scaling

                -- Insert callback rate into RQ as default
            update temp_panbal_SAV  as sav
               set sav.rq = cbck_rate

                -- insert the actual RQ where available
            select account_number
                  ,min(data_return_reliability_metric) as min_rq
              into temp_rq
              from stb_connection_fact
          group by account_number

            commit
            create unique hg index uhgacc on temp_rq(account_number)

            update temp_panbal_SAV as sav
               set rq = min_rq
              from temp_rq as tmp
             where tmp.account_number = sav.account_number


                   -------------------------
                   -- 8. Virtual Panel Flags
                   -------------------------

            update temp_panbal_SAV as sav
               set vp1 = vpa.vp1
                  ,vp2 = vpa.vp2
              from vespa_broadcast_reporting_vp_map as vpa
             where sav.account_number = vpa.account_number


                   -------------
                   -- 9. Finish
                   -------------

                -- Truncate and update panbal_SAV with the latest calculations
          truncate table panbal_sav
            insert into panbal_SAV(
                   account_number
                  ,segment_id
                  ,boxes
                  ,cbck_rate
                  ,rq
                  ,true_rq
                  ,panel
                  ,TA_propensity
                  ,bb_panel
                  ,vp1
                  ,vp2
                  )
            select account_number
                  ,segment_id
                  ,boxes
                  ,cbck_rate
                  ,rq
                  ,true_rq
                  ,panel
                  ,TA_propensity
                  ,bb_panel
                  ,vp1
                  ,vp2
              from temp_panbal_SAV

                if @r_type = 1 begin
                      insert into panbal_SAV_hist(
                             account_number
                            ,segment_id
                            ,boxes
                            ,cbck_rate
                            ,rq
                            ,true_rq
                            ,panel
                            ,TA_propensity
                            ,bb_panel
                            ,vp1
                            ,vp2
                            ,dt
                            )
                      select account_number
                            ,segment_id
                            ,boxes
                            ,cbck_rate
                            ,rq
                            ,true_rq
                            ,panel
                            ,TA_propensity
                            ,bb_panel
                            ,vp1
                            ,vp2
                            ,@today
                        from temp_panbal_SAV
               end

              drop table temp_panbal_SAV
              drop table temp_dupes
              drop table temp_dl_temp
              drop table temp_box_count
              drop table temp_rq

     end; --V306_M04_PanBal_SAV
  commit;

   grant execute on V306_M04_PanBal_SAV to vespa_group_low_security;
  commit;



