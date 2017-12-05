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




**Module:                              M00_Initialise

This module creates the structure and indexes for inter-module tables and the final output tables.

**Tables created:

  waterfall_base
  PanBal_segments_lookup
  PanBal_segment_snapshots
  panbal_SAV
  panbal_amends
  panbal_panel
  secondary_panel_pool
  waterfall_pool
  panbal_results
  panbal_metrics
  panbal_variables
  panbal_segments_lookup_normalised
  panbal_additions

tables where history is kept:
  panbal_run_log
  panbal_metrics_hist
  panbal_sav_hist
  */




  create or replace procedure V306_M00_Initialise
      as begin
   
                -- Recreate Waterfall base table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.0 - Recreate Waterfall_Base table' TO CLIENT
                
                if object_id('Waterfall_Base') is not null begin
                     truncate table Waterfall_Base
--                        drop table Waterfall_Base
               end
              else begin

                      create table Waterfall_Base(
                             account_number           varchar(20) default null primary key null
                            ,l07_prod_latest_dtv      bit         default 0
                            ,l08_country              bit         default 0
                            ,l10_surname              bit         default 0
                            ,l11_standard_accounts    bit         default 0
                            ,l13a_hibernators_pstn    bit         default 0
                            ,l13b_hibernators_bb      bit         default 0
                            ,l14_not_vespa_panel      bit         default 0
                            ,l15_sky_view_panel       bit         default 0
                            ,l20_darwin               varchar(3)            null
                            ,l22_known_prefix         bit         default 1
                            ,l23_empty_prefix         bit         default 1
                            ,l24_last_callback_dt     bit         default 1
                            ,l30_ondemand_downloads   bit         default 1
                            ,knockout_level_PSTN      smallint    default 0
                            ,knockout_level_BB        smallint    default 0
                            ,knockout_level_mix       smallint    default 0
                            ,knockout_level_ROI       smallint    default 0
                            ,knockout_level_ROI_PSTN  smallint    default 0
                            ,knockout_reason_PSTN     varchar(50)           null
                            ,knockout_reason_BB       varchar(50)           null
                            ,knockout_reason_mix      varchar(50)           null
                            ,knockout_reason_ROI      varchar(50)           null
                            ,knockout_reason_ROI_PSTM varchar(50)           null
                             )
                   end
                 grant select on waterfall_base to vespa_group_low_security
            commit

                -- Recreate PanBal_segment_snapshots table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate PanBal_segment_snapshots table' TO CLIENT

                if object_id('PanBal_segment_snapshots') is not null begin
                     truncate table PanBal_segment_snapshots
--                        drop table PanBal_segment_snapshots
               end
              else begin               
                      create table PanBal_segment_snapshots(
                             account_number varchar(30) null
                            ,segment_id     int     not null
                             )
                             
                      create unique hg index uhacc on PanBal_segment_snapshots(account_number)
                       grant select on PanBal_segment_snapshots to vespa_group_low_security

               end
            commit
                   
                -- Recreate panbal_SAV table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate panbal_SAV table' TO CLIENT

                if object_id('panbal_SAV') is not null begin
                     truncate table panbal_SAV
--                        drop table panbal_SAV
               end
              else begin    
                      create table panbal_SAV(
                             account_number varchar(30) default null
                            ,segment_id     int         default null
                            ,boxes          tinyint     default null
                            ,cbck_rate      double      default null
                            ,rq             double      null default 1
                            ,true_rq        double      null default 1
                            ,panel          tinyint     default null
                            ,TA_propensity  double      default null
                            ,bb_panel       bit         default 0
                            ,vp1            bit         default 0
                            ,vp2            bit         default 0
                             )
                      create unique hg index panbal_SAV_u_idx_1 on panbal_SAV(account_number)
                       grant select on PanBal_sav to vespa_group_low_security
                   
               end   
            commit  
           
                 -- Create panbal_SAV_hist table
            MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Create panbal_SAV_hist table' TO CLIENT
 
                if object_id('panbal_SAV_hist') is not null begin
               end
              else begin
                      create table panbal_SAV_hist(
                             account_number varchar(30) default null
                            ,segment_id     int         default null
                            ,boxes          tinyint     default null
                            ,cbck_rate      double      default null
                            ,rq             double      default null
                            ,true_rq        double      default null
                            ,panel          tinyint     default null
                            ,TA_propensity  double      default null
                            ,bb_panel       bit         default 0
                            ,vp1            bit         default 0
                            ,vp2            bit         default 0
                            ,dt             date
                             )
                      create hg index panbal_SAV_u_idx_1 on panbal_SAV_hist(account_number)
                       grant select on PanBal_sav_hist to vespa_group_low_security
               end
            commit
      
                -- Recreate vespa_broadcast_reporting_vp_map table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate vespa_broadcast_reporting_vp_map table' TO CLIENT
      
                if object_id('vespa_broadcast_reporting_vp_map') is not null begin
                     truncate table vespa_broadcast_reporting_vp_map
--                        drop table vespa_broadcast_reporting_vp_map
               end
              else begin
                      CREATE TABLE vespa_broadcast_reporting_vp_map(
                             account_number     varchar(20) NOT NULL PRIMARY KEY DEFAULT NULL
                            ,vespa_panel        int NOT NULL DEFAULT NULL
                            ,vp1                tinyint null DEFAULT 0
                            ,vp2                tinyint null DEFAULT 0
                            ,vp3                tinyint null DEFAULT 0
                            ,vp4                tinyint null DEFAULT 0
                            ,vp5                tinyint null DEFAULT 0
                            ,vp6                tinyint null DEFAULT 0
                            ,vp7                tinyint null DEFAULT 0
                            ,vp8                tinyint null DEFAULT 0
                            ,vp9                tinyint null DEFAULT 0
                            ,vp10               tinyint null DEFAULT 0
                             )  
                       grant select on vespa_broadcast_reporting_vp_map to vespa_group_low_security
               end
            commit

                -- Recreate panbal_amends table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate panbal_amends table' TO CLIENT

                if object_id('panbal_amends') is not null begin
                     truncate table panbal_amends
--                        drop table panbal_amends
               end
              else begin    
                      create table panbal_amends(
                             account_number                                     varchar(30) null
                            ,movement                                           varchar(100) null
                             )
             
                       grant select on panbal_amends to vespa_group_low_security
               end
            commit
   
                -- Recreate panbal_panel table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate panbal_panel table' TO CLIENT

                if object_id('panbal_panel') is not null begin
                    truncate table panbal_panel
--                        drop table panbal_panel
               end
              else begin
                      create table panbal_panel(
                             account_number                                     varchar(30) null
                            ,segment_id                                         int null
                             )   
                       grant select on panbal_panel to vespa_group_low_security
               end
            commit
   
                -- Recreate secondary_panel_pool table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate secondary_panel_pool table' TO CLIENT

                if object_id('secondary_panel_pool') is not null begin                
                truncate table secondary_panel_pool
--                   drop table secondary_panel_pool
               end
              else begin
                      create table secondary_panel_pool(
                             account_number                                     varchar(30) null
                            ,segment_id                                         int null
                            ,rq                                                 double null
                            ,thi                                                double null
                             )
                       grant select on secondary_panel_pool to vespa_group_low_security commit
                      create hg index idx1 on secondary_panel_pool(segment_id) commit
                      create hg index idx2 on secondary_panel_pool(thi) commit
               end   
   
                -- Recreate waterfall_pool table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate waterfall_pool table' TO CLIENT

                if object_id('waterfall_pool') is not null begin
                     truncate table waterfall_pool
--                        drop table waterfall_pool
               end
              else begin
                      create table waterfall_pool(
                             account_number                                     varchar(30) null
                            ,segment_id                                         int null
                            ,rq                                                 double null
                            ,thi                                                double null
                             )
                       grant select on waterfall_pool to vespa_group_low_security commit
                      create hg index idx1 on waterfall_pool(thi) commit
                      create hg index idx3 on waterfall_pool(segment_id) commit
                      create hg index idx4 on waterfall_pool(account_number) commit
               end
      
                -- Recreate panbal_results table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate panbal_results table' TO CLIENT

                if object_id('panbal_results') is not null begin
                     truncate table panbal_results
--                        drop table panbal_results
               end
               else begin
                       create table panbal_results(
                              imbalance                                         double null --the highest variable
                             ,tot_imb                                           double null --total across all variables
                             ,records                                           int null    --in the panel
                             ,from_waterfall                                    bit default 0
                             ,tim                                               datetime
                              )   
                        grant select on panbal_results to vespa_group_low_security
               end
            commit
   
                -- Recreate panbal_metrics table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate panbal_metrics table' TO CLIENT

                if object_id('panbal_metrics') is not null begin
                     truncate table panbal_metrics
--                        drop table panbal_metrics
               end
              else begin
                      create table panbal_metrics(
                             metric                                            varchar(50) null
                            ,value                                             float null
                             )
                       grant select on panbal_metrics to vespa_group_low_security
               end
            commit
   
                -- Recreate panbal_metrics_hist table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Create panbal_metrics_hist table' TO CLIENT

                if object_id('panbal_metrics_hist') is not null begin
               end
              else begin
                      create table panbal_metrics_hist(
                             metric                                            varchar(50) null
                            ,value                                             float null
                            ,dt                                                date
                             )
                       grant select on panbal_metrics_hist to vespa_group_low_security
               end
            commit

                -- Recreate panbal_variables table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate panbal_variables table' TO CLIENT

                if object_id('panbal_variables') is not null begin
                     truncate table panbal_variables
--                        drop table panbal_variables
               end
              else begin    
                      create table panbal_variables(
                             id                                                int null
                            ,aggregation_variable                              varchar(30) null
                             )
                       grant select on panbal_variables to vespa_group_low_security
                      create lf index lfid1 on panbal_variables(id)
               end
            commit
   
                -- Recreate panbal_segments_lookup_normalised table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate panbal_segments_lookup_normalised table' TO CLIENT

                if object_id('panbal_segments_lookup_normalised') is not null begin
                    truncate table panbal_segments_lookup_normalised
--                        drop table panbal_segments_lookup_normalised
               end
              else begin
                      create table panbal_segments_lookup_normalised(
                             segment_id                                        bigint null
                            ,aggregation_variable                              tinyint null
                            ,value                                             varchar(40) null
                            ,curr                                              bit default 0
                             )
                       grant select on panbal_segments_lookup_normalised to vespa_group_low_security
                      create hg index hgseg on panbal_segments_lookup_normalised(segment_id) commit
                      create hg index lfagg on panbal_segments_lookup_normalised(aggregation_variable) commit
                      create hg index lfval on panbal_segments_lookup_normalised(value) commit
               end
          
                -- Recreate PanBal_run_log table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Create PanBal_run_log table' TO CLIENT

                if object_id('panbal_run_log') is not null begin
               end
              else begin
                      create table panbal_run_log(
                             run_time       datetime
                            ,task           varchar(50) null
                            ,notes          varchar(50) null
                             )
                       grant select on PanBal_run_log to vespa_group_low_security
               end

                -- Recreate PanBal_additions
           message cast(now() as timestamp)||' | Initialise M00.1 - Create PanBal_additions table' to client

                if object_id('panbal_additions') is not null begin
                     truncate table panbal_additions
--                        drop table panbal_additions
               end
              else begin
                      create table panbal_additions(
                             account_number varchar(30) null
                             )
                       grant select on panbal_additions to vespa_group_low_security
                      create hg index hgacc on panbal_additions(account_number) commit
               end
            commit
     end;

   grant execute on V306_M00_Initialise to vespa_group_low_security;
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




**Module:                              M01_Process_Manager

This module runs each of the modules required for balancing


*/
  create or replace procedure V306_M01_PanBal
         @max_imb          int   = 20
        ,@min_boxes        int   = 0        --to return per day
        ,@max_boxes        int   = 10000000 --to return per day
        ,@min_vp1b         int   = 0        --to return per day
        ,@min_ta_prop      float = 0
        ,@min_ta_returning float = 0
        ,@now              date  = today()
        ,@gen_schema       bit   = 0
        ,@prec             int   = 500
        ,@run_type         bit   = 0        --default 0 for daily metrics, 1 for full balancing
        ,@country_manager  bit   = 0        --default 0 for UK, 1 for ROI
      as begin

           declare @halt bit default 0      
 
            select @halt = case when task like '%run finished' then 0 else 1 end
              from panbal_run_log
             where run_time = (
                      select max(run_time) 
                        from panbal_run_log
                              )
                if @halt = 0 begin

                         set rowcount 0
                     execute(     'V306_M00_Initialise')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,case when @run_type = 1 then 'Full balancing run started'
                                                     else 'Daily metrics run started'
                              end
                            ,''
                      commit

                     execute('call V306_M02_Waterfall           (@general_schema = @gen_schema
                                                                ,@today          = @now
                                                                 )')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,'Waterfall module complete'
                            ,''
                      commit

                     execute(     'V306_M03_PanBal_Segments')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,'Segments module complete'
                            ,''
                      commit

                     execute(     'V306_M05_VirtPan')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,'VirtPan module complete'
                            ,''
                      commit

                     execute('call V306_M04_PanBal_SAV          (@r_type         = @run_type
                                                                ,@today          = @now
                                                                ,@country        = @country_manager
                                                                 )')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,'SAV module complete'
                            ,''
                      commit

                          if @run_type = 1 begin
                               execute('call V306_M06_Main      (@max_imbalance  = @max_imb
                                                                ,@min_b          = @min_boxes
                                                                ,@precision      = @prec
                                                                 )')
                                insert into panbal_run_log(run_time
                                                          ,task
                                                          ,notes
                                                           )
                                select now()
                                      ,'Main module complete'
                                      ,''
                                commit

                               execute('call V306_M07_VolCheck  (@max_b          = @max_boxes
                                                                ,@min_vp1        = @min_vp1b
                                                                ,@min_ta         = @min_ta_prop
                                                                ,@min_ta_ret     = @min_ta_returning
                                                                 )')
                                insert into panbal_run_log(run_time
                                                          ,task
                                                          ,notes
                                                           )
                                select now()
                                      ,'VolCheck module complete'
                                      ,''
                                commit

                         end
                     execute('call V306_M08_Metrics             (@r_type         = @run_type
                                                                ,@today          = @now
                                                                 )')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,case when @run_type = 1 then 'Full balancing run finished'
                                                     else 'Daily metrics run finished'
                              end
                            ,''
                      commit
           end
          else
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,'Failed attempt to start'
                            ,''
                     message cast(now() as timestamp)||' | Failed attempt to start' to client
                            
         end; -- procedure V306_M01_PanBal
commit;

 grant execute on V306_M01_PanBal to vespa_group_low_security;
commit;


/*
      -- To run the whole process
 execute ('  call V306_M01_PanBal(@max_imb          = 15
                                 ,@min_boxes        = 600000
                                 ,@max_boxes        = 10000000
                                 ,@min_vp1b         = 300000
                                 ,@min_ta_prop      = .25
                                 ,@min_ta_returning = .12
                                 ,@prec             = 500
                                 ,@gen_schema       = 0
                                 ,@run_type         = 0        --default 0 for daily metrics, 1 for full balancing
                                 ,@country_manager  = 0        --0 for UK, 1 for ROI
                                 )')

  insert into panbal_run_log(run_time
                            ,task
                            ,notes
                             )
  select now()
        ,'Manually set to run finished'
        ,''

        -- to update the panel movements log
 execute ('call V306_M09_Update_Movements_Log  (@first_enablement = ''2014-12-01''
                                   )')

*/




/* for testing:

 execute('call V306_M02_Waterfall  (@general_schema = 0
                                   ,@today          = today()
                                    )')

 execute('call V306_M04_PanBal_SAV (@r_type         = 0
                                   ,@today          = today()
                                   ,@country        = 0 --UK
                                    )')

 execute('call V306_M06_Main       (@max_imbalance = 18
                                   ,@min_b         = 0
                                   ,@precision     = 500
                                    )')

 execute('call V306_M07_VolCheck   (@max_b      = 10000000
                                   ,@min_vp1    = 0
                                   ,@min_ta     = 0
                                   ,@min_ta_ret = 0
                                     )')

 execute('call V306_M08_Metrics    (@r_type         = 1
                                   ,@today          = today()
                                    )')
*/


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




**Module:                              M02_Waterfall

This module, previously independent, is now integrated within the balancing process (although thois module could be run on its own)
It flags each account on the sky base against several checks.
If an account passes all the tests, it can be added to the panels. There are separate tests for PSTN and broadband.
Tests are flagged 1 for a pass, and 0 for a fail

** Genral tests performed:
  l07_prod_latest_dtv    Accounts must have a TV package
  l08_country            Must live in the UK
  l10_surname            Must have a surname on file
  l11_standard_accounts  Not Staff, etc.
  l13_hibernators        Not on the list of hibernators, maintained by the Decisioning team
  l14_not_vespa_panel    Must not be on a VESPA panel already
  l24_last_callback_dt   There must have been at least one successful Conditional Access callback in the last six months

** Broadband specific tests:
  l20_darwin             All boxes for the account must be Darwin

** PSTN specific tests:
  l22_known_prefix       It must be known whether there is a dialling prefix on the line
  l23_empty_prefix       There must not be a dialling prefix for the line
  l30_ondemand_downloads
  l31_singlebox

*/
  create or replace procedure V306_M02_Waterfall
         @general_schema bit    =       0
        ,@today          date   =       today()
      as begin

                   ---------------------
                   -- M02.0 - Initialise
                   ---------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.0 - Initialising Environment' TO CLIENT

                -- Prepare temp base table
            create table Temp_Waterfall_Base(
                   account_number           varchar(20) default null primary key
                  ,l07_prod_latest_dtv      bit         default 0
                  ,l08_country              bit         default 0
                  ,l10_surname              bit         default 0
                  ,l11_standard_accounts    bit         default 0
                  ,l13a_hibernators_pstn    bit         default 1
                  ,l13b_hibernators_bb      bit         default 1
                  ,l14_not_vespa_panel      bit         default 0
                  ,l20_darwin               varchar(3)  null
                  ,l22_known_prefix         bit         default 1
                  ,l23_empty_prefix         bit         default 1
                  ,l24_last_callback_dt     bit         default 1
                  ,l30_ondemand_downloads   bit         default 1
                  ,l31_singlebox            bit         default 0
                  ,knockout_level_PSTN      smallint    default 0
                  ,knockout_level_BB        smallint    default 0
                  ,knockout_level_mix       smallint    default 0
                  ,knockout_level_ROI       smallint    default 0
                  ,knockout_level_ROI_PSTN  smallint    default 0
                  ,knockout_reason_PSTN     varchar(50) null
                  ,knockout_reason_BB       varchar(50) null
                  ,knockout_reason_mix      varchar(50) null
                  ,knockout_reason_ROI      varchar(50) null
                  ,knockout_reason_ROI_PSTN varchar(50) null
                   )

                if object_id('waterfall_box_base') is not null begin
                    truncate table waterfall_box_base
--                        drop table waterfall_box_base
               end
              else begin
                      create table waterfall_box_base(
                             account_number varchar(30) null
                            ,subscriber_id   int null
                            ,enable          varchar(7) null
                             )
               end

            create table temp_waterfall_box_rules(
                   account_number        varchar(30) null
                  ,subscriber_id         int null
                  ,service_instance_id   varchar(30) null
                  ,darwin                bit default 0
                  ,known_prefix          bit default 0
                  ,empty_prefix          bit default 0
                  ,last_callback_dt      bit default 0
                  ,last_dl_dt            bit default 0
                   )

            commit
            create hg index uhacc on temp_waterfall_box_rules(account_number)
            create hg index hgser on temp_waterfall_box_rules(service_instance_id)

           declare @6months_ago date

                    --------------------------------------
                    -- M02.1 - Begin populating base table
                    --------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.1 - Begin populating base table' TO CLIENT

                -- Update flags for Active, UK, valid surname, and standard accounts
            insert into Temp_Waterfall_Base(
                   account_number
                  ,l07_prod_latest_dtv
                  ,l08_country
                  ,l10_surname
                  ,l11_standard_accounts
                   )
            select sav.account_number
                  ,min(case when sav.PROD_LATEST_DTV_STATUS = 'Active'        then 1 else 0 end) as l07_prod_latest_dtv
                  ,min(case when sav.PTY_COUNTRY_CODE = 'GBR'                 then 1 else 0 end) as l08_country
                  ,min(case when sav.CB_NAME_SURNAME_soundex IS NOT NULL and
                                 sav.CB_NAME_SURNAME_soundex <> ''            then 1 else 0 end) as l10_surname
                  ,min(case when sav.ACCT_TYPE_code = 'STD'                   then 1 else 0 end) as l11_standard_accounts
              from CUST_SINGLE_ACCOUNT_VIEW as sav
             where sav.CUST_ACTIVE_DTV = 1
          group by sav.account_number

                   --------------------------------------
                   -- M02.2 - Account-based rules
                   --------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.2 - Account-based rules' TO CLIENT

                -- Rule 13 - idenfify hibernator accounts
            update Temp_Waterfall_Base as bas
               set l13a_hibernators_pstn = 0
              from vespa_analysts.panel_exclusions as exc
             where bas.account_number = exc.account_number
               and exclude_from like '%P%'

            update Temp_Waterfall_Base as bas
               set l13b_hibernators_bb = 0
              from vespa_analysts.panel_exclusions as exc
             where bas.account_number = exc.account_number
               and exclude_from like '%B%'
            commit

                -- Rule 14: - identify accounts that are NOT already on a panel
            update Temp_Waterfall_Base
               set l14_not_vespa_panel = 1
             where ACCOUNT_NUMBER NOT IN (SELECT VESPA_PANEL_STATUS.ACCOUNT_NUMBER FROM VESPA_PANEL_STATUS WHERE VESPA_PANEL_STATUS.PANEL_NO in (5, 6, 7, 11, 12))
            commit

                   --------------------------------------
                   -- M02.3 - STB-based rules
                   --------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules' TO CLIENT

                -- Identify active STBs
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Identify active STBs' TO CLIENT

            insert into temp_waterfall_box_rules(
                   account_number
                  ,service_instance_id
                   )
            select account_number
                  ,service_instance_id
              from cust_set_top_box as stb
             where x_active_box_flag_new = 'Y'

                -- Add subscriber_id to those STBs
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Add subscriber_id to those STBs' TO CLIENT

            update temp_waterfall_box_rules as bas
               set bas.subscriber_id = csi.si_external_identifier
              from cust_service_instance as csi
             where csi.src_system_id = bas.service_instance_id
               and effective_to_dt = '9999-09-09'

                -- Get Subscriber Ids allocated to multiple Account Numbers
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Identify subscribers allocated to multiple accounts' TO CLIENT

            select subscriber_id
                  ,count(*) as cow
              into temp_Ambiguous_Sub_Ids
              from temp_waterfall_box_rules
          group by subscriber_id
            having cow > 1

                -- Delete these accounts
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Removing subscribers allocated to multiple accounts' TO CLIENT

            delete from temp_waterfall_box_rules
             where account_number in (
                      select account_number
                        from temp_Ambiguous_Sub_Ids as amb
                             inner join temp_waterfall_box_rules as box on amb.subscriber_id = box.subscriber_id
                                      )

                -- prefixes - null or empty (Rules 22 & 23)
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Identify boxes with NULL or empty dialling prefixes' TO CLIENT

            update temp_waterfall_box_rules as bas
               set bas.known_prefix       = case when cb.subscriber_id is not null then 1 else 0 end           -- Callback record present for the box
                  ,bas.empty_prefix       = case when trim(prefix) = '' or prefix is null then 1 else 0 end
              from vespa_analysts.Waterfall_callback_data  cb
             where bas.subscriber_id = cb.subscriber_id
               and cb.callback_seq = 1

                -- Identify active Darwin STBs
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Identify active Darwin STBs' TO CLIENT

            update temp_waterfall_box_rules as bas
               set bas.darwin = case when  x_model_number like 'DRX 89%'
                                       or  x_manufacturer = 'Samsung'
                                       or (stb.x_manufacturer = 'Pace' and stb.x_pvr_type = 'PVR4') then 1 else 0 end
              from cust_Set_top_box as stb
             where bas.service_instance_id = stb.service_instance_id
               and x_active_box_flag_new = 'Y'

                -- the prefix issue is now fixed for these 2 boxes
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Prefix issue fixed for Darwin' TO CLIENT

            update temp_waterfall_box_rules as bas
               set bas.known_prefix       = 1
                  ,bas.empty_prefix       = 1
             where darwin = 1

                -- last_callback_dt in the last 6 months (Rule 24)
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Calculate last CA callback day' TO CLIENT

                -- Callbacks - fail the test if there were none in the last 6 months
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Callbacks - not all missing from the last 6 months' TO CLIENT

               set @6months_ago = dateadd(month, -6, @today)

            select si_external_identifier
                  ,max(last_callback_dt) as dt
              into temp_lastcall
              from cust_stb_callback_summary
          group by si_external_identifier
            having dt > @6months_ago

            commit
            create unique hg index uhsub on temp_lastcall(si_external_identifier)

            update temp_waterfall_box_rules as bas
               set last_callback_dt = 1
              from temp_lastcall as cal
             where bas.subscriber_id = cast(cal.si_external_identifier as int)

                -- On Demand downloads (by box)
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Determine most recent OnDemand download by card_id' TO CLIENT

            create table temp_dl_by_box(
                   card_id             varchar(30) null
                  ,service_instance_id varchar(30) null
                   )

           declare @6m_ago date
              set @6m_ago = dateadd(month, -6, @today)

            insert into temp_dl_by_box(
                   card_id
                   )
            select card_id
              from temp_waterfall_box_rules as bas
                   inner join CUST_ANYTIME_PLUS_DOWNLOADS as apd on last_modified_dt > @6m_ago
                                                                and bas.account_number = apd.account_number
          group by card_id

            commit
            create unique hg index uhcar on temp_dl_by_box(card_id)

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Add service_instance_id' TO CLIENT

            update temp_dl_by_box as bas
               set bas.service_instance_id = cid.service_instance_id
              from cust_card_issue_dim as cid
             where bas.card_id = left(cid.card_id, 8)
               and card_status = 'Enabled'

                -- if there has been an on demand download in the last 6 months (by box)
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Flag if downloaded within the last 6 months' TO CLIENT

            update temp_waterfall_box_rules as bas
               set last_dl_dt = 1
              from temp_dl_by_box as dls
             where bas.service_instance_id = dls.service_instance_id



                   ---------------------------------------------------
                   -- M02.4 - Apply box info & apply rules to accounts
                   ---------------------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.4 - Apply box info & apply rules to accounts' TO CLIENT

            update Temp_Waterfall_Base as bas
               set bas.l22_known_prefix = 0         -- ALL boxes must have known prefix information
              from temp_waterfall_box_rules as box
             where bas.account_number = box.account_number
               and box.known_prefix = 0

            update Temp_Waterfall_Base as bas
               set bas.l23_empty_prefix = 0         -- ALL boxes must have no prefix
              from temp_waterfall_box_rules as box
             where bas.account_number = box.account_number
               and box.empty_prefix = 0

            update Temp_Waterfall_Base as bas
               set bas.l24_last_callback_dt = 0     -- ALL boxes must have a callback
              from temp_waterfall_box_rules as box
             where bas.account_number = box.account_number
               and box.last_callback_dt = 0

            update Temp_Waterfall_Base as bas
               set bas.l30_ondemand_downloads = 0   -- Each box must have an on demand download in the last 6 months
              from temp_waterfall_box_rules as box
             where bas.account_number = box.account_number
               and box.last_dl_dt = 0

                -- Rule 20 - Darwin
            select account_number
                  ,count(1) as boxes
                  ,sum(darwin) as darwin
              into temp_darwin
              from temp_waterfall_box_rules as box
          group by account_number

            update Temp_Waterfall_Base as bas
               set l20_darwin = case when darwin = boxes then 'Yes'
                                     when darwin = 0     then 'No'
                                     else                     'Mix'
                                end
              from temp_darwin as box
             where bas.account_number = box.account_number

                   ---------------------------------------------------
                   -- M02.5 - Calculate Waterfall knockout levels
                   ---------------------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.5 - Calculate Waterfall knockout levels' TO CLIENT

            update Temp_Waterfall_Base
               set knockout_level_BB  =   case when l07_prod_latest_dtv        = 0 then 7
                                               when l08_country                = 0 then 8
                                               when l10_surname                = 0 then 10
                                               when l11_standard_accounts      = 0 then 11

                                               when l13b_hibernators_bb        = 0 then 13
                                               when l14_not_vespa_panel        = 0 then 14
                                               when l20_darwin            <> 'Yes' then 20
                                               when l24_last_callback_dt       = 0 then 24
                                               when l30_ondemand_downloads     = 0 then 130
                                               else                                     9999 end -- pass!
                  ,knockout_level_PSTN  = case when l07_prod_latest_dtv        = 0 then 7
                                               when l08_country                = 0 then 8
                                               when l10_surname                = 0 then 10
                                               when l11_standard_accounts      = 0 then 11

                                               when l14_not_vespa_panel        = 0 then 14
                                               when l24_last_callback_dt       = 0 then 24
                                               when l13a_hibernators_pstn      = 0 then 113
                                               when l22_known_prefix           = 0 then 122
                                               when l23_empty_prefix           = 0 then 123
                                               else                                     9999 end -- pass!
                  ,knockout_level_ROI =   case when l07_prod_latest_dtv        = 0 then 7
                                               when l08_country                = 1 then 8
                                               when l10_surname                = 0 then 10
                                               when l11_standard_accounts      = 0 then 11

                                               when l13b_hibernators_bb        = 0 then 13
                                               when l14_not_vespa_panel        = 0 then 14
                                               when l20_darwin            <> 'Yes' then 20
                                               when l24_last_callback_dt       = 0 then 24
                                               when l30_ondemand_downloads     = 0 then 130
                                               else                                     9999 end -- pass!
                  ,knockout_level_ROI_PSTN  = case
                                               when l07_prod_latest_dtv        = 0 then 7
                                               when l08_country                = 1 then 8
                                               when l10_surname                = 0 then 10
                                               when l11_standard_accounts      = 0 then 11

                                               when l14_not_vespa_panel        = 0 then 14
                                               when l24_last_callback_dt       = 0 then 24
                                               when l13a_hibernators_pstn      = 0 then 113
                                               when l22_known_prefix           = 0 then 122
                                               when l23_empty_prefix           = 0 then 123
                                               else                                     9999 end -- pass!

            insert into waterfall_box_base(
                   account_number
                  ,subscriber_id
                  ,enable)
            select bas.account_number
                  ,subscriber_id
                  ,case when last_dl_dt             = 1
                         and l13b_hibernators_bb    = 1 then 'BB'
                        when l13a_hibernators_pstn  = 1
                         and known_prefix           = 1
                         and empty_prefix           = 1 then 'PSTN'
                        else                                 'Neither' end
              from Temp_Waterfall_Base as bas
                   left join temp_waterfall_box_rules as box on bas.account_number = box.account_number
             where l07_prod_latest_dtv    = 1
               and l08_country            = 1
               and l10_surname            = 1
               and l11_standard_accounts  = 1
               and l14_not_vespa_panel    = 1
               and l24_last_callback_dt   = 1
               and knockout_level_PSTN    < 9999
               and knockout_level_BB      < 9999

                -- count boxes that can be pstn/ bb by account
            select account_number
                  ,sum(case when enable = 'Neither' then 1 else 0 end) as neither
                  ,sum(case when enable = 'PSTN'    then 1 else 0 end) as pstn
              into temp_waterfall_box_base_accounts
              from waterfall_box_base
          group by account_number

            commit
            create unique hg index uhacc on temp_waterfall_box_base_accounts(account_number)

            update Temp_Waterfall_Base as bas
               set knockout_level_bb = 9999
              from temp_waterfall_box_base_accounts as box
             where bas.account_number = box.account_number
               and knockout_level_BB = 131
               and neither = 0
               and pstn = 0

            update Temp_Waterfall_Base as bas
               set knockout_level_mix  =  case when l07_prod_latest_dtv        = 0 then 7
                                               when l08_country                = 0 then 8
                                               when l10_surname                = 0 then 10
                                               when l11_standard_accounts      = 0 then 11
                                               when l14_not_vespa_panel        = 0 then 14
                                               when l24_last_callback_dt       = 0 then 24
                                               when knockout_level_BB = 9999 and knockout_level_PSTN = 9999 then  99 -- already allocated to PSTN or BB
                                               when neither > 0                                             then 100 -- at least one box can go on neither panel
                                               else                                                             9999 -- pass!
                                          end
              from temp_waterfall_box_base_accounts as box
             where bas.account_number = box.account_number

            update Temp_Waterfall_Base as bas
               set knockout_reason_bb = case knockout_level_bb when 7   then 'DTV account'
                                                               when 8   then 'Country'
                                                               when 10  then 'Surname'
                                                               when 11  then 'Standard_accounts'
                                                               when 13  then 'Hibernators'
                                                               when 14  then 'Not_vespa_panel'
                                                               when 15  then 'Sky_view_panel'
                                                               when 20  then 'Darwin'
                                                               when 24  then 'Last_callback_dt'
                                                               when 130 then 'On Demand downloads'
                                                               else          'Potential BB panellist'
                                        end
                  ,knockout_reason_pstn = case knockout_level_pstn when 8   then 'Country'
                                                                   when 10  then 'Surname'
                                                                   when 11  then 'Standard_accounts'
                                                                   when 14  then 'Not_vespa_panel'
                                                                   when 15  then 'Sky_view_panel'
                                                                   when 20  then 'Darwin'
                                                                   when 24  then 'Last_callback_dt'
                                                                   when 113 then 'Hibernators'
                                                                   when 122 then 'Prefix information unknown'
                                                                   when 123 then 'Empty prefix'
                                                                   else          'Potential PSTN panellist'
                                          end
                  ,knockout_reason_mix = case when knockout_level_bb   < 125 or knockout_level_bb   = 9999  then 'Already accounted for'
                                              when knockout_level_pstn < 113 or knockout_level_pstn = 9999 then 'Already accounted for'
                                              when knockout_level_mix  = 100 then 'At least one box cannot go on either panel'
                                              else                                'Potential Mixed panellist'
                                          end
                 ,knockout_reason_roi = case knockout_level_roi when 7   then 'DTV account'
                                                                when 8   then 'Country'
                                                                when 10  then 'Surname'
                                                                when 11  then 'Standard_accounts'
                                                                when 13  then 'Hibernators'
                                                                when 14  then 'Not_vespa_panel'
                                                                when 15  then 'Sky_view_panel'
                                                                when 20  then 'Darwin'
                                                                when 24  then 'Last_callback_dt'
                                                                when 130 then 'On Demand downloads'
                                                                else          'Potential ROI panellist'
                                         end
                 ,knockout_reason_roi_pstn = case knockout_level_roi_pstn when 8   then 'Country'
                                                                   when 10  then 'Surname'
                                                                   when 11  then 'Standard_accounts'
                                                                   when 14  then 'Not_vespa_panel'
                                                                   when 15  then 'Sky_view_panel'
                                                                   when 20  then 'Darwin'
                                                                   when 24  then 'Last_callback_dt'
                                                                   when 113 then 'Hibernators'
                                                                   when 122 then 'Prefix information unknown'
                                                                   when 123 then 'Empty prefix'
                                                                   else          'Potential ROI PSTN panellist'
                                          end

                   ---------------------------------------------------
                   -- M02.6 - Save output waterfall_base table
                   ---------------------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.6 - Save output waterfall_base table' TO CLIENT

                if @general_schema = 0 begin

                          -- Create Waterfall_Base table if necessary (for standalone execution of this procedure)
                          if object_id('Waterfall_Base') is null begin
                                create table Waterfall_Base(
                                       account_number         varchar(20) default null primary key
                                      ,l07_prod_latest_dtv    bit         default 0
                                      ,l08_country            bit         default 0
                                      ,l10_surname            bit         default 0
                                      ,l11_standard_accounts  bit         default 0
                                      ,l13a_hibernators_pstn  bit         default 0
                                      ,l13b_hibernators_bb    bit         default 0
                                      ,l14_not_vespa_panel    bit         default 0
                                      ,l15_sky_view_panel     bit         default 0
                                      ,l20_darwin             varchar(3) null
                                      ,l22_known_prefix       bit         default 1
                                      ,l23_empty_prefix       bit         default 1
                                      ,l24_last_callback_dt   bit         default 1
                                      ,l30_ondemand_downloads bit         default 1
                                      ,knockout_level_PSTN    smallint    default 0 null
                                      ,knockout_level_BB      smallint    default 0 null
                                      ,knockout_level_mix     smallint    default 0 null
                                      ,knockout_level_ROI     smallint    default 0 null
                                      ,knockout_reason_PSTN   varchar(50) null
                                      ,knockout_reason_BB     varchar(50) null
                                      ,knockout_reason_mix    varchar(50) null
                                      ,knockout_reason_ROI    varchar(50) null
                                       )
                             commit
                             grant select on waterfall_base to vespa_group_low_security

                         end

                    truncate table waterfall_base
                      commit

                      insert into waterfall_base(
                             account_number
                            ,l07_prod_latest_dtv
                            ,l08_country
                            ,l10_surname
                            ,l11_standard_accounts
                            ,l13a_hibernators_pstn
                            ,l13b_hibernators_bb
                            ,l14_not_vespa_panel
                            ,l20_darwin
                            ,l22_known_prefix
                            ,l23_empty_prefix
                            ,l24_last_callback_dt
                            ,l30_ondemand_downloads
                            ,knockout_level_PSTN
                            ,knockout_level_BB
                            ,knockout_level_mix
                            ,knockout_level_ROI
                            ,knockout_level_ROI_PSTN
                            ,knockout_reason_PSTN
                            ,knockout_reason_BB
                            ,knockout_reason_mix
                            ,knockout_reason_ROI
                            ,knockout_reason_ROI_PSTN
                             )
                      select account_number
                            ,l07_prod_latest_dtv
                            ,l08_country
                            ,l10_surname
                            ,l11_standard_accounts
                            ,l13a_hibernators_pstn
                            ,l13b_hibernators_bb
                            ,l14_not_vespa_panel
                            ,l20_darwin
                            ,l22_known_prefix
                            ,l23_empty_prefix
                            ,l24_last_callback_dt
                            ,l30_ondemand_downloads
                            ,knockout_level_PSTN
                            ,knockout_level_BB
                            ,knockout_level_mix
                            ,knockout_level_ROI
                            ,knockout_level_ROI_PSTN
                            ,knockout_reason_PSTN
                            ,knockout_reason_BB
                            ,knockout_reason_mix
                            ,knockout_reason_ROI
                            ,knockout_reason_ROI_PSTN
                        from Temp_Waterfall_Base

                             commit

               end
              else begin
                          if object_id('vespa_analysts.Waterfall_Base') is not null begin
                               execute('call dba.sp_drop_table (''vespa_analysts'',''Waterfall_Base'')')
                               execute('call dba.sp_create_table (''vespa_analysts'',''Waterfall_Base'',''
                                             account_number         varchar(20) default null primary key
                                            ,l07_prod_latest_dtv    bit         default 0
                                            ,l08_country            bit         default 0
                                            ,l10_surname            bit         default 0
                                            ,l11_standard_accounts  bit         default 0
                                            ,l13a_hibernators_pstn  bit         default 0
                                            ,l13b_hibernators_bb    bit         default 0
                                            ,l14_not_vespa_panel    bit         default 0
                                            ,l20_darwin             varchar(3) null
                                            ,l22_known_prefix       bit         default 1
                                            ,l23_empty_prefix       bit         default 1
                                            ,l24_last_callback_dt   bit         default 1
                                            ,l30_ondemand_downloads bit         default 1
                                            ,knockout_level_PSTN    smallint    default 0 null
                                            ,knockout_level_BB      smallint    default 0 null
                                            ,knockout_level_mix     smallint    default 0 null
                                            ,knockout_level_ROI     smallint    default 0 null
                                            ,knockout_level_ROI_PSTN smallint    default 0 null
                                            ,knockout_reason_PSTN   varchar(50) null
                                            ,knockout_reason_BB     varchar(50) null
                                            ,knockout_reason_mix    varchar(50) null
                                            ,knockout_reason_ROI    varchar(50) null
                                            ,knockout_reason_ROI_PSTN varchar(50) null''
                                       )')

                                insert into vespa_analysts.waterfall_base(
                                       account_number
                                      ,l07_prod_latest_dtv
                                      ,l08_country
                                      ,l10_surname
                                      ,l11_standard_accounts
                                      ,l13a_hibernators_pstn
                                      ,l13b_hibernators_bb
                                      ,l14_not_vespa_panel
                                      ,l20_darwin
                                      ,l22_known_prefix
                                      ,l23_empty_prefix
                                      ,l24_last_callback_dt
                                      ,l30_ondemand_downloads
                                      ,knockout_level_PSTN
                                      ,knockout_level_BB
                                      ,knockout_level_mix
                                      ,knockout_level_ROI
                                      ,knockout_level_ROI_PSTN
                                      ,knockout_reason_PSTN
                                      ,knockout_reason_BB
                                      ,knockout_reason_mix
                                      ,knockout_reason_ROI
                                      ,knockout_reason_ROI_PSTN
                                       )
                                select account_number
                                      ,l07_prod_latest_dtv
                                      ,l08_country
                                      ,l10_surname
                                      ,l11_standard_accounts
                                      ,l13a_hibernators_pstn
                                      ,l13b_hibernators_bb
                                      ,l14_not_vespa_panel
                                      ,l20_darwin
                                      ,l22_known_prefix
                                      ,l23_empty_prefix
                                      ,l24_last_callback_dt
                                      ,l30_ondemand_downloads
                                      ,knockout_level_PSTN
                                      ,knockout_level_BB
                                      ,knockout_level_mix
                                      ,knockout_level_ROI
                                      ,knockout_level_ROI_PSTN
                                      ,knockout_reason_PSTN
                                      ,knockout_reason_BB
                                      ,knockout_reason_mix
                                      ,knockout_reason_ROI
                                      ,knockout_reason_ROI_PSTN
                                  from Temp_Waterfall_Base
                         end
               end
             commit

               drop table temp_Waterfall_Base
               drop table temp_waterfall_box_rules
               drop table temp_Ambiguous_Sub_Ids
               drop table temp_lastcall
               drop table temp_dl_by_box
               drop table temp_darwin
               drop table temp_waterfall_box_base_accounts

            MESSAGE cast(now() as timestamp)||' | Waterfall M02 - DONE' TO CLIENT

     end; -- V306_M02_Waterfall procedure
  commit;

   grant execute on V306_M02_Waterfall to vespa_group_low_security;
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

  create or replace procedure V306_M03_PanBal_Segments
           @include_churned bit   = 0
      as begin

            create table temp_PanBal_weekly_sample (
                   account_number    varchar(30) null
                  ,cb_key_household  bigint null
                  ,cb_key_individual bigint null
                  ,adsmbl            varchar(30) default 'Non-Adsmartable' null
                  ,region            varchar(40) null
                  ,hhcomp            varchar(30) null
                  ,tenure            varchar(30) null
                  ,package           varchar(30) null
                  ,mr                bit         default 0
                  ,hd                bit         default 0
                  ,pvr               bit         default 0
                  ,valseg            varchar(30) null
                  ,mosaic            varchar(30) null
                  ,fss               varchar(30) null
                  ,onnet             bit         default 0
                  ,skygo             bit         default 0
                  ,st                bit         default 0
                  ,bb                bit         default 0
                  ,bb_capable        varchar(8)  default 'No Panel' null
                   )
            create unique hg index idx1 on temp_PanBal_weekly_sample(account_number)
            create        lf index idx2 on temp_PanBal_weekly_sample(region)
            create        lf index idx3 on temp_PanBal_weekly_sample(hhcomp)
            create        lf index idx4 on temp_PanBal_weekly_sample(tenure)
            create        lf index idx5 on temp_PanBal_weekly_sample(package)
            create        lf index idx6 on temp_PanBal_weekly_sample(valseg)
            create        lf index idx7 on temp_PanBal_weekly_sample(mosaic)
            create        lf index idx8 on temp_PanBal_weekly_sample(fss)

            create table temp_PanBal_segments_lookup(
                   segment_id        bigint identity primary key null
                  ,adsmbl            varchar(30)   default 'Non-Adsmartable' null
                  ,region            varchar(40) null
                  ,hhcomp            varchar(30)   default 'U' null
                  ,tenure            varchar(30) null
                  ,package           varchar(30) null
                  ,mr                bit           default 0
                  ,hd                bit           default 0
                  ,pvr               bit           default 0
                  ,valseg            varchar(30)   default 'Unknown' null
                  ,mosaic            varchar(30)   default 'U' null
                  ,fss               varchar(30)   default 'U' null
                  ,onnet             bit           default 0
                  ,skygo             bit           default 0
                  ,st                bit           default 0
                  ,bb                bit           default 0
                  ,bb_capable        varchar(8)    default 'No Panel' null
                  ,panel_accounts    decimal(10,2) default 0 null
                  ,base_accounts     int           default 0 null
                   )
            create lf index lfads on temp_PanBal_segments_lookup(adsmbl)
            create lf index lfreg on temp_PanBal_segments_lookup(region)
            create lf index lfhhc on temp_PanBal_segments_lookup(hhcomp)
            create lf index lften on temp_PanBal_segments_lookup(tenure)
            create lf index lfpac on temp_PanBal_segments_lookup(package)
            create lf index lfval on temp_PanBal_segments_lookup(valseg)
            create lf index lfmos on temp_PanBal_segments_lookup(mosaic)
            create lf index lffss on temp_PanBal_segments_lookup(fss)
            create lf index lfbbc on temp_PanBal_segments_lookup(bb_capable)

            create table temp_PanBal_segments_lookup_unnormalised(
                   segment_id bigint identity null
                  ,v1         varchar(30)   default 'Non-Adsmartable' null
                  ,v2         varchar(40) null
                  ,v3         varchar(30)   default 'U' null
                  ,v4         varchar(30) null
                  ,v5         varchar(30) null
                  ,v6         bit           default 0
                  ,v7         bit           default 0
                  ,v8         bit           default 0
                  ,v9         varchar(30)   default 'Unknown' null
                  ,v10        varchar(30)   default 'U' null
                  ,v11        varchar(30)   default 'U' null
                  ,v12        bit           default 0
                  ,v13        bit           default 0
                  ,v14        bit           default 0
                  ,v15        bit           default 0
                  ,v16        varchar(8)    default 'No Panel' null
                   )

            commit
            create hg index hgseg on temp_PanBal_segments_lookup_unnormalised(segment_id)
            create lf index lfv1  on temp_PanBal_segments_lookup_unnormalised(v1)
            create lf index lfv2  on temp_PanBal_segments_lookup_unnormalised(v2)
            create lf index lfv3  on temp_PanBal_segments_lookup_unnormalised(v3)
            create lf index lfv4  on temp_PanBal_segments_lookup_unnormalised(v4)
            create lf index lfv5  on temp_PanBal_segments_lookup_unnormalised(v5)
            create lf index lfv9  on temp_PanBal_segments_lookup_unnormalised(v9)
            create lf index lfv10 on temp_PanBal_segments_lookup_unnormalised(v10)
            create lf index lfv11 on temp_PanBal_segments_lookup_unnormalised(v11)
            create lf index lfv16 on temp_PanBal_segments_lookup_unnormalised(v16)

            create table temp_matches(
                   segment_id bigint
                  )

           declare @counter bigint

          truncate table panbal_variables
            insert into panbal_variables(
                   id
                  ,aggregation_variable
                   )
            select 1
                 ,'adsmbl'
             union
            select 2
                  ,'region'
             union
            select 3
                  ,'hhcomp'
             union
            select 4
                  ,'tenure'
             union
            select 5
                  ,'package'
             union
            select 6
                  ,'mr'
             union
            select 7
                  ,'hd'
--             union
--            select 8
--                  ,'pvr'
             union
            select 9
                  ,'valseg'
             union
            select 10
                  ,'mosaic'
             union
            select 11
                  ,'fss'
             union
            select 12
                  ,'onnet'
             union
            select 13
                  ,'skygo'
             union
            select 14
                  ,'st'
             union
            select 15
                  ,'bb'

           declare @profiling_thursday date
           execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
               set @profiling_thursday = @profiling_thursday - 2                           -- but we want a Thursday

            create table temp_weekly_sample(
                   account_number            varchar(30) null
                  ,cb_key_household          bigint null
                  ,cb_key_individual         bigint null
                  ,current_short_description varchar(50) null
                  ,rank                      int
                  ,uk_standard_account       bit default 0
                  ,isba_tv_region            varchar(20) null
                  )

                -- Captures all active accounts in cust_subs_hist
                if @include_churned = 1 begin
                      insert into temp_weekly_sample
                      SELECT account_number
                            ,cb_key_household
                            ,cb_key_individual
                            ,current_short_description
                            ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
                            ,convert(bit, 0)  AS uk_standard_account
                            ,convert(VARCHAR(20), NULL) AS isba_tv_region
                        FROM cust_subs_hist as csh
                       WHERE subscription_sub_type IN ('DTV Primary Viewing')
                         AND status_code IN ('AC', 'AB', 'PC', 'SC', 'PO')
                         AND effective_from_dt    <= @profiling_thursday
                         AND effective_to_dt      > @profiling_thursday
                         AND EFFECTIVE_FROM_DT    IS NOT NULL
                         AND cb_key_household     > 0
                         AND cb_key_household     IS NOT NULL
                         AND cb_key_individual    IS NOT NULL
                         AND service_instance_id  IS NOT NULL
               end
              else begin
                      insert into temp_weekly_sample
                      SELECT account_number
                            ,cb_key_household
                            ,cb_key_individual
                            ,current_short_description
                            ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
                            ,convert(bit, 0)  AS uk_standard_account
                            ,convert(VARCHAR(20), NULL) AS isba_tv_region
                        FROM cust_subs_hist as csh
                       WHERE subscription_sub_type IN ('DTV Primary Viewing')
                         AND status_code IN ('AC', 'AB', 'PC')
                         AND effective_from_dt    <= @profiling_thursday
                         AND effective_to_dt      > @profiling_thursday
                         AND EFFECTIVE_FROM_DT    IS NOT NULL
                         AND cb_key_household     > 0
                         AND cb_key_household     IS NOT NULL
                         AND cb_key_individual    IS NOT NULL
                         AND service_instance_id  IS NOT NULL
               end

                -- De-dupe accounts
            COMMIT
            DELETE FROM temp_weekly_sample WHERE rank > 1

            COMMIT
            CREATE UNIQUE hg INDEX uhacc ON temp_weekly_sample (account_number)
            CREATE        lf INDEX lfcur ON temp_weekly_sample (current_short_description)

                -- Take out non-standard accounts as these are not currently in the scope of Vespa
            UPDATE temp_weekly_sample
               SET uk_standard_account = CASE WHEN b.acct_type='Standard' AND b.account_number <>'?' THEN 1
                                              ELSE 0
                                         END
                  ,isba_tv_region      = b.isba_tv_region
                  ,cb_key_individual   = b.cb_key_individual
              FROM temp_weekly_sample AS a
                   inner join cust_single_account_view AS b ON a.account_number = b.account_number

            COMMIT
            DELETE FROM temp_weekly_sample WHERE uk_standard_account = 0


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
              INTO temp_cv_pp
              FROM EXPERIAN_CONSUMERVIEW cv,
                   PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
             WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
               AND cv.cb_key_individual is not null
          GROUP BY cv.cb_key_household
                  ,cv.cb_key_family
                  ,cv.cb_key_individual

            COMMIT
            CREATE LF INDEX idx1 on temp_cv_pp(p_head_of_household)
            CREATE HG INDEX idx2 on temp_cv_pp(cb_key_family)
            CREATE HG INDEX idx3 on temp_cv_pp(cb_key_individual)

            SELECT cb_key_individual
                  ,cb_row_id
                  ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
                  ,rank() over(partition by cb_key_individual ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_ind
                  ,h_household_composition
                  ,mosaic
                  ,fss
              INTO temp_cv_keys
              FROM temp_cv_pp
             WHERE cb_key_individual IS not NULL
               AND cb_key_individual <> 0

            commit
            DELETE FROM temp_cv_keys WHERE rank_fam != 1 AND rank_ind != 1

            commit
            CREATE INDEX index_ac on temp_cv_keys (cb_key_individual)

                -- Populate package
            INSERT INTO temp_PanBal_weekly_sample (
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
              FROM temp_weekly_sample AS fbp
                   left join cust_entitlement_lookup AS cel ON fbp.current_short_description = cel.short_description
             WHERE fbp.cb_key_household IS NOT NULL
               AND fbp.cb_key_individual IS NOT NULL

            commit

                -- Experian variables
            UPDATE temp_PanBal_weekly_sample as sws
               SET sws.hhcomp = case when cv.h_household_composition in ('00', '01', '02', '03', '09', '10')         then 'A'
                                     when cv.h_household_composition in ('04', '05')                                 then 'B'
                                     when cv.h_household_composition in ('06', '07', '08', '11')                     then 'C'
                                     else                                                                                 'D'
                                end
                  ,fss    = cv.fss
                  ,mosaic = cv.mosaic
              FROM temp_cv_keys AS cv
             where sws.cb_key_individual = cv.cb_key_individual

                -- Coalesce didn't work, so...
            UPDATE temp_PanBal_weekly_sample as sws set hhcomp = 'U' where hhcomp is null
            UPDATE temp_PanBal_weekly_sample as sws set mosaic = 'U' where mosaic is null
            UPDATE temp_PanBal_weekly_sample as sws set fss    = 'U'    where fss is null

                -- ROI region preliminary
            select bas.account_number
                  ,pty_country_code
                  ,isba_tv_region
                  ,case when cb_address_status = '1' and roi_address_match_source is not null and cb_address_county is not null then upper(cb_address_county) -- take cleansed geographic county where address has been fully matched to Geodirectory
                                                                        when upper(pty_county_raw) like '%DUBLIN%'    then 'DUBLIN' -- otherwise use standardised form of county from the Chordiant raw county field for all 26 counties
                                                                        when upper(pty_county_raw) like '%WESTMEATH%' then 'WESTMEATH' -- make sure WESTMEATH is above MEATH in the hierarchy otherwise WESTMEATH will get set to MEATH!
                                                                        when upper(pty_county_raw) like '%MEATH%'     then 'MEATH'
                        when upper(pty_county_raw) like '%CARLOW%'    then 'CARLOW'
                        when upper(pty_county_raw) like '%CAVAN%'     then 'CAVAN'
                        when upper(pty_county_raw) like '%CLARE%'     then 'CLARE'
                        when upper(pty_county_raw) like '%CORK%'      then 'CORK'
                        when upper(pty_county_raw) like '%DONEGAL%'   then 'DONEGAL'
                        when upper(pty_county_raw) like '%GALWAY%'    then 'GALWAY'
                        when upper(pty_county_raw) like '%KERRY%'     then 'KERRY'
                        when upper(pty_county_raw) like '%KILDARE%'   then 'KILDARE'
                        when upper(pty_county_raw) like '%KILKENNY%'  then 'KILKENNY'
                        when upper(pty_county_raw) like '%LAOIS%'     then 'LAOIS'
                        when upper(pty_county_raw) like '%LEITRIM%'   then 'LEITRIM'
                        when upper(pty_county_raw) like '%LIMERICK%'  then 'LIMERICK'
                        when upper(pty_county_raw) like '%LONGFORD%'  then 'LONGFORD'
                        when upper(pty_county_raw) like '%LOUTH%'     then 'LOUTH'
                        when upper(pty_county_raw) like '%MAYO%'      then 'MAYO'
                        when upper(pty_county_raw) like '%MONAGHAN%'  then 'MONAGHAN'
                        when upper(pty_county_raw) like '%OFFALY%'    then 'OFFALY'
                        when upper(pty_county_raw) like '%ROSCOMMON%' then 'ROSCOMMON'
                        when upper(pty_county_raw) like '%SLIGO%'     then 'SLIGO'
                        when upper(pty_county_raw) like '%TIPPERARY%' then 'TIPPERARY'
                        when upper(pty_county_raw) like '%WATERFORD%' then 'WATERFORD'
                        when upper(pty_county_raw) like '%WEXFORD%'   then 'WEXFORD'
                        when upper(pty_county_raw) like '%WICKLOW%'   then 'WICKLOW'
                                                                        when pty_county_raw is null and upper(pty_town_raw) like '%DUBLIN%' then 'DUBLIN' -- otherwise look for Dublin postal districts as raw county often null for these
                                                                        else 'Unknown'
                                                                end as roi_county
              into temp_roi_region
              from cust_single_account_view as sav
                   inner join temp_PanBal_weekly_sample as bas on sav.account_number = bas.account_number

                -- Region
            update temp_PanBal_weekly_sample as bas
               set region = case when pty_country_code = 'IRL' then case when ROI_County in ('DUBLIN','KILDARE','LAOIS','LONGFORD','LOUTH','MEATH','OFFALY','WESTMEATH','WICKLOW')    then 'ROI EASTERN AND MIDLANDS'
                                                                                                                                 when ROI_County in ('CAVAN','DONEGAL','GALWAY','LEITRIM','MAYO','MONAGHAN','ROSCOMMON','SLIGO')              then 'ROI NORTHERN AND WESTERN'
                                                                                                                                 when ROI_County in ('CARLOW','CLARE','CORK','KERRY','KILKENNY','LIMERICK','TIPPERARY','WATERFORD','WEXFORD') then 'ROI SOUTHERN'
                                                                                                                                else 'ROI Not Defined'
                                                                                                                        end
                                 else isba_tv_region
                             end
              from temp_roi_region as reg
             where bas.account_number = reg.account_number

                -- Tenure
            UPDATE temp_PanBal_weekly_sample as bas
               SET bas.tenure = CASE WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  304 THEN 'A) 0-10 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'B) 10-24 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3652 THEN 'C) 2-10 Years'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) >  3652 THEN 'D) 10 Years+'
                                     ELSE 'E) Unknown'
                                END
              FROM cust_single_account_view as sav
             WHERE bas.account_number = sav.account_number


            COMMIT

                -- MR, HD, PVR
            SELECT account_number
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
                  ,1 AS pvr
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
              INTO temp_scaling_box_level_viewing
              FROM cust_subs_hist AS csh
             WHERE effective_FROM_dt <= @profiling_thursday
               AND effective_to_dt    > @profiling_thursday
               AND status_code IN  ('AC','AB','PC')
               AND SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing', 'DTV Sky+', 'DTV Extra Subscription', 'DTV HD')
          GROUP BY account_number

            commit

            update temp_PanBal_weekly_sample as bas
               set bas.hd = blv.hd
                  ,bas.mr = blv.mr
                  ,bas.pvr = blv.pvr
              from temp_scaling_box_level_viewing as blv
             where bas.account_number = blv.account_number

            update temp_PanBal_weekly_sample as bas
               set valseg = coalesce(seg.value_seg, 'Unknown')
              from VALUE_SEGMENTS_DATA as seg
             where bas.account_number = seg.account_number

                -- coalesce didn't work again, so...
            update temp_PanBal_weekly_sample as bas
               set valseg = 'Unknown' where valseg is null

            update temp_PanBal_weekly_sample as bas
               set skygo = 1
              from SKY_PLAYER_USAGE_DETAIL as spu
             where bas.account_number = spu.account_number
               and activity_dt >= '2011-08-18'
                -- this query takes 10 mins

                -- The OnNet goes by postcode, so...
            select account_number
                  ,min(cb_address_postcode) as postcode
                  ,convert(bit, 0) as onnet
              into temp_onnet_patch
              from cust_single_account_view
             where cust_active_dtv = 1
          group by account_number

            update temp_onnet_patch
               set postcode = upper(REPLACE(postcode,' ',''))

            commit
            create unique hg index idx1 on temp_onnet_patch (account_number)
            create        index joinsy  on temp_onnet_patch (postcode)

                -- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes
            SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
              INTO temp_bpe
              FROM BROADBAND_POSTCODE_EXCHANGE
          GROUP BY postcode

            update temp_bpe
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on temp_bpe (postcode)

                -- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
            SELECT postcode as postcode, MAX(exchange_id) as exchID
              INTO temp_p2e
              FROM BB_POSTCODE_TO_EXCHANGE
          GROUP BY postcode

            update temp_p2e
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on temp_p2e (postcode)

                -- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
            SELECT COALESCE(temp_p2e.postcode, temp_bpe.postcode) AS postcode
                  ,COALESCE(temp_p2e.exchID, temp_bpe.exchID) as exchange_id
                  ,'OFFNET' as exchange
              INTO temp_onnet_lookup
              FROM temp_bpe FULL JOIN temp_p2e ON temp_bpe.postcode = temp_p2e.postcode

            commit
            create unique index fake_pk on temp_onnet_lookup (postcode)

                -- 4) Update with latest Easynet exchange information
            UPDATE temp_onnet_lookup
               SET exchange = 'ONNET'
              FROM temp_onnet_lookup AS base
                   INNER JOIN easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
             WHERE easy.exchange_status = 'ONNET'

                -- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
                --   spaces removed so your table will either need to have a similar filed or use a REPLACE
                --   function in the join
            UPDATE temp_onnet_patch
               SET onnet = CASE WHEN tgt.exchange = 'ONNET'
                                THEN 1
                                ELSE 0
                           END
              FROM temp_onnet_patch AS bas
                   INNER JOIN temp_onnet_lookup AS tgt on bas.postcode = tgt.postcode

            commit

            update temp_PanBal_weekly_sample as bas
               set bas.onnet = onn.onnet
              from temp_onnet_patch as onn
             where bas.account_number = onn.account_number

            update temp_PanBal_weekly_sample as bas
               set bb = 1
              from cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'Broadband DSL Line'
               and status_code in ('AC', 'AB', 'PC', 'CF', 'PT')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            update temp_PanBal_weekly_sample as bas
               set st = 1
              from cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'SKY TALK SELECT'
               and status_code in ('A', 'FBP', 'PC', 'RI')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            create table temp_noconsent(account_number varchar(30))

            insert into temp_noconsent
            select account_number
              from cust_single_account_view as sav
             where cust_viewing_data_capture_allowed <> 'Y'
          group by account_number

            create table temp_adsmbl(account_number varchar(30))

            insert into temp_adsmbl
            select account_number
              from cust_set_top_box
             where active_box_flag = 'Y'
               and (x_pvr_type in ('PVR5', 'PVR6') and x_manufacturer not in ('Samsung'))
           group by account_number

            commit
            create unique hg index idx1 on temp_adsmbl(account_number)
            create unique hg index idx1 on temp_noconsent(account_number)

            update temp_PanBal_weekly_sample as bas
               set adsmbl = case when con.account_number is null then 'Adsmartable consent'
                                                                 else 'Adsmartable non-consent'
                            end
              from temp_adsmbl as ads
                   left join temp_noconsent as con on con.account_number = ads.account_number
             where bas.account_number = ads.account_number

--            update temp_PanBal_weekly_sample as sam
--               set bb_capable = l20_darwin
--              from waterfall_base as wat
--             where sam.account_number = wat.account_number
--               and l07_prod_latest_dtv        = 1
--               and l08_country                = 1
--               and l10_surname                = 1
--               and l11_standard_accounts      = 1
--               and l24_last_callback_dt       = 1

                -- set unused variables to a default valuefor ROI
            update temp_PanBal_weekly_sample as bas
               set fss = 'Not Defined'
                  ,hhcomp = 'Not Defined'
                  ,mosaic = 'Not Defined'
                  ,onnet = 0
              from cust_single_account_view as sav
             where bas.account_number = sav.account_number
               and pty_country_code = 'IRL'

                -- count boxes for every account
            select account_number
                  ,card_subscriber_id
              into temp_ccs
              from CUST_CARD_SUBSCRIBER_LINK as ccs
             where effective_to_dt = '9999-09-09'
          group by account_number
                  ,card_subscriber_id

            commit
            create hg index hgacc on temp_ccs(account_number)

            select ccs.account_number
                  ,sum(case when cust_active_dtv = 1 then 1 else 0 end) as boxes
              into temp_sky_box_count
              from temp_ccs as ccs
                   left join cust_single_account_view as sav on ccs.account_number = sav.account_number
          group by ccs.account_number

            insert into temp_PanBal_segments_lookup(
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
              from temp_PanBal_weekly_sample as sam
                   inner join temp_sky_box_count as sbc on sam.account_number = sbc.account_number
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

            insert into temp_PanBal_segments_lookup_unnormalised(
                   v1
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
            select max(case when aggregation_variable = 1 then value else null end)
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
              from panbal_segments_lookup_normalised
          group by segment_id

                -- update the lookup table with segment id from unnormalised table
                -- db space issue, so have to do this query a bit at a time
               set @counter = 0
             while @counter < (select max(segment_id) from temp_PanBal_segments_lookup_unnormalised) begin
                      update temp_PanBal_segments_lookup as lkp
                         set segment_id = unn.segment_id
                        from temp_PanBal_segments_lookup_unnormalised as unn
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
              from temp_PanBal_weekly_sample               as sam
                   inner join temp_PanBal_segments_lookup as lkp on sam.adsmbl     = lkp.adsmbl
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
            insert into temp_matches
            select bas.segment_id
              from temp_PanBal_segments_lookup as bas
                   inner join temp_PanBal_segments_lookup_unnormalised as unn on v1 = adsmbl
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
            insert into panbal_segments_lookup_normalised(
                   segment_id
                  ,aggregation_variable
                  ,value
                   )
            select bas.segment_id
                  ,1
                  ,cast(adsmbl as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,2
                  ,region
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,3
                  ,hhcomp
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,4
                  ,tenure
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,5
                  ,package
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,6
                  ,cast(mr as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,7
                  ,cast(hd as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,8
                  ,cast(pvr as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,9
                  ,valseg
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,10
                  ,mosaic
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,11
                  ,fss
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,12
                  ,cast(onnet as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,13
                  ,cast(skygo as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,14
                  ,cast(st as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,15
                  ,cast(bb as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,16
                  ,cast(bb_capable as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null

            update panbal_segments_lookup_normalised
               set curr = 0

            update panbal_segments_lookup_normalised as bas
               set curr = 1
              from panbal_segment_snapshots as snp
             where bas.segment_id = snp.segment_id

              drop table temp_PanBal_weekly_sample
              drop table temp_PanBal_segments_lookup
              drop table temp_matches
              drop table temp_weekly_sample
              drop table temp_cv_pp
              drop table temp_cv_keys
              drop table temp_roi_region
              drop table temp_scaling_box_level_viewing
              drop table temp_onnet_patch
              drop table temp_bpe
              drop table temp_p2e
              drop table temp_onnet_lookup
              drop table temp_noconsent
              drop table temp_adsmbl
              drop table temp_ccs
              drop table temp_sky_box_count
              drop table temp_PanBal_segments_lookup_unnormalised
     end; --V306_M03_PanBal_Segments
 commit;

 grant execute on V306_M03_PanBal_Segments to vespa_group_low_security;
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




**Module:                              M05_VirtPan

This module, previously independent, is now part of the Panel Balancing procedure.

Panel definition:
               CHANNEL 4
               ---------
               All those subscribers/accounts that:
               - were on the panels last week
               - have data return metric > 0
               - have ever downloaded any On Demand content

               Additional conditions for the first round (08/03/2013):
               - top up with manual selection of those who have recently downloaded
                 On Demand content, less those that had the prefix issues in the
                 recent enablement files which we will be disabling soon (SBE to advise)
               Expected panel size is between 300k and 350k Subscriber Ids

               CHANNEL 5
               ---------
               All Accounts on any vespa panel with at least one box where:
               - reporting quality known
               - reporting quality > 0


*/




  create or replace procedure V306_M05_VirtPan as begin

           MESSAGE cast(now() as timestamp)||' | VirtPan M05.0 - Create and initialise VirtPan_Channel45_01_Universe_Selection' TO CLIENT

                if object_id('VirtPan_Channel45_01_Universe_Selection') is not null begin
                    truncate table VirtPan_Channel45_01_Universe_Selection
--                        drop table VirtPan_Channel45_01_Universe_Selection
               end
              else begin
                      create table VirtPan_Channel45_01_Universe_Selection (
                             Row_Id                     bigint        identity null
                            ,Run_Date                   date          default today()
                            ,Account_Number             varchar(50)   default null
                            ,Subscriber_id              bigint        default null
                            ,Source                     varchar(30)   default null
                            ,Panel_Id                   tinyint       default 0 null
                            ,On_Demand_DL_Ever          bit           default 0
                            ,Box_Reporting_Quality      decimal(10,3) default -1 null
                            ,Low_Data_Quality_Flag      bit           default 0
                            ,Channel4_Panel_Flag        bit           default 0
                            ,Channel5_Panel_Flag        bit           default 0
                            ,Random_Num                 decimal(10,6) default 0 null
                            ,Created_By                 varchar(30)   default user
                            ,Created_On                 timestamp     default timestamp
                             )
                      create      date index idx1 on VirtPan_Channel45_01_Universe_Selection(Run_Date)
                      create        hg index idx2 on VirtPan_Channel45_01_Universe_Selection(Account_Number)
                      create unique hg index idx3 on VirtPan_Channel45_01_Universe_Selection(Subscriber_id)
                      create           index idx4 on VirtPan_Channel45_01_Universe_Selection(Random_Num)
               end

                   -- #################################################################################
                   -- ##### Create universe - get all boxes from all panels                       #####
                   -- #################################################################################

            MESSAGE cast(now() as timestamp)||' | VirtPan M05.1 - Create universe - insert accounts and subscribers' TO CLIENT

            declare @var_multiplier bigint
                set @var_multiplier = datepart(millisecond,now()) + 1

             insert into VirtPan_Channel45_01_Universe_Selection(
                    account_number
                   ,subscriber_id
                   ,panel_id
                   ,source
                   ,box_reporting_quality
                   ,random_num
                    )
             select vss.account_number
                   ,cast(card_subscriber_id as int) as subscriber_id
                   ,vss.panel_no
                   ,'SBV'
                   ,-1
                   ,rand(number(*) * @var_multiplier)
               from vespa_subscriber_status as vss
              where result = 'Enabled'
                and vss.panel_no in (5, 6, 7, 11, 12)
           group by vss.account_number
                   ,subscriber_id
                   ,vss.panel_no

            MESSAGE cast(now() as timestamp)||' | VirtPan M05.1 - Create universe - Add Reporting Quality from SBV' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection as bas
               set Box_Reporting_Quality = Reporting_Quality
              from vespa_analysts.vespa_single_box_view as sbv
             where bas.subscriber_id = sbv.subscriber_id
               and status_vespa = 'Enabled'

                -- #################################################################################
                -- ##### Append account/subscriber metrics                                     #####
                -- #################################################################################
                -- Append On Demand DL information

           MESSAGE cast(now() as timestamp)||' | VirtPan M05.2 - Append account-level On Demand download activity flag' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection base
               set base.On_Demand_DL_Ever = 1
              from (select Account_Number
                          ,min(last_modified_dt) as first_dl_date
                          ,max(last_modified_dt) as last_dl_date
                      from cust_anytime_plus_downloads
                  group by Account_Number) det
             where base.Account_Number = det.Account_Number





                -- #################################################################################
                -- ##### Data quality checks                                                   #####
                -- #################################################################################

                -- ##### Multiple accounts for a single box #####

           MESSAGE cast(now() as timestamp)||' | VirtPan M05.3 - Identify and flag subscribers associated with multiple accounts' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection base
               set base.Low_Data_Quality_Flag = 1
              from (select Subscriber_Id
                          ,count(distinct Account_Number) as Acc_Nums
                      from VirtPan_Channel45_01_Universe_Selection
                  group by Subscriber_Id) det
             where base.Subscriber_Id = det.Subscriber_Id
               and det.Acc_Nums > 1



                -- Propagate to all associated subscriber Ids within the the account
           MESSAGE cast(now() as timestamp)||' | VirtPan M05.3 - Identify and flag accounts containing subscribers associated with multiple accounts' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection base
               set base.Low_Data_Quality_Flag = 1
              from (select Account_Number
                          ,max(Low_Data_Quality_Flag) as Low_Quality
                      from VirtPan_Channel45_01_Universe_Selection
                  group by Account_Number
                    having Low_Quality > 0) det
             where base.Account_Number = det.Account_Number



                -- ##### Boxes within an account being on different panels #####
           MESSAGE cast(now() as timestamp)||' | VirtPan M05.3 - Identify and flag accounts containing subscribers associated with multiple accounts' TO CLIENT

            update VirtPan_Channel45_01_Universe_Selection base
               set base.Low_Data_Quality_Flag = 1
              from (select Account_Number
                          ,count(distinct Panel_Id) as Panel_Nums
                      from VirtPan_Channel45_01_Universe_Selection
                  group by Account_Number
                    having Panel_Nums > 1) det
            where base.Account_Number = det.Account_Number

              -- #################################################################################
              -- ##### Virtual panel selection                                               #####
              -- #################################################################################

              -- Reset
         MESSAGE cast(now() as timestamp)||' | VirtPan M05.4 - Reset Channel 4/5 panel flags' TO CLIENT

          update VirtPan_Channel45_01_Universe_Selection
             set Channel4_Panel_Flag = 0,
                 Channel5_Panel_Flag = 0



         MESSAGE cast(now() as timestamp)||' | VirtPan M05.4 - Reset Channel 4/5 panel flags' TO CLIENT

          update VirtPan_Channel45_01_Universe_Selection base
             set Channel4_Panel_Flag = 1                                          -- Joined at Subscriber Id level, so account may include a mixture of boxes "on panel" and "not on panel"
            from (select Subscriber_Id
                    from VirtPan_Channel45_01_Universe_Selection
                   where Run_Date = today()
                     and Low_Data_Quality_Flag = 0
                     and On_Demand_DL_Ever = 1
                     and (   (Box_Reporting_Quality > 0 and Source = 'SBV')          -- SBV boxes
                          or (Box_Reporting_Quality = -1 and Source <> 'SBV')        -- Manual top-up boxes
                         )
          group by Subscriber_Id) det
             where base.Subscriber_Id = det.Subscriber_Id

            update VirtPan_Channel45_01_Universe_Selection base
               set Channel5_Panel_Flag = 1                                          -- Joined at Subscriber Id level, so account may include a mixture of boxes "on panel" and "not on panel"
              from (select Subscriber_Id
                      from VirtPan_Channel45_01_Universe_Selection
                     where Run_Date = today()
                       and Low_Data_Quality_Flag = 0
                       and Box_Reporting_Quality > 0
                       and Source = 'SBV'
                  group by Subscriber_Id) det
             where base.Subscriber_Id = det.Subscriber_Id


               -- #################################################################################
               -- ##### Populate final table                                                  #####
               -- #################################################################################
               -- Add non-existing accounts first

           insert into vespa_broadcast_reporting_vp_map (Account_Number, Vespa_Panel)
           select base.Account_Number
                 ,base.Panel_Id
             from VirtPan_Channel45_01_Universe_Selection base
                  left join vespa_broadcast_reporting_vp_map det on base.Account_Number = det.Account_Number
            where det.Account_Number is null
              and (   Channel4_Panel_Flag = 1
                   or Channel5_Panel_Flag = 1
                  )
          group by base.Account_Number, base.Panel_Id       -- Our table is Subscriber ID based, the destination is Account Number one

                -- Reset selection
            update vespa_broadcast_reporting_vp_map base
               set base.vp1     = 0
                  ,base.vp2     = 0

                -- Add flags
            update vespa_broadcast_reporting_vp_map base
               set base.vp1     = det.Channel4_Panel_Flag,
                   base.vp2     = det.Channel5_Panel_Flag
              from (select Account_Number
                          ,max(Channel4_Panel_Flag) as Channel4_Panel_Flag
                          ,max(Channel5_Panel_Flag) as Channel5_Panel_Flag
                      from VirtPan_Channel45_01_Universe_Selection
                  group by Account_Number) det
             where base.Account_Number = det.Account_Number

   commit
      end; --V306_M05_VirtPan
 commit;

 grant execute on V306_M05_VirtPan to vespa_group_low_security;
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




**Module:                              M06_Main

This module is the central loop of the balancing process. After initialising all of the tables which are used within this module, it calculates the imbalance for each variable in the traffic_lights table.
If any of the variables has an imbalance higher than the lmit then another loop of the process is induced.
A loop of the code adds a number of accounts to the panel (set by @precision) to minimise the maximum imbalance.

*/


  create or replace procedure V306_M06_Main
         @max_imbalance int
        ,@min_b         int --boxes to return per day
        ,@precision     int --the number of accounts to add at a time
      as begin

                   --------------------
                   -- Section A - Setup
                   --------------------
            create table temp_PanBal_all_aggregated_results(
                   aggregation_variable                              int null
                  ,variable_value                                    varchar(60) default null
                  ,Sky_Base_Households                               int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,Good_Household_Index                              double default 0 null
                  ,GHIplus1                                          double default 0 null
                  ,GHIminus1                                         double default 0 null
                  ,incr_diff                                         double default 0 null
                  ,decr_diff                                         double default 0 null
                   )

            commit
            create lf index lfagg on temp_PanBal_all_aggregated_results(aggregation_variable)
            create lf index lfvar on temp_PanBal_all_aggregated_results(variable_value)

            create table temp_panel_households(
                   aggregation_variable                              int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,sky_base_households                               int null
                   )

            create table temp_primary_panel_pool(
                   account_number                                    varchar(30) null
                  ,segment_id                                        int null
                  ,rq                                                double null
                   )

            create table temp_PanBal_panel(
                   account_number                                    varchar(30) null
                  ,segment_id                                        int null
                   )

            create table temp_PanBal_Scaling_Segment_Profiling (
                   segment_id                                        int null
                  ,Sky_Base_Households                               int null
                  ,Panel_households                                  decimal(10,2) default 0 null
                  ,primary key (segment_id)
                   )

            create table temp_PanBal_traffic_lights(
                   variable_name                                     int null
                  ,imbalance_rating                                  decimal(6,2) null
                   )

            create table temp_descrs(
                   segment_id                                        int null
                  ,descrs                                            double null
                   )

            create table temp_panel_segmentation(
                   segment_id                                        int null
                  ,Panel_Households                                  decimal(10,2) null
                   )

            create table temp_new_adds(
                   account_number                                    varchar(50) null
                  ,segment_id                                        int null
                  ,thi                                               double null
                   )

            create table temp_segment_THIs(
                   segment_id                                        int null
                  ,thi                                               double    default 0 null
                   )

            commit
            create unique hg index uhseg on temp_segment_THIs(segment_id)

            create table temp_panbal_segments_lookup_normalised(
                   segment_id                                        int null
                  ,aggregation_variable                              tinyint null
                  ,value                                             varchar(30) null
                   )

            commit
            create hg index hgseg on temp_panbal_segments_lookup_normalised(segment_id)
            create hg index lfagg on temp_panbal_segments_lookup_normalised(aggregation_variable)
            create hg index lfval on temp_panbal_segments_lookup_normalised(value)

            create table temp_lookup(
                   segment_id                                       int null
                  ,diff                                             double null
                   )

            commit
            create unique hg index uhseg on temp_lookup(segment_id)

                -- declarations
           declare @total_sky_base                                  int
           declare @panel_reporters                                 int
           declare @cow                                             int default 0 -- the count of boxes on the daily panels
           declare @accounts_remaining                              int default 1000000
           declare @temp                                            int
           declare @imbalance                                       double default 100
           declare @prev_imbalance                                  double default 200
           declare @tot_imb                                         double default 100000
           declare @prev_tot_imb                                    int default 1000000
           declare @max_rq                                          double default 0 --reporting quality should aim be at least 0.8
           declare @counter                                         int
           declare @vars                                            tinyint
           declare @from_waterfall                                  tinyint default 0
           declare @records_out                                     int default 1 -- are there any accounts that can be added?
           declare @reached                                         double default 100
           declare @started                                         double default 200

                -- set starting variables
               set rowcount 0

                   --------------------------------------
                   -- Section B - Fill the accounts lists
                   --------------------------------------

                -- primary panel list
            insert into temp_primary_panel_pool(account_number
                                    ,rq
                                    ,segment_id
                                    )
            select account_number
                  ,rq
                  ,segment_id
              from panbal_sav
             where panel in (11, 12)

                -- secondary panel accounts
          truncate table secondary_panel_pool

            insert into secondary_panel_pool
            select account_number
                  ,segment_id
                  ,rq
                  ,0
              from panbal_sav as sav
             where panel in (5, 6)
               and sav.rq >= @max_rq

                -- waterfall accounts
          truncate table waterfall_pool

            insert into waterfall_pool(
                   account_number
                  ,segment_id
                  ,rq
                  ,thi
                   )
            select bas.account_number
                  ,segment_id
                  ,1
                  ,0
              from panbal_sav           as bas
             where panel is null
          group by bas.account_number
                  ,segment_id

                -- check for any accounts missing from segments
            delete from waterfall_pool where account_number not in (
                      select account_number
                        from panbal_segment_snapshots
                             )

                -- only count UK accounts
            insert into temp_PanBal_Scaling_Segment_Profiling (
                   segment_id
                  ,Sky_Base_Households
                   )
            select segment_id
                  ,sum(case when pty_country_code = 'GBR' then 1 else 0 end) as Sky_Base_Households
              from PanBal_segment_snapshots as seg
                   inner join cust_single_account_view as sav on seg.account_number = sav.account_number
          group by segment_id

            select @total_sky_base = sum(Sky_Base_Households) from temp_PanBal_Scaling_Segment_Profiling

               -- add any accounts first from the table panbal_additions
           insert into temp_PanBal_panel(account_number
                                    ,segment_id
                                    )
           select pan.account_number
                 ,segment_id
             from secondary_panel_pool as pan
                  inner join panbal_additions as ads on pan.account_number = ads.account_number

           insert into temp_PanBal_panel(account_number
                                    ,segment_id
                                    )
           select pan.account_number
                 ,segment_id
             from waterfall_pool as pan
                  inner join panbal_additions as ads on pan.account_number = ads.account_number

           delete from secondary_panel_pool
            where account_number in (select account_number from panbal_additions)

           delete from waterfall_pool
            where account_number in (select account_number from panbal_additions)

                -- start the panel off with just the acceptable daily panel accounts
            insert into temp_PanBal_panel(account_number
                                    ,segment_id
                   )
            select account_number
                  ,segment_id
              from temp_primary_panel_pool

                -- we only need to work out THIs once for each segment that we have any accounts in list3 or list4
            insert into temp_segment_THIs(segment_id)
            select distinct(segment_id)
              from (select segment_id as segment_id
                      from secondary_panel_pool
                     union
                    select segment_id
                      from waterfall_pool) as sub

            update temp_PanBal_Scaling_Segment_Profiling as bas
               set Panel_Households             = 0

            insert into temp_panbal_segments_lookup_normalised(
                   segment_id
                  ,aggregation_variable
                  ,value
                   )
            select bas.segment_id
                  ,aggregation_variable
                  ,value
              from panbal_segments_lookup_normalised as bas

            select @vars = count(distinct aggregation_variable) from temp_panbal_segments_lookup_normalised

          truncate table panbal_results

                   ----------------------------
                   -- Section C - The Main Loop
                   ----------------------------
             while ((@imbalance >= @max_imbalance or @cow < @min_b) and @records_out > 0) begin

                    truncate table temp_panel_segmentation

                          -- reqd for traffic lights
                      insert into temp_panel_segmentation(segment_id
                                                     ,Panel_Households)
                      select bas.segment_id
                            ,sum(case when sav.rq is null then 0
                                      when sav.rq > 1 then 1
                                      else sav.rq
                                 end)
                        from temp_PanBal_panel as bas
                             inner join panbal_sav as sav on bas.account_number = sav.account_number
                    group by bas.segment_id

                      update temp_PanBal_Scaling_Segment_Profiling as bas
                         set Panel_Households     = 0

                      update temp_PanBal_Scaling_Segment_Profiling as bas
                         set Panel_Households     = seg.Panel_Households
                        from temp_panel_segmentation as seg
                       where bas.segment_id       = seg.segment_id

                    truncate table temp_PanBal_all_aggregated_results

                          -- create the traffic light report
                      insert into temp_PanBal_all_aggregated_results(
                             aggregation_variable
                            ,variable_value
                            ,Sky_Base_Households
                            ,Panel_Households
                             )
                      select ssl.aggregation_variable
                            ,value
                            ,sum(sky_base_households)
                            ,sum(Panel_households)
                        from temp_PanBal_Scaling_Segment_Profiling as ssp
                             inner join temp_panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
                    group by aggregation_variable
                            ,value

                          -- if any values are Unknown, then we don't want to balance these
                      delete from temp_PanBal_all_aggregated_results
                       where variable_value in ('Non-scalable', 'NS', 'U', 'Not Defined', 'E) Unknown', 'Unknown', 'No Panel')
                          or sky_base_households < 1000
                          or sky_base_households is null

                    truncate table temp_panel_households

                      insert into temp_panel_households(
                             aggregation_variable
                            ,Panel_Households
                            ,sky_base_households
                             )
                      select aggregation_variable
                            ,sum(panel_households)
                            ,sum(sky_base_households)
                        from temp_PanBal_all_aggregated_results
                    group by aggregation_variable

                      update temp_PanBal_all_aggregated_results as bas
                         set Good_Household_Index = 100.0 *  bas.Panel_households               * hsh.Sky_Base_Households / bas.Sky_Base_Households /  hsh.Panel_Households      --index value for each variable value
                            ,GHIplus1             = 100.0 * (bas.Panel_households + @precision) * hsh.Sky_Base_Households / bas.Sky_Base_Households / (hsh.Panel_Households + @precision) --what the Good Household Index (GHI) would be if we added 1 account
                            ,GHIminus1            = 100.0 *  bas.Panel_households               * hsh.Sky_Base_Households / bas.Sky_Base_Households / (hsh.Panel_Households + @precision) --what the Good Household Index (GHI) would be if we added 1 account to a different variable value
                        from temp_panel_households as hsh
                       where bas.aggregation_variable = hsh.aggregation_variable

                          -- the difference (squared) between GHI and GHI+1
                      update temp_PanBal_all_aggregated_results as bas
                         set incr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIplus1-100) * (GHIplus1-100))

                          -- the difference (squared) between GHI and GHI-1
                      update temp_PanBal_all_aggregated_results as bas
                         set decr_diff = ((Good_Household_Index-100) * (Good_Household_Index-100) - (GHIminus1-100) * (GHIminus1-100))

                    truncate table temp_PanBal_traffic_lights

                      insert into temp_PanBal_traffic_lights
                      select aggregation_variable
                            ,sqrt(avg((Good_Household_Index - 100) * (Good_Household_Index - 100))) as imbalance_rating
                        from temp_PanBal_all_aggregated_results
                    group by aggregation_variable

                      select @imbalance = max(imbalance_rating) from temp_PanBal_traffic_lights

                          -- if we are short of the number of boxes required then we will need to continue
                      select @cow = sum(rq * boxes)
                        from temp_PanBal_panel as bas
                             inner join panbal_sav as sav on bas.account_number = sav.account_number

                          -- if the panel is not balanced, then continue
                          if (@imbalance >= @max_imbalance or @cow < @min_b)
                       begin

                                    -- calculate the THIs
                                update temp_segment_THIs as bas
                                   set thi = 0

                              truncate table temp_lookup

                                insert into temp_lookup(
                                       segment_id
                                      ,diff
                                       )
                                select segment_id
                                      ,sum(case when lkp.value = agg.variable_value then incr_diff else decr_diff end) as diff
                                  from temp_PanBal_all_aggregated_results as agg
                                       inner join temp_panbal_segments_lookup_normalised as lkp on lkp.aggregation_variable = agg.aggregation_variable
                              group by segment_id

                                update temp_segment_THIs as bas
                                   set thi = thi + diff
                                  from temp_lookup as lkp
                                 where bas.segment_id = lkp.segment_id

                                    -- update THI for alternate day panels
                                update secondary_panel_pool as bas
                                   set bas.thi = thi.thi * rq
                                  from temp_segment_THIs as thi
                                 where bas.segment_id = thi.segment_id

                              truncate table temp_new_adds

                                insert into temp_new_adds
                                select li3.account_number
                                      ,li3.segment_id
                                      ,thi
                                  from secondary_panel_pool           as li3
                                       left join temp_PanBal_panel as bas on li3.account_number = bas.account_number
                                 where bas.account_number is null
--                                 and thi > 0

                                select @accounts_remaining = count(1)
                                  from temp_new_adds

                                    if (@from_waterfall = 2)
                                 begin
                                              if (@started - ((@reached - @max_imbalance) / 2) < @imbalance)
                                           begin
                                                       set @from_waterfall = 1
                                             end
                                            else
                                           begin
                                                       set @from_waterfall = 0
                                             end
                                   end
                                    if (@from_waterfall = 1)
                                 begin
                                              -- update THI for waterfall list
                                          update waterfall_pool as bas
                                             set bas.thi = thi.thi * rq
                                            from temp_segment_THIs as thi
                                           where bas.segment_id = thi.segment_id

                                              -- are there any that can be added?
                                          select @records_out = count(1)
                                            from waterfall_pool
                                           where thi > 0

                                             set rowcount @precision

                                                        -- add some from the waterfall
                                                    insert into temp_PanBal_panel(account_number
                                                                             ,segment_id)
                                                    select li4.account_number
                                                          ,li4.segment_id
                                                      from waterfall_pool as li4
                                                     where thi > 0
                                                  order by thi desc

                                             set rowcount 0

                                              -- remove the ones we have just added from the waterfall list
                                          delete from waterfall_pool where account_number in (select account_number from temp_PanBal_panel)

                                             set @prev_imbalance = 200
                                             set @prev_tot_imb = 100000
                                             set @from_waterfall = 2
                                             set @imbalance = 100
                                   end
                                  else
                                 begin
--                                              if (@accounts_remaining >= @precision and @tot_imb + 20 <= @prev_tot_imb and ((@imbalance - (@prev_imbalance - @imbalance) * (@accounts_remaining / @precision)) < @max_imbalance)) --if there are a reasonable amount that can be added, and the target can be reached
                                              if (@accounts_remaining >= @precision and (((@imbalance - (@prev_imbalance - @imbalance) * (@accounts_remaining / @precision)) < @max_imbalance))  or @cow < @min_b) --if there are a reasonable amount that can be added, and the target can be reached
                                           begin
                                                       set rowcount @precision -- only show this many lines in all queries

                                                              insert into temp_PanBal_panel(
                                                                     account_number
                                                                    ,segment_id
                                                              )
                                                              select account_number
                                                                    ,segment_id
                                                                from temp_new_adds
                                                            order by thi desc

                                                       set rowcount 0 --back to normal

                                                       set @prev_imbalance = @imbalance
                                                       set @prev_tot_imb = @tot_imb

                                             end
                                            else
                                           begin

                                                       set @from_waterfall = 1

                                                        -- remove all marketing panel accounts from the panel
                                                    delete from temp_PanBal_panel where account_number in (select account_number from secondary_panel_pool)
                                                       set @reached = @imbalance
                                                       set @imbalance = 200
                                             end --if
                                   end --if
                         end

                      select @tot_imb = sum((100-good_household_index) * (100-good_household_index)) from temp_PanBal_all_aggregated_results


                      insert into panbal_results(imbalance
                                                ,tot_imb
                                                ,records
                                                ,from_waterfall
                                                ,tim)
                      select @imbalance
                            ,@tot_imb
                            ,count(1)
                            ,@from_waterfall
                            ,now()
                        from temp_PanBal_panel

                      commit

                          if @started = 200 begin
                                   set @started = @imbalance
                         end

               end --while

          truncate table panbal_amends

                if (@records_out = 0) begin
                      insert into panbal_amends(
                             account_number
                            ,movement)
                      select 0
                            ,'Not enough accounts available to find a balance'
               end

               -- accounts to add to panel 12 from alt. panels
            insert into panbal_amends(
                   account_number
                  ,movement)
            select bas.account_number
                  ,'Account to add to primary panels from secondary panels'
              from temp_PanBal_panel as bas
                   inner join secondary_panel_pool as li3 on bas.account_number = li3.account_number
            commit

            insert into panbal_amends(
                   account_number
                  ,movement)
            select bas.account_number
                  ,'Account to add to primary panels from secondary panels (from adds table)'
              from temp_PanBal_panel as bas
                   inner join panbal_additions as ads on bas.account_number = ads.account_number

          truncate table panbal_panel
            insert into panbal_panel(
                   account_number
                  ,segment_id
                   )
            select account_number
                  ,segment_id
              from temp_PanBal_panel
            commit
            
              drop table temp_PanBal_all_aggregated_results
              drop table temp_panel_households
              drop table temp_primary_panel_pool
              drop table temp_PanBal_panel
              drop table temp_PanBal_Scaling_Segment_Profiling
              drop table temp_PanBal_traffic_lights
              drop table temp_descrs
              drop table temp_panel_segmentation
              drop table temp_new_adds
              drop table temp_segment_THIs
              drop table temp_panbal_segments_lookup_normalised
              drop table temp_lookup

     end; --V306_M06_Main
 commit;

 grant execute on V306_M06_Main to vespa_group_low_security;
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




**Module:                              M07_VolCheck

This module adds additional accounts to the panels if any are required to meet the requirements for TA propensity coverage, virtual panel coverage, or business requirements for the primary or combined panels.
If the maximum size of the primary or combined panels has been exceeded, then the required number of accounts rae removed from the panels.

*/


  create or replace procedure V306_M07_VolCheck
                    @max_b      int --boxes to return per day  
                   ,@min_vp1    int
                   ,@min_ta     float --turnaround propensity coverage
                   ,@min_ta_ret float --turnaround propendity from returning boxes
      as begin

            create table temp_lookup(
                   account_number                                     varchar(30) null
                  ,boxes                                              int null
                   )

            create table temp_list1_rq(
                   account_number                                     varchar(30) null
                  ,rq                                                 double null
                  ,boxes                                              int null
                   )

           declare @cow         int
           declare @precision   int
           declare @virtuals    int
           declare @ta          double
           declare @records_out int default 1
                                                                                        
               set @precision = 1000
               
                -- check whether we are over the limit for boxes dialling back per day
            select @cow = sum(case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end) * boxes
              from PanBal_panel as bas
                   inner join panbal_sav as sav on bas.account_number = sav.account_number

                if (@cow > @max_b) begin

                       while (@cow > @max_b) begin
                              truncate table temp_lookup

                                   set rowcount @precision

                                          insert into temp_lookup
                                          select pan.account_number
                                                ,boxes * case when sav.rq is null then case when cbck_rate is null then 1 
                                                                                                                   else cbck_rate 
                                                                                        end 
                                                              when sav.rq > 1 then 1 
                                                                              else sav.rq 
                                                          end
                                            from panbal_panel as pan
                                                 inner join panbal_sav as sav on pan.account_number = sav.account_number
                                        order by sav.rq
                                                ,pan.account_number

                                   set rowcount 0

                                delete from panbal_panel where account_number in (select account_number from temp_lookup)

                                select @cow = sum(case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end) * boxes
                                  from PanBal_panel as bas
                                       inner join panbal_sav as sav on bas.account_number = sav.account_number

                                insert into panbal_amends(account_number, movement)
                                select account_number
                                      ,'Account to remove from primary panels'
                                  from temp_lookup

                         end --while
               end --if
                                                                                                                                                                                                                                                
                -- now need to remove the ones we've added fom the secondary panel (we have already deleted the ones we've added from the waterfall pool)
            delete from secondary_panel_pool where account_number in (select account_number from PanBal_panel)

                -- accounts to add to secondary panels, to make 50% in each segment (if poss)
            select sum(case when pan.account_number is null then 0 else 1 end)                                    as vespa
                  ,sum(case when alt.account_number is not null and pan.account_number is null then 1 else 0 end) as alt
                  ,bss.segment_id
              into temp_panels
              from PanBal_segment_snapshots                     as bss
                   left join PanBal_panel                       as pan on bss.account_number = pan.account_number
                   left join secondary_panel_pool               as alt on bss.account_number = alt.account_number
                   inner join panbal_segments_lookup_normalised as lkp on bss.segment_id = lkp.segment_id
             where aggregation_variable = 2
               and value not like 'ROI%'
          group by bss.segment_id

            select segment_id
                  ,vespa - (alt*2) as reqd
              into temp_reqd
              from temp_panels
             where reqd > 0

            select wat.account_number
                  ,wat.segment_id
                  ,rank() over (partition by wat.segment_id order by cbck_rate desc, vp1 desc) as rnk
                  ,vp1
                  ,boxes
              into temp_available
              from waterfall_pool                          as wat
                   inner join panbal_sav                   as sav on wat.account_number = sav.account_number
                   left join PanBal_panel                  as bas on wat.account_number = bas.account_number
             where bas.account_number is null

            insert into panbal_amends(account_number, movement)
            select account_number
                  ,'Account to add to secondary panels as segment backup'
              from temp_available        as ava
                   inner join  temp_reqd as req on ava.segment_id = req.segment_id
             where rnk <= reqd

                                                                                                delete from waterfall_pool
                                                                                                where account_number in (select account_number 
                                                                                                                                   from panbal_amends
                                                                                                                                                                                                                                                                                                        )
                                                                                                                                                                                                                
            select @virtuals = sum(boxes) --count boxes on the virtual panel on the new panel
              from PanBal_panel                            as bas
                   inner join panbal_sav                   as sav on bas.account_number = sav.account_number
             where vp1 = 1

            select @virtuals = @virtuals + sum(boxes) --add on the remaining accounts left in the secondary panel pool
              from secondary_panel_pool                                              as bas
                   inner join panbal_sav                   as sav on bas.account_number = sav.account_number
                   left join panbal_amends                 as ame on bas.account_number = ame.account_number
             where vp1 = 1
               and ame.account_number is null

                if (@virtuals < @min_vp1) begin --do we need any more on the channel 4 panel?

                    truncate table temp_list1_rq
                      insert into temp_list1_rq
                      select li4.account_number
                            ,case when sav.rq is null then case when cbck_rate is null then 1 else cbck_rate end when sav.rq > 1 then 1 else sav.rq end
                            ,boxes
                        from waterfall_pool                              as li4
                             inner join panbal_sav                       as sav on li4.account_number = sav.account_number
                       where vp1 = 1

                       while (@virtuals < @min_vp1 and @records_out > 0) begin
                              truncate table temp_lookup
                                                                                                                                                                                                                                                
                                select @records_out = count(1)
                                  from list1_rq
                                                                                                                                                                                                                                                
                                   set rowcount @precision

                                                                                                                                                                                                                                                                                                                                                insert into temp_lookup
                                                                                                                                                                                                                                                                                                                                                select account_number
                                                                                                                                                                                                                                                                                                                                                                                                ,boxes
                                                                                                                                                                                                                                                                                                                                                                from temp_list1_rq
                                                                                                                                                                                                                                                                                                                                order by rq
                                                                                                                                                                                                                                                                                                                                                                                                ,account_number

                                                                                                                                                                                                                                                                                        set rowcount 0
                                   set @virtuals = @virtuals + (select sum(boxes) from temp_lookup)

                                select @cow = count(1) from temp_list1_rq

                                    if (@cow = 0) begin

                                             set @cow = @min_vp1 - @virtuals

                                          insert into panbal_amends(account_number, movement)
                                          select null
                                                ,@cow || ' more boxes needed on the virtual panel'

                                             set @virtuals = @min_vp1
                                   end

                                delete from temp_list1_rq where account_number in (select account_number from temp_lookup)

                                insert into panbal_amends(account_number, movement)
                                select account_number
                                      ,'Account to add to secondary panels for virtual panel req.'
                                  from temp_lookup

                         end --while
                      commit

               end --if

                                                                                                                        if (@records_out = 0) begin
                                                                                                                                                                        insert into panbal_amends(account_number
                                                                                                                                                                                                                                                                                                                                                                                ,movement)
                                                                                                                                                                        select 0
                           ,'Not enough accounts available to fill the Virtual Panel 1 requirement'
                                                                                                                end
                                                                                                                                                                                                        
                -- check TA coverage - we need at least 25% from enabled accounts on all panels
            select @ta = sum(ta_propensity)
              from panbal_sav as sav
                   left join panbal_amends  as pan on sav.account_number = pan.account_number
             where panel is not null
                or pan.account_number is not null

            select @ta = @ta / sum(ta_propensity)  from vespa_analysts.SkyBase_TA_scores

             while (@ta < @min_ta and @records_out > 0) begin
                                                                                                        
                                                                                                                                                                                select @records_out = count(1)
                                                                                                                                                                                                from waterfall_pool
                                                                                                                                                                                                                                                
                         set rowcount @precision

                                                                                                                                                                                                                                                truncate table temp_lookup

                                                                                                                                                                                                                                                                insert into temp_lookup(account_number)
                                                                                                                                                                                                                                                                select li4.account_number
                                                                                                                                                                                                                                                                                from waterfall_pool as li4
                                                                                                                                                                                                                                                                                                                        inner join panbal_sav as sav on li4.account_number = sav.account_number
                                                                                                                                                                                                                                                order by case when sav.rq is null then case when cbck_rate is null then 1 else cbck_rate end when sav.rq > 1 then 1 else sav.rq end * ta_propensity desc

                         set rowcount 0

                      delete from waterfall_pool where account_number in (select account_number from temp_lookup)

                      insert into panbal_amends(account_number, movement)
                      select account_number
                            ,'Account to add to secondary panels for TA coverage'
                        from temp_lookup

                      select @ta = sum(ta_propensity)
                            ,@virtuals = count(1)
                        from panbal_sav as sav
                             left join panbal_amends  as pan on sav.account_number = pan.account_number
                       where panel is not null
                          or pan.account_number is not null

                      select @ta = @ta / @virtuals

               end --while

                                                                                                                        if (@records_out = 0) begin
                                                                                                                                                                        insert into panbal_amends(account_number
                                                                                                                                                                                                                                                                                                                                                                                ,movement)
                                                                                                                                                                        select 0
                           ,'Not enough accounts available to fill the turnaround propensity requirement'
                                                                                                                end

                                                                                                                -- check TA coverage - we also need at least 12% from accounts returning data on all panels
            select @ta = sum(ta_propensity)
              from panbal_sav as sav
                   left join panbal_amends  as pan on sav.account_number = pan.account_number
             where rq >= 0.5
               and (panel is not null or pan.account_number is not null)

            select @ta = @ta / sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores

             while (@ta < @min_ta_ret and @records_out > 0) begin

                                                                                                                                                                                select @records_out = count(1)
                                                                                                                                                                                                from waterfall_pool
                                                                                                                                                                                                
                                                                                                                set rowcount @precision

                                                                                                                                                                                                                                                truncate table temp_lookup

                                                                                                                                                                                                                                                                insert into temp_lookup(account_number)
                                                                                                                                                                                                                                                                select li4.account_number
                                                                                                                                                                                                                                                                                from waterfall_pool as li4
                                                                                                                                                                                                                                                                                                                        inner join panbal_sav as sav on li4.account_number = sav.account_number
                                                                                                                                                                                                                                                order by case when sav.rq is null then case when cbck_rate is null then 1 else cbck_rate end when sav.rq > 1 then 1 else sav.rq end * ta_propensity desc

                         set rowcount 0

                      delete from waterfall_pool where account_number in (select account_number from temp_lookup)

                      insert into panbal_amends(account_number, movement)
                      select account_number
                            ,'Account to add to secondary panels for TA coverage'
                        from temp_lookup

                      select @ta = sum(ta_propensity)
                            ,@virtuals = count(1)
                        from panbal_sav as sav
                             left join panbal_amends  as pan on sav.account_number = pan.account_number
                       where panel is not null
                          or pan.account_number is not null

                      select @ta = @ta / @virtuals

               end --while

                                                                                                                        if (@records_out = 0) begin
                                                                                                                                                                        insert into panbal_amends(account_number
                                                                                                                                                                                                                                                                                                                                                                                ,movement)
                                                                                                                                                                        select 0
                           ,'Not enough accounts available to fill the turnaround propensity requirement'
                                                                                                                end
                                                                                                                        
                -- recreate list4
          truncate table waterfall_pool

            insert into waterfall_pool(
                   account_number
                  ,segment_id
                  ,rq
                  ,thi
            )
            select bas.account_number
                  ,segment_id
                  ,1
                  ,0
              from panbal_sav as bas
             where panel is null
          group by bas.account_number
                  ,segment_id

                -- New accounts to add to alternate day panels
            insert into panbal_amends(account_number, movement)
            select bas.account_number
                   ,'Account to add to secondary panels, eventually for primary panels'
              from PanBal_panel as bas
                   inner join waterfall_pool as li4 on bas.account_number = li4.account_number

                                                                                                                                -- drop unneeded tables
              drop table temp_lookup
              drop table temp_list1_rq                                                                                                          
              drop table temp_panels
              drop table temp_reqd
              drop table temp_available

     end; --V306_M07_VolCheck
        commit;
        
        grant execute on V306_M07_VolCheck to vespa_group_low_security;
        commit;
  create or replace procedure V306_M08_Metrics
         @r_type bit = 0
        ,@today  date = today()
      as begin

          truncate table panbal_metrics

            create table temp_PanBal_all_aggregated_results(
                   aggregation_variable                              int null
                  ,variable_value                                    varchar(60) default null
                  ,Sky_Base_Households                               int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,Good_Household_Index                              double default 0 null
                  ,GHIplus1                                          double default 0 null
                  ,GHIminus1                                         double default 0 null
                  ,incr_diff                                         double default 0 null
                  ,decr_diff                                         double default 0 null
                   )

            commit
            create lf index lfagg on temp_PanBal_all_aggregated_results(aggregation_variable)
            create lf index lfvar on temp_PanBal_all_aggregated_results(variable_value)

            create table temp_panel_households(
                   aggregation_variable                              int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,sky_base_households                               int null
                   )

            create table temp_PanBal_panel(
                   account_number                                    varchar(30) null
                  ,segment_id                                        int null
                   )

            create table temp_PanBal_Scaling_Segment_Profiling (
                   segment_id                                        int null
                  ,Sky_Base_Households                               int null
                  ,Panel_households                                  decimal(10,2) default 0 null
                  ,actual_households                                 int null
                  ,primary key (segment_id)
                   )

            create table temp_panel_segmentation(
                   segment_id                                        int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,actual_households                                 int null
                   )

                -- declarations
           declare @total_sky_base  int
           declare @cow             int

            create table temp_uk_accounts(account_number varchar(30))

            insert into temp_uk_accounts
            select snp.account_number
              from panbal_segment_snapshots as snp
                   inner join panbal_segments_lookup_normalised as lkp on snp.segment_id = lkp.segment_id
             where aggregation_variable = 2
               and value not like 'ROI%'
          group by snp.account_number

            insert into temp_PanBal_Scaling_Segment_Profiling (
                   segment_id
                  ,Sky_Base_Households
                   )
            select snp.segment_id
                  ,count(distinct account_number) as Sky_Base_Households
              from PanBal_segment_snapshots as snp
                   inner join panbal_segments_lookup_normalised as lkp on snp.segment_id = lkp.segment_id
                   inner join temp_uk_accounts as uka on uka.account_number = snp.account_number
          group by snp.segment_id

            select @total_sky_base = sum(Sky_Base_Households) from temp_PanBal_Scaling_Segment_Profiling

            insert into temp_PanBal_panel(
                   account_number
                  ,segment_id
                   )
            select account_number
                  ,segment_id
              from panbal_sav
             where panel in (11, 12)

                if @r_type = 1 begin
                      insert into temp_PanBal_panel(account_number)
                      select account_number
                        from panbal_amends where movement in ('Account to add to secondary panels, eventually for primary panels'
                                                             ,'Account to add to primary panels from secondary panels'
                                                             )

                      delete from temp_PanBal_panel
                       where account_number in (select account_number
                                                  from panbal_amends
                                                 where movement = 'Account to remove from primary panels'
                                               )

                      update temp_PanBal_panel as bas
                         set bas.segment_id = snp.segment_id
                        from panbal_segment_snapshots as snp
                       where bas.account_number = snp.account_number

               end

            insert into temp_panel_segmentation(segment_id
                                           ,Panel_Households
                                           ,actual_households
                                           )
            select bas.segment_id
                  ,sum(case when @r_type = 1 then case when sav.rq is null then 0
                                                       when sav.rq > 1 then 1
                                                       else sav.rq
                                                   end
                                             else case when sav.rq is null then 0
                                                       when sav.rq > 1 then 1
                                                       else sav.rq
                                                   end
                        end
                      )
                  ,count(1)
              from temp_PanBal_panel as bas
                   inner join panbal_sav as sav on bas.account_number = sav.account_number
          group by bas.segment_id

            update temp_PanBal_Scaling_Segment_Profiling as bas
               set Panel_Households     = seg.Panel_Households
                  ,actual_Households    = seg.actual_Households
              from temp_panel_segmentation as seg
             where bas.segment_id       = seg.segment_id

             if @r_type = 0 begin
                      insert into panbal_metrics(metric, value)
                      select 'Actual / ' || var.aggregation_variable || ' / ' || value as metric
                            ,sum(actual_households)
                        from temp_PanBal_Scaling_Segment_Profiling as ssp
                             inner join panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
                             left  join panbal_variables                  as var on ssl.aggregation_variable = var.id
                       where var.aggregation_variable is not null
                    group by metric
               end

            insert into temp_PanBal_all_aggregated_results(
                   aggregation_variable
                  ,variable_value
                  ,Sky_Base_Households
                  ,Panel_Households
                   )
            select ssl.aggregation_variable
                  ,ssl.value
                  ,sum(sky_base_households)
                  ,sum(Panel_households)
              from temp_PanBal_Scaling_Segment_Profiling as ssp
                   inner join panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
          group by ssl.aggregation_variable
                  ,ssl.value

                -- insert panel aggregated results into metrics table
            insert into panbal_metrics(metric, value)
            select case when @r_type = 1 then 'Proposed' else 'Current' end || ' Panel / ' || var.aggregation_variable || ' / ' || value as metric
                  ,sum(panel_households)
              from temp_PanBal_Scaling_Segment_Profiling as ssp
                   inner join panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
                   left  join panbal_variables                  as var on ssl.aggregation_variable = var.id
             where value not like 'ROI%'
          group by metric

                -- insert sky base aggregated results into metrics table
            insert into panbal_metrics(metric, value)
            select 'Sky Base' || ' / ' || var.aggregation_variable || ' / ' || value as metric
                  ,sum(sky_base_households)
              from temp_PanBal_Scaling_Segment_Profiling as ssp
                   inner join panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
                   left  join panbal_variables                  as var on ssl.aggregation_variable = var.id
             where var.aggregation_variable is not null
               and value not like 'ROI%'
          group by metric

                -- if any values are Unknown, then we don't want to balance these
            delete from temp_PanBal_all_aggregated_results
             where variable_value in ('Non-scalable', 'NS', 'U', 'Not Defined', 'D) Unknown', 'Unknown', 'No Panel', 'ROI Not Defined')
                or sky_base_households < 1000
                or sky_base_households is null

          truncate table temp_panel_households

            insert into temp_panel_households(
                   aggregation_variable
                  ,Panel_Households
                  ,sky_base_households
                   )
            select aggregation_variable
                  ,sum(panel_households)
                  ,sum(sky_base_households)
              from temp_PanBal_all_aggregated_results
          group by aggregation_variable

            update temp_PanBal_all_aggregated_results as bas
               set Good_Household_Index = 100.0 *  bas.Panel_households      * hsh.Sky_Base_Households / bas.Sky_Base_Households /  hsh.Panel_Households      --index value for each variable value
              from temp_panel_households as hsh
             where bas.aggregation_variable = hsh.aggregation_variable

                -- insert traffic lights into metric table
            insert into panbal_metrics(metric, value)
            select case when @r_type = 1 then 'Proposed / ' else 'Current / ' end || cast(var.aggregation_variable as varchar(20))
                  ,sqrt(avg((Good_Household_Index - 100) * (Good_Household_Index - 100))) as imbalance_rating
              from temp_PanBal_all_aggregated_results as bas
                   left join panbal_variables     as var on bas.aggregation_variable = var.id
             where var.aggregation_variable is not null
          group by var.aggregation_variable

            declare @scaling_day date

             select @scaling_day = max(adjusted_event_start_date_vespa)
               from viq_viewing_data_scaling

             select sum(calculated_scaling_weight * calculated_scaling_weight) as large
                   ,sum(calculated_scaling_weight)                             as small
                   ,count(*)                                                   as total_accounts
                   ,adjusted_event_start_date_vespa
               into temp_ess2
               from viq_viewing_data_scaling
              where adjusted_event_start_date_vespa = @scaling_day
           group by adjusted_event_start_date_vespa

                -- insert ESS from VIQ into metrics table
            insert into panbal_metrics(metric,value)
            select case when @r_type = 1 then 'Proposed' else 'Current' end || ' / ESS from VIQ'
                  ,(small * small) / large
              from temp_ess2

                -- insert scaled accounts count into metrics
            insert into panbal_metrics(metric,value)
            select 'Scaled Accounts'
                  ,count(account_number)
              from viq_viewing_data_scaling
             where adjusted_event_start_date_vespa = (select max(adjusted_event_start_date_vespa) from viq_viewing_data_scaling)

              -- insert primary panel accounts count into metrics
            insert into panbal_metrics(metric,value)
            select case when @r_type = 1 then 'Proposed' else 'Current' end || ' / Primary panel accounts'
                  ,count(account_number)
              from temp_PanBal_panel

                -- insert primary panel box count into metrics
            insert into panbal_metrics(metric,value)
            select case when @r_type = 1 then 'Proposed' else 'Current' end || ' / Primary panel boxes'
                  ,sum(boxes)
              from temp_PanBal_panel as bas
                   inner join panbal_sav as sav on bas.account_number = sav.account_number

            insert into temp_PanBal_panel(
                   account_number
                   )
            select bas.account_number
              from panbal_sav as bas
                   left join temp_PanBal_panel as pan on bas.account_number = pan.account_number
             where panel in (5, 6, 7)
               and pan.account_number is null

                if @r_type = 1 begin
                      insert into temp_PanBal_panel(
                             account_number
                             )
                      select ame.account_number
                        from panbal_amends                        as ame
                             left join temp_PanBal_panel              as bas on ame.account_number = bas.account_number
                       where bas.account_number is null
                         and movement in ('Account to remove from primary panels'
                                         ,'Account to add to secondary panels as segment backup'
                                         ,'Account to add to secondary panels for virtual panel req.'
                                         ,'Account to add to secondary panels for TA coverage'
                                         )
               end

                -- insert combined panel box count into metrics
            insert into panbal_metrics(metric, value)
            select case when @r_type = 1 then 'Proposed' else 'Current' end || ' / Combined panels boxes'
                  ,sum(boxes)
              from temp_PanBal_panel as bas
                   inner join panbal_sav as sav on bas.account_number = sav.account_number

                -- insert combined accounts count into metrics
            insert into panbal_metrics(metric, value)
            select case when @r_type = 1 then 'Proposed' else 'Current' end || ' / Combined panels accounts'
                  ,count(account_number)
              from temp_PanBal_panel

                -- insert virtual panel count into metrics
            insert into panbal_metrics(metric, value)
            select 'Virtual Panel Boxes'
                  ,sum(boxes)
              from temp_PanBal_panel         as bas
                   inner join panbal_sav as sav on bas.account_number = sav.account_number
             where vp1 = 1

                -- insert TA propensity coverage into metrics
            select @cow = sum(ta_propensity)
              from panbal_sav as sav
                   left join temp_PanBal_panel as pan on sav.account_number = pan.account_number

            insert into panbal_metrics(metric, value)
            select 'TA coverage'
                  ,@cow / sum(ta_propensity)
              from vespa_analysts.SkyBase_TA_scores

                -- insert TA propensity coverage from returning accounts into metrics
            select @cow = sum(ta_propensity)
              from panbal_sav as sav
                   left join temp_PanBal_panel as pan on sav.account_number = pan.account_number
             where rq >= 0.5

                -- insert TA propensity coverage from returning accounts into metrics
            select @cow = sum(ta_propensity)
              from panbal_sav as sav
                   left join temp_PanBal_panel as pan on sav.account_number = pan.account_number
             where rq >= 0.5

            insert into panbal_metrics(metric, value)
            select 'TA coverage from returning accounts'
                  ,@cow / sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores

            insert into panbal_metrics_hist(
                   metric
                  ,value
                  ,dt
                   )
            select metric
                  ,value
                  ,@today
              from panbal_metrics

              drop table temp_PanBal_all_aggregated_results
              drop table temp_panel_households
              drop table temp_PanBal_panel
              drop table temp_PanBal_Scaling_Segment_Profiling
              drop table temp_panel_segmentation
              drop table temp_ess2
              drop table temp_uk_accounts

     end; --V306_M08_Metrics
  commit;
  grant execute on V306_M08_Metrics to vespa_group_low_security;
