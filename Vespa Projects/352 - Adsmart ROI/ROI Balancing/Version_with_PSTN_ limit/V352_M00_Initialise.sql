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
**Analyst:                              Jonathan Green
**Contributions From:                   Hoi Yu Tang, Leonardo Ripoli, Jason Thompson, Jose Loureda
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306, V352
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




  create or replace procedure V352_M00_Initialise
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
                            ,knockout_reason_ROI_PSTN varchar(50)           null
                             )
                   end
                 grant select on waterfall_base to vespa_group_low_security
            commit

                -- Recreate PanBal_segment_snapshots table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate PanBal_segment_snapshots table' TO CLIENT

                if object_id('PanBal_segment_snapshots') is not null begin
                     truncate table PanBal_segment_snapshots
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
   
                -- Recreate pstn_panel_pool table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate pstn_panel_pool table' TO CLIENT

                if object_id('pstn_panel_pool') is not null begin                
                truncate table pstn_panel_pool
               end
              else begin
                      create table pstn_panel_pool(
                             account_number                                     varchar(30) null
                            ,segment_id                                         int null
                            ,rq                                                 double null
                            ,thi                                                double null
                             )
                       grant select on pstn_panel_pool to vespa_group_low_security commit
                      create hg index hgpst on pstn_panel_pool(segment_id) commit
                      create hg index hgthi on pstn_panel_pool(thi) commit
               end   
               
                -- Recreate waterfall_pool table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate waterfall_pool table' TO CLIENT

                if object_id('waterfall_pool') is not null begin
                     truncate table waterfall_pool
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
      
                -- Recreate pstn_waterfall_pool table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate pstn_waterfall_pool table' TO CLIENT

                if object_id('pstn_waterfall_pool') is not null begin
                     truncate table pstn_waterfall_pool
               end
              else begin
                      create table pstn_waterfall_pool(
                             account_number                                     varchar(30) null
                            ,segment_id                                         int null
                            ,rq                                                 double null
                            ,thi                                                double null
                             )
                       grant select on pstn_waterfall_pool to vespa_group_low_security commit
                      create hg index hgthi on pstn_waterfall_pool(thi) commit
                      create hg index hgseg on pstn_waterfall_pool(segment_id) commit
                      create unique hg index uhacc on pstn_waterfall_pool(account_number) commit
               end
      
                -- Recreate panbal_results table
           MESSAGE cast(now() as timestamp)||' | Initialise M00.1 - Recreate panbal_results table' TO CLIENT

                if object_id('panbal_results') is not null begin
                     truncate table panbal_results
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
  commit;

   grant execute on V352_M00_Initialise to vespa_group_low_security;
  commit;

