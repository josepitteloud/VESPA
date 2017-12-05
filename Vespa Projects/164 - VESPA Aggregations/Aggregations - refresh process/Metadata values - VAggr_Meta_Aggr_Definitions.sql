/*###############################################################################
# Created on:   18/09/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - metadata information creation:
#               VAggr_Meta_Aggr_Definitions
#
#               (updated for historical purposes only, not to be run unless all
#                data has to be recreated from scratch)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VESAP_Shared.VAggr_Meta_Aggr_Definitions
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 18/09/2013  SBE   Initial version
# 14/05/2014  ABA   EE Re-branding changes to Entertainment package names
#
###############################################################################*/



--truncate table VESPA_Shared.VAggr_Meta_Aggr_Definitions;


  -- ######################################################################################################################################################
  -- ######## "Initial 18 CIA" aggregations                                                                                                        ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (1,    -- ### CntUnq__Days_Data_Returned ###
    'max(case
           when acc.Ent_DTV_Sub = 0               or acc.Movmt_DTV_Sub = 1 then -3                                    -- Active/No movement for DTV
             else coalesce(acc.Days_Data_Returned, 0)
         end)'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (2,    -- ### VwDur__All_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
         else sum( coalesce(vw.Instance_Duration, 0) )
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (3,    -- ### VwDur__Pack_Original ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Original) = 0         or max(acc.Movmt_DTV_Pack_Original) = 1 then -3                          -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Original_Pay = 1 or vw.F_CType_O_V_F_FTA = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)   -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (4,    -- ### VwDur__Pack_Variety ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Variety) = 0   or max(acc.Movmt_DTV_Pack_Variety) = 1 then -3                    -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Variety_Pay = 1 or vw.F_CType_O_V_F_FTA = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)   -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (5,    -- ### VwDur__Pack_Family ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Family) = 0 or max(acc.Movmt_DTV_Pack_Family) = 1 then -3            -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Family_Pay = 1 or vw.F_CType_O_V_F_FTA = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)   -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (6,    -- ### VwDur__Prem_Movies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case
                    when vw.F_CType_Retail_Movies = 1 then coalesce(vw.Instance_Duration, 0)
                    when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                          vw.F_CType_Retail_ALC_Movies_Pack = 1 then coalesce(vw.Instance_Duration, 0)
                      else 0
                  end)                                                                                                -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (7,    -- ### VwDur__Prem_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)           -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (8,    -- ### VwDur__Prem_ALa_Carte ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when (max(acc.Ent_ESPN_Sub) = 1       and max(acc.Movmt_ESPN_Sub) = 0) or                                      -- Active/No movement relevant package/subscription (ESPN)
            (max(acc.Ent_ChelseaTV_Sub) = 1  and max(acc.Movmt_ChelseaTV_Sub) = 0) or                                 -- Active/No movement relevant package/subscription (Chelsea TV)
            (max(acc.Ent_MUTV_Sub) = 1       and max(acc.Movmt_MUTV_Sub) = 0) or                                      -- Active/No movement relevant package/subscription (MUTV)
            (max(acc.Ent_MGM_Sub) = 1        and max(acc.Movmt_MGM_Sub) = 0)                                          -- Active/No movement relevant package/subscription (MGM)
           then sum(case
                      when vw.F_CType_Retail_ALa_Carte = 1 then coalesce(vw.Instance_Duration, 0)
                      when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                            vw.F_CType_Retail_ALC_Movies_Pack = 1 then coalesce(vw.Instance_Duration, 0)
                        else 0
                    end)
          else -3                                                                                                     -- Otherwise no package or movement
     end'
                       );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (9,    -- ### VwDur__Pack_HD ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_HD_Sub) = 0               or max(acc.Movmt_HD_Sub) = 1 then -3                                -- Active/No movement relevant package/subscription
         else sum(case when vw.F_Format_HD = 1 or vw.F_Format_3D = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)   -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (10,   -- ### VwDur__Pack_3D ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_TV3D_Sub) = 0             or max(acc.Movmt_TV3D_Sub) = 1 then -3                              -- Active/No movement relevant package/subscription
         else sum(case when vw.F_Format_3D = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)   -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (11,   -- ### VwDur__Prem_3rd_Party ###
    'case
       when max(acc.Ent_DTV_Sub) = 0 or max(acc.Movmt_DTV_Sub) = 1 then -3                              -- Active/No movement for DTV
         else sum(case when vw.F_CType_3rd_Party = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)     -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (12,   -- ### VwDur__Pack_Original_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Original) = 0         or max(acc.Movmt_DTV_Pack_Original) = 1 then -3                          -- Active/No movement relevant package/subscription
         else sum(case when F_CType_Original_Pay = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)                    -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (13,   -- ### VwDur__Pack_Original_FTA_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Original) = 0         or max(acc.Movmt_DTV_Pack_Original) = 1 then -3                          -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_O_V_F_FTA = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)            -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (14,   -- ### VwDur__Pack_Variety_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Variety) = 0   or max(acc.Movmt_DTV_Pack_Variety) = 1 then -3                    -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Variety_Pay = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)           -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (15,   -- ### VwDur__Pack_Variety_FTA_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Variety) = 0   or max(acc.Movmt_DTV_Pack_Variety) = 1 then -3                    -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_O_V_F_FTA = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)            -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (16,   -- ### VwDur__Pack_Family_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Family) = 0 or max(acc.Movmt_DTV_Pack_Family) = 1 then -3            -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Family_Pay = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)      -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (17,   -- ### VwDur__Pack_Family_FTA_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Pack_Family) = 0 or max(acc.Movmt_DTV_Pack_Family) = 1 then -3            -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_O_V_F_FTA = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)            -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (18,   -- ### VwDur__Recorded_Viewing ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_PVR_Enabled) = 0 then -3                                                                      -- Active/No movement relevant package/subscription
         else sum(case when vw.F_Playback = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)                      -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (19,   -- ### VwDur__All_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
         else sum(case when vw.F_CType_Pay = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)                     -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (20,   -- ### Flag__Low_CQM_Score ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Acc_Univ_Model_Score) between 1 and 22 then 1                                                     -- Relevant model scores only
         else 0
     end'
                                                                             );

commit;



insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (21,   -- ### AvDVw__All_TV ###
    'case
       when #4# = -3 or #5# = -3 then -3                                                                              -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #5# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #4# / #5#                                                                                               -- Derivation
     end',
    0, 0, 0, 2, 1                             -- [x], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (22,   -- ### AvDVw__Pack_Original ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    3, 0, 0, 2, 1                             -- [Total viewing - Original], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (23,   -- ### AvDVw__Pack_Variety ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    4, 0, 0, 2, 1                             -- [Total viewing - Variety], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (24,   -- ### AvDVw__Pack_Family ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    5, 0, 0, 2, 1                             -- [Total viewing - Family], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (25,   -- ### SOV__Prem_Movies ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    6, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (26,   -- ### SOV__Prem_Sports ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    7, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (27,   -- ### SOV__Prem_ALa_Carte ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    8, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (28,   -- ### SOV__Pack_HD ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    9, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (29,   -- ### SOV__Pack_3D ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    10, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (30,   -- ### SOV__Prem_3rd_Party ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    11, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (31,   -- ### SOV__Pack_Original_Pay_TV ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    12, 3, 0, 2, 1                             -- [Aggregation related viewing], [Original Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (32,   -- ### SOV__Pack_Original_FTA_TV ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    13, 3, 0, 2, 1                             -- [Aggregation related viewing], [Original Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (33,   -- ### SOV__Pack_Variety_Pay_TV ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    14, 4, 0, 2, 1                             -- [Aggregation related viewing], [Variety Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (34,   -- ### SOV__Pack_Variety_FTA_TV ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    15, 4, 0, 2, 1                             -- [Aggregation related viewing], [Variety Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (35,   -- ### SOV__Pack_Family_Pay_TV ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    16, 5, 0, 2, 1                             -- [Aggregation related viewing], [Family Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (36,   -- ### SOV__Pack_Family_FTA_TV ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    17, 5, 0, 2, 1                             -- [Aggregation related viewing], [Family Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (37,   -- ### SOV__Recorded_Viewing ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    18, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (38,   -- ### SOV__All_Pay_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    19, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );



insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (39,   -- ### Flag__Offer_Seeker ###
    '
    select
          ##^PERIOD^## as Period_Key,                      -- Period Key
          ##^AGGR_KEY^## as Aggregation_Key,               -- Aggregation Key
          acc.Account_Number,                              -- Account Number
          acc.Panel_Id,
          case
            when met1.Metric_Value = -3 then -3                                         -- "Not eligible"
            when met1.Metric_Value = -2 then -2                                         -- "Excluded" (data return ratio <50%)
            when dim.High_Level_Banding like ''%High'' and met2.Metric_Value = 1 then 1     -- Derivation
              else 0                                                                    -- Default
          end
      from VESPA_Shared.Aggr_Account_Attributes acc
              inner join VESPA_Shared.Aggr_Period_Dim prd             on acc.Period_Key = prd.Period_Key
                                                                     and acc.Period_Key = ##^PERIOD^##

                -- HML classification for "Pay TV"
              left join VESPA_Shared.Aggr_Fact met1                   on acc.Account_Number = met1.Account_Number
                                                                     and acc.Period_Key = met1.Period_Key
                                                                     and met1.Aggregation_Key = 38

              left join VESPA_Shared.Aggr_Metric_Group_Dim dim        on met1.Metric_Group_Key = dim.Metric_Group_Key

                -- Model score
              left join VESPA_Shared.Aggr_Fact met2                   on acc.Account_Number = met2.Account_Number
                                                                     and acc.Period_Key = met2.Period_Key
                                                                     and met2.Aggregation_Key = 20
    ',
    0, 0, 0, 0, 0                             -- [x], [x], [x], [x], [x]
                                                                             );

commit;




  -- ######################################################################################################################################################
  -- ######## Number of programmes                                                                                                                 ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (40,    -- ### NumProgs__All_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                           else vw.Prog_Instance_Id
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (41,    -- ### NumProgs__Non_Premium ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Original_Pay = 1 or vw.F_CType_Variety_Pay = 1 or
                                               vw.F_CType_Family_Pay = 1 or vw.F_CType_O_V_F_FTA = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (42,    -- ### NumProgs__Prem_Movies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) then vw.Prog_Instance_Id
                                         when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                               vw.F_CType_Retail_ALC_Movies_Pack = 1 then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (43,    -- ### NumProgs__Prem_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (44,    -- ### NumProgs__Prem_ALa_Carte ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when (max(acc.Ent_ESPN_Sub) = 1       and max(acc.Movmt_ESPN_Sub) = 0) or                                      -- Active/No movement relevant package/subscription (ESPN)
            (max(acc.Ent_ChelseaTV_Sub) = 1  and max(acc.Movmt_ChelseaTV_Sub) = 0) or                                 -- Active/No movement relevant package/subscription (Chelsea TV)
            (max(acc.Ent_MUTV_Sub) = 1       and max(acc.Movmt_MUTV_Sub) = 0) or                                      -- Active/No movement relevant package/subscription (MUTV)
            (max(acc.Ent_MGM_Sub) = 1        and max(acc.Movmt_MGM_Sub) = 0)                                          -- Active/No movement relevant package/subscription (MGM)
         then coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_ALa_Carte = 1) then vw.Prog_Instance_Id
                                         when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                               vw.F_CType_Retail_ALC_Movies_Pack = 1 then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
          else -3                                                                                                     -- Otherwise no package or movement
     end'
                       );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (45,    -- ### NumProgs__Pack_HD ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_HD_Sub) = 0               or max(acc.Movmt_HD_Sub) = 1 then -3                                -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Format_HD = 1 or vw.F_Format_3D = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (46,   -- ### NumProgs__Pack_3D ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_TV3D_Sub) = 0             or max(acc.Movmt_TV3D_Sub) = 1 then -3                              -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Format_3D = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (47,   -- ### NumProgs__Prem_3rd_Party ###
    'case
       when max(acc.Ent_DTV_Sub) = 0 or max(acc.Movmt_DTV_Sub) = 1 then -3                                            -- Active/No movement for DTV
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_3rd_Party = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (48,   -- ### NumProgs__Non_Premium_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Original_Pay = 1 or vw.F_CType_Variety_Pay = 1 or
                                               vw.F_CType_Family_Pay = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (49,   -- ### NumProgs__Non_Premium_FTA_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_FTA = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (50,   -- ### NumProgs__Recorded_Viewing ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_PVR_Enabled) = 0 then -3                                                                      -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Playback = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (51,   -- ### NumProgs__All_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Pay = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );




insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (52,   -- ### AvDNumProgs__All_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    40, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (53,   -- ### AvDNumProgs__Non_Premium ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    41, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (54,   -- ### AvDNumProgs__Prem_Movies ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    42, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (55,   -- ### AvDNumProgs__Prem_Sports ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    43, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (56,   -- ### AvDNumProgs__Prem_ALa_Carte ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    44, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (57,   -- ### AvDNumProgs__Pack_HD ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    45, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (58,   -- ### AvDNumProgs__Pack_3D ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    46, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (59,   -- ### AvDNumProgs__Prem_3rd_Party ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    47, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (60,   -- ### AvDNumProgs__Non_Premium_Pay_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    48, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (61,   -- ### AvDNumProgs__Non_Premium_FTA_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    49, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (62,   -- ### AvDNumProgs__Recorded_Viewing ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    50, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (63,   -- ### AvDNumProgs__All_Pay_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    51, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Number of complete programmes                                                                                                        ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (64,    -- ### NumCompleteProgs__All_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (65,    -- ### NumCompleteProgs__Non_Premium ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Original_Pay = 1 or vw.F_CType_Variety_Pay = 1 or
                                               vw.F_CType_Family_Pay = 1 or vw.F_CType_O_V_F_FTA = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (66,    -- ### NumCompleteProgs__Prem_Movies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1)  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                         when (
                                                (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                                 vw.F_CType_Retail_ALC_Movies_Pack = 1
                                              )  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (67,    -- ### NumCompleteProgs__Prem_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1)  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (68,    -- ### NumCompleteProgs__Prem_ALa_Carte ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when (max(acc.Ent_ESPN_Sub) = 1       and max(acc.Movmt_ESPN_Sub) = 0) or                                      -- Active/No movement relevant package/subscription (ESPN)
            (max(acc.Ent_ChelseaTV_Sub) = 1  and max(acc.Movmt_ChelseaTV_Sub) = 0) or                                 -- Active/No movement relevant package/subscription (Chelsea TV)
            (max(acc.Ent_MUTV_Sub) = 1       and max(acc.Movmt_MUTV_Sub) = 0) or                                      -- Active/No movement relevant package/subscription (MUTV)
            (max(acc.Ent_MGM_Sub) = 1        and max(acc.Movmt_MGM_Sub) = 0)                                          -- Active/No movement relevant package/subscription (MGM)
         then coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_ALa_Carte = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                         when (
                                                (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                                 vw.F_CType_Retail_ALC_Movies_Pack = 1
                                              ) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
          else -3                                                                                                     -- Otherwise no package or movement
     end'
                       );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (69,    -- ### NumCompleteProgs__Pack_HD ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_HD_Sub) = 0               or max(acc.Movmt_HD_Sub) = 1 then -3                                -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Format_HD = 1 or vw.F_Format_3D = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (70,   -- ### NumCompleteProgs__Pack_3D ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_TV3D_Sub) = 0             or max(acc.Movmt_TV3D_Sub) = 1 then -3                              -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Format_3D = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (71,   -- ### NumCompleteProgs__Prem_3rd_Party ###
    'case
       when max(acc.Ent_DTV_Sub) = 0 or max(acc.Movmt_DTV_Sub) = 1 then -3                                            -- Active/No movement for DTV
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_3rd_Party = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (72,   -- ### NumCompleteProgs__Non_Premium_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Original_Pay = 1 or vw.F_CType_Variety_Pay = 1 or
                                               vw.F_CType_Family_Pay = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (73,   -- ### NumCompleteProgs__Non_Premium_FTA_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_FTA = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id

                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (74,   -- ### NumCompleteProgs__Recorded_Viewing ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_PVR_Enabled) = 0 then -3                                                                      -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Playback = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id

                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (75,   -- ### NumCompleteProgs__All_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Pay = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id

                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (76,   -- ### AvDNumCompleteProgs__All_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    64, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (77,   -- ### AvDNumCompleteProgs__Non_Premium ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    65, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (78,   -- ### AvDNumCompleteProgs__Prem_Movies ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    66, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (79,   -- ### AvDNumCompleteProgs__Prem_Sports ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    67, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (80,   -- ### AvDNumCompleteProgs__Prem_ALa_Carte ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    68, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (81,   -- ### AvDNumCompleteProgs__Pack_HD ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    69, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (82,   -- ### AvDNumCompleteProgs__Pack_3D ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    70, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (83,   -- ### AvDNumCompleteProgs__Prem_3rd_Party ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    71, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (84,   -- ### AvDNumCompleteProgs__Non_Premium_Pay_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    72, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (85,   -- ### AvDNumCompleteProgs__Non_Premium_FTA_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    73, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (86,   -- ### AvDNumCompleteProgs__Recorded_Viewing ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    74, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (87,   -- ### AvDNumCompleteProgs__All_Pay_TV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    75, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Non-premium aggregated viewing                                                                                                       ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (88,    -- ### VwDur__Non_Premium ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)            -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (89,    -- ### VwDur__Non_Premium_Pay_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Pay = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)            -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (90,    -- ### VwDur__Non_Premium_FTA_TV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_FTA = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)            -- Relevant viewing only
     end'
                                                                             );




insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (91,   -- ### AvDVw__Non_Premium ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    88, 0, 0, 2, 1                             -- [Total viewing - Non-Premium], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (92,   -- ### SOV__Non_Premium_Pay_TV ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    89, 88, 0, 2, 1                             -- [Aggregation related viewing], [Family Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (93  ,   -- ### SOV__Non_Premium_FTA_TV ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    90, 88, 0, 2, 1                             -- [Aggregation related viewing], [Family Viewing], [x], [Total viewing], [Days data returned]
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Genres based viewing - total viewing duration                                                                                        ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (94,    -- ### VwDur__Genre_Non_Prem_Children ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Children = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (95,    -- ### VwDur__Genre_Non_Prem_Movies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Movies = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)         -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (96,    -- ### VwDur__Genre_Non_Prem_News_Documentaries ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_News_Documentaries = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (97,    -- ### VwDur__Genre_Non_Prem_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Sports = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)         -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (98,    -- ### VwDur__Genre_Non_Prem_Action_SciFi ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Action_SciFi = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)   -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (99,    -- ### VwDur__Genre_Non_Prem_Arts_Lifestyle ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Arts_Lifestyle = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (100,    -- ### VwDur__Genre_Non_Prem_Comedy_Game_Shows ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Comedy_GameShows = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (101,    -- ### VwDur__Genre_Non_Prem_Drama_Crime ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Drama_Crime = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)    -- Relevant viewing only
     end'
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (102,    -- ### VwDur__Genre_Prem_Movies_Action_Adventure ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case
                    when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Prem_Movies_Action_Adventure = 1 then coalesce(vw.Instance_Duration, 0)
                    when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                          vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Action_Adventure = 1 then coalesce(vw.Instance_Duration, 0)
                      else 0
                  end)                                                                                                -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (103,    -- ### VwDur__Genre_Prem_Movies_Comedy ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case
                    when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Prem_Movies_Comedy = 1 then coalesce(vw.Instance_Duration, 0)
                    when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                          vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Comedy = 1 then coalesce(vw.Instance_Duration, 0)
                      else 0
                  end)                                                                                                -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (104,    -- ### VwDur__Genre_Prem_Movies_Drama_Romance ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case
                    when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Prem_Movies_Drama_Romance = 1 then coalesce(vw.Instance_Duration, 0)
                    when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                          vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Drama_Romance = 1 then coalesce(vw.Instance_Duration, 0)
                      else 0
                  end)                                                                                                -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (105,    -- ### VwDur__Genre_Prem_Movies_Family ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case
                    when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Prem_Movies_Family = 1 then coalesce(vw.Instance_Duration, 0)
                    when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                          vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Family = 1 then coalesce(vw.Instance_Duration, 0)
                      else 0
                  end)                                                                                                -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (106,    -- ### VwDur__Genre_Prem_Movies_Horror_Thriller ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case
                    when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Prem_Movies_Horror_Thriller = 1 then coalesce(vw.Instance_Duration, 0)
                    when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                          vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Horror_Thriller = 1 then coalesce(vw.Instance_Duration, 0)
                      else 0
                  end)                                                                                                -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (107,    -- ### VwDur__Genre_Prem_Movies_SciFi_Fantasy ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case
                    when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Prem_Movies_SciFi_Fantasy = 1 then coalesce(vw.Instance_Duration, 0)
                    when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                          vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_SciFi_Fantasy = 1 then coalesce(vw.Instance_Duration, 0)
                      else 0
                  end)                                                                                                -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (108,    -- ### VwDur__Genre_Prem_Sports_American ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_American = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (109,    -- ### VwDur__Genre_Prem_Sports_Boxing_Wrestling ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_Boxing_Wrestling = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (110,    -- ### VwDur__Genre_Prem_Sports_Cricket ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_Cricket = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (111,    -- ### VwDur__Genre_Prem_Sports_Football ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_Football = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (112,    -- ### VwDur__Genre_Prem_Sports_Golf ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_Golf = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (113,    -- ### VwDur__Genre_Prem_Sports_Motor_Extreme ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_Motor_Extreme = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (114,    -- ### VwDur__Genre_Prem_Sports_Rugby ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_Rugby = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (115,    -- ### VwDur__Genre_Prem_Sports_Tennis ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_Tennis = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (116,    -- ### VwDur__Genre_Prem_Sports_Niche_Sport ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else sum(case when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Prem_Sports_Niche_Sport = 1 then coalesce(vw.Instance_Duration, 0) else 0 end) -- Relevant viewing only
     end'
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Genres based viewing - number of programmes watched                                                                                  ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (117,    -- ### NumProgs__Genre_Non_Prem_Children ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Children = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (118,    -- ### NumProgs__Genre_Non_Prem_Movies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Movies = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (119,    -- ### NumProgs__Genre_Non_Prem_News_Documentaries ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_News_Documentaries = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (120,    -- ### NumProgs__Genre_Non_Prem_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Sports = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (121,    -- ### NumProgs__Genre_Non_Prem_Action_SciFi ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Action_SciFi = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (122,    -- ### NumProgs__Genre_Non_Prem_Arts_Lifestyle ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Arts_Lifestyle = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (123,    -- ### NumProgs__Genre_Non_Prem_Comedy_Game_Shows ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Comedy_GameShows = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (124,    -- ### NumProgs__Genre_Non_Prem_Drama_Crime ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1 and vw.F_Genre_Non_Prem_Drama_Crime = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (125,    -- ### NumProgs__Genre_Prem_Movies_Action_Adventure ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and (vw.F_Genre_Prem_Movies_Action_Adventure = 1) then vw.Prog_Instance_Id
                                         when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Action_Adventure = 1 then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (126,    -- ### NumProgs__Genre_Prem_Movies_Comedy ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and (vw.F_Genre_Prem_Movies_Comedy = 1) then vw.Prog_Instance_Id
                                         when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Comedy = 1 then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (127,    -- ### NumProgs__Genre_Prem_Movies_Drama_Romance ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and (vw.F_Genre_Prem_Movies_Drama_Romance = 1) then vw.Prog_Instance_Id
                                         when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Drama_Romance = 1 then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (128,    -- ### NumProgs__Genre_Prem_Movies_Family ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and (vw.F_Genre_Prem_Movies_Family = 1) then vw.Prog_Instance_Id
                                         when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Family = 1 then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (129,    -- ### NumProgs__Genre_Prem_Movies_Horror_Thriller ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and (vw.F_Genre_Prem_Movies_Horror_Thriller = 1) then vw.Prog_Instance_Id
                                         when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_Horror_Thriller = 1 then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (130,    -- ### NumProgs__Genre_Prem_Movies_SciFi_Fantasy ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and (vw.F_Genre_Prem_Movies_SciFi_Fantasy = 1) then vw.Prog_Instance_Id
                                         when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Prem_Movies_SciFi_Fantasy = 1 then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (131,    -- ### NumProgs__Genre_Prem_Sports_American ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_American = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_American = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (132,    -- ### NumProgs__Genre_Prem_Sports_Boxing_Wrestling ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_Boxing_Wrestling = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_Boxing_Wrestling = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (133,    -- ### NumProgs__Genre_Prem_Sports_Cricket ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_Cricket = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_Cricket = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (134,    -- ### NumProgs__Genre_Prem_Sports_Football ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_Football = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_Football = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (135,    -- ### NumProgs__Genre_Prem_Sports_Golf ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_Golf = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_Golf = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (136,    -- ### NumProgs__Genre_Prem_Sports_Motor_Extreme ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_Motor_Extreme = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_Motor_Extreme = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (137,    -- ### NumProgs__Genre_Prem_Sports_Rugby ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_Rugby = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_Rugby = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (138,    -- ### NumProgs__Genre_Prem_Sports_Tennis ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_Tennis = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_Tennis = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (139,    -- ### NumProgs__Genre_Prem_Sports_Niche_Sport ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) and (vw.F_Genre_Prem_Sports_Niche_Sport = 1) then null
                                         when (vw.F_CType_Retail_Sports = 1) and (vw.F_Genre_Prem_Sports_Niche_Sport = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Genres based viewing - number of complete programmes watched                                                                         ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (140,    -- ### NumCompleteProgs__Genre_Non_Prem_Children ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1) and (vw.F_Genre_Non_Prem_Children = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (141,    -- ### NumCompleteProgs__Genre_Non_Prem_Movies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1) and (vw.F_Genre_Non_Prem_Movies = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (142,    -- ### NumCompleteProgs__Genre_Non_Prem_News_Documentaries ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1) and (vw.F_Genre_Non_Prem_News_Documentaries = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (143,    -- ### NumCompleteProgs__Genre_Non_Prem_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1) and (vw.F_Genre_Non_Prem_Sports = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (144,    -- ### NumCompleteProgs__Genre_Non_Prem_Action_SciFi ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1) and (vw.F_Genre_Non_Prem_Action_SciFi = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (145,    -- ### NumCompleteProgs__Genre_Non_Prem_Arts_Lifestyle ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1) and (vw.F_Genre_Non_Prem_Arts_Lifestyle = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (146,    -- ### NumCompleteProgs__Genre_Non_Prem_Comedy_Game_Shows ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1) and (vw.F_Genre_Non_Prem_Comedy_GameShows = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (147,    -- ### NumCompleteProgs__Genre_Non_Prem_Drama_Crime ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_Any = 1) and (vw.F_Genre_Non_Prem_Drama_Crime = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (148,    -- ### NumCompleteProgs__Genre_Prem_Movies_Action_Adventure ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and
                                              (vw.F_Genre_Prem_Movies_Action_Adventure = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                         when (vw.F_Genre_Prem_Movies_Action_Adventure = 1) and
                                              (
                                                (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                                 vw.F_CType_Retail_ALC_Movies_Pack = 1
                                              )  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (149,    -- ### NumCompleteProgs__Genre_Prem_Movies_Comedy ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and
                                              (vw.F_Genre_Prem_Movies_Comedy = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                         when (vw.F_Genre_Prem_Movies_Comedy = 1) and
                                              (
                                                (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                                 vw.F_CType_Retail_ALC_Movies_Pack = 1
                                              )  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (150,    -- ### NumCompleteProgs__Genre_Prem_Movies_Drama_Romance ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and
                                              (vw.F_Genre_Prem_Movies_Drama_Romance = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                         when (vw.F_Genre_Prem_Movies_Drama_Romance = 1) and
                                              (
                                                (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                                 vw.F_CType_Retail_ALC_Movies_Pack = 1
                                              )  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (151,    -- ### NumCompleteProgs__Genre_Prem_Movies_Family ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and
                                              (vw.F_Genre_Prem_Movies_Family = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                         when (vw.F_Genre_Prem_Movies_Family = 1) and
                                              (
                                                (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                                 vw.F_CType_Retail_ALC_Movies_Pack = 1
                                              )  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (152,    -- ### NumCompleteProgs__Genre_Prem_Movies_Horror_Thriller ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and
                                              (vw.F_Genre_Prem_Movies_Horror_Thriller = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                         when (vw.F_Genre_Prem_Movies_Horror_Thriller = 1) and
                                              (
                                                (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                                 vw.F_CType_Retail_ALC_Movies_Pack = 1
                                              )  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (153,    -- ### NumCompleteProgs__Genre_Prem_Movies_SciFi_Fantasy ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Movies) = 0      or max(acc.Movmt_DTV_Prem_Movies) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Movies = 1) and
                                              (vw.F_Genre_Prem_Movies_SciFi_Fantasy = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                         when (vw.F_Genre_Prem_Movies_SciFi_Fantasy = 1) and
                                              (
                                                (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Family = 1 or acc.Ent_HD_Sub = 1)) and
                                                 vw.F_CType_Retail_ALC_Movies_Pack = 1
                                              )  and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );


insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (154,    -- ### NumCompleteProgs__Genre_Prem_Sports_American ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_American = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (155,    -- ### NumCompleteProgs__Genre_Prem_Sports_Boxing_Wrestling ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_Boxing_Wrestling = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (156,    -- ### NumCompleteProgs__Genre_Prem_Sports_Cricket ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_Cricket = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (157,    -- ### NumCompleteProgs__Genre_Prem_Sports_Football ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_Football = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (158,    -- ### NumCompleteProgs__Genre_Prem_Sports_Golf ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_Golf = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (159,    -- ### NumCompleteProgs__Genre_Prem_Sports_Motor_Extreme ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_Motor_Extreme = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (160,    -- ### NumCompleteProgs__Genre_Prem_Sports_Rugby ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_Rugby = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (161,    -- ### NumCompleteProgs__Genre_Prem_Sports_Tennis ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_Tennis = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (162,    -- ### NumCompleteProgs__Genre_Prem_Sports_Niche_Sport ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV
       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription
         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_Retail_Sports = 1) and
                                              (vw.F_Genre_Prem_Sports_Niche_Sport = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Genres based viewing - share of viewing                                                                                              ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (163,   -- ### SOV__Genre_Non_Prem_Children ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    94, 88, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (164,   -- ### SOV__Genre_Non_Prem_Movies ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    95, 88, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (165,   -- ### SOV__Genre_Non_Prem_News_Documentaries ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    96, 88, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (166,   -- ### SOV__Genre_Non_Prem_Sports ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    97, 88, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (167,   -- ### SOV__Genre_Non_Prem_Action_SciFi ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    98, 88, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (168,   -- ### SOV__Genre_Non_Prem_Arts_Lifestyle ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    99, 88, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (169,   -- ### SOV__Genre_Non_Prem_Comedy_Game_Shows ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    100, 88, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (170,   -- ### SOV__Genre_Non_Prem_Drama_Crime ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    101, 88, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (171,   -- ### SOV__Genre_Prem_Movies_Action_Adventure ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    102, 6, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (172,   -- ### SOV__Genre_Prem_Movies_Comedy ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    103, 6, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (173,   -- ### SOV__Genre_Prem_Movies_Drama_Romance ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    104, 6, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (174,   -- ### SOV__Genre_Prem_Movies_Family ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    105, 6, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (175,   -- ### SOV__Genre_Prem_Movies_Horror_Thriller ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    106, 6, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (176,   -- ### SOV__Genre_Prem_Movies_SciFi_Fantasy ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    107, 6, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (177,   -- ### SOV__Genre_Prem_Sports_American ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    108, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (178,   -- ### SOV__Genre_Prem_Sports_Boxing_Wrestling ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    109, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (179,   -- ### SOV__Genre_Prem_Sports_Cricket ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    110, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (180,   -- ### SOV__Genre_Prem_Sports_Football ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    111, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (181,   -- ### SOV__Genre_Prem_Sports_Golf ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    112, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (182,   -- ### SOV__Genre_Prem_Sports_Motor_Extreme ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    113, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (183,   -- ### SOV__Genre_Prem_Sports_Rugby ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    114, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (184,   -- ### SOV__Genre_Prem_Sports_Tennis ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    115, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (185,   -- ### SOV__Genre_Prem_Sports_Niche_Sport ###
    'case
       when #1# = -3 or #2# = -3 or #4# = -3 or #5# = -3 then -3                                                      -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #2# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #2#                                                                                               -- Derivation
     end',
    116, 7, 0, 2, 1                             -- [Aggregation related viewing], [Package Viewing], [x], [Total viewing], [Days data returned]
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Genres based viewing - average daily number of programmes                                                                            ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (186,   -- ### AvDNumProgs__Genre_Non_Prem_Children ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    117, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (187,   -- ### AvDNumProgs__Genre_Non_Prem_Movies ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    118, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (188,   -- ### AvDNumProgs__Genre_Non_Prem_News_Documentaries ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    119, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (189,   -- ### AvDNumProgs__Genre_Non_Prem_Sports ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    120, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (190,   -- ### AvDNumProgs__Genre_Non_Prem_Action_SciFi ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    121, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (191,   -- ### AvDNumProgs__Genre_Non_Prem_Arts_Lifestyle ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    122, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (192,   -- ### AvDNumProgs__Genre_Non_Prem_Comedy_Game_Shows ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    123, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (193,   -- ### AvDNumProgs__Genre_Non_Prem_Drama_Crime ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    124, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (194,   -- ### AvDNumProgs__Genre_Prem_Movies_Action_Adventure ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    125, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (195,   -- ### AvDNumProgs__Genre_Prem_Movies_Comedy ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    126, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (196,   -- ### AvDNumProgs__Genre_Prem_Movies_Drama_Romance ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    127, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (197,   -- ### AvDNumProgs__Genre_Prem_Movies_Family ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    128, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (198,   -- ### AvDNumProgs__Genre_Prem_Movies_Horror_Thriller ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    129, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (199,   -- ### AvDNumProgs__Genre_Prem_Movies_SciFi_Fantasy ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    130, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (200,   -- ### AvDNumProgs__Genre_Prem_Sports_American ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    131, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (201,   -- ### AvDNumProgs__Genre_Prem_Sports_Boxing_Wrestling ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    132, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (202,   -- ### AvDNumProgs__Genre_Prem_Sports_Cricket ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    133, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (203,   -- ### AvDNumProgs__Genre_Prem_Sports_Football ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    134, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (204,   -- ### AvDNumProgs__Genre_Prem_Sports_Golf ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    135, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (205,   -- ### AvDNumProgs__Genre_Prem_Sports_Motor_Extreme ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    136, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (206,   -- ### AvDNumProgs__Genre_Prem_Sports_Rugby ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    137, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (207,   -- ### AvDNumProgs__Genre_Prem_Sports_Tennis ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    138, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (208,   -- ### AvDNumProgs__Genre_Prem_Sports_Niche_Sport ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    139, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Genres based viewing - average daily number of complete programmes                                                                   ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (209,   -- ### AvDNumCompleteProgs__Genre_Non_Prem_Children ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    140, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (210,   -- ### AvDNumCompleteProgs__Genre_Non_Prem_Movies ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    141, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (211,   -- ### AvDNumCompleteProgs__Genre_Non_Prem_News_Documentaries ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    142, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (212,   -- ### AvDNumCompleteProgs__Genre_Non_Prem_Sports ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    143, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (213,   -- ### AvDNumCompleteProgs__Genre_Non_Prem_Action_SciFi ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    144, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (214,   -- ### AvDNumCompleteProgs__Genre_Non_Prem_Arts_Lifestyle ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    145, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (215,   -- ### AvDNumCompleteProgs__Genre_Non_Prem_Comedy_Game_Shows ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    146, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (216,   -- ### AvDNumCompleteProgs__Genre_Non_Prem_Drama_Crime ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    147, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (217,   -- ### AvDNumCompleteProgs__Genre_Prem_Movies_Action_Adventure ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    148, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (218,   -- ### AvDNumCompleteProgs__Genre_Prem_Movies_Comedy ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    149, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (219,   -- ### AvDNumCompleteProgs__Genre_Prem_Movies_Drama_Romance ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    150, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (220,   -- ### AvDNumCompleteProgs__Genre_Prem_Movies_Family ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    151, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (221,   -- ### AvDNumCompleteProgs__Genre_Prem_Movies_Horror_Thriller ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    152, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (222,   -- ### AvDNumCompleteProgs__Genre_Prem_Movies_SciFi_Fantasy ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    153, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (223,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_American ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    154, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (224,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_Boxing_Wrestling ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    155, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (225,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_Cricket ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    156, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (226,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_Football ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    157, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (227,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_Golf ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    158, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (228,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_Motor_Extreme ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    159, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (229,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_Rugby ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    160, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (230,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_Tennis ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    161, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (231,   -- ### AvDNumCompleteProgs__Genre_Prem_Sports_Niche_Sport ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    162, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Channel viewing - total viewing duration                                                                                             ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (232,    -- ### VwDur__Channel_BBC_News ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_BBC_News = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (233,    -- ### VwDur__Channel_BBC1_BBC2_BBC3_BB4 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_BBC1_BBC2_BBC3_BB4 = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (234,    -- ### VwDur__Channel_BT_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

         else sum(case when vw.F_Channel_BT_Sports = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (235,    -- ### VwDur__Channel_CBeebies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_CBeebies = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (236,    -- ### VwDur__Channel_Channel_4 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Channel_4 = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (237,    -- ### VwDur__Channel_Channel_5 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Channel_5 = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (238,    -- ### VwDur__Channel_Dave ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Dave = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (239,    -- ### VwDur__Channel_Discovery_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Discovery_All = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (240,    -- ### VwDur__Channel_History ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_History = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (241,    -- ### VwDur__Channel_ITV_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_ITV_All = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (242,    -- ### VwDur__Channel_MTV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_MTV = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (243,    -- ### VwDur__Channel_NatGeo_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_NatGeo_All = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (244,    -- ### VwDur__Channel_Sky_1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Sky_1 = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (245,    -- ### VwDur__Channel_Sky_Atlantic ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Sky_Atlantic = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (246,    -- ### VwDur__Channel_Sky_Living ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Sky_Living = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (247,    -- ### VwDur__Channel_Sky_News ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Sky_News = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (248,    -- ### VwDur__Channel_Sky_Sports_1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else sum(case when vw.F_Channel_Sky_Sports_1 = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (249,    -- ### VwDur__Channel_Sky_Sports_2 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else sum(case when vw.F_Channel_Sky_Sports_2 = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (250,    -- ### VwDur__Channel_Sky_Sports_F1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else sum(case when vw.F_Channel_Sky_Sports_F1 = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (251,    -- ### VwDur__Channel_Watch ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else sum(case when vw.F_Channel_Watch = 1 then coalesce(vw.Instance_Duration, 0) else 0 end)       -- Relevant viewing only
     end'
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Channel viewing - number of programmes watched                                                                                       ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (252,    -- ### NumProgs__Channel_BBC_News ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_BBC_News = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (253,    -- ### NumProgs__Channel_BBC1_BBC2_BBC3_BB4 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_BBC1_BBC2_BBC3_BB4 = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (254,    -- ### NumProgs__Channel_BT_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_BT_Sports = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (255,    -- ### NumProgs__Channel_CBeebies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_CBeebies = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (256,    -- ### NumProgs__Channel_Channel_4 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Channel_4 = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (257,    -- ### NumProgs__Channel_Channel_5 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Channel_5 = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (258,    -- ### NumProgs__Channel_Dave ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Dave = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (259,    -- ### NumProgs__Channel_Discovery_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Discovery_All = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (260,    -- ### NumProgs__Channel_History ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_History = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (261,    -- ### NumProgs__Channel_ITV_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_ITV_All = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (262,    -- ### NumProgs__Channel_MTV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_MTV = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (263,    -- ### NumProgs__Channel_NatGeo_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_NatGeo_All = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (264,    -- ### NumProgs__Channel_Sky_1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_1 = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (265,    -- ### NumProgs__Channel_Sky_Atlantic ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Atlantic = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (266,    -- ### NumProgs__Channel_Sky_Living ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Living = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (267,    -- ### NumProgs__Channel_Sky_News ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_News = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (268,    -- ### NumProgs__Channel_Sky_Sports_1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Sports_1 = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (269,    -- ### NumProgs__Channel_Sky_Sports_2 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Sports_2 = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (270,    -- ### NumProgs__Channel_Sky_Sports_F1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Sports_F1 = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (271,    -- ### NumProgs__Channel_Watch ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Watch = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Channel viewing - number of complete programmes watched                                                                              ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (272,    -- ### NumCompleteProgs__Channel_BBC_News ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_BBC_News = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (273,    -- ### NumCompleteProgs__Channel_BBC1_BBC2_BBC3_BB4 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_BBC1_BBC2_BBC3_BB4 = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (274,    -- ### NumCompleteProgs__Channel_BT_Sports ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_BT_Sports = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (275,    -- ### NumCompleteProgs__Channel_CBeebies ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_CBeebies = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (276,    -- ### NumCompleteProgs__Channel_Channel_4 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Channel_4 = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (277,    -- ### NumCompleteProgs__Channel_Channel_5 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Channel_5 = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (278,    -- ### NumCompleteProgs__Channel_Dave ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Dave = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (279,    -- ### NumCompleteProgs__Channel_Discovery_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Discovery_All = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (280,    -- ### NumCompleteProgs__Channel_History ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_History = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (281,    -- ### NumCompleteProgs__Channel_ITV_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_ITV_All = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (282,    -- ### NumCompleteProgs__Channel_MTV ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_MTV = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (283,    -- ### NumCompleteProgs__Channel_NatGeo_All ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_NatGeo_All = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (284,    -- ### NumCompleteProgs__Channel_Sky_1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_1 = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (285,    -- ### NumCompleteProgs__Channel_Sky_Atlantic ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Atlantic = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (286,    -- ### NumCompleteProgs__Channel_Sky_Living ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Living = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (287,    -- ### NumCompleteProgs__Channel_Sky_News ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_News = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (288,    -- ### NumCompleteProgs__Channel_Sky_Sports_1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Sports_1 = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (289,    -- ### NumCompleteProgs__Channel_Sky_Sports_2 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Sports_2 = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (290,    -- ### NumCompleteProgs__Channel_Sky_Sports_F1 ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Prem_Sports) = 0      or max(acc.Movmt_DTV_Prem_Sports) = 1 then -3                       -- Active/No movement relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Sky_Sports_F1 = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation) values (291,    -- ### NumCompleteProgs__Channel_Watch ###
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_Channel_Watch = 1) and
                                              (
                                                (vw.F_Genre_Sport = 1 and vw.Prog_Instance_Broadcast_Duration >= 5400 and vw.Prog_Instance_Viewed_Duration >= 3600) or
                                                ( (vw.F_Genre_Sport = 0 or vw.Prog_Instance_Broadcast_Duration < 5400) and 1.0 * vw.Prog_Instance_Viewed_Duration / vw.Prog_Instance_Broadcast_Duration >= 0.6)
                                              ) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of complete programme viewings
     end'
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Channel viewing - share of viewing                                                                                                   ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (292,   -- ### SOV__Channel_BBC_News ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    232, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (293,   -- ### SOV__Channel_BBC1_BBC2_BBC3_BB4 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    233, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (294,   -- ### SOV__Channel_BT_Sports ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    234, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (295,   -- ### SOV__Channel_CBeebies ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    235, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (296,   -- ### SOV__Channel_Channel_4 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    236, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (297,   -- ### SOV__Channel_Channel_5 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    237, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (298,   -- ### SOV__Channel_Dave ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    238, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (299,   -- ### SOV__Channel_Discovery_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    239, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (300,   -- ### SOV__Channel_History ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    240, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (301,   -- ### SOV__Channel_ITV_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    241, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (302,   -- ### SOV__Channel_MTV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    242, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (303,   -- ### SOV__Channel_NatGeo_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    243, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (304,   -- ### SOV__Channel_Sky_1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    244, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (305,   -- ### SOV__Channel_Sky_Atlantic ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    245, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (306,   -- ### SOV__Channel_Sky_Living ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    246, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (307,   -- ### SOV__Channel_Sky_News ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    247, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (308,   -- ### SOV__Channel_Sky_Sports_1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    248, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (309,   -- ### SOV__Channel_Sky_Sports_2 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    249, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (310,   -- ### SOV__Channel_Sky_Sports_F1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    250, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (311,   -- ### SOV__Channel_Watch ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# / #5# < 1 then -1                                                                                     -- "Did not watch" (Average Aggregation Daily Viewing < 1)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #4#                                                                                               -- Derivation
     end',
    251, 0, 0, 2, 1                             -- [Aggregation related viewing], [x], [x], [Total viewing], [Days data returned]
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Genres based viewing - average daily number of programmes                                                                            ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (312,   -- ### AvDNumProgs__Channel_BBC_News ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    252, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (313,   -- ### AvDNumProgs__Channel_BBC1_BBC2_BBC3_BB4 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    253, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (314,   -- ### AvDNumProgs__Channel_BT_Sports ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    254, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (315,   -- ### AvDNumProgs__Channel_CBeebies ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    255, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (316,   -- ### AvDNumProgs__Channel_Channel_4 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    256, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (317,   -- ### AvDNumProgs__Channel_Channel_5 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    257, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (318,   -- ### AvDNumProgs__Channel_Dave ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    258, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (319,   -- ### AvDNumProgs__Channel_Discovery_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    259, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (320,   -- ### AvDNumProgs__Channel_History ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    260, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (321,   -- ### AvDNumProgs__Channel_ITV_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    261, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (322,   -- ### AvDNumProgs__Channel_MTV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    262, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (323,   -- ### AvDNumProgs__Channel_NatGeo_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    263, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (324,   -- ### AvDNumProgs__Channel_Sky_1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    264, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (325,   -- ### AvDNumProgs__Channel_Sky_Atlantic ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    265, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (326,   -- ### AvDNumProgs__Channel_Sky_Living ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    266, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (327,   -- ### AvDNumProgs__Channel_Sky_News ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    267, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (328,   -- ### AvDNumProgs__Channel_Sky_Sports_1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    268, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (329,   -- ### AvDNumProgs__Channel_Sky_Sports_2 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    269, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (330,   -- ### AvDNumProgs__Channel_Sky_Sports_F1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    270, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (331,   -- ### AvDNumProgs__Channel_Watch ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    271, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );
commit;


  -- ######################################################################################################################################################
  -- ######## Genres based viewing - average daily number of complete programmes                                                                   ########
  -- ######################################################################################################################################################
insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (332,   -- ### AvDNumCompleteProgs__Channel_BBC_News ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    272, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (333,   -- ### AvDNumCompleteProgs__Channel_BBC1_BBC2_BBC3_BB4 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    273, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (334,   -- ### AvDNumCompleteProgs__Channel_BT_Sports ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    274, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (335,   -- ### AvDNumCompleteProgs__Channel_CBeebies ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    275, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (336,   -- ### AvDNumCompleteProgs__Channel_Channel_4 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    276, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (337,   -- ### AvDNumCompleteProgs__Channel_Channel_5 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    277, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (338,   -- ### AvDNumCompleteProgs__Channel_Dave ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    278, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (339,   -- ### AvDNumCompleteProgs__Channel_Discovery_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    279, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (340,   -- ### AvDNumCompleteProgs__Channel_History ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    280, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (341,   -- ### AvDNumCompleteProgs__Channel_ITV_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    281, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (342,   -- ### AvDNumCompleteProgs__Channel_MTV ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    282, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (343,   -- ### AvDNumCompleteProgs__Channel_NatGeo_All ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    283, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (344,   -- ### AvDNumCompleteProgs__Channel_Sky_1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    284, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (345,   -- ### AvDNumCompleteProgs__Channel_Sky_Atlantic ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    285, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (346,   -- ### AvDNumCompleteProgs__Channel_Sky_Living ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    286, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (347,   -- ### AvDNumCompleteProgs__Channel_Sky_News ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    287, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (348,   -- ### AvDNumCompleteProgs__Channel_Sky_Sports_1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    288, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (349,   -- ### AvDNumCompleteProgs__Channel_Sky_Sports_2 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    289, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (350,   -- ### AvDNumCompleteProgs__Channel_Sky_Sports_F1 ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    290, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );

insert into VAggr_Meta_Aggr_Definitions (Aggregation_Key, Derivation, Metric_1, Metric_2, Metric_3, Metric_4, Metric_5)
                                                                      values (351,   -- ### AvDNumCompleteProgs__Channel_Watch ###
    'case
       when #1# = -3 or #4# = -3 or #5# = -3 then -3                                                                  -- "Not eligible"
       when 1.0 * #5# / prd.Period_Num_Days < 0.5 then -2                                                             -- "Excluded" (data return ratio <50%)
       when #4# / #5# < 10 then -1                                                                                    -- "Did not watch" (Average Total Daily Viewing < 10)
       when #1# = 0 then -1                                                                                           -- "Did not watch" (0 programmes)
       when #4# = 0 then -2                                                                                           -- "Excluded" - #DIV/0 error handling, should not happen at this stage
         else #1# / #5#                                                                                               -- Derivation
     end',
    291, 0, 0, 2, 1                             -- [Aggregation related number of programmes], [x], [x], [Total viewing], [Days data returned]
                                                                             );
commit;




/*
update VAggr_Meta_Aggr_Definitions
set Derivation =
    'case
       when max(acc.Ent_DTV_Sub) = 0              or max(acc.Movmt_DTV_Sub) = 1 then -3                               -- Active/No movement for DTV

       when max(acc.Ent_DTV_Pack_Original) = 0        and max(acc.Ent_DTV_Pack_Variety) = 0  and
            max(acc.Ent_DTV_Pack_Family) = 0 then -3                                                          -- No active package

       when ( max(acc.Ent_DTV_Pack_Original) = 1             and max(acc.Movmt_DTV_Pack_Original) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Variety) = 1       and max(acc.Movmt_DTV_Pack_Variety) = 1 ) or
            ( max(acc.Ent_DTV_Pack_Family) = 1  and max(acc.Movmt_DTV_Pack_Family) = 1 ) then -3      -- Movement in relevant package/subscription

         else coalesce( count(distinct case
                                         when (vw.Prog_Instance_Viewed_Duration < 180) then null
                                         when (vw.F_CType_O_V_F_FTA = 1) then vw.Prog_Instance_Id
                                           else null
                                       end),
                       0 )                                                                                            -- Count number of 3min+ programme viewings
     end'
where Aggregation_Key = 49
;

*/



