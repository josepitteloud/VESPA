/*###############################################################################
# Created on:   05/11/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Filtering and binning preparation
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - creating aggregations
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VAggrAnal_SkyGo_Account_Attributes
#     - VAggrAnal_SkyGo_Usage_Summary
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 05/11/2013  SBE   Initial version
# 19/11/2013  SBE   Change of definitions/requirements - script adjustment
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### STEP 0.1 - preparing environment                                                                   #####
  -- ##############################################################################################################


  -- ##############################################################################################################
  -- ##### STEP 1.0 - creating aggregations                                                                   #####
  -- ##############################################################################################################

-- ##############################################################################################################
-- ##### Account attributes                                                                                 #####
-- ##############################################################################################################



-- ##############################################################################################################
-- ##### ALL                                                                                                #####
-- ##############################################################################################################
  -- ##### 1 - All #####
insert into bednaszs.VAggrAnal_Fact (Aggregation_Key, Account_Number, Metric_Value)
  select
        1,
        acc.Account_Number,
        case

          when acc.Ent_DTV_Sub = 0 or acc.Movmt_DTV_Sub = 1 then -3                                               -- Inactive or movement for DTV

          when acc.Ent_DTV_Pack_Ent = 0 and acc.Ent_DTV_Pack_Ent_Extra = 0 and
               acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 then -3                                                        -- No active package

          when ( acc.Ent_DTV_Pack_Ent = 1 and acc.Movmt_DTV_Pack_Ent = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra = 1 and acc.Movmt_DTV_Pack_Ent_Extra = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 and acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 ) or
               ( acc.Ent_SkyGo_Extra_Sub = 1 and acc.Movmt_SkyGo_Extra_Sub = 1 ) then -3                          -- Movement in relevant package/subscription

          when usg.Account_Number is null or usg.Vol_All = 0 then -1                                              -- No viewing/usage - "Did not watch"

            else usg.Vol_All

        end
    from VAggrAnal_SkyGo_Account_Attributes acc
            left join VAggrAnal_SkyGo_Usage_Summary usg   on acc.Account_Number = usg.Account_Number
   where acc.Account_Type = 'Standard';
commit;


  -- ##### 2 - All_Sports #####
insert into bednaszs.VAggrAnal_Fact (Aggregation_Key, Account_Number, Metric_Value)
  select
        2,
        acc.Account_Number,
        case

          when acc.Ent_DTV_Sub = 0 or acc.Movmt_DTV_Sub = 1 then -3                                               -- Inactive or movement for DTV

          when acc.Ent_DTV_Pack_Ent = 0 and acc.Ent_DTV_Pack_Ent_Extra = 0 and
               acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 then -3                                                        -- No active package

          when acc.Ent_DTV_Prem_Sports = 0 then -3                                                                -- No active Premium package

          when ( acc.Ent_DTV_Pack_Ent = 1 and acc.Movmt_DTV_Pack_Ent = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra = 1 and acc.Movmt_DTV_Pack_Ent_Extra = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 and acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 ) or

               ( acc.Ent_DTV_Prem_Sports = 1 and acc.Movmt_DTV_Prem_Sports = 1 ) or

               ( acc.Ent_SkyGo_Extra_Sub = 1 and acc.Movmt_SkyGo_Extra_Sub = 1 ) then -3                          -- Movement in relevant package/subscription

          when usg.Account_Number is null or usg.Vol_All_Sports = 0 or usg.Vol_All = 0 then -1                    -- No viewing/usage - "Did not watch"

            else 1.0 * usg.Vol_All_Sports

        end
    from VAggrAnal_SkyGo_Account_Attributes acc
            left join VAggrAnal_SkyGo_Usage_Summary usg   on acc.Account_Number = usg.Account_Number
   where acc.Account_Type = 'Standard';
commit;


  -- ##### 3 - All_Movies #####
insert into bednaszs.VAggrAnal_Fact (Aggregation_Key, Account_Number, Metric_Value)
  select
        3,
        acc.Account_Number,
        case

          when acc.Ent_DTV_Sub = 0 or acc.Movmt_DTV_Sub = 1 then -3                                               -- Inactive or movement for DTV

          when acc.Ent_DTV_Pack_Ent = 0 and acc.Ent_DTV_Pack_Ent_Extra = 0 and
               acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 then -3                                                        -- No active package

          when acc.Movmt_DTV_Prem_Movies = 0 then -3                                                              -- No active Premium package

          when ( acc.Ent_DTV_Pack_Ent = 1 and acc.Movmt_DTV_Pack_Ent = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra = 1 and acc.Movmt_DTV_Pack_Ent_Extra = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 and acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 ) or

               ( acc.Movmt_DTV_Prem_Movies = 1 and acc.Movmt_DTV_Prem_Movies = 1 ) or

               ( acc.Ent_SkyGo_Extra_Sub = 1 and acc.Movmt_SkyGo_Extra_Sub = 1 ) then -3                          -- Movement in relevant package/subscription

          when usg.Account_Number is null or usg.Vol_All_Movies = 0 or usg.Vol_All = 0 then -1                    -- No viewing/usage - "Did not watch"

            else 1.0 * usg.Vol_All_Movies

        end
    from VAggrAnal_SkyGo_Account_Attributes acc
            left join VAggrAnal_SkyGo_Usage_Summary usg   on acc.Account_Number = usg.Account_Number
   where acc.Account_Type = 'Standard';
commit;


  -- ##### 4 - All_Non_Premium #####
insert into bednaszs.VAggrAnal_Fact (Aggregation_Key, Account_Number, Metric_Value)
  select
        4,
        acc.Account_Number,
        case

          when acc.Ent_DTV_Sub = 0 or acc.Movmt_DTV_Sub = 1 then -3                                               -- Inactive or movement for DTV

          when acc.Ent_DTV_Pack_Ent = 0 and acc.Ent_DTV_Pack_Ent_Extra = 0 and
               acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 then -3                                                        -- No active package

          when ( acc.Ent_DTV_Pack_Ent = 1 and acc.Movmt_DTV_Pack_Ent = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra = 1 and acc.Movmt_DTV_Pack_Ent_Extra = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 and acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 ) or

               ( acc.Ent_SkyGo_Extra_Sub = 1 and acc.Movmt_SkyGo_Extra_Sub = 1 ) then -3                          -- Movement in relevant package/subscription

          when usg.Account_Number is null or usg.Vol_All_Non_Premium = 0 or usg.Vol_All = 0 then -1               -- No viewing/usage - "Did not watch"

            else 1.0 * usg.Vol_All_Non_Premium

        end
    from VAggrAnal_SkyGo_Account_Attributes acc
            left join VAggrAnal_SkyGo_Usage_Summary usg   on acc.Account_Number = usg.Account_Number
   where acc.Account_Type = 'Standard';
commit;



-- ##############################################################################################################
-- ##### LINEAR                                                                                             #####
-- ##############################################################################################################
  -- ##### 5 - Linear #####
insert into bednaszs.VAggrAnal_Fact (Aggregation_Key, Account_Number, Metric_Value)
  select
        5,
        acc.Account_Number,
        case

          when acc.Ent_DTV_Sub = 0 or acc.Movmt_DTV_Sub = 1 then -3                                               -- Inactive or movement for DTV

          when acc.Ent_DTV_Pack_Ent = 0 and acc.Ent_DTV_Pack_Ent_Extra = 0 and
               acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 then -3                                                        -- No active package

          when ( acc.Ent_DTV_Pack_Ent = 1 and acc.Movmt_DTV_Pack_Ent = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra = 1 and acc.Movmt_DTV_Pack_Ent_Extra = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 and acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 ) or
               ( acc.Ent_SkyGo_Extra_Sub = 1 and acc.Movmt_SkyGo_Extra_Sub = 1 ) then -3                          -- Movement in relevant package/subscription

          when usg.Account_Number is null or usg.Vol_Linear = 0 then -1                                           -- No viewing/usage - "Did not watch"

            else usg.Vol_Linear

        end
    from VAggrAnal_SkyGo_Account_Attributes acc
            left join VAggrAnal_SkyGo_Usage_Summary usg   on acc.Account_Number = usg.Account_Number
   where acc.Account_Type = 'Standard';
commit;


-- ##############################################################################################################
-- ##### VOD                                                                                                #####
-- ##############################################################################################################
  -- ##### 6 - VOD #####
insert into bednaszs.VAggrAnal_Fact (Aggregation_Key, Account_Number, Metric_Value)
  select
        6,
        acc.Account_Number,
        case

          when acc.Ent_DTV_Sub = 0 or acc.Movmt_DTV_Sub = 1 then -3                                               -- Inactive or movement for DTV

          when acc.Ent_DTV_Pack_Ent = 0 and acc.Ent_DTV_Pack_Ent_Extra = 0 and
               acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 then -3                                                        -- No active package

          when ( acc.Ent_DTV_Pack_Ent = 1 and acc.Movmt_DTV_Pack_Ent = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra = 1 and acc.Movmt_DTV_Pack_Ent_Extra = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 and acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 ) or
               ( acc.Ent_SkyGo_Extra_Sub = 1 and acc.Movmt_SkyGo_Extra_Sub = 1 ) then -3                          -- Movement in relevant package/subscription

          when usg.Account_Number is null or usg.Vol_VOD = 0 then -1                                              -- No viewing/usage - "Did not watch"

            else usg.Vol_VOD

        end
    from VAggrAnal_SkyGo_Account_Attributes acc
            left join VAggrAnal_SkyGo_Usage_Summary usg   on acc.Account_Number = usg.Account_Number
   where acc.Account_Type = 'Standard';
commit;



-- ##############################################################################################################
-- ##### DL                                                                                                #####
-- ##############################################################################################################
  -- ##### 7 - DL #####
insert into bednaszs.VAggrAnal_Fact (Aggregation_Key, Account_Number, Metric_Value)
  select
        7,
        acc.Account_Number,
        case

          when acc.Ent_DTV_Sub = 0 or acc.Movmt_DTV_Sub = 1 then -3                                               -- Inactive or movement for DTV

          when acc.Ent_DTV_Pack_Ent = 0 and acc.Ent_DTV_Pack_Ent_Extra = 0 and
               acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 then -3                                                        -- No active package

          when acc.Ent_SkyGo_Extra_Sub = 0 then -3                                                                -- No active package

          when ( acc.Ent_DTV_Pack_Ent = 1 and acc.Movmt_DTV_Pack_Ent = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra = 1 and acc.Movmt_DTV_Pack_Ent_Extra = 1 ) or
               ( acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 and acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 ) or
               ( acc.Ent_SkyGo_Extra_Sub = 1 and acc.Movmt_SkyGo_Extra_Sub = 1 ) then -3                          -- Movement in relevant package/subscription

          when usg.Account_Number is null or usg.Vol_DL = 0 then -1                                              -- No viewing/usage - "Did not watch"

            else usg.Vol_DL

        end
    from VAggrAnal_SkyGo_Account_Attributes acc
            left join VAggrAnal_SkyGo_Usage_Summary usg   on acc.Account_Number = usg.Account_Number
   where acc.Account_Type = 'Standard';
commit;



  -- ##############################################################################################################








