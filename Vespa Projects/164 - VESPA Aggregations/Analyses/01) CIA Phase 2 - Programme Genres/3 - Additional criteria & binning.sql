/*###############################################################################
# Created on:   08/11/2013
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  CIA Phase 2 (genres) - additional data manipulation steps & creating
#               bins
#
# List of steps:
#               STEP 1 - Applying rules for additional segments
#               STEP 2 - Binning
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables required:
#     - VAggr_03_Account_Metrics_PH2_GENRES
#
# => Procedures required:
#     - VAggrAnal_Binning_Setup
#     - VAggrAnal_Binning
#     - VAggrAnal_Binning_Cleanup
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 08/11/2013  SBE   Initial version
#
###############################################################################*/

  -- ##############################################################################################################
  -- ##### STEP 1 - Applying rules for additional segments:                                                   #####
  -- #####            Not eligible                                                                            #####
  -- #####            Excluded                                                                                #####
  -- #####            Did not watch segment                                                                   #####
  -- ##############################################################################################################
  -- Apply rules for "Excluded"
update VAggr_03_Account_Metrics_PH2_GENRES
   set Account_Status = -2
 where Account_Status > -3
   and 1.0 * Days_Data_Returned / Days_Period < 0.5;
commit;

  -- Apply rules for "Did not watch" (output metric only)
update VAggr_03_Account_Metrics_PH2_GENRES base
   set Account_Status = -1
 where (
        Avg_Daily_Viewing_Dur < 10
        or
        Avg_Daily_Aggr_Viewing_Dur < 1
       )
   and Account_Status > -2;
commit;


  -- ##############################################################################################################
  -- ##### STEP 1 - Binning                                                                                   #####
  -- ##############################################################################################################
  -- Reset environment
execute VAggrAnal_Binning_Setup;

  -- Populate the fact table for binning
insert into VAggrAnal_Fact (Aggregation_Key, Metric_Group_Key, Account_Number, Metric_Value)
  select
        Aggregation_Id,
        null,
        Account_Number,
        case
          when Account_Status < 0 then Account_Status
            else Share_Of_Viewing
        end
    from VAggr_03_Account_Metrics_PH2_GENRES
   where Aggregation_Id >= 10;
commit;

  -- Populate account attributes table for binning
insert into VAggrAnal_Account_Attributes (Account_Number, Median_Scaling_Weight)
select
      Account_Number,
      Median_Scaling_Weight
  from VESPA_Shared.Aggr_Account_Attributes
 where Period_Key = 5;
commit;


  -- Run binning
execute VAggrAnal_Binning 10, 'E - Children',                         1,  4, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 11, 'E - Movies',                           1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 12, 'E - News & Documentaries',             1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 13, 'E - Sports',                           1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 14, 'E - Action & SciFi',                   1,  5, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 15, 'E - Arts & Lifestyle',                 1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 16, 'E - Comedy & Gameshow',                1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 17, 'E - Drama & Crime',                    1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]

execute VAggrAnal_Binning 20, 'EE - Children',                        1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 21, 'EE - Movies',                          1,  8, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 22, 'EE - News & Documentaries',            1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 23, 'EE - Sports',                          1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 24, 'EE - Action & SciFi',                  1,  4, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 25, 'EE - Arts & Lifestyle',                1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 26, 'EE - Comedy & Gameshow',               1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 27, 'EE - Drama & Crime',                   1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]

execute VAggrAnal_Binning 30, 'EE+ - Children',                       1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 31, 'EE+ - Movies',                         1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 32, 'EE+ - News & Documentaries',           1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 33, 'EE+ - Sports',                         1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 34, 'EE+ - Action & SciFi',                 1,  4, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 35, 'EE+ - Arts & Lifestyle',               1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 36, 'EE+ - Comedy & Gameshow',              1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 37, 'EE+ - Drama & Crime',                  1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]

execute VAggrAnal_Binning 40, 'Premium Movies - Action & Adventure',  1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 41, 'Premium Movies - Comedy',              1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 42, 'Premium Movies - Drama & Romance',     1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 43, 'Premium Movies - Family',              1,  8, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 44, 'Premium Movies - Horror & Thriller',   1,  8, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 45, 'Premium Movies - SciFi & Fantasy',     1,  8, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]

execute VAggrAnal_Binning 50, 'Premium Sports - American',            1,  4, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 51, 'Premium Sports - Boxing & Wrestling',  1,  5, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 52, 'Premium Sports - Cricket',             1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 53, 'Premium Sports - Football',            1, 10, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 54, 'Premium Sports - Golf',                1,  5, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 55, 'Premium Sports - Motor & Extreme',     1,  5, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 56, 'Premium Sports - Rugby',               1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 57, 'Premium Sports - Tennis',              1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 58, 'Premium Sports - Niche Sport',         1,  6, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]

  -- Clean up
--execute VAggrAnal_Binning_Cleanup;


  -- ##############################################################################################################
  -- ##############################################################################################################










