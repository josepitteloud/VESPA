/*###############################################################################
# Created on:   11/11/2013
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  CIA Phase 2 (Sky Wifi) - creating bins
#
# List of steps:
#               STEP 1 - Binning
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables required:
#     - VAggrAnal_SkyWifi_Account_Summary
#
# => Procedures required:
#     - VAggrAnal_Binning_Setup
#     - VAggrAnal_Binning
#     - VAggrAnal_Binning_Cleanup
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 11/11/2013  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### STEP 1 - Binning                                                                                   #####
  -- ##############################################################################################################
  -- Reset environment
execute VAggrAnal_Binning_Setup;

  -- Populate the fact table for binning - FREQUENCY
insert into VAggrAnal_Fact (Aggregation_Key, Metric_Group_Key, Account_Number, Metric_Value)
  select
        1,
        null,
        Account_Number,
        case
          when Total_Wifi_Active_Days = 0 or Bytes_Transferred_Total = 0 then -1
            else Total_Wifi_Active_Days
        end
    from VAggrAnal_SkyWifi_Account_Summary
   where Days_Wifi_Entitlement >= 31;
commit;

  -- Populate the fact table for binning - USAGE
insert into VAggrAnal_Fact (Aggregation_Key, Metric_Group_Key, Account_Number, Metric_Value)
  select
        2,
        null,
        Account_Number,
        case
          when Total_Wifi_Active_Days = 0 or Bytes_Transferred_Total = 0 then -1
            else 1.0 * Bytes_Transferred_Total / Total_Wifi_Active_Days
        end
    from VAggrAnal_SkyWifi_Account_Summary
   where Days_Wifi_Entitlement >= 31;
commit;



  -- Run binning
execute VAggrAnal_Binning 1, 'Sky Wifi - frequency', 0, 5, '', null;       -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 2, 'Sky Wifi - usage', 0, 7, '', null;           -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]

  -- Clean up
--execute VAggrAnal_Binning_Cleanup;


  -- ##############################################################################################################
  -- ##############################################################################################################





