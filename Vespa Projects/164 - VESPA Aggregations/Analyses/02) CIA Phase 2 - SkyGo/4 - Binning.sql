/*###############################################################################
# Created on:   19/11/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Binning rules
#
# List of steps:
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Procedures required:
#     - VAggrAnal_Binning_Setup
#     - VAggrAnal_Binning
#     - VAggrAnal_Binning_Cleanup
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 19/11/2013  SBE   Initial version
#
###############################################################################*/


execute VAggrAnal_Binning 1, 'All SkyGo',           0,  7, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 2, 'All Sports',          0,  7, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 3, 'All Movies',          0,  2, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 4, 'All Non-premium',     0,  5, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 5, 'All Linear',          0,  7, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 6, 'All VOD',             0,  5, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
execute VAggrAnal_Binning 7, 'All DL',              0,  3, '', null;         -- [@parAggregationKey], [@parAggregationName], [@parGroupScaled], [@parNumBins], [@parRefreshIdentifier], [@parBuildId]
commit;



  -- ##############################################################################################################








