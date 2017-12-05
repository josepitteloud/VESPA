/*###############################################################################
# Created on:   06/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation - clean up redundant tables. It
#               supposed to be run at the end, post QA and results checks
#               (Phase 1 data)
#
# List of steps:
#               STEP 1.1 - cleaning up
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2012  SBE   v01 - initial version
# 20/08/2012  SBE   v02 - adjusted to meet requirements of C02 v02
# 23/08/2012  SBE   v02 - adjusted to follow changes in C02 v03
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### STEP 1.1 - cleaning up                                                                             #####
  -- ##############################################################################################################
if object_id('VESPA_MinAttr_Phase1_01_Viewing_Delta') is not null then drop table VESPA_MinAttr_Phase1_01_Viewing_Delta endif;
if object_id('VESPA_MinAttr_Phase1_02_All_Live_Viewing') is not null then drop table VESPA_MinAttr_Phase1_02_All_Live_Viewing endif;
if object_id('VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min') is not null then drop table VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min endif;
if object_id('VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records') is not null then drop table VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records endif;
if object_id('VESPA_MinAttr_Phase1_tmp_Minute_Ranks') is not null then drop table VESPA_MinAttr_Phase1_tmp_Minute_Ranks endif;
if object_id('VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration') is not null then drop table VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration endif;
if object_id('VESPA_MinAttr_Phase1_tmp_First_Longest_Ranks') is not null then drop table VESPA_MinAttr_Phase1_tmp_First_Longest_Ranks endif;
if object_id('VESPA_MinAttr_Phase1_tmp_Surf_Constituents') is not null then drop table VESPA_MinAttr_Phase1_tmp_Surf_Constituents endif;
if object_id('VESPA_MinAttr_Phase1_tmp_Surf_Constituents_Front_Min') is not null then drop table VESPA_MinAttr_Phase1_tmp_Surf_Constituents_Front_Min endif;
drop view if exists VESPA_MinAttr_Phase1_AugmentedTable


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



