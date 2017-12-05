/*###############################################################################
# Created on:   29/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation - clean up redundant tables. It
#               supposed to be run at the end, post QA and results checks
#               (Phase 2 data)
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
# 29/08/2012  SBE   v01 - initial version
# 05/11/2012  SBE   v02 - update forced by main script changes (v04)
#
###############################################################################*/

if object_id('Minute_Attribution_Cleanup_v01') is not null then drop procedure Minute_Attribution_Cleanup_v01 endif;
commit;

create procedure Minute_Attribution_Cleanup_v01
as
begin
 -- #### (procedure start) #####

        -- ##############################################################################################################
        -- ##### STEP 1.1 - cleaning up                                                                             #####
        -- ##############################################################################################################
      if object_id('VESPA_MinAttr_Phase2_01_Viewing_Delta')                 is not null drop table VESPA_MinAttr_Phase2_01_Viewing_Delta
      if object_id('VESPA_MinAttr_Phase2_02_All_Live_Viewing')              is not null drop table VESPA_MinAttr_Phase2_02_All_Live_Viewing
      if object_id('VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min')       is not null drop table VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
      if object_id('VESPA_MinAttr_Phase2_tmp_Minute_Ranks')                 is not null drop table VESPA_MinAttr_Phase2_tmp_Minute_Ranks
      if object_id('VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration')       is not null drop table VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration
      if object_id('VESPA_MinAttr_Phase2_tmp_Longest_Group')                is not null drop table VESPA_MinAttr_Phase2_tmp_Longest_Group
      if object_id('VESPA_MinAttr_Phase2_tmp_Longest_Group_Max_Duration')   is not null drop table VESPA_MinAttr_Phase2_tmp_Longest_Group_Max_Duration
      if object_id('VESPA_MinAttr_Phase2_tmp_First_Longest_Ranks')          is not null drop table VESPA_MinAttr_Phase2_tmp_First_Longest_Ranks
      if object_id('VESPA_MinAttr_Phase2_tmp_Minute_Summaries')             is not null drop table VESPA_MinAttr_Phase2_tmp_Minute_Summaries
      if object_id('VESPA_MinAttr_Phase2_tmp_Dates_Processed')              is not null drop table VESPA_MinAttr_Phase2_tmp_Dates_Processed
      if object_id('VESPA_MinAttr_Phase2_tmp_Surf_Constituents')            is not null drop table VESPA_MinAttr_Phase2_tmp_Surf_Constituents
      if object_id('VESPA_MinAttr_Phase2_tmp_Surf_Constituents_Front_Min')  is not null drop table VESPA_MinAttr_Phase2_tmp_Surf_Constituents_Front_Min

 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################


