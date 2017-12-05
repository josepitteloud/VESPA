/*###############################################################################
# Created on:   26/04/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Aug Enhancement process - clean up procedure to remove temporary
#               tables
#
# List of steps:
#
# To do:
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 26/04/2013  SBE   v01 - initial version
#
#
###############################################################################*/


if object_id('aug_AugEnhancement_Cleanup_v01') is not null then drop procedure aug_AugEnhancement_Cleanup_v01 endif;
commit;

create procedure aug_AugEnhancement_Cleanup_v01
as
begin
 -- #### (procedure start) #####

      if object_id('VESPA_AugEnh_tmp_Source_Snapshot_Aggr') is not null drop table VESPA_AugEnh_tmp_Source_Snapshot_Aggr
      if object_id('VESPA_AugEnh_tmp_Source_Snapshot_Full') is not null drop table VESPA_AugEnh_tmp_Source_Snapshot_Full
      if object_id('VESPA_AugEnh_tmp_Aug_Data') is not null drop table VESPA_AugEnh_tmp_Aug_Data
      drop view if exists Vespa_AugEnhancement_tmp_Source_Data

 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



