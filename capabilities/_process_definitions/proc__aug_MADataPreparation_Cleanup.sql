/*###############################################################################
# Created on:   24/04/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  MA Data Preparation - clean up procedure to remove temporary
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
# 24/04/2013  SBE   v01 - initial version
#
#
###############################################################################*/


if object_id('aug_MADataPreparation_Cleanup_v01') is not null then drop procedure aug_MADataPreparation_Cleanup_v01 endif;
commit;


create procedure aug_MADataPreparation_Cleanup_v01
      @parProcessDate           date = null
as
begin
 -- #### (procedure start) #####

      declare @varSql                         varchar(10000)      -- SQL string for dynamic SQL execution

      set @varSql = '
                    if object_id(''Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence'') is not null drop table Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence
                    if object_id(''Vespa_MADataPrep_tmp_Aug_' || dateformat(@parProcessDate, 'yyyymmdd') || ''') is not null drop table Vespa_MADataPrep_tmp_Aug_' || dateformat(@parProcessDate, 'yyyymmdd') || '
                    drop view if exists Vespa_MADataPrep_tmp_Source_Data
                    '
      execute(@varSql)
      commit


 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



