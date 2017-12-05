/*###############################################################################
# Created on:   18/09/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - metadata information creation:
#               Aggr_Metric_Group_Dim
#
#               (updated for historical purposes only, not to be run unless all
#                data has to be recreated from scratch)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VESAP_Shared.Aggr_Metric_Group_Dim
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 18/09/2013  SBE   Initial version
#
###############################################################################*/



--truncate table VESPA_Shared.Aggr_Metric_Group_Dim;
insert into VESPA_Shared.Aggr_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding, High_Level_Banding) values (1, 'Not eligible', 'Not eligible', 'Not eligible');
insert into VESPA_Shared.Aggr_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding, High_Level_Banding) values (2, 'Excluded', 'Excluded', 'Excluded');
insert into VESPA_Shared.Aggr_Metric_Group_Dim (Metric_Group_Key, Group_Name, Low_Level_Banding, High_Level_Banding) values (3, 'Did not watch', 'Did not watch', 'Did not watch');














