/*###############################################################################
# Created on:   28/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation - script which creates required
#               structures (Phase 2 data)
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
#
###############################################################################*/


  -- ###############################################################################
  -- ##### VESPA_SURF_MINUTES_PHASE2                                           #####
  -- ###############################################################################
if object_id('VESPA_SURF_MINUTES_PHASE2') is not null then drop table VESPA_SURF_MINUTES_PHASE2 endif;
create table VESPA_SURF_MINUTES_PHASE2 (
    Surf_Id                     bigint      identity primary key,
    Subscriber_Id               bigint      not null,
    Surf_Minute_Start           datetime    not null,
    Surf_Minute_End             datetime    not null,
    Build_Date                  datetime    null
);

create unique index idx1 on VESPA_SURF_MINUTES_PHASE2(Subscriber_Id, Surf_Minute_Start, Surf_Minute_End);


  -- ###############################################################################
  -- ##### VESPA_SURF_CONSTITUENTS_PHASE2                                      #####
  -- ###############################################################################
if object_id('VESPA_SURF_CONSTITUENTS_PHASE2') is not null then drop table VESPA_SURF_CONSTITUENTS_PHASE2 endif;
create table VESPA_SURF_CONSTITUENTS_PHASE2 (
    Surf_Id                     bigint      not null,
    Instance_Id                 bigint      not null,
    Minute_Seq                  tinyint     not null default 1,
    Front_Minute                bit         not null default 0,
    Build_Date                  datetime    null,
    primary key (Surf_Id, Instance_Id)
);

create hg index idx1 on VESPA_SURF_CONSTITUENTS_PHASE2(Surf_Id);
create hg index idx2 on VESPA_SURF_CONSTITUENTS_PHASE2(Instance_Id);


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################

























