/*###############################################################################
# Created on:   06/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation - script which creates required
#               structures (Phase 1 data)
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
# 23/08/2012  SBE   v02 - VESPA_SURF_CONSTITUENTS added
#                       - "PHASE1" suffix added
#
###############################################################################*/


  -- ###############################################################################
  -- ##### VESPA_SURF_MINUTES_PHASE1                                           #####
  -- ###############################################################################
if object_id('VESPA_SURF_MINUTES_PHASE1') is not null then drop table VESPA_SURF_MINUTES_PHASE1 endif;
create table VESPA_SURF_MINUTES_PHASE1 (
    Surf_Id                     bigint      identity primary key,
    Subscriber_Id               bigint      not null,
    Surf_minute_Start           datetime    not null,
    Surf_minute_End             datetime    not null,
    Build_Date                  datetime    null
);

create unique index idx1 on VESPA_SURF_MINUTES_PHASE1(subscriber_id, surf_minute_start, surf_minute_end);


  -- ###############################################################################
  -- ##### VESPA_SURF_CONSTITUENTS_PHASE1                                      #####
  -- ###############################################################################
if object_id('VESPA_SURF_CONSTITUENTS_PHASE1') is not null then drop table VESPA_SURF_CONSTITUENTS_PHASE1 endif;
create table VESPA_SURF_CONSTITUENTS_PHASE1 (
    Surf_Id                     bigint      not null,
    Cb_row_Id                   bigint      not null,
    Minute_Seq                  tinyint     not null default 1,
    Front_Minute                bit         not null default 0,
    Build_Date                  datetime    null,
    primary key (Surf_Id, Cb_row_Id)
);

create hg index idx1 on VESPA_SURF_CONSTITUENTS_PHASE1(Surf_Id);
create hg index idx2 on VESPA_SURF_CONSTITUENTS_PHASE1(Cb_row_Id);


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################

























