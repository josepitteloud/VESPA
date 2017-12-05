/*###############################################################################
# Created on:   06/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation - batch execution over number of
#               tables (Phase 1 data structures)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# "Minute_Attribution_Phase1_v02" procedure installed
# "Minute_Attribution_QA_Phase1_v02" procedure installed
# Access to Augmented tables as in Vespa_Analyst schema
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2012  SBE   v01 - initial version
#
###############################################################################*/


create variable @varSql               varchar(15000);
create variable @varCntr              smallint;
create variable @varNumIterations     smallint;
create variable @varBuildDate         date;

/*
  -- ###############################################################################
  -- ##### This part only copies augmented tables to a local schema for        #####
  -- ##### testing purposes - USE WITH CAUTION!!!!                             #####
  -- ###############################################################################
if object_id('vespa_daily_augs_20120203') is not null then drop table vespa_daily_augs_20120203 endif;
select *
  into vespa_daily_augs_20120203
  from vespa_analysts.vespa_daily_augs_20120203;
commit;
create unique hg index idx1 on vespa_daily_augs_20120203(cb_row_id);

if object_id('vespa_daily_augs_20120204') is not null then drop table vespa_daily_augs_20120204 endif;
select *
  into vespa_daily_augs_20120204
  from vespa_analysts.vespa_daily_augs_20120204;
commit;
create unique hg index idx1 on vespa_daily_augs_20120204(cb_row_id);

if object_id('vespa_daily_augs_20120205') is not null then drop table vespa_daily_augs_20120205 endif;
select *
  into vespa_daily_augs_20120205
  from vespa_analysts.vespa_daily_augs_20120205;
commit;
create unique hg index idx1 on vespa_daily_augs_20120205(cb_row_id);

if object_id('vespa_daily_augs_20120206') is not null then drop table vespa_daily_augs_20120206 endif;
select *
  into vespa_daily_augs_20120206
  from vespa_analysts.vespa_daily_augs_20120206;
commit;
create unique hg index idx1 on vespa_daily_augs_20120206(cb_row_id);
*/


  -- ###############################################################################
  -- ##### Loop through each table                                             #####
  -- ###############################################################################
set @varCntr = 0;
set @varNumIterations = 2;
set @varBuildDate = '2012-02-03';     -- Must match suffix in the first augmented table

set @varSql = '
              create view VESPA_MinAttr_Phase1_AugmentedTable as
              select
                    cb_row_id,
                    subscriber_id,
                    viewing_starts,
                    viewing_stops,
                    viewing_duration,
                    case
                      when timeshifting = ''LIVE'' then 1
                        else 0
                    end as Live_Flag,
                    BARB_minute_start,
                    BARB_minute_end
                from vespa_daily_augs_##^^*^*##
              ';


  -- ####### Loop through daily tables ######
while @varCntr < @varNumIterations
  begin

      drop view if exists VESPA_MinAttr_Phase1_AugmentedTable

        -- Create view - to handle Live_Flag field
      execute(replace(@varSql, '##^^*^*##', dateformat(@varBuildDate, 'yyyymmdd')))
      commit

      -- Run minute attribution stuff
      execute Minute_Attribution_Phase1_v03 'VESPA_MinAttr_Phase1_AugmentedTable', dateformat(@varBuildDate, 'yyyymmdd'), @varBuildDate, 1, @varBuildDate
      commit

        -- Run QA stuff
      execute Minute_Attribution_QA_Phase1_v03 5, @varBuildDate
      commit

      set @varBuildDate = dateadd(day, 1, @varBuildDate)
      set @varCntr = @varCntr + 1
      commit

  end;

commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



