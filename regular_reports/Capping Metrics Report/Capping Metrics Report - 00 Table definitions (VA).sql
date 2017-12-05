/*###############################################################################
# Created on:   05/08/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Capping Metrics Report
#
# List of steps:
#               N/A
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 05/08/2013  SBE   v01 - initial version
#
###############################################################################*/


  -- ### Metadata ###
-- call dba.sp_drop_table('vespa_analysts', 'CAP_Metrics_Rep_01_Period_Definitions');
call dba.sp_create_table('vespa_analysts',
                         'CAP_Metrics_Rep_01_Period_Definitions',
                         '
                          Id_Key                  bigint          identity,
                          Event_Date              date            default null,
                          WeekCommencing_Date     date            default null,
                          Sky_Week                tinyint         default null,
                          Daily                   bit             default 0,
                          Weekly                  bit             default 0,
                          Monthly                 bit             default 0,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on vespa_analysts.CAP_Metrics_Rep_01_Period_Definitions(Event_Date);
create date index idx2 on vespa_analysts.CAP_Metrics_Rep_01_Period_Definitions(WeekCommencing_Date);


  -- ### Overview page tables ###
-- call dba.sp_drop_table('vespa_analysts', 'CAP_Metrics_Rep_02_Overview');
call dba.sp_create_table('vespa_analysts',
                         'CAP_Metrics_Rep_02_Overview',
                         '
                          Id_Key                  bigint          identity,
                          Event_Date              date            default null,
                          Variable_Group          varchar(50)     default null,
                          Variable_Name           varchar(50)     default null,
                          Category                varchar(50)     default null,
                          Num_Subscriber_Ids      bigint          default null,
                          Num_Accounts            bigint          default null,
                          Total_Precap_Viewing    bigint          default null,
                          Total_Postcap_Viewing_Src bigint       default null,
                          Total_Postcap_Viewing_Augs bigint       default null,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on vespa_analysts.CAP_Metrics_Rep_02_Overview(Event_Date);


-- drop view if exists v_CAP_Metrics_Rep_02_Overview;
create view v_CAP_Metrics_Rep_02_Overview as
  select
        a.Event_Date,
        a.Variable_Group,
        a.Variable_Name,
        a.Category,
        a.Num_Subscriber_Ids,
        a.Num_Accounts,
        a.Total_Precap_Viewing,
        a.Total_Postcap_Viewing_Src,
        a.Total_Postcap_Viewing_Augs
    from Vespa_Analysts.CAP_Metrics_Rep_02_Overview a,
         Vespa_Analysts.CAP_Metrics_Rep_01_Period_Definitions b
   where a.Event_Date = b.Event_Date
   order by a.Event_Date;





