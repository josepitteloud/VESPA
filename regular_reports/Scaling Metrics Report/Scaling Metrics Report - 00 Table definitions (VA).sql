/*###############################################################################
# Created on:   27/06/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Scaling Metrics Report - Object definitions
#
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 27/06/2013  SBE   v01 - initial version
#
###############################################################################*/


  -- ### Metadata ###
-- call dba.sp_drop_table('vespa_analysts', 'SC_Metrics_Rep_01_Period_Definitions');
call dba.sp_create_table('vespa_analysts',
                         'SC_Metrics_Rep_01_Period_Definitions',
                         '
                          Id_Key                  bigint          identity,
                          Scaling_Date            date            default null,
                          WeekCommencing_Date     date            default null,
                          Sky_Week                tinyint         default null,
                          Daily                   bit             default 0,
                          Weekly                  bit             default 0,
                          Monthly                 bit             default 0,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on vespa_analysts.SC_Metrics_Rep_01_Period_Definitions(Scaling_Date);
create date index idx2 on vespa_analysts.SC_Metrics_Rep_01_Period_Definitions(WeekCommencing_Date);


  -- ### Overview page tables ###
-- call dba.sp_drop_table('vespa_analysts', 'SC_Metrics_Rep_02_Overview');
call dba.sp_create_table('vespa_analysts',
                         'SC_Metrics_Rep_02_Overview',
                         '
                          Id_Key                  bigint          identity,
                          Scaling_Date            date            default null,
                          Vespa_Panel             bigint          default null,
                          Sky_Base                bigint          default null,
                          Population_Coverage     bigint          default null,
                          Minimum_Weight          decimal(15,6)   default null,
                          Maximum_Weight          decimal(15,6)   default null,
                          Average_Weight          decimal(15,6)   default null,
                          Sum_Of_Convergence      bigint           default null,
                          Iterations              smallint        default null,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on vespa_analysts.SC_Metrics_Rep_02_Overview(Scaling_Date);


  -- ### Variables page tables ###
-- call dba.sp_drop_table('vespa_analysts', 'SC_Metrics_Rep_03_Variables');
call dba.sp_create_table('vespa_analysts',
                         'SC_Metrics_Rep_03_Variables',
                         '
                          Id_Key                  bigint          identity,
                          Scaling_Date            date            default null,
                          Variable_Group          varchar(15)     default null,
                          Variable_Name           varchar(30)     default null,
                          Category                varchar(50)     default null,
                          Sky_Base                bigint          default null,
                          Population_Coverage     bigint          default null,
                          Convergence             bigint          default null,
                          Convergence_Abs         bigint          default null,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on Vespa_Analysts.SC_Metrics_Rep_03_Variables(Scaling_Date);
create lf index idx2 on Vespa_Analysts.SC_Metrics_Rep_03_Variables(Variable_Group);


  -- ### Coverage by category page tables ###
-- call dba.sp_drop_table('vespa_analysts', 'SC_Metrics_Rep_04_Coverage_By_Cat_Raw');
call dba.sp_create_table('vespa_analysts',
                         'SC_Metrics_Rep_04_Coverage_By_Cat_Raw',
                         '
                          Id_Key                  bigint          identity,
                          Scaling_Date            date            default null,
                          WeekCommencing_Date     date            default null,
                          Variable_Name           varchar(30)     default null,
                          Category                varchar(50)     default null,
                          Sky_Base                bigint          default null,
                          Sum_Of_Weights          bigint          default null,
                          Population_Coverage     bigint          default null,
                          Segments_Num            bigint          default null,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw(Scaling_Date);
create date index idx2 on Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw(WeekCommencing_Date);
create lf index idx3 on Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw(Variable_Name);
create unique index idx4 on Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw(Scaling_Date, Variable_Name, Category);


-- call dba.sp_drop_table('vespa_analysts', 'SC_Metrics_Rep_04_Coverage_By_Cat_Weekly');
call dba.sp_create_table('vespa_analysts',
                         'SC_Metrics_Rep_04_Coverage_By_Cat_Weekly',
                         '
                          Id_Key                  bigint          identity,
                          WeekCommencing_Date     date            default null,
                          Variable_Name           varchar(30)     default null,
                          Category                varchar(50)     default null,
                          Sky_Base                bigint          default null,
                          Sum_Of_Weights          bigint          default null,
                          Population_Coverage     decimal(15, 6)  default null,
                          Segment_Coverage        decimal(15, 6)  default null,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Weekly(WeekCommencing_Date);
create lf index idx2 on Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Weekly(Variable_Name);
create unique index idx3 on Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Weekly(WeekCommencing_Date, Variable_Name, Category);


  -- ### Traffic Lights page tables ###
-- call dba.sp_drop_table('vespa_analysts', 'SC_Metrics_Rep_05_Traffic_Lights_Weekly');
call dba.sp_create_table('vespa_analysts',
                         'SC_Metrics_Rep_05_Traffic_Lights_Weekly',
                         '
                          Id_Key                  bigint          identity,
                          WeekCommencing_Date     date            default null,
                          Variable_Name           varchar(50)     default null,
                          Category                varchar(50)     default null,
                          Total_Sky_Base          bigint          default null,
                          Category_Sky_Base       bigint          default null,
                          Sum_Of_Weights          decimal(15, 6)  default null,
                          Total_Vespa_Base        bigint          default null,
                          Category_Vespa_Base     bigint          default null,
                          Acceptably_Reliable_HHs bigint          default null,
                          Unreliable_HHs          bigint          default null,
                          Zero_Reporting_HHs      bigint          default null,
                          Recently_Enabled_HHs    bigint          default null,
                          Good_Household_Index    decimal(15, 6)  default null,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on Vespa_Analysts.SC_Metrics_Rep_05_Traffic_Lights_Weekly(WeekCommencing_Date);
create lf index idx2 on Vespa_Analysts.SC_Metrics_Rep_05_Traffic_Lights_Weekly(Variable_Name);
create unique index idx3 on Vespa_Analysts.SC_Metrics_Rep_05_Traffic_Lights_Weekly(WeekCommencing_Date, Variable_Name, Category);


-- call dba.sp_drop_table('vespa_analysts', 'SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary');
call dba.sp_create_table('vespa_analysts',
                         'SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary',
                         '
                          Id_Key                  bigint          identity,
                          WeekCommencing_Date     date            default null,
                          Variable_Name           varchar(50)     default null,
                          Category_Index          decimal(15, 6)  default null,
                          Category_Convergence    decimal(15, 6)  default null,
                          Category_Convergence_Abs decimal(15, 6)  default null,
                          Convergence_Std         decimal(15, 6)  default null,
                          Updated_On              datetime        default timestamp,
                          Updated_By              varchar(30)     default user_name()
                         '
                        );

create date index idx1 on Vespa_Analysts.SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary(WeekCommencing_Date);
create lf index idx2 on Vespa_Analysts.SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary(Variable_Name);
create unique index idx3 on Vespa_Analysts.SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary(WeekCommencing_Date, Variable_Name);




    -- To be moved to Vespa Analysts when DBA's enable that functionality
-- drop view if exists v_SC_Metrics_Rep_02_Overview;
create view v_SC_Metrics_Rep_02_Overview as
  select
        a.Scaling_Date,
        a.Vespa_Panel,
        a.Sky_Base,
        a.Population_Coverage,
        a.Minimum_Weight,
        a.Maximum_Weight,
        a.Average_Weight,
        a.Sum_Of_Convergence,
        a.Iterations
    from Vespa_Analysts.SC_Metrics_Rep_02_Overview a,
         Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions b
   where a.Scaling_Date = b.Scaling_Date
   order by a.Scaling_Date;


-- drop view if exists v_SC_Metrics_Rep_03_Variables_Scaling;
create view v_SC_Metrics_Rep_03_Variables_Scaling as
  select
        a.Scaling_Date,
        a.Variable_Name,
        a.Category,
        a.Sky_Base,
        a.Population_Coverage,
        a.Convergence_Abs as Convergence
    from Vespa_Analysts.SC_Metrics_Rep_03_Variables a,
         Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions b
   where a.Scaling_Date = b.Scaling_Date
     and a.Variable_Group = 'scaling'
   order by a.Scaling_Date, a.Variable_Group, a.Variable_Name, a.Category;


-- drop view if exists v_SC_Metrics_Rep_03_Variables_Misc;
create view v_SC_Metrics_Rep_03_Variables_Misc as
  select
        a.Scaling_Date,
        a.Variable_Name,
        a.Category,
        a.Sky_Base,
        a.Population_Coverage,
        a.Convergence_Abs as Convergence
    from Vespa_Analysts.SC_Metrics_Rep_03_Variables a,
         Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions b
   where a.Scaling_Date = b.Scaling_Date
     and a.Variable_Group = 'misc'
   order by a.Scaling_Date, a.Variable_Group, a.Variable_Name, a.Category;


-- drop view if exists v_SC_Metrics_Rep_04_Coverage_By_Category;
create view v_SC_Metrics_Rep_04_Coverage_By_Category as
  select
        a.WeekCommencing_Date,
        a.Variable_Name,
        a.Category,
        a.Sky_Base,
        a.Sum_Of_Weights,
        a.Population_Coverage,
        a.Segment_Coverage
    from Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Weekly a,
         Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions b
   where a.WeekCommencing_Date = b.WeekCommencing_Date
     and b.Weekly = 1
   order by a.WeekCommencing_Date, a.Variable_Name, a.Category;


-- drop view if exists v_SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary;
create view v_SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary as
  select
        a.WeekCommencing_Date,
        a.Variable_Name,
        a.Category_Index,
        a.Category_Convergence,
        a.Category_Convergence_Abs,
        a.Convergence_Std
    from Vespa_Analysts.SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary a,
         Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions b
   where a.WeekCommencing_Date = b.WeekCommencing_Date
     and b.Weekly = 1
   order by a.WeekCommencing_Date, a.Variable_Name;
















