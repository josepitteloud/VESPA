/*###############################################################################
# Created on:   12/07/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Scaling Metrics Report - Report output: Traffic Lights
#                 This procedure prepares output for "Traffic Lights" page
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - removing conflicting results
#               STEP 2.0 - creating relevant extracts
#               STEP 3.0 - creating traffic lights summary
#               STEP 4.0 - creating final summary (for report)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions
#     - Vespa_Analysts.Vespa_Single_Box_View
#     - Vespa_Analysts.SC2_Intervals
#     - Vespa_Analysts.SC2_Weightings
#     - SC_Metrics_Rep_tmp_Universe
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 12/07/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('SC_Metrics_Rep_Traffic_Lights') is not null then drop procedure SC_Metrics_Rep_Traffic_Lights end if;
create procedure SC_Metrics_Rep_Traffic_Lights
      @parStartDate             date,                -- Calculation period start date
      @parEndDate               date,                -- Calculation period end date
      @parRefreshIdentifier     varchar(40) = '',    -- Logger - refresh identifier
      @parBuildId               bigint = null        -- Logger - add events to an existing logger process
as
begin

        -- ##############################################################################################################
        -- ##### STEP 0.1 - preparing environment                                                                   #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Define and set variables                                            #####
        -- ###############################################################################

      declare @varReportingStartDate          date
      declare @varReportingEndDate            date
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @varSQL                         varchar(15000)

      set @varReportingStartDate = @parStartDate

      set @varReportingEndDate = @parEndDate

      set @varProcessIdentifier        = 'SCMetRep_TrafLhs_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Traffic Lights] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'User context: ' || @varUsername, null
      execute logger_add_event @varBuildId, 3, 'Period: ' || dateformat(@varReportingStartDate, 'dd/mm/yyyy') || ' - ' || dateformat(@varReportingEndDate, 'dd/mm/yyyy'), null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - removing conflicting results                                                            #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Removing conflicting results <<<<<', null

        -- Delete results for current date range (full weeks only)
      delete from Vespa_Analysts.SC_Metrics_Rep_05_Traffic_Lights_Weekly base
        from (select
                    WeekCommencing_Date
                from Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
               where Scaling_Date between @varReportingStartDate and @varReportingEndDate
               group by WeekCommencing_Date
              having sum(Daily) = 7) dts
       where base.WeekCommencing_Date = dts.WeekCommencing_Date
      commit

      execute logger_add_event @varBuildId, 3, 'Deleted existing weekly summaries', @@rowcount


      delete from Vespa_Analysts.SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary base
        from (select
                    WeekCommencing_Date
                from Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
               where Scaling_Date between @varReportingStartDate and @varReportingEndDate
               group by WeekCommencing_Date
              having sum(Daily) = 7) dts
       where base.WeekCommencing_Date = dts.WeekCommencing_Date
      commit

      execute logger_add_event @varBuildId, 3, 'Deleted existing weekly aggregated summaries', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 2.0 - creating relevant extracts                                                              #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Creating relevant extracts <<<<<', null

        -- Summary of all panel 12 accounts
      if object_id('SC_Metrics_Rep_tmp_Panel_Accounts_Summary') is not null drop table SC_Metrics_Rep_tmp_Panel_Accounts_Summary
      create table SC_Metrics_Rep_tmp_Panel_Accounts_Summary (
          Account_Number                  varchar(20)     default null,
          Scaling_Date                    date            default null,
          Most_Recent_Enablement          date            default null,
          Reporting_Categorisation        varchar(20)     default null,
          Reporting_Quality               decimal(15, 6)  default 0
      )

      create index idx1 on SC_Metrics_Rep_tmp_Panel_Accounts_Summary(Account_Number)
      create index idx2 on SC_Metrics_Rep_tmp_Panel_Accounts_Summary(Scaling_Date)

      insert into SC_Metrics_Rep_tmp_Panel_Accounts_Summary
             (Account_Number, Scaling_Date, Most_Recent_Enablement, Reporting_Categorisation, Reporting_Quality)
        select
              sbv.Account_Number,
              dts.WeekCommencing_Date,
              max(sbv.Enablement_Date),
              case
                when datediff(day, max(sbv.Enablement_Date), dts.WeekCommencing_Date) < 15    then 'Recently enabled'
                when min(sbv.Logs_Every_Day_30d) = 1                                          then 'Acceptable'
                when min(sbv.Logs_Returned_In_30d) >= 25 or min(Reporting_Quality) >= 0.9     then 'Acceptable'
                when max(sbv.Logs_Returned_In_30d) = 0                                        then 'Zero reporting'
                  else 'Unreliable'
              end,
              min(Reporting_Quality)
          from Vespa_Analysts.Vespa_Single_Box_View sbv,
               Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
         where sbv.panel = 'VESPA'
           and sbv.Status_Vespa = 'Enabled'
           and sbv.Enablement_Date <= (dts.WeekCommencing_Date + 7)
           and dts.Scaling_Date between @varReportingStartDate and @varReportingEndDate
           and dts.Weekly = 1
         group by sbv.Account_Number, dts.WeekCommencing_Date
      commit

      execute logger_add_event @varBuildId, 3, 'Panel account summary created', @@rowcount


        -- List of relevant weights
      if object_id('SC_Metrics_Rep_tmp_Scaling_Weights') is not null drop table SC_Metrics_Rep_tmp_Scaling_Weights
      create table SC_Metrics_Rep_tmp_Scaling_Weights (
          Account_Number                  varchar(20)     default null,
          Scaling_Date                    date            default null,
          Weighting                       decimal(15, 6)  default 0
      )

      create index idx1 on SC_Metrics_Rep_tmp_Scaling_Weights(Account_Number)
      create index idx2 on SC_Metrics_Rep_tmp_Scaling_Weights(Scaling_Date)


      insert into SC_Metrics_Rep_tmp_Scaling_Weights
             (Account_Number, Scaling_Date, Weighting)
        select
              intv.Account_Number,
              weig.Scaling_Day,
              max(weig.Weighting) as Weighting
          from Vespa_Analysts.SC2_Intervals intv,
               Vespa_Analysts.SC2_Weightings weig,
               Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dt
         where weig.Scaling_Day >= intv.Reporting_Starts
           and weig.Scaling_Day <= intv.Reporting_Ends
           and intv.Scaling_Segment_Id = weig.Scaling_Segment_Id
           and weig.Scaling_Day = dt.Scaling_Date
           and dt.Scaling_Date between @varReportingStartDate and @varReportingEndDate
           and dt.Weekly = 1
         group by weig.Scaling_Day, intv.Account_Number
      commit

      execute logger_add_event @varBuildId, 3, 'Scaling weight snapshot created', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 3.0 - creating traffic lights summary                                                         #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.0: Creating traffic lights summaries <<<<<', null

      if object_id('SC_Metrics_Rep_tmp_Traffic_Ligths') is not null drop table SC_Metrics_Rep_tmp_Traffic_Ligths
      create table SC_Metrics_Rep_tmp_Traffic_Ligths (
          Scaling_Date                    date            default null,
          Variable_Name                   varchar(50)     default null,
          Category                        varchar(50)     default null,
          Total_Sky_Base                  bigint          default null,
          Category_Sky_Base               bigint          default null,
          Sum_Of_Weights                  decimal(15, 6)  default null,
          Total_Vespa_Base                bigint          default null,
          Category_Vespa_Base             bigint          default null,
          Acceptably_Reliable_HHs         bigint          default null,
          Unreliable_HHs                  bigint          default null,
          Zero_Reporting_HHs              bigint          default null,
          Recently_Enabled_HHs            bigint          default null,
          Good_Household_Index            decimal(15, 6)  default null
      )


      set @varSQL = '
                      insert into SC_Metrics_Rep_tmp_Traffic_Ligths
                             (Scaling_Date, Variable_Name, Category, Total_Sky_Base, Category_Sky_Base, Sum_Of_Weights, Total_Vespa_Base,
                              Category_Vespa_Base, Acceptably_Reliable_HHs, Unreliable_HHs, Zero_Reporting_HHs, Recently_Enabled_HHs, Good_Household_Index)
                      select
                            base.Scaling_Date                                                                     as Scaling_Date,
                            ''##^1^##''                                                                           as Variable_Name,
                            base.##^2^##                                                                          as Category,
                            cast(0 as bigint)                                                                     as Total_Sky_Base,
                            count(distinct base.Account_Number)                                                   as Category_Sky_Base,
                            cast(coalesce( sum(wgh.Weighting), 0 ) as decimal(15, 6))                             as Sum_Of_Weights,
                            cast(0 as bigint)                                                                     as Total_Vespa_Base,
                            count(distinct vsp.Account_Number)                                                    as Category_Vespa_Base,
                            sum(case when vsp.Reporting_Categorisation = ''Acceptable''       then 1 else 0 end)  as Acceptably_Reliable_HHs,
                            sum(case when vsp.Reporting_Categorisation = ''Unreliable''       then 1 else 0 end)  as Unreliable_HHs,
                            sum(case when vsp.Reporting_Categorisation = ''Zero reporting''   then 1 else 0 end)  as Zero_Reporting_HHs,
                            sum(case when vsp.Reporting_Categorisation = ''Recently enabled'' then 1 else 0 end)  as Recently_Enabled_HHs,
                            cast(0 as decimal(15, 6))                                                             as Good_Household_Index
                        from SC_Metrics_Rep_tmp_Universe base

                                left join SC_Metrics_Rep_tmp_Panel_Accounts_Summary vsp               on base.Account_Number = vsp.Account_Number
                                                                                                     and base.Scaling_Date = vsp.Scaling_Date

                                inner join Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts    on base.Scaling_Date = dts.Scaling_Date
                                                                                                     and dts.Scaling_Date between ''' || @varReportingStartDate || ''' and ''' || @varReportingEndDate || '''
                                                                                                     and dts.Weekly = 1

                                left join SC_Metrics_Rep_tmp_Scaling_Weights wgh                      on base.Account_Number = wgh.Account_Number
                                                                                                     and base.Scaling_Date = wgh.Scaling_Date

                       group by base.Scaling_Date, Category
                      commit
                    '


        --   ##^1^##  - variable name - used within the report in lookups
        --   ##^2^##  - categories (in SQL use table variable name)
      execute(replace( replace( @varSQL, '##^1^##', 'Universe' ), '##^2^##', 'Universe' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Universe', null

      execute(replace( replace( @varSQL, '##^1^##', 'Region' ), '##^2^##', 'Region' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Region', null

      execute(replace( replace( @varSQL, '##^1^##', 'Household Composition' ), '##^2^##', 'HH_Composition' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Household Composition', null

      execute(replace( replace( @varSQL, '##^1^##', 'Tenure' ), '##^2^##', 'Tenure' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Tenure', null

      execute(replace( replace( @varSQL, '##^1^##', 'TV Package' ), '##^2^##', 'TV_Package' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: TV Package', null

      execute(replace( replace( @varSQL, '##^1^##', 'Box Type' ), '##^2^##', 'Box_Type' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Box Type', null


      execute(replace( replace( @varSQL, '##^1^##', 'Value Segment' ), '##^2^##', 'Value_Segment' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Value Segment', null

      execute(replace( replace( @varSQL, '##^1^##', 'Experian Mosaic' ), '##^2^##', 'Experian_Mosaic' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Experian Mosaic', null

      execute(replace( replace( @varSQL, '##^1^##', 'Financial Strategy Segments' ), '##^2^##', 'Financial_Strategy_Segm' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Financial Strategy Segments', null

      execute(replace( replace( @varSQL, '##^1^##', 'OnNet / OffNet area' ), '##^2^##', 'OnOffNet_Area' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: OnNet / OffNet area', null

      execute(replace( replace( @varSQL, '##^1^##', 'SkyGO users' ), '##^2^##', 'SkyGO_User' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: SkyGO users', null



      update SC_Metrics_Rep_tmp_Traffic_Ligths base
         set base.Total_Sky_Base  = det.Total_Sky_Base
        from (select
                    Scaling_Date,
                    count(distinct Account_Number) as Total_Sky_Base
               from SC_Metrics_Rep_tmp_Universe
              group by Scaling_Date) det
       where base.Scaling_Date = det.Scaling_Date
      commit

      execute logger_add_event @varBuildId, 3, '"Total Sky Base" updated', @@rowcount


      update SC_Metrics_Rep_tmp_Traffic_Ligths base
         set base.Total_Vespa_Base  = det.Total_Vespa_Base
        from (select
                    Scaling_Date,
                    count(distinct Account_Number) as Total_Vespa_Base
               from SC_Metrics_Rep_tmp_Panel_Accounts_Summary
              where Reporting_Categorisation = 'Acceptable'
              group by Scaling_Date) det
       where base.Scaling_Date = det.Scaling_Date
      commit

      execute logger_add_event @varBuildId, 3, '"Total Vespa Base" updated', @@rowcount


      update SC_Metrics_Rep_tmp_Traffic_Ligths
        set Good_Household_Index = case
                                      when (100 * (Acceptably_Reliable_HHs) * Total_Sky_Base / Category_Sky_Base / Total_Vespa_Base) > 200 then 200
                                        else 100 * (Acceptably_Reliable_HHs) * Total_Sky_Base / Category_Sky_Base / Total_Vespa_Base
                                   end
      commit

      execute logger_add_event @varBuildId, 3, '"Good HH Index" updated', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 4.0 - creating final summary (for report)                                                     #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.0: Publishing results <<<<<', null

      insert into Vespa_Analysts.SC_Metrics_Rep_05_Traffic_Lights_Weekly
             (WeekCommencing_Date, Variable_Name, Category, Total_Sky_Base, Category_Sky_Base, Sum_Of_Weights, Total_Vespa_Base, Category_Vespa_Base,
              Acceptably_Reliable_HHs, Unreliable_HHs, Zero_Reporting_HHs, Recently_Enabled_HHs, Good_Household_Index)
        select
              base.Scaling_Date,
              base.Variable_Name,
              base.Category,
              base.Total_Sky_Base,
              base.Category_Sky_Base,
              base.Sum_Of_Weights,
              base.Total_Vespa_Base,
              base.Category_Vespa_Base,
              base.Acceptably_Reliable_HHs,
              base.Unreliable_HHs,
              base.Zero_Reporting_HHs,
              base.Recently_Enabled_HHs,
              base.Good_Household_Index
          from SC_Metrics_Rep_tmp_Traffic_Ligths base,
               Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
         where base.Scaling_Date = dts.WeekCommencing_Date
           and dts.Scaling_Date between @varReportingStartDate and @varReportingEndDate
         group by base.Scaling_Date, base.Variable_Name, base.Category, base.Total_Sky_Base, base.Category_Sky_Base, base.Sum_Of_Weights, base.Total_Vespa_Base,
                  base.Category_Vespa_Base, base.Acceptably_Reliable_HHs, base.Unreliable_HHs, base.Zero_Reporting_HHs, base.Recently_Enabled_HHs,
                  base.Good_Household_Index
        having sum(dts.Daily) = 7
      commit

      execute logger_add_event @varBuildId, 3, 'Weekly summaries created', @@rowcount


      insert into Vespa_Analysts.SC_Metrics_Rep_06_Traffic_Lights_Weekly_Summary
             (WeekCommencing_Date, Variable_Name, Category_Index, Category_Convergence, Category_Convergence_Abs, Convergence_Std)
        select
              base.Scaling_Date,
              base.Variable_Name,
              sqrt(avg( (Good_Household_Index - 100) * (Good_Household_Index - 100) ))  as Category_Index,
              sum( Category_Sky_Base - Sum_Of_Weights )                                 as Category_Convergence,
              sum(abs( Category_Sky_Base - Sum_Of_Weights ))                            as Category_Convergence_Abs,
              stddev(Category_Sky_Base - Sum_Of_Weights)                                as Convergence_Std
          from SC_Metrics_Rep_tmp_Traffic_Ligths base,
               (select
                      WeekCommencing_Date
                  from Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
                 where dts.Scaling_Date between @varReportingStartDate and @varReportingEndDate
                 group by WeekCommencing_Date
                having sum(dts.Daily) = 7) dts
         where base.Scaling_Date = dts.WeekCommencing_Date
         group by base.Scaling_Date, base.Variable_Name
      commit

      execute logger_add_event @varBuildId, 3, 'Weekly aggregated summaries created', @@rowcount



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Traffic Lights] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


