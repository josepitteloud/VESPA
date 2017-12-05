/*###############################################################################
# Created on:   10/07/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Scaling Metrics Report - Report output: Coverage by categories
#                 This procedure prepares output for "Coverage by variable category"
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - removing conflicting results
#               STEP 2.0 - publishing daily summaries
#               STEP 2.1 - publishing weekly summaries (for complete weeks only)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions
#     - SC_Metrics_Rep_tmp_Universe
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/07/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('SC_Metrics_Rep_Coverage_By_Cat') is not null then drop procedure SC_Metrics_Rep_Coverage_By_Cat end if;
create procedure SC_Metrics_Rep_Coverage_By_Cat
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

      set @varProcessIdentifier        = 'SCMetRep_CovCat_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Coverage by Category] - process started #######', null
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

        -- Delete results for current date range
      delete from Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw base
        from Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
       where base.Scaling_Date = dts.Scaling_Date
         and dts.Scaling_Date between @varReportingStartDate and @varReportingEndDate
      commit

      execute logger_add_event @varBuildId, 3, 'Deleted current period dates (daily summaries)', @@rowcount


      delete from Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Weekly base
        from (select
                    WeekCommencing_Date
                from Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
               where Scaling_Date between @varReportingStartDate and @varReportingEndDate
               group by WeekCommencing_Date
              having sum(Daily) = 7) dts
       where base.WeekCommencing_Date = dts.WeekCommencing_Date
      commit

      execute logger_add_event @varBuildId, 3, 'Deleted current period dates (weekly summaries)', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 2.0 - publishing daily summaries                                                              #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Publishing daily summaries <<<<<', null

        -- "Overall" group
      insert into Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw
             (Scaling_Date, WeekCommencing_Date, Variable_Name, Category, Sky_Base,
              Sum_Of_Weights, Population_Coverage, Segments_Num)
        select
              base.Scaling_Date,
              base.WeekCommencing_Date,
              '(Overall)',
              '(all)',
              base.Sky_Base,
              base.Sum_Of_Weights,
              cov.Population_Coverage,
              cov.Segments_Num

          from (select
                      a.Scaling_Date,
                      max(b.WeekCommencing_Date) as WeekCommencing_Date,
                      count(*) as Sky_Base,
                      round(sum(Scaling_Weight), 0) as Sum_Of_Weights
                  from SC_Metrics_Rep_tmp_Universe a,
                       Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions b
                 where a.Scaling_Date = b.Scaling_Date
                 group by a.Scaling_Date) base

               left join

               (select
                      a.Scaling_Date,
                      max(c.WeekCommencing_Date) as WeekCommencing_Date,
                      count(*) as Population_Coverage,
                      count(distinct a.scaling_segment_id) as Segments_Num
                  from SC_Metrics_Rep_tmp_Universe a,
                       (select
                              Scaling_Date,
                              Scaling_Segment_ID
                          from SC_Metrics_Rep_tmp_Universe
                         where Vespa_Panel_Flag = 1
                         group by Scaling_Date, Scaling_Segment_ID) b,
                       Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions c
                 where a.Scaling_Date = b.Scaling_Date
                   and a.Scaling_Segment_ID = b.Scaling_Segment_ID
                   and a.Scaling_Date = c.Scaling_Date
                 group by a.Scaling_Date) cov

               on base.Scaling_Date = cov.Scaling_Date
      commit

      execute logger_add_event @varBuildId, 3, 'Daily summary created: (Overall)', null


        -- Individual variables
      set @varSQL = '
                      insert into Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw
                             (Scaling_Date, WeekCommencing_Date, Variable_Name, Category, Sky_Base,
                              Sum_Of_Weights, Population_Coverage, Segments_Num)
                        select
                              base.Scaling_Date,
                              base.WeekCommencing_Date,
                              ''##^1^##'',                                                                            -- Variable Name
                              base.##^2^##,                                                                           -- Category (variable name)
                              base.Sky_Base,
                              base.Sum_Of_Weights,
                              case when cov.Population_Coverage is null then 0 else cov.Population_Coverage end,
                              case when cov.Segments_Num is null then 0 else cov.Segments_Num end

                          from (select
                                      a.Scaling_Date,
                                      a.##^2^##,
                                      max(b.WeekCommencing_Date) as WeekCommencing_Date,
                                      count(*) as Sky_Base,
                                      round(sum(Scaling_Weight), 0) as Sum_Of_Weights
                                  from SC_Metrics_Rep_tmp_Universe a,
                                       Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions b
                                 where a.Scaling_Date = b.Scaling_Date
                                 group by a.Scaling_Date, a.##^2^##) base

                               left join

                               (select
                                      a.Scaling_Date,
                                      a.##^2^##,
                                      max(c.WeekCommencing_Date) as WeekCommencing_Date,
                                      count(*) as Population_Coverage,
                                      count(distinct a.scaling_segment_id) as Segments_Num
                                  from SC_Metrics_Rep_tmp_Universe a,
                                       (select
                                              Scaling_Date,
                                              Scaling_Segment_ID
                                          from SC_Metrics_Rep_tmp_Universe
                                         where Vespa_Panel_Flag = 1
                                         group by Scaling_Date, Scaling_Segment_ID) b,
                                       Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions c
                                 where a.Scaling_Date = b.Scaling_Date
                                   and a.Scaling_Segment_ID = b.Scaling_Segment_ID
                                   and a.Scaling_Date = c.Scaling_Date
                                 group by a.Scaling_Date, a.##^2^##) cov

                               on base.Scaling_Date = cov.Scaling_Date
                              and base.##^2^## = cov.##^2^##
                      commit
                    '

        --   ##^1^##  - variable name - used within the report in lookups
        --   ##^2^##  - categories (in SQL use table variable name)
      execute(replace( replace( @varSQL, '##^1^##', 'Universe' ), '##^2^##', 'Universe' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: Universe', null

      execute(replace( replace( @varSQL, '##^1^##', 'Region' ), '##^2^##', 'Region' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: Region', null

      execute(replace( replace( @varSQL, '##^1^##', 'Household Composition' ), '##^2^##', 'HH_Composition' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: Household Composition', null

      execute(replace( replace( @varSQL, '##^1^##', 'Tenure' ), '##^2^##', 'Tenure' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: Tenure', null

      execute(replace( replace( @varSQL, '##^1^##', 'TV Package' ), '##^2^##', 'TV_Package' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: TV Package', null

      execute(replace( replace( @varSQL, '##^1^##', 'Box Type' ), '##^2^##', 'Box_Type' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: Box Type', null


      execute(replace( replace( @varSQL, '##^1^##', 'Value Segment' ), '##^2^##', 'Value_Segment' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: Value Segment', null

      execute(replace( replace( @varSQL, '##^1^##', 'Experian Mosaic' ), '##^2^##', 'Experian_Mosaic' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: Experian Mosaic', null

      execute(replace( replace( @varSQL, '##^1^##', 'Financial Strategy Segments' ), '##^2^##', 'Financial_Strategy_Segm' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: Financial Strategy Segments', null

      execute(replace( replace( @varSQL, '##^1^##', 'OnNet / OffNet area' ), '##^2^##', 'OnOffNet_Area' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: OnNet / OffNet area', null

      execute(replace( replace( @varSQL, '##^1^##', 'SkyGO users' ), '##^2^##', 'SkyGO_User' ))
      execute logger_add_event @varBuildId, 3, 'Daily summary created: SkyGO users', null



        -- ##############################################################################################################
        -- ##### STEP 2.1 - publishing weekly summaries (for complete weeks only)                                   #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.1: Publishing weekly summaries <<<<<', null

        -- Workaround for non-matching values & column names in the lookup
      drop view if exists v_SC_Metrics_Rep_tmp_Segments_Lookup
      create view v_SC_Metrics_Rep_tmp_Segments_Lookup as
        select
              Scaling_Segment_Id,
              Universe                        as Universe,
              Isba_Tv_Region                  as Region,
              case HHComposition
                when '00' then 'Families'
                when '01' then 'Extended families'
                when '02' then 'Extended households'
                when '03' then 'Pseudo families'
                when '04' then 'Single males'
                when '05' then 'Single females'
                when '06' then 'Male homesharers'
                when '07' then 'Female homesharers'
                when '08' then 'Mixed homesharers'
                when '09' then 'Abbreviated male families'
                when '10' then 'Abbreviated female families'
                when '11' then 'Multi-occupancy dwellings'
                when 'U'  then 'Unclassified'
                when 'NS' then 'Unclassified'
                  else 'Unclassified'
              end                             as HH_Composition,
              Tenure                          as Tenure,
              Package                         as TV_Package,
              BoxType                         as Box_Type
          from Vespa_Analysts.SC2_Segments_Lookup_v2_1


        -- "Overall" group
      insert into Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Weekly
             (WeekCommencing_Date, Variable_Name, Category, Sky_Base,
              Sum_Of_Weights, Population_Coverage, Segment_Coverage)

        select
              base.WeekCommencing_Date,
              base.Variable_Name,                                                                                     -- Variable Name
              base.Category,                                                                                          -- Category (variable name)
              avg(base.Sky_Base),
              avg(base.Sum_Of_Weights),
              case
                when avg(base.Population_Coverage) / avg(base.Sky_Base) is null then 0
                  else avg(base.Population_Coverage) / avg(base.Sky_Base)
              end,
              case
                when avg(base.Segments_Num) / max(sgm.Segments_Num) is null then 0
                  else avg(base.Segments_Num) / max(sgm.Segments_Num)
              end
          from Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw base,

               (select
                      WeekCommencing_Date
                  from Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
                 where Scaling_Date between @varReportingStartDate and @varReportingEndDate
                 group by WeekCommencing_Date
                having sum(Daily) = 7) dts,

               (select
                      '(all)' as Category,
                      count(*) as Segments_Num
                  from v_SC_Metrics_Rep_tmp_Segments_Lookup
                 group by Category) sgm

         where base.Category = sgm.Category
           and base.WeekCommencing_Date = dts.WeekCommencing_Date
         group by base.WeekCommencing_Date, base.Variable_Name, base.Category
      commit

      execute logger_add_event @varBuildId, 3, 'Weekly summary created: (Overall)', null


        -- Individual variables
      set @varSQL = '
                      insert into Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Weekly
                             (WeekCommencing_Date, Variable_Name, Category, Sky_Base,
                              Sum_Of_Weights, Population_Coverage, Segment_Coverage)

                        select
                              base.WeekCommencing_Date,                                                               -- Variable Name
                              base.Variable_Name,                                                                     -- Category (variable name)
                              base.Category,
                              avg(base.Sky_Base),
                              avg(base.Sum_Of_Weights),
                              case
                                when avg(base.Population_Coverage) / avg(base.Sky_Base) is null then 0
                                  else avg(base.Population_Coverage) / avg(base.Sky_Base)
                              end,
                              case
                                when avg(base.Segments_Num) / max(sgm.Segments_Num) is null then 0
                                  else avg(base.Segments_Num) / max(sgm.Segments_Num)
                              end
                          from Vespa_Analysts.SC_Metrics_Rep_04_Coverage_By_Cat_Raw base,

                               (select
                                      WeekCommencing_Date
                                  from Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions dts
                                 where Scaling_Date between ''' || @varReportingStartDate || ''' and ''' || @varReportingEndDate || '''
                                 group by WeekCommencing_Date
                                having sum(Daily) = 7) dts,

                               (select
                                      ##^1^## as Category,
                                      count(*) as Segments_Num
                                  from v_SC_Metrics_Rep_tmp_Segments_Lookup
                                 group by Category) sgm

                         where base.Category = sgm.Category
                           and base.WeekCommencing_Date = dts.WeekCommencing_Date
                         group by base.WeekCommencing_Date, base.Variable_Name, base.Category
                      commit
                    '

        --   ##^1^##  - categories/variables in VA segments lookup
      execute(replace( @varSQL, '##^1^##', 'Universe' ))
      execute logger_add_event @varBuildId, 3, 'Weekly summaries updated: Universe', null

      execute(replace( @varSQL, '##^1^##', 'Region' ))
      execute logger_add_event @varBuildId, 3, 'Weekly summaries updated: Region', null

      execute(replace( @varSQL, '##^1^##', 'HH_Composition' ))
      execute logger_add_event @varBuildId, 3, 'Weekly summaries updated: Household Composition', null

      execute(replace( @varSQL, '##^1^##', 'Tenure' ))
      execute logger_add_event @varBuildId, 3, 'Weekly summaries updated: Tenure', null

      execute(replace( @varSQL, '##^1^##', 'TV_Package' ))
      execute logger_add_event @varBuildId, 3, 'Weekly summaries updated: TV Package', null

      execute(replace( @varSQL, '##^1^##', 'Box_Type' ))
      execute logger_add_event @varBuildId, 3, 'Weekly summaries updated: Box Type', null



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Coverage by Category] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


