/*###############################################################################
# Created on:   28/06/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Scaling Metrics Report - Report output: Variables
#                 This procedure prepares output for the following pages:
#                   - Scaling variables
#                   - Miscellaneous variables
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - removing conflicting results
#               STEP 2.0 - publishing summaries
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - SC_Metrics_Rep_tmp_Value_Segment_Dates
#     - SC_Metrics_Rep_tmp_Universe
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/06/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('SC_Metrics_Rep_Variables') is not null then drop procedure SC_Metrics_Rep_Variables end if;
create procedure SC_Metrics_Rep_Variables
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

      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @varSQL                         varchar(15000)


      set @varProcessIdentifier        = 'SCMetRep_Vars_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Variables] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'User context: ' || @varUsername, null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - removing conflicting results                                                            #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Removing conflicting results <<<<<', null

        -- Delete results for current date range
      delete from Vespa_Analysts.SC_Metrics_Rep_03_Variables base
        from SC_Metrics_Rep_tmp_Value_Segment_Dates det
       where base.Scaling_Date = det.Scaling_Date
      commit

      execute logger_add_event @varBuildId, 3, 'Deleted current period dates', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 2.0 - publishing summaries                                                                    #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Publishing summaries <<<<<', null

        -- "Overall" group
      insert into Vespa_Analysts.SC_Metrics_Rep_03_Variables
             (Scaling_Date, Variable_Group, Variable_Name, Category, Sky_Base, Population_Coverage)
        select
              Scaling_Date,
              'scaling',
              '(Overall)',
              '(all)',
              count(*),                                                                               -- Sky Base
              sum(case when Vespa_Panel_Flag = 1 then Scaling_Weight else 0 end)                      -- Population Coverage
          from SC_Metrics_Rep_tmp_Universe
         group by Scaling_Date
      commit

      execute logger_add_event @varBuildId, 3, 'Summary created: (Overall)', null


        -- Individual variables
      set @varSQL = '
                      insert into Vespa_Analysts.SC_Metrics_Rep_03_Variables
                             (Scaling_Date, Variable_Group, Variable_Name, Category, Sky_Base, Population_Coverage)
                        select
                              Scaling_Date,
                              ''##^1^##''       as Variable_Group,                                                    -- Variable Group (Scaling/Misc etc.)
                              ''##^2^##''       as Variable_Name,                                                     -- Variable Name
                              ##^3^##           as Category,                                                          -- Category (variable name)
                              count(*),                                                                               -- Sky Base
                              sum(case when Vespa_Panel_Flag = 1 then Scaling_Weight else 0 end)                      -- Population Coverage
                          from SC_Metrics_Rep_tmp_Universe
                         group by Scaling_Date, Category
                      commit
                    '

        --   ##^1^##  - variable group - group defines which page within the rport variable appears on
        --   ##^2^##  - variable name - used within the report in lookups
        --   ##^3^##  - categories (in SQL use table variable name)
      execute(replace( replace( replace( @varSQL, '##^1^##', 'scaling' ), '##^2^##', 'Universe' ), '##^3^##', 'Universe' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Universe', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'scaling' ), '##^2^##', 'Region' ), '##^3^##', 'Region' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Region', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'scaling' ), '##^2^##', 'Household Composition' ), '##^3^##', 'HH_Composition' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Household Composition', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'scaling' ), '##^2^##', 'Tenure' ), '##^3^##', 'Tenure' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Tenure', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'scaling' ), '##^2^##', 'TV Package' ), '##^3^##', 'TV_Package' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: TV Package', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'scaling' ), '##^2^##', 'Box Type' ), '##^3^##', 'Box_Type' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Box Type', null


      execute(replace( replace( replace( @varSQL, '##^1^##', 'misc' ), '##^2^##', 'Value Segment' ), '##^3^##', 'Value_Segment' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Value Segment', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'misc' ), '##^2^##', 'Experian Mosaic' ), '##^3^##', 'Experian_Mosaic' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Experian Mosaic', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'misc' ), '##^2^##', 'Financial Strategy Segments' ), '##^3^##', 'Financial_Strategy_Segm' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: Financial Strategy Segments', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'misc' ), '##^2^##', 'OnNet / OffNet area' ), '##^3^##', 'OnOffNet_Area' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: OnNet / OffNet area', null

      execute(replace( replace( replace( @varSQL, '##^1^##', 'misc' ), '##^2^##', 'SkyGO users' ), '##^3^##', 'SkyGO_User' ))
      execute logger_add_event @varBuildId, 3, 'Summary created: SkyGO users', null


      update Vespa_Analysts.SC_Metrics_Rep_03_Variables
         set Convergence     = Sky_Base - Population_Coverage,
             Convergence_Abs = abs(Sky_Base - Population_Coverage)
      commit

      execute logger_add_event @varBuildId, 3, 'Convergence calculated', @@rowcount



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Variables] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


