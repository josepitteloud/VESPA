/*###############################################################################
# Created on:   09/08/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Capping Metrics Report - Report output: Overview
#                 This procedure prepares output for the "Overview" page
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - final data preparation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - CAP_Metrics_Rep_tmp_Universe
#     - CAP_Metrics_Rep_tmp_Viewing_Records
#     - CAP_Metrics_Rep_tmp_Value_Segment_Dates
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 09/08/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('CAP_Metrics_Rep_Overview') is not null then drop procedure CAP_Metrics_Rep_Overview end if;
create procedure CAP_Metrics_Rep_Overview
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

      set @varProcessIdentifier        = 'CAPMetRep_Ovrvw_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Capping Metrics Report [Overview] - process started #######', null
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
      delete from Vespa_Analysts.CAP_Metrics_Rep_02_Overview base
        from CAP_Metrics_Rep_tmp_Value_Segment_Dates det
       where base.Event_Date = det.Event_Date
      commit

      execute logger_add_event @varBuildId, 3, 'Deleted current period dates', @@rowcount



        -- ##############################################################################################################
        -- ##### STEP 2.0 - publishing summaries                                                                    #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Publishing summaries <<<<<', null

        -- Individual variables - summaries
      set @varSQL = '
                      insert into Vespa_Analysts.CAP_Metrics_Rep_02_Overview
                             (Event_Date, Variable_Group, Variable_Name, Category, Num_Subscriber_Ids, Num_Accounts,
                              Total_Precap_Viewing, Total_Postcap_Viewing_Src, Total_Postcap_Viewing_Augs)
                        select
                              acc.Event_Date,
                              ''##^1^##''       as Variable_Group,                                                    -- Variable Group (Scaling/Misc etc.)
                              ''##^2^##''       as Variable_Name,                                                     -- Variable Name
                              trim(##^3^##)     as Category,                                                          -- Category (variable name)
                              count(distinct vw.Subscriber_Id),
                              count(distinct acc.Account_Number),
                              sum(vw.Instance_Duration),
                              sum(vw.Instance_Duration_Capped),
                              sum(vw.Instance_Duration_Capped_Augs)
                          from CAP_Metrics_Rep_tmp_Universe acc,
                               CAP_Metrics_Rep_tmp_Viewing_Records vw
                         where acc.Account_Number = vw.Account_Number
                           and acc.Event_Date = vw.Event_Start_Date
                         group by acc.Event_Date, Category
                      commit

                      execute logger_add_event ' || @varBuildId || ', 3, ''Summary created: ##^1^## [##^2^##]'', @@rowcount
                    '

        --   ##^1^##  - variable group
        --   ##^2^##  - variable name
        --   ##^3^##  - categories (in SQL use table variable name)
      execute(replace( replace( replace( @varSQL, '##^1^##', '(Overall)' ), '##^2^##', '(all)' ), '##^3^##', '''(all)''' ))

      execute(replace( replace( replace( @varSQL, '##^1^##', 'Viewing Type' ), '##^2^##', 'Timeshift Type' ), '##^3^##', 'vw.Timeshift_Type' ))

      execute(replace( replace( replace( @varSQL, '##^1^##', 'Scaling Variables' ), '##^2^##', 'Universe' ), '##^3^##', 'acc.Universe' ))
      execute(replace( replace( replace( @varSQL, '##^1^##', 'Scaling Variables' ), '##^2^##', 'Region' ), '##^3^##', 'acc.Region' ))
      execute(replace( replace( replace( @varSQL, '##^1^##', 'Scaling Variables' ), '##^2^##', 'Household Composition' ), '##^3^##', 'acc.HH_Composition' ))
      execute(replace( replace( replace( @varSQL, '##^1^##', 'Scaling Variables' ), '##^2^##', 'Tenure' ), '##^3^##', 'acc.Tenure' ))
      execute(replace( replace( replace( @varSQL, '##^1^##', 'Scaling Variables' ), '##^2^##', 'TV Package' ), '##^3^##', 'acc.TV_Package' ))
      execute(replace( replace( replace( @varSQL, '##^1^##', 'Scaling Variables' ), '##^2^##', 'Box Type' ), '##^3^##', 'acc.Box_Type' ))

      execute(replace( replace( replace( @varSQL, '##^1^##', 'Capping Variables' ), '##^2^##', 'Channel Pack' ), '##^3^##', 'vw.Channel_Pack' ))
      execute(replace( replace( replace( @varSQL, '##^1^##', 'Capping Variables' ), '##^2^##', 'Programme Genre' ), '##^3^##', 'vw.Programme_Genre' ))

      execute(replace( replace( replace( @varSQL, '##^1^##', 'Week Components' ), '##^2^##', 'Weekday/Weekend' ), '##^3^##', 'vw.Weekday' ))

      execute(replace( replace( replace( @varSQL, '##^1^##', 'Time Periods' ), '##^2^##', 'Capping Time Division' ), '##^3^##', 'vw.Time_Division' ))
      execute(replace( replace( replace( @varSQL, '##^1^##', 'Time Periods' ), '##^2^##', 'Industry Standard Day Part' ), '##^3^##', 'vw.Standard_Day_Parts' ))



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Capping Metrics Report [Overview] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


