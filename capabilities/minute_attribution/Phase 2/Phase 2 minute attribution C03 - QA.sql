/*###############################################################################
# Created on:   29/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation - QA procedure (Phase 1 data)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# - Minute Attribution procedure completed
# - Input table (VESPA_MinAttr_Phase2_02_All_Live_Viewing) with the following fields:
#     - Viewing_Starts
#     - Viewing_Stops
#     - Viewing_Starts_Min
#     - Viewing_Stops_Min
#     - StartMinute_Result
#     - EndMinute_Result
#     - StartEndMinute_Relation
#     - BARB_Minute_Start
#     - BARB_Minute_End
#     - Source
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 29/08/2012  SBE   v01 - initial version
#
###############################################################################*/


if object_id('Minute_Attribution_QA_Phase2_v01') is not null then drop procedure Minute_Attribution_QA_Phase2_v01 endif;
commit;

create procedure Minute_Attribution_QA_Phase2_v01
      @parReportingLevel     tinyint = 5,       -- Value between 1 and 5, 1=only high priority tsks are run, 5=all QA tasks are run
      @parRefreshIdentifier  varchar(40) = ''   -- Logger - refresh identifier
as
begin
 -- #### (procedure start) #####


        -- ###############################################################################
        -- ##### Define and set variables                                            #####
        -- ###############################################################################
      declare @varMinMinuteViewingCutoff      tinyint
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @QA_result                      integer             -- QA result field

      set @varMinMinuteViewingCutoff   = 30  -- Included in the lower range, i.e. "30" means "30 or less" -> viewing
                                             --   must be 31 seconds or more
      set @varProcessIdentifier        = 'Vespa_MinAttrP2QAV01'


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      execute citeam.logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute citeam.logger_add_event @varBuildId, 3, '##### Minute Attribution QA - process started #####'

      execute citeam.logger_add_event @varBuildId, 3, '(note: reporting level is set to ' || @parReportingLevel || ' [1=low, 5=high])'

      set @QA_result = -1
      commit



        -- ##############################################################################################################
        -- ##### REPORTING LEVEL: 1                                                                                 #####
        -- ##############################################################################################################

      if (@parReportingLevel >= 1)
        begin

                  -- ###############################################################################
                  -- ##### StartMinute > EndMinute                                             #####
                  -- ###############################################################################
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
               where BARB_Minute_Start > BARB_Minute_End

              execute citeam.logger_add_event @varBuildId, 2, 'StartMinute > EndMinute [expected: 0]', @QA_result
              set @QA_result = -1
              commit


                  -- ###############################################################################
                  -- ##### Either Start or End Minute is null (only one at the time)           #####
                  -- ###############################################################################
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
               where (BARB_Minute_Start is null and BARB_Minute_End is not null)
                  or (BARB_Minute_Start is not null and BARB_Minute_End is null)

              execute citeam.logger_add_event @varBuildId, 2, 'Either Start or End Minute is null [expected: 0]', @QA_result
              set @QA_result = -1
              commit


                  -- ###############################################################################
                  -- ##### Viewing >30s in a minute minute but the result is NULL              #####
                  -- ###############################################################################
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
               where minute(Viewing_Starts_Min) = minute(Viewing_Stops_Min)
                 and Event_Duration > @varMinMinuteViewingCutoff
                 and BARB_Minute_Start is null
                 and Event_Surfing_Flag = 0

              execute citeam.logger_add_event @varBuildId, 2, 'Viewing >30s in a minute minute but the result is NULL [expected: 0]', @QA_result
              set @QA_result = -1
              commit


                  -- ###############################################################################
                  -- ##### Viewing duration >=60 but the result is NULL                        #####
                  -- ###############################################################################
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
               where datediff(second, Viewing_Starts, Viewing_Stops) >= 60
                 and BARB_Minute_Start is null
                 and Event_Surfing_Flag = 0

              execute citeam.logger_add_event @varBuildId, 2, 'Viewing duration >=60 but the result is NULL [expected: 0]', @QA_result
              set @QA_result = -1
              commit


                  -- ###############################################################################
                  -- ##### Result defaulted to 01/01/1900 (start minute)                        #####
                  -- ###############################################################################
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
               where BARB_Minute_Start = '1900-01-01 00:00:00'

              execute citeam.logger_add_event @varBuildId, 2, 'Result defaulted to 01/01/1900 (start minute) [expected: 0]', @QA_result
              set @QA_result = -1
              commit


                  -- ###############################################################################
                  -- ##### Result defaulted to 01/01/1900 (end minute)                        #####
                  -- ###############################################################################
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
               where datediff(second, Viewing_Starts, Viewing_Stops) >= 60
                 and BARB_Minute_End = '1900-01-01 00:00:00'

              execute citeam.logger_add_event @varBuildId, 2, 'Result defaulted to 01/01/1900 (end minute) [expected: 0]', @QA_result
              set @QA_result = -1
              commit

        end



        -- ##############################################################################################################
        -- ##### REPORTING LEVEL: 2                                                                                 #####
        -- ##############################################################################################################

      if (@parReportingLevel >= 2)
        begin

                -- ###############################################################################
                -- ##### Start & End minute result mismatch for events starting and ending   #####
                -- ##### in the same minute                                                  #####
                -- ###############################################################################
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
               where StartMinute_Result <> EndMinute_Result and StartEndMinute_Relation = 2

              execute citeam.logger_add_event @varBuildId, 2, 'Start/End minute result mismatch where EventStartMin = EventEndMin [expected: 0]', @QA_result
              set @QA_result = -1
              commit

        end



        -- ##############################################################################################################
        -- ##### REPORTING LEVEL: 3                                                                                 #####
        -- ##############################################################################################################

      if (@parReportingLevel >= 3)
        begin

                -- ###############################################################################
                -- #####                                                                     #####
                -- ###############################################################################

              set @QA_result = -1
              commit

        end



        -- ##############################################################################################################
        -- ##### REPORTING LEVEL: 4                                                                                 #####
        -- ##############################################################################################################

      if (@parReportingLevel >= 4)
        begin

                -- ###############################################################################
                -- ##### Report volume of multiple occurences of attribution for the same    #####
                -- ##### minute for a single Subscriber_Id                                   #####
                -- ###############################################################################
              select @QA_result = count(1)
                from (select
                            Subscriber_Id,
                            BARB_Minute_Start,
                            min(Source) as Min_Source,
                            max(Source) as Max_Source,
                            count(*) as Records
                        from (select
                                    Subscriber_Id,
                                    BARB_Minute_Start,
                                    BARB_Minute_End,
                                    '0) Viewing' as Source
                                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
                               where BARB_Minute_Start is not null
                               union all
                              select
                                    Subscriber_Id,
                                    surf_minute_start as BARB_Minute_Start,
                                    surf_minute_end as BARB_Minute_End,
                                    '1) Surfing' as Source
                                from VESPA_SURF_MINUTES_PHASE2) det
                       group by Subscriber_Id, BARB_Minute_Start
                      having count(*) > 1) res


              execute citeam.logger_add_event @varBuildId, 2, 'Multiple occurences of attribution for the same minute (minute overlap) - live events only [expected: 0]', @QA_result
              set @QA_result = -1
              commit


        end



        -- ##############################################################################################################
        -- ##### REPORTING LEVEL: 5                                                                                 #####
        -- ##############################################################################################################

      if (@parReportingLevel >= 5)
        begin

                -- ###############################################################################
                -- #####                                                                     #####
                -- ###############################################################################

              set @QA_result = -1
              commit

        end


      execute citeam.logger_add_event @varBuildId, 3, '##### Minute Attribution QA - process completed #####'

 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



