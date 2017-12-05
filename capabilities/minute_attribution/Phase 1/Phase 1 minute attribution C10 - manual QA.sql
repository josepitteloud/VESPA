/*###############################################################################
# Created on:   06/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation - collection of queries for QA
#               purposes (Phase 1 data)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# - Minute Attribution procedure completed
# - Input table (VESPA_BARBMin_02_All_Live_Viewing) with the following fields:
#     - viewing_starts
#     - viewing_stops
#     - Viewing_Starts_Min
#     - Viewing_Stops_Min
#     - StartMinute_Result
#     - EndMinute_Result
#     - StartEndMinute_Relation
#     - BARB_Minute_Start
#     - BARB_Minute_End
#     - source
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2012  SBE   v01 - initial version
#
###############################################################################*/


create variable @varMinMinuteViewingCutoff tinyint;

select @varMinMinuteViewingCutoff   = 30  -- Included in the lower range, i.e. "30" means "30 or less" -> viewing
                                          --   must be 31 seconds or more



  -- ###############################################################################
  -- ##### Search for non-allowed scenarios                                    #####
  -- ###############################################################################
select
      case
        when ( StartMinute_Result <> EndMinute_Result and StartEndMinute_Relation = 2 ) then '1) Start/End minute result mismatch (StartMin=EndMin)'
        when ( BARB_Minute_Start > BARB_Minute_End ) then '2) StartMinute > EndMinute'
        when ( (BARB_Minute_Start is null and BARB_Minute_End is not null) or (BARB_Minute_Start is not null and BARB_Minute_End is null) ) then '3) Either Start or End Minute is null'
        when ( minute(Viewing_Starts_Min) = minute(Viewing_Stops_Min) and Event_Duration > @varMinMinuteViewingCutoff and BARB_Minute_Start is null ) then '4) Viewing >30s within one minute but result is NULL'
        when ( datediff(second, viewing_starts, viewing_stops) >= 60 and BARB_Minute_Start is null ) then '5) Viewing duration >=60 but result is NULL'
        when ( BARB_Minute_Start = '1900-01-01 00:00:00' ) then '6) Result defaulted to 01/01/1900 (start minute)'
        when ( BARB_Minute_End = '1900-01-01 00:00:00' ) then '7) Result defaulted to 01/01/1900 (end minute)'
          else '??'
      end Error,
      count(*) as Volume
  from VESPA_BARBMin_02_All_Live_Viewing
 where
       ( StartMinute_Result <> EndMinute_Result and StartEndMinute_Relation = 2 )
    or ( BARB_Minute_Start > BARB_Minute_End )
    or ( (BARB_Minute_Start is null and BARB_Minute_End is not null) or (BARB_Minute_Start is not null and BARB_Minute_End is null) )
    or ( minute(Viewing_Starts_Min) = minute(Viewing_Stops_Min) and Event_Duration > @varMinMinuteViewingCutoff and BARB_Minute_Start is null )
    or ( datediff(second, viewing_starts, viewing_stops) >= 60 and BARB_Minute_Start is null )
    or ( BARB_Minute_Start = '1900-01-01 00:00:00' )
    or ( BARB_Minute_End = '1900-01-01 00:00:00' )
 group by Error
 ;


  -- ###############################################################################
  -- ##### Search for multiple occurences of attribution for the same minute   #####
  -- ##### within a single subscriber_id                                       #####
  -- ###############################################################################
select
      subscriber_id,
      BARB_Minute_Start,
      min(Source) as Min_Source,
      max(Source) as Max_Source,
      count(*) as Records
  from (select
              subscriber_id,
              BARB_Minute_Start,
              BARB_Minute_End,
              '0) Viewing' as Source
          from VESPA_BARBMin_02_All_Live_Viewing
         where BARB_Minute_Start is not null
         union all
        select
              subscriber_id,
              surf_minute_start as BARB_Minute_Start,
              surf_minute_end as BARB_Minute_End,
              '1) Surfing' as Source
          from VESPA_SURF_MINUTES) det
 group by subscriber_id, BARB_Minute_Start
 having count(*) > 1;



  -- ###############################################################################
  -- ##### Data export for manual QA - Live                                    #####
  -- ###############################################################################
select *
  from (select
              subscriber_id,
              Channel_Identifier,
              viewing_starts,
              viewing_stops,
              Event_Duration as Duration,
              StartMinute_Result,
              EndMinute_Result,
              BARB_Minute_Start,
              BARB_Minute_End,
              case
                when BARB_Minute_Start is not null then 'Viewing'
                  else ''
              end as Source
          from VESPA_BARBMin_02_All_Live_Viewing
         union all
        select
              subscriber_id,
              null,
              surf_minute_start as viewing_starts,
              surf_minute_end as viewing_stops,
              null,
              null,
              null,
              surf_minute_start as BARB_Minute_Start,
              surf_minute_end as BARB_Minute_End,
              '#### Surfing ####' as Source
          from VESPA_SURF_MINUTES) det
 order by subscriber_id, viewing_starts, viewing_stops, BARB_Minute_Start, BARB_Minute_End;


  -- ###############################################################################
  -- ##### Data export for manual QA - Playback                                #####
  -- ###############################################################################
select
      subscriber_id,
      viewing_starts,
      viewing_stops,
      Viewing_Duration as Duration,
      Recorded_Time_UTC as Recorded_Time_Start,
      dateadd(second, Viewing_Duration - 1, Recorded_Time_Start) as Recorded_Time_End,
      BARB_Minute_Start,
      BARB_Minute_End
  from VESPA_BARBMin_01_Viewing_Delta
 where Live_Flag = 0
 order by subscriber_id, Recorded_Time_Start, Recorded_Time_End, BARB_Minute_Start, BARB_Minute_End;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################

























