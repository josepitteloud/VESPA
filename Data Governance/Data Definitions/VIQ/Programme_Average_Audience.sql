/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Sample code for deriving a Programme's Average Audience
#		metric for VIQ (derived from 
#		Sebastian Bednaszynski's work) .
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/08/2012  TKD   v01 - initial version
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Original Sebastian Query								    #####
-- ##############################################################################################################


CREATE VARIABLE @varProgrammeId unsigned int;
CREATE VARIABLE @varObsWindow smallint;               -- Define the maximum time period of the broadcast time to look for events (playback & polling)

SELECT @varProgrammeId      = 12345;
SELECT @varObsWindow        = 716;            -- 716 hours / 30 days

SELECT
      prog.programme_trans_sk,
      prog.Prog_Name,
      1.0 * sum(Event_Duration) / max(Prog_Duration) AS Average_Audience
  FROM {eventsTable} ev,
       {programeTable} prog
 WHERE prog.programme_trans_sk = ev.programme_trans_sk
   AND ev.programme_trans_sk = @varProgrammeId
   AND (
        x_type_of_viewing_event = 'TV Channel Viewing' OR
        x_type_of_viewing_event = 'HD Viewing Event' OR
        (
          x_type_of_viewing_event = 'Other Service Viewing Event' AND
          x_si_service_type = 'High Definition TV test service'
        ) OR
        x_type_of_viewing_event = 'Sky+ time-shifted viewing event'
      )
  AND Video_Playing_Flag = 1
  AND (Play_Back_Speed = 2 OR Play_Back_Speed IS NULL)
  AND X_Viewing_Start_Time IS NOT NULL
  AND X_Viewing_End_Time IS NOT NULL
  AND Adjusted_Event_Start_Time >= TX_Start_Datetime_UTC
  AND Adjusted_Event_Start_Time <= dateadd( HOUR, @varObsWindow, TX_Start_Datetime_UTC )
  AND X_Adjusted_Event_End_Time <= dateformat( getdate(), 'yyyy-mm-dd 01:29:59' )
  AND Document_Creation_Date <=
         CASE
           WHEN dateformat( Adjusted_Event_Start_Time, 'hh:mm:ss' ) <= '05:59:59'
                THEN dateformat( dateadd(DAY, 1, Adjusted_Event_Start_Time), 'yyyy-mm-dd 05:59:59' )
             ELSE dateformat( dateadd(DAY, 2, Adjusted_Event_Start_Time), 'yyyy-mm-dd 05:59:59' )
         END
 GROUP BY prog.programme_trans_sk, prog.Prog_Name;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									                    #####
-- ##############################################################################################################

-- ##############################################################################################################
-- ##### STEP 2.0 - Vespa Phase 2 Converted Query							    #####
-- ##############################################################################################################


CREATE VARIABLE @varProgrammeId unsigned int;
CREATE VARIABLE @varObsWindow smallint;               -- Define the maximum time period of the broadcast time to look for events (playback & polling)

SELECT @varProgrammeId      = 12345;
SELECT @varObsWindow        = 716;            -- 716 hours / 30 days

SELECT
      pk_programme_instance_dim,
      programme_instance_name,
      1.0 * sum(Duration) / max(programme_instance_duration) AS Average_Audience    --1.0 to ensure decimal result
  FROM SK_PROD.VESPA_EVENTS_ALL ev
 WHERE ev.dk_programme_instance_dim = @varProgrammeId
   AND (                                                                            --(set fo standard filters)
        type_of_viewing_event = 'TV Channel Viewing' OR
        type_of_viewing_event = 'HD Viewing Event' OR
        (
          type_of_viewing_event = 'Other Service Viewing Event' AND
          service_type_description  = 'High Definition TV test service'
        ) OR
        type_of_viewing_event = 'Sky+ time-shifted viewing event'
      )
  AND Video_Playing_Flag = 1
  AND (PlayBack_Speed = 2 OR PlayBack_Speed IS NULL)                                --Time-shifted OR Live event
  AND INSTANCE_START_DATE_TIME_UTC IS NOT NULL                                      --Must be greater than zero to remove dummy events/instances
  AND INSTANCE_end_DATE_TIME_UTC IS NOT NULL                                        --Must be greater than zero to remove dummy events/instances
  AND EVENT_START_DATE_TIME_UTC >= broadcast_start_date_time_utc                    --Viewing must start on or after programme TX time
  AND EVENT_START_DATE_TIME_UTC <= dateadd( HOUR, @varObsWindow, broadcast_start_date_time_utc) --Viewing must start up to 716 hours of the original broadcast time
  AND event_end_date_time_utc <= dateformat( getdate(), 'yyyy-mm-dd 01:29:59' )     --Viewing must complete by the end of previous day and before Polling Window commences  (time: 01:29:59)
  AND log_start_date_time_utc <=                                                    --Event must be polled no later than the end of Day1PW (time: 05:59:59) of the event time
         CASE
           WHEN dateformat( EVENT_START_DATE_TIME_UTC, 'hh:mm:ss' ) <= '05:59:59'
                THEN dateformat( dateadd(DAY, 1, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' )    --If Event time was between 00:00 and 05:59 - allow 1 days for polling
             ELSE dateformat( dateadd(DAY, 2, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' )       --If Event time was between 06:00 and 23:59 - allow 2 days for polling
         END
 GROUP BY pk_programme_instance_dim, programme_instance_name;

-- ##############################################################################################################
-- ##### STEP 2.0 Ended									                    #####
-- ##############################################################################################################

-- ##############################################################################################################
-- ##### STEP 3.0 - Columns mapping							    		    #####
-- ##############################################################################################################


--Video_Playing_Flag  -   VESPA_EVENTS_ALL
--x_type_of_viewing_event - type_of_viewing_event -   VESPA_EVENTS_ALL
--play_back_speed - playback_speed -   VESPA_EVENTS_ALL
--x_viewing_start_time -  INSTANCE_START_DATE_TIME_UTC-   VESPA_EVENTS_ALL
--X_Viewing_End_Time - INSTANCE_END_DATE_TIME_UTC-   VESPA_EVENTS_ALL
--tx_start_datetime_utc - broadcast_start_date_time_utc VESPA_EVENTS_ALL
--Adjusted_Event_Start_Time - EVENT_START_DATE_TIME_UTC -   VESPA_EVENTS_ALL
--X_Adjusted_Event_End_Time - event_end_date_time_utc -   VESPA_EVENTS_ALL
--document_creation_date - log_start_date_time_utc -   VESPA_EVENTS_ALL
--x_viewing_end_time -  INSTANCE_START_DATE_TIME_UTC -   VESPA_EVENTS_ALL
--x_si_service_type - service_type_description - VESPA_EVENTS_ALL
--programme_trans_sk - dk_programme_instance_dim - VESPA_EVENTS_ALL
--event_start - EVENT_START_DATE_TIME_UTC - VESPA_EVENTS_ALL
--event_end - EVENT_END_DATE_TIME_UTC - VESPA_EVENTS_ALL
--Event_Duration - duration -   VESPA_EVENTS_ALL

--programme_trans_sk - pk_programme_instance_dim - VESPA_PROGRAMME_SCHEDULE
--Prog_Name - programme_instance_name    - VESPA_PROGRAMME_SCHEDULE
--Prog_Duration - programme_instance_duration - VESPA_PROGRAMME_SCHEDULE


-- ##############################################################################################################
-- ##### STEP 3.0 Ended									                    #####
-- ##############################################################################################################
