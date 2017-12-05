/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Sample code for deriving the Timeshift 7 day metric for VIQ (derived from 
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


CREATE VARIABLE @varTXDate date;
CREATE VARIABLE @varObsWindow unsigned smallint;
SELECT @varTXDate = '2011-02-06';      -- "Day0" definition
SELECT @varObsWindow = 164;            -- 164 hours / 7 days


WHERE
      x_type_of_viewing_event = 'Sky+ time-shifted viewing event'
  AND Video_Playing_Flag = 1
  AND Play_Back_Speed = 2
  AND X_Viewing_Start_Time IS NOT NULL
  AND X_Viewing_End_Time IS NOT NULL
  AND TX_Start_Datetime_UTC >= dateformat( @varTXDate, 'yyyy-mm-dd 06:00:00' )
  AND TX_Start_Datetime_UTC <= dateformat( dateadd(DAY, 1, @varTXDate), 'yyyy-mm-dd 05:59:59' )
  AND Adjusted_Event_Start_Time >= dateformat( dateadd(DAY, 1, @varTXDate), 'yyyy-mm-dd 01:30:00' )
  AND X_Adjusted_Event_End_Time <= dateformat( getdate(), 'yyyy-mm-dd 01:29:59' )
  AND Adjusted_Event_Start_Time <= dateadd( HOUR, @varObsWindow, TX_Start_Datetime_UTC )
  AND Document_Creation_Date <=
         CASE
           WHEN dateformat( Adjusted_Event_Start_Time, 'hh:mm:ss' ) <= '05:59:59'
                THEN dateformat( dateadd(DAY, 1, Adjusted_Event_Start_Time), 'yyyy-mm-dd 05:59:59' )
             ELSE dateformat( dateadd(DAY, 2, Adjusted_Event_Start_Time), 'yyyy-mm-dd 05:59:59' )
         END

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									                    #####
-- ##############################################################################################################


-- ##############################################################################################################
-- ##### STEP 2.0 - Vespa Phase 2 Converted Query							    #####
-- ##############################################################################################################


CREATE VARIABLE @varTXDate date;
CREATE VARIABLE @varObsWindow unsigned smallint;
SELECT @varTXDate = '2011-02-06';      -- "Day0" definition
SELECT @varObsWindow = 164;            -- 164 hours / 7 days


WHERE
      type_of_viewing_event = 'Sky+ time-shifted viewing event' --Time-shifted event
  AND Video_Playing_Flag = 1
  AND playback_speed = 2 --Time-shifted event
  AND INSTANCE_START_DATE_TIME_UTC IS NOT NULL --Must be greater than zero to remove dummy events/instances
  AND INSTANCE_END_DATE_TIME_UTC IS NOT NULL --Must be greater than zero to remove dummy events/instances
  AND broadcast_start_date_time_utc >= dateformat( @varTXDate, 'yyyy-mm-dd 06:00:00' ) --Adjust TX start time to the beginning of Day0
  AND broadcast_start_date_time_utc <= dateformat( dateadd(DAY, 1, @varTXDate), 'yyyy-mm-dd 05:59:59' ) --Adjust TX start time to the end of Day0
  AND EVENT_START_DATE_TIME_UTC >= dateformat( dateadd(DAY, 1, @varTXDate), 'yyyy-mm-dd 01:30:00' ) --Viewing must start on or after Day0PW as VOSDAL events are excluded
  AND event_end_date_time_utc <= dateformat( getdate(), 'yyyy-mm-dd 01:29:59' ) --Viewing must complete by the end of previous day and before Polling Window commences  (time: 01:29:59)
  AND EVENT_START_DATE_TIME_UTC <= dateadd( HOUR, @varObsWindow, broadcast_start_date_time_utc ) --Viewing must start up to 164 hours of the original broadcat time
  AND log_start_date_time_utc <=    --Event must be polled no later than the end of Day1PW (time: 05:59:59) of the event time
         CASE
           WHEN dateformat( EVENT_START_DATE_TIME_UTC, 'hh:mm:ss' ) <= '05:59:59'
                THEN dateformat( dateadd(DAY, 1, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' ) --If Event time was between 00:00 and 05:59 - allow 1 days for polling
             ELSE dateformat( dateadd(DAY, 2, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' ) --If Event time was between 06:00 and 23:59 - allow 2 days for polling
         END

-- ##############################################################################################################
-- ##### STEP 2.0 Ended									                    #####
-- ##############################################################################################################

-- ##############################################################################################################
-- ##### STEP 3.0 - Columns mapping							    		    #####
-- ##############################################################################################################


--Video_Playing_Flag  VESPA_EVENTS_ALL
--x_type_of_viewing_event - type_of_viewing_event VESPA_EVENTS_ALL
--play_back_speed - playback_speed VESPA_EVENTS_ALL
--x_viewing_start_time -  INSTANCE_START_DATE_TIME_UTC    VESPA_EVENTS_ALL
--tx_start_datetime_utc - broadcast_start_date_time_utc VESPA_EVENTS_ALL
--Adjusted_Event_Start_Time - EVENT_START_DATE_TIME_UTC
--X_Adjusted_Event_End_Time - event_end_date_time_utc
--document_creation_date - log_start_date_time_utc
--x_viewing_end_time -  INSTANCE_START_DATE_TIME_UTC    VESPA_EVENTS_ALL


-- ##############################################################################################################
-- ##### STEP 3.0 Ended									                    #####
-- ##############################################################################################################
