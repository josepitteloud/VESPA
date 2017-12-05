/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Sample code for deriving Live Viewing metric for VIQ (derived from 
#		Sebastian Bednaszynski's work) .
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/08/2012  TKD   v01 - initial version
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


CREATE VARIABLE @varTXDate date;
SELECT @varTXDate = '2011-02-06';      -- "Day0" definition


WHERE (
        x_type_of_viewing_event = 'TV Channel Viewing' OR
        x_type_of_viewing_event = 'HD Viewing Event' OR
        (
          x_type_of_viewing_event = 'Other Service Viewing Event' AND
          x_si_service_type = 'High Definition TV test service'
        )
      )
  AND Video_Playing_Flag = 1
  AND Play_Back_Speed IS NULL
  AND X_Viewing_Start_Time IS NOT NULL
  AND X_Viewing_End_Time IS NOT NULL
  AND @varTXDate < dateformat( getdate(), 'yyyy-mm-dd' )
  AND TX_Start_Datetime_UTC >= dateformat( @varTXDate, 'yyyy-mm-dd 06:00:00' )
  AND TX_Start_Datetime_UTC <= dateformat( dateadd(DAY, 1, @varTXDate), 'yyyy-mm-dd 05:59:59')
  AND Adjusted_Event_Start_Time >= TX_Start_Datetime_UTC
  AND X_Adjusted_Event_End_Time <= dateformat( getdate(), 'yyyy-mm-dd 01:29:59' )
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
SELECT @varTXDate = '2011-02-06';      -- "Day0" definition


WHERE (
        type_of_viewing_event = 'TV Channel Viewing' OR
        type_of_viewing_event = 'HD Viewing Event' OR
        (
          type_of_viewing_event = 'Other Service Viewing Event' AND
          service_type_description = 'High Definition TV test service'
        )
      )
  AND Video_Playing_Flag = 1
  AND PlayBack_Speed IS NULL                    --Live event
  AND INSTANCE_START_DATE_TIME_UTC IS NOT NULL  --Must be greater than zero to remove dummy events/instances
  AND INSTANCE_END_DATE_TIME_UTC IS NOT NULL    --Must be greater than zero to remove dummy events/instances
  AND @varTXDate < dateformat( getdate(), 'yyyy-mm-dd' )    --Calculation cannot be performed on the same day as TX date
  AND broadcast_start_date_time_utc >= dateformat( @varTXDate, 'yyyy-mm-dd 06:00:00' )  --Adjust TX start time to the beginning of Day0 (time: 06:00:00)
  AND broadcast_start_date_time_utc <= dateformat( dateadd(DAY, 1, @varTXDate), 'yyyy-mm-dd 05:59:59')  --Adjust TX start time to the end of Day0 (time: 05:59:59 the following day)
  AND EVENT_START_DATE_TIME_UTC >= broadcast_start_date_time_utc    --Viewing must start on or after programme TX time
  AND event_end_date_time_utc <= dateformat( getdate(), 'yyyy-mm-dd 01:29:59' ) --Viewing must complete by the end of previous day and before Polling Window commences  (time: 01:29:59)
  AND log_start_date_time_utc <=                                                --Event must be polled no later than the end of Day1PW (time: 05:59:59) of the event time
         CASE
           WHEN dateformat( EVENT_START_DATE_TIME_UTC, 'hh:mm:ss' ) <= '05:59:59'
                THEN dateformat( dateadd(DAY, 1, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' )    --If Event time was between 00:00 and 05:59 - allow 1 days for polling
             ELSE dateformat( dateadd(DAY, 2, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' )       --If Event time was between 06:00 and 23:59 - allow 2 days for polling
         END

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
--Household - cb_key_household - VESPA_EVENTS_ALL


--programme_trans_sk - pk_programme_instance_dim - VESPA_PROGRAMME_SCHEDULE
--Prog_Name - programme_instance_name    - VESPA_PROGRAMME_SCHEDULE
--Prog_Duration - programme_instance_duration - VESPA_PROGRAMME_SCHEDULE
--Prog_Start_Minute - broadcast_start_date_time_utc - VESPA_PROGRAMME_SCHEDULE
--Prog_End_Minute - broadcast_end_date_time_utc - VESPA_PROGRAMME_SCHEDULE

-- ##############################################################################################################
-- ##### STEP 3.0 Ended									                    #####
-- ##############################################################################################################
