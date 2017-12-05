/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Sample code for deriving Frequency metric for VIQ (derived from 
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

/*###############################################################################
#Prerequisites:
#- Each event record must have Event Start, Event End and Duration values calculated
#- Programme Start Minute, Programme End Minute and Programme Duration must be available
#
###############################################################################*/


CREATE VARIABLE @varObsWindow smallint;               -- Define the maximum time period of the broadcast time to look for events (playback & polling)
CREATE VARIABLE @varFreqMin smallint;                 -- Define minimum number of events required

SELECT @varObsWindow        = 716;            -- 716 hours / 30 days
SELECT @varFreqMin          = 2;

  -- Metric calculation
SELECT
      count(*) AS Frequency
  FROM (SELECT
              ev.Household,
              count(distinct
                         CASE
                           WHEN (ev.Event_Start <= prg.Prog_Start_Minute) AND (ev.Event_End >= prg.Prog_End_Minute) AND (datediff(MINUTE, prg.Prog_Start_Minute, prg.Prog_End_Minute) + 1 >= 3) THEN prg.programme_trans_sk
                           WHEN (ev.Event_Start > prg.Prog_Start_Minute) AND (ev.Event_End < prg.Prog_End_Minute) AND (datediff(MINUTE, ev.Event_Start, ev.Event_End) + 1 >= 3) THEN prg.programme_trans_sk
                           WHEN (ev.Event_Start > prg.Prog_Start_Minute) AND (ev.Event_End >= prg.Prog_End_Minute) AND (datediff(MINUTE, ev.Event_Start, prg.Prog_End_Minute) + 1 >= 3) THEN prg.programme_trans_sk
                           WHEN (ev.Event_Start <= prg.Prog_Start_Minute) AND (ev.Event_End < prg.Prog_End_Minute) AND (datediff(MINUTE, prg.Prog_Start_Minute, ev.Event_End) + 1 >= 3) THEN prg.programme_trans_sk
                             ELSE null
                         END) AS Events
          FROM events ev,
               programmes prg
         WHERE ev.programme_trans_sk IN ({list of related programmes})
           AND ev.programme_trans_sk = prg.programme_trans_sk
           AND ev.Event_Start <= prg.Prog_End_Minute
           AND ev.Event_End >= prg.Prog_Start_Minute
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
         GROUP  BY Household
        HAVING Events >= @varFreqMin) src;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									                    #####
-- ##############################################################################################################



-- ##############################################################################################################
-- ##### STEP 2.0 - Vespa Phase 2 Converted Query							    #####
-- ##############################################################################################################



CREATE VARIABLE @varObsWindow smallint;               -- Define the maximum time period of the broadcast time to look for events (playback & polling)
CREATE VARIABLE @varFreqMin smallint;                 -- Define minimum number of events required

SELECT @varObsWindow        = 716;            -- 716 hours / 30 days
SELECT @varFreqMin          = 2;

  -- Metric calculation

SELECT
      count(*) AS Frequency
  FROM (SELECT
              ev.ACCOUNT_NUMBER AS household,
              count(distinct
                         CASE
                           WHEN (ev.INSTANCE_START_DATE_TIME_UTC <= prg_inst.prog_start_minute) AND (ev.INSTANCE_END_DATE_TIME_UTC >= prg_inst.prog_end_minute) AND (datediff(MINUTE, prg_inst.prog_start_minute, prg_inst.prog_end_minute) + 1 >= 3) THEN prg.pk_programme_instance_dim
                           WHEN (ev.INSTANCE_START_DATE_TIME_UTC > prg_inst.prog_start_minute) AND (ev.INSTANCE_END_DATE_TIME_UTC < prg_inst.prog_end_minute) AND (datediff(MINUTE, ev.INSTANCE_START_DATE_TIME_UTC, ev.INSTANCE_END_DATE_TIME_UTC) + 1 >= 3) THEN prg.pk_programme_instance_dim
                           WHEN (ev.INSTANCE_START_DATE_TIME_UTC > prg_inst.prog_start_minute) AND (ev.INSTANCE_END_DATE_TIME_UTC >= prg_inst.prog_end_minute) AND (datediff(MINUTE, ev.INSTANCE_START_DATE_TIME_UTC, prg_inst.prog_end_minute) + 1 >= 3) THEN prg.pk_programme_instance_dim
                           WHEN (ev.INSTANCE_START_DATE_TIME_UTC <= prg_inst.prog_start_minute) AND (ev.INSTANCE_END_DATE_TIME_UTC < prg_inst.prog_end_minute) AND (datediff(MINUTE, prg_inst.prog_start_minute, ev.INSTANCE_END_DATE_TIME_UTC) + 1 >= 3) THEN prg.pk_programme_instance_dim
                             ELSE null
                         END) AS Events
          FROM sk_prod.vespa_events_all ev,
               sk_prod.vespa_programme_schedule prg,
		(select pk_programme_instance_dim, 
		case when datepart(ss,broadcast_start_date_time_utc) >= 31
		then dateadd(second,(60 - datepart(ss,broadcast_start_date_time_utc)), broadcast_start_date_time_utc)
		when datepart(ss,broadcast_start_date_time_utc) <= 30
		then dateadd(second, -(datepart(ss,broadcast_start_date_time_utc)) , broadcast_start_date_time_utc)
		else null end prog_start_minute, 
		case when datepart(ss,broadcast_end_date_time_utc) >= 31
		then dateadd(second,( - datepart(ss,broadcast_end_date_time_utc)), broadcast_end_date_time_utc)
		when datepart(ss,broadcast_end_date_time_utc) <= 30
		then dateadd(second, -(datepart(ss,broadcast_end_date_time_utc) + 60) , broadcast_end_date_time_utc)
		else null end prog_end_minute from sk_prod.vespa_programme_schedule  
		where pk_programme_instance_dim in ({list of related programmes}) prg_inst 
         WHERE prg.pk_programme_instance_dim = prg_inst.pk_programme_instance_dim
		and ev.dk_programme_instance_dim  = prg.pk_programme_instance_dim
           AND ev.INSTANCE_START_DATE_TIME_UTC <= prg.broadcast_start_date_time_utc
           AND ev.INSTANCE_END_DATE_TIME_UTC >= prg.broadcast_end_date_time_utc
           AND (                                                                   --(set fo standard filters)
                type_of_viewing_event = 'TV Channel Viewing' OR
                type_of_viewing_event = 'HD Viewing Event' OR
                (
                  type_of_viewing_event = 'Other Service Viewing Event' AND
                  service_type_description = 'High Definition TV test service'
                ) OR
                type_of_viewing_event = 'Sky+ time-shifted viewing event'
              )
          AND Video_Playing_Flag = 1
          AND (PlayBack_Speed = 2 OR PlayBack_Speed IS NULL)    --Time-shifted OR Live event
          AND INSTANCE_START_DATE_TIME_UTC IS NOT NULL          --Must be greater than zero to remove dummy events/instances
          AND INSTANCE_END_DATE_TIME_UTC IS NOT NULL            --Must be greater than zero to remove dummy events/instances
          AND EVENT_START_DATE_TIME_UTC >= broadcast_start_date_time_utc    --Viewing must start on or after programme TX time
          AND EVENT_START_DATE_TIME_UTC <= dateadd( HOUR, @varObsWindow, broadcast_start_date_time_utc) --Viewing must start up to 716 hours of the original broadcast time
          AND event_end_date_time_utc <= dateformat( getdate(), 'yyyy-mm-dd 01:29:59' ) --Viewing must complete by the end of previous day and before Polling Window commences  (time: 01:29:59)
          AND log_start_date_time_utc <=
                 CASE
                   WHEN dateformat( EVENT_START_DATE_TIME_UTC, 'hh:mm:ss' ) <= '05:59:59'
                        THEN dateformat( dateadd(DAY, 1, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' )    --Event must be polled no later than the end of Day31PW (time: 05:59:59) of TX time
                     ELSE dateformat( dateadd(DAY, 2, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' )       --Event must be polled no later than the end of Day1PW (time: 05:59:59) of the event time
                 END
         GROUP  BY ACCOUNT_NUMBER
        HAVING Events >= @varFreqMin) src;

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
