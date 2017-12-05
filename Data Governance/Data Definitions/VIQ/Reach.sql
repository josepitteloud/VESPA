/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Sample code for deriving the Reach metric for VIQ (derived from 
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


CREATE VARIABLE @varProgrammeId unsigned int;
CREATE VARIABLE @varObsWindow smallint;               -- Define the maximum time period of the broadcast time to look for events (playback & polling)
CREATE VARIABLE @varProgrammeStart datetime;          -- Programme start minute
CREATE VARIABLE @varProgrammeEnd datetime;            -- Programme end minute
CREATE VARIABLE @varProgrammeDuration smallint;       -- Programme duration (in minutes)

CREATE VARIABLE @varReach int;
CREATE VARIABLE @varUniverse int;

SELECT @varProgrammeId      = 12345;
SELECT @varObsWindow        = 716;            -- 716 hours / 30 days
SELECT @varProgrammeStart   = (SELECT Prog_Start_Minute FROM programmes WHERE programme_trans_sk = @varProgrammeId);
SELECT @varProgrammeEnd     = (SELECT Prog_End_Minute FROM programmes WHERE programme_trans_sk = @varProgrammeId);
SELECT @varProgrammeDuration= (SELECT Prog_Duration FROM programmes WHERE programme_trans_sk = @varProgrammeId);


  -- Programme audience calculation
SELECT
      count(distinct
                CASE
                  WHEN (ev.Event_Start <= @varProgrammeStart) AND (ev.Event_End >= @varProgrammeEnd) AND (datediff(MINUTE, @varProgrammeStart, @varProgrammeEnd) + 1 >= 3) THEN Household
                  WHEN (ev.Event_Start > @varProgrammeStart) AND (ev.Event_End < @varProgrammeEnd) AND (datediff(MINUTE, ev.Event_Start, ev.Event_End) + 1 >= 3) THEN Household
                  WHEN (ev.Event_Start > @varProgrammeStart) AND (ev.Event_End >= @varProgrammeEnd) AND (datediff(MINUTE, ev.Event_Start, @varProgrammeEnd) + 1 >= 3) THEN Household
                  WHEN (ev.Event_Start <= @varProgrammeStart) AND (ev.Event_End < @varProgrammeEnd) AND (datediff(MINUTE, @varProgrammeStart, ev.Event_End) + 1 >= 3) THEN Household
                    ELSE NULL
                END) INTO @varReach
  FROM events ev
 WHERE ev.programme_trans_sk = @varProgrammeId
   AND ev.Event_Start <= @varProgrammeEnd
   AND ev.Event_End >= @varProgrammeStart
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
         END;


  -- Universe calculation
SELECT
      count(distinct Household) INTO @varUniverse
  FROM events ev;



  -- Reach calculation
SELECT
      @varReach as Reach,
      1.0 * @varReach / @varUniverse as ReachPerc;


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									                    #####
-- ##############################################################################################################

-- ##############################################################################################################
-- ##### STEP 2.0 - Vespa Phase 2 Converted Query							    #####
-- ##############################################################################################################


/*###############################################################################
#Prerequisites:
#- Each event record must have Event Start, Event End and Duration values calculated
#- Programme Start Minute, Programme End Minute and Programme Duration must be available
#
###############################################################################*/



CREATE VARIABLE @varProgrammeId unsigned int;
CREATE VARIABLE @varObsWindow smallint;               -- Define the maximum time period of the broadcast time to look for events (playback & polling)
CREATE VARIABLE @varProgrammeStart datetime;          -- Programme start minute
CREATE VARIABLE @varProgrammeEnd datetime;            -- Programme end minute
CREATE VARIABLE @varProgrammeDuration smallint;       -- Programme duration (in minutes)

CREATE VARIABLE @varReach int;
CREATE VARIABLE @varUniverse int;

SELECT @varProgrammeId      = 12345;
SELECT @varObsWindow        = 716;            -- 716 hours / 30 days
--SELECT @varProgrammeStart   = (SELECT broadcast_start_date_time_utc  FROM sk_prod.vespa_programme_schedule WHERE pk_programme_instance_dim = @varProgrammeId);
--SELECT @varProgrammeEnd     = (SELECT broadcast_end_date_time_utc  FROM sk_prod.vespa_programme_schedule WHERE pk_programme_instance_dim = @varProgrammeId);

--TK ADDED TO TAKE INTO ACCOUNT MINUTE ATTRIBUTION RULES

SELECT @varProgrammeStart   = (SELECT case when datepart(ss,broadcast_start_date_time_utc) >= 31
then dateadd(second,(60 - datepart(ss,broadcast_start_date_time_utc)), broadcast_start_date_time_utc)
when datepart(ss,broadcast_start_date_time_utc) <= 30
then dateadd(second, -(datepart(ss,broadcast_start_date_time_utc)) , broadcast_start_date_time_utc)
else null end prog_start_minute FROM SK_PROD.VESPA_PROGRAMME_SCHEDULE WHERE PK_PROGRAMME_INSTANCE_DIM = @varProgrammeId);

SELECT @varProgrammeEnd     = (SELECT case when datepart(ss,broadcast_end_date_time_utc) >= 31
then dateadd(second,( - datepart(ss,broadcast_end_date_time_utc)), broadcast_end_date_time_utc)
when datepart(ss,broadcast_end_date_time_utc) <= 30
then dateadd(second, -(datepart(ss,broadcast_end_date_time_utc) + 60) , broadcast_end_date_time_utc)
else null end prog_end_minute FROM SK_PROD.VESPA_PROGRAMME_SCHEDULE WHERE PK_PROGRAMME_INSTANCE_DIM = @varProgrammeId);



SELECT @varProgrammeDuration= (SELECT programme_instance_duration FROM sk_prod.vespa_programme_schedule WHERE pk_programme_instance_dim = @varProgrammeId);


  -- Programme audience calculation
SELECT
      count(distinct
                CASE
                  WHEN (ev.INSTANCE_START_DATE_TIME_UTC <= @varProgrammeStart) AND (ev.INSTANCE_END_DATE_TIME_UTC >= @varProgrammeEnd) AND (datediff(MINUTE, @varProgrammeStart, @varProgrammeEnd) + 1 >= 3) THEN account_number
                  WHEN (ev.INSTANCE_START_DATE_TIME_UTC > @varProgrammeStart) AND (ev.INSTANCE_END_DATE_TIME_UTC < @varProgrammeEnd) AND (datediff(MINUTE, ev.INSTANCE_START_DATE_TIME_UTC, ev.INSTANCE_END_DATE_TIME_UTC) + 1 >= 3) THEN account_number
                  WHEN (ev.INSTANCE_START_DATE_TIME_UTC > @varProgrammeStart) AND (ev.INSTANCE_END_DATE_TIME_UTC >= @varProgrammeEnd) AND (datediff(MINUTE, ev.INSTANCE_START_DATE_TIME_UTC, @varProgrammeEnd) + 1 >= 3) THEN account_number
                  WHEN (ev.INSTANCE_START_DATE_TIME_UTC <= @varProgrammeStart) AND (ev.INSTANCE_END_DATE_TIME_UTC < @varProgrammeEnd) AND (datediff(MINUTE, @varProgrammeStart, ev.INSTANCE_END_DATE_TIME_UTC) + 1 >= 3) THEN account_number
                    ELSE NULL
                END) INTO @varReach
  FROM sk_prod.vespa_events_all ev
 WHERE ev.dk_programme_instance_dim = @varProgrammeId
   AND ev.INSTANCE_START_DATE_TIME_UTC <= @varProgrammeEnd
   AND ev.INSTANCE_end_DATE_TIME_UTC >= @varProgrammeStart
   AND (
        type_of_viewing_event = 'TV Channel Viewing' OR
        type_of_viewing_event = 'HD Viewing Event' OR
        (
          type_of_viewing_event = 'Other Service Viewing Event' AND
          SERVICE_TYPE_DESCRIPTION = 'High Definition TV test service'
        ) OR
        type_of_viewing_event = 'Sky+ time-shifted viewing event'
      )
  AND Video_Playing_Flag = 1
  AND (PlayBack_Speed = 2 OR PlayBack_Speed IS NULL)
  AND INSTANCE_START_DATE_TIME_UTC IS NOT NULL
  AND INSTANCE_END_DATE_TIME_UTC IS NOT NULL
  AND EVENT_START_DATE_TIME_UTC >= broadcast_start_date_time_utc
  AND EVENT_START_DATE_TIME_UTC <= dateadd( HOUR, @varObsWindow, broadcast_start_date_time_utc)
  AND event_end_date_time_utc <= dateformat( getdate(), 'yyyy-mm-dd 01:29:59' )
  AND log_start_date_time_utc <=
         CASE
           WHEN dateformat( EVENT_START_DATE_TIME_UTC, 'hh:mm:ss' ) <= '05:59:59'
                THEN dateformat( dateadd(DAY, 1, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' )
             ELSE dateformat( dateadd(DAY, 2, EVENT_START_DATE_TIME_UTC), 'yyyy-mm-dd 05:59:59' )
         END;


  -- Universe calculation
SELECT
      count(distinct ACCOUNT_NUMBER) INTO @varUniverse
  FROM events ev;



  -- Reach calculation
SELECT
      @varReach as Reach,
      1.0 * @varReach / @varUniverse as ReachPerc;

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
