/* CBI UAT Process
Project Name: Capping UAT (CBI)
Project Code: 90
*/
-----Acceptance Test----
---Checking to ascertain that there are no null Panel ID's---TSTIQ_SMI_ETL.CAPPING_METADATA_DIM

SELECT Panelid,
count(*)as hits
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20120809
 Group by Panelid
 
 ----Another way----
 SELECT
count(*)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20120819
 where Panelid is null
 
 ----Subscriber ID----This is just for one day, run for all the dates----
 select count(*)
 from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20120809
 where (Subscriberid is null or Subscriberid <0)

 ------Event Type---
 
 select count(*)
 from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20120809
 where Event_type is null
 
 ---The below stands for the event start time and end times -----
 SELECT count(*)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20120819
   where Adjusted_Event_start_Time is null
 
 SELECT count(*)as hits
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20120819
   where X_Adjusted_Event_End_Time is null
 
 
 /*----Checking to ascertain that we don't have cases where the playbackspeed is not null and the recorded start or end time is null)
 Also, note that if an event is live, the event start and end time will be same as above i.e Adjusted_Event_start_Time and X_Adjusted_Event_End_Time. If it is recorded then the start time and end times are, Recorded time and recorded end time 
 Note that these are substitutes for Viewing start time and Viewing end time)*/
 
 SELECT count(*)as hits
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20120809
  where (Playbackspeed is not null) and (Recordedtime is null or Recordedendtime is null)
 
  
  /*Checking the Recorded start and end times which are other wise the viewing start and end times if not live
  */
  
 select count(*)
 from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20120809
 where ( Recordedtime is not null or recordedendtime is not null )and Playbackspeed is null
