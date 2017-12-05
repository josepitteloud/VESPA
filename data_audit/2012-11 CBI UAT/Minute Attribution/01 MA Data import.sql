/*

CREATE TABLE TSTIQ_DIS_ETL.CAPPED_EVENTS
(
    VIEWING_EVENT_ID BIGINT,
        CHAR_VIEWING_EVENT_ID CHARACTER VARYING(200),
    PANELID INTEGER,
        DOCUMENTCREATIONDATE TIMESTAMP,
        DOCUMENTVERSION INTEGER,
        TVID INTEGER,
        STBLOGCREATIONDATE TIMESTAMP,
    SUBSCRIBERID INTEGER,
        PRIVATEDATA CHARACTER VARYING(200),
        EVSTANDBYIN_ET TIMESTAMP,
        EVSTANDBYOUT_ET TIMESTAMP,
        EVPOWERUP_ET TIMESTAMP,
        EVSURF_ET TIMESTAMP,
        EVTRACKSURF_ET TIMESTAMP,
        EVIDLE_ET TIMESTAMP,
        EVCHANGEVIEW_ET TIMESTAMP,
        ORIGINALNETWORKID CHARACTER VARYING(200),
        TRANSPORTSTREAMID CHARACTER VARYING(200),
    SISERVICEID CHARACTER VARYING(200),
    SERVICEKEY CHARACTER VARYING(200),
    VIDEOPLAYING CHARACTER VARYING(200),
        VIDEOTRACKTAG INTEGER,
        AUDIOTRACKTAG INTEGER,
        DATATRACKTAG INTEGER,
        PRODUCERID INTEGER,
        APPLICATIONID INTEGER,
    RECORDEDTIME TIMESTAMP,
    RECORDEDENDTIME TIMESTAMP,
    PLAYBACKSPEED CHARACTER VARYING(200),
        PARENTFILE CHARACTER VARYING(200),
        CHILDFILE CHARACTER VARYING(200),
        DISRECPROCESSDATE TIMESTAMP,
        FRAGMENT_COUNTER INTEGER,
        EVENT_IN_FRAGMENT_COUNTER INTEGER,
        COUNTER BIGINT,
    ORIGINAL_EVENT_TIME TIMESTAMP,
    EVENT_TYPE CHARACTER VARYING(12),
        INCLUDE_IN_EXTRACT INTEGER,
    ADJUSTED_TIME TIMESTAMP,
    ADJUSTED_TIME_2 TIMESTAMP,
    ADJUSTED_EVENT_START_TIME TIMESTAMP,
        V_VIDEO_TRACK_TAG_CHANGED INTEGER,
    X_ADJUSTED_EVENT_END_TIME TIMESTAMP,
        V_VIDEO_TRACK_TAG_CHANGED2 INTEGER,
        PREV_X_ADJUSTED_EVENT_END_TIME TIMESTAMP,
        EVENT_SEQUENCE_ID BIGINT,
        ORIGINAL_EVENT_SEQUENCE_ID CHARACTER VARYING(1),
        X_DURATION_SINCE_LAST_VIEWING_EVENT BIGINT,
        DATEOFEVENT DATE,
    X_TYPE_OF_VIEWING_EVENT CHARACTER VARYING(31),
    TX_END_DATETIME_UTC TIMESTAMP,
    EVENT_DURATION_SECOND INTEGER,
        EVENT_DURATION_MINUTE NUMERIC(10,2),
        EVENT_DURATION_TIME TIME,
        CAPPING_METADATA_KEY INTEGER,
    SHORT_DURATION_CAPPED_FLAG INTEGER,
    LONG_DURATION_CAPPED_FLAG INTEGER,
    LIVE_EVENT_FLAG INTEGER,
        SILO_TYPE CHARACTER VARYING(10),
        SILO_HOUR INTEGER,
        SILO_BOX CHARACTER VARYING(50),
        SILO_CHANNEL_PACK CHARACTER VARYING(50),
        SILO_GENRE CHARACTER VARYING(50),
        SILO_KEY BIGINT,
        SEGMENT_KEY BIGINT,
        SEGMENT_CHANNEL CHARACTER VARYING(40),
        NTILE_NUMBER INTEGER,
        NTILE_EXISTS_FLAG INTEGER,
        MAX_CUTOFF_FLAG INTEGER,
        NTILE_REQUIRED_FLAG INTEGER,
        MIN_CUTOFF_FLAG INTEGER,
        SEGMENT_PROG_FLAG INTEGER,
        RANDOM_ROW_ID INTEGER,
        ASSIGNED_RANDOM_ROW_ID INTEGER,
    CAPPED_EVENT_END_TIME TIMESTAMP,
        CAPPED_EVENT_DURATION_MINUTE NUMERIC(10,2),
    CAPPED_EVENT_DURATION_SECOND INTEGER,
        CAPPED_EVENT_DURATION_TIME TIME,
        ADJUSTED_EVENT_START_HOUR_LOCAL INTEGER,
        ADJUSTED_EVENT_START_DATE_LOCAL DATE,
        ADJUSTED_EVENT_START_TIME_LOCAL TIME,
        SILO_DATE DATE,
)
DISTRIBUTE ON RANDOM;
*/





-- ###############################################################
-- ###############################################################
-- ####  Netezza  ################################################
-- ###############################################################
-- ###############################################################
  -- Basic checks
select count(*) - count(distinct VIEWING_EVENT_ID) as diff
  from TSTIQ_DIS_PREPARE..CAPPED_EVENTS;

select count(*) - count(distinct VIEWING_EVENT_ID) as diff
  from TSTIQ_DIS_PREPARE..FINAL_MINUTE_ATTRIBUTION;

select
      date(ADJUSTED_EVENT_START_TIME) as dt,
      count(*) as Records_Cnt,
      count(distinct subscriberid) as Subs_Id,
      count(*) - count(distinct VIEWING_EVENT_ID) as diff
from TSTIQ_DIS_PREPARE..CAPPED_EVENTS
group by dt
order by dt




CREATE EXTERNAL TABLE 'C:\\_Playpen_\\2012-10-24 VIQ CBI UAT\\SBE CBI UAT Input 1.csv'
USING
(
	DELIMITER ','
	Y2BASE 2000
	ENCODING 'internal'
	REMOTESOURCE 'ODBC'
	ESCAPECHAR '\'
)
AS
  select
        VIEWING_EVENT_ID,
        PANELID,
        SUBSCRIBERID,
        SISERVICEID,
        SERVICEKEY,
        VIDEOPLAYING,
        RECORDEDTIME,
        RECORDEDENDTIME,
        PLAYBACKSPEED,
        ORIGINAL_EVENT_TIME,
        EVENT_TYPE,
        ADJUSTED_TIME,
        ADJUSTED_TIME_2,
        ADJUSTED_EVENT_START_TIME,
        X_ADJUSTED_EVENT_END_TIME,
        X_TYPE_OF_VIEWING_EVENT,
        TX_END_DATETIME_UTC,
        EVENT_DURATION_SECOND,
        LIVE_EVENT_FLAG,
        CAPPED_EVENT_END_TIME
    from TSTIQ_DIS_PREPARE..CAPPED_EVENTS
   where date(ADJUSTED_EVENT_START_TIME) = '2012-11-05';



CREATE EXTERNAL TABLE 'C:\\_Playpen_\\2012-10-24 VIQ CBI UAT\\SBE CBI UAT Input 2.csv'
USING
(
	DELIMITER ','
	Y2BASE 2000
	ENCODING 'internal'
	REMOTESOURCE 'ODBC'
	ESCAPECHAR '\'
)
AS
  select
        VIEWING_EVENT_ID,
        PANELID,
        SUBSCRIBERID,
        SISERVICEID,
        SERVICEKEY,
        VIDEOPLAYING,
        RECORDEDTIME,
        RECORDEDENDTIME,
        PLAYBACKSPEED,
        ORIGINAL_EVENT_TIME,
        EVENT_TYPE,
        ADJUSTED_TIME,
        ADJUSTED_TIME_2,
        ADJUSTED_EVENT_START_TIME,
        X_ADJUSTED_EVENT_END_TIME,
        X_TYPE_OF_VIEWING_EVENT,
        TX_END_DATETIME_UTC,
        EVENT_DURATION_SECOND,
        LIVE_EVENT_FLAG,
        CAPPED_EVENT_END_TIME
    from TSTIQ_DIS_PREPARE..CAPPED_EVENTS
   where date(ADJUSTED_EVENT_START_TIME) = '2012-11-06';



CREATE EXTERNAL TABLE 'C:\\_Playpen_\\2012-10-24 VIQ CBI UAT\\SBE CBI UAT Results 1.csv'
USING
(
	DELIMITER ','
	Y2BASE 2000
	ENCODING 'internal'
	REMOTESOURCE 'ODBC'
	ESCAPECHAR '\'
)
AS
  select *
    from TSTIQ_DIS_PREPARE..FINAL_MINUTE_ATTRIBUTION
   where date(event_start_datetime) = '2012-11-05';



CREATE EXTERNAL TABLE 'C:\\_Playpen_\\2012-10-24 VIQ CBI UAT\\SBE CBI UAT Results 1.csv'
USING
(
	DELIMITER ','
	Y2BASE 2000
	ENCODING 'internal'
	REMOTESOURCE 'ODBC'
	ESCAPECHAR '\'
)
AS
  select *
    from TSTIQ_DIS_PREPARE..FINAL_MINUTE_ATTRIBUTION
   where date(event_start_datetime) = '2012-11-06';




-- ###############################################################'
-- ###############################################################
-- ####  Sybase  #################################################
-- ###############################################################
-- ###############################################################
DROP TABLE MinAttrUAT_01_Raw;
CREATE TABLE MinAttrUAT_01_Raw (
    VIEWING_EVENT_ID BIGINT,
    PANELID INTEGER,
    SUBSCRIBERID INTEGER,
    SISERVICEID VARCHAR(200),
    SERVICEKEY VARCHAR(200),
    VIDEOPLAYING VARCHAR(200),
    RECORDEDTIME TIMESTAMP,
    RECORDEDENDTIME TIMESTAMP,
    PLAYBACKSPEED VARCHAR(200),
    ORIGINAL_EVENT_TIME TIMESTAMP,
    EVENT_TYPE VARCHAR(12),
    ADJUSTED_TIME TIMESTAMP,
    ADJUSTED_TIME_2 TIMESTAMP,
    ADJUSTED_EVENT_START_TIME TIMESTAMP,
    X_ADJUSTED_EVENT_END_TIME TIMESTAMP,
    X_TYPE_OF_VIEWING_EVENT VARCHAR(31),
    TX_END_DATETIME_UTC TIMESTAMP,
    EVENT_DURATION_SECOND INTEGER,
    LIVE_EVENT_FLAG INTEGER,
    CAPPED_EVENT_END_TIME TIMESTAMP
);
create hg index idx1 on MinAttrUAT_01_Raw(SUBSCRIBERID);
create dttm index idx2 on MinAttrUAT_01_Raw(ADJUSTED_EVENT_START_TIME);
create hg index idx3 on MinAttrUAT_01_Raw(VIEWING_EVENT_ID);


DROP TABLE MinAttrUAT_01_Raw__01;
CREATE TABLE MinAttrUAT_01_Raw__01 (
    VIEWING_EVENT_ID BIGINT,
    PANELID INTEGER,
    SUBSCRIBERID INTEGER,
    SISERVICEID VARCHAR(200),
    SERVICEKEY VARCHAR(200),
    VIDEOPLAYING VARCHAR(200),
    RECORDEDTIME TIMESTAMP,
    RECORDEDENDTIME TIMESTAMP,
    PLAYBACKSPEED VARCHAR(200),
    ORIGINAL_EVENT_TIME TIMESTAMP,
    EVENT_TYPE VARCHAR(12),
    ADJUSTED_TIME TIMESTAMP,
    ADJUSTED_TIME_2 TIMESTAMP,
    ADJUSTED_EVENT_START_TIME TIMESTAMP,
    X_ADJUSTED_EVENT_END_TIME TIMESTAMP,
    X_TYPE_OF_VIEWING_EVENT VARCHAR(31),
    TX_END_DATETIME_UTC TIMESTAMP,
    EVENT_DURATION_SECOND INTEGER,
    LIVE_EVENT_FLAG INTEGER,
    CAPPED_EVENT_END_TIME TIMESTAMP
);
create hg index idx1 on MinAttrUAT_01_Raw__01(SUBSCRIBERID);
create dttm index idx2 on MinAttrUAT_01_Raw__01(ADJUSTED_EVENT_START_TIME);
create hg index idx3 on MinAttrUAT_01_Raw__01(VIEWING_EVENT_ID);


DROP TABLE MinAttrUAT_01_Raw__02;
CREATE TABLE MinAttrUAT_01_Raw__02 (
    VIEWING_EVENT_ID BIGINT,
    PANELID INTEGER,
    SUBSCRIBERID INTEGER,
    SISERVICEID VARCHAR(200),
    SERVICEKEY VARCHAR(200),
    VIDEOPLAYING VARCHAR(200),
    RECORDEDTIME TIMESTAMP,
    RECORDEDENDTIME TIMESTAMP,
    PLAYBACKSPEED VARCHAR(200),
    ORIGINAL_EVENT_TIME TIMESTAMP,
    EVENT_TYPE VARCHAR(12),
    ADJUSTED_TIME TIMESTAMP,
    ADJUSTED_TIME_2 TIMESTAMP,
    ADJUSTED_EVENT_START_TIME TIMESTAMP,
    X_ADJUSTED_EVENT_END_TIME TIMESTAMP,
    X_TYPE_OF_VIEWING_EVENT VARCHAR(31),
    TX_END_DATETIME_UTC TIMESTAMP,
    EVENT_DURATION_SECOND INTEGER,
    LIVE_EVENT_FLAG INTEGER,
    CAPPED_EVENT_END_TIME TIMESTAMP
);
create hg index idx1 on MinAttrUAT_01_Raw__02(SUBSCRIBERID);
create dttm index idx2 on MinAttrUAT_01_Raw__02(ADJUSTED_EVENT_START_TIME);
create hg index idx3 on MinAttrUAT_01_Raw__02(VIEWING_EVENT_ID);


DROP TABLE MinAttrUAT_02_Results;
CREATE TABLE MinAttrUAT_02_Results
(
	VIEWING_EVENT_ID BIGINT,
	SCMS_SUBSCRIBER_ID INTEGER,
	SERVICE_KEY VARCHAR(200),
	TYPE_OF_VIEWING_EVENT VARCHAR(200),
	VIEWING_EVENT_TYPE VARCHAR(200),
	EVENT_START_DATETIME TIMESTAMP,
	EVENT_END_CAPPED_DATETIME TIMESTAMP,
	TX_EVENT_START_DATETIME TIMESTAMP,
	TX_EVENT_END_CAPPED_DATETIME TIMESTAMP,
	CAPPED_EVENT_DURATION_SECOND INTEGER,
	ATTRIBUTION_VALID INTEGER,
	ATTRIBUTION_SERVICE_KEY VARCHAR(200),
	ATTRIBUTION_START TIMESTAMP,
	ATTRIBUTION_END TIMESTAMP
);
create hg index idx1 on MinAttrUAT_02_Results(SCMS_SUBSCRIBER_ID);
create dttm index idx2 on MinAttrUAT_02_Results(EVENT_START_DATETIME);
create hg index idx3 on MinAttrUAT_02_Results(VIEWING_EVENT_ID);


DROP TABLE MinAttrUAT_02_Results__01;
CREATE TABLE MinAttrUAT_02_Results__01
(
	VIEWING_EVENT_ID BIGINT,
	SCMS_SUBSCRIBER_ID INTEGER,
	SERVICE_KEY VARCHAR(200),
	TYPE_OF_VIEWING_EVENT VARCHAR(200),
	VIEWING_EVENT_TYPE VARCHAR(200),
	EVENT_START_DATETIME TIMESTAMP,
	EVENT_END_CAPPED_DATETIME TIMESTAMP,
	TX_EVENT_START_DATETIME TIMESTAMP,
	TX_EVENT_END_CAPPED_DATETIME TIMESTAMP,
	CAPPED_EVENT_DURATION_SECOND INTEGER,
	ATTRIBUTION_VALID INTEGER,
	ATTRIBUTION_SERVICE_KEY VARCHAR(200),
	ATTRIBUTION_START TIMESTAMP,
	ATTRIBUTION_END TIMESTAMP
);
create hg index idx1 on MinAttrUAT_02_Results__01(SCMS_SUBSCRIBER_ID);
create dttm index idx2 on MinAttrUAT_02_Results__01(EVENT_START_DATETIME);
create hg index idx3 on MinAttrUAT_02_Results__01(VIEWING_EVENT_ID);


DROP TABLE MinAttrUAT_02_Results__02;
CREATE TABLE MinAttrUAT_02_Results__02
(
	VIEWING_EVENT_ID BIGINT,
	SCMS_SUBSCRIBER_ID INTEGER,
	SERVICE_KEY VARCHAR(200),
	TYPE_OF_VIEWING_EVENT VARCHAR(200),
	VIEWING_EVENT_TYPE VARCHAR(200),
	EVENT_START_DATETIME TIMESTAMP,
	EVENT_END_CAPPED_DATETIME TIMESTAMP,
	TX_EVENT_START_DATETIME TIMESTAMP,
	TX_EVENT_END_CAPPED_DATETIME TIMESTAMP,
	CAPPED_EVENT_DURATION_SECOND INTEGER,
	ATTRIBUTION_VALID INTEGER,
	ATTRIBUTION_SERVICE_KEY VARCHAR(200),
	ATTRIBUTION_START TIMESTAMP,
	ATTRIBUTION_END TIMESTAMP
);
create hg index idx1 on MinAttrUAT_02_Results__02(SCMS_SUBSCRIBER_ID);
create dttm index idx2 on MinAttrUAT_02_Results__02(EVENT_START_DATETIME);
create hg index idx3 on MinAttrUAT_02_Results__02(VIEWING_EVENT_ID);




-- ###############################################################
-- ####  Load data ###############################################
-- ###############################################################
delete from MinAttrUAT_01_Raw__01;
commit;
load table  MinAttrUAT_01_Raw__01
(
  VIEWING_EVENT_ID',',
  PANELID',',
  SUBSCRIBERID',',
  SISERVICEID',',
  SERVICEKEY',',
  VIDEOPLAYING',',
  RECORDEDTIME',',
  RECORDEDENDTIME',',
  PLAYBACKSPEED',',
  ORIGINAL_EVENT_TIME',',
  EVENT_TYPE',',
  ADJUSTED_TIME',',
  ADJUSTED_TIME_2',',
  ADJUSTED_EVENT_START_TIME',',
  X_ADJUSTED_EVENT_END_TIME',',
  X_TYPE_OF_VIEWING_EVENT',',
  TX_END_DATETIME_UTC',',
  EVENT_DURATION_SECOND',',
  LIVE_EVENT_FLAG',',
  CAPPED_EVENT_END_TIME'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/SBE CBI UAT Input 3.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 200000
DELIMITED BY ','
;



delete from MinAttrUAT_01_Raw__02;
commit;
load table  MinAttrUAT_01_Raw__02
(
  VIEWING_EVENT_ID',',
  PANELID',',
  SUBSCRIBERID',',
  SISERVICEID',',
  SERVICEKEY',',
  VIDEOPLAYING',',
  RECORDEDTIME',',
  RECORDEDENDTIME',',
  PLAYBACKSPEED',',
  ORIGINAL_EVENT_TIME',',
  EVENT_TYPE',',
  ADJUSTED_TIME',',
  ADJUSTED_TIME_2',',
  ADJUSTED_EVENT_START_TIME',',
  X_ADJUSTED_EVENT_END_TIME',',
  X_TYPE_OF_VIEWING_EVENT',',
  TX_END_DATETIME_UTC',',
  EVENT_DURATION_SECOND',',
  LIVE_EVENT_FLAG',',
  CAPPED_EVENT_END_TIME'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/SBE CBI UAT Input 2.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 200000
DELIMITED BY ','
;




delete from MinAttrUAT_02_Results__01;
commit;
load table  MinAttrUAT_02_Results__01
(
	VIEWING_EVENT_ID',',
	SCMS_SUBSCRIBER_ID',',
	SERVICE_KEY',',
	TYPE_OF_VIEWING_EVENT',',
	VIEWING_EVENT_TYPE',',
	EVENT_START_DATETIME',',
	EVENT_END_CAPPED_DATETIME',',
	TX_EVENT_START_DATETIME',',
	TX_EVENT_END_CAPPED_DATETIME',',
	CAPPED_EVENT_DURATION_SECOND',',
	ATTRIBUTION_VALID',',
	ATTRIBUTION_SERVICE_KEY',',
	ATTRIBUTION_START',',
	ATTRIBUTION_END'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/SBE CBI UAT Results 3.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 200000
DELIMITED BY ','
;



delete from MinAttrUAT_02_Results__02;
commit;
load table  MinAttrUAT_02_Results__02
(
	VIEWING_EVENT_ID',',
	SCMS_SUBSCRIBER_ID',',
	SERVICE_KEY',',
	TYPE_OF_VIEWING_EVENT',',
	VIEWING_EVENT_TYPE',',
	EVENT_START_DATETIME',',
	EVENT_END_CAPPED_DATETIME',',
	TX_EVENT_START_DATETIME',',
	TX_EVENT_END_CAPPED_DATETIME',',
	CAPPED_EVENT_DURATION_SECOND',',
	ATTRIBUTION_VALID',',
	ATTRIBUTION_SERVICE_KEY',',
	ATTRIBUTION_START',',
	ATTRIBUTION_END'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/SBE CBI UAT Results 2.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 200000
DELIMITED BY ','
;



/*
input into MinAttrUAT_01_Raw__02
 from 'D:\Temp\SBE\2012-08-08 Input.csv' format ascii;
commit;
*/

/*
-- Field mapping
    VIEWING_EVENT_ID            => pk_viewing_prog_instance_fact
    PANELID                     => Panel_Id
    SUBSCRIBERID                => Subscriber_Id
    SERVICEKEY                  => dk_channel_dim
    VIDEOPLAYING                => video_playing_flag
    RECORDEDTIME                => DERIVE time_in_seconds_since_recording
    PLAYBACKSPEED               => playback_speed
    ADJUSTED_EVENT_START_TIME   => instance_start_date_time_utc
    X_ADJUSTED_EVENT_END_TIME   => instance_end_date_time_utc
    X_TYPE_OF_VIEWING_EVENT     => type_of_viewing_event
    TX_END_DATETIME_UTC         => DERIVE time_in_seconds_since_recording
    SHORT_DURATION_CAPPED_FLAG  =>
    LONG_DURATION_CAPPED_FLAG   =>
    LIVE_EVENT_FLAG             => live flag
    CAPPED_EVENT_END_TIME       =>
*/
    VIEWING_EVENT_ID,
    PANELID,
    SUBSCRIBERID,
    SISERVICEID,
    SERVICEKEY,
    VIDEOPLAYING,
    RECORDEDTIME,
    RECORDEDENDTIME,
    PLAYBACKSPEED,
    ORIGINAL_EVENT_TIME,
    EVENT_TYPE,
    ADJUSTED_TIME,
    ADJUSTED_TIME_2,
    ADJUSTED_EVENT_START_TIME,
    X_ADJUSTED_EVENT_END_TIME,
    X_TYPE_OF_VIEWING_EVENT,
    TX_END_DATETIME_UTC,
    EVENT_DURATION_SECOND,
    LIVE_EVENT_FLAG



-- ###############################################################
-- ####  Merge sources ###########################################
-- ###############################################################
truncate table MinAttrUAT_01_Raw;

insert into MinAttrUAT_01_Raw
  select * from MinAttrUAT_01_Raw__01;

insert into MinAttrUAT_01_Raw
  select * from MinAttrUAT_01_Raw__02;
commit;


truncate table MinAttrUAT_02_Results;

insert into MinAttrUAT_02_Results
  select * from MinAttrUAT_02_Results__01;

insert into MinAttrUAT_02_Results
  select * from MinAttrUAT_02_Results__02;
commit;


/*
  -- ### Adjust some values - this is for manual loads only ###
select x_type_of_viewing_event, count(*) from MinAttrUAT_01_Raw group by x_type_of_viewing_event;
select LIVE_EVENT_FLAG, count(*) from MinAttrUAT_01_Raw group by LIVE_EVENT_FLAG;

delete from MinAttrUAT_01_Raw
 where viewing_event_id is null;


update MinAttrUAT_01_Raw set x_type_of_viewing_event = 'TV Channel Viewing' where x_type_of_viewing_event = 'V Channel Viewing';
update MinAttrUAT_01_Raw set x_type_of_viewing_event = 'Sky+ time-shifted viewing event' where x_type_of_viewing_event = 'Sky+  ime-shif ed viewing even';
update MinAttrUAT_01_Raw set x_type_of_viewing_event = 'Digital Radio Viewing' where x_type_of_viewing_event = 'Digi al Radio Viewing';
update MinAttrUAT_01_Raw set x_type_of_viewing_event = 'Other Service Viewing Event' where x_type_of_viewing_event = 'O her Service Viewing Even';
update MinAttrUAT_01_Raw set VIDEOPLAYING = 'true' where VIDEOPLAYING = 'rue';



select type_of_viewing_event, count(*) as cnt from MinAttrUAT_02_Results group by type_of_viewing_event;
select viewing_event_type, count(*) as cnt from MinAttrUAT_02_Results group by viewing_event_type;

delete from MinAttrUAT_02_Results
 where viewing_event_id is null;

update MinAttrUAT_02_Results set viewing_event_type = 'TV Channel Viewing' where viewing_event_type = 'V Channel Viewing';
update MinAttrUAT_02_Results set viewing_event_type = 'Sky+ time-shifted viewing event' where viewing_event_type = 'Sky+  ime-shif ed viewing even';
update MinAttrUAT_02_Results set viewing_event_type = 'Other Service Viewing Event' where viewing_event_type = 'O her Service Viewing Even';
*/


  -- Check duplicates
select count(*) - count(distinct VIEWING_EVENT_ID) as diff
from  MinAttrUAT_01_Raw
;
select count(*) - count(distinct VIEWING_EVENT_ID) as diff
from  MinAttrUAT_02_Results
;





  -- ### Create deduped input table ###
if object_id('MinAttrUAT_03_MinAttr_CBI') is not null then drop table MinAttrUAT_03_MinAttr_CBI end if;
CREATE TABLE MinAttrUAT_03_MinAttr_CBI (
      pk_viewing_prog_instance_fact       int         identity,
      VIEWING_EVENT_ID                    BIGINT      default null,
      Subscriber_Id                       int         default null,
      instance_start_date_time_utc        datetime    default null,
      instance_end_date_time_utc          datetime    default null,
      capped_event_end_time               datetime    default null,
      dk_programme_dim                    int         default null,
      dk_channel_dim                      int         default null,
      time_in_seconds_since_recording     int         default null,
      type_of_viewing_event               varchar(50) default null,
      playback_speed                      tinyint     default null,
      service_type_description            varchar(50) default null,
      video_playing_flag                  tinyint     default null,
      CBI_minute_start                    datetime    default null,
      CBI_minute_end                      datetime    default null,
      barb_minute_start                   datetime    default null,
      barb_minute_end                     datetime    default null,
);

create hg index idx1 on MinAttrUAT_03_MinAttr_CBI(Subscriber_Id);
create dttm index idx2 on MinAttrUAT_03_MinAttr_CBI(instance_start_date_time_utc);
create hg index idx3 on MinAttrUAT_03_MinAttr_CBI(pk_viewing_prog_instance_fact);
create hg index idx4 on MinAttrUAT_03_MinAttr_CBI(VIEWING_EVENT_ID);


truncate table MinAttrUAT_03_MinAttr_CBI;
insert into MinAttrUAT_03_MinAttr_CBI
              (VIEWING_EVENT_ID, Subscriber_Id, instance_start_date_time_utc, instance_end_date_time_utc, capped_event_end_time,
               dk_programme_dim, dk_channel_dim, time_in_seconds_since_recording, type_of_viewing_event, playback_speed,
               service_type_description, video_playing_flag)
  select
      VIEWING_EVENT_ID,
      SUBSCRIBERID,
      ADJUSTED_EVENT_START_TIME,
      CAPPED_EVENT_END_TIME,
      --X_ADJUSTED_EVENT_END_TIME,
      CAPPED_EVENT_END_TIME,
      0,
      case
            when SISERVICEID is null then SERVICEKEY
              else SISERVICEID
      end,
      case
        when X_TYPE_OF_VIEWING_EVENT = 'Sky+ time-shifted viewing event' then datediff(second, RECORDEDTIME, ADJUSTED_EVENT_START_TIME)
          else null
      end,
      X_TYPE_OF_VIEWING_EVENT,
      case
        when cast(PLAYBACKSPEED as tinyint) is null then 1
        when cast(PLAYBACKSPEED as tinyint)= 2 then 1
          else cast(PLAYBACKSPEED as tinyint)
      end,
      '',
      case
        when VIDEOPLAYING = 'true' then 1
          else 0
      end
    from MinAttrUAT_01_Raw;
commit;

select * from MinAttrUAT_03_MinAttr_CBI;

  -- Check duplicates
select count(*) - count(distinct VIEWING_EVENT_ID) as diff
from  MinAttrUAT_03_MinAttr_CBI
;



  -- ### Data quality checks ###

  -- Check for different attribution per subscriber/event record
  -- expected all 1
/*
select scms_subscriber_id, event_Start_datetime, count(distinct case when attribution_start is null then cast('1900-01-01 00:00:00' as datetime) else attribution_start end) as cnt
  from  MinAttrUAT_02_Results
 where attribution_start is not null
 group by  scms_subscriber_id, event_Start_datetime
 order by cnt desc;

  -- check for proportion of surfing & long duration events
select
      case
        when datediff(second, instance_start_date_time_utc, instance_end_date_time_utc) <= 15 then '1) Surfing'
        when datediff(second, instance_start_date_time_utc, instance_end_date_time_utc) >= 5 * 60 * 60 then '7) 5hrs+'
        when datediff(second, instance_start_date_time_utc, instance_end_date_time_utc) >= 4 * 60 * 60 then '6) 4hrs+'
        when datediff(second, instance_start_date_time_utc, instance_end_date_time_utc) >= 3 * 60 * 60 then '5) 3hrs+'
        when datediff(second, instance_start_date_time_utc, instance_end_date_time_utc) >= 2 * 60 * 60 then '4) 2hrs+'
        when datediff(second, instance_start_date_time_utc, instance_end_date_time_utc) >= 1 * 60 * 60 then '3) 1hrs+'
          else '2) within 3 hrs'
      end as DurationGroup,
      sum(case
            when CBI_minute_start is not null then 1
              else 0
          end) as AtributedCnt,
      count(*) as Cnt
  from MinAttrUAT_03_MinAttr_CBI
 group by DurationGroup
 order by DurationGroup
;
*/


  -- ### Append CBI minute attribution results ###
update MinAttrUAT_03_MinAttr_CBI base
   set base.CBI_minute_start  = det.Attribution_Start,
       base.CBI_minute_end    = det.Attribution_End
  from MinAttrUAT_02_Results det
 where base.viewing_event_id = det.viewing_event_id
commit;




  -- ### Run Minute Attribution ###
execute MA_UAT;
execute logger_get_latest_job_events 'MA_UAT', 4;


  -- ### Tests ###
if object_id('MinAttrUAT_tmp_Duplicate_Ids') is not null then drop table MinAttrUAT_tmp_Duplicate_Ids end if;
select
      VIEWING_EVENT_ID,
      cast(1 as tinyint) as Dummy
  into MinAttrUAT_tmp_Duplicate_Ids
  from MinAttrUAT_03_MinAttr_CBI
 group by VIEWING_EVENT_ID
 having count(*) > 1;
commit;

create unique hg index idx1 on MinAttrUAT_tmp_Duplicate_Ids(VIEWING_EVENT_ID);




select a.*
  from MinAttrUAT_03_MinAttr_CBI a left join MinAttrUAT_tmp_Duplicate_Ids b
    on a.VIEWING_EVENT_ID = b.VIEWING_EVENT_ID
 where (
          (CBI_minute_start is null and barb_minute_start is not null) or
          (CBI_minute_start is not null and barb_minute_start is null) or
          (CBI_minute_end is null and barb_minute_end is not null) or
          (CBI_minute_end is not null and barb_minute_end is null)
       )
   and b.VIEWING_EVENT_ID is null
 order by subscriber_id, instance_start_date_time_utc;



select a.*
  from MinAttrUAT_03_MinAttr_CBI a left join MinAttrUAT_tmp_Duplicate_Ids b
    on a.VIEWING_EVENT_ID = b.VIEWING_EVENT_ID
 where CBI_minute_start is not null
   and barb_minute_start is not null
   and CBI_minute_end is not null
   and barb_minute_end is not null
   and (
        CBI_minute_start <> barb_minute_start
        or
        CBI_minute_end <> barb_minute_end
       )
   and b.VIEWING_EVENT_ID is null
   and instance_start_date_time_utc < instance_end_date_time_utc
 order by a.subscriber_id, a.instance_start_date_time_utc;




  -- ### Sybase ###
select
      Viewing_event_Id,
      Subscriber_Id,
      video_playing_flag              as Video_Playing_Flag,
      type_of_viewing_event           as Type_Of_Event,
      dk_channel_dim                  as Channel_Id,
      instance_start_date_time_utc    as Event_Start_Time,
      instance_end_date_time_utc      as Event_End_Time,
      datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
                                      as Duration,
      time_in_seconds_since_recording as Time_Since_Recording,
      case
        when (type_of_viewing_event = 'Sky+ time-shifted viewing event') then
                dateadd(second, -time_in_seconds_since_recording, instance_start_date_time_utc)
          else null
      end                             as Recorded_Time,
      case
        when (type_of_viewing_event = 'Sky+ time-shifted viewing event') then
                dateadd(second, Duration, Recorded_Time)
          else null
      end                             as Recorded_Time_End,
      CBI_minute_start                as CBI_minute_start  ,
      CBI_minute_end                  as CBI_minute_end,
      barb_minute_start               as barb_minute_start,
      barb_minute_end                 as barb_minute_end
  from MinAttrUAT_03_MinAttr_CBI
 where Subscriber_Id in (
24472, 58574,88058,282167
 )
 order by Subscriber_Id, instance_start_date_time_utc, instance_end_date_time_utc;


select *
  from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
 where subscriber_id in (
12594955

 )
 order by subscriber_id, minute, viewing_starts, viewing_stops;





  -- ### Netezza ###
  select
      a.VIEWING_EVENT_ID,
      a.SUBSCRIBERID,
      case
        when a.VIDEOPLAYING = 'true' then 1
          else 0
      end as Video_Playing_Flag,
      a.X_TYPE_OF_VIEWING_EVENT,
      case
            when a.SISERVICEID is null then a.SERVICEKEY
              else a.SISERVICEID
      end as ChannelId,
      a.ADJUSTED_EVENT_START_TIME,
      a.X_ADJUSTED_EVENT_END_TIME,
      a.EVENT_DURATION_SECOND,
      a.RECORDEDTIME as Recorded_Time_Start,
      a.RECORDEDENDTIME as Recorded_Time_End,
      a.PLAYBACKSPEED,
      b.ATTRIBUTION_START,
      b.ATTRIBUTION_END
  from TSTIQ_DIS_PREPARE..CAPPED_EVENTS a,
       TSTIQ_DIS_PREPARE..FINAL_MINUTE_ATTRIBUTION b
 where a.VIEWING_EVENT_ID = b.VIEWING_EVENT_ID
   and SUBSCRIBERID in (
   24472, 58574,88058,282167
   )
 order by SUBSCRIBERID, ADJUSTED_EVENT_START_TIME, X_ADJUSTED_EVENT_END_TIME;














