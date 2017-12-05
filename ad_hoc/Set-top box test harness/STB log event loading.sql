/*
  -- ##### Temporary table, create once in your schema #####
if object_id('STB_Harness_Temp') is not null then drop table STB_Harness_Temp end if;
create table STB_Harness_Temp (
      ID                        bigint        identity,
      EVENT_KEY                 varchar(100)  default null,
      CARDID	                  varchar(20)   default null,
      PANEL_NO	                varchar(20)   default null,
      LOG_CREATION_DATE	        varchar(50)   default null,
      LOG_CREATION_DATE_DT      datetime      default null,
      OFFSET	                  time          default null,
      OFFSET_SECONDS            bigint        default null,
      EVENT	                    varchar(50)   default null,
      SERV_KEY	                int           default null,
      ORG_NW_ID	                int           default null,
      TS_ID	                    int           default null,
      SERV_ID	                  int           default null,
      VIDEO_TAG	                int           default null,
      AUDIO_TAG	                int           default null,
      DATA_TAG	                int           default null,
      PVDR_ID	                  int           default null,
      APP_ID	                  int           default null,
      VIDEO_RUNNING	            int           default null,
      RECORDED_TIME	            varchar(50)   default null,
      RECORDED_TIME_DT          datetime      default null,
      SPEED                     int           default null
);

create        hg index idx1 on STB_Harness_Temp(EVENT_KEY);

grant select on STB_Harness_Temp to vespa_group_low_security;
*/


truncate table STB_Harness_Temp;

input into STB_Harness_Temp
      (CARDID, PANEL_NO, LOG_CREATION_DATE, OFFSET, EVENT, SERV_KEY, ORG_NW_ID, TS_ID, SERV_ID,
       VIDEO_TAG, AUDIO_TAG, DATA_TAG, PVDR_ID, APP_ID, VIDEO_RUNNING, RECORDED_TIME, SPEED)
 from 'C:\_Playpen_\MEASUREMENTS\2013-07-15 STB test harness\SSP_TOOL\SSP_OUT.CSV' format ascii;
commit;

delete from STB_Harness_Temp
 where CARDID = 'CARDID';
commit;

update STB_Harness_Temp a
   set a.EVENT_KEY              = upper(
                                    replace(
                                        replace(
                                            replace(
                                                replace(
                                                    replace( CARDID || LOG_CREATION_DATE || OFFSET, '.000', ''),
                                                        ':', ''),
                                                    '_', ''),
                                                '-', ''),
                                            ' ', '')
                                       ),
       a.LOG_CREATION_DATE_DT   = cast(substr(a.LOG_CREATION_DATE, 5, 19) as datetime),
       a.OFFSET_SECONDS         = (60 * 60 * hour(OFFSET)) + (60 * minute(OFFSET)) + (second(OFFSET)),
       a.RECORDED_TIME_DT       = cast(substr(a.RECORDED_TIME, 5, 19) as datetime);
commit;



insert into VESPA_Analysts.STB_Harness_Events
       (EVENT_KEY, CARDID, PANEL_NO, EVENT_START_DATE_TIME, EVENT_END_DATE_TIME, RECORDED_TIME, TIME_SINCE_RECORDING, EVENT_TYPE,
        SERV_KEY, ORG_NW_ID, TS_ID, SERV_ID, VIDEO_TAG, AUDIO_TAG, DATA_TAG, PVDR_ID, APP_ID, VIDEO_RUNNING,
        OFFSET, OFFSET_SECONDS, SPEED, LOG_CREATION_DATE, LOG_CREATION_DATE_ORIG, RECORDED_TIME_ORIG)
  select
        a.EVENT_KEY,
        cast(a.CARDID as bigint),
        cast(a.PANEL_NO as bigint),
        dateadd(second, a.OFFSET_SECONDS, a.LOG_CREATION_DATE_DT) as Event_Start_Dt,
        lag(Event_Start_Dt, 1) over (partition by a.CARDID order by Event_Start_Dt desc, a.Id desc),
        RECORDED_TIME_DT,
        datediff(second, a.RECORDED_TIME_DT, dateadd(second, a.OFFSET_SECONDS, a.LOG_CREATION_DATE_DT)),
        a.EVENT,
        cast(a.SERV_KEY as bigint),
        cast(a.ORG_NW_ID as bigint),
        cast(a.TS_ID as bigint),
        cast(a.SERV_ID as bigint),
        cast(a.VIDEO_TAG as bigint),
        cast(a.AUDIO_TAG as bigint),
        cast(a.DATA_TAG as bigint),
        cast(a.PVDR_ID as bigint),
        cast(a.APP_ID as bigint),
        cast(a.VIDEO_RUNNING as bigint),
        a.OFFSET,
        a.OFFSET_SECONDS,
        a.SPEED,
        a.LOG_CREATION_DATE_DT,
        a.LOG_CREATION_DATE,
        a.RECORDED_TIME
  from STB_Harness_Temp a
          left join VESPA_Analysts.STB_Harness_Events b    on a.EVENT_KEY = b.EVENT_KEY
 where b.EVENT_KEY is null;
commit;


update VESPA_Analysts.STB_Harness_Events a
   set a.EVENT_DURATION   = datediff(second, EVENT_START_DATE_TIME, EVENT_END_DATE_TIME);
commit;

update VESPA_Analysts.STB_Harness_Events a
   set a.EVENT_END_DATE_TIME   = null,
       a.EVENT_DURATION        = null
 where a.EVENT_TYPE = 'STDBY_IN';
commit;



select * from VESPA_Analysts.STB_Harness_Events order by 1;



/*
--call dba.sp_drop_table('vespa_analysts', 'STB_Harness_Events');
call dba.sp_create_table('vespa_analysts',
                         'STB_Harness_Events',
                         '
                          ID                        bigint        identity,
                          EVENT_KEY                 varchar(100)  default null,

                          CARDID	                  bigint        default null,
                          PANEL_NO	                tinyint       default null,
                          EVENT_START_DATE_TIME     datetime      default null,
                          EVENT_END_DATE_TIME       datetime      default null,
                          RECORDED_TIME	            datetime      default null,
                          TIME_SINCE_RECORDING      smallint      default null,
                          EVENT_DURATION            bigint        default null,
                          EVENT_TYPE                varchar(50)   default null,
                          SERV_KEY	                int           default null,
                          ORG_NW_ID	                int           default null,
                          TS_ID	                    int           default null,
                          SERV_ID	                  int           default null,
                          VIDEO_TAG	                int           default null,
                          AUDIO_TAG	                int           default null,
                          DATA_TAG	                int           default null,
                          PVDR_ID	                  int           default null,
                          APP_ID	                  int           default null,
                          VIDEO_RUNNING	            int           default null,
                          OFFSET	                  time          default null,
                          OFFSET_SECONDS            bigint        default null,
                          SPEED                     int           default null,
                          LOG_CREATION_DATE         datetime      default null,
                          LOG_CREATION_DATE_ORIG    varchar(50)   default null,
                          RECORDED_TIME_ORIG        varchar(50)   default null,
                         '
                        );

create        hg index idx1 on VESPA_Analysts.STB_Harness_Events(EVENT_KEY);
create      dttm index idx2 on VESPA_Analysts.STB_Harness_Events(EVENT_START_DATE_TIME);
create      dttm index idx3 on VESPA_Analysts.STB_Harness_Events(EVENT_END_DATE_TIME);
create        lf index idx4 on VESPA_Analysts.STB_Harness_Events(EVENT_TYPE);
create        lf index idx5 on VESPA_Analysts.STB_Harness_Events(SERV_KEY);
*/