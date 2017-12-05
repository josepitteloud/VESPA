-- ###############################################################
-- ###############################################################
-- ####  Netezza  ################################################
-- ###############################################################
-- ###############################################################


drop table SBE_DUPLICATE_EVENT_IDS;

select
		VIEWING_EVENT_ID,
		count(*) as Cnt
  into SBE_DUPLICATE_EVENT_IDS
  from TSTIQ_DIS_PREPARE..CAPPED_EVENTS
 group by VIEWING_EVENT_ID
having count(*) > 1;




drop table SBE_INTO_PROGRAMME_UAT;

select
      a.viewing_event_id,
      a.subscriberid,
      a.videoplaying,
      a.stblogcreationdate,
      a.recordedtime,
      a.playbackspeed,

      a.broadcast_start_datetime_utc,
      a.broadcast_end_datetime_utc,

      a.adjusted_event_start_time,
      a.x_adjusted_event_end_time,
      --a.x_viewing_start_time,
      --a.x_viewing_end_time,
      case
        when a.ADJUSTED_EVENT_START_TIME + a.PROGRAMME_START_INTERVAL > a.ADJUSTED_EVENT_START_TIME then a.ADJUSTED_EVENT_START_TIME + a.PROGRAMME_START_INTERVAL
          else a.ADJUSTED_EVENT_START_TIME
      end as INSTANCE_START_TIME,
      case
        when a.ADJUSTED_EVENT_START_TIME + a.PROGRAMME_END_INTERVAL < a.X_ADJUSTED_EVENT_END_TIME then a.ADJUSTED_EVENT_START_TIME + a.PROGRAMME_END_INTERVAL
           else a.X_ADJUSTED_EVENT_END_TIME
      end as INSTANCE_END_TIME,

      a.CAPPED_FULL_FLAG,
      case
        when INSTANCE_START_TIME >= b.CAPPED_EVENT_END_TIME then 1
          else 0
      end                               as CALC_FULLY_CAPPED,

      a.CAPPED_PARTIAL_FLAG,
      case
       when INSTANCE_START_TIME >= b.CAPPED_EVENT_END_TIME then 0
       when INSTANCE_END_TIME > b.CAPPED_EVENT_END_TIME then 1
         else 0
      end                               as CALC_PARTIALLY_CAPPED,

      a.CAPPED_EVENT_END_TIME,
      b.CAPPED_EVENT_END_TIME           as CALC_CAPPED_EVENT_END_TIME,

--      b.SHORT_DURATION_CAPPED_FLAG      as CALC_SHORT_DURATION_CAPPED_FLAG,
--      b.LONG_DURATION_CAPPED_FLAG       as CALC_LONG_DURATION_CAPPED_FLAG,

      a.X_BARB_MIN_START,
      a.X_BARB_MIN_END,
      case
        when CALC_FULLY_CAPPED = 1 then null
        when (a.broadcast_start_datetime_utc <= c.ATTRIBUTION_START) then c.ATTRIBUTION_START
        when date_part('second', a.broadcast_start_datetime_utc) <= 29 then a.broadcast_start_datetime_utc
        when date_part('second', a.broadcast_start_datetime_utc) >= 30 then a.broadcast_start_datetime_utc + interval '1 minute'
          else null
      end as CALC_ATTRIBUTION_START,
      case
        when CALC_FULLY_CAPPED = 1 then null
        when (a.broadcast_end_datetime_utc >= c.ATTRIBUTION_END) then c.ATTRIBUTION_END
        when date_part('second', a.broadcast_end_datetime_utc) <= 29 then a.broadcast_end_datetime_utc - interval '1 minute'
        when date_part('second', a.broadcast_end_datetime_utc) >= 30 then a.broadcast_end_datetime_utc
          else null
      end as CALC_ATTRIBUTION_END,

    -- case
    --   when (a.broadcast_start_datetime_utc <= c.ATTRIBUTION_START) then c.ATTRIBUTION_START
    --   when date_part('second', a.broadcast_start_datetime_utc) <= 29 then a.broadcast_start_datetime_utc
    --   when date_part('second', a.broadcast_start_datetime_utc) >= 30 then a.broadcast_start_datetime_utc + 1
    --     else '1900-01-01 00:00:00'
    -- end as CALC_ATTRIBUTION_START,
    -- case
    --   when (a.broadcast_end_datetime_utc >= c.ATTRIBUTION_END) then c.ATTRIBUTION_END
    --   when date_part('second', a.broadcast_end_datetime_utc) <= 29 then a.broadcast_end_datetime_utc - 1
    --   when date_part('second', a.broadcast_end_datetime_utc) >= 30 then a.broadcast_end_datetime_utc
    --     else '1900-01-01 00:00:00'
    -- end as CALC_ATTRIBUTION_END,

      c.ATTRIBUTION_START,
      c.ATTRIBUTION_END,

      a.WEIGHT_SAMPLE,
      a.WEIGHT_SCALED,

      a.SENSITIVE_FLAG,
      a.SENSITIVE_CHANNEL_FLAG,
      a.SENSITIVE_PROGRAMME_FLAG,
      a.SENSITIVE_PROG_PERCENTAGE_FLAG,
      a.SENSITIVE_MISSING_SK_MAPPING_FLAG,
      a.SENSITIVE_UNKNOWN_SK_MAPPING_FLAG,
      a.BARB_REPORTED_FLAG,

      a.event_sequence_id,
      a.programme_trans_sk,
      a.x_type_of_viewing_event,
      a.x_channel_name,
      a.x_epg_title
into SBE_INTO_PROGRAMME_UAT
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_48 a left join TSTIQ_DIS_PREPARE..CAPPED_EVENTS b on a.VIEWING_EVENT_ID = b.VIEWING_EVENT_ID
     left join TSTIQ_DIS_PREPARE..FINAL_MINUTE_ATTRIBUTION c on a.VIEWING_EVENT_ID = c.VIEWING_EVENT_ID
where a.viewing_event_id not in (select VIEWING_EVENT_ID from SBE_DUPLICATE_EVENT_IDS)

  -- a.viewing_event_id = -9133945651190471098
  --CAPPED_FULL_FLAG <> CALC_FULLY_CAPPED
order by a.subscriberid, a.adjusted_event_start_time, INSTANCE_START_TIME, INSTANCE_END_TIME;
--limit 500;





  -- ##### Tests #####
select
      *
  from SBE_INTO_PROGRAMME_UAT
 where CAPPED_FULL_FLAG <> CALC_FULLY_CAPPED;


select
      *
  from SBE_INTO_PROGRAMME_UAT
 where CAPPED_PARTIAL_FLAG <> CALC_PARTIALLY_CAPPED   ;


select *
  from SBE_INTO_PROGRAMME_UAT
 where X_BARB_MIN_START <> CALC_ATTRIBUTION_START
    or X_BARB_MIN_END <> CALC_ATTRIBUTION_END;

/*
full/partial for test  1084448497217866046
*/


select
      a.viewing_event_id,
      a.subscriberid,
      a.videoplaying,
      a.stblogcreationdate,
      a.recordedtime,
      a.playbackspeed,
      a.broadcast_start_datetime_utc,
      a.broadcast_end_datetime_utc,
      a.adjusted_event_start_time,
      a.x_adjusted_event_end_time,
      a.x_viewing_start_time,
      a.x_viewing_end_time,

      a.CAPPED_FULL_FLAG,
      a.CAPPED_PARTIAL_FLAG,
      a.CAPPED_EVENT_END_TIME,
      a.X_BARB_MIN_START,
      a.X_BARB_MIN_END
  from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_48 a
 where viewing_event_id = 5347529157570064515
 order by x_viewing_start_time, x_viewing_end_time, broadcast_start_datetime_utc, broadcast_end_datetime_utc
;

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
        SHORT_DURATION_CAPPED_FLAG,
        LONG_DURATION_CAPPED_FLAG,
        CAPPED_EVENT_END_TIME
    from TSTIQ_DIS_PREPARE..CAPPED_EVENTS
   where viewing_event_id = 5347529157570064515
   order by adjusted_event_start_time, x_adjusted_event_end_time
  ;


  select
        a.viewing_event_id,
        a.subscriberid,
        a.videoplaying,
        a.playbackspeed,

        a.recordedtime,

        a.broadcast_start_datetime_utc,
        a.broadcast_end_datetime_utc,

        a.adjusted_event_start_time,
        a.x_adjusted_event_end_time,
        a.INSTANCE_START_TIME,
        a.INSTANCE_END_TIME,
        a.CAPPED_EVENT_END_TIME as Capped_time_orig,
        CALC_CAPPED_EVENT_END_TIME as Capped_time_calc,

        a.CAPPED_FULL_FLAG,
        CALC_FULLY_CAPPED,

        a.CAPPED_PARTIAL_FLAG,
        CALC_PARTIALLY_CAPPED,

        a.CAPPED_EVENT_END_TIME,
        CALC_CAPPED_EVENT_END_TIME,

        a.X_BARB_MIN_START,
        a.X_BARB_MIN_END,
        CALC_ATTRIBUTION_START,
        CALC_ATTRIBUTION_END

    from SBE_INTO_PROGRAMME_UAT a
   where viewing_event_id in (
-4720155832640101510

)
   order by subscriberid, adjusted_event_start_time, INSTANCE_START_TIME, INSTANCE_END_TIME, broadcast_start_datetime_utc, broadcast_end_datetime_utc
  ;





































