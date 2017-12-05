
  -- ##### CBI results #####
select
      pk_viewing_prog_instance_fact,
      Subscriber_Id,
      date(EVENT_START_DATE_TIME_UTC) as Event_Date,
      date(INSTANCE_START_DATE_TIME_UTC) as Instance_Date,

      dk_log_received_datehour_dim,

      DK_EVENT_START_DATEHOUR_DIM,
      DK_INSTANCE_START_DATEHOUR_DIM,
      EVENT_START_DATE_TIME_UTC,
      EVENT_END_DATE_TIME_UTC,
      INSTANCE_START_DATE_TIME_UTC,
      INSTANCE_END_DATE_TIME_UTC,
      BROADCAST_START_DATE_TIME_UTC,
      BROADCAST_END_DATE_TIME_UTC,
      datediff(second, EVENT_START_DATE_TIME_UTC, EVENT_END_DATE_TIME_UTC) as Event_Duration,
      datediff(second, INSTANCE_START_DATE_TIME_UTC, INSTANCE_END_DATE_TIME_UTC) as Instance_Duration,

      case
        when capping_end_date_time_utc is not null then datediff(second, EVENT_START_DATE_TIME_UTC, capping_end_date_time_utc)
          else datediff(second, EVENT_START_DATE_TIME_UTC, event_end_date_time_utc)
      end as Event_Duration_Capped,

      case
        when capped_full_flag = 1 then 0
        when capped_partial_flag = 1 then datediff(second, INSTANCE_START_DATE_TIME_UTC, capping_end_date_time_utc)
          else datediff(second, INSTANCE_START_DATE_TIME_UTC, INSTANCE_END_DATE_TIME_UTC)
      end as Instance_Duration_Capped,

      case
        when capping_end_date_time_utc is not null then capping_end_date_time_utc
          else event_end_date_time_utc
      end as Event_End_Time_Capped,

      case
        when capped_full_flag = 1 then null
        when capped_partial_flag = 1 then capping_end_date_time_utc
          else INSTANCE_END_DATE_TIME_UTC
      end as Instance_End_Time_Capped,

      REPORTED_PLAYBACK_SPEED,
      case
        when REPORTED_PLAYBACK_SPEED is null then 'Live'
        when REPORTED_PLAYBACK_SPEED = 2 and date(BROADCAST_START_DATE_TIME_UTC) = date(INSTANCE_START_DATE_TIME_UTC) then 'Vosdal'
        when REPORTED_PLAYBACK_SPEED = 2 and date(BROADCAST_START_DATE_TIME_UTC) < date(INSTANCE_START_DATE_TIME_UTC) then 'Playback'
          else '???'
      end Timeshift,
      capped_full_flag,
      capped_partial_flag,
      duration_minutes,
      capping_end_date_time_utc,
      capping_end_date_time_local,
      case
        when Duration > 6
             and DK_BROADCAST_START_DATEHOUR_DIM >= 2012110500
             and type_of_viewing_event <> 'Non viewing event'
             and INSTANCE_START_DATE_TIME_UTC < INSTANCE_END_DATE_TIME_UTC then 1
          else 0
      end as Filter

  into --drop table
       e2e_test_raw_viewing3
  from sk_prod.VESPA_EVENTS_ALL
 where (REPORTED_PLAYBACK_SPEED is null or REPORTED_PLAYBACK_SPEED = 2)
   and Panel_id = 12
   -- and BROADCAST_START_DATE_TIME_UTC >= dateadd(day, -28, EVENT_START_DATE_TIME_UTC)
   and account_number is not null
   and DK_EVENT_START_DATEHOUR_DIM >= 2012120300
   and DK_EVENT_START_DATEHOUR_DIM <= 2012120400
   and subscriber_id is not null;
commit;

create dttm index idx1 on e2e_test_raw_viewing3(INSTANCE_START_DATE_TIME_UTC);
create dttm index idx2 on e2e_test_raw_viewing3(INSTANCE_END_DATE_TIME_UTC);
create lf index idx3 on e2e_test_raw_viewing3(Timeshift);
create unique hg index idx4 on e2e_test_raw_viewing3(pk_viewing_prog_instance_fact);

/*
select
      DK_EVENT_START_DATEHOUR_DIM,
      dk_log_received_datehour_dim,
      sum(case when capping_end_date_time_utc is null then 1 else 0 end) as missing_cap_time,
      count(*) as cnt
  from sk_prod.VESPA_EVENTS_ALL
 where DK_EVENT_START_DATEHOUR_DIM between 2012120300 and 2012120323
 group by DK_EVENT_START_DATEHOUR_DIM, dk_log_received_datehour_dim;


select
      DK_EVENT_START_DATEHOUR_DIM,
      dk_log_received_datehour_dim,
      count(*) as cnt
  from (select distinct
              subscriber_id,
              dk_log_received_datehour_dim,
              DK_EVENT_START_DATEHOUR_DIM,
              Event_Duration_Capped
          from e2e_test_raw_viewing3
         where Event_Duration_Capped > 180 * 60
           and DK_EVENT_START_DATEHOUR_DIM between 2012120300 and 2012120323) a
 group by DK_EVENT_START_DATEHOUR_DIM, dk_log_received_datehour_dim;
*/



  -- #### Summary ####
  -- Overall level for selected day - NON CAPPED
select
      count(*) as cnt,
      sum(Instance_Duration) as total_viewing,
      count(distinct subscriber_id) as boxes
  from e2e_test_raw_viewing3
 where DK_EVENT_START_DATEHOUR_DIM between 2012120300 and 2012120323
   and Filter = 1;

select
      count(*) as cnt,
      sum(Instance_Duration_Capped) as total_viewing,
      count(distinct subscriber_id) as boxes
  from e2e_test_raw_viewing3
 where DK_EVENT_START_DATEHOUR_DIM between 2012120300 and 2012120323
   and capped_full_flag = 0
   and Filter = 1;









  -- ##### Vespa team capping #####
-- RUN CAPPING FIRST --
select
     vr.cb_row_id
    ,vr.subscriber_id
    ,vr.account_number
    ,vr.adjusted_event_start_time
    ,vr.X_Adjusted_Event_End_Time
    ,vr.x_viewing_start_time
    ,vr.x_viewing_end_time
    ,cewe.capped_event_end_time
    ,case
       when vr.live = 1                                                                       then 'Live'

       when date(vr.x_viewing_start_time) = program_air_date                                  then 'Vosdal'

       when date(vr.x_viewing_start_time) > program_air_date and
            vr.x_viewing_start_time <= dateadd(hour, 170, cast(program_air_date as datetime)) then 'Playback'

       when vr.x_viewing_start_time > dateadd(hour, 170, cast(program_air_date as datetime))  then 'Playback'

         else '???'

     end as Timeshift

    ,case
        when cewe.subscriber_id is not null then 11 -- 11 for things that need capping treatment
        else 0                                      -- 0 for no capping
      end as capped_flag
    ,vr.program_air_date
    ,vr.live
    ,vr.genre
    ,cast(null as datetime) as viewing_starts
    ,cast(null as datetime) as viewing_stops
    ,cast(null as bigint) as viewing_duration
    ,cast(null as bigint) as viewing_duration_uncapped

  into e2e_aug_capped_data
  from Capping2_01_Viewing_Records as vr left join CP2_capped_events_with_endpoints as cewe
    on cewe.subscriber_id             = vr.subscriber_id
   and cewe.adjusted_event_start_time = vr.adjusted_event_start_time;
commit;

create unique hg index idx0 on e2e_aug_capped_data(cb_row_id);
create dttm index idx1 on e2e_aug_capped_data(adjusted_event_start_time);
create dttm index idx2 on e2e_aug_capped_data(x_viewing_start_time);
create dttm index idx3 on e2e_aug_capped_data(x_viewing_end_time);
create dttm index idx4 on e2e_aug_capped_data(viewing_starts);
create dttm index idx5 on e2e_aug_capped_data(viewing_stops);
create lf index idx6 on e2e_aug_capped_data(Timeshift);
create hg index idx7 on e2e_aug_capped_data(subscriber_id);


update e2e_aug_capped_data
  set viewing_starts = case
          -- if start of viewing_time is beyond capped end time then flag as null
          when capped_event_end_time <= x_viewing_start_time then null
          -- else leave start of viewing time unchanged
          else x_viewing_start_time
      end
     ,viewing_stops = case
          -- if start of viewing_time is beyond capped end time then flag as null
          when capped_event_end_time <= x_viewing_start_time then null
          -- if capped event end time is beyond end time then leave end time unchanged
          when capped_event_end_time > x_viewing_end_time then x_viewing_end_time
          -- if capped event end time is null then leave end time unchanged
          when capped_event_end_time is null then x_viewing_end_time
          -- otherwise set end time to capped event end time
          else capped_event_end_time
      end
  where capped_flag = 11;
commit;

-- And now the more basic case where there's no capping;
update e2e_aug_capped_data
   set viewing_starts = x_viewing_start_time
      ,viewing_stops = x_viewing_end_time
 where capped_flag = 0;
commit;

update e2e_aug_capped_data
  set capped_flag = case
                      when viewing_stops < x_viewing_end_time then 2
                      when viewing_starts is null then 3
                        else 1
                    end
 where capped_flag = 11;
commit;

update e2e_aug_capped_data
    set viewing_duration = datediff(second, viewing_starts, viewing_stops),
        viewing_duration_uncapped = datediff(second, x_viewing_start_time, x_viewing_end_time);
commit;






/*
select
       cb_row_id
      ,subscriber_id
      ,case when timeshifting in ('PLAYBACK28', 'PLAYBACK7') then 'PLAYBACK' else timeshifting end as Timeshift
      ,cast(viewing_starts as varchar(13)) as Instance_Hour_Start
      ,viewing_starts
      ,viewing_stops
      ,viewing_duration
  into --drop table
       e2e_test_aug_viewing
  from vespa_analysts.Vespa_Daily_Augs_20121203;
commit;

insert into e2e_test_aug_viewing select cb_row_id,subscriber_id,case when timeshifting in ('PLAYBACK28', 'PLAYBACK7') then 'PLAYBACK' else timeshifting end as Timeshift,cast(viewing_starts as varchar(13)) as Instance_Hour_Start,viewing_starts,viewing_stops,viewing_duration from vespa_analysts.Vespa_Daily_Augs_20121204; commit;
insert into e2e_test_aug_viewing select cb_row_id,subscriber_id,case when timeshifting in ('PLAYBACK28', 'PLAYBACK7') then 'PLAYBACK' else timeshifting end as Timeshift,cast(viewing_starts as varchar(13)) as Instance_Hour_Start,viewing_starts,viewing_stops,viewing_duration from vespa_analysts.Vespa_Daily_Augs_20121205; commit;
insert into e2e_test_aug_viewing select cb_row_id,subscriber_id,case when timeshifting in ('PLAYBACK28', 'PLAYBACK7') then 'PLAYBACK' else timeshifting end as Timeshift,cast(viewing_starts as varchar(13)) as Instance_Hour_Start,viewing_starts,viewing_stops,viewing_duration from vespa_analysts.Vespa_Daily_Augs_20121206; commit;
insert into e2e_test_aug_viewing select cb_row_id,subscriber_id,case when timeshifting in ('PLAYBACK28', 'PLAYBACK7') then 'PLAYBACK' else timeshifting end as Timeshift,cast(viewing_starts as varchar(13)) as Instance_Hour_Start,viewing_starts,viewing_stops,viewing_duration from vespa_analysts.Vespa_Daily_Augs_20121207; commit;
insert into e2e_test_aug_viewing select cb_row_id,subscriber_id,case when timeshifting in ('PLAYBACK28', 'PLAYBACK7') then 'PLAYBACK' else timeshifting end as Timeshift,cast(viewing_starts as varchar(13)) as Instance_Hour_Start,viewing_starts,viewing_stops,viewing_duration from vespa_analysts.Vespa_Daily_Augs_20121208; commit;
insert into e2e_test_aug_viewing select cb_row_id,subscriber_id,case when timeshifting in ('PLAYBACK28', 'PLAYBACK7') then 'PLAYBACK' else timeshifting end as Timeshift,cast(viewing_starts as varchar(13)) as Instance_Hour_Start,viewing_starts,viewing_stops,viewing_duration from vespa_analysts.Vespa_Daily_Augs_20121209; commit;
commit;

create dttm index idx1 on e2e_test_aug_viewing(viewing_starts);
create dttm index idx2 on e2e_test_aug_viewing(viewing_stops);
create lf index idx3 on e2e_test_aug_viewing(Timeshift);
create unique hg index idx4 on e2e_test_aug_viewing(cb_row_id);
*/


  -- #### Summary ####
  -- Overall level for selected day - NON-CAPPED
select
      count(*) as cnt,
      sum(viewing_duration_uncapped) as total_viewing,
      count(distinct subscriber_id) as boxes
  from e2e_aug_capped_data
 where adjusted_event_start_time between '2012-12-03 00:00:00' and '2012-12-03 23:59:59';


select
      count(*) as cnt,
      sum(viewing_duration) as total_viewing,
      count(distinct subscriber_id) as boxes
  from e2e_aug_capped_data
 where capped_flag <> 3
   and adjusted_event_start_time between '2012-12-03 00:00:00' and '2012-12-03 23:59:59';






  -- #### Comparisons ####

select
      coalesce(a.subscriber_id, b.subscriber_id) as subscriber_id,
      case when a.instances is null then 0 else a.instances end as sk_prod_instances,
      case when b.instances is null then 0 else b.instances end as aug_instances
  into --drop table
       e2e_test_source_comp
  from (select
              subscriber_id,
              count(*) as instances
          from e2e_test_raw_viewing3
         where DK_EVENT_START_DATEHOUR_DIM between 2012120300 and 2012120323
           and Filter = 1
         group by subscriber_id) a full join
       (select
              subscriber_id,
              count(*) as instances
          from Capping2_01_Viewing_Records
         where adjusted_event_start_time between '2012-12-03 00:00:00' and '2012-12-03 23:59:59'
         group by subscriber_id) b
    on a.subscriber_id = b.subscriber_id;
commit;

select
      sum(case when sk_prod_instances = aug_instances then 1 else 0 end) as equal,
      sum(case when sk_prod_instances > aug_instances then 1 else 0 end) as sk_prod_more,
      sum(case when sk_prod_instances < aug_instances then 1 else 0 end) as aug_more
  from  e2e_test_source_comp;

select * from e2e_test_source_comp;






  -- Get some examples for instances where sk_prod > aug
select
      case when DK_EVENT_START_DATEHOUR_DIM between 2012120300 and 2012120323 and Filter = 1 then 'X' else '' end as OK,
      *
  from e2e_test_raw_viewing3
 where subscriber_id in (
27144192,
20217369,
)
 order by subscriber_id, event_start_date_time_utc, instance_start_date_time_utc;

select
      case when adjusted_event_start_time between '2012-12-03 00:00:00' and '2012-12-03 23:59:59' then 'X' else '' end as OK,
      *
  from Capping2_01_Viewing_Records
 where subscriber_id in (
27144192,
20217369,
)
 order by subscriber_id, adjusted_event_start_time, x_viewing_start_time;




select
      coalesce(cb_row_id, pk_viewing_prog_instance_fact) as row_id,
      INSTANCE_START_DATE_TIME_UTC,
      viewing_starts,
      capped_full_flag,
      capped_partial_flag,
      Event_Duration,
      Instance_Duration,
      case
        when cb_row_id is not null then 1
          else 0
      end as aug,
      case
        when pk_viewing_prog_instance_fact is not null then 1
          else 0
      end as sk
  into --drop table
       e2e_aug_sk_recon
  from e2e_test_raw_viewing3 full join e2e_test_aug_viewing
    on cb_row_id = pk_viewing_prog_instance_fact
   and DK_EVENT_START_DATEHOUR_DIM between 2012120300 and 2012120323
   and viewing_starts between '2012-12-03 00:00:00' and '2012-12-03 23:59:59'
   and capped_full_flag = 0;
commit;

select * from e2e_aug_sk_recon;


select *
  from e2e_test_raw_viewing3

select count(*) as Num_Instances, sum(viewing_duration) as Total_Duration, count(distinct subscriber_id) as Num_Boxes
  from e2e_test_aug_viewing
 where viewing_starts between '2012-12-03 00:00:00' and '2012-12-03 23:59:59'

select count(*) as Num_Instances, sum(Instance_Duration_Capped) as Total_Duration, count(distinct subscriber_id) as Num_Boxes
  from e2e_test_raw_viewing3
 where Instance_Date = '2012-12-03'
   and capped_full_flag = 0;





