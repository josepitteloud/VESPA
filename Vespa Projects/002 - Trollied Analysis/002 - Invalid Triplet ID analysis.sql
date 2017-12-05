


-----------repeat for Panel ID = 1----


CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2011-05-10';
SET @var_prog_period_end    = '2011-12-10';

--drop table vespa_analysts.invalid_viewing_test_loop_panel_id1;
-- To store all the viewing records:
create table vespa_analysts.invalid_viewing_test_loop ( 
    event_date                    date
    ,boxes                        integer
    ,subs_with_invalid_records                        integer
);

SET @var_cntr = 0;
SET @var_num_days = 220;       -- Get events up to 30 days of the programme broadcast time (only 20 in this case due to Vespa Suspension at end August

-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into vespa_analysts.invalid_viewing_test_loop
    select dateadd(day, @var_cntr, @var_prog_period_start)
,count(distinct subscriber_id) as boxes, count(distinct case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then subscriber_id else null end) as subs_with_invalid_records
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
          
        -- Filter for viewing events during extraction
     where 
Panel_id in (4,5)'
      ;


  -- ####### Loop through to populate table: Sybase Interactive style (not entirely tested) ######
--FLT_1: LOOP

    --EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd'));

    --SET @var_cntr = @var_cntr + 1;
    --IF @var_cntr > @var_num_days THEN LEAVE FLT_1;
    --END IF ;

--END LOOP FLT_1;
  -- ####### End of loop (this loop structure not tested yet) ######

  -- ####### Alternate Loop: WinSQL style (tested, good) ######
while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;


--select play_back_speed , count(*) as records from VESPA_tmp_all_viewing_records_trollied_20110811 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from VESPA_tmp_all_viewing_records_trollied_20110811 group by day_view order by day_view;


commit;

select event_date
,sum(boxes)
,sum(subs_with_invalid_records)
from  vespa_analysts.invalid_viewing_test_loop
group by event_date
order by event_date
;


-----------repeat for Panel ID = 1----


CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2011-05-10';
SET @var_prog_period_end    = '2011-12-10';

--drop table vespa_analysts.invalid_viewing_test_loop_panel_id1;
-- To store all the viewing records:
create table vespa_analysts.invalid_viewing_test_loop_panel_id1 ( 
    event_date                    date
    ,boxes                        integer
    ,subs_with_invalid_records                        integer
);

SET @var_cntr = 0;
SET @var_num_days = 220;       -- Get events up to 30 days of the programme broadcast time (only 20 in this case due to Vespa Suspension at end August

-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into vespa_analysts.invalid_viewing_test_loop_panel_id1
    select dateadd(day, @var_cntr, @var_prog_period_start)
,count(distinct subscriber_id) as boxes, count(distinct case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then subscriber_id else null end) as subs_with_invalid_records
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
          
        -- Filter for viewing events during extraction
     where 
Panel_id in (1)'
      ;


  -- ####### Loop through to populate table: Sybase Interactive style (not entirely tested) ######
--FLT_1: LOOP

    --EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd'));

    --SET @var_cntr = @var_cntr + 1;
    --IF @var_cntr > @var_num_days THEN LEAVE FLT_1;
    --END IF ;

--END LOOP FLT_1;
  -- ####### End of loop (this loop structure not tested yet) ######

  -- ####### Alternate Loop: WinSQL style (tested, good) ######
while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;


--select play_back_speed , count(*) as records from VESPA_tmp_all_viewing_records_trollied_20110811 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from VESPA_tmp_all_viewing_records_trollied_20110811 group by day_view order by day_view;


commit;

select event_date
,sum(boxes)
,sum(subs_with_invalid_records)
from  vespa_analysts.invalid_viewing_test_loop_panel_id1
group by event_date
order by event_date
;
