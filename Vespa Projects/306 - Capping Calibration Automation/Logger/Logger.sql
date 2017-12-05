/*----------------------------------------------------------------------

        SK2417 - Logger
        ---------------

        Author  :  Nick Leech
        Date    :  14th January 2010
        For     :  Everyone
        Version :  1.0.0


  Change log
  ----------
  29/10/2012 SBE  - Table name prefixes changed to "z_" so relevant tables are
                    listed at the end of user's table list
                  - "alter procedure" changed to "create procedure"
                  - "drop procedure" statements added
                  - other minor tweaks

-----------------------------------------------------------------------*/



-------------------------------------------------------Create Runs Table
if object_id('z_logger_runs') is not null then drop table z_logger_runs end if;
create table z_logger_runs (
        run_id          bigint          identity
       ,run_time        datetime        not null
       ,job_number      varchar(20)     not null
       ,run_description varchar(50)     not null
);

create unique hg index idx_run_id_uhg    on z_logger_runs(run_id);
create      DTTM index idx_run_time      on z_logger_runs(run_time);
create        hg index idx_job_number_hg on z_logger_runs(job_number);

grant all on z_logger_runs to public;



-------------------------------------------------------Create Events Table
if object_id('z_logger_events') is not null then drop table z_logger_events end if;
create table z_logger_events (
        event_id                bigint        identity
       ,run_id                  bigint        not null
       ,event_time              datetime      not null
       ,event_level             tinyint       not null   -- 1 = FATAL , 2 = WARNING, 3 = Info, 4 = Debug
       ,event_description       varchar(200)  not null
       ,value                   integer       null
);

create unique hg index idx_event_id_uhg   on z_logger_events(event_id);
create        hg index idx_run_id_uhg     on z_logger_events(run_id);
create      DTTM index idx_event_date     on z_logger_events(event_time);
create        lf index idx_event_level    on z_logger_events(event_level);

grant all on z_logger_events to public;



------------------------------Make the Run Setup Procedure
if object_id('logger_create_run') is not null then drop procedure logger_create_run end if;
create procedure logger_create_run   @job_number varchar(20)
                                   , @run_description varchar(50)
                                   , @run_id bigint output          AS

    begin transaction

        INSERT INTO z_logger_runs (run_time,job_number,run_description)
        VALUES (NOW(), @job_number, @run_description)

    save transaction run_added

        SELECT @run_id = MAX(run_id) from z_logger_runs


    commit transaction

;

grant execute on logger_create_run to public;



------------------------------Make the Add Event Proceedure
if object_id('logger_add_event') is not null then drop procedure logger_add_event end if;
create procedure logger_add_event   @run_id bigint
                                   ,@event_level tinyint
                                   ,@event_description varchar(200)
                                   ,@value integer = null  AS
  begin transaction

      INSERT INTO z_logger_events (run_id,event_time ,event_level,event_description, value)
      VALUES ( @run_id, NOW(),@event_level,@event_description,@value)

  commit transaction
;

grant execute on logger_add_event to public;



------------------------------Make get Latest Run Events Proceedure
  --Lists the events for a run if you know the run_id
if object_id('logger_list_events_for_run') is not null then drop procedure logger_list_events_for_run end if;
create procedure logger_list_events_for_run  @run_id      bigint
                                            ,@event_level tinyint = 3   AS

        SELECT runs.run_id, runs.job_number, runs.run_description,
               evts.event_time, evts.event_level, evts.event_description, evts.value
          FROM z_logger_runs as runs
               INNER JOIN z_logger_events as evts ON runs.run_id = evts.run_id
         WHERE runs.run_id = @run_id
           AND evts.event_level <= @event_level
      ORDER BY evts.event_time


commit;

grant execute on logger_list_events_for_run to public;



------------------------------Make get Latest Job Events Proceedure
  --AS run events above but looks for the latest run_id for a job Number and uses that
if object_id('logger_get_latest_job_events') is not null then drop procedure logger_get_latest_job_events end if;
create procedure logger_get_latest_job_events    @job_number varchar(20)
                                                ,@event_level tinyint = 3   AS

        --Get the RunID
        Declare @run_id integer

        SELECT @run_id = Max(run_id)
          FROM z_logger_runs
         WHERE job_number = @job_number

        --Call the logger_list_events_for_run proceedure

        execute logger_list_events_for_run @run_id, @event_level


commit;

grant execute on logger_get_latest_job_events to public;



------------------------------Make List Latest Runs Proceedure
  --Lists the Last 50 runs for a Specific Job
if object_id('logger_list_latest_runs') is not null then drop procedure logger_list_latest_runs end if;
create procedure logger_list_latest_runs  @job_number varchar(20) AS

        SELECT top 50 job_number, run_id, run_time, run_description
          FROM z_logger_runs
         WHERE job_number =  @job_number
      ORDER BY run_time desc

commit;

grant execute on logger_list_latest_runs to public;



----------------------------------------------------------------------------
--    QA
----------------------------------------------------------------------------
/*
create variable @rid bigint; -- the Run_ID to be used for events

execute logger_create_run 'SK9898', 'Test Run', @rid output;
execute logger_add_event @rid, 3, 'First Event';
execute logger_add_event @rid, 3, 'Second Event', 100;
execute logger_add_event @rid, 2, 'Third Event - WARNING';

execute logger_get_latest_job_events 'SK9898', 3;

select * from z_logger_runs where run_id = @rid;
select * from z_logger_events where run_id = @rid order by event_id;

*/
