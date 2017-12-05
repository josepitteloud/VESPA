-- NO LONGER IN PLAY!!
-- This guy did what it had to do, told us that the ping was coming in from 'kmandal'
-- and now we don't need it any more. Also, we're trapping the pinging user in the main
-- scheduler proc too.

-- To support the regular reporting, we want to be able to tell if each report run
-- was a live build (ie pinged by the overnight scheduler) or if it was queued by
-- some analyst some other time during the day. We could go by time of processing,
-- but then we wouldn't catch test builds that are kicked off overnight. So: we
-- have this procedure (which we've added to the scheduler queue) to tell us the
-- name of the user who pings the scheduler (Spoiler: it's Kuntal, with user name
-- 'kmandal'). Then in our reports we can check if the executing user is 'kmandal'
-- and if so, it's a live run, and if not, it's a dev / test / catchup run.

create table VES024_executor_names (
        executor                varchar(60)
        ,date_applied           datetime default now()
);

grant select, insert on VES024_executor_names to public;

create procedure VES024_trap_executor
as
begin
        insert into VES024_executor_names (executor)
        select user
end;
grant execute on VES024_trap_executor to public;

execute vespa_analysts.VES024_trap_executor;


select * from vespa_analysts.VES024_executor_names;


insert into CITeam.VES024_report_schedule
    (report_reference, report_category, report_procedure, report_priority, queuing_day_of_week)
values (
    'Executor trap'
    ,'Vespa support'
    ,'vespa_analysts.VES024_trap_executor'
    ,70                    -- Important, but not quite as important as the RTM refresh (might put RTM on the single box view?)
    ,2                      -- Monday
);
