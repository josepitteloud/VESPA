-- Project Vespa Core Control 2: active & available reports

-- We're going to pull the items from the regular report scheduler, the
-- main table will have the last time the thing was run, the next time
-- it'll be run, and so by comparing these guys to the week we're in now,
-- we'll be able to tell whether they've been updated or not yet, as well
-- as things like if they're flagged as active or not. Wait, can we just
-- select those straight into the control sheet? That's a cheap hack that
-- might just work out.

select
    report_reference
    ,case when datepart(week,today()) = datepart(week, coalesce(next_scheduled_day, last_scheduled_day))
        then task_status
        else 'Not scheduled' end as task_status
    ,case when datepart(week,today()) = datepart(week, coalesce(next_scheduled_day, last_scheduled_day))
        then coalesce(next_scheduled_day, last_scheduled_day)
        else null end as this_weeks_report_day
from CITeam.VES024_report_schedule
where report_category = 'Vespa regulars'
and report_active = 1
order by report_ID
;
