-- Project Vespa Core Control: reporting procedures

-- Covered by both logger and the scheduler. Don't need any more.

-- Go read:
--  http://mktskyportal/Campaign%20Handbook/Logger.aspx

-- Still, leave this file here as a reminder why we're not building additional structures.

-- But then! Turns out there might be generic reporting procedures after all. Like this one:

-- So this procedure returns the end date for reports we're runnign in this week; no
-- longer will the reporting period depend on the day we're running the report, they'll
-- always do their cycle and end on the Saturday just passed.
if object_id('vespa_analysts.Regulars_Get_report_end_date') is not null
   drop procedure vespa_analysts.Regulars_Get_report_end_date;
create procedure vespa_analysts.Regulars_Get_report_end_date
    @end_of_report_cycle date output
as
begin

    select @end_of_report_cycle = max(calendar_date) + 2 -- Plus two to make the date a Saturday
    from sk_prod.sky_calendar
    where subs_last_day_of_week = 'Y' -- Last day of week is always Thursday
    and calendar_date < today() - 2 -- If today is Monday, this will pick out the Thursday before the previous
                                    -- Saturday, which is still the Thursday we want. If it's a Friday, it will
                                    -- pick out the Thursday before the previous Thursday, which means we're
                                    -- still running the report on the previous month like we want to.

end;

-- no grant, don't expect these things to be used outside the Vespa reports which will
-- all get run under vespa_analyst anyway

-- And even further! A procedure to get the name of the active user space within a procedure

-- Because if you try to call user() within a procedure, you still get the outermost logged-in
-- user returned. So how do you tell if it's a live build on vespa_analysts or a dev build in
-- a private user space? It could be either person running either proc.

-- Okay, so here's a dirty nasty trick to use to be able to tell which schema a procedure is
-- currently using; put one of these in your schema and at compile time it will be dynamically
-- set with your user name which is then static for execution. That said, it's pretty important
-- to leave off the 'vespa_analysts' or other user space designation when you call it, because
-- explicitly invoking the vespa_analysts version will of course just give you 'vespa_analysts'.
-- The idea is that Sybase checks your own schema by default, so if you're working on your own
-- dev stuff it shows as dev and if you're running the same code on a vespa_analysts table (if
-- you're accessing it using a proc or suchlike) then it shows up as a live build.

IF object_id('Regulars_whats_my_namespace') IS NOT NULL
    DROP procedure Regulars_whats_my_namespace;

execute(
'
create procedure Regulars_whats_my_namespace
        @usernamething          varchar(80) output
as
        set @usernamething = ''' || user || '''
');

commit;

-- No permissions, since it should only ever be called by other procedures which have the same
-- owner; either vespa_analysts for the live build or your own user space for a dev build, but
-- you shouldn't be specifically invoking a particular user's name cache.

-- Also: it turns out that when you're checking if a table exists inside a proc, user space has
-- to be explicit because object_id() etc all respond to logged-in user rather than proxied user
-- through stored procedure permissions. Don't want to put a literal "vespa_analysts" in there
-- because that will fail with test / dev builds in other people's schemas.
