-- This script is cloned from the "Central_report_scheduler" folder in the
-- Customer group wiki, specifically the file "crs_opt_in_script.sql" which
-- tells the scheduler that we're ready to go ahead with all this week's
-- tasks (but they won't start until it gets pinged).

insert into CITeam.VES024_report_opt_in_markers (go_flag)
values (1)
;
