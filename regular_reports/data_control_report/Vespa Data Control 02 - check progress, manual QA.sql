/******************************************************************************
**
** Project Vespa: Data Control Report
**                  - Progress & issue tracking
**
** Currerntly there is no Excel report build for this guy, it's just a thing
** that we get to manually review and then escelate anything that's 
** particularly troubling.
**
******************************************************************************/

-- So here's the query to track progress, though this build is currently so fast
-- that it's not any kind of big deal:
EXECUTE citeam.logger_get_latest_job_events 'VespaDataControl', 3;

-- OK, so what are this week's failures like?
select *
from vespa_analysts.Vespa_DataCont_Flag_log
where noted + 7 > today()
order by issue_severity desc, id
;

-- That table contains all the historical stuff too, so if you want to look at
-- the evolution of those failures over time, then you can.
