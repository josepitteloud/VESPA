/******************************************************************************
**
** Project Vespa: Weekly Status Report
**                  - Results Extraction
**
** So after you run the weekly build script ("Vespa WeStat 01 - build reports.sql")
** you can use the logger call in here to check how far it's got to. Then the
** QA scripts to check that everything is good (heh), and then the various report
** construction output pulls. Or rather, now that report construction procedures
** are automated, you just look through this guy once the report is complete.
**
******************************************************************************/



/****************** TRACKING PROGRESS: ******************/

EXECUTE citeam.logger_get_latest_job_events 'VespaWeStat', 3;
-- Same setup as every other regular report.

/****************** Q00: QA ON WeStat BUILD ******************/

-- What different transition states are we logging? Are they consistent?
select initial_or_final, panel, transition_state, count(1) as hits
from vespa_analysts.vespa_westat_population_breakdown
group by initial_or_final, panel, transition_state
order by initial_or_final, panel, transition_state;
-- Yeah, they're consistent, but heh, we don't have anything in the numbers 3-4 or 6-10
-- as they're largely all Panel 6 & 7 tracking and flux things. Add somethign about that
-- in the glossary then.

