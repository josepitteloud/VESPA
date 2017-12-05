/******************************************************************************
**
** Sky View panel: Dialback Report
**                  - Results Extraction
**
** Following the weekly refresh script ("Sky View Dialback 01 - build reports.sql")
** you run all of these guys manually and it gives you the outputs. See either
** that file or the wiki page for details:
**
****  http://rtci/vespa1/Sky%20View%20Dialback%20report.aspx
**
** There might end up manual QA in here, otherwise just a bunch of reports to
** pull out.
**
** Features to implement:
**  8. ...
**
** Recently implemented:
**
**  5. Maybe a graph of total log return count vs number of intervals?
**  4. Split by 7 and 30 days
**  6. Result queries split out into separate files because that's what the
**      reporting engine wants to see.
**  7. Move all the simple QA things (which expect zero results) back over to
**      the automated file & turn them into Logger level 2 (Warning!) calls.
**
**
******************************************************************************/

/****************** TRACKING PROGRESS: ******************/

-- Because the other script is just fire & forget. This will tell you where
-- it's up to (or if it were scheduled, if it ran at all).
EXECUTE citeam.logger_get_latest_job_events 'SkyViewDialback', 3;

--EXECUTE citeam.logger_get_latest_job_events 'SVDialback Test ***', 3;
-- Reports builds you run from your own login get flagged as test runs in
-- the logger; you have to replace *** with your initials eg RST in order
-- to find the events.


/****************** MANUAL QA: CONSISTENT MATRIX TRANSITION ELEMENTS ******************/

-- Currently no manual QA, other than checking that none of the level 2 warnings
-- were thrown into the logger.
--EXECUTE citeam.logger_get_latest_job_events 'SkyViewDialback', 2;
EXECUTE citeam.logger_get_latest_job_events 'SVDialback Test ***', 2;
