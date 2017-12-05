/******************************************************************************
**
** Project Vespa: Operational Dashboard Report
**                  - Results Extraction
**
** So after you run the weekly build script ("Vespa OpDash 01 - build reports.sql")
** you can use the logger call in here to check how far it's got to. Then the
** QA scripts to check that everything is good (heh), and then the various report
** construction output pulls.
**
** Slowly working our way towards better automation.
**
** Features to implement:
**
**  5. ...
**
** Implemented changes:
**
**  4. Additional data pull for requirement 47 (Acquisitions) in the 01-build
**      script. Have to change run instructions too.
**  2. We might also split this guy out into the 8 different query files,
**      since the automated reporting thing works well with one query per
**      file.
**  3. We could also add a stored procedure call here if we're going to work
**      everything from a scheduler somewhere; have it in the proc already,
**      in this script call it once and then check the progress, then pull
**      out the reports. Then kill the transient tables. Edit: do the QA
**      first because that's important?!
**  1. There's currently no standardised QA section. We should build one.
**      Update: in such a way that erroneous control totals can go into
**      logger calls with warning / fatal error mark levels? Update: a bunch
**      more logger detail now go in during the build, giving control totals
**      at various points along the way, and these can be inspected (during
**      or after processing) to make sure they're sensible numbers.
**
******************************************************************************/

-- Section titles and numbers held over from when the whole OpDash was all in
-- the same file.

/****************** TRACKING PROGRESS: ******************/

-- Reports builds you run from your own login get flagged as test runs in
-- the logger; you have to replace *** with your initials eg RST in order
-- to find the events.
--EXECUTE citeam.logger_get_latest_job_events 'OpDash Test ***', 3;

-- For the regular scheduled overnight builds:
EXECUTE citeam.logger_get_latest_job_events 'VespaOpDash', 3;
-- When the last logged event of today's date is "OpDash: weekly refresh complete!",
-- the report is finished and you can move on to the results pulls below. You should
-- check the various numbers next to the logger thing and make sure there are no
-- zeros (or -1's) because that indicates something hasn't updated properly. Blanks
-- are fine though, that indicates that there was no automated QA component for that
-- section (but check the later dedicated QA bits, as there might be relevant stuff
-- there - once it's built).
-- The numbers reported are generally the number of responsive records in that section,
-- for example, the control total in Logger for S01 (Sky base pop) is exactly the Sky
-- base population size that was observed in this run, and the control total on E02
-- (Boxes returning) is the number of boxes returning data. If any of the numbers are
-- weird or small (or colossally huge), there might be an issue there. So check those.

/****************** Q00: QA ON OPDASH BUILD ******************/

-- So we're still not testing if our report built fully correctly, because there
-- are structural things in it by design which make the control totals weird...
select coalesce(vss.result, 'Not Found!') as DB_category,
        count(1) as hits
from vespa_analysts.vespa_OpDash_subscriber_dashboard as osd
left join sk_prod.vespa_subscriber_status as vss
on osd.subscriber_id = convert(bigint, vss.card_subscriber_id)
group by DB_category
order by DB_category
;
-- We're currently taking the subscriber activation from the cells, not the DB,
-- so we want to know how much they diverge by. Also, we've got the daily reports
-- which are from another source and so don't really line up with these either.
/* As of late November: pretty typical for pre-CCN
DisablePending	    71871
Disabled	        22
EnableFailed	    47
EnablePending	    6008
EnableRequested	    275250
Enabled	            272572
*/
/* For 22 Feb 2012: it's all Enabled now!
Enabled	606483
*/
/* For 28th of Feb: a bunch of empty things, awesome...
Enabled	616950
Not Found!	21752
*/
/* For 22nd March: all stable!
Enabled	612702
*/
/* For 28th March: still stable!
Enabled	611561
*/
