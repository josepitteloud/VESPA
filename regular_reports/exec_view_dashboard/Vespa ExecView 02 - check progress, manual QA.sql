/******************************************************************************
**
** Project Vespa: Exec View Dashboard Report
**                  - Progress tracking & manual QA
**
** So after you run the weekly build script ("Vespa ExecView 01 - build reports.sql")
** you can use the logger call in here to check how far it's got to. Then the
** QA scripts to check that everything is good (heh), and then the various report
** construction output pulls. Or rather, now that report construction procedures
** are automated, you just look through this guy once the report is complete.
**
******************************************************************************/



/****************** TRACKING PROGRESS: ******************/

EXECUTE citeam.logger_get_latest_job_events 'VespaExecView', 3;
-- Same setup as every other regular report.

/****************** Q00: QA ON ExecView BUILD ******************/

-- Uh yeah....
