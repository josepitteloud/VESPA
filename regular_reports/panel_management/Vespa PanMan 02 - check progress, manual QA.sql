/******************************************************************************
**
** Project Vespa: Panel Management Report
**                  - Progress tracking & manual QA
**
** So after you run the weekly build script ("Vespa PanMan 01 - build reports.sql")
** you can use the logger call in here to check how far it's got to. Then the
** QA scripts to check that everything is good (heh), and then the various report
** construction output pulls. Or rather, now that report construction procedures
** are automated, you just look through this guy once the report is complete.
**
******************************************************************************/


/****************** TRACKING PROGRESS: ******************/

-- Same setup as every other regular report.
EXECUTE citeam.logger_get_latest_job_events 'VespaPanMan', 3;

-- There are indeed some automated tests which we run, which you should check...
EXECUTE citeam.logger_get_latest_job_events 'VespaPanMan', 2;


/****************** Q00: QA ON PANMAN BUILD ******************/

