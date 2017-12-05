/****************************************************************
**
**      PROJECT VESPA: SPOT LOADING TRACKER
**
** The load is one big script, but it fires things into Logger
** so you can see how it's going from another session. Chances
** are, though, that one section is going to provide most of
** the slowdown, and everything else will look speedy.
**
****************************************************************/

EXECUTE citeam.logger_get_latest_job_events 'VespaAdSpotLoading',3;
