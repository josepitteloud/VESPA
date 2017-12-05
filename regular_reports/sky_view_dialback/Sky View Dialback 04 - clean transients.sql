-- Project Vespa Dialback Report: clean out transient tables
-- These tables are permanent but they just hold the results of
-- queries which are going into excel in their current form.

-- Now everything lives in the vespa_analysts schema because
-- we're running it as a procedure...

execute vespa_analysts.SkyView_Dialback_clear_transients
;
