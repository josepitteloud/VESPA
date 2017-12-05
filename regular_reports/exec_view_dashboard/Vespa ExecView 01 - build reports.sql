/******************************************************************************
**
** Project Vespa: Exec View Dashboard Report
**                  - Weekly Refresh Script
**
** The Exec View Dashboard report looks at who stable the Vespa panel is with
** to scaling. Coverage of segments, over / under indexing of stuff, most
** needed boxes for upcoming activation, stuff like that I guess. It's still
** very much in design, but will eventually be scheduled.
**
** See also:
**
**      http://rtci/vespa1/Exec%20Dashboard%20View.aspx
**
** Still to do:
**
**  1. Decide on what the Exec build will contain
**  2. Design report
**  3. Build! A lot might come from other report builds though.
**  4. Automate. That might be where most of the work lives, since we're just
**      summarising other existing reports with this guy.
**
** Recently completed:
**
******************************************************************************/

-- This guy still has the proc wrapper, but it's still heavy deep in dev and is just
-- getting stepped through manually until it's stable.

if object_id('vespa_analysts.ExecView_make_report') is not null
   drop procedure vespa_analysts.ExecView_make_report;
create procedure vespa_analysts.ExecView_make_report
as
begin

/****************** A01: SETTING UP THE LOGGER ******************/

DECLARE @ExecView_logging_ID      bigint
DECLARE @Refresh_identifier     varchar(40)
declare @run_Identifier         varchar(20)
-- For putting control totals in prior to logging:
DECLARE @QA_catcher             integer

if user = 'vespa_analysts'
    set @run_Identifier = 'VespaExecView'
else
    set @run_Identifier = 'ExecView test ' || upper(right(user,1)) || upper(left(user,2))

set @Refresh_identifier = convert(varchar(10),today(),123) || ' ExecView refresh'
EXECUTE citeam.logger_create_run @run_Identifier, @Refresh_identifier, @Model_logging_ID output

-- Given we're doing Exec View Dashboard and not anything about reporting, we'll take
-- the date as the profiling date as when the Sky Base refresh last turned.
execute vespa_analysts.Regulars_Get_report_end_date @report_end_date output -- A Saturday


commit

-- So identifying churn in a period is easy. So now... do any of these show up
-- in our Vespa table?

EXECUTE citeam.logger_add_event @ExecView_logging_ID, 3, 'A01: Complete! (Report setup)'
commit

/****************** A02: TABLE RESETS ******************/

execute ExecView_clear_transients

EXECUTE citeam.logger_add_event @ExecView_logging_ID, 3, 'A02: Complete! (Table resets)'
commit

/****************** B01: OTHER STUFFS... ******************/



/****************** X01: AND WE'RE DONE! ******************/

EXECUTE citeam.logger_add_event @ExecView_logging_ID, 3, 'ExecView: weekly refresh complete!'
COMMIT

end;

-- And somethign else to clean up the junk that was built:
if object_id('ExecView_clear_transients') is not null
   drop procedure ExecView_clear_transients;
create procedure ExecView_clear_transients
as
begin
    delete from vespa_ExecView_panel_population
    if object_id('vespa_ExecView_01_SomeOutput') is not null
        drop table vespa_ExecView_01_SomeOutput
end;


grant execute on vespa_analysts.ExecView_make_report          to public;
grant execute on vespa_analysts.ExecView_clear_transients     to public;
-- Need the central scheduler thing to be able to call the procs. But it gets
-- run within the vespa_analytics account, so it doesn't mean that any random
-- public person can see what's in the resulting tables.


