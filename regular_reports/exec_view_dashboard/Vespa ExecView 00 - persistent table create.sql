/******************************************************************************
**
** Project Vespa: Exec View Dashboard Report
**                  - Persistent Table Creation
**
** Permanent tables for the Exec View Dashboard report, though moslty it might
** just end up pulling results from the other constructed tables of the various
** dashboards and dialback reports. Otherwise we're just doing the same thing
** twice, though there's an innate aspect of that in this task anyway.
**
**      http://rtci/vespa1/Exec%20Dashboard%20View.aspx
**
** See also "Vespa ExecView 01 - build reports.sql" for outstanding task list.
**
******************************************************************************/

-- Accounts and profiling:

if object_id('vespa_ExecView_panel_population') is not null
   drop table vespa_ExecView_panel_population;
create table vespa_ExecView_panel_population (
    subscriber_id                   decimal(10,0)
    ,account_number                 varchar(20)         not null
);

commit;
create          index fake_pk       on vespa_ExecView_panel_population (subscriber_id);
create          index for_joining   on vespa_ExecView_panel_population (account_number);
commit;

-- And then whatever else we want...

/**************** PERMISSIONS! ****************/

grant select on vespa_ExecView_panel_population to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;

commit;
