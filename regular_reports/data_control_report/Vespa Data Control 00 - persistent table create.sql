/******************************************************************************
**
** Project Vespa: Data Control Report
**                  - Persistent Table Creation
**
** Essentially, all we do with the persistent tables for the data control
** report are just log all the various failures that get noticed. Flat. Not
** special. Contunually growing though, but not really useful after an issue
** is spotted, raised, and resolved.
**
******************************************************************************/


create table Vespa_DataCont_Flag_log (
    ID                  bigint          not null identity primary key
    ,noted              date            not null default today()
    ,issue_table        varchar(60)     not null
    ,issue_class        varchar(40)
    ,issue_description  varchar(100)
    ,issue_count        bigint
    ,issue_portion      float                               -- for when we want to do percentages
    ,issue_severity     tinyint         default 0           -- 0 is a bit of an inconvenience, up to 5 for supercritical?
);

-- Also managing severity via what goes into the logger I guess.

grant select on Vespa_DataCont_Flag_log to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
