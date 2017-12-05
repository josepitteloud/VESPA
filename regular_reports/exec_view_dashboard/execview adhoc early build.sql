-- So there are a few things which we're currently building for fairly informal reporting,
-- these will eventually get turned into something a patch more stable, but right now these
-- concise numbers are:

/*

-- So this gets all the numbers, but not the percentages..
select count(1)                                                                                 as boxes_enabled
    ,sum(case when logs_returned_in_30d > 0                     then 1 else 0 end)              as boxes_returning_data_30d
    ,sum(case when PS_flag = 'P'                                then 1 else 0 end)              as enabled_primary_boxes
    ,sum(case when PS_flag = 'P' and logs_returned_in_30d > 0   then 1 else 0 end)              as primary_boxes_returning_data_30d
    ,sum(case when PS_flag = 'S'                                then 1 else 0 end)              as enabled_secondary_boxes
    ,sum(case when PS_flag = 'S' and logs_returned_in_30d > 0   then 1 else 0 end)              as secondary_boxes_returning_data_30d
    ,count(distinct account_number)                                                             as enabled_accounts
    ,count(distinct case when logs_returned_in_30d > 0 then account_number else 'nope' end) - 1 as accounts_returning_data
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA';
*/

-- Copy this whole load into a browser, press GO, and it'll spit out most of the
-- stuff you need, only have to copy this guy into Excel and do the percentage
-- formats yourself...
create table #exec_hack_report_dump (
    id                      tinyint     primary key     -- We'll just use this for making sure that all the numbers come out in the right order
    ,vespa_value             integer
    ,vespa_statement         varchar(100)
);

insert into #exec_hack_report_dump
select
    1
    ,count(1)
    ,' boxes enabled'
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA';

insert into #exec_hack_report_dump
select
    2
    ,count(1)
    ,' boxes returned data (in the last 30 days)'
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA'
and logs_returned_in_30d > 0;

insert into #exec_hack_report_dump
select
    4
    ,count(1)
    ,' primary boxes enabled'
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA' and PS_flag = 'P';

insert into #exec_hack_report_dump
select
    5
    ,count(1)
    ,' returned data (in the last 30 days)'
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA' and PS_flag = 'P' and logs_returned_in_30d > 0;

insert into #exec_hack_report_dump
select
    6
    ,null
    ,' primary boxes returned data in the last 30 days'
;

insert into #exec_hack_report_dump
select
    8
    ,count(1)
    ,' secondary boxes enabled'
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA' and PS_flag = 'S';

insert into #exec_hack_report_dump
select
    9
    ,count(1)
    ,' returned data (in the last 30 days)'
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA' and PS_flag = 'S' and logs_returned_in_30d > 0;

insert into #exec_hack_report_dump
select
    10
    ,null
    ,' secondary boxes returned data in the last 30 days'
;

insert into #exec_hack_report_dump
select
    12
    ,count(distinct account_number)
    ,' accounts enabled'
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA';

insert into #exec_hack_report_dump
select
    13
    ,count(distinct account_number)
    ,' accounts returning data (in the last 30 days)'
from vespa_analysts.vespa_single_box_view
where panel = 'VESPA' and logs_returned_in_30d > 0;

-- and yeah, there are a few holes for formatting:
insert into #exec_hack_report_dump (ID) values (3);
insert into #exec_hack_report_dump (ID) values (7);
insert into #exec_hack_report_dump (ID) values (11);

-- Stil have to sort out the percentages in Excel or something :(
select vespa_value, vespa_statement
from #exec_hack_report_dump
order by id;

-- Maybe there's something we can do in Sybase for format masking or something,
-- but generally, it seems kind of annoying to format numbers. Simplest might
-- be a looping stored proc that adds the commas to a string ourself....