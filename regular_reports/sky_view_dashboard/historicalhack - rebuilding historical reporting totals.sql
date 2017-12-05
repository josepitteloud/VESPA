-- Manual hack: repopulating the historical dialback information for the Sky View panel
-- This goes into the operational dashboard. We're not doing reporting boxes though,
-- because we have no list of who's selected for data return at any point.

-- So a bunch of this is stolen directly from the SVD build... we eventually want to
-- repopulate vespa_analysts.vespa_SVD_log_aggregated_archive

create variable @SVD_logging_ID         bigint;
create variable @Refresh_identifier     varchar(40);

set @Refresh_identifier = convert(varchar(10),today(),123) || ' SVD aggs rebuild'
EXECUTE citeam.logger_create_run 'SVD aggregates rebuild', @Refresh_identifier, @SVD_logging_ID output;

create variable @SQL_daily_kludge       varchar(2000);
create variable @scanning_day           date;

-- Not particularly cool to be batching this manually, but we're only running it once ever, so yeah.
create variable @first_batch_date       date;
create variable @last_batch_date        date;

set @first_batch_date   = '2011-09-01';
set @last_batch_date    = '2011-11-01';

/* Batch chunks:
First:  '2011-07-01' and '2011-09-01'
Second: '2011-09-01' and '2011-11-01'
Third:  '2012-11-01' and '2011-01-01'
- After that, just run as regular updates.
*/

-- With hindsight: processing a two month batch only takes 5 minutes, probably didn't
-- need to batch that at all. Oh well.

-- from here needs to be run each time:

drop table vespa_SVD_log_archive_dump;
create table vespa_SVD_log_archive_dump (
    subscriber_id                   decimal(10,0)
    ,stb_log_creation_date          datetime
    ,account_number                 varchar(20)
    ,doc_creation_date_from_6am     date
    ,log_id                          varchar(100)
);

if object_id('SVD_log_archive_thingy') is not null
   drop table SVD_log_archive_thingy;
-- Summary of suitably new log entries. They get pulled out of the daily tables above, and
-- then get deduplicated (across daily tables) as they arrive in this log listing. Older
-- log numbers are just pulled out of the archives though.
create table SVD_log_archive_thingy (
--        id                              bigint          identity not null primary key -- only useful in the discontinued deduplication procedure
        log_id                          varchar(100)    not null primary key
        ,subscriber_id                  decimal(8)      not null
        ,account_number                 varchar(20)     not null
--        ,document_creation_date         date            not null -- never gets used, he can get left out
        ,doc_creation_date_from_6am     date            not null
);

set @SQL_daily_kludge = 'insert into vespa_SVD_log_archive_dump (
        subscriber_id
        ,stb_log_creation_date
        ,account_number
        ,doc_creation_date_from_6am
)
select
        subscriber_id
        ,stb_log_creation_date
        ,min(account_number)
        ,min(convert(date, dateadd(hour, -6, document_creation_date))) as doc_creation_date_from_6am
            -- Anything coming in before 6AM gets treated as if it were the prior day 
from sk_prod.VESPA_STB_PROG_EVENTS_#*££*# -- will get replaced by the daily stamp of each table
where panel_id = 1 -- 1 is Sky View Panel
and document_creation_date is not null
group by subscriber_id, stb_log_creation_date
';

-- Hardcoding: this is when the Sky View Panel historical stuff will start:
set @scanning_day = @first_batch_date;

-- Do we want to batch these much? I dunno, we could just create a giant thing...
-- we only care about the distinct accouts and boxes, but we do have to worry
-- about the whole 6AM shuffle thing... though, is there that broadcast day field
-- we can use? tx_date? There is Adjusted_event_date.... might be that the flags
-- we want just aren't on the older tables. Oh well. Ok, so, yeah, we're batching
-- these kind of manually into two minth chunks. We'll see how long it takes. Not
-- that long, probably, to be honest.

while @scanning_day < @last_batch_date
begin
    execute(replace(@SQL_daily_kludge, '#*££*#', dateformat(@scanning_day,'yyyymmdd')))
    -- Not bothering to check that the daily exists, it should do.
    execute citeam.logger_add_event @SVD_logging_ID, 3, 'C01: Daily table scanned... (' || dateformat(@scanning_day,'yyyymmdd') || ')'
    
    set @scanning_day = dateadd(day, 1, @scanning_day)
    commit
end;
-- above loop is done! for batch 1; apparently it's all done now.


update vespa_SVD_log_archive_dump
set log_id = cast((subscriber_id||' '||stb_log_creation_date) as varchar(100));
commit;

-- Should be redundant given the table was recreated and dropped, but Sybase is
-- being weird about it...
delete from SVD_log_archive_thingy;
commit;

-- Now we can summarise those into one record per log batch (those batches
-- started off spread over different daily event tables)
insert into SVD_log_archive_thingy (log_id, subscriber_id, account_number, doc_creation_date_from_6am)
select
        log_id
        ,min(subscriber_id)
        ,min(account_number)
        ,min(doc_creation_date_from_6am)
from vespa_SVD_log_archive_dump
group by log_id;

-- That's the last of the pull queries: the repopulation query is:
commit;

insert into vespa_analysts.vespa_SVD_log_aggregated_archive (
        doc_creation_date_from_6am
        ,log_count
        ,distinct_accounts
        ,distinct_boxes
)
select
        convert(date, doc_creation_date_from_6am)
        ,count(*) as logs
        ,count(distinct account_number) as distinct_accounts
        ,count(distinct subscriber_id) as distinct_boxes
from SVD_log_archive_thingy
where doc_creation_date_from_6am >= @first_batch_date
-- converting to date basically sets the time to 00:00:00 so we start
-- archiving from the begining of the day we haven't archived at all yet
and doc_creation_date_from_6am < @last_batch_date
group by doc_creation_date_from_6am;

commit;

execute citeam.logger_add_event @SVD_logging_ID, 3, 'C01: Full pass complete!';