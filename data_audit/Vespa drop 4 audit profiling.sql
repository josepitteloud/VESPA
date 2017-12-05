/*****************************************************************************************
**
** Project Vespa: Automated Data Audit Script.
**
** This is a script which clicks over a bunch of tables and pulls out basic audit data
** for each. We're using it each time the new data drop turns up. It's long and slow,
** but will tell us some basic stuff about the data.
**
** In drop 1 there were about 600 manual queries which gathered things in silly ways.
** This is much more automated, a bit more comprehensive, etc, looping over system
** column information tables.
**
** So we want to extract for each table:
** a/ count of records in table
** b/ count of nulls
** c/ count of emptystrings (varchar fields only)
** d/ number of distinct records
** e!/ count of instances of most common non-null value
** f!/ sample most common non-null value - if that count exceeds 10 (convert to varchar 255?)
**
** ! fields e & f: They'd be annoying to calculate, they're not in scope. Also, (d) is
** only being calculated for tables with less than two million rows, because otherwise
** it'll take ages.
**
** This is going to take a whole chunk of processing time, but hey. That's why we're
** using a cursor, so we can leave it overnight. [Update: Due to some weird artifact of
** Sybase's cursor/thread "optimisation", the events view query won't run if you try to
** do it all from the same cursor. Instead you have to put extra filters on and form the
** batches manually. Kind of a pain really. And it still takes and age of processing too.]
**
** Update: This is in play for all tables except the events view. The events view is so
** big and slow that the processing gets halfway through and then it's a week later and
** more events arrive. So we're not going to be counting the distinct things from the
** events view. But everything else was done alread, as of 
**
*****************************************************************************************/


/****************** AUTOMATIC REPORT TABLE CREATION & POPULATION ******************/

-- Form table in which we'll build the whole data report
if object_id('stafforr.vespa_drop4_audit_profiling') is not null
   drop table stafforr.vespa_drop4_audit_profiling;
-- Be carefull dropping it and starting it again, the distinct value counts take an age.
create table stafforr.vespa_drop4_audit_profiling (
        id                              bigint identity not null primary key
        ,table_name                     varchar(120) not null
        ,column_order                   int not null
        ,column_name                    varchar(120) not null
        ,column_type                    varchar(20) not null
        ,field_length                   int not null
        ,tolerates_nulls                varchar(1)
        ,table_row_count                bigint default null
        ,column_null_count              bigint default null
        ,column_emptystring_count       bigint default null
        ,column_distinct_valules        bigint default null
);
-- Start off by populating table with data on each of the columns from the system columns lookup
insert into stafforr.vespa_drop4_audit_profiling (
        table_name
        ,column_order
        ,column_name
        ,column_type
        ,field_length
        ,tolerates_nulls
)
select
        tname
        ,colno
        ,cname
        ,coltype
        ,length
        ,nulls
from sys.syscolumns
where creator = 'sk_prod' and (lower(tname) like 'vespa%') -- syscolumns is case sensitive :/
and lower(tname) not like '%events_201%' -- There seem to be a lot of events components and backups, all with timestamps appended, and we don't want any of them here
and lower(tname) not like '%_backup%' -- but not all of them have timestamps appended
-- There may yet be other tables we also want to exclude, but hey, we'll manage that should they turn up.
order by lower(tname), colno;

-- We shouldn't have more than one oclumn in each location per table:
create unique index column_index on stafforr.vespa_drop4_audit_profiling (table_name, column_order);

-- Quick QA to see if which tables we're dealing with:
--select distinct table_name from stafforr.vespa_drop4_audit_profiling;
-- OK, we're still not touching the events view though, it's Server Death.

/****************** VARIABLES WE'LL NEED ******************/

create variable @dynamic_SQL_cludge             varchar(2000);  -- for building the SQL that queries each column & table
create variable @id                             bigint;         -- for recording which record in the audit table we update
create variable @table_name                     varchar(120);
create variable @column_name                    varchar(120);
create variable @column_type                    varchar(20);    -- we need to know if it's VARCHAR or not to count the emptystrings.
create variable @table_row_count                bigint;
create variable @column_null_count              bigint;
create variable @column_emptystring_count       bigint;
create variable @column_distinct_valules        bigint;
-- create variable @column_common_max              bigint; -- Not in play.
-- create variable @column_common_value            varchar(255); -- Ditto.

/****************** FIRST THE ROW COUNTS FOR WHOLE TABLES (OR VIEWS) ******************/

/* From http://infocenter.sybase.com/help/topic/com.sybase.dc20020_1251/html/databases/X61512.htm
** @@sqlstatus = 0 means successful fetch
** @@sqlstatus = 1 means error on previous fetch
** @@sqlstatus = 2 means end of result set reached
** but we're not going to be doing too much with error catching tbh
*/

-- Group from the column listing into a table based view; we'll only scan each table
-- once for the total count.
declare table_row_cursor insensitive cursor for
select distinct table_name
from stafforr.vespa_drop4_audit_profiling

open table_row_cursor
-- All these queries are without ; terminators as putting them in makes the
-- transaction close or something, and then the cursor declaration dissappears
-- and nothing works. But, without the ; terminators, it all goes happily and
-- we get what we need.
fetch next table_row_cursor into @table_name

while (@@sqlstatus = 0)
begin
        if lower(@table_name) like '%_events_%'
                or lower(@table_name) like '%test%'
                or lower(@table_name) like '%test'
                or lower(@table_name) like '%sensitive%'
        begin
            fetch next table_row_cursor into @table_name
            continue
        end

        set @dynamic_SQL_cludge = 'select @table_row_count = count(1) from sk_prod.' || @table_name
        -- In theory this puts the count into the variable; hopefully there are
        -- no problems with scope given that this happens inside another call
        execute(@dynamic_SQL_cludge)
        -- wohoo! initial testing indicate that this should work as we need it to.

        -- Push the row counts for each table into the report form
        update stafforr.vespa_drop4_audit_profiling
        set table_row_count = @table_row_count
        where table_name = @table_name

        --commit -- closing the transactions apparently improves performance, but it
        -- seems to have a habit of borking up the cursor :(

        -- Moving on!
        fetch next table_row_cursor into @table_name
end
close table_row_cursor

/****************** REPORTING ******************/

select * from stafforr.vespa_drop4_audit_profiling
order by table_name, column_order;

