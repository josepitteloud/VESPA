/******************************************************************************
**
** Project Vespa: Extract viewing data for specific programme keys
**
** Note: this procedure doesn't cap views, and as such, isn't really usable
** at the moment. Maybe later this feature will be added, but until then,
** these procedures aren't really useful.
**
** See also:
**      http://rtci/vespa1/Vespa%20viewing%20sample%20procedures.aspx
**
** Update: These guys are no longer the recommended way to perform any kind of
** analysis as the data set is growing beyond what servers can process in this
** manner. The advice is to work directly from daily tables, filter & summarise
** as much as you can from there, so as to reduce server load to a feasible
** level. These stored procedures have been removed from the database.
**
** So now we're building a stored procedure which pulls out all the viewing
** data (or perhaps just a smaple of the viewing data?) associated with a
** particular bunch of programme keys (UNSIGNED BIGINTs). These keys you put
** in a table and pass the name of the table into the procedure. It's assumed
** that the relevant column name in this table is programme_trans_sk. Include
** the user space in the table name.
**
** The results all get spat out but you can put them into a table for future
** use as follows:
**
**  grant select on my_prog_list to vespa_analysts;
**
**  select *
**  into my_new_table
**  from vespa_analysts.get_viewing_by_programme('stafforr.my_prog_list');
** 
** By default the procedure gives you the live viewing and the first 30 days
** of recorded viewing. You can pass in an additional perameter to specifiy
** the timeshifted viewing limit, in days:
**
**  select *
**  into my_new_table
**  from vespa_analysts.get_viewing_by_programme('stafforr.my_prog_list', 14);
**
** This returns Live + first two weeks of timeshifted. The rules are:
**  -1  :   get all timeshifted viewing, scan all daily tables up to today()
**  0   :   get only live viewing
**  X>0 :   get live + first X days of timeshifted viewing
**
** The the procedure runs under vespa_analysts so it will fail if vespa_analysts
** doesn't have SELECT permissions on the table storing the program keys.
**
******************************************************************************/

if object_id('get_viewing_by_programme') is not null drop procedure get_viewing_by_programme;
create procedure get_viewing_by_programme @programmelist varchar(80), @maxdays smallint = 30
as
begin
        declare @SQL_query_sludge               varchar(2000)
        declare @dailytablelooper               date
        declare @lastdailytocheck               date

        create table #relevant_EPG_entries (
                programme_trans_sk              unsigned bigint not null primary key,
                tx_start_datetime_utc           datetime not null,
                tx_end_datetime_utc             datetime not null,
                epg_title                       varchar(40),
                genre_description               varchar(20),                      
                sub_genre_description           varchar(20),
                barb_code                       decimal(10,0),
                Channel_name                    varchar(40),
                epg_group_name                  varchar(30)
        )
        
        -- First off; pull a sample out of the EPG data with the things we need
        set @SQL_query_sludge = '
        insert into #relevant_EPG_entries (
                programme_trans_sk,
                tx_start_datetime_utc,
                tx_end_datetime_utc,
                epg_title,
                genre_description,
                sub_genre_description,
                barb_code,
                Channel_name,
                epg_group_name
        )
        select 
                epg.programme_trans_sk,
                tx_start_datetime_utc,
                tx_end_datetime_utc,
                epg_title,
                genre_description,
                sub_genre_description,
                barb_code,
                Channel_name,
                epg_group_name 
        from sk_prod.vespa_epg_dim as epg
        where epg.programme_trans_sk in
            (select distinct programme_trans_sk
            from ' || @programmelist || ')
        and epg.sensitive_channel = 0' -- do we want to be filtering sensitive channels here? well now we are
        
        execute(@SQL_query_sludge)
        
        -- Results holding table:
        create table #sampled_results (
                cb_row_id                       unsigned bigint not null primary key,
                subscriber_id                   decimal(8,0) not null,
                account_number                  varchar(20) not null,
                ADJUSTED_EVENT_START_TIME       datetime not null,
                X_ADJUSTED_EVENT_END_TIME       datetime not null,
                programme_trans_sk              unsigned bigint,
                epg_title                       varchar(40),
                RECORDED_TIME_UTC               datetime,
                X_VIEWING_START_TIME            datetime,
                X_VIEWING_END_TIME              datetime,
                tx_start_datetime_utc           datetime not null,
                tx_end_datetime_utc             datetime not null,
                genre_description               varchar(20),                      
                sub_genre_description           varchar(20),
                barb_code                       decimal(10,0),
                Channel_name                    varchar(40),
                epg_group_name                  varchar(30)
        )
        
        -- Prepare all of the SQL except for the timestamp bit:
        set @SQL_query_sludge = '
        insert into #sampled_results (
                cb_row_id,
                subscriber_id,
                account_number,
                ADJUSTED_EVENT_START_TIME,
                X_ADJUSTED_EVENT_END_TIME,
                programme_trans_sk,
                epg_title,
                RECORDED_TIME_UTC,
                X_VIEWING_START_TIME,
                X_VIEWING_END_TIME,
                tx_start_datetime_utc,
                tx_end_datetime_utc,
                genre_description,
                sub_genre_description,
                barb_code,
                Channel_name,
                epg_group_name
        )
        select
                ev.cb_row_id,
                ev.subscriber_id,
                ev.account_number,
                ev.ADJUSTED_EVENT_START_TIME,
                ev.X_ADJUSTED_EVENT_END_TIME,
                ev.programme_trans_sk,
                epg.epg_title,
                ev.RECORDED_TIME_UTC,
                ev.X_VIEWING_START_TIME,
                ev.X_VIEWING_END_TIME,
                epg.tx_start_datetime_utc,
                epg.tx_end_datetime_utc,
                epg.genre_description,
                epg.sub_genre_description,
                epg.barb_code,
                epg.Channel_name,
                epg.epg_group_name
        from #relevant_EPG_entries as epg
        inner join sk_prod.VESPA_STB_PROG_EVENTS_##&!*&!*## as ev
        on epg.programme_trans_sk = ev.programme_trans_sk
        where ##£@£@£@##
        and x_programme_viewed_duration>0
        and panel_id in (4,5)
        and x_type_of_viewing_event <> ''Non viewing event''
        '
        -- what other filters are we going to apply for viewing events? Hey,
        -- if we're going to have a lot of dynamic stuff around the place,
        -- maybe we define a common interface table which gives us the filters
        -- we should be using? that'd bea bit of a hack, but might be fun.

        -- The live-or-not flag depends on the @maxdays perameter:
        if @maxdays = 0
            -- 0 means live viewing only
            set @SQL_query_sludge = replace(@SQL_query_sludge, '##£@£@£@##', 'play_back_speed is null')
        else
            -- every other value of @maxdays wants timeshifted too
            set @SQL_query_sludge = replace(@SQL_query_sludge, '##£@£@£@##', '(play_back_speed is null or play_back_speed=2)')
        
        -- Okay, no we can figure out where the daily table scan needs to start
        select @dailytablelooper = convert(date, min(tx_start_datetime_utc)) from #relevant_EPG_entries
        -- There's a 5 hour rollover whereby stuff before 5AM goes into the previous
        -- daily table instead, so here we catch those cases and start on the daily
        -- table before if required.
        if datepart(hour, @dailytablelooper) <= 5
            set @dailytablelooper = dateadd(day, -1, @dailytablelooper)
        
        -- Could do this with IF ... SET but one SELECT ... CASE is nicer than nested IFs.
        -- And also, we need when the various shows finish. ## We're clicking through all
        -- intermediate daily tables too. Maybe we should build a separate list of daily
        -- tables to scroll over, rather than simply incrementing stuff? And only scan the
        -- daily tables which have the shows we want? But that's only going to be useful
        -- for live, wed still need all the daily tables in between for timeshifted. ##
        select @lastdailytocheck =
            case
                when @maxdays < 0 then today() -- -1 means no bounds on timeshifted viewing
                when @maxdays = 0 then convert (date, max(tx_end_datetime_utc)) -- 0 means live events only (do we want to test for 6AM cutoff here too? might result in one less daily table to check)
                else dateadd(day, @maxdays, @dailytablelooper) -- anything else means that many days
            end
        from #relevant_EPG_entries
        
        -- Now loop over the daily tables we want:
        while datediff(day, @lastdailytocheck, @dailytablelooper) < 1
        begin
                -- Rather than rebuilding the whole query each time, we're
                -- just substituting in the timestamp for the daily table
                if object_id('sk_prod.VESPA_STB_PROG_EVENTS_' || dateformat(@dailytablelooper, 'yyyymmdd')) is not null
                        execute(replace(@SQL_query_sludge, '##&!*&!*##', dateformat(@dailytablelooper, 'yyyymmdd')))
        
                set @dailytablelooper = dateadd(day, 1, @dailytablelooper)
        end
        
        select * from #sampled_results
end;

commit;
grant execute on get_viewing_by_programme to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
commit;
