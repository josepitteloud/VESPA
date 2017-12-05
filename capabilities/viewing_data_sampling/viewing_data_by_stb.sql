/******************************************************************************
**
** Project Vespa: Extract viewing data for specific set top boxes
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
** This is similar to the procedure for set top boxes, but works for a list
** of subscriber IDs instead. Don't use to many, otherwise the resulting table
** will of course be huge. The results all get spat out but you can put them
** into a table for future use as follows:
**
**  grant select on mySTBlist to vespa_analysts;
**
**  select *
**  into my_new_table
**  from vespa_analysts.get_viewing_by_stb('stafforr.mySTBlist',
**                                          '2011-07-19',
**                                          '2011-07-24');
**
** The the procedure runs under vespa_analysts so it will fail if vespa_analysts
** doesn't have SELECT permissions on the table storing the program keys. Also,
** make sure that the table you pass in has a column called subscriber_id which
** is of data type decimal(8,0) because that's the format in the viewing data.
**
** Really try to limit the number of set top boxes you put in the list, you'll
** otherwise just end up creating a table comparable in size to the events
** view, and that will never work out.
**
** The data that comes out isn't a complete set of fields you'd get from the
** events view; we've concentrated on fields which should be most useful. We
** might expand the field listing later if it turns out we need more things
** (allocated BARB minutes etc).
**
** Bug:
**  ## Still need to look into the long range failures, because that threads
**      thing might still give us issues. This will also propogate into the
**      by programme procedure, if it's an issue.
**
******************************************************************************/

-- This guy is largely cobbled together from the old viewing by programme thing.
-- Still not sure if this procedure is going to be useful, pulling out a tiny
-- sample kind of runs counter to the purpose of Vespa. Oh well.

if object_id('get_viewing_by_stb') is not null drop procedure get_viewing_by_stb;
create procedure get_viewing_by_stb @sbtlist varchar(80), @startdate date, @enddate date
as
begin
        declare @SQL_query_sludge               varchar(2000)
        declare @dailytablelooper               date

        create table #sampled_results (
                cb_row_id                       unsigned bigint not null primary key,
                subscriber_id                   decimal(8,0) not null,
                account_number                  varchar(20) not null,
                ADJUSTED_EVENT_START_TIME       datetime not null,
                X_ADJUSTED_EVENT_END_TIME       datetime not null,
                programme_trans_sk              unsigned bigint not null,
                RECORDED_TIME_UTC               datetime,
                X_VIEWING_START_TIME            datetime,
                X_VIEWING_END_TIME              datetime
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
                RECORDED_TIME_UTC,
                X_VIEWING_START_TIME,
                X_VIEWING_END_TIME,
        )
        select
                ev.cb_row_id,
                ev.subscriber_id,
                ev.account_number,
                ev.ADJUSTED_EVENT_START_TIME,
                ev.X_ADJUSTED_EVENT_END_TIME,
                ev.programme_trans_sk,
                ev.RECORDED_TIME_UTC,
                ev.X_VIEWING_START_TIME,
                ev.X_VIEWING_END_TIME
        from sk_prod.VESPA_STB_PROG_EVENTS_##&!*&!*## as ev
        inner join ' || @sbtlist || ' as sample_list
        on ev.subscriber_id = sample_list.subscriber_id
        where (play_back_speed is null or play_back_speed=2)
        and x_programme_viewed_duration>0
        and panel_id in (4,5)
        and x_type_of_viewing_event <> ''Non viewing event''
        '
        -- remember this filter appears in all the other Vespa procs too. Do they
        -- have the same names in the daily tables? I hope so?

        -- Okay, no we can figure out where the daily table scan needs to start
        select @dailytablelooper = @startdate

        -- Now to build a loop over the requested daily tables, with an extra catch
        -- just to make sure the thing does eventually terminate sometime, as we'll
        -- never need to scan things from the future.
        while datediff(day, @enddate, @dailytablelooper) < 1 and datediff(day, today(), @dailytablelooper) < 1
        begin
                -- Rather than rebuilding the whole query each time, we're
                -- just substituting in the timestamp for the daily table
                if object_id('sk_prod.VESPA_STB_PROG_EVENTS_' || dateformat(@dailytablelooper, 'yyyymmdd')) is not null
                        -- And only run it if the daily table exists
                        execute(replace(@SQL_query_sludge, '##&!*&!*##', dateformat(@dailytablelooper, 'yyyymmdd')))
               
                set @dailytablelooper = dateadd(day, 1, @dailytablelooper)
        end

        -- Pull out the EPG values as we form the set of results
        select
                sr.cb_row_id,
                sr.subscriber_id,
                sr.account_number,
                sr.ADJUSTED_EVENT_START_TIME,
                sr.X_ADJUSTED_EVENT_END_TIME,
                sr.programme_trans_sk,
                epg.epg_title,
                sr.RECORDED_TIME_UTC,
                sr.X_VIEWING_START_TIME,
                sr.X_VIEWING_END_TIME,
                epg.tx_start_datetime_utc,
                epg.tx_end_datetime_utc,
                epg.genre_description,
                epg.sub_genre_description,
                epg.barb_code,
                epg.Channel_name,
                epg.epg_group_name
        from #sampled_results as sr
        inner join sk_prod.Vespa_EPG_dim as epg
        on sr.programme_trans_sk = epg.programme_trans_sk

end;

commit;
grant execute on get_viewing_by_stb to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
commit;
