/******************************************************************************
**
** Project Vespa: MBM graph stored procedure (by channel)
**
** Note: this procedure doesn't cap views, and as such, isn't really usable
** at the moment. Maybe later this feature will be added, but until then,
** these procedures aren't really useful.
**
** See also:
**      http://rtci/vespa1/Vespa%20minute-by-minute%20procedures.aspx
**
** Get MBM graph for a given channel across a given time interval, with a
** specified resolution (default to 1 minute). This guy is kind of a fork
** of "make_MBM_graph" though they work slightly differently; this one lets
** you scan a particular area in fine detail without having to pull all of
** the graph for a whole show. Good for analysing commercial breaks up close,
** or for profiling accross many programms and getting their results into a
** single graph.
**
** << Begin COPY from make_MBM_graph documentation: >>
**
** Currently we're only supporting live events, but this comes from the Vespa
** events not putting timeshifted stuff in the X_VIEWING_START_TIME / END_TIME
** columns rather than exclusions in this code. We are supporting shows that
** span multiple days, even shows with durations longer than 24 hours, and
** we've also got support for all future daily tables (provided Ops don't
** change their table naming scheme).
**
** This procedure doens't go through the motions of allocating minutes to
** channels or shows or whatnot, these all (will) get pre-computed during the
** data load, and the different methods of allocating minutes etc go into the
** different columns. This currently uses X_VIEWING_START_TIME / END_TIME but
** as we roll out the Barb minute-by-minute version, we'll probably remove the
** optional sample time from that procedure, since it's silly to try and get
** finer details from data that you've already normalised to a coarser scale.
**
** Result set: Delivers a big table with the following in columns:
**      i/ the start point of each time slice (usually minutes by default)
**      ii/ the total number watching the programme
**      iii/ number of viewers scaled to the total number of enabled boxes
**              at the point the show was aired.
**
** << End of COPY from make_MBM_graph >>
**
** Arguments:
**      1/ BARB code for the channel you want to graph
**      2/ Start time for graph (as a DATETIME)
**      3/ End time for graph (as a DATETIME)
**      4/ Slice resolution in seconds, optional, defaults to 60 (1 minute)
**              and a minimum of 2s is enforced
**
** Features needed:
**  1. Add columns to distinguish between live and time-shifted viewing.
**  2. Add functionality to scale numbers up to sky base (Scaling 2 now!)
**  3. How are we going to treat scaling of time-shifted viewing?
**  4. Introduce Capping now that capping 2 is available
** (Note these feature requests also match those of the MBM_by_prog procedure)
**
*/

if object_id('make_Uncapped_MBM_channel_graph') is not null drop procedure make_Uncapped_MBM_channel_graph;
create procedure make_Uncapped_MBM_channel_graph
        @barb_code              int             -- build the viewing graph for the station of this BARB code
        ,@start_graph           datetime        -- Start the graph from this time
        ,@end_graph             datetime        -- Go until this time
        ,@measureslice          int = 60        -- Have one data point for each interval of this size (in seconds)
as
begin
        -- Viewing window is capped below at 2 seconds:
        if @measureslice < 2
        begin
                set @measureslice = 2
        end
        
        declare @programme_day                  date            -- because we want to scan multiple days, show might go past midnight
        declare @dailyend                       datetime        -- the endpoint of the last programme we consider for each daily table
        declare @current_programme_end          datetime
        declare @minute_sample_point            datetime        -- the start/end columns are assumed processed for us, we need only one sample point
        declare @viewing_minute                 datetime
--        declare @programme_trans_sk             varchar(20)     -- Apparently Sybase has problems putting unsigned bigints in variables or something crazy like that?
        declare @programme_trans_sk             unsigned bigint
        declare @show_sequence                  int             -- to track which of the shows we'd need to be profiling at any point
        declare @enabled_box_count              bigint          -- To scale the viewhood against enabled boxes at the time the thing aired
        declare @dailytable_query_hurg          varchar(2000)   -- For dynamically pulling stuff from different daily tables

        -- The table that will eventually get pushed out as the report:
        create table #minute_by_minute_graph_table (
                Slice_start                     datetime    not null,
                Source_day                      varchar(20) not null,
                Viewed_Live_per_day             bigint      not null
                --scaled_Viewed_Live_per_day      double      default null -- this is updated after the loop, so defaults to null
        )
        
        create unique index consistency_checker on #minute_by_minute_graph_table (Slice_start, source_day)
        
        -- Make a list of all the programmes we'll need, in order
        select
                programme_trans_sk,
                TX_START_DATETIME_UTC,
                TX_END_DATETIME_UTC,
                rank() over (order by TX_END_DATETIME_UTC) as show_sequence
        into #relevant_programs
        from sk_prod.vespa_epg_dim
        where barb_code = @barb_code
            and TX_START_DATETIME_UTC < @end_graph 
            and TX_END_DATETIME_UTC > @start_graph
        
        -- We leave the initialisation of variables until we're in a particular daily file, since we
        -- don't know exactly how long events are managed over multiple daily tables.

        set @programme_day = convert(date, @start_graph) -- so, yeah, you won't get any results back if @start_graph > @end_graph
        
        -- Loop through all the daily tables we might need
        while @programme_day <= convert(date, @end_graph) -- Need equality here as this is the check of which daily tables to scan
        begin 
                -- Only proceed if the appropriate daily table exists; do we really need to check this?
                if object_id('sk_prod.VESPA_STB_PROG_EVENTS_' || dateformat(@programme_day,'yyyymmdd')) is not null
                begin
                        
                        -- Find the earliest programme which has some overlap with the day in question
                        select @show_sequence = min(show_sequence)
                        from #relevant_programs
                        -- So we don't scan all of the porogrammes again when we go to the next daily table

                        -- Get the details we need for the next show to scan
                        select
                                @viewing_minute         = case when @start_graph > TX_START_DATETIME_UTC then @start_graph else TX_START_DATETIME_UTC end
                                ,@programme_trans_sk    = programme_trans_sk
                                ,@current_programme_end = TX_END_DATETIME_UTC
                        from #relevant_programs
                        where show_sequence = @show_sequence

                        -- Since there's processing going into @viewing_minute, it's easier to build the sample point here
                        set @minute_sample_point = dateadd(second, 1, @viewing_minute)
                        
                        -- Using the same kind of poor-mans-parameterised query trick as previously.
                        -- Does Sybase even support parameterised queries being defined in the middle
                        -- of a stored procedure?
                        set @dailytable_query_hurg = 'insert into #minute_by_minute_graph_table (
                                Slice_start,
                                Source_day,
                                Viewed_Live_per_day
                        )
                        select ''##&&##'',
                                ''' || dateformat(@programme_day, 'yyyy-mm-dd') || ''',
                                count(1)
                        from sk_prod.VESPA_STB_PROG_EVENTS_' || dateformat(@programme_day,'yyyymmdd') || '
                        where programme_trans_sk = ##**##
                                and X_VIEWING_START_TIME < ''##££##''
                                and X_VIEWING_END_TIME > ''##££##''
                                and (play_back_speed is null or play_back_speed=2)
                                and x_programme_viewed_duration>0
                                and Panel_id in (4, 5)
                                -- and Event_Type = ''evChangeView''
                                and x_type_of_viewing_event <> ''Non viewing event'''
                        -- Okay, so:
                        --      ##&&## gets replaced by the the start point of the relevant interval
                        --      ##££## gets replaced by sampling point of said interval
                        --      ##**## gets replaced by the programme key
                        -- But the daily table, it gets fixed in the query now.

                        -- Oh hey but we also need to figure out where to stop looking through this daily table
                        -- and move on to the next one...
                        select @dailyend = max(TX_END_DATETIME_UTC)
                        from #relevant_programs
                        where convert(date, TX_START_DATETIME_UTC) = @programme_day
                        -- ie, sampling all programs that start on the day of the daily table
                        
                        -- But we also need to check how this compares to the limit that we're graphing up to...
                        set @dailyend = case when @dailyend > @end_graph then @end_graph else @dailyend end

                        -- Iterate and build up the MBM graph:
                        WHILE @minute_sample_point < @dailyend
                        begin
                                -- Replace the placeholders with the minutes for each loop
                                execute(replace(
                                        replace(
                                                replace(@dailytable_query_hurg,
                                                        '##&&##',
                                                        dateformat(@viewing_minute, 'yyyy-mm-dd hh:mm:ss')
                                                        ),
                                                '##**##',
                                                convert(varchar(20), @programme_trans_sk)
                                                ),
                                        '##££##',
                                        dateformat(@minute_sample_point, 'yyyy-mm-dd hh:mm:ss')
                                        ))
                                -- Don't really mind having other stuf in the minute by minute cycle, the
                                -- bottleneck is still the above execution.
                                        
                                -- Click the counters on by the size of the sample window;
                                set @viewing_minute             = dateadd(second, @measureslice, @viewing_minute)
                                set @minute_sample_point        = dateadd(second, @measureslice, @minute_sample_point)

                                -- Check if we need to move on to the next programme key:
                                if @minute_sample_point > @current_programme_end
                                -- Need to use the sample point, as that's the one that actually determines hits
                                begin
                                        set @show_sequence = @show_sequence + 1
                                        select
                                                -- Only need the keys and next boundry, the viewing minute etc are still fine
                                                @programme_trans_sk     = programme_trans_sk
                                                ,@current_programme_end = TX_END_DATETIME_UTC
                                        from #relevant_programs
                                        where show_sequence = @show_sequence
                                END                        
                        END
                END
                
                -- Move on to the next daily table
                set @programme_day = dateadd(day, 1, @programme_day)
        END

        -- Normalise the viewing data by number of enabled boxes
        --select @enabled_box_count = count(1)
        --from sk_prod.VESPA_SUBSCRIBER_STATUS
        --where result='Enabled'
        --and cast(request_dt as date) <= cast(@start_graph as date)
        -- Profile for the day on which the graph starts - not doing dynamic enablement across
        -- the graphed days yet - might we in the future?

        -- Not yet integrated the new scaling procedures. We could, they're set up, but yeah.
        --update #minute_by_minute_graph_table set 
        --scaled_Viewed_Live_per_day = Viewed_Live_per_day / convert(double, @enabled_box_count) -- if they both get left as INTs then integer division makes it zero
        
        -- The returning query: GROUP out the source table because we only care about the hit counts
        select
                Slice_start,
                sum(Viewed_Live_per_day) as Viewed
        from #minute_by_minute_graph_table
        group by Slice_start
        order by Slice_start

end;

commit;
grant execute on make_Uncapped_MBM_channel_graph to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
commit;
