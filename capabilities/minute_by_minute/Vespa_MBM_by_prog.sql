/******************************************************************************
**
** Project Vespa: MBM graph stored procedure (by program)
**
** Note: this procedure doesn't cap views, and as such, isn't really usable
** at the moment. The deal is that the capping and scaling are going to get
** built in pre-aggrgated tables, or otherwise augments to the daily tables,
** so we still don't have to actualyl do any of it here.
**
** See also:
**      http://rtci/vespa1/Vespa%20minute-by-minute%20procedures.aspx
**
** So this guy is originally based on the F1 report MBM version, which is
** specifically built to aggregate in the middle of the loop and not result
** in huge disk usage for one record per minute per box, which is crazy huge.
**
** Currently we're only supporting live events, but this comes from the Vespa
** events not putting timeshifted stuff in the X_VIEWING_START_TIME / END_TIME
** columns rather than exclusions in this code. We are supporting shows that
** span multiple days, even shows with durations longer than 24 hours, and
** we've also got support for all future daily tables - provided Ops don't
** change their table naming scheme. [ED: which they will be, soon.]
**
** Result set: Delivers a big table with the following in columns:
**      i/ the start point of each time slice (usually minutes by default)
**      ii/ the total number of boxes watching live at that point
**      iii/ the total number of boxes watching that minute as playback
**
** Arguments:
**      1/ programme_trans_sk: the programme key, an unsigned bigint.
**      2/ measure slice: the number of seconds to group viewing data by, an
**              integer, defaults to 60, minimum is 2. Optional.
**      3/ boundry only: an integer, limit the scan to the first N minutes (if
**              positive) and the last N minutes (if negative). Optional.
**
** This means that for analysing adds over the break between program keys @P1
** and @P2 you can call it like:
**
**      vespa_analysts.make_Uncapped_MBM_prog_graph @P1, 2, -1, -4
**      vespa_analysts.make_Uncapped_MBM_prog_graph @P2, 2, -1, 4
** 
** This will give you the last four minutes of @P1 and the first four minutes
** of @P2 with the results delivered in 2 second increments. You just have to
** stitch together the two tables, and you're done.
**
** This procedure doens't go through the motions of allocating minutes to
** channels or shows or whatnot, these all (will) get pre-computed during the
** data load, and the different methods of allocating minutes etc go into the
** different columns. This currently uses X_VIEWING_START_TIME / END_TIME but
** as we roll out the Barb minute-by-minute version, we'll probably remove the
** optional sample time from that procedure, since it's silly to try and get
** finer details from data that you've already normalised to a coarser scale.
** [ED: this still isn't implemented for time-shifted events, so, yeah, the
** time-shifted bit of this is still pants.]
**
** Fun update: we do have x_barb_min_start and x_barb_min_end columns, but
** they're not populated for older data, sadface. Guess we're messing around
** with events ourselves then.
**
** Features needed:
**  2. Add functionality to scale numbers up to sky base (Scaling V2 now!)
**  3. How are we going to treat scaling of time-shifted viewing?
**  4. Redirect to capped structures (Capping V2 in play too!)
** (Note these feature requests also match those of the MBM_by_channel procedure)
**
** Recently implemented:
**  1. Add columns to distinguish between live and time-shifted viewing. Update:
**      so it turns out that Sybase can't handle stored procedures that vary the
**      result column structure when putting the results into a table. So now by
**      default the MBM proc will by default calculate VOSDAL (same day), 7 day,
**      and 28 day playback divisions, which is mostly in line with the rest of
**      the industry rerporting.
**
******************************************************************************/

-- Eventually we'll have separate procedures for BARB minutes, sbs, etc, but just live for now:
if object_id('make_Uncapped_MBM_prog_graph') is not null drop procedure make_Uncapped_MBM_prog_graph;
create procedure make_Uncapped_MBM_prog_graph
    @programme_trans_sk     bigint,
    @measureslice           int = 60,
    @boundary_limit         int = 0
-- again doing silly things without colons to placate the Sybase stored procedure gods
-- we also can't commit at any point through this, since if the results get seleted into
-- a table (which is a pretty common mode of use) then it's a transactional event and no
-- commits are allowed in between. That's kind of annoying, thanks Sybase.
as
begin
        -- Viewing window is capped below at 2 seconds:
        if @measureslice < 2
        begin
                set @measureslice = 2
        end
        
        declare @graphing_time_start            smalldatetime   -- only precise as far as minutes, which is all we need
        declare @graphing_time_end              smalldatetime
        declare @programme_time_start           smalldatetime   -- Until BARB minutes are everywhere, we also need to know when the program starts & ends for event juggling doing the timeshifting stuff
        declare @programme_time_end             smalldatetime
        declare @programme_day                  date            -- because we want to scan multiple days, show might go past midnight
        declare @minute_sample_point            datetime        -- the start/end columns are assumed processed for us, we need only one sample point
        declare @viewing_minute                 datetime
        declare @enabled_box_count              bigint          -- To scale the viewhood against enabled boxes at the time the thing aired
        declare @dailytable_query_hurg          varchar(2000)   -- For dynamically pulling stuff from different daily tables
        
        -- The table that will eventually get pushed out as the report:
        create table #minute_by_minute_graph_table (
                Slice_start                     datetime    not null
                ,Source_day                     varchar(20) not null
                ,Viewed_per_day                 bigint      not null
                ,live_viewing                   bit         not null
                ,primary key (Slice_start, live_viewing, Source_day)
                --scaled_Viewed_Live_per_day      double      default null -- this is updated after the loop, so defaults to null
        )

        -- Pull the programme data out of the EPG lookup; we need start & end times, and also need to know which daily tables we want
        select
                -- Subtract off the seconds part to round down to the nearest minute; we don't care
                -- about ms as events are aligned exactly to minutes (programmes don't always start
                -- exactly on a minute - or even a second)
                @programme_day         = convert(date, tx_start_datetime_utc)
                ,@programme_time_start = tx_start_datetime_utc -- needed to stitch together our own playback stuff
                ,@programme_time_end   = tx_end_datetime_utc
                ,@graphing_time_start  = case
                                                when @boundary_limit >= 0 then -- Full scan or initial segment, we start when the show starts
                                                        dateadd(ss, -datepart(ss,tx_start_datetime_utc), tx_start_datetime_utc)
                                                when @boundary_limit < 0 then -- Scan of only last few minutes, we start just before the end of the show
                                                        dateadd(minute,
                                                                @boundary_limit, -- Boundary limit is already negative, so this gets the time before the end of the show
                                                                dateadd(ss, -datepart(ss,tx_end_datetime_utc), tx_end_datetime_utc))
                                          end
                ,@graphing_time_end    = case
                                                when @boundary_limit <= 0 then -- either full scan or last segment, we're still going up to the end of the show
                                                        dateadd(ss, -datepart(ss,tx_end_datetime_utc), tx_end_datetime_utc)
                                                when @boundary_limit > 0 then -- Scan of only first few minutes, stop a few minutes after the show starts
                                                        dateadd(minute,
                                                                @boundary_limit,
                                                                dateadd(ss, -datepart(ss,tx_start_datetime_utc), tx_start_datetime_utc))
                                          end
                 -- currently UTC and local are alligned, going forward we might need to change these references if the data all gets made local
        from sk_prod.VESPA_EPG_DIM
        where programme_trans_sk = @programme_trans_sk
        
        -- Loop through all the daily tables we might need
        while @programme_day <= convert(date, @graphing_time_end) -- Need equality here as this is the check of which daily tables to scan
        begin 
                -- Only proceed if the appropriate daily table exists; do we really need to check this?
                if object_id('sk_prod.VESPA_STB_PROG_EVENTS_' || dateformat(@programme_day,'yyyymmdd')) is not null
                        begin
                        
                        -- Return the inner loop counters to their initial states
                        set @minute_sample_point = dateadd(second, 1, @graphing_time_start)
                        -- the interval boundaries don't matter so much under the assumption that the timings are
                        -- normalised (which they're not in this test but will be when we're live). It's now 1 since
                        -- we're allowing the window size to float, but capped from below at 2.
                        set @viewing_minute = @graphing_time_start

                        
                        -- Wow, this hack. So we're only building most of the string once, and inserting into
                        -- it tokens which we'll replace with the perameters as we loop around. Kind of like
                        -- a perameterised query, except without any of the benefits (like robustness vs SQL
                        -- injections, established index usage decisions, and everything else that perameterised
                        -- queries exist to resolve). Oh well.
                        set @dailytable_query_hurg = 'insert into #minute_by_minute_graph_table (
                                Slice_start,
                                Source_day,
                                Viewed_per_day,
                                live_viewing
                        )
                        select ''#View#Minute#'',
                                ''' || dateformat(@programme_day, 'yyyy-mm-dd') || ''',
                                count(1),
                                1
                        from ' || 'sk_prod.VESPA_STB_PROG_EVENTS_' || dateformat(@programme_day,'yyyymmdd') || '
                        where programme_trans_sk = ' || @programme_trans_sk || '
                                and X_VIEWING_START_TIME < ''#View#Sample#Point#''
                                and X_VIEWING_END_TIME > ''#View#Sample#Point#''
                                and play_back_speed is null
                                and x_programme_viewed_duration>0
                                and Panel_id in (4, 5)
                                -- and Event_Type = ''evChangeView''
                                and x_type_of_viewing_event <> ''Non viewing event'''
                        -- Okay, so:
                        --      #View#Minute# gets replaced by the the start point of the relevant interval
                        --      #View#Sample#Point# gets replaced by sampling point of said interval
                        -- The other things don't change through the loop, so they get fixed now.
                        
                        -- If we get the modifications we'd like, then all of the filters can be
                        -- dropped except for the programme_trans_sk and the viewing time checks.
                        
                        -- Iterate and build up the MBM graph:
                        WHILE @minute_sample_point < @graphing_time_end
                        begin
                                -- Replace the placeholders with the minutes for each loop
                                execute(replace(
                                        replace(@dailytable_query_hurg,'#View#Minute#',dateformat(@viewing_minute, 'yyyy-mm-dd hh:mm:ss')),
                                        '#View#Sample#Point#',
                                        dateformat(@minute_sample_point, 'yyyy-mm-dd hh:mm:ss'))
                                        )

                                -- Click the counters on by the size of the sample window;
                                set @viewing_minute             = dateadd(second, @measureslice, @viewing_minute)
                                set @minute_sample_point        = dateadd(second, @measureslice, @minute_sample_point)

                                --commit
                        
                        END
                END
                
                set @programme_day = dateadd(day, 1, @programme_day)
        END

        /**** OK, so now all the live stuff is done, we go hunting for the playback ****/
        
        -- OK, now the timeshifted version of the minute by minute query:
        set @dailytable_query_hurg = 'insert into #minute_by_minute_graph_table (
                        Slice_start,
                        Source_day,
                        Viewed_per_day,
                        live_viewing
                )
                select ''#View#Minute#'',
                        ''#Source#Day#'',
                        count(1),
                        0
                from ' || 'sk_prod.VESPA_STB_PROG_EVENTS_#Daily#Table#Date#
                where programme_trans_sk = ' || @programme_trans_sk || '
                        and recorded_time_utc < ''#View#Sample#Point#''
                        and dateadd(second, x_event_duration, recorded_time_utc) > ''#View#Sample#Point#''
                        and play_back_speed = 2
                        and x_programme_viewed_duration>0
                        and Panel_id in (4, 5)
                        -- and Event_Type = ''evChangeView''
                        and x_type_of_viewing_event <> ''Non viewing event'''

        set @programme_day = convert(date, @programme_time_start)

        while @programme_day <= convert(date, dateadd(day, 3, @programme_time_end))
        begin

                if object_id('sk_prod.VESPA_STB_PROG_EVENTS_' || dateformat(@programme_day,'yyyymmdd')) is not null
                begin

                        set @minute_sample_point = dateadd(second, 1, @graphing_time_start)
                        set @viewing_minute = @graphing_time_start
                        
                        WHILE @minute_sample_point < @graphing_time_end
                        begin
                                -- Replace the placeholders with the minutes for each loop
                                execute(replace(replace(replace(replace(
                                                    @dailytable_query_hurg,
                                                    '#View#Minute#',
                                                    dateformat(@viewing_minute, 'yyyy-mm-dd hh:mm:ss')
                                                ),
                                                '#View#Sample#Point#',
                                                dateformat(@minute_sample_point, 'yyyy-mm-dd hh:mm:ss')
                                            ),
                                            '#Source#Day#',
                                            dateformat(@programme_day, 'yyyy-mm-dd')
                                        ),
                                        '#Daily#Table#Date#',
                                        dateformat(@programme_day,'yyyymmdd')
                                        )
                                    )
                                        
                                -- Click the counters on by the size of the sample window;
                                set @viewing_minute             = dateadd(second, @measureslice, @viewing_minute)
                                set @minute_sample_point        = dateadd(second, @measureslice, @minute_sample_point)

                        end
                END                        

                set @programme_day = dateadd(day, 1, @programme_day)
        end
        
        -- Normalise the viewing data by number of enabled boxes
        --select @enabled_box_count = count(1)
        --from sk_prod.VESPA_SUBSCRIBER_STATUS
        --where result='Enabled'
        --and cast(request_dt as date) <= cast(@graphing_time_start as date)
        -- As in, enabled when the show was first cast

        -- Not scaling for these guys. Structures are available, but we don't care. Besides,
        -- don't have the capping structures yet either...
        --update #minute_by_minute_graph_table set 
        --scaled_Viewed_Live_per_day = Viewed_per_day / convert(double, @enabled_box_count) -- if they both get left as INTs then integer division makes it zero

        -- By default we're reporting on all of the playback numbers
        select
                Slice_start
                ,sum(case when live_viewing = 1                                                                     then Viewed_per_day else 0 end) as Viewed_Live
                ,sum(case when live_viewing = 0 and datediff(day, @programme_time_end, Source_day) <= 0             then Viewed_per_day else 0 end) as Viewed_VOSDAL
                ,sum(case when live_viewing = 0 and datediff(day, @programme_time_end, Source_day) between 1 and 7  then Viewed_per_day else 0 end) as Viewed_1_to_7_days
                ,sum(case when live_viewing = 0 and datediff(day, @programme_time_end, Source_day) between 8 and 28 then Viewed_per_day else 0 end) as Viewed_8_to_28_days
                from #minute_by_minute_graph_table
        group by Slice_start
        order by Slice_start

end;

commit;
grant execute on make_Uncapped_MBM_prog_graph to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
commit;