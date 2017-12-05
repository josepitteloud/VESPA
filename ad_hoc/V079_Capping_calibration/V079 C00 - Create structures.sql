/******************************************************************************
** Capping calibration exercise - ONE-OFF script
**
******************************************************************************/



/****************** PART A01: RAW LOGS TABLE ******************/

if object_id('CP2_viewing_records') is not null drop table CP2_viewing_records;
create table CP2_viewing_records (
    -- This is the collection of stuff we're pulling out of the daily table:
    cb_row_ID                           bigint          not null primary key
    ,Account_Number                     varchar(20)     not null
    ,Subscriber_Id                      integer
    ,X_Type_Of_Viewing_Event            varchar(40)     not null
    ,Adjusted_Event_Start_Time          datetime
    ,X_Adjusted_Event_End_Time          datetime
    ,X_Viewing_Start_Time               datetime
    ,X_Viewing_End_Time                 datetime
    ,Tx_Start_Datetime_UTC              datetime
    ,X_Event_Duration                   decimal(10,0)
    ,X_Programme_Viewed_Duration        decimal(10,0)
    ,Programme_Trans_Sk                 bigint          not null
    ,daily_table_date                   date            not null
    ,live                               bit
    ,genre                              varchar(25)
    ,sub_genre                          varchar(25)
    ,epg_channel                        varchar(30)
    ,channel_name                       varchar(30)     -- This guy gets populated as uppercase and trimmed

    -- These guys aren't used in the capping, but they are needed for BARB minutes:
    ,program_air_date                   date
    ,program_air_datetime               datetime

    -- Then the following things are ranks and stuff which we derive later;

    -- Um... there aren't that many? the whole dance all the way to sov4 was just
    -- to delete some duplicated items and reset some broken viewing start / end
    -- times? Do we even need to rank there or can we figure out some more direct
    -- trick? We'll look into it... there are a few items which traditionally got
    -- added with ALTER TABLE statements later...
    ,event_start_day                    integer         default null
    ,event_start_hour                   integer         default null

    -- Other things that previously got ALTER'd on later:
    ,box_subscription                   varchar(1)      default 'U'     -- Primary or Secondary flag

    -- Putting capping onto the viewing records table. Do we really need this?
    -- Can we instead cap the events and then push those caps back based on the
    -- existing subscriber_ID & adjusted event start time trick? Maybe.
    ,initial_genre                      varchar(30)
    ,initial_channel_name               varchar(30)     -- Dunno that we need this, it should always match channel_name, channel change will always start a new event?
    ,pack                               varchar(100)    -- Yowch! Can we avoid putting another VARCHAR(100) on every viewing record?
    ,pack_grp                           varchar(30)
    -- So... if we're not putting max_dur_mins on the viewing data, do we really need the pack etc?

    -- Other composite keys:
    ,bucket_id                          integer         -- Composite lookup for: event_start_hour, event_start_day, initial_channel_name, Live
);

-- Indices we need:
create index view_uniquifier on CP2_viewing_records (subscriber_id, adjusted_event_start_time, X_Viewing_Start_Time);
-- ^^ Should be unique but there are always dupes that go in which we need to cull
create index for_lookups     on CP2_viewing_records (account_number);

create index for_buckets     on CP2_viewing_records (bucket_id);

commit;
go

/****************** PART A02: EVENTS AND THEIR START TIMES ******************/

-- So this table we have one record per viewing event, and these are the things
-- we need to cap. This table used to be called "one_week" in the previous build,
-- but that was an utterly unhelpful table name. There might also be other tables
-- which kind of filled this role, so they'll be tacked into here as well.

if object_id('CP2_event_listing') is not null drop table CP2_event_listing;
create table CP2_event_listing (
    Subscriber_Id                       integer         not null
    ,account_number                     varchar(20)     not null
    ,fake_cb_row_id                     bigint          not null    -- we just need it to break some ties; it'll still be unique
    ,X_Type_Of_Viewing_Event            varchar(40)     not null
    ,Adjusted_Event_Start_Time          datetime        not null
    ,X_Adjusted_Event_End_Time          datetime
    ,event_start_hour                   tinyint
    ,event_start_day                    tinyint
    ,X_Event_Duration                   decimal(10,0)
    ,event_dur_mins                     integer
    ,live                               bit
    ,initial_genre                      varchar(25)
    ,initial_sub_genre                  varchar(25)
    ,initial_channel_name               varchar(30)     -- This guy gets populated as uppercase and trimmed
    ,program_air_date                   date
    ,program_air_datetime               datetime
    ,num_views                          int
    ,num_genre                          int
    ,num_sub_genre                      int
    ,viewed_duration                    int

    -- These guys are a channel categorisation lookup
    ,pack                               varchar(100)
    ,pack_grp                           varchar(30)
    -- We also use P/S box flags:
    ,box_subscription                   varchar(1)

    -- Columns used in applying caps:
    ,bucket_id                          integer         -- Composite lookup for: event_start_hour, event_start_day, initial_channel_name, Live
    ,max_dur_mins                       int             default null
    ,capped_event                       bit             default 0

    -- Yeah, structure is always good:
    ,primary key (Subscriber_Id, Adjusted_Event_Start_Time) -- So we... *shouldn't* have any more than one event starting at the same time per box... might have to manage some deduplication...
);
-- We'll also need indices on this guy...

create index    for_joins           on CP2_event_listing (account_number);
-- What other indices for computational stuffs...
create index    start_time_index    on CP2_event_listing (Adjusted_Event_Start_Time);
-- This one is needed in order to pick up the pack information:
create index    init_channel_index  on CP2_event_listing (initial_channel_name);
-- For joining on the flags we use to do the capping:
create index for_the_joining_group  on CP2_event_listing
    (event_start_hour, event_start_day, initial_genre, box_subscription, pack_grp, Live);
-- This one has all the same info as the last, but acts a little more effectively
create index by_bucket_index        on CP2_event_listing
    (bucket_id, pack_grp, box_subscription);

commit;
go

/****************** PART A03: FIRST PROGRAMME IN EVENT ******************/

-- Part of the capping process needs to look at what the first programme
-- was when the event started. This table holds all the relevant details of
-- the virst bit of viewing in each event.
IF object_id('CP2_First_Programmes_In_Event') IS NOT NULL DROP TABLE CP2_First_Programmes_In_Event;

create table CP2_First_Programmes_In_Event (
    subscriber_id                       integer         not null
    ,adjusted_event_start_time          datetime        not null
    -- For the genre assignement bits:
    ,genre                              varchar(25)
    ,sub_genre                          varchar(25)
    ,channel_name                       varchar(30)
    -- Things needed to assign caps to end of first program viewed (sectino C02.e)
    ,X_Adjusted_Event_End_Time          datetime
    ,x_viewing_end_time                 datetime
    ,sequencer                          integer         -- only needed for deduplication
    ,primary key (subscriber_id, adjusted_event_start_time, sequencer)
);
-- Ultimately (subscriber_id, adjusted_event_start_time) form a unique
-- index, but we need the sequencer too so we can identify & cull all
-- the items that aren't the first in the event.

commit;
go

-- Doesn't need any more indices, all the joins use the same constraint as the PK

/****************** PART A04: CHANNEL LOOKUP TABLE ******************/

-- Here's where we would create the table "CP2_channel_lookup" except that we have
-- no idea how that table is built, but refer to "CP2 - import channel_lookup.sql"
-- for the import thing.


/****************** PART B01: INTERNAL VIEWING MINUTES ******************/

-- Yes, the BARB minute calcs come after the capping application in the build, but
-- section B for BARB and section C for Capping tables was too good to pass up.

-- The first table looks at the vieiwng data and catures all complete minutes
-- watched. Table 2 captures the begining and end of those viewing records to
-- determine if the part minutes is in fact a BARB minute of viewing (at least 30
-- seconds long and the majority viewed in that minutes). Once we know the complete
-- minutes and the BARB minutes these can be merged into a single table to determine
-- how many minutes of each channel, per day were watched by each subscriber.

if object_id('CP2_4BARB_internal_viewing') is not null drop table CP2_4BARB_internal_viewing;
create table CP2_4BARB_internal_viewing (
    cb_row_id                           bigint          --primary key - it's not a PK any more because we get dupes adding the edge minutes back in
    ,subscriber_id                      decimal(10)
    ,epg_channel                        varchar(20)
    ,minute_started                     datetime        not null
    ,minute_stopped                     datetime        not null
    ,program_air_datetime               datetime
    ,live                               smallint
    ,vosdal                             smallint
    ,playback                           smallint

);
-- This index will let you quickly find out who's watching each minute.
create index for_MBM  on CP2_4BARB_internal_viewing (epg_channel, minute_started, minute_stopped);
create index not_a_PK on CP2_4BARB_internal_viewing (cb_row_id);

/****************** PART B02: ALL THE MESSY BARB ENDPOINTS ******************/

-- This table has all the part-minute bits from the beginning and the end of
-- each viewing event, used for the BARB capping
if object_id('CP2_4BARB_view_endpoints') is not null drop table CP2_4BARB_view_endpoints;
create table CP2_4BARB_view_endpoints (
    cb_row_id                           bigint          not null    -- can't PK it because we'll have ones for the start and the end..
    ,subscriber_id                      decimal(10)
    ,epg_channel                        varchar(20)
    ,minute_start                       datetime        not null
    ,viewing_starts                     datetime        not null
    ,viewing_ends                       datetime        not null
    ,program_air_datetime               datetime
    ,live                               smallint
    ,vosdal                             smallint
    ,playback                           smallint
);

-- This index is what we want to help group all the viewing into allocated minutes
create index for_groupings1 on CP2_4BARB_view_endpoints (subscriber_id, epg_channel, minute_start);

commit;
go

/****************** PART B03:  ******************/

/****************** PART B04:  ******************/


/****************** PART C01: TABLES CONTAINING THE CAPS BY BUCKET ******************/

-- Week caps is important enough to want to get split out into his own permanent-like
-- table... This table holds the caps we calculate for each "bucket"
if object_id('CP2_calculated_viewing_caps') is not null drop table CP2_calculated_viewing_caps;
create table CP2_calculated_viewing_caps (
    -- If we ever want to try to roll out some kind of bucket key:
    sub_bucket_id                       integer identity primary key
    ,bucket_id                          integer         -- We don't use pack_grp or box_subscription in the buckets, this gets picked up based just on event_start_day, event_start_hour, initial_genre and Live

    -- The composite PK columns: these define a "bucket"
    ,Live                               bit
    ,event_start_day                    tinyint
    ,event_start_hour                   tinyint
    ,box_subscription                   varchar(1)      -- 'P' or 'S' or 'U'
    ,pack_grp                           varchar(30)
    ,initial_genre                      varchar(25)

    -- Important derived columns
    ,max_dur_mins                       integer         -- the length of the cap to be applied, in minutes

);
-- That table will hold all the caps for one day, since we're looping to build cap
-- viewing data one day at a a time.

commit;
-- Indices: still not convinced we need all of these, that they all do anything useful...
create hng index idx1 on CP2_calculated_viewing_caps(event_start_day);
create hng index idx2 on CP2_calculated_viewing_caps(event_start_hour);
create hng index idx4 on CP2_calculated_viewing_caps(box_subscription);
create hng index idx5 on CP2_calculated_viewing_caps(pack_grp);
create hng index idx6 on CP2_calculated_viewing_caps(initial_genre);
commit;
-- This one, however, supports the application of caps to viewing data:
create unique index forcing_uniqueness on CP2_calculated_viewing_caps
    (event_start_hour, event_start_day, initial_genre, box_subscription, pack_grp, Live);
-- Unique forces the bucketing we're expecting to observe. But this one:
create index for_the_joining_group on CP2_calculated_viewing_caps
    (bucket_id, box_subscription, pack_grp);
-- That's the one that actually gets used in joins, since the bucket_ID does
-- a lot of simplification for the DB.

-- I dunno why anyone else needs this, but they don't need more than SELECT

commit;
go

/****************** PART C02: CAPPING BUCKETS LOOKUP ******************/

-- This guy is a composite key that summarises event_start_hour, event_start_day,
-- initial_genre and live into one integer that's easy to use (/index/join). Helps
-- reduce the number of columns needed in some summaries and joins by 3, so that's
-- a good thing.
if object_id('CP2_capping_buckets') is not null drop table CP2_capping_buckets;
create table CP2_capping_buckets (
    bucket_id                           integer identity primary key
    ,event_start_hour                   tinyint not null
    ,event_start_day                    tinyint not null
    ,initial_genre                      varchar(30) not null
    ,live                               bit
);

-- So this table still isn't as wildely used as it could be in the build, it's
-- implemented in a few places to facilitate a few things, but the big messy
-- middle bit of the code which makes the caps according to the various rules
-- doesn't really use it. But stuff there is split up enough to not really
-- need it. Maybe pushing it back onto the viewing data will need it, but we
-- are okay so far.

create unique index for_uniqueness on CP2_capping_buckets
    (event_start_hour, event_start_day, initial_genre, live);

commit;
go

/****************** PART C03: HOLDING PEN PRIOR TO DYNAMIC TABLE ******************/

-- This table is where we prepare all the cap details that we want, just before we
-- chuck it all into the dynamically named daily caps table.
if object_id('CP2_capped_data_holding_pen') is not null drop table CP2_capped_data_holding_pen;
create table CP2_capped_data_holding_pen (
    cb_row_id                   bigint              primary key     -- Links to the viewing data daily table of the same day
    ,subscriber_id              bigint              not null
    ,account_number             varchar(20)         not null
    ,scaling_segment_id         bigint                              -- To help with the MBM proc builds....                     -- NYIP!
    ,scaling_weighting          float                               --                                                          -- NYIP!
    ,programme_trans_sk         bigint                              -- To make the minute-by-minute stuff real easy
    ,viewing_starts             datetime                            -- Capped viewing start time
    ,viewing_stops              datetime
    ,viewing_duration           bigint                              -- Capped viewing in seconds
    ,BARB_minute_start          datetime                            -- Viewing with Capping treatment + BARB minute allocation  -- NYIP!
    ,BARB_minute_end            datetime                            -- BARB minutes are pulled back to broadcast time           -- NYIP!
    ,timeshifting               varchar(10)                         -- 'LIVE' or 'VOSDAL' (same day as live) or 'PLAYBACK7' (playback within 7 days) or 'PLAYBACK28' (otherwise)
    ,capped_flag                tinyint                             -- 0-3 depending on capping treatment, or 11 if there are lingering events that are not yet treated
    ,capped_event_end_time      datetime
    -- So those are the columns that go into the dynamically named table,
    -- but there are a few others used to process those out:
    ,adjusted_event_start_time  datetime
    ,X_Adjusted_Event_End_Time  datetime
    ,x_viewing_start_time       datetime
    ,x_viewing_end_time         datetime
    -- Other things we only need to maintain our control totals:
    ,program_air_date           date
    ,live                       tinyint
    ,genre                      varchar(50)
);

-- Indices? what else are we doing here?

/****************** PART D01: STRUCTURAL PLACEHOLDER FOR DALIES ******************/

-- So these guys are built dynamically for each day, but the sctucture should be
-- identical to this for each day:
/* (Commented out because we don't actually build the things like this)
create table vespa_analysts.Vespa_daily_augs_YYYYMMDD (
    cb_row_id                   bigint              primary key     -- Links to the viewing data daily table of the same day
    ,subscriber_id              bigint              not null
    ,account_number             varchar(20)         not null
    ,programme_trans_sk         bigint                              -- to help out with the minute-by-minute stuff
    ,scaling_segment_id         bigint                              -- To help with the MBM proc builds....                         -- NYIP!
    ,scaling_weighting          float                               -- Also assisting with the MBM proc builds                      -- NYIP!
    ,viewing_starts             datetime                            -- Capped viewing start time
    ,viewing_stops              datetime
    ,viewing_duration           bigint                              -- Capped viewing in seconds
    ,BARB_minute_start          datetime                            -- Viewing with Capping treatment + BARB minute allocation      -- NYIP!
    ,BARB_minute_end            datetime                                                                                            -- NYIP!
    ,timeshifting               varchar(10)                         -- 'LIVE' or 'VOSDAL' (same day as live) or 'PLAYBACK7' (playback within 7 days) or 'PLAYBACK28' (otherwise)
    ,capped_flag                tinyint                             -- 0-2 depending on capping treatment: 0 -> event not capped, 1 -> event capped but doesn't effect viewing, 2 -> event capped & shortens viewing, 3 -> event capped & excludes viewing (actually 3 doesn't turn up in the table, but that's what it means during processing)
    ,capped_event_end_time      datetime                            -- Only populated for capped events
);

create index for_MBM            on vespa_analysts.Vespa_daily_augs_YYYYMMDD (scaling_segment_id, viewing_starts, viewing_stops)
create index for_barb_MBM       on vespa_analysts.Vespa_daily_augs_YYYYMMDD (scaling_segment_id, BARB_minute_start, BARB_minute_end)
create index subscriber_id      on vespa_analysts.Vespa_daily_augs_YYYYMMDD (subscriber_id);
create index account_number     on vespa_analysts.Vespa_daily_augs_YYYYMMDD (account_number);


commit;

*/
-- Initial studies show these tables to be about 800MB per day - on the pre-rampup panel of 210k
-- boxes returning data (190k accounts). That's quite a bit, means we're guessing at... 150GB of
-- capping cache stuff to go back to November 2011. Awesome. That's not actually a whole lot in
-- the scheme of things (though yeah, it's a lot more than the scaling builds.)

/****************** PART Q01: TABLES TRACKING VIEWING TOTALS ******************/

-- We're storing the totals of viewing for each major stage of processing, and also for
-- each capping strand
IF object_id('CP2_QA_viewing_control_totals') IS NOT NULL DROP TABLE CP2_QA_viewing_control_totals;
create table CP2_QA_viewing_control_totals (
    build_date                  date                not null -- The date that the caps apply to
    ,data_state                 varchar(20)         not null
    ,program_air_date           date                not null
    ,live                       bit
    ,genre                      varchar(25)
    ,viewing_records            int
    ,total_viewing_in_days      decimal(8,2)        not null
    ,primary key (build_date, data_state, program_air_date, live, genre)
);
/* What we expect for the data states in the above table (for each build_date):
    *. '1.) Collect' should match '2.) Pre-Cap'
    *. '4a.) Uncapped' + '4c.) Truncated' should add up to '3.) Capped',
    *. '4a.) Uncapped' + '4b.) Excluded' + '4c.) Truncated' + '4d.) T-Margin' should add up to '1.) Collect'
They should match pretty much exactly, since we've rounded everything to 2dp in hours.
*/

commit;

-- We're also tracking how many viewing events fal into each category of the capping
IF object_id('CP2_QA_viewing_control_distribs') IS NOT NULL DROP TABLE CP2_QA_viewing_control_distribs;
create table CP2_QA_viewing_control_distribs (
    build_date                  date                not null -- The date that the caps apply to
    ,data_state                 varchar(20)         not null -- '1.) Uncapped' or '2.) Capped'
    ,duration_interval          int                 not null -- batched into 10s chunks, so 0 means viewing durations between 0s and 10s
    ,viewing_events             int                          -- Er... but these are not events, but viewing bits... oh well
    ,primary key (build_date, data_state, duration_interval)
);

-- Now also doign the same thing not for viewing items, but for event durations
-- and with a resolution of 1 minute because these things are much longer.
IF object_id('CP2_QA_event_control_distribs') IS NOT NULL DROP TABLE CP2_QA_event_control_distribs;
create table CP2_QA_event_control_distribs (
    build_date                  date                not null -- The date that the caps apply to
    ,data_state                 varchar(20)         not null -- '1.) Uncapped' or '2.) Capped'
    ,duration_interval          int                 not null -- batched into 1m chunks, so 0 means viewing durations between 0s and 1 minute
    ,viewing_events             int
    ,primary key (build_date, data_state, duration_interval)
);

commit;

go

/****************** PART Q02: BARB MINUTE BY MINUTE CONTROL TOTALS ******************/

-- Tables which track the daily viewing totals before and after the BARB minute batching

/****************** PART Q03: HISTORICAL TRACKING OF DAILY TOTAL VIEWING ******************/

-- Tables which track the total viewing for the various stages of processing through both
-- capping and BARB minute allocation. These averages are just over people who watch *some*
-- TV at all, so will be higher than the average TV watching since boxes supplying only
-- logs don't get considered here. Also notice that this is daily viewing *on panel* and
-- the Sky Base average isn't calculated here (because it depends on Scaling and we're not
-- sure that we have the appropriate eights prepared when the capping gets done).
IF object_id('CP2_QA_daily_average_viewing') IS NOT NULL DROP TABLE CP2_QA_daily_average_viewing;
create table CP2_QA_daily_average_viewing (
    build_date                  date                not null primary key
    ,subscriber_count           int                 not null        -- Number of boxes noticed in the build
    ,average_uncleansed_viewing int                 default null    -- All the viewing counts are in minutes per box
    ,average_uncapped_viewing   int                 default null
    ,average_capped_viewing     int                 default null
    ,average_BARB_viewing       int                 default null    -- IE the average viewing per box after BARB minute-by-minute processing has been applied (NYIP)
);


/****************** PART Q04: HISTORICAL TRACKING OF MAGNITUDE OF CALCULATED CAPS ******************/

-- We want to know how big the various caps are that we're calculating, just
-- to see how much viewing we think is okay for each case
IF object_id('CP2_QA_viewing_control_cap_distrib') IS NOT NULL DROP TABLE CP2_QA_viewing_control_cap_distrib;
create table CP2_QA_viewing_control_cap_distrib (
    build_date                  date                not null -- The date that the caps apply to
    ,max_dur_mins               int                 not null
    ,cap_instances              int                 not null
    ,primary key (build_date, max_dur_mins)
);

commit;
go
