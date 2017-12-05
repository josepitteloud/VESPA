/******************************************************************************
**
**      Project Vespa: Scalability investigations: household cube demo
**
** Building on the scalability hacks stuff, we're now looking at building some
** cubes to do some simple studies of what real life instances of household
** aggregation will look like.
**
** Filters we can apply:
**  * Limit to three channels
**  * Limit to three days of data (or one?)
**  * Limit to households which have multiple reporting boxes for that period
**
** This will hugely reduce the data set that we have to pull out, which will
** make everything easier.
**
** Unfulfilled dependency: capping stuff hugely influences this, and isn't at
** all well established yet. We're implementing a two hour capping rule, but
** this is something liable to change in the future.
**
** Chunks of this guy pertaining to chain construction originated in the script
** "viewing_overlappage.sql" which is where we first build the chaining process.
** There might be a little more documentation there on how / why it works as it
** does, but it doesn't use the same flags-on-source-table that we use here.
**
** OK, comment: because chains are currently defined in terms of programmes
** rather than channels, we end up with a lot of chains spanning entire shows.
** If we instead did it by channel (normalised channel even? SD & HD & +1?) we
** would get a bit better visibility over cross-programme persistence...
**
** Funny thing is, in all this time we've had Vespa online, we've not once built
** anything which joins all the adjacent events together to give a view of how
** long a session lasts. That should be important, no? Well, it's not being done.
**
** Sections: now with rebuild markers.
**
**  1. Identify programme_trans_sk keys & households to sample
**  2. Extract data from daily tables
**  3. Do capping
**  4. Build viewing chains
**  5. QA of chain construction
**  6. Permissions on cube
**  7. Control totals for reporting
**  8. Example households
**  9. Bulk metric construction
** 10. Basic reporting profiles
** 11. Viewing instances per device
** 12. Average instance duration
** 13. Proportion of show viewed
** 14. Largest continuous view duration
**
** Also now with household box counts on the cube. Note that the DB build has
** fluxed a bit and this exact code moght not build the exact same control
** totals, and indeed, later builds against things that should be static (eg
** the daily tables!) are returning different control totals. This is mostly
** due to long dev time and changing requirements, but hey, it's still
** partially stable. Tracked at least.
**
** Agile Actions: not for a radical overhaul. Maybe into a different script?
**
**  1. Chains are now out completely. We're reporting into a 5x5 lower triangular
**      box of total-boxes-in-household vs Box Number.
**  2. Put all the various marks on, but leave off programme key! Include: live
**      or playback, PVR vs non-PVR (wtf is that? Bhavesh should forward me
**      profiling code, or otherwise get Julie to forward me code), lifestage,
**      time of day, genre.... some of these need to go into the EPG too, so
**      maybe we suck it up and take those joins to the face.
**  3. But yeah, all the aggregateds from here aren't summarising a single view
**      of a household at all, but instead cutting up multiroom households into
**      what happens on the different boxes. Oh well.
**
******************************************************************************/

-- Okay, so it looks like we might even want to be building the stuff for
-- single box households too. Rolfwombat. The change in scope meant that all
-- single box households started off out of scope, but ended up inside it. And
-- since we're saying "how much are they watching?", we probably don't want to
-- be limiting to sample channels either. Awesomes. Oh well. Update: No, just
-- take out all the stuff on Box Zero, just number all the boxes as MR stuff
-- (check that it's done on the full box count, not just the channel filter
-- listing.)

/************ 1.a Choose channel & days & isolate programe_trans_sk ************/
-- Dave, SS1, BBC1. Collapsing all the SD and HD into one.

drop table hh_cube_programme_lookup;

select
    programme_trans_sk
    ,Tx_Start_Datetime_UTC
    ,Tx_End_Datetime_UTC
    ,Channel_Name
    ,Epg_Title
    ,Genre_Description
    ,Sub_Genre_Description
    ,case
        when lower(CHANNEL_name) in ('dave', 'dave hd') then 'Dave' -- missing HS from lookup
        when lower(CHANNEL_name) in ('sky sports 1', 'sky sports hd1') then 'Sky Sports 1'
        else 'BBC1'
    end as Normed_channel
into stafforr.hh_cube_programme_lookup
from sk_prod.vespa_epg_dim
where tx_date in ('20110701','20110702','20110703')
and (lower(CHANNEL_name) in ('dave', 'dave hd', 'sky sports 1', 'sky sports hd1')
or lower(CHANNEL_name) like 'bbc 1%'
or lower(CHANNEL_name) like 'bbc one%');
-- 1724 programmes. Except that Dave HD didn't exist until 10th October 2011
-- so it doesn't show up in this data at all.

create unique index fake_pk on stafforr.hh_cube_programme_lookup (programme_trans_sk);

/************ 1.b Identify households with multiple devices reporting views ************/
-- Just looking for accounts with multiple reporting over our period, and now also limiting
-- it to vespa panel viewing events. Oh yeah, which means the control totals for the rest of
-- this section are no longer alligned.

select distinct account_number, subscriber_id
into #hh_cube_account_listing
from sk_prod.VESPA_STB_PROG_EVENTS_20110701
where (play_back_speed is null or play_back_speed = 2) -- NULL means live, 2 is timeshifted
and x_programme_viewed_duration > 0
and Panel_id = 5
and x_type_of_viewing_event <> 'Non viewing event';

insert into #hh_cube_account_listing
select distinct account_number, subscriber_id
from sk_prod.VESPA_STB_PROG_EVENTS_20110702
where (play_back_speed is null or play_back_speed = 2) -- NULL means live, 2 is timeshifted
and x_programme_viewed_duration > 0
and Panel_id = 5
and x_type_of_viewing_event <> 'Non viewing event';

insert into #hh_cube_account_listing
select distinct account_number, subscriber_id
from sk_prod.VESPA_STB_PROG_EVENTS_20110703
where (play_back_speed is null or play_back_speed = 2) -- NULL means live, 2 is timeshifted
and x_programme_viewed_duration > 0
and Panel_id = 5
and x_type_of_viewing_event <> 'Non viewing event';

-- For other control totals: how many boxes reporting?
select count(distinct account_number), count(distinct subscriber_id) from #hh_cube_account_listing;
-- 175768  219195

-- okay, and so how many are multiroom?
select count(1) households, sum(boxes) as reporting_boxes
from (
        select account_number, count(distinct subscriber_id) as boxes
        from #hh_cube_account_listing
        group by account_number
        having boxes > 1
) as t;
-- 39105   82532

-- So we don't duplicate viewing records in daily table extraction:
drop table stafforr.hh_cube_account_listing_dd;

select account_number
into stafforr.hh_cube_account_listing_dd
from #hh_cube_account_listing
group by account_number
having count(distinct subscriber_id) > 1;

create unique index fake_pk on stafforr.hh_cube_account_listing_dd (account_number);

select count(1) from stafforr.hh_cube_account_listing_dd;
-- 39105

/************ 1.c Get flags for box numbering ************/
-- For MR households: label the primary box 1, subsequent secondary boxes
-- 2 onwards. For single box households: label the box zero. This is all
-- coming of the log snapshot table where the P / S flag is, so there's no
-- guarantee that these boxes report in the period we're looking at. Still,
-- it's better than the enabled-but-never-reporting thing we'd get from
-- subscriber status.

select distinct account_number
into #reporting_households
from #hh_cube_account_listing;

-- Don't need this guy any more.
--drop table #hh_cube_account_listing;
-- hahano, this guy gets used a couple of placees yet.

commit;
create unique index fake_pk on #reporting_households (account_number);
commit;

Select
    sls.account_number, sls.subscriber_id, sls.service_instance_type,
    rank() over (partition by sls.account_number order by sls.service_instance_type, sls.subscriber_id) as box_no
into #box_numbering
from sk_prod.vespa_stb_log_snapshot as sls
inner join #reporting_households as t
on sls.account_number = t.account_number;

commit;
create unique index fake_pk on #box_numbering (account_number, subscriber_id);
commit;

-- Okay, now identify all the single households and push those numbers onto
-- this lookup
/*select account_number
into #single_box_households
from #box_numbering
group by account_number
having count(1) = 1;

commit;
create unique index fake_pk on #single_box_households (account_number);

update #box_numbering
set box_no = 0
from #box_numbering
inner join #single_box_households as sbh
on #box_numbering.account_number = sbh.account_number;
-- OK so all the multirooms with only one reporting box in our data period
-- are also being marked as single box households, but w/e
*/
-- single box households no longer being split out, they were anoying to
-- explain about multiroom households having only one box on our sample
-- channels....

select hits, boxmax, boxmin, count(1) as households from (
    select account_number, count(1) as hits, max(box_no) as boxmax, min(box_no) as boxmin
    from #box_numbering
    group by account_number
) as t
group by hits, boxmax, boxmin
order by hits, boxmax, boxmin;
/*
1	0	0	100883
2	2	1	23596
3	3	1	2003
4	4	1	199
5	5	1	14
*/

-- And now push those lookups onto our data set

-- Wait, no, that happens in a later section, we don't have the daily table yet.

/************ 2. Assemble data set from daily tables ************/

drop table stafforr.hh_cube_viewing;
-- Explicit creation, slighty long way around, but hey, slightly more reliable,
-- and mostly copied from the wiki anyway.
create table stafforr.hh_cube_viewing (
    cb_row_ID                       bigint      not null primary key
    ,Account_Number                 varchar(20) not null
    ,Subscriber_Id                  decimal(8,0) not null
    ,Lifestage                      varchar(30)
    ,Affluence                      varchar(10)
    ,hh_box_count                   tinyint
    ,Adjusted_Event_Start_Time      datetime
    ,X_Adjusted_Event_End_Time      datetime
    ,X_Viewing_Start_Time           datetime
    ,X_Viewing_End_Time             datetime
    ,Tx_Start_Datetime_UTC          datetime
    ,Tx_End_Datetime_UTC            datetime
    ,Recorded_Time_UTC              datetime
    ,Play_Back_Speed                decimal(4,0)
    ,X_Event_Duration               decimal(10,0)
    ,X_Programme_Duration           decimal(10,0)
    ,X_Programme_Viewed_Duration    decimal(10,0)
    ,X_Programme_Percentage_Viewed  decimal(3,0)
    ,X_Viewing_Time_Of_Day          varchar(15)
    ,Programme_Trans_Sk             bigint      not null
    ,Channel_Name                   varchar(30)
    ,Epg_Title                      varchar(50)
    ,Genre_Description              varchar(30)
    ,Sub_Genre_Description          varchar(30)
    ,Normed_channel                 varchar(20)
    -- Additional stuff for capping:
    ,c2h_Viewing_Start_Time         datetime
    ,c2h_Viewing_End_Time           datetime
    ,c2h_Programme_Viewed_Duration  decimal(10,0) -- do we even use the duration after the capping? yes, for metrics.
    -- Additional stuff for chain construction:
    ,c2h_Chain_ID                   bigint
    ,c2h_Chain_ID_event_count       bigint      -- number of events in chain that event belongs to
    -- And then stuff for putting denormalised chain details in:
    ,c2h_view_chain_start_time      datetime
    ,c2h_view_chain_end_time        datetime
    ,c2h_view_chain_duration        bigint
);

insert into stafforr.hh_cube_viewing (
    cb_row_ID
    ,Account_Number
    ,Subscriber_Id
    ,Adjusted_Event_Start_Time
    ,X_Adjusted_Event_End_Time
    ,X_Viewing_Start_Time
    ,X_Viewing_End_Time
    ,Tx_Start_Datetime_UTC
    ,Tx_End_Datetime_UTC
    ,Recorded_Time_UTC
    ,Play_Back_Speed
    ,X_Event_Duration
    ,X_Programme_Duration
    ,X_Programme_Viewed_Duration
    ,X_Programme_Percentage_Viewed
    ,X_Viewing_Time_Of_Day
    ,Programme_Trans_Sk
    ,Channel_Name
    ,Epg_Title
    ,Genre_Description
    ,Sub_Genre_Description
    ,Normed_channel
)
select
    -- Also grabbing select block from the "http://rtci/vespa1/Daily%20tables.aspx"
    vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Adjusted_Event_Start_Time
    ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
    ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
    ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
    ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
    ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
    -- And adding this guy:
    ,prog.Normed_channel
from sk_prod.VESPA_STB_PROG_EVENTS_20110701 as vw
--from sk_prod.VESPA_STB_PROG_EVENTS_20110702 as vw
--from sk_prod.VESPA_STB_PROG_EVENTS_20110703 as vw
inner join stafforr.hh_cube_programme_lookup as prog
    on vw.programme_trans_sk = prog.programme_trans_sk
inner join stafforr.hh_cube_account_listing_dd as ac
    on vw.account_number = ac.account_number
-- Restrict to viewing events, using filter from wiki
where (play_back_speed is null or play_back_speed = 2) -- NULL means live, 2 is timeshifted
and x_programme_viewed_duration > 0
and Panel_id = 5
and x_type_of_viewing_event <> 'Non viewing event';

-- Okay, and we need some indices to make stuff not slow:
create index for_joins      on stafforr.hh_cube_viewing (account_number, programme_trans_sk);

select count(1) from  stafforr.hh_cube_viewing;
-- 1273987. SLightly more now we've expanded boxes to multiroom-over-days stuff.

/************ 2.b) Stitch on Lifestage and Affluence profiling tags ************/

select account_number,
    max(case when ilu_lifestage = '01'  then '18-24 ,Living with Parents'
        when ilu_lifestage = '02'      then '18-24 ,Left Home'
        when ilu_lifestage = '03'      then '25-34 ,Single (no kids)'
        when ilu_lifestage = '04'      then '25-34 ,Couple (no kids)'
        when ilu_lifestage in ('05','06','07')         then '25-34 (kids)'
        when ilu_lifestage = '08'      then '35-44 ,Single (no kids)'
        when ilu_lifestage = '09'      then '35-44 ,Couple (no kids)'
        when ilu_lifestage = '10'      then '45-54 ,Single (no kids)'
        when ilu_lifestage = '11'      then '45-54 ,Couple (no kids)'
        when ilu_lifestage in ('12','13','14','15')    then '35-54 (kids)'
        when ilu_lifestage = '16'      then '55-64 ,Not Retired - Single'
        when ilu_lifestage = '17'      then '55-64 ,Not Retired - Couple'
        when ilu_lifestage = '18'      then '55-64 ,Retired'
        when ilu_lifestage = '19'      then '65-74 ,Not retired'
        when ilu_lifestage = '20'      then '65-74 ,Retired Single'
        when ilu_lifestage = '21'      then '65-74 ,Retired Couple'
        when ilu_lifestage = '22'      then '75+ ,Single'
        when ilu_lifestage = '23'      then '75+ ,Couple'
        else                                            'Unknown'
    end) as lifestage,
    max(CASE WHEN ilu_affluence in ('01','02','03','04')    THEN 'Very Low'
        WHEN ilu_affluence in ('05','06')               THEN 'Low'
        WHEN ilu_affluence in ('07','08')               THEN 'Mid Low'
        WHEN ilu_affluence in ('09','10')               THEN 'Mid'
        WHEN ilu_affluence in ('11','12')               THEN 'Mid High'
        WHEN ilu_affluence in ('13','14','15')          THEN 'High'
        WHEN ilu_affluence in ('16','17')               THEN 'Very High'
        ELSE                                                 'Unknown'
    END) as affluence
into #profiling_lookup
from sk_prod.cust_single_account_view
where account_number is not null
and account_number <> '99999999999999'
and account_number <> ''
group by account_number;

create unique index fake_pk on #profiling_lookup (account_number);

update stafforr.hh_cube_viewing
set cv.lifestage = pl.lifestage,
    cv.affluence = pl.affluence
from stafforr.hh_cube_viewing as cv
inner join #profiling_lookup as pl
on cv.account_number = pl.account_number;
-- 1273987 affected

/************ 2.c Stitch on box number for MR stuffs ************/

alter table stafforr.hh_cube_viewing add box_no tinyint not null default 27;
-- we're not pushing on anything bigger than 5, so any remaining 27's need
-- to be investigated.

-- This guy is a temp table which gets created in a different section, which
-- is a dirty trick, but hey. Blame will tell you when stuff in the script at
-- the same time.
update hh_cube_viewing set box_no = 27;

update hh_cube_viewing
set box_no = bn.box_no
from hh_cube_viewing
inner join #box_numbering as bn
on hh_cube_viewing.account_number = bn.account_number
and hh_cube_viewing.subscriber_id = bn.subscriber_id;
-- Account number not required, but that's how the PK of #box_numbering is built.

commit;


select count(1), count(distinct account_number), count(distinct subscriber_id)
from stafforr.hh_cube_viewing where box_no = 27;
-- roflcopter - 189414  8642    10310
-- So are they not on the stb log snapshot? the Panel_ID = 5 is in the extraction filter...

select top 10 subscriber_id from hh_cube_viewing
where box_no = 27
group by subscriber_id;
/*
10506603
237487
15750
11757540
8697581
26784560
163759
37375
120467
25939155
*/

select * from sk_prod.vespa_stb_log_snapshot
where subscriber_id in (10506603,
237487,
15750,
11757540,
8697581,
26784560,
163759,
37375,
120467,
25939155);

-- yeah, they're not there. How do we handle this? I guess we cleanse.

delete from hh_cube_viewing where box_no = 27;
-- byebye to 10k boxes worth of data, the stb_log_snapshot table don't know who you are.

-- OK, so how does that box breakdown work?
select box_no, count(1) as hits from hh_cube_viewing
group by box_no
order by box_no;
/*
0       169031
1       604456
2       231325
3       17816
4       1618
5       126
*/
-- Awesome, so there's a lot of MR households for which we only have one
-- box worth of data returned. Won't make analysis awkward at all, no sir.

-- And we also want the total number of boxes reporting ever per household:
/* So we were going to use this build, except that there are fewer results here
** than in the boxes-reporting build. So this could be something about the box
** being replaced or changed or something in the clients home, and might be
** overstating multiroom, but the numbers involved are so huge (100k box switches
** within three days?) doesn't sound feasible, instead we're just distructing
** the data. Oh well.
select rh.account_number, count(1) as box_count
into #hh_box_counting
from stafforr.hh_cube_account_listing_dd as caldd
inner join sk_prod.vespa_stb_log_snapshot as sls
on caldd.account_number = sls.account_number
group by account_number;

commit;
create unique index fake_pk on #hh_box_counting (account_number);
commit;

update hh_cube_viewing
set hh_box_count = hbc.box_count
from hh_cube_viewing inner join #hh_box_counting as hbc
on hh_cube_viewing.account_number = hbc.account_number

commit;
*/

-- Patching numbers in from reporting box counts instead:
select account_number, count(distinct subscriber_id) as hh_box_count
into #hh_box_counting
from #hh_cube_account_listing
group by account_number
having count(distinct subscriber_id) > 1; -- should have filtered down to MR in the previous pull

commit;
create unique index fake_pk on #hh_box_counting (account_number);
commit;

update hh_cube_viewing set hh_box_count = 27;
commit;
update hh_cube_viewing
set hh_box_count = t.hh_box_count
from hh_cube_viewing inner join #hh_box_counting as t
on hh_cube_viewing.account_number = t.account_number;

-- Shouldn't have any with 1, shouldn't have any with 27.
select hh_box_count, count(1) as hits
from hh_cube_viewing
group by hh_box_count
order by hh_box_count;
-- And we don't cool.

select box_no, hh_box_count, count(1) as hits, count (distinct subscriber_id) as boxes
from hh_cube_viewing
where box_no > hh_box_count
group by box_no, hh_box_count;
/*
3	2	2745	177
4	2	72	4
4	3	456	22
5	4	95	4
*/
-- Awesome, so the STB thing doesn't line up with the dailies. So we're goign to hack
-- around it...

select account_number, max(box_no) as new_hh_box_count
into #box_no_patching
from hh_cube_viewing
where box_no > hh_box_count
group by account_number;

commit;

create unique index fake_pk on #box_no_patching (account_number);

update hh_cube_viewing
set hh_box_count = t.new_hh_box_count
from hh_cube_viewing inner join #box_no_patching as t
on hh_cube_viewing.account_number = t.account_number;

commit;
-- Bit of a hack, but sure, now we at least have (internally) consistent stuff.

/************ 3. Populate capped start & end times (now only using 2 hour rule) ************/

-- First the capped viewing for non-replay events...
update stafforr.hh_cube_viewing
set
    c2h_Viewing_Start_Time = x_viewing_start_time, -- this should be min of adjusted_event_start_time and tx_start_datetime_UTC, already done for us
    c2h_Viewing_End_Time = case
                -- case when view is capped and cap passes end of show:
                when X_Event_Duration >= 7200 and dateadd(ss, 7200, adjusted_event_start_time) > tx_end_datetime_utc then tx_end_datetime_utc
                -- The case where the event is capped, but the cap does not pass the end of the show:
                when X_Event_Duration >= 7200 then dateadd(ss, 7200, adjusted_event_start_time)
                -- The case where the event is not capped but extends past the end of the show:
                when dateadd(ss, X_Event_Duration, adjusted_event_start_time) > tx_end_datetime_utc then tx_end_datetime_utc
                -- The case where the event is not capped and ends midway through the show, and the event started in the middle of the show:
                when adjusted_event_start_time >= TX_start_datetime_utc then dateadd(ss, X_Event_Duration, adjusted_event_start_time)
                -- Where the event is not capped and ends midway through the show, but also started before the programme did
                else dateadd(ss, X_Programme_viewed_Duration, TX_start_datetime_utc) end
where recorded_time_UTC is null;
-- 907158 updated.

-- OK, so the above has been adjusted to not bother with "reverse" capping, ie, there's
-- no longer any viewing associated with the last two hours of an event. Which is consistent
-- with what everyone else is doing, but it means that if you turn on the TV and sit down
-- and don't change the channel, that's veiwing we're missing since you're still on a previous
-- event. Still, this is important to tidy up some bug fixed with unreasonably long viewing
-- durations, so, sure.

-- <- needs full rerun from here.

-- Then capping for recorded events: also capping @ 2 hours, plus normalising to the broadcast time
update stafforr.hh_cube_viewing
set
    c2h_Viewing_Start_Time = case when recorded_time_UTC >= TX_start_datetime_utc then recorded_time_UTC else TX_start_datetime_utc end,
    c2h_Viewing_End_Time   = case
                                -- The case where the event is capped and the cap passes the end of the show:
                                when X_Event_Duration >= 7200 and dateadd(ss, 7200, recorded_time_UTC) > tx_end_datetime_utc then tx_end_datetime_utc
                                -- The case where the event is capped, but the cap does not pass the end of the show:
                                when X_Event_Duration >= 7200 then dateadd(ss, 7200, recorded_time_UTC)
                                -- The case where the event is not capped but extends past the end of the show:
                                when dateadd(ss, X_Event_Duration, recorded_time_UTC) > tx_end_datetime_utc then tx_end_datetime_utc
                                -- The case where the event is not capped and ends midway through the show, and the event started in the middle of the show:
                                when recorded_time_UTC >= TX_start_datetime_utc then dateadd(ss, X_Event_Duration, recorded_time_UTC)
                                -- Where the event is not capped and ends midway through the show, but also started before the programme did
                                else dateadd(ss, X_Programme_viewed_Duration, TX_start_datetime_utc) end 
where recorded_time_UTC is not null;
-- 117214 updated (including the box culling from the other branch, lols)

-- Wait, there's another case; where it gets capped but the cap doesn't pass the end of the show
-- and the evet started before the program...

-- Oh wait, we want to blank the ones where the start time ends up after the
-- programme finishes or the end time before the programme starts....
update stafforr.hh_cube_viewing
set     c2h_viewing_start_time = null,
        c2h_viewing_end_time = null
where   c2h_viewing_start_time >= Tx_End_Datetime_UTC
    or  c2h_viewing_end_time <= Tx_start_Datetime_UTC;
-- 0 rows affected? really? well, because we're acting on a data set that we already cleansed
-- those things from, so sure. (On a clean run through, this wouldd see values).

-- OK, that was easy. Now update the viewed durations too:
update stafforr.hh_cube_viewing
set 
    c2h_Programme_Viewed_Duration = case
                when c2h_viewing_start_time is null or c2h_viewing_end_time is null then null
                else datediff(ss, c2h_viewing_start_time, c2h_viewing_end_time) end
;
-- 1024372 updated

-- QA: How many instances are there where we claim the capped start happens before the programme start?
-- or any cases where the capped duration exceeds the programme viewed duration?
select * from stafforr.hh_cube_viewing
where c2h_Programme_Viewed_Duration > X_Programme_viewed_Duration
or TX_start_datetime_utc > c2h_Viewing_Start_Time;
-- Nothing! sweet! problem fixed!

-- oh, heh, now these are the columns we actually need to index:
create index for_c2h_chaining   on stafforr.hh_cube_viewing (Subscriber_id, programme_trans_sk, c2h_Viewing_Start_Time);

select max(c2h_Programme_Viewed_Duration)
from stafforr.hh_cube_viewing;
-- 7200! that's our cap limit, so, cool.

-- Oh, wait, we don't even care about those things that got capped out:
delete from stafforr.hh_cube_viewing
where c2h_Programme_Viewed_Duration is null;
-- Yeah, having decent capping rules kicks out a lot of the stuff. This is probably why we
-- had huge results before and not so much any more; much less overlap when we haven't got
-- whole days worth of stuff that overlaps because boxes happened to be left on tuned to
-- the same channel.

select count(1) from stafforr.hh_cube_viewing;
-- 962016 - not so much. How much overlap are we going to get? maybe not many chains now.

/************ 4. Build chains, populate chain IDs: 2 hour cap ************/

-- Now for live demo we're only going to use the 2 hour capping.

select l.cb_row_id
into #c2h_overlapping_viewing_keys
from stafforr.hh_cube_viewing as l
inner join stafforr.hh_cube_viewing as r
on l.account_number = r.account_number
and l.programme_trans_sk = r.programme_trans_sk
and l.cb_row_id <> r.cb_row_id
and (   (l.c2h_viewing_start_time <= r.c2h_viewing_start_time and l.c2h_viewing_end_time > r.c2h_viewing_start_time)
    or  (r.c2h_viewing_start_time <= l.c2h_viewing_start_time and r.c2h_viewing_end_time > l.c2h_viewing_start_time)
);
-- 2 hour capping: 170474  fixed! probably. The issue was with recorded stuff on the same box, 
-- not being properly linked to live stuff or even other recorded stuff. Should be solved now?

select distinct cb_row_id
into #c2h_overlapping_viewing_keys_dd
from #c2h_overlapping_viewing_keys;
-- 2h cap: 110762 now.

create unique index fake_PK on #c2h_overlapping_viewing_keys_dd (cb_row_id);

-- Identifying keys then extracting avoids duplicates...

drop table #c2h_chainmaking_holdingpen;

select
    t.cb_row_id -- doesn't get used in processing, but audit trails are good
    ,t.account_number
    ,t.subscriber_id
    ,t.programme_trans_sk
    ,t.c2h_viewing_start_time
    ,t.c2h_viewing_end_time
    ,t.c2h_programme_viewed_duration
into #c2h_chainmaking_holdingpen
from stafforr.hh_cube_viewing as t
inner join #c2h_overlapping_viewing_keys_dd as mvo
on t.cb_row_id = mvo.cb_row_id;

drop table #c2h_overlapping_viewing_keys_dd;
drop table #c2h_overlapping_viewing_keys;

-- For QA or auditing or whatever:
select
        count(distinct account_number) as households,
        count(distinct subscriber_id) as total_boxes,
        count(distinct programme_trans_sk) as different_programmes,
        count(1) as hits
from #c2h_chainmaking_holdingpen;
-- 2h cap: 13354	19596	1198	110762
-- Note this is the actualy 2hour cap now, not the buggy 2hour cap we previously had in place.

create unique index fake_PK on #c2h_chainmaking_holdingpen (cb_row_id);
create index chain_joining  on #c2h_chainmaking_holdingpen (account_number, programme_trans_sk);
create index box_joining    on #c2h_chainmaking_holdingpen (subscriber_id);

-- OK, now with the isolated population we can start to build the chains:
drop table stafforr.c2h_viewing_chains;
-- This one has to be permanent since we can't ALTER temp tables;
select
        min(r.cb_row_id) as cb_row_id -- this in theory should end up unique across chains?
        ,r.account_number
        ,r.programme_trans_sk
        ,r.c2h_viewing_start_time
        ,max(r.c2h_viewing_end_time) as chain_viewing_end_time
        ,r.c2h_viewing_start_time as previous_loop_viewing_end_time -- for init, we just want something that differs from chain_viewing_end_time
into stafforr.c2h_viewing_chains
from #c2h_chainmaking_holdingpen as l
right join #c2h_chainmaking_holdingpen as r
on l.account_number = r.account_number
and l.programme_trans_sk = r.programme_trans_sk
-- Want things with overlapping viewing intervals with l coming before r
and l.c2h_viewing_start_time < r.c2h_viewing_start_time
and l.c2h_viewing_end_time >= r.c2h_viewing_start_time
-- Want only vierwing intervals that are first in the chain
where l.account_number is null
-- To catch instances where two boxes in the same house activate at
-- exactly the same time: can't use the rank() - delete trick as we
-- want the event that lasts the longest and rank() just gets the
-- first in the table.
group by r.account_number, r.programme_trans_sk, r.c2h_viewing_start_time;
-- 2h cap: 37598

alter table c2h_viewing_chains add id bigint not null identity;
alter table c2h_viewing_chains add primary key (id);
create unique index chain_join on c2h_viewing_chains (account_number, programme_trans_sk, c2h_viewing_start_time);

-- Ongoing QA: check that the cb_row_id is unique: should get nothing
select cb_row_id, count(1) as hits
from c2h_viewing_chains
group by cb_row_id
having hits > 1
order by hits desc;
-- c2h: nothing, still okay

-- OK, now iterating until our thing concludes... (MANUAL LOOP START)

select
        hhvc.id,
        max(vs.c2h_viewing_end_time) as next_time
into #next_chain_iteration
from c2h_viewing_chains as hhvc
inner join #c2h_chainmaking_holdingpen as vs
-- The households and programmes match...
on hhvc.account_number = vs.account_number
and hhvc.programme_trans_sk = vs.programme_trans_sk
-- Skip processing on chains that are already terminated:
and hhvc.chain_viewing_end_time <> hhvc.previous_loop_viewing_end_time
-- And the intervals overlap with the new table forward in time...
and vs.c2h_viewing_start_time >= hhvc.c2h_viewing_start_time
and vs.c2h_viewing_start_time <= hhvc.chain_viewing_end_time
group by hhvc.id;
-- Refer to viewing_overlapage.sql for a bit more on this process
create unique index fake_pk on #next_chain_iteration (id);
-- Update the chain ends:
update c2h_viewing_chains
set
        hhvc.previous_loop_viewing_end_time = hhvc.chain_viewing_end_time,
        hhvc.chain_viewing_end_time = nci.next_time
from c2h_viewing_chains as hhvc
inner join #next_chain_iteration as nci
on hhvc.id = nci.id;

-- Then loop the above until the terminated chains are the full population
select
        count(1) as chains,
        sum(case when chain_viewing_end_time = previous_loop_viewing_end_time then 1 else 0 end) as terminated_chains
from c2h_viewing_chains;

commit;
drop table #next_chain_iteration;
commit;

--  (MANUAL LOOP END)

-- Ha! And now all the timeshifted stuff is in, the chains are taking
-- a lot longer to converge, as you might expect. Even purging out the
-- impropperly capped stuff, it took quite a while.

/************ 5. QA on chain construction, pushing Chain IDs onto table ************/

-- Check they each did what they should, then push the flags onto the cube.

-- First, QA on the 2 hour capping results:
select vs.*
from #c2h_chainmaking_holdingpen as vs
left join c2h_viewing_chains as hhvc
-- Join condition for including events in chains:
on vs.account_number = hhvc.account_number
and vs.programme_trans_sk = hhvc.programme_trans_sk
and vs.c2h_viewing_start_time >= hhvc.c2h_viewing_start_time
and vs.c2h_viewing_end_time <= hhvc.chain_viewing_end_time
-- But we only want the non-included ones
where hhvc.id is null;
-- Want this to be empty, so that every viewing event is in some chain.

-- Can also chech that the chains themselves don't overlap. If they do,
-- most likely some of the boundary conditions on the interval overlap
-- checks aren't right, and / or we'll get fencepost errors there.
select l.id, r.id
from c2h_viewing_chains as l
inner join c2h_viewing_chains as r
on l.account_number = r.account_number
and l.programme_trans_sk = r.programme_trans_sk
and l.id <> r.id -- only care for QA if any exist, don't need full symmetrical view
-- restrict to chains that overlap:
and (   (l.c2h_viewing_start_time <= r.c2h_viewing_start_time and l.chain_viewing_end_time > r.c2h_viewing_start_time)
    or  (r.c2h_viewing_start_time <= l.c2h_viewing_start_time and r.chain_viewing_end_time > l.c2h_viewing_start_time)
);
-- No erroneous indicators from either of those, cool.

update stafforr.hh_cube_viewing set c2h_Chain_ID = null;

-- OK, now push the flags and chain details onto the cube table:
update stafforr.hh_cube_viewing
set
    c2h_Chain_ID                = hhvc.cb_row_id
    ,c2h_view_chain_start_time  = hhvc.c2h_viewing_start_time
    ,c2h_view_chain_end_time    = hhvc.chain_viewing_end_time
    ,c2h_view_chain_duration    = datediff(ss, hhvc.c2h_viewing_start_time, hhvc.chain_viewing_end_time)
from stafforr.hh_cube_viewing as vs
inner join c2h_viewing_chains as hhvc
-- Join condition for including events in chains:
on vs.account_number = hhvc.account_number
and vs.programme_trans_sk = hhvc.programme_trans_sk
and vs.c2h_viewing_start_time >= hhvc.c2h_viewing_start_time
and vs.c2h_viewing_end_time <= hhvc.chain_viewing_end_time;
-- 110762 which lines up, that's good.

-- OK, so these totals should match the number of chains we built and
-- the populations of the overlapping tables we first isolated:
select
    count(distinct c2h_Chain_ID) as c2h_chains
    ,sum(case when c2h_Chain_ID is null then 0 else 1 end) as c2h_chained_pop
    ,sum(case when c2h_viewing_start_time < c2h_view_chain_start_time then 1 else 0 end) as c2h_start_fail
    ,sum(case when c2h_viewing_end_time > c2h_view_chain_end_time then 1 else 0 end) as c2h_end_fail
from stafforr.hh_cube_viewing;
-- 37598	110762	0	0
-- Sweet, internal consistnecy attained.

-- Now we need to push on the event counts per chain...
select c2h_Chain_ID, count(1) as hits
into #c2h_chain_counts
from stafforr.hh_cube_viewing
where c2h_Chain_ID is not null
group by c2h_Chain_ID;
-- 37598 records, which is what we expected

create unique index for_joining on #c2h_chain_counts (c2h_Chain_ID);

update stafforr.hh_cube_viewing
set c2h_Chain_ID_event_count = hits
from stafforr.hh_cube_viewing as cv
inner join #c2h_chain_counts as cc
on cv.c2h_Chain_ID = cc.c2h_Chain_ID;
-- 110762 updates

-- Everything that is left over didn't go into a chain, which means
-- it didn't get into the overlap table, which means they're all by
-- themselves, which means that they get their own ID as the chain
-- ID, since they're the singleton chains.
update stafforr.hh_cube_viewing
set 
    c2h_Chain_ID                = cb_row_id
    ,c2h_Chain_ID_event_count   = 1
    ,c2h_view_chain_start_time  = c2h_Viewing_Start_Time
    ,c2h_view_chain_end_time    = c2h_Viewing_End_Time  
    ,c2h_view_chain_duration    = c2h_Programme_Viewed_Duration
where c2h_Chain_ID is null;
-- 851254 updated

select count(1) from stafforr.hh_cube_viewing;
-- 962016 - so yeah, all those totals all add up nicely, and it's consistent,
-- and we happy, though the amount of overlap in here is tiny anyway :/

-- Summary of all chain behaviour to check if stuff overlaps: (because it shoudln't)
select c2h_chain_id,
    min(account_number) as account_number,
    min(programme_trans_sk) as programme_trans_sk,
    min(c2h_view_chain_start_time) as c2h_view_chain_start_time,
    min(c2h_view_chain_end_time) as c2h_view_chain_end_time
into #all_chain_summary
from hh_cube_viewing
group by c2h_chain_id;
-- 888852; is that what we expect?

commit;
create unique index forjoins on #all_chain_summary (account_number, programme_trans_sk, c2h_chain_id);
commit;

-- This should in theory return no results, because  and overlaps should have
-- caused the chains to merge.
select l.c2h_chain_id, r.c2h_chain_id
from #all_chain_summary as l
inner join #all_chain_summary as r
on l.account_number = r.account_number
and l.programme_trans_sk = r.programme_trans_sk
and l.c2h_chain_id <> r.c2h_chain_id
-- restrict to chains that overlap:
and (   (l.c2h_view_chain_start_time <= r.c2h_view_chain_start_time and l.c2h_view_chain_end_time > r.c2h_view_chain_start_time)
    or  (r.c2h_view_chain_start_time <= l.c2h_view_chain_start_time and r.c2h_view_chain_end_time > l.c2h_view_chain_start_time)
);
-- Nothing! sweet!

-- Temp tables we no longer need:
drop table #c2h_chainmaking_holdingpen;
drop table #c2h_chain_counts;
drop table #all_chain_summary;

/************ 6. Oh, hey, permissions! ************/

grant select on stafforr.hh_cube_viewing to vespa_analysts, greenj, dbarnett, jacksons, sbednaszynski, jchung;

/************ 7. Control totals introducing reporting ************/

-- OK, so the old build still restricted it to our three channels. We don't
-- want that from the first set of control totals, but rather demonstrate how-
-- much of the full data set our sample is.
create table #hh_all_household_boxes (
    Account_Number                  varchar(20)     not null
    ,Subscriber_Id                  decimal(8,0)    not null
    ,box_viewing                    bigint          not null
);

insert into #hh_all_household_boxes
select Account_Number, Subscriber_Id, sum(x_programme_viewed_duration)
from sk_prod.VESPA_STB_PROG_EVENTS_20110701 as vw
--from sk_prod.VESPA_STB_PROG_EVENTS_20110702 as vw
--from sk_prod.VESPA_STB_PROG_EVENTS_20110703 as vw
-- Restrict to viewing events, using filter from wiki
where (play_back_speed is null or play_back_speed = 2) -- NULL means live, 2 is timeshifted
and x_programme_viewed_duration > 0
and Panel_id = 5
and x_type_of_viewing_event <> 'Non viewing event'
group by Account_Number, Subscriber_Id;

select Account_Number, count(distinct Subscriber_Id) as boxes_count, sum(box_viewing) as household_viewing
into #hh_all_households
from #hh_all_household_boxes
group by Account_Number;

select boxes_count, count(1) as households, round(sum(household_viewing) / 60.0 / 60.0,1) as total_viewing_hours
from #hh_all_households
group by boxes_count
order by boxes_count;
/* OK, so now we have how many households have multiple boxes etc...
1	120203	4161788
2	29212	2322789
3	2650	338243
4	257	    45898
5	21	    4294
6	1	    340
*/

-- And so how does this compare to what's in our population?
select Account_Number,
    count(distinct Subscriber_Id) as boxes_count,
    sum(x_programme_viewed_duration) as uncapped_viewing,
    sum(c2h_programme_viewed_duration) as capped_viewing,
    sum(case when normed_channel = 'BBC1' then c2h_programme_viewed_duration else 0 end) as BBC1_capped_viewing,
    sum(case when normed_channel = 'Dave' then c2h_programme_viewed_duration else 0 end) as Dave_capped_viewing,
    sum(case when normed_channel = 'Sky Sports 1' then c2h_programme_viewed_duration else 0 end) as Sports1_capped_viewing
into #hh_sampled_households
from stafforr.hh_cube_viewing
group by Account_Number;

select boxes_count,
    count(1) as households,
    round(sum(uncapped_viewing) / 60.0 / 60.0, 1) as total_viewing_hours,
    round(sum(capped_viewing) / 60.0 / 60.0, 1) as capped_viewing_hours,
    round(sum(BBC1_capped_viewing) / 60.0 / 60.0, 1) as BBC1_viewing_hours,
    round(sum(Dave_capped_viewing) / 60.0 / 60.0, 1) as Dave_viewing_hours,
    round(sum(Sports1_capped_viewing) / 60.0 / 60.0, 1) as Sports1_viewing_hours
from #hh_sampled_households
group by boxes_count
order by boxes_count;
-- Why do we have households with boxes_count being 1? oh, because of capping?
-- Nope, because we have all multiroom. A lot of multiroom didn't have more
-- than one box hitting any of our target stations.
/*
1	15786	94378.9	    79946.5	    66636.7	    4007.5	    9302.3
2	13269	146488.9	124057.0	99881.7	    6060.1	    18115.2
3	608	    9236.7	    7803.3	    6165.6	    440.4	    1197.3
4	29	    656.6	    539.5	    387.1	    24.4	    128.0
5	1	    3.8	        3.8	        2.8	        1.1	        0.0
*/

-- Oh, wait, also, how many boxes were enabled as at July 1st?
select count(distinct account_number) as households, count(1) as boxes
from sk_prod.VESPA_SUBSCRIBER_STATUS
where result='Enabled'
and request_dt >= '2011-07-01 00:00:00';
-- 315284	425879

/************ 9. Metric builds in bulk ************/

-- Metrics we want (from the PPT with Gavin) include:
/*
Total time viewed
Total distinct time
Proportion of show viewed
Count of viewing instances
Viewing instances per device
Largest continuous instance
Largest instance proportion
Average instance duration
Peak device count - um, erhm, no idea how to do this well inside a DB :(
*/

-- Okay, so we used to be doing stuff over each household. Now instead we're
-- going to twist this into grouping by box_no to get the different behaviours
-- across the different boxes.

-- Some intermediate normalised things we need first:
-- 1/ number of boxes in each household
-- because, like, show duration is already on the cube.
drop table c2h_metrics_by_box_no;

create table c2h_metrics_by_box_no (
    box_no                                  tinyint         not null
    ,Lifestage                              varchar(30)
    ,Affluence                              varchar(10)
    ,programme_trans_sk                     bigint          not null
    ,epg_title                              varchar(50)
    ,channel_name                           varchar(30)
    ,normed_channel                         varchar(20)
    ,X_Programme_Duration                   decimal(10,0)
    ,Tx_Start_Datetime_UTC                  datetime
    ,Tx_End_Datetime_UTC                    datetime
    ,boxes_viewing_show                     bigint
    ,total_time_viewed                      bigint
--    ,average_proportion_of_show_viewed      float     -- requires update - not sure this one makes sense given the new aggregation path
    ,count_of_viewing_instances             integer
    ,viewing_instances_per_dev              float     -- requires update - can do these averages, but they'll end up broken if you try to pivot them
    ,instances_per_dev_per_hour             float     -- requires update - can do these averages, but they'll end up broken if you try to pivot them
    ,average_instance_duration              bigint
    ,primary key (programme_trans_sk, lifestage, affluence, box_no)
);

insert into c2h_metrics_by_box_no
(
    box_no
    ,Lifestage
    ,Affluence
    ,programme_trans_sk
    ,epg_title
    ,channel_name
    ,normed_channel
    ,X_Programme_Duration
    ,Tx_Start_Datetime_UTC
    ,Tx_End_Datetime_UTC
    ,boxes_viewing_show
    ,total_time_viewed
    ,count_of_viewing_instances
    ,average_instance_duration
)
select
    box_no
    ,Lifestage
    ,Affluence
    ,programme_trans_sk
    ,min(epg_title)
    ,min(channel_name)
    ,min(normed_channel)
    ,min(X_Programme_Duration)
    ,min(Tx_Start_Datetime_UTC)
    ,min(Tx_End_Datetime_UTC)
    ,count(distinct subscriber_id)
    ,sum(c2h_Programme_Viewed_Duration)
    ,count(1)
    ,avg(c2h_Programme_Viewed_Duration)
from hh_cube_viewing
group by box_no, lifestage, affluence, programme_trans_sk;
-- 5779 items (just box_no) and now with affluence & lifestage: 174935. Sure.
-- That'll fit in a pivot. But will it tell us anything?


-- And other people would like to be able to see it...
grant select on stafforr.c2h_metrics_by_box_no to vespa_analysts, greenj, dbarnett, jacksons, sbednaszynski, jchung;

-- May as well share the chains tables too...
grant select on stafforr.c2h_viewing_chains to vespa_analysts, greenj, dbarnett, jacksons, sbednaszynski, jchung;

-- Other indices to support analysis:
create index index_programme_trans_sk on c2h_metrics_block (programme_trans_sk);

/************ 10. Basical reporting on households by box count ************/

-- In addition to the box number, we also now have box count... wait... we
-- were going to rebuild box number off the subscriber status table, or
-- at least take the 0's out of it...
select hh_box_count, box_no, count(1) as hits, count (distinct subscriber_id) as boxes
from hh_cube_viewing
group by box_no, hh_box_count
order by hh_box_count, box_no;
/* hh_box_count	box_no	hits	boxes
2	1	619441	24069
2	2	187556	12751
3	1	60481	2323
3	2	19584	1372
3	3	14591	1051
4	1	5029	238
4	2	2048	155
4	3	1675	116
4	4	1390	98
5	1	743	21
5	2	137	12
5	3	36	5
5	4	83	6
5	5	116	6
6	1	26	1
6	2	20	1
*/
-- Okay, so we have some good numbers for the boox things, though, don't like those
-- 6's going to manually push those back to 3's rather than delete them...

update hh_cube_viewing set hh_box_count = 3 where hh_box_count = 6;
-- ugly, but not huge.

-- Okay, so things we want on this dice: hh box count and box number (they're
-- the main ones), programme_trans_sk for fine grained filtering, and then
-- the variuos totals metrics on interval duration and number and we'll also
-- need programme duration so we can do instances per hour...
drop table c2h_metrics_by_box_counts;

create table c2h_metrics_by_box_counts (
    hh_box_count                            tinyint         not null
    ,box_no                                 tinyint         not null
    ,programme_trans_sk                     bigint          not null
    ,epg_title                              varchar(50)
    ,channel_name                           varchar(30)
    ,normed_channel                         varchar(20)
    ,X_Programme_Duration_in_minutes        decimal(10,0)
    ,Tx_Start_Datetime_UTC                  datetime
    ,Tx_End_Datetime_UTC                    datetime
    ,boxes_viewing_show                     bigint
    ,total_instances                        bigint
    ,total_duration_in_minutes              bigint
    ,primary key (programme_trans_sk, hh_box_count, box_no)
);

insert into c2h_metrics_by_box_counts
(
    hh_box_count
    ,box_no
    ,programme_trans_sk
    ,epg_title
    ,channel_name
    ,normed_channel
    ,X_Programme_Duration
    ,Tx_Start_Datetime_UTC
    ,Tx_End_Datetime_UTC
    ,boxes_viewing_show
    ,total_instances
    ,total_duration_in_minutes
)
select
    hh_box_count
    ,box_no
    ,programme_trans_sk
    ,min(epg_title)
    ,min(channel_name)
    ,min(normed_channel)
    ,floor(min(X_Programme_Duration) / 60.0)
    ,min(Tx_Start_Datetime_UTC)
    ,min(Tx_End_Datetime_UTC)
    ,count(distinct subscriber_id)
    ,floor(sum(c2h_Programme_Viewed_Duration) / 60.0)
    ,count(1)
from hh_cube_viewing
group by programme_trans_sk, box_no, hh_box_count;
-- 8947 records. Not much at all, cool.

grant select on stafforr.c2h_metrics_by_box_counts to vespa_analysts, greenj, dbarnett, jacksons, sbednaszynski, jchung;

/************ 11. Examples of particular stuff; as illustrated by the Pivots ************/

-- It turns out that big skew we saw for Lee Mack was only for certain
-- demographic sections, it wasn't an overall thing. Still, we'd like
-- to look into that ratio-of-box-one-to-box-two thing, and maybe pull
-- out that particular demographic as a special case.

-- Though; it seems to be driven by Lifestage "18-24 ,Left Home" (all
-- affluences) which watch a large portion of particular shows vastly
-- more on the 2nd box than the first. Might pull out a list of those
-- top ratios of things...

select distinct lifestage from c2h_metrics_by_box_no;

-- But yeah, there's a bit of stuff there, but the volumes are small
-- enough for it to just be low sample size things.

drop table #The_2_box_analysis;

select programme_trans_sk
    ,min(epg_title) as epg_title
    ,min(channel_name) as channel_name
    ,min(normed_channel) as normed_channel
    ,floor(min(X_Programme_Duration) / 60.0) as Programme_Duration_minutes
    ,min(Tx_Start_Datetime_UTC) as Tx_Start_Datetime_UTC
    ,min(Tx_End_Datetime_UTC) as Tx_End_Datetime_UTC
    ,sum(case when box_no = 1 then boxes_viewing_show                   else 0 end) as box_1_viewers
    ,sum(case when box_no = 1 then count_of_viewing_instances           else 0 end) as box_1_instances
    ,sum(case when box_no = 1 then floor(total_time_viewed / 60.0)      else 0 end) as box_1_duration_minutes
    ,sum(case when box_no = 2 then boxes_viewing_show                   else 0 end) as box_2_viewers
    ,sum(case when box_no = 2 then count_of_viewing_instances           else 0 end) as box_2_instances
    ,sum(case when box_no = 2 then floor(total_time_viewed / 60.0)      else 0 end) as box_2_duration_minutes
    ,sum(case when box_no = 3 then boxes_viewing_show                   else 0 end) as box_3_viewers
    ,sum(case when box_no = 3 then count_of_viewing_instances           else 0 end) as box_3_instances
    ,sum(case when box_no = 3 then floor(total_time_viewed / 60.0)      else 0 end) as box_3_duration_minutes
    ,sum(boxes_viewing_show) as total_viewers
    ,sum(count_of_viewing_instances) as total_instances
    ,floor(sum(total_time_viewed) / 60.0) as total_duration
into #The_2_box_analysis
from c2h_metrics_by_box_no
-- where lifestage = '18-24 ,Left Home           '
group by programme_trans_sk;

/************ 12. Bringing back the old household build, but including HH boxes ************/

-- Oh wait, it always had it.... but some bits still need changing...

drop table c2h_metrics_by_household;

create table c2h_metrics_by_household (
    account_number                  varchar(20)     not null
    ,programme_trans_sk             bigint          not null
    ,epg_title                      varchar(50)
    ,channel_name                   varchar(30)
    ,normed_channel                 varchar(20)
    ,Programme_Duration_in_minutes  decimal(10,0)
    ,Tx_Start_Datetime_UTC          datetime
    ,Tx_End_Datetime_UTC            datetime
    ,boxes_in_household             integer
    ,boxes_viewing_show             integer
    ,total_duration                 bigint
    ,total_distinct_duration        bigint          -- requires patch
    ,count_of_viewing_instances     integer
    ,largest_cont_view_instance     bigint
    ,primary key (account_number, programme_trans_sk)
);

insert into c2h_metrics_by_household
(
    account_number
    ,programme_trans_sk
    ,epg_title
    ,channel_name
    ,normed_channel
    ,Programme_Duration_in_minutes
    ,Tx_Start_Datetime_UTC
    ,Tx_End_Datetime_UTC
    ,boxes_in_household
    ,boxes_viewing_show
    ,total_duration
    ,count_of_viewing_instances
    ,largest_cont_view_instance
)
select
    account_number
    ,programme_trans_sk
    ,min(epg_title)
    ,min(channel_name)
    ,min(normed_channel)
    ,floor(min(X_Programme_Duration) / 60.0)
    ,min(Tx_Start_Datetime_UTC)
    ,min(Tx_End_Datetime_UTC)
    ,max(hh_box_count)
    ,count(distinct subscriber_id)
    ,sum(c2h_Programme_Viewed_Duration)
    ,count(1)
    ,max(c2h_Programme_Viewed_Duration)
from hh_cube_viewing
group by account_number, programme_trans_sk;
-- 585515 items. Sure.

-- ok, patch in boxes reporting per household:
create table #hh_cube_boxes (
    Account_Number                  varchar(20)     not null
    ,Subscriber_Id                  decimal(8,0)    not null
);

-- Poke in the thing about distinct viewing...
select account_number, programme_trans_sk,
    sum(datediff(ss, c2h_viewing_start_time, chain_viewing_end_time)) as tdv
into #hh_cube_tdv
from c2h_viewing_chains
group by account_number, programme_trans_sk;

create unique index fake_pk on #hh_cube_tdv (account_number, programme_trans_sk);

update c2h_metrics_by_household
set total_distinct_duration = coalesce(dd.tdv, mb.total_duration)
from c2h_metrics_by_household as mb
left join #hh_cube_tdv as dd
on mb.account_number = dd.account_number
and mb.programme_trans_sk = dd.programme_trans_sk;

-- And other people would like to be able to see it...
grant select on stafforr.c2h_metrics_by_household to vespa_analysts, greenj, dbarnett, jacksons, sbednaszynski, jchung;

-- Other indices to support analysis:
create index index_programme_trans_sk on c2h_metrics_by_household (programme_trans_sk);


