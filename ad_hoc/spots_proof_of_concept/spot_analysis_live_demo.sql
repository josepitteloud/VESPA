/****************************************************************
**
**      PROJECT VESPA: SPOT ANALYSIS TEST SCRIPT v2
**
** So we're got the spot import process built, though it's slow
** and we're probably delaying it until 1/ it's essential, or 2/
** we're on our own environment.
**
** We have, however, loaded about 10 days worth of adds for Sky
** Sports 3, and now we can pull out live data vs actual spots
** and tell who was watching what, who left when a particular
** advert was played, etc. Might be more interesting to do with
** a channel which shows programs rather than just sports, but
** this is a proof of concept thing, so we're all good.
**
** Things we want:
** 1. A MBM graph with 5 second slices over the whole break
** 2. A graph of commercial IDs and total number of watchers and
**      number of spots used and the total broadcast time covered
** 3. A graph of how many people watched a specific commercial at
**      various different broadcast times.
**
** It's going to suck that pivot charts can't use scatter plots,
** because that's more or less exactly how we want to show this.
**
**
****************************************************************/

-- There was previously a build of some spot analysis which fudged
-- some stuff together based on some demo spot logs we got which
-- were dated from May 2010. Now we've got a real 10 day sample of
-- actual logs, so we're going to proces those instead.

-- Okay, so now we've got the MBM script built for the by-channel stuff,
-- or at least debugged... we can now pull stuff out.

/************************** 0: Poking about, data familiarisation **************************/

select top 100 * from vespa_analysts.spots_all;
-- So we've got channel 4945 which is Sky Sports 3.

select distinct barb_code, break_start_time, break_total_duration
from vespa_analysts.spots_all
where barb_code = 4924;

/************************** 1: MBM graph w/ 5 sec slices **************************/
-- Let's use barb_code = 4924 and these three breaks:
-- break_start_time        break_total_duration
-- 2011-06-17 08:55:59.000000   150
-- 2011-06-15 17:08:22.000000   210
-- 2011-06-17 21:13:48.000000   210
-- 2011-06-17 17:57:17.000000   150
-- 2011-06-11 12:56:46.000000   90
-- 2011-06-13 03:16:31.000000   180
-- tegether the start time and the channel define a whole break. (Right now all
-- our data is 4945 thouhg.) Grab the two minutes before and after the break too
-- (more or less):

select dateadd(second, 150, '2011-06-17 08:55:59.000000');
-- 2011-06-17 08:58:29.000000

-- So 6 minutes at 20 samples per minute is 120 samples for this break...
select
        1 as Break_number,
        '2011-06-17 08:55:59' as Break_start_time,
        *
into spots_poc_MBM_graphs
from vespa_analysts.make_MBM_channel_graph(4924, '2011-06-17 08:54:00', '2011-06-17 09:00:00', 3);

-- Ok, and for subsequent ones we insert because the table already exists;
insert into spots_poc_MBM_graphs
select
        2 as Break_number,
        '2011-06-17 17:57:17' as Break_start_time,
        *
from vespa_analysts.make_MBM_channel_graph(4924, '2011-06-17 17:55:00', '2011-06-17 18:01:00', 3);

insert into spots_poc_MBM_graphs
select
        3 as Break_number,
        '2011-06-17 21:13:48' as Break_start_time,
        *
from vespa_analysts.make_MBM_channel_graph(4924, '2011-06-17 21:12:00', '2011-06-17 21:18:00', 3);

-- <- to here...

-- And now we have some graphs! But we want to be able to line the viewing up against the commercials.

select * from vespa_analysts.spots_all
where abs(datediff(second, break_start_time, '2011-06-08 18:25:58')) < 5;

-- Also! Spot Sequence ID is borked, we're getting some stuff pushed off in a weird way.
-- the sequence currently uses r_date_of_transmission, broadcasters_break_id so let's see
-- what dupes we have there: this example is break ID 42.
select * from vespa_analysts.spots_all where broadcasters_break_id = 42
order by spot_start_time;

/* Okay, so a hack to fix the sequences this one time:
select load_id, infile_id,
        rank() over (partition by barb_code, break_start_time order by spot_start_time) as new_sequence_id
into #spot_sequence_fixing
from vespa_analysts.spots_all;

create index joinsers on #spot_sequence_fixing (load_id, infile_id);

update vespa_analysts.spots_all
set spot_sequence_id = t.new_sequence_id
from vespa_analysts.spots_all
inner join #spot_sequence_fixing as t
on vespa_analysts.spots_all.load_id = t.load_id
and vespa_analysts.spots_all.infile_id = t.infile_id;
*/

-- Now we can get the spot stuff. But we also really want some things to tell
-- us where each advert started and stopped, so we can tell where the break
-- lives and also if people tuned in / out for particular adds.
select
        case
                when break_start_time = '2011-06-08 18:25:58' then 1
                when break_start_time = '2011-06-08 19:31:06' then 2
                when break_start_time = '2011-06-08 19:49:58' then 3
        end as break_number,
        spot_start_time,
        dateadd(second, spot_duration, spot_start_time) as spot_end_time,
        spot_sequence_id,
        clearcast_commercial_number
from vespa_analysts.spots_all
where break_start_time in ('2011-06-08 18:25:58', '2011-06-08 19:31:06', '2011-06-08 19:49:58')
order by break_start_time, spot_sequence_id;

-- Pull out the combined result set:
select * from spots_poc_MBM_graphs;

-- Wow, so Excel is just useless. It can't pivot scatter graphs at all, with the result
-- that all these graphs have to be either manually built or selected using Javascript
-- or something. Generally it's super inconvenient. Everything's been streamlined so
-- that it's easy to do what Excel wants, and basically impossible to find the things
-- you actually want to do :/

-- Okay, we ended up hacking both the spot and the viewing data into the same columns
-- in Excel, and adding the final spot end points on after all the starts (so that we
-- have all the endpoints defined. Still not actually a good solution, interesting to
-- see how this gets managed long term. Maybe we just add a further type, so that the
-- viewing slices are categorised as to whether they occur in the first show, second
-- show, or any particular spot? That could work, and then we'd also be able to pivot
-- it. But if we don't normalise the times, you still won't be able to compare them.
-- Still, they are some good ideas which would make the excel side a bit easier, and
-- by that I mean I'd have to spend less time there, just build a pivot thing.


/************************** 2: Analysis across all Commercial IDs **************************/

-- Okay, so the Clearcast commercial ID thing is a key which links together
-- all the instances of the same add being shown. Let's pull those out and
-- see which are the most common, etc. Getting viewing data for each could
-- be slightly nasty, as wee have to pull stuff out of the events view or
-- some kind of sample and basically it means we need another procedure :/

select clearcast_commercial_number
        ,count(1) as broadcasting_count
        ,sum(spot_duration) as total_broadcasting_time_in_seconds
from vespa_analysts.spots_all
group by clearcast_commercial_number
order by broadcasting_count desc;
-- 380 records. That's okay for our sample, but we'll need to cap that
-- going forwards. But yeah, we do want to push in the viewing data for each,
-- that's going to be a pain :/

-- Okay, so for the proc, you pass in the name of a table which has a bunch
-- of commercial IDs in it, and a time range. And it gives you back all these
-- profiles.

-- Go build a thing then.

/************************** 3: Broadcasting of one Commercial ID **************************/

-- So we pick one commercial ID, and then we want to assess how it did over
-- all the times it was broadcast. Hmrnph. This is another proc, because we
-- have to go back to the viewing data and we don't want to touch the events
-- view. But we're getting better at this, right? The procs are easier and
-- easier to make each time? hope so.

-- This one should give us back the channel, spot start time, and spot sequencing
-- ID for every broadcast of a particular commercial ID within a given time. Ok,
-- so we feed in the commercial ID and the time boundaries. What's the default?
-- watched any amount of the spot? More will be annoying, since we have to worry
-- about spots that are split across program transitions, and therefor have the
-- viewing in different event records which would need to be combined. Still,
-- good to have procs which encapsulate how we're allocating spot watching to
-- viewers. Also, how are we going to handle people watching the same spot more
-- than once? I reakon we just count them all distinctly, so that we get a count
-- of how many distinct people have been exposed to the commercial.

-- OK, let's go away and build this thing.

