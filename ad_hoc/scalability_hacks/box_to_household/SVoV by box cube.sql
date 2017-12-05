/******************************************************************************
**
**      Project Vespa: SVoV by box cube
**
** See also, as in,
**      http://rtci/vespa1/Single%20view%20of%20viewing.aspx
**
** This is the next of the attempts to build the SVoV reports. It's changed a
** lot since it was anything to do with scalability or household metrics over
** multirooom accounts. Now we're grabbing all the viewing data for all the
** accounts (multiroom or not) and also not filtering for channel.
**
** This has led to the recommendation that the way to do this is to cube from
** the base daily tables. This is a total departure from how we were trying to
** do everything from the events view, or at least, a standardised coagulation
** of events tables. Now we're putting essentially all of the query into some
** dynamic SQL and bouncing it over a loop.
**
** Additionally, a large number of the metrics are only going to be available
** in excel after the pivot, since they're not conserved through averages or
** other such operations after they're grouped. So we're not actually building
** any of the metrics in SQL. And since we're splitting it up by box, all the
** chains have dissappeared, since they only exist at the level of a household.
** Maybe we could put the chains back in? But then, it'd be in a separate part
** of the table, and wouldn't be subject to the same filters (because they need
** to be rebuilt for each round of constraints...)
**
** Plan:
**  1. Figure out exactly which data elements we're going to need to populate
**      our .ppt report thing - Update: all manner of distinct-ness is out of
**      scope at the moment.
**  2. Pull out just those numbers
**
** Slides:
**  a. Do households watch the same content on different boxes?
**      - For one day: MR accounts only, Channel! duration, box rank, hh box count
**      - pick three popular channels and do the 5x5
**      - advanced; pull out number of records which overlap broadcast time & channel
**  b. Are boxes watched at the same time or different times?
**      - Can I do this without chains? maybe just group by the ToD, or the viewing hour
**      - again, pick two or three hours and build the 5x5 for each?
**      - advanced; how many events overlap with consumtion time within a HH?
** OK, so that seems like all we need? the rest is documentation? yay, .pptx :-/
**
** Other things to implement if we rebuild (otherwise, for next time):
**  1. Have to replace uncommon genres with Other; nominally Specialist,
**      undefined and blank, maybe also music & radio. So as to not have lots
**      of tiny little irrelvant indestinguishable things cluttering up the
**      pie charts etc.
**
******************************************************************************/

/********** Setp 1: assemble box numbering and other account level data **********/

-- Assemble box numbering from the boxes returning data ever:
select
    sls.account_number -- still need account number to link lifestage
    ,sls.subscriber_id
    ,convert(tinyint, 0) as boxes_in_house
    ,case
        when service_instance_type = 'P' then 1
        when service_instance_type = 'S' then 1 + -- to offset the P from the S
            rank() over (partition by sls.account_number, service_instance_type order by request_dt, subscriber_id)
        else -1
        end as box_rank
    ,convert(bit, 0) as PVR
    ,'SD' as box_type
    ,convert(varchar(40), null) as lifestage
into SVoV_box_lookup
from sk_prod.VESPA_STB_LOG_SNAPSHOT as sls
inner join sk_prod.vespa_subscriber_status as vss
on sls.account_number = vss.account_number -- not required, but the other join condition is pants so this helps
and sls.subscriber_id = convert(bigint, vss.card_subscriber_id)
and panel_id = 5 and result = 'Enabled';
-- 303642
create unique index fake_pk     on SVoV_box_lookup (subscriber_id);
create        index for_joining on SVoV_box_lookup (account_number);

commit;

-- Pack on box counts by household (are we going enabled or returning? Returning.)
select account_number,
    max(box_rank) box_count_from_reporting
into #boxes_per_account
from SVoV_box_lookup
group by account_number;

-- Doing it this way round to avoids with primary boxes not reporting
-- but secondary boxes are reporting

commit;
create unique index fake_pk on #boxes_per_account (account_number);
commit;

update SVoV_box_lookup
set boxes_in_house = box_count_from_reporting
from SVoV_box_lookup inner join #boxes_per_account
on SVoV_box_lookup.account_number = #boxes_per_account.account_number;

-- QA: should be empty.
select count(1) as boxes, count(distinct account_number) as households
from SVoV_box_lookup where boxes_in_house = 0;
-- 0    0 - awesome.

select boxes_in_house, box_rank , count(1) as hits
from SVoV_box_lookup
group by boxes_in_house, box_rank
order by boxes_in_house, box_rank
-- goes as far as 6. Sure.

-- What else is interesting is how much this stuff reports in:
select enabled_boxes, count(1) as households
from (
    select account_number, count(1) as enabled_boxes
    from sk_prod.VESPA_SUBSCRIBER_STATUS
    where result = 'Enabled'
    group by account_number
) as t
group by enabled_boxes
order by enabled_boxes;
/* enabled_boxes        households
1       346734
2       127398
3       22401
4       4065
5       641
6       140
7       45
8       13
9       3
11      3
12      1
*/

-- 

/********** 1.b) box & account details From sky data **********/

-- OK so we're taking a different track from Julie for expediency...
select distinct csh.account_number,
    csh.service_instance_id
into #box_lookup_midpoint
from SVoV_box_lookup inner join sk_prod.cust_subs_hist as csh
on SVoV_box_lookup.account_number = csh.account_number
where csh.effective_from_dt <= '2011-07-01'
     and csh.effective_to_dt   >  '2011-07-01'
     and csh.subscription_sub_type in ('DTV Primary Viewing', 'DTV Extra subscription')
;

commit;
create unique index fake_pk on #box_lookup_midpoint (service_instance_id);
commit;

-- If this guy is super slow, I could do this in two steps, with the pulls into the
-- cust_set_top_box and cust_service_instance tables separately.
select distinct
    convert(bigint, csi.si_external_identifier) as subscriber_id
    ,stb.x_pvr_type
    ,stb.x_box_type
into #box_types
from #box_lookup_midpoint as blm
inner join sk_prod.cust_set_top_box as stb
    on blm.service_instance_id = stb.service_instance_id and active_box_flag = 'Y'
inner join sk_prod.cust_service_instance as csi
    on csi.src_system_id       = blm.service_instance_id
;
-- Active_box_flag is not used by julie... but it should be? well, otherwise
-- there were a bunch of duplicates. But this is giving us the most recent
-- cut, not the historical cut of what was happening at the profiling date.
-- So... maybe we just leave HD out of our analysis?

-- 236792
commit;
create index fake_pk on #box_types (subscriber_id);
-- Yeah, that took 40 minutes.

-- defaults are already in place so...
update SVoV_box_lookup
set PVR = 1
from SVoV_box_lookup inner join #box_types as bl
on SVoV_box_lookup.subscriber_id = bl.subscriber_id
and x_pvr_type like '%PVR%';
commit;
update SVoV_box_lookup
set box_type = 'HD'
from SVoV_box_lookup inner join #box_types as bl
on SVoV_box_lookup.subscriber_id = bl.subscriber_id
and x_box_type like '%HD%';
commit;
-- See, those are basically instant. Why the others take tens of minutes?

-- And now stitch on lifestage; first condense everything up to account level because
-- that's where lifestage lives
select
    distinct account_number
    ,convert(bigint, null) as cb_key_household
    ,convert(varchar(50), null) as ilu_hhlifestage
into SVoV_lifestage_patch
from SVoV_box_lookup;

commit;
create unique index fake_pk on SVoV_lifestage_patch (account_number);

update SVoV_lifestage_patch
set cb_key_household = sav.cb_key_household
from SVoV_lifestage_patch inner join sk_prod.cust_single_account_view as sav
on SVoV_lifestage_patch.account_number = sav.account_number;

commit;
create index for_joining on SVoV_lifestage_patch (cb_key_household);
-- Now that's in, index that, and then pull out the lifestages:
update SVoV_lifestage_patch
set ilu_hhlifestage = case ilu.ilu_hhlifestage when  1 then '18-24 ,Left home'
                                              when  2 then '25-34 ,Single (no kids)'
                                              when  3 then '25-34 ,Couple (no kids)'
                                              when  4 then '25-34 ,Child 0-4'
                                              when  5 then '25-34 ,Child5-7'
                                              when  6 then '25-34 ,Child 8-16'
                                              when  7 then '35-44 ,Single (no kids)'
                                              when  8 then '35-44 ,Couple (no kids)'
                                              when  9 then '45-54 ,Single (no kids)'
                                              when 10 then '45-54 ,Couple (no kids)'
                                              when 11 then '35-54 ,Child 0-4'
                                              when 12 then '35-54 ,Child 5-10'
                                              when 13 then '35-54 ,Child 11-16'
                                              when 14 then '35-54 ,Grown up children at home'
                                              when 15 then '55-64 ,Not retired - single'
                                              when 16 then '55-64 ,Not retired - couple'
                                              when 17 then '55-64 ,Retired'
                                              when 18 then '65-74 ,Not retired'
                                              when 19 then '65-74 ,Retired single'
                                              when 20 then '65-74 ,Retired couple'
                                              when 21 then '75+   ,Single'
                                              when 22 then '75+   ,Couple'
                                              else         'Unknown' end
from SVoV_lifestage_patch inner join sk_prod.ilu as ilu
on SVoV_lifestage_patch.cb_key_household = ilu.cb_key_household;
-- Not quite using the same cb_row_id approach that Julie did, but household
-- lifestages should be consistent across household keys...

-- Push those back onto the main lookup:
update SVoV_box_lookup
set lifestage = coalesce(ilu_hhlifestage, 'Unknown')
from SVoV_box_lookup left join SVoV_lifestage_patch as sao
on SVoV_box_lookup.account_number = sao.account_number;

drop table SVoV_lifestage_patch;

/********** 1.c) QA on lookup before we pull out data **********/

select distinct boxes_in_house, box_rank
from SVoV_box_lookup
order by boxes_in_house, box_rank;
-- all looks good.

select account_number, box_rank, count(1)
from SVoV_box_lookup
group by account_number, box_rank
having count(1) > 1;
-- Two accounts have multiple primary boxes. Fine.

select count(1) from SVoV_box_lookup where box_rank > 5;
-- 4. Let's scale everything that's >5 back to 5, and just say
-- that 5 in either case means 5 or more.

update SVoV_box_lookup set box_rank = 5 where box_rank > 5;
-- 4 rows affected
update SVoV_box_lookup set boxes_in_house = 5 where boxes_in_house > 5;
-- 22 rows affected
commit;
-- Those guys are probably not enough to have any noticable effect on the totals though.

select distinct boxes_in_house, box_rank
from SVoV_box_lookup
order by boxes_in_house, box_rank;
-- Okay, yeah, they're just the 15 we want.

/********** 2. Control totals over enablement etc **********/

-- Does this affect the aggregation? Should we be spending much time on it at all?
-- Hahahno, enablement probably some other stream, build all this stuff so it geos
-- off the number of reporting boxes per household. Reportingness currently goes
-- at less than 50% of enablement, and that's a completely separate thing to SVoV
-- (probably). Update: all enablement discussions are out; enablement and multibox
-- together are just a huge headache :(

select boxes_in_house, box_rank, count(1) as households
from SVoV_box_lookup
group by boxes_in_house, box_rank
order by boxes_in_house, box_rank;
-- This gives the number of boxes in each cell, but only for the overall cut. Putting
-- Live_Viewing, broadcast_day, broadcast_time in the cube means it's not a partition
-- on subscriberID, so we can't easily sum the cells in Excel :/
/* boxes_in_house       box_rank        households
1       1       190297
2       1       42881
2       2       56231
3       1       3499
3       2       4434
3       3       4434
4       1       363
4       2       434
4       3       434
4       4       434
5       1       29
5       2       42
5       3       42
5       4       42
5       5       46
*/
-- Yeah, all of these guys have the dip where primary boxes are not reporting but we
-- know it's at least a 3 box household because we have 2 secondary boxes reporting.

/********** 3. Cubing data one daily at a time... **********/

-- Great, these capping boundry conditions are now completely different to the previous
-- builds, they're using only durations. This construction is no good for building chains
-- then...

create variable @pull_date date;
create variable @final_date date;

create variable @big_SQL_hurg varchar(4000);

set @pull_date = '2011-07-01';
set @final_date = '2011-07-8'; -- pull out 1 week of data...

-- Static capping of 2 hours as items come off the daily table...
set @big_SQL_hurg ='insert into SVoV_cube_build
select
    ev.adjusted_event_date as consumption_date
    ,bl.boxes_in_house
    ,bl.box_rank
    ,bl.lifestage
    ,bl.PVR
    ,bl.box_type as HD_box
    ,case when epg.channel_name like ''%HD%'' then ''HD'' else ''SD'' end as HD_channel
    ,case when ev.play_back_speed is null then ''Live'' else ''Playback'' end as Live_viewing
    ,epg.genre_description
    ,datepart(hh, case when recorded_time_UTC is null then x_viewing_start_time else dateadd(ss, x_duration_since_last_viewing_event, recorded_time_UTC) end) as broadcast_time
    ,datepart(weekday, case when recorded_time_UTC is null then x_viewing_start_time else dateadd(ss, x_duration_since_last_viewing_event, recorded_time_UTC) end) as broadcast_day
    ,count(1) as event_count
    -- An awesome query in here with the various cases for recorded, not recorded, capped, not capped, cap spilling into program...
    ,round(sum (case -- so it turns out that for durations, live and playback can both be treated with the same cases...
                when x_event_duration <= 7200 -- no capping in play; all subsequent cases get capped
                    then x_programme_viewed_duration
                when x_duration_since_last_viewing_event > 7200 -- when the event gets capped before this show starts...
                    then 0
                when x_duration_since_last_viewing_event + x_programme_duration < 7200 -- when the event gets capped and this cap limit is after the end of the show...
                    then x_programme_viewed_duration
                else -- when the cap ends midway through this show
                    7200 - x_duration_since_last_viewing_event
          end ) / 60.0 / 60.0, 2) as total_duration_hours
from sk_prod.VESPA_STB_PROG_EVENTS_££YYYYMMDD££ as ev
inner join sk_prod.vespa_epg_dim as epg on ev.programme_trans_sk = epg.programme_trans_sk
inner join SVoV_box_lookup as bl on ev.subscriber_id = bl.subscriber_id
where (ev.play_back_speed is null or ev.play_back_speed = 2) -- NULL means live, 2 is timeshifted
and ev.x_programme_viewed_duration > 0
and ev.Panel_id = 5
and ev.x_type_of_viewing_event <> ''Non viewing event''
group by
    consumption_date
    ,bl.boxes_in_house
    ,bl.box_rank
    ,bl.lifestage
    ,bl.PVR
    ,HD_box
    ,HD_channel
    ,Live_viewing
    ,epg.genre_description
    ,broadcast_time
    ,broadcast_day
;';
-- For some reason this is only working for me inside p5x1 on the SQL
-- interactive. Reasonably annoying :/

drop table SVoV_cube_build;
create table SVoV_cube_build (
    consumption_date        date
    ,boxes_in_house         tinyint
    ,box_rank               tinyint
    ,lifestage              varchar(50)
    ,PVR                    bit
    ,HD_box                 varchar(2)
    ,HD_channel             varchar(2)
    ,Live_viewing           varchar(10)
    ,genre_description      varchar(30)
    ,broadcast_time         tinyint     -- 0 to 23
    ,broadcast_day          tinyint     -- 1 to 7? 0 to 6? we'll find out.
    ,event_count            bigint
    ,total_duration_hours   decimal(16,2)
);

while @pull_date < @final_date
begin
    execute(replace(@big_SQL_hurg, '££YYYYMMDD££', dateformat(@pull_date,'yyyymmdd')))
    
    set @pull_date = dateadd(day, 1, @pull_date)
    commit
end;
-- guy is going with what should be good build.

select count(1) from  SVoV_cube_build;
-- 1,036,516

-- Okay, and as one of our things is weekday and we only ran it over one week, there's
-- no additional gropuing need to be done. Still 1M rows though :(

delete from SVoV_cube_build where total_duration_hours = 0;
-- because now we've got cases where small samples get rounded down into nothing
-- and we don't actually care about them.

select count(1) from SVoV_cube_build;
-- 910710
-- Okay, that would have sped things up quite a bit.

-- For old builds with multiple weeks of data:
drop table #SVoV_25_box_cube;
select 
    boxes_in_house
    ,box_rank
    ,lifestage
    ,PVR
    ,HD_box
    ,HD_channel
    ,Live_viewing
    ,genre_description
    ,broadcast_time
    ,broadcast_day
    ,sum(event_count) as event_count
    ,sum(total_duration_hours) as total_duration
into #SVoV_25_box_cube
from SVoV_cube_build
group by
    boxes_in_house
    ,box_rank
    ,lifestage
    ,PVR
    ,HD_box
    ,HD_channel
    ,Live_viewing
    ,genre_description
    ,broadcast_time
    ,broadcast_day
;
-- Whole week of data in there, though it does span wimbledon...

-- Still not sure what to do about the SD-HD thing. Hopefully it's not a big
-- number coming out?

-- We've got total duration and we've also got event count. This means we can get number
-- of events per hour of viewing, eg, a measure of impatience.

-- Also important: control totals for things that do partition our
-- account space. Wait, no, that's all in section 6.

/********** 4. The same but by consumption time: **********/
-- Not expecting this to be so different, but it might be?

create variable @pull_date date;
create variable @final_date date;

create variable @big_SQL_hurg varchar(4000);

set @pull_date = '2011-07-01';
set @final_date = '2011-07-8'; -- pull out 1 week of data...

-- Static capping of 2 hours as items come off the daily table...
set @big_SQL_hurg ='insert into SVoV_cube_build_consumption
select
    ev.adjusted_event_date as consumption_date
    ,bl.boxes_in_house
    ,bl.box_rank
    ,bl.lifestage
    ,bl.PVR
    ,bl.box_type as HD_box
    ,case when epg.channel_name like ''%HD%'' then ''HD'' else ''SD'' end as HD_channel
    ,case when ev.play_back_speed is null then ''Live'' else ''Playback'' end as Live_viewing
    ,epg.genre_description
    ,datepart(hh, case when recorded_time_UTC is null then x_viewing_start_time else dateadd(ss, x_duration_since_last_viewing_event, adjusted_event_start_time) end) as consumption_time
    ,datepart(weekday, case when recorded_time_UTC is null then x_viewing_start_time else dateadd(ss, x_duration_since_last_viewing_event, adjusted_event_start_time) end) as consumption_day
    ,count(1) as event_count
    -- An awesome query in here with the various cases for recorded, not recorded, capped, not capped, cap spilling into program...
    ,round(sum (case -- so it turns out that for durations, live and playback can both be treated with the same cases...
                when x_event_duration <= 7200 -- no capping in play; all subsequent cases get capped
                    then x_programme_viewed_duration
                when x_duration_since_last_viewing_event > 7200 -- when the event gets capped before this show starts...
                    then 0
                when x_duration_since_last_viewing_event + x_programme_duration < 7200 -- when the event gets capped and this cap limit is after the end of the show...
                    then x_programme_viewed_duration
                else -- when the cap ends midway through this show
                    7200 - x_duration_since_last_viewing_event
          end ) / 60.0 / 60.0, 2) as total_duration_hours
from sk_prod.VESPA_STB_PROG_EVENTS_££YYYYMMDD££ as ev
inner join sk_prod.vespa_epg_dim as epg on ev.programme_trans_sk = epg.programme_trans_sk
inner join SVoV_box_lookup as bl on ev.subscriber_id = bl.subscriber_id
where (ev.play_back_speed is null or ev.play_back_speed = 2) -- NULL means live, 2 is timeshifted
and ev.x_programme_viewed_duration > 0
and ev.Panel_id = 5
and ev.x_type_of_viewing_event <> ''Non viewing event''
group by
    consumption_date
    ,bl.boxes_in_house
    ,bl.box_rank
    ,bl.lifestage
    ,bl.PVR
    ,HD_box
    ,HD_channel
    ,Live_viewing
    ,epg.genre_description
    ,consumption_time
    ,consumption_day
;';
-- For some reason this is only working for me inside p5x1 on the SQL
-- interactive. Reasonably annoying :/

drop table SVoV_cube_build_consumption;
create table SVoV_cube_build_consumption (
    consumption_date        date
    ,boxes_in_house         tinyint
    ,box_rank               tinyint
    ,lifestage              varchar(50)
    ,PVR                    bit
    ,HD_box                 varchar(2)
    ,HD_channel             varchar(2)
    ,Live_viewing           varchar(10)
    ,genre_description      varchar(30)
    ,consumption_time       tinyint     -- 0 to 23
    ,consumption_day        tinyint     -- 1 to 7? 0 to 6? we'll find out.
    ,event_count            bigint
    ,total_duration_hours   decimal(16,2)
);

while @pull_date < @final_date
begin
    execute(replace(@big_SQL_hurg, '££YYYYMMDD££', dateformat(@pull_date,'yyyymmdd')))
    
    set @pull_date = dateadd(day, 1, @pull_date)
    commit
end;

select count(1) from  SVoV_cube_build_consumption;
-- 733487

delete from SVoV_cube_build_consumption where total_duration_hours = 0;

select count(1) from SVoV_cube_build_consumption;
-- 686942

-- Now extract in Excel via ODBC! (Because the data set is too large for other
-- migration methods.)

/********** 5. Getting control totals for how many programmes overlap **********/

-- This guy could be based off things we built ages ago which showed how
-- MR usage overlapped across boxes of the same house. See if we can dig
-- out that presentation, find where the code went, see if we can cludge
-- it into the build easily.

-- Yeah, okay, so "viewing_overlappage.sql" has now been integrated to
-- a large degree into this process, though there's nothing feeding back
-- into here yet.

/********** 6. Now for starting on the whole house level metrics:... **********/

-- And so yeah, for the chains we want to limit everything to those households that
-- have multiple reporting boxes. We can control the pull via populating the same
-- profiling table. So... what do we do about households that don't have a primary
-- box but have multiple secondary boxes? I think for clarity, we just clip out at
-- this point everything that doesn't have a primary box. Which turns the filter into
-- a super easy thing. Except... what if one of the secondary boxes has HD or PVR?
-- We still want to know that functionality exists. OK, slightly more complicated
-- but still not so bad off really.

drop table SVoV_MR_account_lookup;
select
    account_number -- still need account number to link lifestage
    ,max(boxes_in_house) as boxes_in_household
    ,max(PVR) as has_PVR
    ,min(box_type) as has_HD -- 'HD' is smaller than 'SD'
    ,min(lifestage) as lifestage
into SVoV_MR_account_lookup
from SVoV_box_lookup
where boxes_in_house > 1
group by account_number
having min(box_rank) = 1; -- ensures that primary box exists

create unique index fake_pk on SVoV_MR_account_lookup (account_number);

-- If we're doing the chain stuff, then... I guess we just build a big list of chains
-- and then slap the profiling on when we go into the cube.
select boxes_in_household, count(1) as household_count
from SVoV_MR_account_lookup
group by boxes_in_household
order by boxes_in_household;
/* Yup, these are exactly what we expect.
2       42881
3       3499
4       363
5       29
*/

-- Okay, and the control totals for things that are actually partitions of
-- the account space:
select boxes_in_household, has_PVR, has_HD, lifestage, count(1) as household_count
from SVoV_MR_account_lookup
group by boxes_in_household, has_PVR, has_HD, lifestage
order by boxes_in_household, has_PVR, has_HD, lifestage;
-- So... wait... these differ from the other totals we pulled into Excel
-- because these are MR only, and filtered to PB reporting households,
-- and clump 5+ households into 5.

-- An actual control table build then:
drop table SVoV_account_lookup;
select
    account_number -- still need account number to link lifestage
    ,max(boxes_in_house) as boxes_in_household
    ,max(PVR) as has_PVR
    ,min(box_type) as has_HD -- 'HD' is smaller than 'SD'
    ,min(lifestage) as lifestage
into SVoV_account_lookup
from SVoV_box_lookup
group by account_number;

create unique index fake_pk on SVoV_account_lookup (account_number);

-- Kind of, except that for our control totals this skips the thing
-- about whether or not the primary box is reporting, so, our actual
-- partitioned control totals will be:
select
    boxes_in_house
    ,box_rank
    ,PVR as has_PVR
    ,box_type as has_HD
    ,lifestage
    ,count(1) as box_count
from SVoV_box_lookup
group by boxes_in_house, box_rank, PVR, box_type, lifestage
order by boxes_in_house, box_rank, PVR, box_type, lifestage;
-- Yeah, there's slight corruption on number of boxes where we limited
-- everything to 5, but this only affects the box rank of only 4 boxes
-- in our entire data set, so, whatever. Not making any difference when
-- reporting 3 significant figures over averages in a population of 300k
-- so yeah.

/********** 7. So Excel is failing it's own control totals... **********/

select sum(event_count),
 sum(total_duration_hours)
  from SVoV_cube_build;
-- 47477840        9897141.76

select count(1),
count(distinct account_number)
 from SVoV_box_lookup;
-- 303642  251436 - okay

select sum(total_duration_hours) from SVoV_cube_build;
-- 9897141.76

select 9897141.76 / 303642 / 7.0

-- 4.6? so... why has excel given us 9.6 etc? WTF Excel?
-- well, at least 4.6 is more conservative viewing totals :/
-- Looks like Excel has failed some refreshes, time to restart
-- all of the insight stuff :/
