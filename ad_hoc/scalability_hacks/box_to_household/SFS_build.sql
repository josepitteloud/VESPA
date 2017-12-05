/******************************************************************************
**
**    Project Vespa: Scalability investigations: box to household SFS method
**
** For additional information and background on scaling up to household view,
** refer to the RTCI wiki page at
**
**  http://rtci/vespa1/Household%20aggregation%20Daily%20chained%20viewing.aspx
**
** This script is for the Spot-Free Statistical method, a variation on the
** overly straightforward Account/Programme statistical method. Similarities
** in result structure include dropping all the timestamp information and
** guaranteeing only one record per household per programme.
**
** Except that's not quite true, because programmes are broken into blocks
** by spots, and SFS gives one record per household per programme block. SFS
** was conceived to offset the disadvantages of ACP, where you can't actually
** tell which bits of the show were watched. With SFS, you get total viewing
** time on each of the blocks of the programme, and each of the ad spots in
** between. That's right! SFS elevates spots to the level of programmes, and
** provides more of each in the household view.
**
** But wait... that means we either need a new key to distinguish the various
** instances of adds shown, or we summarise into household viewing of spots
** per day. Keeping everything around entail a huge data set increases; compare
** with CHD and you go from maybe 6 chains per program to 3 blocks and then
** another 30 records for each household and spot. We're looking at in excess
** of 50GB per day.
**
** Upside: a bunch of the advertising processing is done beforehand.
** Downside: identifying continuous periods of watching is tricky. But then,
**      contiguity disappeared when we aggregate to household level anyways.
** Other downside: if we don't have spot logs for a channel, how do we proceed?
**      do we want different processing based on the absence of a data source?
**      would we rather break all shows into say 1/4 hour blocks and juggle
**      those to make the boundaries line up on spots (and promos?) when it's
**      applicable, giving slightly better visibility over longer shows and
**      for channels we don't have spot data for?
**
** Yeah, makes it feel like we're trying to solve a problem (no visibility
** inside the aggregate for spots and MBM) by cludging it into a different
** problem (the specific minutes of interest). Not sure that's the best way
** to go about it. Meditation.
**
** Also: how much of this is a whole lot easier if we start from the chains?
** um, unfortunately not much, since the spots will turn up midway through a
** chain and we've already aggregated out things like how many boxes tuned in
** at any particular point, so that won't work out so well, won't get the
** detail we'd like.
**
** So, yeah. First off, let's try to estimate the scale of data we'll invoke.
**
** Plan:
**  1/ Estimate data size for this plan from spot logs
**  2/ Find out if we have spots that cover July 16th (but still only some channels)
**  3/ Jury rig some spots onto the 16th of July
**
******************************************************************************/

select
        convert(varchar(10), break_start_time, 103) as spot_day,
        barb_code,
        count(1) as total_spots,
        count(distinct clearcast_commercial_number) as distinct_ads,
        max(spot_sequence_id) as biggest_spot_sequence,
        round(avg(spot_sequence_id) * 2,2) as avg_spot_length_or_something
from vespa_analysts.spots_all
group by spot_day, barb_code;
-- Some averages and pivots in excel, we've got 8 chanels for 10 days,
-- total of 30857 total spots and 13894 for the distinct option. Average
-- spot length seems to be about 8, or near enough for this kind of
-- guesswork. So: making some awful awful assumptions about homogenious
-- ad distribution over the day, then for the daily table... we can take
-- the total viewing time for each household, take that as a portion of
-- the day, multiply that by 30857 / 8 = 4000, and that's the number of
-- extra records we get. Oh, add a factor of two to the original sample
-- because now the programmes are in chunks too. So:

-- Doing this from the chains because we'd distinct-out the viewship
-- anyways...
select
        count(1)
        ,count(distinct account_number) as households
        ,(sum(total_live_seconds) + sum(total_playback_seconds)) / 60.0 / 60 / 24.0 as total_viewing_in_days
 from stb_2_hh_viewing_chains;
-- 9469531 173147  91372.89

-- so... the extra number of adds we need to tack in is... wait.. what
-- it all boild down to is that for each hour of viewing, there's new
-- records ~ 4000 / 24. Therefore total number of records is...
select 9469531 * 4000.0 / 24
-- 1578255166.667
-- and so at 90bytes / row, we end up at...
select 9469531 * 4000.0 / 24 * 90 / 1024 / 1024.0 / 1024
-- 130GB. per day. yeah, not really managable.

-- hahahah, yeah, discontinue this guy for the moment then.

-- Oh, wait, that's only on the July 16th sample. That still has to scale
-- up to the 75% of 5m... the ultimate factor of increase is going to be...
select 4000 / 24;
-- x166 per hour of viewing by household. Wait, how many records do we
-- currently have per hour of viewing?

select 9469531 / 91372.89 * 24;
-- 2500 or so. So... multiply that by... no... silly...

-- Didn't acount for having 10 days worth of logs. So, the number of extra
-- rows per hour of TV is 400 / 24 = 16. Which is completely reasonable.
-- The total data set inflates by a factor of 16. But it's over the ACP
-- method, which was a bit smaller than the chains. If we use the distinct
-- one, then... it's a bit less than half. But we loose that visibility again.