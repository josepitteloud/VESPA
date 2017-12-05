-- So we've also found some strange things turning up in the capping table with
-- things not being capped at all apparently, so we're going to have to address
-- theese too in this recalibration thingy.

---- Exhibit (1). Some events not actually being capped at all ----

select datediff(hour, viewing_starts, viewing_stops) as hours_viewed, count(1) as hits
from stafforr.vespa_daily_augs_20120514
group by hours_viewed
order by hours_viewed desc;
/* Okay, that filter on the non-viewing things has knocked out most of the long
** duration stuff, though... items with 4-6 hours of viewing? that's still too
** much and there's 100+ events about that size, look into him again...
23,2
22,1
21,1
18,1
17,2
16,1
15,1
14,1
13,1
12,1
11,3
10,4
9,4
8,3
7,2
6,23
5,15
4,55
3,158
2,322
1,5757
0,4773257
*/

-- More investigations and bugfixing...
select * from stafforr.vespa_daily_augs_20120514
where datediff(hour, viewing_starts, viewing_stops) between 4 and 6;
-- All this stuff not being flagged for capping at all, though the viewing
-- durations are huge. Do we know which buckets they're in, ntile values
-- and soforth?

select programme_trans_sk, 
count(1) as hits
from stafforr.vespa_daily_augs_20120514
where datediff(hour, viewing_starts, viewing_stops) >= 4
group by programme_trans_sk
order by hits desc;
/* Most of it lives in two programmes...
201205140000004687,17
201205150000005905,16
201205140000008599,3
201205150000007199,3
201205150000014113,3
201205150000014099,3
201205150000002438,3
201205160000014267,2
201205150000010463,2
201205150000006775,2
201205150000006859,2
*/

select * from sk_prod.vespa_epg_dim
where programme_trans_sk in (201205140000004687, 201205150000005905);
-- Another of the items with a service key but no channel name, tx_date is null,
-- tx_time is also null... 

select count(1) from sk_prod.vespa_epg_dim
where tx_time is not null;
-- 9337497  -- not null
--  111890  -- tx_time is null

-- So about 1% of the time, there's no tx_time. What are these programmes? Do they
-- ever have epg names or channels or anything?

select channel_name, epg_title, duration, count(1) as hits
from sk_prod.vespa_epg_dim
where tx_time is null
group by channel_name, epg_title, duration
order by channel_name, epg_title, duration;
-- ,,,111890
-- So yeah, no duration, no titles, don't think these are actually programmes you
-- can watch.

-- Does that account for all of them?
select programme_trans_sk, 
count(1) as hits
into #prog_lookup
from stafforr.vespa_daily_augs_20120514
where datediff(hour, viewing_starts, viewing_stops) >= 4
group by programme_trans_sk
order by hits desc;

commit;

select tx_time, channel_name, epg_title, duration, count(1) as hits, sum(pl.hits) as tot
from #prog_lookup as pl
inner join sk_prod.vespa_epg_dim as epg
on pl.programme_trans_sk = epg.programme_trans_sk
group by tx_time, channel_name, epg_title, duration
order by tot desc;
/* Heh of course not.... a bit less than half...
,,,,8,42
'220000','Classic FM','Smooth Classics','04:00:00',1,3
'100000','XFM','Ian Camfield','04:00:00',2,3
'240000','Disney Cine','Cinemagic Preview','06:00:00',1,3
'240000','Gold','Gold''s Greatest Hits','06:00:00',2,3
'260000','Magic','Magic Introduces...','04:00:00',1,3
<and 52 more rows>
*/

-- But these NULL tx_time guys are now ruled out of the capping
-- build so that's a small improvement...

-- OK, so let's see if we can find out what went wrong with the others...
select top 10 pl.programme_trans_sk
from #prog_lookup as pl
inner join sk_prod.vespa_epg_dim as epg
on pl.programme_trans_sk = epg.programme_trans_sk
where tx_time is not null;
-- An example: 201205140000004847

select datediff(hour, viewing_starts, viewing_stops) as hours_viewed, count(1) as hits
from stafforr.vespa_daily_augs_20120514
where programme_trans_sk = 201205140000004847
group by hours_viewed
order by hours_viewed desc;
-- Only two viewing events associated with this programme...

select * from stafforr.vespa_daily_augs_20120514
where programme_trans_sk = 201205140000004847
-- neither identified as needing capping...
-- long one is cb_row_id is 6463255550836115226
-- and has duration of 15k seconds...
-- it's subscriber_id 7708572

-- Into the capping construction tables:
select * from stafforr.CP2_viewing_records
where programme_trans_sk = 201205140000004847
-- the long one is in bucket ID 698

select count(1)
from stafforr.CP2_viewing_records
where bucket_id = 698
-- 159 items, which is not a whole lot for the centiles to work with, especially
-- as that's instances and not events.

select count(1)
from stafforr.CP2_event_listing
where bucket_id = 698
-- just 120 distinct events

select *
from stafforr.CP2_event_listing
where bucket_id = 698
order by viewed_duration desc
-- all the things say capped down to 488s duration events?

select *
from stafforr.CP2_event_listing
where bucket_id = 698
and subscriber_id = 7708572
-- OK, at this point, here it's being shown as requiring capping, but somehow the
-- application of caps isn't making it out to the other end or something? Why is the
-- capped flag not ending up on the instance level table?
-- adjusted_event_start_time is '2012-05-14 00:42:40.000'

select * from stafforr.CP2_viewing_records
where subscriber_id = 7708572
and adjusted_event_start_time = '2012-05-14 00:42:40.000'
order by x_viewing_start_time;
-- 4 of them, just transitioning through programme keys.

select * from CP2_capped_data_holding_pen
where subscriber_id = 7708572
and adjusted_event_start_time = '2012-05-14 00:42:40.000'
order by viewing_sdarts;
-- OK, here they're marked as cappeded_flag = 0, so somewhere in between those the
-- capping is not happening for all items... heh, we've got no more visibility
-- than that because it happens in a temporary table which no longer exists. We'll
-- make that temp table permanent, kick off another build, and see how it all fits
-- together.

-- Okay, with those modifications in place...
select datediff(hour, viewing_starts, viewing_stops) as hours_viewed, count(1) as hits
from stafforr.vespa_daily_augs_20120514
group by hours_viewed
order by hours_viewed desc;
/* Those 150 or so 3 hour events; we'd still like to do something about those I guess...
11,1
9,1
6,16
5,10
4,51
3,151
2,305
1,5759
0,4761207
*/

select programme_trans_sk, 
count(1) as hits
from stafforr.vespa_daily_augs_20120514
where datediff(hour, viewing_starts, viewing_stops) >= 4
group by programme_trans_sk
order by hits desc;
-- Yeah, a lot more evenly spread across a bunch of programmes now.

select count(1) from sk_prod.vespa_epg_dim
where tx_time is not null;
-- 112832 where tx_time is null
-- 9414261 where tx_time is not null

-- OK, so looking into these long duration things; is there anything there that's good?
select cb_row_id
into #items_to_check
from stafforr.vespa_daily_augs_20120514
where datediff(hour, viewing_starts, viewing_stops) >= 3;
-- 230 items, not that many really.

commit;

select vr.*
from CP2_viewing_records as vr
where vr.cb_row_id in (select cb_row_id from #items_to_check);
-- they all have buckets... looks like... the first programme in a long event with a
-- not particularly populated bucket and the first show gets asigned the end time of
-- the programme or whatever? does it still do that?

select bucket_id, count(1) as hits
into #buckets_to_check
from CP2_viewing_records as vr
where vr.cb_row_id in (select cb_row_id from #items_to_check)
group by bucket_id;
-- 54 buckets...

commit;
create unique index fake_pk on #buckets_to_check (bucket_id);
commit;

-- OK, so how many items are there in each of these buckets?
select bucket_id, count(1) as hits
into #bucket_counts
from stafforr.CP2_event_listing
where bucket_id in (select bucket_id from #buckets_to_check)
group by bucket_id;

select * from #bucket_counts
-- OK, several of them have thousands, some of them have tens of thousands. These
-- ntlies should be fine? Well, what else are the ntiles grouping over? is there
-- something more fine-grained than buckets which governs the ntiles? I think there
-- is? We don't directly track the ntile groups... Yeah, leave this there, continue
-- with the BARB data alignment. It's notbad enough now, we can do something.


