-- Extraction of control totals in support of household daily aggregation work

-- For the raw data:
select
        count(1) as total_records,
        count(distinct subscriber_id) as box_count,
        count(distinct account_number) as household_count,
        floor(sum(x_programme_viewed_duration) / 60.0 / 60.0 / 24.0) as total_viewing_in_days
from sk_prod.VESPA_STB_PROG_EVENTS_20110716
where panel_id = 5 -- want Vespa viewing events
and (play_back_speed is null or play_back_speed = 2)
and x_programme_viewed_duration > 0
and x_type_of_viewing_event <> 'Non viewing event';
-- 12455720        207399  173219  118874

-- Once capping is applied:
select
        count(1) as total_records,
        count(distinct subscriber_id) as box_count,
        count(distinct account_number) as household_count,
        floor(sum(x_programme_viewed_duration) / 60.0 / 60.0 / 24.0) as total_viewing_in_days
from stb_2_hh_events_vespa_capped;
-- 9518200 207293  173147  91372

-- Capping and Primary Box Only filter:
select
        count(1) as total_records,
        count(distinct c.subscriber_id) as box_count,
        count(distinct c.account_number) as household_count,
        floor(sum(c.x_programme_viewed_duration) / 60.0 / 60.0 / 24.0) as total_viewing_in_days
from stb_2_hh_events_vespa_capped as c
inner join sk_prod.vespa_stb_log_snapshot as ls
on c.subscriber_id = ls.subscriber_id
and upper(ls.service_instance_type) = 'P';
-- 6910056 136488  136488  60613

-- Then with CHD applied:
select
        count(1) as total_records,
        null as box_count,
        count(distinct account_number) as household_count,
        floor(sum(total_live_seconds + total_playback_seconds) / 60.0 / 60.0 / 24.0) as total_viewing_in_days
from stb_2_hh_viewing_chains;
-- 9469531         173147  91372

-- Then the first ACP build:
select
        count(1) as total_records,
        null as box_count,
        count(distinct account_number) as household_count,
        floor(sum(total_live_seconds + total_playback_seconds) / 60.0 / 60.0 / 24.0) as total_viewing_in_days
from stb_2_hh_demo_aggregates;
-- 6169340         173147  91372

-- Now for SFS: (not yet in play though)

