-- Yeah, this doesn't work right now, the defaults turn out to be essential...

-- So occaisonally we'll have a rather new build of the Operational Dashboardand we'll
-- want to be able to test it in an isolated schema. This script helps this effort by
-- preparing the historical structures and copying over the most recent values from
-- the core vespa_analysts database into whichever testing schema you're using. This
-- script is non-essintial run-to-run, but then, these code folders aren't looked at
-- except during maintenance and upgrades and suchlike anyway.

-- Heh, a little bit more protection to not drop some important historical tables...
if object_id('vespa_OpDash_log_aggregated_archive') is not null and user <> 'vespa_analysts'
   drop table vespa_OpDash_log_aggregated_archive;
if object_id('vespa_OpDash_boxes_returning_archive') is not null and user <> 'vespa_analysts'
   drop table vespa_OpDash_boxes_returning_archive;
if object_id('vespa_OpDash_new_joiners_RTMs') is not null and user <> 'vespa_analysts'
   drop table vespa_OpDash_new_joiners_RTMs;

-- and now pull out the most recent Vespa builds:

select *
into vespa_OpDash_log_aggregated_archive
from vespa_analysts.vespa_OpDash_log_aggregated_archive;
-- It's not just the cheap dirty way of doing it, it also entails less column maintenance

select *
into vespa_OpDash_boxes_returning_archive
from vespa_analysts.vespa_OpDash_boxes_returning_archive;

select *
into vespa_OpDash_new_joiners_RTMs
from vespa_analysts.vespa_OpDash_new_joiners_RTMs;

-- But we do need the keys and indices etc:
create unique index fake_pk on vespa_OpDash_log_aggregated_archive (doc_creation_date_from_9am);
create unique index fake_pk on vespa_OpDash_boxes_returning_archive (subscriber_id);
create unique index fake_pk on vespa_OpDash_new_joiners_RTMs (rtm, date_archived);
-- No other indices, the whole structure is powered by the PKs.

-- Not bothering with GRANTs, because who else needs to get at testing builds?
-- We're also not getting the table defaults in place, but meh. Are there any places
-- on these tables where the defaults are important? The only default is the 'Y' on
-- the boxes returning archive, and the cull instead goes on active suscriber matching
-- 'CULL' so a null is still fine there. Sweet. Oh, wait, no, the defaults also populate
-- the not-null columns withthe people and time reporting, plus it's in the PK for the
-- new joiners thing, so yeah, do need the default somehow, is a little broken...

-- And yeah, don't forget to clean out all the tables in your own space afterwards
-- anyways. There will be a bunch of stuff all floating around, with naming prefixes,
-- but there will be cruft nonetheless.
