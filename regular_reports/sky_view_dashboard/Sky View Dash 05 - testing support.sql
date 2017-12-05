-- Erp, in fact, no, the defaults are essential, the defaults define part of the PK,
-- we kind of have to have them around yes.

-- So occaisonally we'll have a rather new build of the Sky View Dashboard, and similar
-- to the Vespa version there are historical tables to be managed. Again we copy recent
-- values from the core vespa_analysts database into your own testing schema. This
-- script is non-essintial run-to-run, but then, these code folders aren't looked at
-- except during maintenance and upgrades and suchlike anyway.

-- Heh, a little bit more protection to not drop some important historical tables...
if object_id('vespa_SVD_log_aggregated_archive') is not null and user <> 'vespa_analysts'
   drop table vespa_SVD_log_aggregated_archive;

-- and now pull out the most recent Vespa builds:

select *
into vespa_SVD_log_aggregated_archive
from vespa_analysts.vespa_SVD_log_aggregated_archive;
-- It's not just the cheap dirty way of doing it, it also entails less column maintenance

-- But we do need the keys and indices etc:
create unique index fake_pk on vespa_SVD_log_aggregated_archive (doc_creation_date_from_6am);
-- No other indices, the whole structure is powered by the PKs.

-- Not bothering with GRANTs, because who else needs to get at testing builds?
-- We're also not getting the table defaults in place, but meh. Are there any places
-- on these tables where the defaults are important? The only default is the 'Y' on
-- the boxes returning archive, and the cull instead goes on active suscriber matching
-- 'CULL' so a null is still fine there. Sweet.

-- And yeah, don't forget to clean out all the tables in your own space afterwards
-- anyways. There will be a bunch of stuff all floating around, with naming prefixes,
-- but there will be cruft nonetheless.
