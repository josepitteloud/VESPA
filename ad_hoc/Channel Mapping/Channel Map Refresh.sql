

-- Refreshing BackUp...

/* CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES */

delete from vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES;
commit;

go


/* CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES */

delete from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES;
commit;

go


/* CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES */

delete from vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES;
commit;

go



/* CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB */

delete from vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB;
commit;

go



/* CHANNEL_MAP_DEV_SERVICE_KEY_BARB */

delete from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB;
commit;

go



/* CHANNEL_MAP_ARC_SERVICE_KEY_BARB */

delete from vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_BARB_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_BARB_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_BARB;
commit;

go



/* CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK */

delete from vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK;
commit;

go



/* CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK */

delete from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK;
commit;

go



/* CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK */

delete from vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK_BKP;
commit;

insert	into vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK_BKP
select	*
from	vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK;
commit;

go


-- Cleaning base tables...
delete from vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES;
delete from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES;
delete from vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES;
delete from vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB;
delete from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB;
delete from vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_BARB;
delete from vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK;
delete from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK;
delete from vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK;


-- Refreshing tables content...

insert	into vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES
select	*
from	neighbom.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES;
commit;

insert	into vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES -- OK
select	*
from	neighbom.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES;
commit;

insert	into vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES -- OK
select	*
from	neighbom.CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES;
commit;

insert	into vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB -- OK
select	*
from	neighbom.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB;
commit;

insert	into vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB -- OK
select	*
from	neighbom.CHANNEL_MAP_DEV_SERVICE_KEY_BARB;
commit;

insert	into vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_BARB -- OK
select	*
from	neighbom.CHANNEL_MAP_ARC_SERVICE_KEY_BARB;
commit;

insert	into vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK -- OK
select	*
from	neighbom.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK;
commit;

insert	into vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK -- OK
select	*
from	neighbom.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK;
commit;

insert	into vespa_analysts.CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK -- OK
select	*
from	neighbom.CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK;
commit;