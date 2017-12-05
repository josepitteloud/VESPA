/******************************************************************************
**
**  PROJECT VESPA - FILTERING FOR PRE-RAMP-UP BOXES
**
** For technical reasons, we're not reporting on the ramp up and we're handling
** this via blocking out everything except boxes we want. This time around we
** also include all the boxes that are starting to ramp up, and also indicate
** which of the boxes we expect to see report back and which we don't, in order
** to be happier about the subscriber IDs lining up as we expected. For more
** information, see the brief and tracking item at:
**
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=65
**
******************************************************************************/


-- OK, let's save a copy of the SBV we built the stuff from, so that in later
-- weeks we can refer to the same data even if SBV is rebuilt...
select subscriber_id, account_number, panel, reporting_quality
into stafforr.V065_canonical_SBV_items
from vespa_analysts.vespa_single_box_view
where panel in ('VESPA','SKYVIEW')
;

select top 10 * from  stafforr.V065_canonical_SBV_items;
-- Yup, this is all the stuff that we want. We can build the main lookup from this.

select top 10 * from V059_enablement_listing_all_panels;
-- Yeah, that's the only other thing we need...

create unique index fake_pk     on stafforr.V065_canonical_SBV_items (subscriber_id);
create        index for_joins   on stafforr.V065_canonical_SBV_items (account_number);

commit;

-- OK, so how much collision do we have between this SBV and the enablement listing?
-- ideally should be none, or at least small, as we never really got the ramp-up
-- boxes into SBV.
select count(1) from stafforr.V065_canonical_SBV_items as csi
inner join V059_enablement_listing_all_panels as elap
on csi.account_number = elap.account_number
where elap.panel_id <> 0;
-- 681, that's not many, I'm kind of fine with that.
-- But wait, it's accounts, I need subscriber IDs... but the ramp up is all golden boxes,
-- so I can cross reference Jon's table for that?

select top 10 * from vespa_analysts.golden_boxes;
-- Yeah, that should work fine.

-- OK, let's build this table of subscribers, with one column showing which category
-- of stuff we're referring to; we'll care about different allignment of different
-- categories.
drop table V065_detailed_subscriber_listing;

select subscriber_id,
    panel,
    convert(varchar(20), case
        when reporting_quality >= 0.9   then 'A_GOOD_REPORTING'
        when reporting_quality = 0      then 'C_NO_REPORTING'
        else                                 'B_INCONSISTENT'
      end) as category
into stafforr.V065_detailed_subscriber_listing
from stafforr.V065_canonical_SBV_items;

commit;

create unique index fake_pk     on stafforr.V065_detailed_subscriber_listing (subscriber_id);
-- OK, so now we have to patch in everything that's not already in this table and is part
-- of the ramp up...

-- The enablement selection went by account_number, so we need to relate those to
-- subscriber IDs... this is generally pretty tricky to do, but they're all golden
-- boxes (as they were part of the ramp up!) so we cn use the golden box table...
select elap.account_number
    ,convert(decimal(10,0), gb.subscriber_id) as subscriber_id
into #rampupguys
from V059_enablement_listing_all_panels as elap
inner join vespa_analysts.golden_boxes as gb
on elap.account_number = gb.account_number
and elap.panel_id <> 0 -- don't want to include the disables here (yet) - hope this doesn't cause more questions, they're probably in the no reporting category anyways.
;

commit;
create unique index fake_pk     on #rampupguys (subscriber_id);
commit;

insert into V065_detailed_subscriber_listing
select rug.subscriber_id
    ,'RAMPUP' -- they're not on anything yet
    ,'D_RAMPUP'
from #rampupguys as rug
left join V065_detailed_subscriber_listing as dsl
on rug.subscriber_id = dsl.subscriber_id
where dsl.subscriber_id is null; -- Don't want to add the ones that are somehow already on
-- Vespa panel, those ones are fine where they are

commit;

-- OK, so what do we get?
select panel, category, count(1) as hits
from V065_detailed_subscriber_listing
group by panel, category
order by panel, category;
/*
'RAMPUP','D_RAMPUP',1932996
'SKYVIEW','A_GOOD_REPORTING',15107
'SKYVIEW','B_INCONSISTENT',6090
'SKYVIEW','C_NO_REPORTING',11450
'VESPA','A_GOOD_REPORTING',152349
'VESPA','B_INCONSISTENT',125900
'VESPA','C_NO_REPORTING',108548
*/

-- OK, so let's pull eveything out and see where it goes:
select *
from V065_detailed_subscriber_listing
order by subscriber_id, panel, category;
output to 'C:\\Users\\stafforr\\Documents\\2_Project\\Vespa\\V065 infrastructure filtering\\V065_detailed_subscriber_listing.csv';
-- Sweet! but, eep, comes in at 66MB! that's too much, kill it. Wait, what does it
-- compress into? oh, ok, 5.5MB, that compression is fine. Ship it!

