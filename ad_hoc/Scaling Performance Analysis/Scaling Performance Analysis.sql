select  * from sys.syscatalog where lower(tname) like 'v_sc2%' and creator = 'bednaszs'

select top 10 * from bednaszs.v_SC2_Category_Subtotals
select top 10 * from bednaszs.v_SC2_Intervals
select top 10 * from bednaszs.v_SC2_Metrics
select top 10 * from bednaszs.v_SC2_Non_Convergences
select top 10 * from bednaszs.v_SC2_Sky_base_segment_snapshots
select top 10 * from bednaszs.v_SC2_Weightings
select top 10 * from bednaszs.v_SC2_Vespa_Household_Weighting

-- how many segments are into play...
select  count(distinct scaling_segment_id)
from    bednaszs.v_SC2_Weightings -- 80006

-- how many left over...
select  count(distinct scaling_segment_id)
from    vespa_analysts.SC2_Segments_lookup -- 304201
where   scaling_segment_id not in   (
                                        select  distinct scaling_segment_id
                                        from    bednaszs.v_SC2_Weightings
                                    ) -- 224195

-- lets see how do they look like...
select  top 10 *
from    vespa_analysts.SC2_Segments_lookup -- 304201
where   scaling_segment_id not in   (
                                        select  distinct scaling_segment_id
                                        from    bednaszs.v_SC2_Weightings
                                    ) -- 224195

select  scaling_segment_id
        ,universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
into    sccheck_emptysegments
from    vespa_analysts.SC2_Segments_lookup -- 304201
where   scaling_segment_id not in   (
                                        select  distinct scaling_segment_id
                                        from    bednaszs.v_SC2_Weightings
                                    ) -- 224195

--so we can measure per value on each variable how many empty segments are, related to such value...
select  universe
        ,count(1) as empty_hits
from    sccheck_emptysegments
group   by  universe

select  isba_tv_region
        ,count(1) as empty_hits
from    sccheck_emptysegments
group   by  isba_tv_region

select  hhcomposition
        ,count(1) as empty_hits
from    sccheck_emptysegments
group   by  hhcomposition

select  tenure
        ,count(1) as empty_hits
from    sccheck_emptysegments
group   by  tenure

select  package
        ,count(1) as empty_hits
from    sccheck_emptysegments
group   by  package

select  boxtype
        ,count(1) as empty_hits
from    sccheck_emptysegments
group   by  boxtype

---

select  lookup.universe
        ,count() as empty_vespa
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.universe


select  lookup.isba_tv_region
        ,count() as empty_vespa
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.isba_tv_region


select  lookup.hhcomposition
        ,count() as empty_vespa
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.hhcomposition


select  lookup.tenure
        ,count() as empty_vespa
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.tenure

select  lookup.package
        ,count() as empty_vespa
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.package

select  lookup.boxtype
        ,count() as empty_vespa
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.boxtype





-------------------

/* UNIVERSE */

--Segments used + Sky accounts
select  lookup.universe
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.universe

--Segments used + Sky accounts + Vespa accounts
select  lookup.universe
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.universe

--Segments used + Sky Accounts - Vespa accounts
select  lookup.universe
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts = 0
group   by  lookup.universe



/* REGION */

--Segments used + Sky accounts
select  lookup.isba_tv_region
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.isba_tv_region

--Segments used + Sky accounts + Vespa accounts
select  lookup.isba_tv_region
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.isba_tv_region

select  lookup.isba_tv_region
        ,sum(vespa_accounts) as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.isba_tv_region

--Segments used + Sky Accounts - Vespa accounts
select  lookup.isba_tv_region
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts = 0
group   by  lookup.isba_tv_region



/* HH COMPOSITION */

--Segments used + Sky accounts
select  lookup.hhcomposition
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.hhcomposition

--Segments used + Sky accounts + Vespa accounts
select  lookup.hhcomposition
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.hhcomposition

--Segments used + Sky Accounts - Vespa accounts
select  lookup.hhcomposition
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts = 0
group   by  lookup.hhcomposition




/* TENURE */

--Segments used + Sky accounts
select  lookup.tenure
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.tenure

--Segments used + Sky accounts + Vespa accounts
select  lookup.tenure
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.tenure

select  lookup.tenure
        ,sum(vespa_accounts) as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.tenure

--Segments used + Sky Accounts - Vespa accounts
select  lookup.tenure
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts = 0
group   by  lookup.tenure



/* PACKAGE */

--Segments used + Sky accounts
select  lookup.package
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.package

--Segments used + Sky accounts + Vespa accounts
select  lookup.package
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.package

select  lookup.package
        ,sum(vespa_accounts) as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.package

--Segments used + Sky Accounts - Vespa accounts
select  lookup.package
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts = 0
group   by  lookup.package



/* BOX TYPE */

--Segments used + Sky accounts
select  lookup.boxtype
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
group   by  lookup.boxtype

--Segments used + Sky accounts + Vespa accounts
select  lookup.boxtype
        ,sum(vespa_accounts) as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts > 0
group   by  lookup.boxtype

--Segments used + Sky Accounts - Vespa accounts
select  lookup.boxtype
        ,count() as hits
from    bednaszs.v_SC2_Weightings as weight
        inner join vespa_analysts.SC2_Segments_lookup as lookup
        on weight.scaling_segment_id = lookup.scaling_segment_id
where   scaling_day = (select max(scaling_day) from bednaszs.v_SC2_Weightings)
and     weight.vespa_accounts = 0
group   by  lookup.boxtype





select  knockout_level
        ,count(distinct account_number) as hits
from    vespa_analysts.waterfall_base
group   by  knockout_level


select  count(distinct account_number)
from    vespa_analysts.waterfall_base


select  count(distinct water.account_number) as hits
from    vespa_analysts.vespa_single_box_view as sbv
        inner join vespa_analysts.waterfall_base as water
        on sbv.account_number = water.account_number 