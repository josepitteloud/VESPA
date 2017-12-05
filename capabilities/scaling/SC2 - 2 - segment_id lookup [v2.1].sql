/******************************************************************************
**
** Project Vespa: Scaling 2 - Segment lookup population
**
** Part of the scaling process involves abstracting variable combinations into
** a single integer. This allows greater flexibility between scaling builds,
** and helps isolate process code from various business logic changes. This
** script builds and populates the table that assigns integer segment IDs to
** combinations of variables. Refer to the wiki page on scaling:
**
**      http://rtci/Vespa1/Scaling.aspx
**
** Thing still to do:
**  3. ...
**
** Things recently done:
**  1. Add the item about the variables that get looped during the iteration
**  2. Doesn't have segment name on this guy? Need to add him.
**
** CODE SECTIONS
**
** PART A           - Populate segment_id lookup table
**              A01 - Cartesian join of universe and 5 scaling variables
**              A02 - Add id for non-scalable accounts (i.e. those that do not have complete viewing data)
**              A03 - Populate segment name
**
** PART B           - Populate SC2_Variables_Lookup_v2_1 table
**              B01 - Make the whole thing (it's simple)
**
******************************************************************************/

--------------------------------------------------------------------------------
-- PART A POPULATE SEGMENT_ID LOOKUP TABLE
--------------------------------------------------------------------------------

/*
PART A   - Populate segment_id lookup table
     A01 - Cartesian join of universe and 5 scaling variables

*/

--------------------------------------------------------------- A01 - Cartesian join of universe and 5 scaling variables
-- A01 - Cartesian join of universe and 5 scaling variables

-- 1) Universe

CREATE TABLE #Universe (
    Universe                 VARCHAR(20)
);

INSERT INTO #Universe VALUES ('A) Single box HH');
INSERT INTO #Universe VALUES ('B) Multiple box HH');

-- 2) ISBA_tv_region

SELECT DISTINCT isba_tv_region
INTO #isba_tv_region
FROM sk_prod.cust_single_account_view;


-- 2) HHComposition

-- Not listing things explicitly, just pulling out whatever is in the ConsumerView table.

select distinct h_household_composition
into #hhcomposition
from sk_prod.EXPERIAN_CONSUMERVIEW;

-- 3) Tenure

CREATE TABLE #tenure (
    tenure              VARCHAR(20)

);

INSERT INTO #tenure VALUES ('A) 0-2 Years');
INSERT INTO #tenure VALUES ('B) 3-10 Years');
INSERT INTO #tenure VALUES ('C) 10 Years+');
INSERT INTO #tenure VALUES ('D) Unknown');


-- 4) Package

CREATE TABLE #Package (
    PACKAGE                 VARCHAR(20)
);

INSERT INTO #Package VALUES ('Top Tier');
INSERT INTO #Package VALUES ('Dual Sports');
INSERT INTO #Package VALUES ('Dual Movies');
INSERT INTO #Package VALUES ('Single Sports');
INSERT INTO #Package VALUES ('Single Movies');
INSERT INTO #Package VALUES ('Other Premiums');
INSERT INTO #Package VALUES ('Basic - Ent');
INSERT INTO #Package VALUES ('Basic - Ent Extra');

-- 5) Boxtype

CREATE TABLE #Boxtype (
    Boxtype                 VARCHAR(30)
);

INSERT INTO #Boxtype VALUES ('A) HDx & No_secondary_box');
INSERT INTO #Boxtype VALUES ('B) HD & No_secondary_box');
INSERT INTO #Boxtype VALUES ('C) Skyplus & No_secondary_box');
INSERT INTO #Boxtype VALUES ('D) FDB & No_secondary_box');
INSERT INTO #Boxtype VALUES ('E) HD & HD');
INSERT INTO #Boxtype VALUES ('F) HD & Skyplus');
INSERT INTO #Boxtype VALUES ('G) HD & FDB');
INSERT INTO #Boxtype VALUES ('H) HDx & HDx');
INSERT INTO #Boxtype VALUES ('I) HDx & Skyplus');
INSERT INTO #Boxtype VALUES ('J) HDx & FDB');
INSERT INTO #Boxtype VALUES ('K) Skyplus & Skyplus');
INSERT INTO #Boxtype VALUES ('L) Skyplus & FDB');
INSERT INTO #Boxtype VALUES ('M) FDB & FDB');

commit;


if object_id('SC2_Segments_Lookup_v2_1') is not null then drop table SC2_Segments_Lookup_v2_1 end if;
CREATE TABLE SC2_Segments_Lookup_v2_1 (
    scaling_segment_ID          int             not null identity primary key
    ,universe                   varchar(20)
    ,isba_tv_region             varchar(20)
    ,hhcomposition              varchar(2)      -- Just using the encoded HH composition, we'll decode in the segment name step
    ,tenure                     varchar(20)
    ,package                    varchar(20)
    ,boxtype                    varchar(30)
    ,scaling_segment_name       varchar(150)    -- The name of the segment (for reporting purposes)
);

CREATE INDEX for_segment_identification ON SC2_Segments_Lookup_v2_1
    (universe, isba_tv_region, hhcomposition, tenure, package, boxtype);

-- Note that 280801 is the 'Non-scalable' segment_id for accounts that do not have complete viewing data

grant select on SC2_Segments_Lookup_v2_1 to vespa_group_low_security, sk_prodreg;

commit;
go


-- Cartesian join
INSERT INTO SC2_Segments_Lookup_v2_1 (
    universe
    ,isba_tv_region
    ,hhcomposition
    ,tenure
    ,package
    ,boxtype
)
SELECT
     universe
    ,#isba_tv_region.isba_tv_region
    ,#hhcomposition.h_household_composition
    ,#tenure.tenure
    ,#package.package
    ,#boxtype.boxtype
FROM #universe
inner join #isba_tv_region on 1=1
inner join #hhcomposition on 1=1
inner join #tenure on 1=1
inner join #package on 1=1
inner join #boxtype on 1=1;


--------------------------------------------------------------- A02 - Add id for non-scalable accounts
-- A02 - Add id for non-scalable accounts

-- For Non-scalable accounts (i.e. those that do not have complete viewing data)
INSERT INTO SC2_Segments_Lookup_v2_1 (
    universe
    ,isba_tv_region
    ,hhcomposition
    ,tenure
    ,package
    ,boxtype
)
SELECT 'Non-scalable'
       ,'Non-scalable'
       ,'NS'            -- shortcut for Non-Scalable
       ,'Non-scalable'
       ,'Non-scalable'
       ,'Non-scalable'

-- The non-scalable scaling_segment_id is 280801

-- 3x14x12x5x8x13 = 262080 combinations
-- + 1 non-scalable segment
-- Total segments 262081

commit;
go

--------------------------------------------------------------- A03 - Form segment name

-- PanMan wants a single identifier to be able to publish the name of
-- each segment. Here is also where we identify the hhcomposition
-- descriptions rather than just the codes. The hhcomposition names
-- are outlined in:
--      http://mktskyportal/Shared%20Documents/Data%20Dictionaries%203rd%20Party%20Data/Experian%202012/Household%20Composition.pdf

update SC2_Segments_Lookup_v2_1
set scaling_segment_name
    = '(' || universe || ') - ' ||
      '(' || isba_tv_region || ') - ' ||
      '(' || case hhcomposition
                when '00' then 'Families'
                when '01' then 'Extended family'
                when '02' then 'Extended household'
                when '03' then 'Pseudo family'
                when '04' then 'Single male'
                when '05' then 'Single female'
                when '06' then 'Male homesharers'
                when '07' then 'Female homesharers'
                when '08' then 'Mixed homesharers'
                when '09' then 'Abbreviated male families'
                when '10' then 'Abbreviated female families'
                when '11' then 'Multi-occupancy dwelling'
                when 'U'  then 'Unclassified'
                when 'NS' then 'Non-scaling'
                else 'FAIL HHCOMP!'
            end || ') - ' ||
      '(' || tenure || ') - ' ||
      '(' || package || ') -' ||
      '(' || boxtype || ')'
;
commit;
go

--------------------------------------------------------------------------------
-- PART B POPULATE SCALING VARIABELS LOOKUP TABLE
--------------------------------------------------------------------------------

-- The Rim-weighting iterates through each of these variables individually until
-- all the category sum of weights have converged to the population category
-- subtotals. Abstracting this guy out also helps avoid having to specifically
-- mention any variable in the iterative rim weighting loop, making it a bit more
-- modular.

--------------------------------------------------------------- B01 - Add all the values
if object_id('SC2_Variables_Lookup_v2_1') is not null then drop table SC2_Variables_Lookup_v2_1 end if;
create table SC2_Variables_Lookup_v2_1 (
    id                                  int             not null primary key
    ,scaling_variable                   varchar(20)     not null
);

commit;
go


delete from SC2_Variables_Lookup_v2_1;

insert into SC2_Variables_Lookup_v2_1 values (1, 'hhcomposition');
insert into SC2_Variables_Lookup_v2_1 values (2, 'package');
insert into SC2_Variables_Lookup_v2_1 values (3, 'isba_tv_region');
insert into SC2_Variables_Lookup_v2_1 values (4, 'tenure');
insert into SC2_Variables_Lookup_v2_1 values (5, 'boxtype');

commit;
go


grant select on SC2_Segments_Lookup_v2_1 to vespa_group_low_security;
grant select on SC2_Variables_Lookup_v2_1 to vespa_group_low_security;
commit;

go
