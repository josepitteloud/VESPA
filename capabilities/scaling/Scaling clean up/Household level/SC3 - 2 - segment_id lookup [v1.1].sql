/*
** Updates made due to Scaling 3.0.
** Tables are created with explicit values set during project.
** Have also created tables containg values for SC3 scaling variables and their
** equivalent values in SC2. This is done so that we can update the table
** SC3_scaling_weekly_sample with the updated scaling segments.
** Called Part C; not sure if this should be done here
** or in SC3 - 0 Table creation
*/

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
**              A01 - Cartesian JOIN of universe and 5 scaling variables
**              A02 - Add id for non-scalable accounts (i.e. those that do not have complete viewing data)
**              A03 - Populate segment name
**
** PART B           - Populate SC2_Variables_Lookup_v2_1 table
**              B01 - Make the whole thing (it's simple)
**
** PART C           - Populate tables containg SC3 scaling variables and their equivalent values in SC2
******************************************************************************/

--------------------------------------------------------------------------------
-- PART A POPULATE SEGMENT_ID LOOKUP TABLE
--------------------------------------------------------------------------------

/*
PART A   - Populate segment_id lookup table
     A01 - Cartesian JOIN of universe and 5 scaling variables

*/

--------------------------------------------------------------- A01 - Cartesian JOIN of universe and 5 scaling variables
-- A01 - Cartesian JOIN of universe and 7 scaling variables

-- 1) Sky_base_universe

CREATE TABLE #Sky_base_universe (
    Sky_base_universe                 VARCHAR(30)
);

INSERT INTO #Sky_base_universe VALUES ('Not adsmartable');
INSERT INTO #Sky_base_universe VALUES ('Adsmartable with consent');
INSERT INTO #Sky_base_universe VALUES ('Adsmartable but no consent');

-- 2) ISBA_tv_region

CREATE TABLE #isba_tv_region (
    isba_tv_region                 VARCHAR(30)
);

INSERT INTO #isba_tv_region VALUES('London');
INSERT INTO #isba_tv_region VALUES('NI, Scotland & Border');
INSERT INTO #isba_tv_region VALUES('North England');
INSERT INTO #isba_tv_region VALUES('South England');
INSERT INTO #isba_tv_region VALUES('Wales & Midlands');
INSERT INTO #isba_tv_region VALUES('Not Defined');

-- 3) HHComposition

CREATE TABLE #hhcomposition (
    h_household_composition                 VARCHAR(30)
);

INSERT INTO #hhcomposition VALUES('A) Families');
INSERT INTO #hhcomposition VALUES('B) Singles');
INSERT INTO #hhcomposition VALUES('C) Homesharers');
INSERT INTO #hhcomposition VALUES('D) Unclassified HHComp');

-- 4) Tenure

CREATE TABLE #tenure (
    tenure              VARCHAR(20)
);

INSERT INTO #tenure VALUES ('A) 0-2 Years');
INSERT INTO #tenure VALUES ('B) 3-10 Years');
INSERT INTO #tenure VALUES ('C) 10 Years+');


-- 5) Package

CREATE TABLE #Package (
    PACKAGE                 VARCHAR(20)
);

INSERT INTO #Package VALUES ('Basic');
INSERT INTO #Package VALUES ('Movies');
INSERT INTO #Package VALUES ('Movies & Sports');
INSERT INTO #Package VALUES ('Sports');

-- 6) No of STBs

CREATE TABLE #no_of_stbs (
    No_of_stbs                 VARCHAR(10)
);

INSERT INTO #no_of_stbs VALUES ('Single');
INSERT INTO #no_of_stbs VALUES ('Multiple');

-- 7) HD subscription

CREATE TABLE #hd_subscription (
    HD_subscription                 VARCHAR(10)
);

INSERT INTO #hd_subscription VALUES ('Yes');
INSERT INTO #hd_subscription VALUES ('No');

-- 8) PVR

CREATE TABLE #pvr (
    pvr                 VARCHAR(10)
);

INSERT INTO #pvr VALUES ('Yes');
INSERT INTO #pvr VALUES ('No');

if object_id('vespa_analysts.SC3_Segments_Lookup_v1_1') is not null then
        Call dba.sp_drop_table ('vespa_analysts',
                                  'SC3_Segments_Lookup_v1_1') end if;
Call dba.sp_create_table ('vespa_analysts',
                          'SC3_Segments_Lookup_v1_1',
                          'scaling_segment_id INT IDENTITY PRIMARY KEY
                          ,sky_base_universe varchar(40)
                          ,isba_tv_region varchar(25)
                          ,hhcomposition  varchar(40)
                          ,tenure         varchar(25)
                          ,package        varchar(25)
                          ,no_of_stbs     varchar(12)
                          ,hd_subscription varchar(4)
                          ,pvr            varchar(4)');
commit;

CREATE INDEX for_segment_identification ON vespa_analysts.SC3_Segments_Lookup_v1_1
    (sky_base_universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,no_of_stbs
        ,hd_subscription
        ,pvr);

-- commit;
-- go

-- Cartesian JOIN in two parts.
-- One part sets pvr to 'No' when the unvierse isn't adsmartable, the other part includes all adsmartable universes when pvr is 'Yes'
insert into vespa_analysts.SC3_Segments_Lookup_v1_1
        (sky_base_universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,no_of_stbs
        ,hd_subscription
        ,pvr)
select *
              from (select sky_base_universe from #Sky_base_universe where sky_base_universe like '%Not%') as sub
        cross JOIN (select isba_tv_region from #isba_tv_region) as sub1
        cross JOIN (select h_household_composition from #hhcomposition) as sub2
        cross JOIN (select tenure from #tenure) as sub3
        cross JOIN (select package from #package) as sub4
        cross JOIN (select No_of_stbs from #no_of_stbs) as sub5
        cross JOIN (select hd_subscription from #hd_subscription) as sub6
        cross JOIN (select 'No' as pvr) as sub7;

insert into vespa_analysts.SC3_Segments_Lookup_v1_1
        (sky_base_universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,no_of_stbs
        ,hd_subscription
        ,pvr)
select *
              from (select sky_base_universe from #Sky_base_universe) as sub
        cross JOIN (select isba_tv_region from #isba_tv_region) as sub1
        cross JOIN (select h_household_composition from #hhcomposition) as sub2
        cross JOIN (select tenure from #tenure) as sub3
        cross JOIN (select package from #package) as sub4
        cross JOIN (select No_of_stbs from #no_of_stbs) as sub5
        cross JOIN (select hd_subscription from #hd_subscription) as sub6
        cross JOIN (select 'Yes' as pvr) as sub7;
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
if object_id('vespa_analysts.SC3_Variables_Lookup_v1_1') is not null then
        Call dba.sp_drop_table ('vespa_analysts',
                                  'SC3_Variables_Lookup_v1_1') end if;
Call dba.sp_create_table ('vespa_analysts',
                          'SC3_Variables_Lookup_v1_1',
                          'id int not null primary key, scaling_variable varchar(20) not null');
commit;
go

delete from vespa_analysts.SC3_Variables_Lookup_v1_1;

insert into vespa_analysts.SC3_Variables_Lookup_v1_1 values (1, 'hhcomposition');
insert into vespa_analysts.SC3_Variables_Lookup_v1_1 values (2, 'package');
insert into vespa_analysts.SC3_Variables_Lookup_v1_1 values (3, 'isba_tv_region');
insert into vespa_analysts.SC3_Variables_Lookup_v1_1 values (4, 'tenure');
insert into vespa_analysts.SC3_Variables_Lookup_v1_1 values (5, 'no_of_stbs');
insert into vespa_analysts.SC3_Variables_Lookup_v1_1 values (6, 'hd_subscription');
insert into vespa_analysts.SC3_Variables_Lookup_v1_1 values (7, 'pvr');

commit;
go

grant select on vespa_analysts.SC3_Segments_Lookup_v1_1 to vespa_group_low_security;
grant select on vespa_analysts.SC3_Variables_Lookup_v1_1 to vespa_group_low_security;
commit;

go
--
-- -- --------------------------------------------------------------------------------
-- -- -- PART C POPULATE TABLES CONTAING SC3 SCALING VARIABLES AND THEIR EQUIVALENT VALUES IN SC2
-- -- --------------------------------------------------------------------------------
-- -- 
-- -- -- Create tables containing scaling variables that are in SC2 and SC3 with their equivalent
-- -- -- values in both. This means that we can update the SC3_scaling_weekly_sample table to include
-- -- -- these values. NOT TOO SURE THIS IS NEEDED AT PRESENT. WILL KEEP FOR THE MOMENT AS IT GIVES US
-- -- -- A DATABASE TABLE WITH OLD AND NEW VALUES.
-- -- 
-- -- --------------------------------------------------------------- B01 - Add all the values
-- -- 
-- -- 1) Sky_base_universe
-- -- Not required as it is a new scaling variable
-- 
-- 
-- -- 2) ISBA_tv_region
-- if object_id('SC3_isba_tv_region') is not null then drop table SC3_isba_tv_region end if;
-- CREATE TABLE SC3_isba_tv_region (
--     SC3_isba_tv_region                 VARCHAR(30)
--    ,SC2_isba_tv_region                 VARCHAR(30)
-- );
-- 
-- INSERT INTO SC3_isba_tv_region VALUES('London','London');
-- INSERT INTO SC3_isba_tv_region VALUES('NI, Scotland, & Border','Border');
-- INSERT INTO SC3_isba_tv_region VALUES('NI, Scotland, & Border','Central Scotland');
-- INSERT INTO SC3_isba_tv_region VALUES('NI, Scotland, & Border','North Scotland');
-- INSERT INTO SC3_isba_tv_region VALUES('NI, Scotland, & Border','Ulster');
-- INSERT INTO SC3_isba_tv_region VALUES('North England','North East');
-- INSERT INTO SC3_isba_tv_region VALUES('North England','North West');
-- INSERT INTO SC3_isba_tv_region VALUES('North England','Yorkshire');
-- INSERT INTO SC3_isba_tv_region VALUES('South England','South West');
-- INSERT INTO SC3_isba_tv_region VALUES('South England','HTV West');
-- INSERT INTO SC3_isba_tv_region VALUES('South England','Meridian (exc. Chann');
-- INSERT INTO SC3_isba_tv_region VALUES('Wales & Midlands','HTV Wales');
-- INSERT INTO SC3_isba_tv_region VALUES('Wales & Midlands','Midlands');
-- INSERT INTO SC3_isba_tv_region VALUES('Wales & Midlands','East Of England');
-- INSERT INTO SC3_isba_tv_region VALUES('Not Defined','Not Defined');
-- 
-- -- 3) HHComposition
-- if object_id('SC3_hhcomposition') is not null then drop table SC3_hhcomposition end if;
-- CREATE TABLE SC3_hhcomposition (
--     SC3_h_household_composition                 VARCHAR(40)
--    ,SC2_h_household_composition                 VARCHAR(40)
-- );
-- 
-- INSERT INTO SC3_hhcomposition VALUES('A) Families','00: Families');
-- INSERT INTO SC3_hhcomposition VALUES('A) Families','01: Extended family');
-- INSERT INTO SC3_hhcomposition VALUES('A) Families','02: Extended household');
-- INSERT INTO SC3_hhcomposition VALUES('A) Families','03: Pseudo family');
-- INSERT INTO SC3_hhcomposition VALUES('B) Singles','04: Single male');
-- INSERT INTO SC3_hhcomposition VALUES('B) Singles','05: Single female');
-- INSERT INTO SC3_hhcomposition VALUES('C) Homesharers','06: Male homesharers');
-- INSERT INTO SC3_hhcomposition VALUES('C) Homesharers','07: Female homesharers');
-- INSERT INTO SC3_hhcomposition VALUES('C) Homesharers','08: Mixed homesharers');
-- INSERT INTO SC3_hhcomposition VALUES('A) Families','09: Abbreviated male families');
-- INSERT INTO SC3_hhcomposition VALUES('A) Families','10: Abbreviated female families');
-- INSERT INTO SC3_hhcomposition VALUES('C) Homesharers','11: Multi-occupancy dwelling');
-- INSERT INTO SC3_hhcomposition VALUES('D) Unclassified HHComp','D) Unclassified HHComp');
-- 
-- -- 4) Tenure
-- -- Not required as values are exactly the same as before
-- 
-- -- 5) Package
-- if object_id('SC3_Package') is not null then drop table SC3_Package end if;
-- CREATE TABLE SC3_Package (
--     SC3_Package                 VARCHAR(20)
--    ,SC2_Package                 VARCHAR(20)
-- );
-- 
-- INSERT INTO SC3_Package VALUES ('Basic','Basic - Ent');
-- INSERT INTO SC3_Package VALUES ('Basic','Basic - Ent Extra');
-- INSERT INTO SC3_Package VALUES ('Movies','Dual Movies');
-- INSERT INTO SC3_Package VALUES ('Sports','Dual Sports');
-- INSERT INTO SC3_Package VALUES ('Movies & Sports','Other Premiums');
-- INSERT INTO SC3_Package VALUES ('Movies','Single Movies');
-- INSERT INTO SC3_Package VALUES ('Sports','Single Sports');
-- INSERT INTO SC3_Package VALUES ('Movies & Sports','Top Tier');
-- 
-- -- 6) No of STBs
-- -- Not required new scaling variable
-- -- 7) HD subscription
-- -- Not required new scaling variable
-- -- 8) PVR
-- -- Not required new scaling variable
