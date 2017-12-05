
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
--------------     00  -- Create Tables For Scaling Model M11
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

-- As parts of the Scaling 3.0 script are used then you will need to run that create table script
-- This can be found on the repository as well as at the end of this script:
-- \Git_repository\Vespa\ad_hoc\V154 - Scaling 3.0\Vespa Analysts - SC3\SC3 - 0 - table creation.sql




--------------------------------------------------------------------------------
-- Skyview versions of Scaling 3.0 tables
--------------------------------------------------------------------------------


--- This tables contains the names of the variables that are used to create
--- the scaling segments. It is static data (unless we decide to change the variables)

if object_id('SC3I_Variables_lookup_v1_1') is not null drop table SC3I_Variables_lookup_v1_1

create table SC3I_Variables_lookup_v1_1 (
        id                      int
        ,scaling_variable        varchar(20)
)

create lf index ind1 on SC3I_Variables_lookup_v1_1(id)
create lf index ind2 on SC3I_Variables_lookup_v1_1(scaling_variable)
commit

insert into SC3I_Variables_lookup_v1_1 values (1, 'hhcomposition')
insert into SC3I_Variables_lookup_v1_1 values (2, 'package')
insert into SC3I_Variables_lookup_v1_1 values (3, 'isba_tv_region')
insert into SC3I_Variables_lookup_v1_1 values (4, 'age_band')
insert into SC3I_Variables_lookup_v1_1 values (5, 'gender')
insert into SC3I_Variables_lookup_v1_1 values (6, 'head_of_hhd')
commit


--- The scaling segments table is also static and just needs to be populated the once with
--- the scaling segments.
--- There are a lot segments to load into the table.
--- A text file in the repo will have the latest which you will need to load
--- or alternatively copy from another schema
if object_id('SC3I_Segments_lookup_v1_1') is not null drop table SC3I_Segments_lookup_v1_1

create table SC3I_Segments_lookup_v1_1 (
        scaling_segment_id int
        ,sky_base_universe varchar(30)
        ,isba_tv_region varchar(30)
        ,hhcomposition varchar(30)
        ,package varchar(30)
        ,head_of_hhd varchar(1)
        ,gender varchar(1)
        ,age_band varchar(10)
)

create hg index ind_seg on SC3I_Segments_lookup_v1_1(scaling_segment_id)
create lf index ind_uni on SC3I_Segments_lookup_v1_1(sky_base_universe)
create lf index ind_region on SC3I_Segments_lookup_v1_1(isba_tv_region)
create lf index ind_comp on SC3I_Segments_lookup_v1_1(hhcomposition)
create lf index ind_package on SC3I_Segments_lookup_v1_1(package)
create lf index ind_head on SC3I_Segments_lookup_v1_1(head_of_hhd)
create lf index ind_gender on SC3I_Segments_lookup_v1_1(gender)
create lf index ind_age on SC3I_Segments_lookup_v1_1(age_band)
commit




if object_id('SC3I_Sky_base_segment_snapshots') is not null drop table SC3I_Sky_base_segment_snapshots

create table SC3I_Sky_base_segment_snapshots (
        account_number                  varchar(20)
        ,profiling_date                 date
        ,HH_person_number               tinyint
        ,population_scaling_segment_id  int
        ,vespa_scaling_segment_id       int
        ,expected_boxes                 int
)
create hg index ind1 on SC3I_Sky_base_segment_snapshots(account_number)
create lf index ind2 on SC3I_Sky_base_segment_snapshots(profiling_date)
create lf index ind3 on SC3I_Sky_base_segment_snapshots(HH_person_number)
create hg index ind4 on SC3I_Sky_base_segment_snapshots(population_scaling_segment_id)
create hg index ind5 on SC3I_Sky_base_segment_snapshots(vespa_scaling_segment_id)

commit


if object_id('SC3I_Todays_panel_members') is not null drop table SC3I_Todays_panel_members

create table SC3I_Todays_panel_members (
        account_number varchar(20)
        ,HH_person_number  tinyint
        ,scaling_segment_id int
)
create hg index ind1 on SC3I_Todays_panel_members(account_number)
create lf index ind2 on SC3I_Todays_panel_members(HH_person_number)
create hg index ind3 on SC3I_Todays_panel_members(scaling_segment_id)

commit



if object_id('SC3I_weighting_working_table') is not null drop table SC3I_weighting_working_table

CREATE TABLE SC3I_weighting_working_table (
    scaling_segment_id      INT             primary key
    ,sky_base_universe      VARCHAR(50)
    ,sky_base_accounts      DOUBLE          not null
    ,vespa_panel            DOUBLE          default 0
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,segment_weight         DOUBLE
    ,indices_actual         DOUBLE
    ,indices_weighted       DOUBLE
)

CREATE HG INDEX indx_un on SC3I_weighting_working_table(sky_base_universe
commit
go



if object_id('SC3I_category_working_table') is not null drop table SC3I_category_working_table

CREATE TABLE SC3I_category_working_table (
     sky_base_universe      VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence_flag       TINYINT     DEFAULT 1
)

create hg index indx_universe on SC3I_category_working_table(sky_base_universe)
create hg index indx_profile on SC3I_category_working_table(profile)
create hg index indx_value on SC3I_category_working_table(value)

commit
go


if object_id('SC3I_category_subtotals') is not null drop table SC3I_category_subtotals

CREATE TABLE SC3I_category_subtotals (
     scaling_date           date
    ,sky_base_universe      VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence            TINYINT
)

create index indx_date on SC3I_category_subtotals(scaling_date)
create hg index indx_universe on SC3I_category_subtotals(sky_base_universe)
create hg index indx_profile on SC3I_category_subtotals(profile)

commit
go


if object_id('SC3I_metrics') is not null drop table SC3I_metrics

CREATE TABLE SC3I_metrics (
     scaling_date           DATE
     ,iterations            int
     ,convergence           tinyint
     ,max_weight            float
     ,av_weight             float
     ,sum_of_weights        float
     ,sky_base              bigint
     ,vespa_panel           bigint
     ,non_scalable_accounts bigint
     ,sum_of_convergence    float
)

create index indx_date on SC3I_metrics(scaling_date)
commit
go



if object_id('SC3I_non_convergences') is not null drop table SC3I_non_convergences

CREATE TABLE SC3I_non_convergences (
      scaling_date           DATE
     ,scaling_segment_id     int
     ,difference             float
)

create index indx_date on SC3I_non_convergences(scaling_date)
commit
go




if object_id('SC3I_Weightings') is not null then drop table SC3I_Weightings end if;
create table SC3I_Weightings (
    scaling_day                 date            not null
    ,scaling_segment_ID         int             not null        -- links to the segments lookup table
    ,vespa_accounts             bigint          default 0       -- Vespa panel accounts in this segment reporting back for this day
    ,sky_base_accounts          bigint          not null        -- Sky base accounts for this day by segment
    ,weighting                  double          default null    -- The weight for an account in this segment
    ,sum_of_weights             double          default null    -- The total weight for all accounts in this segment
    ,indices_actual             double
    ,indices_weighted           double
    ,convergence                tinyint
    ,primary key (scaling_day, scaling_segment_ID)
);

create date index idx1 on SC3I_Weightings(scaling_day);
create hg index idx2 on SC3I_Weightings(scaling_segment_ID);

grant select on SC3I_Weightings to vespa_group_low_security, sk_prodreg;

commit;
go



if object_id('SC3I_Intervals') is not null then drop table SC3I_Intervals end if;
create table SC3I_Intervals (
    account_number              varchar(20)     not null
    ,HH_person_number           tinyint          not null
    ,reporting_starts           date            not null
    ,reporting_ends             date            not null
    ,scaling_segment_ID         int             not null        -- links to the segments lookup table
 --   ,primary key (account_number, reporting_starts)             -- Won't bother forcing the no-overlap in DB constraints, but this is a good start
);

create index for_joining on SC3I_Intervals (scaling_segment_ID, reporting_starts);
create hg index idx1 on SC3I_Intervals (account_number);
create hg index idx2 on SC3I_Intervals (HH_person_number);


grant select on SC3I_Intervals to vespa_group_low_security, sk_prodreg;

commit;
go



if object_id('V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING') is not null then drop table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING end if;
create table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (
    account_number              varchar(20)     not null
    ,HH_person_number           tinyint         not null
    ,scaling_date               date            not null        -- date on which scaling is applied
    ,scaling_weighting          float           not null
    ,build_date                 datetime        not null        -- tracking processing to assist VIQ loads
--    ,primary key (account_number, HH_person_number, scaling_date)
);

create dttm index for_loading on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (build_date);
create hg index idx1 on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (account_number);
create date index idx3 on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (scaling_date);
create hg index idx4 on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING (HH_person_number);


grant select on V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING to vespa_group_low_security, sk_prodreg;

commit;
go



if object_id('V289_M11_04_Barb_weighted_population') is not null then drop table V289_M11_04_Barb_weighted_population end if;
create table V289_M11_04_Barb_weighted_population (
        ageband varchar(10)
        ,gender char(1)
        ,barb_weight double
        );

create lf index ind1 on V289_M11_04_Barb_weighted_population(ageband);
create lf index ind2 on V289_M11_04_Barb_weighted_population(gender);

commit;
go

/******************************************************************************
**
** Project Vespa: Scaling 3 - Core table creation
**
** This is the original code used for Scaling 2.0, but with the prefix 'SC2_'
** changed to 'SC3_'
**
** Table SC3_scaling_weekly_sample updated to include new scaling variables, though
** we have also kept the obsolete scaling variables as the tables are filled directly
** from tables in sk_prod. The new scaling columns variables are inserted later, whilst
** consolidated values are updated later. LOOK AT UPDATING CASE STATEMENTS IN CODE
** TO ACHIEVE THIS LATER.
** All occurences of 'universe' in the RIM weighting tables have been replaced
** with sky_base_universe
**
** THE COMMENTS FROM HERE TO THE END ARE THE COMMENTS MADE FOR THE ORIGINAL SCALING
** CODE.
**
** This script contains all the tables that get used to support the Scaling
** solution. These tables only get built in Vespa_Analysts; they get scheduled
** updates via the existing stored procedures.
**
** These tables provide everything you need to do basic weighting up to the Sky
** UK base. Refer to
**
**      http://rtci/Vespa1/Scaling.aspx
**
** for details on how that works. For anything more complicated, these tables
** still provide the segmentations and targets, but you have to build the weights
** yourself.
**
** A bunch of working tables are also present here, as are some other structures
** which support putting scaling into the VIQ internal interim solution.
**
** We're continuing with the normalisation by segment ID, since the flat table
** yields 15m records a month, whereas the normalised setup is closer to 4m, as
** well as being significantly faster to compute and use.
**
** To do:
** 12. Get all the QA tables we want in place (still need convergence testing -
**      we have convergence *tracking*, but are we throing flags if things don't
**      converge as much as we hope?)
** 13. ...
**
** Completed tasks:
**  8. Update for Scaling 2
**  9. Bring in the Scaling use case structures
** 10. Align table structure with existing process
** 11. Demonstrate functional build of Scaling 2
**
** Methodology established by Sarah Jackson during:
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=21
**
** Restructuring for VIQ interim internal solution by Robert Stafford during:
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=70
**
**
** CODE SECTIONS
**
** PART A           - Main external interface tables
**              A01 - SC2_Weightings                : daily segment totals & usual weights
**              A02 - SC2_Intervals                 : intervals where accounts belong to segments
**              A03 - SC2_Segments_lookup           : a lookup for segment_id
**              A04 - VESPA_HOUSEHOLD_WEIGHTING     : delivery format for VIQ build
**
** PART B           - Internal interface tables
**              B01 - SC2_Sky_base_segment_snapshots: hold the result of the segmentation process
**              B02 - SC2_Todays_panel_members      : for a single day, which accounts are considered to be on the panel
**              B03 - SC2_Todays_segment_weights    : for a single day, the calculated weights
**
** PART C           - Other internal processing tables
**              C01 - SC2_scaling_weekly_sample     : profile information updated on a weekly basis
**
** PART D           - Rim-weighting table
**              D01 - SC2_weighting_working_table   : segment level information for rim-weighting
**              D02 - SC2_category_working_table    : category level information for rim-weighting
**              D03 - Scaling_box_level_viewing     : box level viewing             - DEPRECIATED
**              D04 - SC2_Variables_lookup          : abstracts the scaling variables being used
**
** PART E           - QA Tables
**              E01 - scaling_segment_subtotals     : segment level subtotals       - DEPRECIATED
**              E02 - SC2_category_subtotals        : category level subtotals
**              E03 - SC2_metrics                   : metrics measuring the performance of the rim-weighting
**
**
**  Tables created (and their methodology build counterparts)
**  -------
**--scaling_weights                 -(DEPRECIATED)
**--Scaling_box_level_viewing       -(NOW A TEMP TABLE)
**--Scaling_segments_subtotals      -(DEPRECIATED: ABSORBED INTO SC2_Weightings)
**  SC2_Weightings                  -(NEW)
**  SC2_Intervals                   -(NEW)
**  SC2_Segments_lookup             -(<=Scaling_segments_lookup)
**  SC2_Variables_lookup            -(<=#scaling_variables in the iteration)
**  SC2_Sky_base_segment_snapshots  -(NEW)
**  SC2_Todays_panel_members        -(NEW)
**  SC2_Todays_segment_weights      -(NEW)
**  SC2_scaling_weekly_sample
**  SC2_weighting_working_table
**  SC2_category_working_table      -(<=scaling_categories)
**  SC2_category_subtotals          -(<=scaling_category_subtotals)
**  SC2_metrics                     -(<=scaling_metrics)
**  VESPA_HOUSEHOLD_WEIGHTING       -(NEW)
**
******************************************************************************/

/* Schema reset: use only in development

drop table SC2_scaling_weekly_sample;
drop table SC2_Segments_lookup;
drop table SC2_weighting_working_table;
drop table SC2_category_working_table;
drop table SC2_category_subtotals;
drop table SC2_metrics;
drop table SC2_Weightings;
drop table SC2_Intervals;
drop table SC2_Variables_lookup;
drop table SC2_Sky_base_segment_snapshots;
drop table SC2_Todays_panel_members;
drop table SC2_Todays_segment_weights;
drop table VESPA_HOUSEHOLD_WEIGHTING;

commit;

*/

--------------------------------------------------------------------------------
-- PART A MAIN EXTERNAL INTERFACE TABLES
--------------------------------------------------------------------------------

-- All these tables form the outer boundary of how people interact with scaling.
-- There are two main interfaces; firstly with analysts via the weights and intervals
-- tables as outlined on the wiki, and then separately for VIQII with the flatter
-- less efficient structure.

--------------------------------------------------------------- A01 - SC2_Weightings
-- Assigns weights to scaling segments by day. Also has a few other
-- bits like number of Vespa panel and Sky base acocunts to assist
-- with more involved scaling methodologies, they might help.

if object_id('SC3_Weightings') is not null then drop table SC3_Weightings end if;
create table SC3_Weightings (
    scaling_day                 date            not null
    ,scaling_segment_ID         int             not null        -- links to the segments lookup table
    ,vespa_accounts             bigint          default 0       -- Vespa panel accounts in this segment reporting back for this day
    ,sky_base_accounts          bigint          not null        -- Sky base accounts for this day by segment
    ,weighting                  double          default null    -- The weight for an account in this segment
    ,sum_of_weights             double          default null    -- The total weight for all accounts in this segment
    ,indices_actual             double
    ,indices_weighted           double
    ,convergence                tinyint
    ,primary key (scaling_day, scaling_segment_ID)
);

create date index idx1 on SC3_Weightings(scaling_day);
create hg index idx2 on SC3_Weightings(scaling_segment_ID);

grant select on SC3_Weightings to vespa_group_low_security, sk_prodreg;

commit;
go

--------------------------------------------------------------- A02 - SC3_Intervals
-- Assigns a scaling segment to an account over a period of time.
-- It's no longer just raw dialback, there are already multiroom
-- and reporting consistency artefacts int there. Virtual panel
-- balance will also manifest in there too.

if object_id('SC3_Intervals') is not null then drop table SC3_Intervals end if;
create table SC3_Intervals (
    account_number              varchar(20)     not null
    ,reporting_starts           date            not null
    ,reporting_ends             date            not null
    ,scaling_segment_ID         int             not null        -- links to the segments lookup table
    ,primary key (account_number, reporting_starts)             -- Won't bother forcing the no-overlap in DB constraints, but this is a good start
);

create index for_joining on SC3_Intervals (scaling_segment_ID, reporting_starts);
create hg index idx1 on SC3_Intervals (account_number);

grant select on SC3_Intervals to vespa_group_low_security, sk_prodreg;

commit;
go



--------------------------------------------------------------- A04 - VESPA_HOUSEHOLD_WEIGHTING
-- VIQII wants its weights delivered in a particular format; this format.
if object_id('VESPA_HOUSEHOLD_WEIGHTING') is not null then drop table VESPA_HOUSEHOLD_WEIGHTING end if;
create table VESPA_HOUSEHOLD_WEIGHTING (
    account_number              varchar(20)     not null
    ,cb_key_household           bigint          not null
    ,scaling_date               date            not null        -- date on which scaling is applied
    ,scaling_weighting          float           not null
    ,build_date                 datetime        not null        -- tracking processing to assist VIQ loads
    ,primary key (account_number, scaling_date)
);

create dttm index for_loading on VESPA_HOUSEHOLD_WEIGHTING (build_date);
create hg index idx1 on VESPA_HOUSEHOLD_WEIGHTING (account_number);
create hg index idx2 on VESPA_HOUSEHOLD_WEIGHTING (cb_key_household);
create date index idx3 on VESPA_HOUSEHOLD_WEIGHTING (scaling_date);

grant select on VESPA_HOUSEHOLD_WEIGHTING to vespa_group_low_security, sk_prodreg;

commit;
go

--------------------------------------------------------------------------------
-- PART B OTHER INTERNAL PROCESSING STUFF
--------------------------------------------------------------------------------
-- Scaling has a couple of places where established modules do work and go through
-- defined interfaces between these. (Other tightly coupled procedures like PanMan
-- also hook into some of these structures.) Module rules can be changed and the
-- rest of the code can remain provided that the changes don't entail structural
-- changes to these internal interfaces.

--------------------------------------------------------------- B01 - SC3_Sky_base_segment_snapshots
-- This table stores snapshots of how the Sky base is cut up by the defined scaling
-- segmentation. We also have a profiling date on there because we might end up
-- caching several iterations of profiling, given that the customer DB is the
-- computationally expecnsive part and if all this is done, the Vespa 2 drop will
-- end up comparatively fast to cache. It's not part of the external set becauase...
-- well, it might be quite large, and we might kill bits of it after all the caching
-- to the more eficient tables is complete.

-- For SC3 purposes we have included two scaling segment; one for the population and one for the weights.
-- In SC3 accounts are taken from the Adsmartable with consent and placed in a proxy Adsmartable but no consent
-- universe to try and mimic thos adsmartabel accounts who have not given viewing consent.
if object_id('SC3_Sky_base_segment_snapshots') is not null then drop table SC3_Sky_base_segment_snapshots end if;
create table SC3_Sky_base_segment_snapshots (
    account_number              varchar(20)     not null
    ,profiling_date             date            not null
    ,cb_key_household           bigint          not null    -- needed for VIQ interface
    ,population_scaling_segment_id         bigint
    ,vespa_scaling_segment_id         bigint
    ,expected_boxes             tinyint                     -- number of boxes in household; need to check they're all reporting
    ,primary key (account_number, profiling_date)
);


--------------------------------------------------------------- B02 - SC3_Todays_panel_members
-- The result of the daily panel selection module, this is a list of accounts
-- that are considered to be on the Vespa panel for the processing day in
-- question, and which segment they're in.

if object_id('SC3_Todays_panel_members') is not null then drop table SC3_Todays_panel_members end if;
create table SC3_Todays_panel_members (
    account_number              varchar(20)     not null primary key
    ,scaling_segment_id         bigint          not null
);

--------------------------------------------------------------- B03 - SC3_Todays_segment_weights
-- The results of the weight calculation module, this tells us what the
-- weights are for each scaling segment for whichever day is being
-- processed.

if object_id('SC3_Todays_segment_weights') is not null then drop table SC3_Todays_segment_weights end if;
create table SC3_Todays_segment_weights (
    scaling_segment_id          bigint          not null primary key
    ,scaling_weighting          float           not null
);

commit;
go

--------------------------------------------------------------------------------
-- PART C OTHER INTERNAL PROCESSING STUFF
--------------------------------------------------------------------------------

--------------------------------------------------------------- C01 - SC3_scaling_weekly_sample

-- The SC3_scaling_weekly_sample contains profile information for the accounts that
-- are being scaled. The profile information is re-calculated on a weekly basis.

-- This is used to calculate weights on a weekly basis and should not be deleted.
-- IF object_id('SC3_scaling_weekly_sample') IS NOT NULL
--    DROP TABLE SC3_scaling_weekly_sample;

if object_id('SC3_scaling_weekly_sample') is not null then drop table SC3_scaling_weekly_sample end if;
CREATE TABLE SC3_scaling_weekly_sample (
     account_number                     VARCHAR(20)     primary key
    ,cb_key_household                   BIGINT          not null            -- Needed for VIQ interim solution
    ,cb_key_individual                  BIGINT          not null            -- For ConsumerView linkage
    ,consumerview_cb_row_id             BIGINT                              -- Not really needed for consumerview linkage, but whatever
    ,universe                           VARCHAR(30)                         -- Single or multiple box household. Look at trying to make obsolete
    ,sky_base_universe                  VARCHAR(30)                         -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
    ,vespa_universe                     VARCHAR(30)                         -- Universe used for Vespa
    ,weighting_universe                 VARCHAR(30)                         -- Universe used for weighting purposes
    ,isba_tv_region                     VARCHAR(20)                         -- Scaling variable 1 : Region
    ,hhcomposition                      VARCHAR(2)      DEFAULT 'U'         -- Scaling variable 2: Household composition, originally from Experian Consumerview, collated later
    ,tenure                             VARCHAR(15)     DEFAULT 'E) Unknown'-- Scaling variable 3: Tenure 'Unknown' removed from vespa panel
    ,num_mix                            INT
    ,mix_pack                           VARCHAR(20)
    ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
    ,boxtype                            VARCHAR(35)                         -- Old scaling variable 5: Look at ways to make obsolete.
    ,no_of_stbs                         VARCHAR(15)                         -- Scaling variable 5: No of set top boxes
    ,hd_subscription                    VARCHAR(5)                          -- Scaling variable 6: HD subscription
    ,pvr                                VARCHAR(5)                          -- Scaling variable 6: Is the box pvr capable?
    ,population_scaling_segment_id      INT             DEFAULT NULL        -- segment scaling id for identifying segments in population
    ,vespa_scaling_segment_id       INT             DEFAULT NULL        -- segment scaling id for identifying segments used in rim weighting
    ,mr_boxes                           INT
--    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data - DISCONTINUED; now interfaces with scoring module via defined tables
);

CREATE INDEX for_segment_identification_raw ON SC3_scaling_weekly_sample
    (isba_tv_region,hhcomposition, tenure, package, no_of_stbs, hd_subscription, pvr);
    -- Might it be this one guy? this index rebuild making everything super slow? But it should be going in as a single atomic commit... but on inserts, it still only takes 55 sec...
CREATE INDEX experian_joining ON SC3_scaling_weekly_sample (consumerview_cb_row_id);
CREATE INDEX for_grouping1 ON SC3_scaling_weekly_sample (population_scaling_segment_id);
CREATE INDEX for_grouping2 ON SC3_scaling_weekly_sample (vespa_scaling_segment_id);

-- We don't join on this, it's just to force the data constraint we need for VIQ
--create unique index for_VIQ_uniqueness ON SC3_scaling_weekly_sample (cb_key_household);
-- Hah! except that the index fails because of dupes, we're going to have to pick that
-- up somehow in the publishing step. It might not manifest so much when e get down into
-- the Vespa panel... no, it really will, given the number of duplicates we've got. It's
-- still going to be an issue.

COMMIT;
go

--------------------------------------------------------------------------------
-- PART D RIM-WEIGHTING TABLES
--------------------------------------------------------------------------------

--------------------------------------------------------------- D01 - SC3_weighting_working_table

-- The segments table contains counts of sky base accounts versus vespa panel
-- accounts at a segment level*. This segment table is used to calculate segment
-- weights using rim-weighting.

-- *a segment is a combination of the scaling variables.

-- This is used to calculate weights on a weekly basis and should not be deleted.
-- this guy used to be the Scaling_segments table

if object_id('SC3_weighting_working_table') is not null then drop table SC3_weighting_working_table end if;
CREATE TABLE SC3_weighting_working_table (
    scaling_segment_id      INT             primary key
    ,sky_base_universe      VARCHAR(50)
    ,sky_base_accounts      DOUBLE          not null
    ,vespa_panel            DOUBLE          default 0
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,segment_weight         DOUBLE
    ,indices_actual         DOUBLE
    ,indices_weighted       DOUBLE
);

CREATE HG INDEX indx_un on SC3_weighting_working_table(sky_base_universe);

COMMIT;
go

--------------------------------------------------------------- D02 - SC3_category_working_table

-- The scaling category table contains counts of sky base accounts versus vespa
-- accounts at a category level. Rim-weighting aims to converge the sum_of_weights
-- subtotals to the sky base subtotals.

-- This is used to calculate weights on a weekly basis and should not be deleted.

if object_id('SC3_category_working_table') is not null then drop table SC3_category_working_table end if;
CREATE TABLE SC3_category_working_table (
     sky_base_universe      VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence_flag       TINYINT     DEFAULT 1
);


create hg index indx_universe on SC3_category_working_table(sky_base_universe);
create hg index indx_profile on SC3_category_working_table(profile);
create hg index indx_value on SC3_category_working_table(value);

COMMIT;
go

--------------------------------------------------------------- D03 - Scaling_box_level_viewing

-- No longer in play; now a temp table

--------------------------------------------------------------------------------
-- PART E QA TABLES
--------------------------------------------------------------------------------

--------------------------------------------------------------- E01 - Scaling_segments_subtotals

-- No longer in play; absorbed into Scaling_segments_subtotals

--------------------------------------------------------------- E02 - SC3_category_subtotals
-- This table contains historic information and should not be deleted
if object_id('SC3_category_subtotals') is not null then drop table SC3_category_subtotals end if;
CREATE TABLE SC3_category_subtotals (
     scaling_date           date
    ,sky_base_universe      VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence            TINYINT
);

create index indx_date on SC3_category_subtotals(scaling_date);
create hg index indx_universe on SC3_category_subtotals(sky_base_universe);
create hg index indx_profile on SC3_category_subtotals(profile);

COMMIT;
go

--------------------------------------------------------------- E03 - SC3_metrics

-- This table contains historic data and should not be deleted.
if object_id('SC3_metrics') is not null then drop table SC3_metrics end if;
CREATE TABLE SC3_metrics (
     scaling_date           DATE
     ,iterations            int
     ,convergence           tinyint
     ,max_weight            float
     ,av_weight             float
     ,sum_of_weights        float
     ,sky_base              bigint
     ,vespa_panel           bigint
     ,non_scalable_accounts bigint
     ,sum_of_convergence    float
);

create index indx_date on SC3_metrics(scaling_date);
commit;
go

--------------------------------------------------------------- E03 - SC3_metrics

-- This table contains historic data and should not be deleted.
if object_id('SC3_non_convergences') is not null then drop table SC3_non_convergences end if;
CREATE TABLE SC3_non_convergences (
      scaling_date           DATE
     ,scaling_segment_id     int
     ,difference             float
);

create index indx_date on SC3_non_convergences(scaling_date);
commit;
go



