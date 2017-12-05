/******************************************************************************
**
** Project Vespa: Scaling 2 - Core table creation
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

if object_id('SC2_Weightings') is not null then drop table SC2_Weightings end if;
create table SC2_Weightings (
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

create date index idx1 on SC2_Weightings(scaling_day);
create hg index idx2 on SC2_Weightings(scaling_segment_ID);

grant select on SC2_Weightings to vespa_group_low_security, sk_prodreg;

commit;
go

--------------------------------------------------------------- A02 - SC2_Intervals
-- Assigns a scaling segment to an account over a period of time.
-- It's no longer just raw dialback, there are already multiroom
-- and reporting consistency artefacts int there. Virtual panel
-- balance will also manifest in there too.

if object_id('SC2_Intervals') is not null then drop table SC2_Intervals end if;
create table SC2_Intervals (
    account_number              varchar(20)     not null
    ,reporting_starts           date            not null
    ,reporting_ends             date            not null
    ,scaling_segment_ID         int             not null        -- links to the segments lookup table
    ,primary key (account_number, reporting_starts)             -- Won't bother forcing the no-overlap in DB constraints, but this is a good start
);

create index for_joining on SC2_Intervals (scaling_segment_ID, reporting_starts);
create hg index idx1 on SC2_Intervals (account_number);

grant select on SC2_Intervals to vespa_group_low_security, sk_prodreg;

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

--------------------------------------------------------------- B01 - SC2_Sky_base_segment_snapshots
-- This table stores snapshots of how the Sky base is cut up by the defined scaling
-- segmentation. We also have a profiling date on there because we might end up
-- caching several iterations of profiling, given that the customer DB is the
-- computationally expecnsive part and if all this is done, the Vespa 2 drop will
-- end up comparatively fast to cache. It's not part of the external set becauase...
-- well, it might be quite large, and we might kill bits of it after all the caching
-- to the more eficient tables is complete.
if object_id('SC2_Sky_base_segment_snapshots') is not null then drop table SC2_Sky_base_segment_snapshots end if;
create table SC2_Sky_base_segment_snapshots (
    account_number              varchar(20)     not null
    ,profiling_date             date            not null
    ,cb_key_household           bigint          not null    -- needed for VIQ interface
    ,scaling_segment_id         bigint
    ,expected_boxes             tinyint                     -- number of boxes in household; need to check they're all reporting
    ,primary key (account_number, profiling_date)
);


--------------------------------------------------------------- B02 - SC2_Todays_panel_members
-- The result of the daily panel selection module, this is a list of accounts
-- that are considered to be on the Vespa panel for the processing day in
-- question, and which segment they're in.

if object_id('SC2_Todays_panel_members') is not null then drop table SC2_Todays_panel_members end if;
create table SC2_Todays_panel_members (
    account_number              varchar(20)     not null primary key
    ,scaling_segment_id         bigint          not null
);

--------------------------------------------------------------- B03 - SC2_Todays_segment_weights
-- The results of the weight calculation module, this tells us what the
-- weights are for each scaling segment for whichever day is being
-- processed.

if object_id('SC2_Todays_segment_weights') is not null then drop table SC2_Todays_segment_weights end if;
create table SC2_Todays_segment_weights (
    scaling_segment_id          bigint          not null primary key
    ,scaling_weighting          float           not null
);

commit;
go

--------------------------------------------------------------------------------
-- PART C OTHER INTERNAL PROCESSING STUFF
--------------------------------------------------------------------------------

--------------------------------------------------------------- C01 - SC2_scaling_weekly_sample

-- The SC2_scaling_weekly_sample contains profile information for the accounts that
-- are being scaled. The profile information is re-calculated on a weekly basis.

-- This is used to calculate weights on a weekly basis and should not be deleted.
-- IF object_id('SC2_scaling_weekly_sample') IS NOT NULL
--    DROP TABLE SC2_scaling_weekly_sample;

if object_id('SC2_scaling_weekly_sample') is not null then drop table SC2_scaling_weekly_sample end if;
CREATE TABLE SC2_scaling_weekly_sample (
     account_number                     VARCHAR(20)     primary key
    ,cb_key_household                   BIGINT          not null            -- Needed for VIQ interim solution
    ,cb_key_individual                  BIGINT          not null            -- For ConsumerView linkage
    ,consumerview_cb_row_id             BIGINT                              -- Not really needed for consumerview linkage, but whatever
    ,universe                           VARCHAR(20)                         -- Single, Dual or Multiple box household
    ,isba_tv_region                     VARCHAR(20)                         -- Scaling variable 1 : Region
    ,hhcomposition                      VARCHAR(2)      DEFAULT 'U'         -- Scaling variable 2: Household composition from Experian Consumerview
    ,tenure                             VARCHAR(15)     DEFAULT 'E) Unknown'-- Scaling variable 3: Tenure
    ,num_mix                            INT
    ,mix_pack                           VARCHAR(20)
    ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
    ,boxtype                            VARCHAR(35)                         -- Scaling variable 5: Household boxtype (ranked)
    ,scaling_segment_id                 INT             DEFAULT NULL        -- segment scaling id for identifying segments
    ,mr_boxes                           INT
--    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data - DISCONTINUED; now interfaces with scoring module via defined tables
);

CREATE INDEX for_segment_identification_raw ON SC2_scaling_weekly_sample
    (universe, isba_tv_region,hhcomposition, tenure, package, boxtype);     -- Might it be this one guy? this index rebuild making everything super slow? But it should be going in as a single atomic commit... but on inserts, it still only takes 55 sec...
CREATE INDEX experian_joining ON SC2_scaling_weekly_sample (consumerview_cb_row_id);
CREATE INDEX for_grouping ON SC2_scaling_weekly_sample (scaling_segment_ID);

-- We don't join on this, it's just to force the data constraint we need for VIQ
--create unique index for_VIQ_uniqueness ON SC2_scaling_weekly_sample (cb_key_household);
-- Hah! except that the index fails because of dupes, we're going to have to pick that
-- up somehow in the publishing step. It might not manifest so much when e get down into
-- the Vespa panel... no, it really will, given the number of duplicates we've got. It's
-- still going to be an issue.

COMMIT;
go

--------------------------------------------------------------------------------
-- PART D RIM-WEIGHTING TABLES
--------------------------------------------------------------------------------

--------------------------------------------------------------- D01 - SC2_weighting_working_table

-- The segments table contains counts of sky base accounts versus vespa panel
-- accounts at a segment level*. This segment table is used to calculate segment
-- weights using rim-weighting.

-- *a segment is a combination of the scaling variables.

-- This is used to calculate weights on a weekly basis and should not be deleted.
-- this guy used to be the Scaling_segments table

if object_id('SC2_weighting_working_table') is not null then drop table SC2_weighting_working_table end if;
CREATE TABLE SC2_weighting_working_table (
    scaling_segment_id      INT             primary key
    ,universe               VARCHAR(50)
    ,sky_base_accounts      DOUBLE          not null
    ,vespa_panel            DOUBLE          default 0
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,segment_weight         DOUBLE
    ,indices_actual         DOUBLE
    ,indices_weighted       DOUBLE
);

CREATE HG INDEX indx_un on SC2_weighting_working_table(universe);

COMMIT;
go

--------------------------------------------------------------- D02 - SC2_category_working_table

-- The scaling category table contains counts of sky base accounts versus vespa
-- accounts at a category level. Rim-weighting aims to converge the sum_of_weights
-- subtotals to the sky base subtotals.

-- This is used to calculate weights on a weekly basis and should not be deleted.

if object_id('SC2_category_working_table') is not null then drop table SC2_category_working_table end if;
CREATE TABLE SC2_category_working_table (
     universe               VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence_flag       TINYINT     DEFAULT 1
);


create hg index indx_universe on SC2_category_working_table(universe);
create hg index indx_profile on SC2_category_working_table(profile);
create hg index indx_value on SC2_category_working_table(value);

COMMIT;
go

--------------------------------------------------------------- D03 - Scaling_box_level_viewing

-- No longer in play; now a temp table

--------------------------------------------------------------------------------
-- PART E QA TABLES
--------------------------------------------------------------------------------

--------------------------------------------------------------- E01 - Scaling_segments_subtotals

-- No longer in play; absorbed into Scaling_segments_subtotals

--------------------------------------------------------------- E02 - SC2_category_subtotals
-- This table contains historic information and should not be deleted
if object_id('SC2_category_subtotals') is not null then drop table SC2_category_subtotals end if;
CREATE TABLE SC2_category_subtotals (
     scaling_date           date
    ,universe               VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence            TINYINT
);

create index indx_date on SC2_category_subtotals(scaling_date);
create hg index indx_universe on SC2_category_subtotals(universe);
create hg index indx_profile on SC2_category_subtotals(profile);

COMMIT;
go

--------------------------------------------------------------- E03 - SC2_metrics

-- This table contains historic data and should not be deleted.
if object_id('SC2_metrics') is not null then drop table SC2_metrics end if;
CREATE TABLE SC2_metrics (
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

create index indx_date on SC2_metrics(scaling_date);
commit;
go

--------------------------------------------------------------- E03 - SC2_metrics

-- This table contains historic data and should not be deleted.
if object_id('SC2_non_convergences') is not null then drop table SC2_non_convergences end if;
CREATE TABLE SC2_non_convergences (
      scaling_date           DATE
     ,scaling_segment_id     int
     ,difference             float
);

create index indx_date on SC2_non_convergences(scaling_date);
commit;
go

