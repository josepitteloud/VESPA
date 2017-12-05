/*
** Update of old Scaling 2.0 procedure. Namely, simple replacement of SC2 suffix with SC3_
*/



-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
--            NOT TO BE RUN IN VESPA_ANALYST SCHEMA SINCE IT WIPES OUT ALL THE DATA !!!!!!!!!
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


if object_id('SC3_Recreate_Env') IS NOT NULL THEN DROP PROCEDURE SC3_Recreate_Env end if;

create procedure SC3_Recreate_Env as
begin

--------------------------------------------------------------- A01 - SC2_Weightings
-- Assigns weights to scaling segments by day. Also has a few other
-- bits like number of Vespa panel and Sky base acocunts to assist
-- with more involved scaling methodologies, they might help.


    if object_id('SC3_Weightings') is not null drop table SC3_Weightings
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
    )

    create date index idx1 on SC3_Weightings(scaling_day)
    create hg index idx2 on SC3_Weightings(scaling_segment_ID)

    commit

    --------------------------------------------------------------- A02 - SC3_Intervals
    -- Assigns a scaling segment to an account over a period of time.
    -- It's no longer just raw dialback, there are already multiroom
    -- and reporting consistency artefacts int there. Virtual panel
    -- balance will also manifest in there too.

    if object_id('SC3_Intervals') is not null drop table SC3_Intervals
    create table SC3_Intervals (
        account_number              varchar(20)     not null
        ,reporting_starts           date            not null
        ,reporting_ends             date            not null
        ,scaling_segment_ID         int             not null        -- links to the segments lookup table
        ,primary key (account_number, reporting_starts)             -- Won't bother forcing the no-overlap in DB constraints, but this is a od start
    )

    create index for_joining on SC3_Intervals (scaling_segment_ID, reporting_starts)
    create hg index idx1 on SC3_Intervals (account_number)

    commit

    --------------------------------------------------------------- A04 - Vespa_Household_Weighting
    -- VIQII wants its weights delivered in a particular format this format.
    if object_id('Vespa_Household_Weighting') is not null drop table Vespa_Household_Weighting
    create table Vespa_Household_Weighting (
        account_number              varchar(20)     not null
        ,cb_key_household           bigint          not null
        ,scaling_date               date            not null        -- date on which scaling is applied
        ,scaling_weighting          float           not null
        ,build_date                 datetime        not null        -- tracking processing to assist VIQ loads
        ,primary key (account_number, scaling_date)
    )

    create dttm index for_loading on Vespa_Household_Weighting (build_date)
    create hg index idx1 on Vespa_Household_Weighting (account_number)
    create hg index idx2 on Vespa_Household_Weighting (cb_key_household)
    create date index idx3 on Vespa_Household_Weighting (scaling_date)

    commit


    --------------------------------------------------------------------------------
    -- PART B OTHER INTERNAL PROCESSING STUFF
    --------------------------------------------------------------------------------
    -- Scaling has a couple of places where established modules do work and  through
    -- defined interfaces between these. (Other tightly coupled procedures like PanMan
    -- also hook into some of these structures.) Module rules can be changed and the
    -- rest of the code can remain provided that the changes don't entail structural
    -- changes to these internal interfaces.

    --------------------------------------------------------------- B01 - SC3_Sky_Base_Segment_Snapshots
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
    if object_id('SC3_Sky_Base_Segment_Snapshots') is not null drop table SC3_Sky_Base_Segment_Snapshots
    create table SC3_Sky_Base_Segment_Snapshots (
        account_number              varchar(20)     not null
        ,profiling_date             date            not null
        ,cb_key_household           bigint          not null    -- needed for VIQ interface
        ,population_scaling_segment_id  bigint
        ,vespa_scaling_segment_id       bigint
        ,expected_boxes                 tinyint                     -- number of boxes in household need to check they're all reporting
        ,primary key (account_number, profiling_date)
    )

    --------------------------------------------------------------- B02 - SC3_Todays_Panel_Members
    -- The result of the daily panel selection module, this is a list of accounts
    -- that are considered to be on the Vespa panel for the processing day in
    -- question, and which segment they're in.

    if object_id('SC3_Todays_Panel_Members') is not null drop table SC3_Todays_Panel_Members
    create table SC3_Todays_Panel_Members (
        account_number              varchar(20)     not null primary key
        ,scaling_segment_id         bigint          not null
    )

    --------------------------------------------------------------- B03 - SC3_Todays_segment_weights
    -- The results of the weight calculation module, this tells us what the
    -- weights are for each scaling segment for whichever day is being
    -- processed.
    if object_id('SC3_Todays_segment_weights') is not null drop table SC3_Todays_segment_weights
    create table SC3_Todays_segment_weights (
        scaling_segment_id          bigint          not null primary key
        ,scaling_weighting          float           not null
    )

    commit


    --------------------------------------------------------------------------------
    -- PART C OTHER INTERNAL PROCESSING STUFF
    --------------------------------------------------------------------------------

    --------------------------------------------------------------- C01 - SC3_Scaling_Weekly_Sample

    -- The SC3_Scaling_Weekly_Sample contains profile information for the accounts that
    -- are being scaled. The profile information is re-calculated on a weekly basis.

    -- This is used to calculate weights on a weekly basis and should not be deleted.
    -- IF object_id('SC3_Scaling_Weekly_Sample') IS NOT NULL
    --    DROP TABLE SC3_Scaling_Weekly_Sample

    if object_id('SC3_Scaling_Weekly_Sample') is not null drop table SC3_Scaling_Weekly_Sample
    CREATE TABLE SC3_Scaling_Weekly_Sample (
         account_number                     VARCHAR(20)     primary key
        ,cb_key_household                   BIGINT          not null            -- Needed for VIQ interim solution
        ,cb_key_individual                  BIGINT          not null            -- For ConsumerView linkage
        ,consumerview_cb_row_id             BIGINT                              -- Not really needed for consumerview linkage, but whatever
        ,universe                           VARCHAR(20)                         -- Single, Dual or Multiple box household
        ,sky_base_universe                  VARCHAR(30)                         -- Not adsmartable, Adsmartable with consent, Adsmartable but no consent household
        ,vespa_universe                     VARCHAR(30)                         -- Universe used for Vespa
        ,weighting_universe                 VARCHAR(30)                         -- Universe used for weighting purposes
        ,isba_tv_region                     VARCHAR(30)                         -- Scaling variable 1 : Region
        ,hhcomposition                      VARCHAR(2)      DEFAULT 'U'         -- Scaling variable 2: Household composition from Experian Consumerview
        ,tenure                             VARCHAR(15)     DEFAULT 'E) Unknown'-- Scaling variable 3: Tenure
        ,num_mix                            INT
        ,mix_pack                           VARCHAR(20)
        ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
        ,boxtype                            VARCHAR(35)                         -- Scaling variable 5: Household boxtype (ranked)
        ,no_of_stbs                         VARCHAR(15)                         -- Scaling variable 5: No of set top boxes
        ,hd_subscription                    VARCHAR(5)                          -- Scaling variable 6: HD subscription
        ,pvr                                VARCHAR(5)                          -- Scaling variable 6: Is the box pvr capable?
        ,population_scaling_segment_id      INT             DEFAULT NULL        -- segment scaling id for identifying segments in population
        ,vespa_scaling_segment_id           INT             DEFAULT NULL        -- segment scaling id for identifying segments used in rim weighting
        ,mr_boxes                           INT
    --    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data - DISCONTINUED now interfaces with scoring module via defined tables
    )

    CREATE INDEX for_segment_identification_raw ON SC3_Scaling_Weekly_Sample
        (universe, isba_tv_region,hhcomposition, tenure, package, boxtype)     -- Might it be this one guy? this index rebuild making everything super slow? But it should be ing in as a single atomic commit... but on inserts, it still only takes 55 sec...
    CREATE INDEX experian_joining ON SC3_Scaling_Weekly_Sample (consumerview_cb_row_id)
    CREATE INDEX for_grouping ON SC3_Scaling_Weekly_Sample (scaling_segment_ID)

    -- We don't join on this, it's just to force the data constraint we need for VIQ
    --create unique index for_VIQ_uniqueness ON SC3_Scaling_Weekly_Sample (cb_key_household)
    -- Hah! except that the index fails because of dupes, we're ing to have to pick that
    -- up somehow in the publishing step. It might not manifest so much when e get down into
    -- the Vespa panel... no, it really will, given the number of duplicates we've t. It's
    -- still ing to be an issue.

    COMMIT

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

    if object_id('SC3_Weighting_Working_Table') is not null drop table SC3_Weighting_Working_Table
    CREATE TABLE SC3_Weighting_Working_Table (
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

    CREATE HG INDEX indx_un on SC3_Weighting_Working_Table(universe)

    COMMIT

    --------------------------------------------------------------- D02 - SC3_Category_Working_Table

    -- The scaling category table contains counts of sky base accounts versus vespa
    -- accounts at a category level. Rim-weighting aims to converge the sum_of_weights
    -- subtotals to the sky base subtotals.

    -- This is used to calculate weights on a weekly basis and should not be deleted.

    if object_id('SC3_Category_Working_Table') is not null drop table SC3_Category_Working_Table
    CREATE TABLE SC3_Category_Working_Table (
        sky_base_universe      VARCHAR(50)
        ,profile                VARCHAR(50)
        ,value                  VARCHAR(70)
        ,sky_base_accounts      DOUBLE
        ,vespa_panel            DOUBLE
        ,category_weight        DOUBLE
        ,sum_of_weights         DOUBLE
        ,convergence_flag       TINYINT     DEFAULT 1
    )


    create hg index indx_universe on SC3_Category_Working_Table(universe)
    create hg index indx_profile on SC3_Category_Working_Table(profile)
    create hg index indx_value on SC3_Category_Working_Table(value)

    COMMIT



--------------------------------------------------------------- D03 - Scaling_box_level_viewing

-- No longer in play; now a temp table

--------------------------------------------------------------------------------
-- PART E QA TABLES
--------------------------------------------------------------------------------

--------------------------------------------------------------- E01 - Scaling_segments_subtotals

-- No longer in play; absorbed into Scaling_segments_subtotals

    --------------------------------------------------------------- E02 - SC3_category_subtotals
    -- This table contains historic information and should not be deleted
    if object_id('SC3_category_subtotals') is not null drop table SC3_category_subtotals
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
    )

    create index indx_date on SC3_category_subtotals(scaling_date)
    create hg index indx_universe on SC3_category_subtotals(sky_base_universe)
    create hg index indx_profile on SC3_category_subtotals(profile)

    COMMIT

    --------------------------------------------------------------- E03 - SC3_metrics

    -- This table contains historic data and should not be deleted.
    if object_id('SC3_metrics') is not null drop table SC3_metrics
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
    )

    create index indx_date on SC3_metrics(scaling_date)
    commit

    --------------------------------------------------------------- E03 - SC3_metrics

    -- This table contains historic data and should not be deleted.
    if object_id('SC3_non_convergences') is not null drop table SC3_non_convergences
    CREATE TABLE SC3_non_convergences (
          scaling_date           DATE
         ,scaling_segment_id     int
         ,difference             float
    )

    create index indx_date on SC3_non_convergences(scaling_date)
    commit

end
