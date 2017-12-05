/************ ESTIMATION OF SEGMENT WEIGHTS FOR THREE VESPA PANELS ************
This code takes part of the RIM-weighting algorithm and splits the VESPA panel
data into three parts, considered to be the adsmartable universe:
1 - Members of the VESPA panels who are non-adsmartable
2 - Members of the VESPA panels who are adsmartable but we consider them to have
    not given viewing consent to replicate non-viewing consent, non-Vespa panellists
3 - Members of the VESPA panel who are adsmartabloe and have given viewing consent.

This algorithm will find the RIM weights for all three sets of VESPA panellists. Note,
that unlike SC2_ weights, we have removed Universe as a variable as it can be inferred
from boxtype.
The data containing the split of the panellists, as well as counts for the Sky base can
be found in V154_accounts_aggregated, with the SQL for this table coming from
vespa_sample

As it uses the code from scaling projects there are many mentions of SC2_ in
the code.
*****************************************************************************/

/**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/
begin

     DECLARE @cntr           INT
     DECLARE @iteration      INT
     DECLARE @cntr_var       SMALLINT
     DECLARE @scaling_var    VARCHAR(30)
     DECLARE @convergence    TINYINT
     DECLARE @sky_base       DOUBLE
     DECLARE @vespa_panel    DOUBLE
     DECLARE @sum_of_weights DOUBLE
     declare @scaling_day    date
     declare @profiling_date date
     declare @QA_catcher     bigint
     commit

SELECT   @scaling_day = '2013-07-14'

     -- Figure out which profiling info we're using
     select @profiling_date = max(profiling_date)
     from vespa_analysts.SC2_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     commit

     -- First adding in the Sky base numbers

if object_id('SC3_weighting_working_table') is not null drop table SC3_weighting_working_table
CREATE TABLE SC3_weighting_working_table (
     scaling_segment_id     INT             primary key
    ,adsmartable_universe   VARCHAR(50)
    ,sky_base_accounts      DOUBLE          not null
    ,vespa_panel            DOUBLE          default 0
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,segment_weight         DOUBLE
    ,indices_actual         DOUBLE
    ,indices_weighted       DOUBLE
)

CREATE HG INDEX indx_un on SC3_weighting_working_table(adsmartable_universe)

COMMIT

--------------------------------------------------------------- E02 - SC2_category_subtotals
-- This table contains historic information and should not be deleted
if object_id('SC3_category_subtotals') is not null drop table SC3_category_subtotals
CREATE TABLE SC3_category_subtotals (
     scaling_date           date
    ,adsmartable_universe   VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence            TINYINT
)

create index indx_date on SC3_category_subtotals(scaling_date)
create hg index indx_universe on SC3_category_subtotals(adsmartable_universe)
create hg index indx_profile on SC3_category_subtotals(profile)

COMMIT
--go

--------------------------------------------------------------- D02 - SC2_category_working_table

-- The scaling category table contains counts of sky base accounts versus vespa
-- accounts at a category level. Rim-weighting aims to converge the sum_of_weights
-- subtotals to the sky base subtotals.

-- This is used to calculate weights on a weekly basis and should not be deleted.

if object_id('SC3_category_working_table') is not null drop table SC3_category_working_table
CREATE TABLE SC3_category_working_table (
     adsmartable_universe   VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence_flag       TINYINT     DEFAULT 1
)

create hg index indx_universe on SC3_category_working_table(adsmartable_universe)
create hg index indx_profile on SC3_category_working_table(profile)
create hg index indx_value on SC3_category_working_table(value)

COMMIT
--go

truncate table SC3_weighting_working_table

INSERT INTO SC3_weighting_working_table (scaling_segment_id, adsmartable_universe, sky_base_accounts, vespa_panel)
select updated_scaling_segment as scaling_segment_id, adsmartable_universe, sky_base_accounts, vespa_accounts
     from V154_accounts_aggregated
commit

-- Rim-weighting is an iterative process that iterates through each of the scaling variables
-- individually until the category sum of weights converge to the population category subtotals

     SET @cntr           = 1
     SET @iteration      = 0
     SET @cntr_var       = 1
     SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)

-- The SC3_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
-- the sky base.
-- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
-- to ensure convergence.

-- arbitrary value to ensure convergence
update SC3_weighting_working_table
set vespa_panel = 0.000001
where vespa_panel = 0

-- Initialise working columns
update SC3_weighting_working_table
set sum_of_weights = vespa_panel

commit

     -- The iterative part.
     -- This works by choosing a particular scaling variable and then summing across the categories
     -- of that scaling variable for the sky base, the vespa panel and the sum of weights.
     -- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
     -- for that category.
     -- This category weight is then applied back to the segments table and the process repeats until
     -- the sum_of_weights in the category table converges to the sky base subtotal.

     -- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
     -- base category subtotal within 100 iterations.
     -- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0

     -- The @convergence variable represents how many categories did not converge.
     -- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
     -- has not converged for this particular day.
     -- In this scenario, the person running the code should send the results of the SC3_metrics for that
     -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

     WHILE @cntr <6
     BEGIN
             DELETE FROM SC3_category_working_table

             SET @cntr_var = 1
             WHILE @cntr_var < 6
             BEGIN
                         SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr_var

                         EXECUTE('
                         INSERT INTO SC3_category_working_table (adsmartable_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             SELECT  srs.adsmartable_universe
                                     ,@scaling_var
                                    ,ssl.'||@scaling_var||'
                                    ,SUM(srs.sky_base_accounts)
                                    ,SUM(srs.vespa_panel)
                                    ,SUM(srs.sum_of_weights)
                              FROM SC3_weighting_working_table AS srs
                                     inner join V154_accounts_aggregated AS ssl ON srs.scaling_segment_id = ssl.updated_scaling_segment
                             GROUP BY srs.adsmartable_universe,ssl.'||@scaling_var||'
                             ORDER BY srs.adsmartable_universe
                         ')

                         SET @cntr_var = @cntr_var + 1
             END

             commit

             UPDATE SC3_category_working_table
             SET  category_weight = sky_base_accounts / sum_of_weights
                 ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

             SELECT @convergence = SUM(convergence_flag) FROM SC3_category_working_table
             SET @iteration = @iteration + 1
             SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr

             EXECUTE('
             UPDATE SC3_weighting_working_table
             SET  SC3_weighting_working_table.category_weight = sc.category_weight
                 ,SC3_weighting_working_table.sum_of_weights  = SC3_weighting_working_table.sum_of_weights * sc.category_weight
             FROM SC3_weighting_working_table
                     inner join V154_accounts_aggregated AS ssl ON SC3_weighting_working_table.scaling_segment_id = ssl.updated_scaling_segment
                     inner join SC3_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                      AND sc.adsmartable_universe = ssl.adsmartable_universe
             ')

             commit

             IF @iteration = 100 OR @convergence = 0 SET @cntr = 6
             ELSE

             IF @cntr = 5  SET @cntr = 1
             ELSE
             SET @cntr = @cntr+1

     END

     commit

     -- Calculate segment weight and corresponding indices

     -- This section calculates the segment weight which is the weight that should be applied to viewing data
     -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


     SELECT @sky_base = SUM(sky_base_accounts) FROM SC3_weighting_working_table
     SELECT @vespa_panel = SUM(vespa_panel) FROM SC3_weighting_working_table
     SELECT @sum_of_weights = SUM(sum_of_weights) FROM SC3_weighting_working_table

     UPDATE SC3_weighting_working_table
     SET  segment_weight = sum_of_weights / vespa_panel
         ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
         ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

     commit


     -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     update SC3_weighting_working_table
     set segment_weight  = 0.000001
     where vespa_panel   = 0.000001

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from SC3_weighting_working_table
     where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence

     commit

     -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

     INSERT INTO SC3_category_subtotals (scaling_date,adsmartable_universe,profile,value,sky_base_accounts,vespa_panel,category_weight
                                              ,sum_of_weights, convergence)
     SELECT  @scaling_day
             ,adsmartable_universe
             ,profile
             ,value
             ,sky_base_accounts
             ,vespa_panel
             ,category_weight
             ,sum_of_weights
             ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     FROM SC3_category_working_table

     -- The SC3_metrics table contains metrics for a particular scaling date. It shows whether the
     -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     commit


end

select wei.scaling_segment_id, agg.*, wei.segment_weight
        into V154_full_accounts_aggregated
        from V154_accounts_aggregated agg
  inner join SC3_weighting_working_table wei
          on agg.updated_scaling_segment = wei.scaling_segment_id


-- select * into old_SC3_weighting_working_table from SC3_weighting_working_table
-- select * into old_SC3_category_subtotals from SC3_category_subtotals
-- select * into old_SC3_category_working_table from SC3_category_working_table

