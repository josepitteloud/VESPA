/****************** JACKNIFE ESTIMATION OF SEGMENT WEIGHTS ******************
This code takes part of the RIM-weighting algorithm and incorporates it as part
of a jack-knife procedure to find an estimation of the variance of the weights'
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
     declare @jacknifecntr   int
     declare @jackniferm     int
     declare @maxjacknifeiter int
     declare @max_id         int
     commit

SELECT   @scaling_day = '2013-06-01'

     -- Figure out which profiling info we're using
     select @profiling_date = max(profiling_date)
     from vespa_analysts.SC2_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     commit

     -- First adding in the Sky base numbers

if object_id('MEERR_weighting_working_table') is not null drop table MEERR_weighting_working_table
CREATE TABLE MEERR_weighting_working_table (
     scaling_segment_id      INT             primary key
    ,universe               VARCHAR(50)
    ,sky_base_accounts      DOUBLE          not null
    ,vespa_panel            DOUBLE          default 0
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,segment_weight         DOUBLE
    ,indices_actual         DOUBLE
    ,indices_weighted       DOUBLE
)

CREATE HG INDEX indx_un on MEERR_weighting_working_table(universe)

COMMIT

--------------------------------------------------------------- E02 - SC2_category_subtotals
-- This table contains historic information and should not be deleted
if object_id('MEERR_category_subtotals') is not null drop table MEERR_category_subtotals
CREATE TABLE MEERR_category_subtotals (
     scaling_date           date
    ,universe               VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence            TINYINT
)

create index indx_date on MEERR_category_subtotals(scaling_date)
create hg index indx_universe on MEERR_category_subtotals(universe)
create hg index indx_profile on MEERR_category_subtotals(profile)

COMMIT
--go

--------------------------------------------------------------- D02 - SC2_category_working_table

-- The scaling category table contains counts of sky base accounts versus vespa
-- accounts at a category level. Rim-weighting aims to converge the sum_of_weights
-- subtotals to the sky base subtotals.

-- This is used to calculate weights on a weekly basis and should not be deleted.

if object_id('MEERR_category_working_table') is not null drop table MEERR_category_working_table
CREATE TABLE MEERR_category_working_table (
     universe               VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence_flag       TINYINT     DEFAULT 1
)

create hg index indx_universe on MEERR_category_working_table(universe)
create hg index indx_profile on MEERR_category_working_table(profile)
create hg index indx_value on MEERR_category_working_table(value)

COMMIT
--go

truncate table MEERR_weighting_working_table

INSERT INTO MEERR_weighting_working_table (scaling_segment_id, universe, sky_base_accounts)
select a.scaling_segment_id, max(b.universe), count(1)
     from vespa_analysts.SC2_Sky_base_segment_snapshots a,
          vespa_analysts.SC2_Segments_lookup_v2_1 b
     where a.profiling_date = @profiling_date
       and a.scaling_segment_id = b.scaling_segment_id
     group by a.scaling_segment_id
commit

-- Rim-weighting is an iterative process that iterates through each of the scaling variables
-- individually until the category sum of weights converge to the population category subtotals

     SET @cntr           = 1
     SET @iteration      = 0
     SET @cntr_var       = 1
     SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)

-- The MEERR_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
-- the sky base.
-- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
-- to ensure convergence.

-- arbitrary value to ensure convergence
update MEERR_weighting_working_table
set vespa_panel = 0.000001
where vespa_panel = 0

--create temp table having counts of each segment on the vespa panel
if object_id('#MEERR_vespa_panel') is not null drop table #MEERR_vespa_panel
select scaling_segment_id, count(*) as vespa_panellists
         into #MEERR_vespa_panel
         from vespa_analysts.SC2_intervals
        where reporting_starts <= @scaling_day
          and reporting_ends   >= @scaling_day
     group by scaling_segment_id

-- set no. of vespa panellists
update MEERR_weighting_working_table
set vespa_panel = vespa_panellists
from MEERR_weighting_working_table a, #MEERR_vespa_panel b
where a.scaling_segment_id = b.scaling_segment_id

commit

-- Initialise working columns
update MEERR_weighting_working_table
set sum_of_weights = vespa_panel

commit

--copy MEERR_weighting_working_table into temp_weighting_working_table for use later with the jackknife iteration
if object_id('temp_weighting_working_table') is not null drop table temp_weighting_working_table
select *
        into temp_weighting_working_table
        from MEERR_weighting_working_table
CREATE HG INDEX indx_temp_un on temp_weighting_working_table(universe)

COMMIT


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
     -- In this scenario, the person running the code should send the results of the MEERR_metrics for that
     -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

     WHILE @cntr <6
     BEGIN
             DELETE FROM MEERR_category_working_table

             SET @cntr_var = 1
             WHILE @cntr_var < 6
             BEGIN
                         SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr_var

                         EXECUTE('
                         INSERT INTO MEERR_category_working_table (universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             SELECT  srs.universe
                                     ,@scaling_var
                                    ,ssl.'||@scaling_var||'
                                    ,SUM(srs.sky_base_accounts)
                                    ,SUM(srs.vespa_panel)
                                    ,SUM(srs.sum_of_weights)
                              FROM MEERR_weighting_working_table AS srs
                                     inner join vespa_analysts.SC2_Segments_lookup_v2_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                             GROUP BY srs.universe,ssl.'||@scaling_var||'
                             ORDER BY srs.universe
                         ')

                         SET @cntr_var = @cntr_var + 1
             END

             commit

             UPDATE MEERR_category_working_table
             SET  category_weight = sky_base_accounts / sum_of_weights
                 ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

             SELECT @convergence = SUM(convergence_flag) FROM MEERR_category_working_table
             SET @iteration = @iteration + 1
             SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr

             EXECUTE('
             UPDATE MEERR_weighting_working_table
             SET  MEERR_weighting_working_table.category_weight = sc.category_weight
                 ,MEERR_weighting_working_table.sum_of_weights  = MEERR_weighting_working_table.sum_of_weights * sc.category_weight
             FROM MEERR_weighting_working_table
                     inner join vespa_analysts.SC2_Segments_lookup_v2_1 AS ssl ON MEERR_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id
                     inner join MEERR_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                      AND sc.universe = ssl.universe
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


     SELECT @sky_base = SUM(sky_base_accounts) FROM MEERR_weighting_working_table
     SELECT @vespa_panel = SUM(vespa_panel) FROM MEERR_weighting_working_table
     SELECT @sum_of_weights = SUM(sum_of_weights) FROM MEERR_weighting_working_table

     UPDATE MEERR_weighting_working_table
     SET  segment_weight = sum_of_weights / vespa_panel
         ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
         ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

     commit


     -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     update MEERR_weighting_working_table
     set segment_weight  = 0.000001
     where vespa_panel   = 0.000001

     commit

     set @QA_catcher = -1

     select @QA_catcher = count(1)
     from MEERR_weighting_working_table
     where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence

     commit

     -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

     INSERT INTO MEERR_category_subtotals (scaling_date,universe,profile,value,sky_base_accounts,vespa_panel,category_weight
                                              ,sum_of_weights, convergence)
     SELECT  @scaling_day
             ,universe
             ,profile
             ,value
             ,sky_base_accounts
             ,vespa_panel
             ,category_weight
             ,sum_of_weights
             ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     FROM MEERR_category_working_table

     -- The MEERR_metrics table contains metrics for a particular scaling date. It shows whether the
     -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     commit

     --Jackknife estimation of the weights
     --Jackknife works by, for each iteration, setting a number of sample sizes of the scaling segments
     --to zero and rerunning the RIM-weighting algorithm. The number of samples set to zero should be
     --about equal each iteration and each scaling segment should be set to zero only once during the whole
     --jackknife procedure.
     --After a number of iterations we have a number of estimates of the weights, and one estimate of 0.00001
     --when the sample size was set to zero. Using all estimates of the weights when they are greater than
     --0.00001 (to avoid biasing our examples) we can calculate the variance of our segment weights.

if object_id('temp_category_subtotals') is not null drop table temp_category_subtotals
CREATE TABLE temp_category_subtotals (
     scaling_date           date
    ,universe               VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence            TINYINT
)

create index indx_temp_date on temp_category_subtotals(scaling_date)
create hg index indx_temp_universe on temp_category_subtotals(universe)
create hg index indx_temp_profile on temp_category_subtotals(profile)

COMMIT
--go

--------------------------------------------------------------- D02 - SC2_category_working_table

-- The scaling category table contains counts of sky base accounts versus vespa
-- accounts at a category level. Rim-weighting aims to converge the sum_of_weights
-- subtotals to the sky base subtotals.

-- This is used to calculate weights on a weekly basis and should not be deleted.

if object_id('temp_category_working_table') is not null drop table temp_category_working_table
CREATE TABLE temp_category_working_table (
     universe               VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence_flag       TINYINT     DEFAULT 1
)


create hg index indx_temp_universe on temp_category_working_table(universe)
create hg index indx_temp_profile on temp_category_working_table(profile)
create hg index indx_temp_value on temp_category_working_table(value)

COMMIT

     alter table temp_weighting_working_table add random_number real
     update temp_weighting_working_table
        set random_number =  RAND(NUMBER(*)*(DATEPART(MS,NOW())+1))
      where vespa_panel > 0.000001
create hg index indx_temp_rand on temp_weighting_working_table(random_number)

if object_id('random_ordered_scaling_segments') is not null drop table random_ordered_scaling_segments
     select *
        into random_ordered_scaling_segments
        from temp_weighting_working_table
       where vespa_panel > 0.000001
    order by random_number
 alter table random_ordered_scaling_segments add row_id INT IDENTITY

     commit

     set @jacknifecntr = 1
     set @jackniferm   = (select floor(1.0*count(*)/40) from temp_weighting_working_table where vespa_panel > 0.000001)
     set @maxjacknifeiter  = (select ceiling(1.0*count(*)/@jackniferm) from temp_weighting_working_table where vespa_panel > 0.000001)
     commit

--create table holdings iterations and estimates
if object_id('MEERR_jackknife_weights_table') is not null drop table MEERR_jackknife_weights_table
CREATE TABLE MEERR_jackknife_weights_table (
     iteration              INT
    ,scaling_segment_id     INT
    ,segment_weight         DOUBLE
)

     -- The jackknife part
     -- set a number of vespa panel segments to zero size
     WHILE @jacknifecntr <= @maxjacknifeiter
     BEGIN

     SET @max_id =
        CASE WHEN  @jacknifecntr < @maxjacknifeiter
             THEN  @jacknifecntr * @jackniferm
             ELSE (SELECT COUNT(*) from random_ordered_scaling_segments where vespa_panel > 0.000001)
        END

     SET @cntr           = 1
     SET @iteration      = 0
     SET @cntr_var       = 1
     SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)


     if object_id('temp_weighting_working_table2') is not null drop table temp_weighting_working_table2
     select *
        into temp_weighting_working_table2
        from temp_weighting_working_table

      update temp_weighting_working_table2
         set vespa_panel = 0.000001
       where scaling_segment_id in (
                select scaling_segment_id
                  from random_ordered_scaling_segments
                 where row_id > (@jacknifecntr-1)*@jackniferm
                   and row_id <= @max_id)
      update temp_weighting_working_table2
         set sum_of_weights = vespa_panel

        -- The iterative part from before. Need to check if all these steps are required

         WHILE @cntr <6
         BEGIN
                 DELETE FROM temp_category_working_table

                 SET @cntr_var = 1
                 WHILE @cntr_var < 6
                 BEGIN
                             SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr_var

                             EXECUTE('
                             INSERT INTO temp_category_working_table (universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                                 SELECT  srs.universe
                                         ,@scaling_var
                                        ,ssl.'||@scaling_var||'
                                        ,SUM(srs.sky_base_accounts)
                                        ,SUM(srs.vespa_panel)
                                        ,SUM(srs.sum_of_weights)
                                  FROM temp_weighting_working_table2 AS srs
                                         inner join vespa_analysts.SC2_Segments_lookup_v2_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                                 GROUP BY srs.universe,ssl.'||@scaling_var||'
                                 ORDER BY srs.universe
                             ')

                             SET @cntr_var = @cntr_var + 1
                 END

                 commit

                 UPDATE temp_category_working_table
                 SET  category_weight = sky_base_accounts / sum_of_weights
                     ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

                 SELECT @convergence = SUM(convergence_flag) FROM MEERR_category_working_table
                 SET @iteration = @iteration + 1
                 SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr

                 EXECUTE('
                 UPDATE temp_weighting_working_table2
                 SET  temp_weighting_working_table2.category_weight = sc.category_weight
                     ,temp_weighting_working_table2.sum_of_weights  = temp_weighting_working_table2.sum_of_weights * sc.category_weight
                 FROM temp_weighting_working_table2
                         inner join vespa_analysts.SC2_Segments_lookup_v2_1 AS ssl ON temp_weighting_working_table2.scaling_segment_id = ssl.scaling_segment_id
                         inner join temp_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                          AND sc.universe = ssl.universe
                 ')

                 commit

                 IF @iteration = 100 OR @convergence = 0 SET @cntr = 6
                 ELSE

                 IF @cntr = 5  SET @cntr = 1
                 ELSE
                 SET @cntr = @cntr+1

         END

         commit

     SELECT @sky_base = SUM(sky_base_accounts) FROM temp_weighting_working_table2
     SELECT @vespa_panel = SUM(vespa_panel) FROM temp_weighting_working_table2
     SELECT @sum_of_weights = SUM(sum_of_weights) FROM temp_weighting_working_table2

     UPDATE temp_weighting_working_table2
     SET  segment_weight = sum_of_weights / vespa_panel
         ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
         ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

     commit


     -- OK, now catch those cases where stuff diverged because segments weren't reprresented:
     update temp_weighting_working_table2
     set segment_weight  = 0.000001
     where vespa_panel   = 0.000001

     commit

   insert into MEERR_jackknife_weights_table (iteration, scaling_segment_id, segment_weight)
        select @jacknifecntr, scaling_segment_id, segment_weight
          from temp_weighting_working_table2

        set @jacknifecntr = @jacknifecntr + 1

    end

   commit

   --Find weighting variances
   if object_id('MEERR_jackknife_weight_means') is not null drop table MEERR_jackknife_weight_means
CREATE TABLE MEERR_jackknife_weight_means (
     scaling_segment_id INT
    ,avg_weights        real
)
       insert into MEERR_jackknife_weight_means
       select scaling_segment_id, avg(segment_weight) as avg_weights
         from MEERR_jackknife_weights_table
        where segment_weight > 0.00001
     group by scaling_segment_id
    commit

   if object_id('MEERR_jackknife_weight_vars') is not null drop table MEERR_jackknife_weight_vars
CREATE TABLE MEERR_jackknife_weight_vars (
     scaling_segment_id INT
    ,square_sums        real
    ,avg_weights        real
    ,var_weights        real
)

       insert into MEERR_jackknife_weight_vars(scaling_segment_id, square_sums)
       select scaling_segment_id, sum(POWER(segment_weight, 2))
         from MEERR_jackknife_weights_table
        where segment_weight > 0.00001
     group by scaling_segment_id

       update MEERR_jackknife_weight_vars
          set avg_weights = m.avg_weights
         from MEERR_jackknife_weight_means m
        where m.scaling_segment_id = MEERR_jackknife_weight_vars.scaling_segment_id

      declare @min_seg_no  int
          set @min_seg_no = (select min(scaling_segment_id) from MEERR_jackknife_weight_means)
      declare @n  int
          set @n = (select count(*)
                        from MEERR_jackknife_weights_table
                       where scaling_segment_id = @min_seg_no
                         and segment_weight > 0.00001)

       update MEERR_jackknife_weight_vars
          set var_weights = (square_sums - @n*avg_weights*avg_weights)/(@n - 1)

    commit

end

