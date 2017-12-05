-----------------------------------------------------------------------------------------------------------------------------------------




-- IF object_id('V289_M11_04_SC3I_v1_1__make_weights') IS NOT NULL THEN DROP PROCEDURE V289_M11_04_SC3I_v1_1__make_weights END IF;

-- create procedure V289_M11_04_SC3I_v1_1__make_weights
    -- @scaling_day                date                -- Day for which to do scaling; this argument is mandatory
    -- ,@batch_date                datetime = now()    -- Day on which build was kicked off
    -- ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
-- as
-- begin


        -- -- Only need these if we can't get to execute as a Proc
-- /*        declare @scaling_day  date
        -- declare @batch_date date
        -- declare @Scale_refresh_logging_ID bigint
        -- set @scaling_day = '2013-09-26'
        -- set @batch_date = '2014-07-10'
        -- set @Scale_refresh_logging_ID = 5
-- */


     -- -- So by this point we're assuming that the Sky base segmentation is done
     -- -- (for a suitably recent item) and also that today's panel members have
     -- -- been established, and we're just going to go calculate these weights.

     -- DECLARE @cntr           INT
     -- DECLARE @iteration      INT
     -- DECLARE @cntr_var       SMALLINT
     -- DECLARE @scaling_var    VARCHAR(30)
     -- DECLARE @scaling_count  SMALLINT
     -- DECLARE @convergence    TINYINT
     -- DECLARE @sky_base       DOUBLE
     -- DECLARE @vespa_panel    DOUBLE
     -- DECLARE @sum_of_weights DOUBLE
     -- declare @profiling_date date
     -- declare @QA_catcher     bigint

     -- commit



     -- /**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/

     -- -- Figure out which profiling info we're using;
     -- select @profiling_date = max(profiling_date)
     -- from SC3I_Sky_base_segment_snapshots
     -- where profiling_date <= @scaling_day

     -- commit

     -- -- Log the profiling date being used for the build
      -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Making weights for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
      -- commit

     -- -- First adding in the Sky base numbers
     -- delete from SC3I_weighting_working_table
     -- commit

     -- INSERT INTO SC3I_weighting_working_table (scaling_segment_id, sky_base_accounts)
     -- select population_scaling_segment_id, count(1)
     -- from SC3I_Sky_base_segment_snapshots
     -- where profiling_date = @profiling_date
     -- group by population_scaling_segment_id

     -- commit


-- /**************** update SC3I_weighting_working_table
-- -- Keep the totals for age/gender groups the same but apply Barb %

        -- -- Get SkyBase ageband totals
        -- select
                -- 'age_band' as profile
                -- ,age_band as value
                -- ,count(1) as weighted_population
                -- ,9.999 as percent_of_total_pop
        -- into
                -- #skybase_age_gender_totals
        -- from
                -- SC3I_weighting_working_table w
             -- inner join
                -- vespa_analysts.SC3I_Segments_lookup_v1_1 l
             -- on w.scaling_segment_id = l.scaling_segment_id
        -- group by
                -- profile
                -- ,value
                -- ,weighted_population

        -- -- Get SkyBase ageband totals
        -- insert into #skybase_age_gender_totals
        -- select
                -- 'gender' as profile
                -- ,gender as value
                -- ,count(1) as weighted_population
        -- into
                -- #skybase_age_gender_totals
        -- from
                -- SC3I_weighting_working_table w
             -- inner join
                -- vespa_analysts.SC3I_Segments_lookup_v1_1 l
             -- on w.scaling_segment_id = l.scaling_segment_id
        -- group by
                -- profile
                -- ,value
                -- ,weighted_population

        -- -- Calculate the % of Sky base by age group and by gender group
        -- update #skybase_age_gender_totals sb
        -- set percent_of_total_pop = weighted_population / tot_weighted_population
        -- from
                -- (select profile, sum(weighted_population) as tot_weighted_population
                -- from #skybase_age_gender_totals
                -- group by profile) summary
        -- where sb.profile = summary.profile



-- TABLE #barb_age_gender_weighted_population
        -- profile                 e.g. ageband
        -- value                   e.g 34-45
        -- weighted_population     e.g 12,000,000
        -- percent_of_total_pop    e.g. 0.151


        -- -- Calculate the adjustment to apply to age and gender groups so that they have same Barb profile
        -- select sb.profile, value, (bb.percent_of_total_pop / sb.percent_of_total_pop) as sky_adjust
        -- into #skybase_adjust_for_barb
        -- from
                -- #skybase_age_gender_totals sb
             -- inner join
                -- #barb_age_gender_weighted_population bb
             -- on sb.profile = bb.profile and sb.value = bb.value






        -- update SC3I_weighting_working_table w
        -- set sky_base_accounts =
        -- from

        -- select scaling_segment_id,
        -- from
                -- #skybase_age_gender_totals sb
             -- inner join
                -- #barb_age_gender_weighted_population bb





-- */





     -- -- Now tack on the universe flags; a special case of things coming out of the lookup

     -- update SC3I_weighting_working_table
     -- set sky_base_universe = sl.sky_base_universe
     -- from SC3I_weighting_working_table
-- --      inner join vespa_analysts.SC2_Segments_lookup_v1_1 as sl
     -- inner join vespa_analysts.SC3I_Segments_lookup_v1_1 as sl
     -- on SC3I_weighting_working_table.scaling_segment_id = sl.scaling_segment_id

     -- commit

     -- -- Mix in the Vespa panel counts as determined earlier
     -- select scaling_segment_id
         -- ,count(1) as panel_members
     -- into #segment_distribs
     -- from SC3I_Todays_panel_members
     -- where scaling_segment_id is not null
     -- group by scaling_segment_id

     -- commit
     -- create unique index fake_pk on #segment_distribs (scaling_segment_id)
     -- commit

     -- -- It defaults to 0, so we can just poke values in
     -- update SC3I_weighting_working_table
     -- set vespa_panel = sd.panel_members
     -- from SC3I_weighting_working_table
     -- inner join #segment_distribs as sd
     -- on SC3I_weighting_working_table.scaling_segment_id = sd.scaling_segment_id

     -- -- And we're done! log the progress.
     -- commit
     -- drop table #segment_distribs
     -- commit
     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from SC3I_weighting_working_table

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B01: Complete! (Segmentation totals)', coalesce(@QA_catcher, -1)
     -- commit






     -- /**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/

     -- delete from SC3I_category_subtotals where scaling_date = @scaling_day
     -- delete from SC3I_metrics where scaling_date = @scaling_day
     -- commit

     -- -- Rim-weighting is an iterative process that iterates through each of the scaling variables
     -- -- individually until the category sum of weights converge to the population category subtotals

     -- SET @cntr           = 1
     -- SET @iteration      = 0
     -- SET @cntr_var       = 1
-- --      SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)
     -- SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr)
     -- SET @scaling_count  = (SELECT COUNT(scaling_variable) FROM vespa_analysts.SC3I_Variables_lookup_v1_1)

     -- -- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
     -- -- the sky base.
     -- -- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
     -- -- to ensure convergence.

     -- -- arbitrary value to ensure convergence
     -- update SC3I_weighting_working_table
     -- set vespa_panel = 0.000001
     -- where vespa_panel = 0

     -- commit

     -- -- Initialise working columns
     -- update SC3I_weighting_working_table
     -- set sum_of_weights = vespa_panel

     -- commit

     -- -- The iterative part.
     -- -- This works by choosing a particular scaling variable and then summing across the categories
     -- -- of that scaling variable for the sky base, the vespa panel and the sum of weights.
     -- -- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
     -- -- for that category.
     -- -- This category weight is then applied back to the segments table and the process repeats until
     -- -- the sum_of_weights in the category table converges to the sky base subtotal.

     -- -- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
     -- -- base category subtotal within 100 iterations.
     -- -- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0

     -- -- The @convergence variable represents how many categories did not converge.
     -- -- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
     -- -- has not converged for this particular day.
     -- -- In this scenario, the person running the code should send the results of the SC3I_metrics for that
     -- -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

     -- WHILE @cntr <= @scaling_count
     -- BEGIN
             -- DELETE FROM SC3I_category_working_table

             -- SET @cntr_var = 1
             -- WHILE @cntr_var <= @scaling_count
             -- BEGIN
                         -- SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr_var

                         -- EXECUTE('
                         -- INSERT INTO SC3I_category_working_table (sky_base_universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                             -- SELECT  srs.sky_base_universe
                                    -- ,@scaling_var
                                    -- ,ssl.'||@scaling_var||'
                                    -- ,SUM(srs.sky_base_accounts)
                                    -- ,SUM(srs.vespa_panel)
                                    -- ,SUM(srs.sum_of_weights)
                             -- FROM SC3I_weighting_working_table AS srs
                                     -- inner join vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                             -- GROUP BY srs.sky_base_universe,@scaling_var,ssl.'||@scaling_var||'
                             -- ORDER BY srs.sky_base_universe
                         -- ')

                         -- SET @cntr_var = @cntr_var + 1
             -- END

             -- commit

             -- UPDATE SC3I_category_working_table
             -- SET  category_weight = sky_base_accounts / sum_of_weights
                 -- ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

             -- SELECT @convergence = SUM(convergence_flag) FROM SC3I_category_working_table
             -- SET @iteration = @iteration + 1
             -- SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3I_Variables_lookup_v1_1 WHERE id = @cntr

             -- EXECUTE('
             -- UPDATE SC3I_weighting_working_table
             -- SET  SC3I_weighting_working_table.category_weight = sc.category_weight
                 -- ,SC3I_weighting_working_table.sum_of_weights  = SC3I_weighting_working_table.sum_of_weights * sc.category_weight
             -- FROM SC3I_weighting_working_table
                     -- inner join vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON SC3I_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id
                     -- inner join SC3I_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                      -- AND sc.sky_base_universe = ssl.sky_base_universe
                                                                                                                                          -- AND sc.profile=@scaling_var
             -- ')

             -- commit

             -- IF @iteration = 100 OR @convergence = 0 SET @cntr = (@scaling_count + 1)
             -- ELSE

             -- IF @cntr = @scaling_count  SET @cntr = 1
             -- ELSE
             -- SET @cntr = @cntr+1

     -- END

     -- commit
     -- -- This loop build took about 4 minutes. That's fine.

     -- -- Calculate segment weight and corresponding indices

     -- -- This section calculates the segment weight which is the weight that should be applied to viewing data
     -- -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


     -- SELECT @sky_base = SUM(sky_base_accounts) FROM SC3I_weighting_working_table
     -- SELECT @vespa_panel = SUM(vespa_panel) FROM SC3I_weighting_working_table
     -- SELECT @sum_of_weights = SUM(sum_of_weights) FROM SC3I_weighting_working_table

     -- UPDATE SC3I_weighting_working_table
     -- SET  segment_weight = sum_of_weights / vespa_panel
         -- ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
         -- ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

     -- commit

     -- -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     -- update SC3I_weighting_working_table
     -- set segment_weight  = 0.000001
     -- where vespa_panel   = 0.000001

     -- commit

     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from SC3I_weighting_working_table
     -- where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Midway (Iterations)', coalesce(@QA_catcher, -1)
     -- commit

     -- -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

     -- INSERT INTO SC3I_category_subtotals (scaling_date,sky_base_universe,profile,value,sky_base_accounts,vespa_panel,category_weight
                                              -- ,sum_of_weights, convergence)
     -- SELECT  @scaling_day
             -- ,sky_base_universe
             -- ,profile
             -- ,value
             -- ,sky_base_accounts
             -- ,vespa_panel
             -- ,category_weight
             -- ,sum_of_weights
             -- ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     -- FROM SC3I_category_working_table

     -- -- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
     -- -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     -- commit

     -- -- Apparently it should be reviewed each week, but what are we looking for?

     -- INSERT INTO SC3I_metrics (scaling_date, iterations, convergence, max_weight, av_weight,
                                  -- sum_of_weights, sky_base, vespa_panel, non_scalable_accounts)
     -- SELECT  @scaling_day
            -- ,@iteration
            -- ,@convergence
            -- ,MAX(segment_weight)
            -- ,sum(segment_weight * vespa_panel) / sum(vespa_panel)    -- gives the average weight by account (just uising AVG would give it average by segment id)
            -- ,SUM(segment_weight * vespa_panel)                       -- again need some math because this table has one record per segment id rather than being at acocunt level
            -- ,@sky_base
            -- ,sum(CASE WHEN segment_weight >= 0.001 THEN vespa_panel ELSE NULL END)
            -- ,sum(CASE WHEN segment_weight < 0.001  THEN vespa_panel ELSE NULL END)
     -- FROM SC3I_weighting_working_table

     -- update SC3I_metrics
        -- set sum_of_convergence = abs(sky_base - sum_of_weights)

     -- insert into SC3I_non_convergences(scaling_date,scaling_segment_id, difference)
     -- select @scaling_day
           -- ,scaling_segment_id
           -- ,abs(sum_of_weights - sky_base_accounts)
       -- from SC3I_weighting_working_table
      -- where abs((segment_weight * vespa_panel) - sky_base_accounts) > 3

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Complete (Calculate weights)', coalesce(@QA_catcher, -1)
     -- commit



     -- /**************** PART B03: PUBLISHING WEIGHTS INTO INTERFACE STRUCTURES ****************/

     -- -- Here is where that bit of interface code goes, including extending the intervals
     -- -- in the Scaling midway tables (which now happens one day ata time). Maybe this guy
     -- -- wants to go into a new and different stored procedure?

     -- -- Heh, this deletion process clears out everything *after* the scaling day, meaning we
     -- -- have to start from the beginning doing this processing... I guess we'll just manage
     -- -- the historical build like this. (This is because we'd otherwise have to manage adding
     -- -- additional records to the interval table when we re-run a day and break an interval
     -- -- that already exists, and that whole process would be annoying to manage.)

     -- -- Except we'll only nuke everything if we *rebuild* a day that's not already there.
     -- if (select count(1) from SC3I_Weightings where scaling_day = @scaling_day) > 0
     -- begin
         -- delete from SC3I_Weightings where scaling_day = @scaling_day

         -- delete from SC3I_Intervals where reporting_starts = @scaling_day

         -- update SC3I_Intervals set reporting_ends = dateadd(day, -1, @scaling_day) where reporting_ends >= @scaling_day
     -- end
     -- commit

     -- -- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
     -- -- also keep this in VIQ_prod too.
     -- insert into SC3I_Weightings
     -- select
         -- @scaling_day
         -- ,scaling_segment_id
         -- ,vespa_panel
         -- ,sky_base_accounts
         -- ,segment_weight
         -- ,sum_of_weights
         -- ,indices_actual
         -- ,indices_weighted
         -- ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
     -- from SC3I_weighting_working_table
     -- -- Might have to check that the filter on segment_weight doesn't leave any orphaned
     -- -- accounts about the place...

     -- commit

     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from SC3I_Weightings
     -- where scaling_day = @scaling_day

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 1/4 (Midway weights)', coalesce(@QA_catcher, -1)
     -- commit

     -- -- First off extend the intervals that are already in the table:

-- /*
     -- update SC3I_Intervals
     -- set reporting_ends = @scaling_day
     -- from SC3I_Intervals
     -- inner join SC3I_Todays_panel_members as tpm
     -- on SC3I_Intervals.account_number         = tpm.account_number
     -- and SC3I_Intervals.scaling_segment_ID    = tpm.scaling_segment_ID
     -- where reporting_ends = @scaling_day - 1

     -- -- Next step is adding in all the new intervals that don't appear
     -- -- as extensions on existing intervals. First off, isolate the
     -- -- intervals that got extended

     -- select account_number
     -- into #included_accounts
     -- from SC3I_Intervals
     -- where reporting_ends = @scaling_day

     -- commit
     -- create unique index fake_pk on #included_accounts (account_number)
     -- commit

     -- -- Now having figured out what already went in, we can throw in the rest:
     -- insert into SC3I_Intervals (
         -- account_number
         -- ,HH_person_number
         -- ,reporting_starts
         -- ,reporting_ends
         -- ,scaling_segment_ID
     -- )
     -- select
         -- tpm.account_number
         -- ,HH_person_number
         -- ,@scaling_day
         -- ,@scaling_day
         -- ,tpm.scaling_segment_ID
     -- from SC3I_Todays_panel_members as tpm
     -- left join #included_accounts as ia
     -- on tpm.account_number = ia.account_number
     -- where ia.account_number is null -- we don't want to add things already in the intervals table


     -- commit
     -- drop table #included_accounts
     -- commit
-- */
     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from SC3I_Intervals where reporting_ends = @scaling_day

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 2/4 (Midway intervals)', coalesce(@QA_catcher, -1)
     -- commit

     -- -- Part 2: Update the VIQ interface table (which needs the household key thing)
     -- if (select count(1) from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING where scaling_date = @scaling_day) > 0
     -- begin
         -- delete from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING where scaling_date = @scaling_day
     -- end
     -- commit

     -- insert into V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     -- select
         -- ws.account_number
         -- ,ws.HH_person_number
         -- ,@scaling_day
         -- ,wwt.segment_weight
         -- ,@batch_date
     -- from SC3I_weighting_working_table as wwt
     -- inner join SC3I_Sky_base_segment_snapshots as ws -- need this table to get the cb_key_household items
     -- on wwt.scaling_segment_id = ws.population_scaling_segment_id
     -- inner join SC3I_Todays_panel_members as tpm
     -- on ws.account_number = tpm.account_number       -- Filter for today's panel only
     -- and ws.profiling_date = @profiling_date

     -- commit

     -- set @QA_catcher = -1

     -- select @QA_catcher = count(1)
     -- from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     -- where scaling_date = @scaling_day

     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 3/4 (VIQ interface)', coalesce(@QA_catcher, -1)
     -- commit

     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Complete! (Publish weights)'
     -- commit
     -- -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Weights made for ' || dateformat(@scaling_day, 'yyyy-mm-dd')
     -- commit

-- end; -- of procedure "V289_M11_04_SC3I_v1_1__make_weights"
-- commit;


/*******************************************************************************************************/


create or replace procedure V289_M11_04_SC3I_v1_1__make_weights_BARB
    @profiling_date             date                -- Thursday profilr date
    ,@scaling_day                date                -- Day for which to do scaling; this argument is mandatory
    ,@batch_date                datetime = now()    -- Day on which build was kicked off
    ,@Scale_refresh_logging_ID  bigint = null       -- Might pass the log ID in as an argument if it's a big historical build, otherwise we'll make a new one
as
begin


        /*
        -- Only need these if we can't get to execute as a Proc
                declare @scaling_day  date
        declare @batch_date date
        declare @Scale_refresh_logging_ID bigint
        set @scaling_day = '2013-09-26'
        set @batch_date = '2014-07-10'
        set @Scale_refresh_logging_ID = 5
                COMMIT
        */
        -- CREATE OR REPLACE VARIABLE @profiling_date             date  =       '2015-02-05';
    -- CREATE OR REPLACE VARIABLE @scaling_day                date =    '2015-02-06';
    -- CREATE OR REPLACE VARIABLE @batch_date                datetime = now();
    -- CREATE OR REPLACE VARIABLE @Scale_refresh_logging_ID  bigint = 4131;

        -- SET @profiling_date  =       '2015-02-05'
    -- SET @scaling_day =       '2015-02-06'
    -- SET      @Scale_refresh_logging_ID       = 4131
        -- COMMIT



     -- So by this point we're assuming that the Sky base segmentation is done
     -- (for a suitably recent item) and also that today's panel members have
     -- been established, and we're just going to go calculate these weights.

     DECLARE @cntr           INT
     DECLARE @iteration      INT
     DECLARE @cntr_var       SMALLINT
     DECLARE @scaling_var    VARCHAR(30)
     DECLARE @scaling_count  SMALLINT
     DECLARE @convergence    TINYINT
     DECLARE @sky_base       DOUBLE
     DECLARE @vespa_panel    DOUBLE
     DECLARE @sum_of_weights DOUBLE
--     declare @profiling_date date
     declare @QA_catcher     bigint
     COMMIT -- (^_^)

     -- CREATE OR REPLACE VARIABLE @cntr           INT;
     -- CREATE OR REPLACE VARIABLE @iteration      INT;
     -- CREATE OR REPLACE VARIABLE @cntr_var       SMALLINT;
     -- CREATE OR REPLACE VARIABLE @scaling_var    VARCHAR(30);
     -- CREATE OR REPLACE VARIABLE @scaling_count  SMALLINT;
     -- CREATE OR REPLACE VARIABLE @convergence    TINYINT;
     -- CREATE OR REPLACE VARIABLE @sky_base       DOUBLE;
     -- CREATE OR REPLACE VARIABLE @vespa_panel    DOUBLE;
     -- CREATE OR REPLACE VARIABLE @sum_of_weights DOUBLE;
-- --     CREATE OR REPLACE VARIABLE @profiling_date date;
     -- CREATE OR REPLACE VARIABLE @QA_catcher     bigint;


     /**************** PART B01: GETTING TOTALS FOR EACH SEGMENT ****************/

     -- Figure out which profiling info we're using;
/*     select @profiling_date = max(profiling_date)
     from SC3I_Sky_base_segment_snapshots
     where profiling_date <= @scaling_day

     COMMIT -- (^_^)
*/
     -- Log the profiling date being used for the build
      -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Making weights for ' || dateformat(@scaling_day,'yyyy-mm-dd') || ' using profiling of ' || dateformat(@profiling_date,'yyyy-mm-dd') || '.'
      COMMIT -- (^_^)

        -- First adding in the Sky base numbers
        truncate table SC3I_weighting_working_table
        COMMIT -- (^_^)

        INSERT INTO     SC3I_weighting_working_table(
                        scaling_segment_id
                ,       sky_base_accounts
        )
        select
                        population_scaling_segment_id
                ,       count(1)
        from    SC3I_Sky_base_segment_snapshots
        where   profiling_date  =       @profiling_date
        group by        population_scaling_segment_id
        COMMIT -- (^_^)

                -- Rebase the Sky Base so that it matches EDM processes
        -- The easiest way to do this is to use the total wieghts derived in VIQ_VIEWING_DATA_SCALING

        -- Get Skybase size according to this process
        select  count(distinct account_number) as base_total
        into    #base
        from    SC3I_Sky_base_segment_snapshots
        where   profiling_date  =       @profiling_date

        -- Get Skybase size according to EDM
        select max(adjusted_event_start_date_vespa) as edm_latest_scaling_date
        into   #latest_date
        from   sk_prod.VIQ_VIEWING_DATA_SCALING
        COMMIT -- (^_^)

        select case when @scaling_day <= edm_latest_scaling_date then @scaling_day else edm_latest_scaling_date end as sky_base_universe_date
        into #use_date
        from #latest_date
        COMMIT -- (^_^)

        select  sum(calculated_scaling_weight) as edm_total
        into    #viq_scaling
        from    sk_prod.VIQ_VIEWING_DATA_SCALING
        cross join #use_date
        where   adjusted_event_start_date_vespa = sky_base_universe_date
        COMMIT -- (^_^)


        update SC3I_weighting_working_table    as      w
        set             w.sky_base_accounts     = w.sky_base_accounts * v.edm_total / b.base_total
        from            #base b, #viq_scaling v
        COMMIT -- (^_^)

        /**************** update SC3I_weighting_working_table
        -- Re-scale Sky base to Barb age/gender totals
        -- Will only rescale to barb households that have any viewing data for the day being scaled
        -- and NOT the barb base
        */

        -- Get individuals from Barb who have viewed tv for the processing day
        select
                        household_number
                ,       person_number
        into    #barb_viewers
        from    skybarb_fullview
        where   date(start_time_of_session)     =       @scaling_day
        group by
                        household_number
                ,       person_number
        COMMIT -- (^_^)

        create hg index ind1 on #barb_viewers(household_number) COMMIT -- (^_^)
        create lf index ind2 on #barb_viewers(person_number) COMMIT -- (^_^)


        
        -- Now reduce that to the corresponding households that've registered some viewing
        select  household_number
        into    #barb_hhd_viewers
        from    #barb_viewers
        group by        household_number
        COMMIT -- (^_^)

        create hg index ind1 on #barb_hhd_viewers(household_number)
        COMMIT -- (^_^)



        -- Get details on individuals from Sky households in BARB (no date-dependency here)
        select
                        h.house_id                              as      household_number
                ,       h.person                                as      person_number
                ,       h.age
                ,       case
                                when age <= 19                  then 'U'
                                when h.sex = 'Male'     then 'M'
                                when h.sex = 'Female'   then 'F'
                        end                                             as      gender
                ,       case
                                when age <= 11                          then '0-11'
                                WHEN age BETWEEN 12 AND 19      then '12-19'
                                WHEN age BETWEEN 20 AND 24      then '20-24'
                                WHEN age BETWEEN 25 AND 34      then '25-34'
                                WHEN age BETWEEN 35 AND 44      then '35-44'
                                WHEN age BETWEEN 45 AND 64      then '45-64'
                                WHEN age >= 65                          then '65+'
                        end                                             as      ageband
                ,       cast(h.head as char(1)) as      head_of_hhd
                ,       w.processing_weight             as      processing_weight
        into    #barb_inds_with_sky
        from
                                        skybarb                 as      h
                inner join      barb_weights    as      w       on      h.house_id      =       w.household_number
                                                                                        and     h.person        =       w.person_number
        COMMIT -- (^_^)





        -------------- Summarise Barb Data

        -- Define the default matrix of ageband, hhsize (and now viewed_tv) to deal with empty Barb segments
        select
                        hh_size
                ,       age_band
                ,       viewed_tv
                ,       head_of_hhd
                ,       1.0                     as      default_weight
        into    #default_m
        from
                                        (
                                                select  distinct hh_size
                                                from    vespa_analysts.SC3I_Segments_lookup_v1_1
                                        )       as      a
                cross join      (
                                                select  distinct age_band
                                                from    vespa_analysts.SC3I_Segments_lookup_v1_1
                                        )       as      b
                cross join      (
                                                select  distinct viewed_tv
                                                from    vespa_analysts.SC3I_Segments_lookup_v1_1
                                        )       as      c
                cross join      (
                                                select  distinct head_of_hhd
                                                from    vespa_analysts.SC3I_Segments_lookup_v1_1
                                        )       as      d
        COMMIT -- (^_^)



        -- Add BARB individual weights from the available segments, aggregated by gender-age/viewed-tv/hhsize attributes
        truncate table V289_M11_04_Barb_weighted_population
        COMMIT -- (^_^)

        insert into V289_M11_04_Barb_weighted_population(
                        ageband
                -- ,    gender
                ,       viewed_tv
                ,       head_of_hhd
                ,       hh_size
                ,       barb_weight
                )
        select
                        (
                                case
                                        when    ageband = '0-19'        then    'U'
                                        else                                                            gender
                                end
                        )       || ' ' || ageband                                                               as      gender_ageband -- now adapted to be a gender-age attribute
                -- ,    'A'                                                                                             as      gender  -- simply a dummy variable now
                -- ,    'Y' as viewed_tv
                ,       case
                                when v.household_number is not null         then    'Yes'
                                when v_hhd.household_number is not null     then    'NV - Viewing HHD'
                                else                                                'NV - NonViewing HHD'
                        end   as      viewed_tv       -- Derive viewer/non-viewer flag
                ,       i.head_of_hhd -- this has also been fixed                       as      head_of_hhd     -- '9'
                ,       z.hh_gr                                                                                         as      hh_size
                ,       sum(processing_weight)                                                          as      barb_weight
        from
                                        #barb_inds_with_sky     as      i       -- Sky individuals in BARB
                /*
                inner join      #barb_hhd_viewers       as      hv      -- Filter for households that have reported viewing     -- HYT don't want this inner join as we want non-viewers as well
                                                                                                on      i.household_number      =       hv.household_number
                */
                inner join      (       -- Cap household sizes
                                                select
                                                                household_number
                                                        ,       case
                                                                        when hhsize < 8         then    cast(hhsize as varchar(2)) 
                                                                        else                                            '8+'
                                                                end             as hh_gr 
                                                from    (
                                                                        select  -- First calculate the household sizes
                                                                                        household_number
                                                                                ,       count(1) as hhsize
                                                                        from    barb_weights 
                                                                        group by        household_number
                                                                )       as      w 
                                        )                                       as      z       on      i.household_number      =       z.household_number
                left join       #barb_viewers           as      v       on      i.household_number      =       v.household_number
                                                                                                and     i.person_number         =       v.person_number
                left join       #barb_hhd_viewers v_hhd                 on      i.household_number      =       v_hhd.household_number
        group by
                        gender_ageband
                -- ,    gender
                ,       viewed_tv
                ,       head_of_hhd
                ,       hh_size
        COMMIT -- (^_^)
        
        -- Clean up
        drop table #barb_inds_with_sky
        COMMIT -- (^_^)

        -- Set the dummy variable here
        update V289_M11_04_Barb_weighted_population
        set gender = 'A'
        COMMIT -- (^_^)


        -- Now add default weights for any segments that were missing in the above
        insert into     V289_M11_04_Barb_weighted_population(
                        ageband
                ,       gender
                ,       viewed_tv
                ,       head_of_hhd
                ,       hh_size
                ,       barb_weight
                )
        select
                        m.age_band
                ,       'A'
                -- ,    'Y'
                ,       m.viewed_tv
                ,       m.head_of_hhd   --      '9'
                ,       m.hh_size
                ,       default_weight
        from
                                        #default_m                                                              as      m
                left join       V289_M11_04_Barb_weighted_population    as      b       on      m.hh_size               =       b.hh_size
                                                                                                                                        and     m.age_band              =       b.ageband
                                                                                                                                        and     m.viewed_tv             =       b.viewed_tv
                                                                                                                                        and     m.head_of_hhd   =       b.head_of_hhd
        where   b.hh_size is null
        COMMIT -- (^_^)


        /* -- Some checks...
        select top 20 * from V289_M11_04_Barb_weighted_population;
        ageband gender  viewed_tv       head_of_hhd     hh_size barb_weight
        A 25-34 A       Y       9       4       486293.5
        A 45-64 A       Y       9       4       1161471.4
        U 0-19  A       Y       9       4       1440588.4
        A 45-64 A       Y       9       2       1843330.6
        A 45-64 A       Y       9       1       633042.6

        select sum(barb_weight) from V289_M11_04_Barb_weighted_population; -- 23,356,984

        select 
                        viewed_tv
                ,   sum(barb_weight)    as  SOW
        from V289_M11_04_Barb_weighted_population
        group by viewed_tv
        ;
        viewed_tv       SOW
        N       4,538,490.3
        Y       18,818,493.7

        */



        ----

/**************** NOT NEEDED IF WE ARE SCALING TO BARB NUMBERS ******************

        --- Calculate the percentage of viewers and non-viewers by Barb segments
        select           a.ageband
                        ,a.head_of_hhd
                        ,a.hh_size
                        ,a.viewed_tv
                        ,a.v_weight / b.tot_barb as viewed_pc
        into            #viewed_pc
        from
                        (select ageband, head_of_hhd, hh_size, viewed_tv, sum(barb_weight) as v_weight
                         from V289_M11_04_Barb_weighted_population
                         group by ageband, head_of_hhd, hh_size, viewed_tv) a
        inner join
                        (select ageband, head_of_hhd, hh_size, sum(barb_weight) as tot_barb
                         from V289_M11_04_Barb_weighted_population
                         group by ageband, head_of_hhd, hh_size) b on a.ageband = b.ageband and a.head_of_hhd = b.head_of_hhd and a.hh_size = b.hh_size



        

        -- All Skybase has been set to tv viewers
        -- This will rescale them to Barb viewers by age gender group
        
        update  SC3I_weighting_working_table    as      w
        set             w.sky_base_accounts     =       w.sky_base_accounts * p.viewed_pc
        from            vespa_analysts.SC3I_Segments_lookup_v1_1        as      l
        cross join      #viewed_pc p
        where           w.scaling_segment_id    =       l.scaling_segment_id
        and             l.age_band              =       p.ageband
        and             l.head_of_hhd           =       p.head_of_hhd
        and             l.hh_size               =       p.hh_size
        and             l.viewed_tv             =       p.viewed_tv


        COMMIT -- (^_^)

*/


        -- Calculate SKy Base Total by Barb Segment
        select          l.age_band
                        ,l.head_of_hhd
                        ,l.hh_size
                        ,l.viewed_tv
                        ,sum(w.sky_base_accounts) as tot_base_accounts
        into            #base_totals
        from            SC3I_weighting_working_table w
        inner join      vespa_analysts.SC3i_Segments_lookup_v1_1 l
                                on w.scaling_segment_id    =       l.scaling_segment_id
        group by        l.age_band
                        ,l.head_of_hhd
                        ,l.hh_size
                        ,l.viewed_tv
        commit -- (^_^)


        update  SC3I_weighting_working_table    as      w
        set             w.sky_base_accounts     =       w.sky_base_accounts * barb_weight / cast(b.tot_base_accounts as float)
        from            vespa_analysts.SC3i_Segments_lookup_v1_1        as      l
        cross join      #base_totals b
        cross join      V289_M11_04_Barb_weighted_population p
        where           w.scaling_segment_id    =       l.scaling_segment_id
        and             l.age_band              =       b.age_band
        and             l.head_of_hhd           =       b.head_of_hhd
        and             l.hh_size               =       b.hh_size
        and             l.viewed_tv             =       b.viewed_tv
        and             l.age_band              =       p.ageband
        and             l.head_of_hhd           =       p.head_of_hhd
        and             l.hh_size               =       p.hh_size
        and             l.viewed_tv             =       p.viewed_tv

        commit -- (^_^)




        /***********************************************/


     -- Now tack on the universe flags; a special case of things coming out of the lookup

     update     SC3I_weighting_working_table
     set        sky_base_universe = sl.sky_base_universe
     from
                                        SC3I_weighting_working_table
                 -- inner join vespa_analysts.SC2_Segments_lookup_v1_1 as sl
                 inner join vespa_analysts.SC3I_Segments_lookup_v1_1    as      sl              on      SC3I_weighting_working_table.scaling_segment_id =       sl.scaling_segment_id
     COMMIT -- (^_^)

     -- Mix in the Vespa panel counts as determined earlier
     select
                        scaling_segment_id
                ,       count(1) as panel_members
     into       #segment_distribs
     from       SC3I_Todays_panel_members
     where      scaling_segment_id is not null
     group by   scaling_segment_id

     COMMIT -- (^_^)
     create unique index fake_pk on #segment_distribs (scaling_segment_id)
     COMMIT -- (^_^)

     -- It defaults to 0, so we can just poke values in
     update     SC3I_weighting_working_table
     set        vespa_panel = sd.panel_members
     from
                                        SC3I_weighting_working_table
                inner join      #segment_distribs       as      sd              on      SC3I_weighting_working_table.scaling_segment_id =       sd.scaling_segment_id

     -- And we're done! log the progress.
     COMMIT -- (^_^)
     drop table #segment_distribs
     COMMIT -- (^_^)
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3I_weighting_working_table
     COMMIT -- (^_^)

     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B01: Complete! (Segmentation totals)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)






     /**************** PART B02: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/

     delete from SC3I_category_subtotals 
         where scaling_date = @scaling_day
         COMMIT -- (^_^)
         
     delete from SC3I_metrics
         where scaling_date = @scaling_day
     COMMIT -- (^_^)

     -- Rim-weighting is an iterative process that iterates through each of the scaling variables
     -- individually until the category sum of weights converge to the population category subtotals

     SET @cntr                  = 1     COMMIT -- (^_^)
     SET @iteration             = 0     COMMIT -- (^_^)
     SET @cntr_var              = 1     COMMIT -- (^_^)
--      SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC2_Variables_lookup_v2_1 WHERE id = @cntr)  COMMIT -- (^_^)

        SET @scaling_var        =       (
                                                                SELECT  scaling_variable
                                                                FROM    vespa_analysts.SC3I_Variables_lookup_v1_1
                                                                WHERE   id = @cntr
                                                        )
        COMMIT -- (^_^)
        
        SET @scaling_count      =       (
                                                                SELECT  COUNT(scaling_variable)
                                                                FROM    vespa_analysts.SC3I_Variables_lookup_v1_1
                                                        )
        COMMIT -- (^_^)

        
        
     -- The SC3I_weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
     -- the sky base.
     -- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
     -- to ensure convergence.

     -- arbitrary value to ensure convergence
     update     SC3I_weighting_working_table
     set        vespa_panel = 0.000001
     where      vespa_panel = 0
     COMMIT -- (^_^)

     -- Initialise working columns
     update     SC3I_weighting_working_table
     set        sum_of_weights = vespa_panel
     COMMIT -- (^_^)

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
     -- In this scenario, the person running the code should send the results of the SC3I_metrics for that
     -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

        WHILE @cntr <= @scaling_count
        BEGIN
        
                TRUNCATE TABLE SC3I_category_working_table
                COMMIT

                SET @cntr_var = 1
                COMMIT
                
                WHILE @cntr_var <= @scaling_count
                        BEGIN
                        
                                SELECT  @scaling_var = scaling_variable
                                FROM    vespa_analysts.SC3I_Variables_lookup_v1_1
                                WHERE   id = @cntr_var
                                COMMIT

                                EXECUTE('
                                        INSERT INTO SC3I_category_working_table(
                                                        sky_base_universe
                                                ,       profile
                                                ,       value
                                                ,       sky_base_accounts
                                                ,       vespa_panel
                                                ,       sum_of_weights
                                                )
                                        SELECT 
                                                        srs.sky_base_universe
                                                ,       @scaling_var
                                                ,       ssl.'||@scaling_var||'
                                                ,       SUM(srs.sky_base_accounts)
                                                ,       SUM(srs.vespa_panel)
                                                ,       SUM(srs.sum_of_weights)
                                        FROM
                                                                        SC3I_weighting_working_table AS srs
                                                inner join      vespa_analysts.SC3I_Segments_lookup_v1_1        AS      ssl             ON      srs.scaling_segment_id  =       ssl.scaling_segment_id
                                        GROUP BY
                                                        srs.sky_base_universe
                                                ,       @scaling_var
                                                ,       ssl.'||@scaling_var||'
                                        ORDER BY
                                                        srs.sky_base_universe
                                        ')
                                COMMIT
                                
                                SET     @cntr_var       =       @cntr_var + 1
                                COMMIT
                        END -- while @cntr_var <= @scaling_count
                COMMIT

                UPDATE  SC3I_category_working_table
                SET
                                category_weight         =       sky_base_accounts / sum_of_weights
                        ,       convergence_flag        =       CASE
                                                                                        WHEN abs(sky_base_accounts - sum_of_weights) < 3        THEN    0
                                                                                        ELSE                                                                                                            1
                                                                                END
                COMMIT

                SELECT  @convergence = SUM(convergence_flag)
                FROM    SC3I_category_working_table
                COMMIT
                
                SET @iteration = @iteration + 1
                COMMIT
                
                SELECT  @scaling_var = scaling_variable
                FROM    vespa_analysts.SC3I_Variables_lookup_v1_1
                WHERE   id = @cntr
                COMMIT

                EXECUTE('
                        UPDATE  SC3I_weighting_working_table
                        SET
                                        SC3I_weighting_working_table.category_weight    =       sc.category_weight
                                ,       SC3I_weighting_working_table.sum_of_weights             =       SC3I_weighting_working_table.sum_of_weights * sc.category_weight
                        FROM
                                                        SC3I_weighting_working_table
                                inner join      vespa_analysts.SC3I_Segments_lookup_v1_1        AS      ssl             ON      SC3I_weighting_working_table.scaling_segment_id =       ssl.scaling_segment_id
                                inner join      SC3I_category_working_table                                     AS      sc              ON      sc.value                                =       ssl.'||@scaling_var||'
                                                                                                                                                                        AND     sc.sky_base_universe    =       ssl.sky_base_universe
                                                                                                                                                                        AND     sc.profile                              =       @scaling_var
                        ')

                COMMIT

                IF      (@iteration = 100 OR @convergence = 0)
                        SET @cntr = @scaling_count + 1
                ELSE
                        IF      @cntr   =       @scaling_count
                                SET     @cntr   =       1
                        ELSE
                                SET     @cntr   =       @cntr + 1
                COMMIT

        END
        COMMIT -- (^_^)

        -- This loop build took about 4 minutes. That's fine. 
        -- HYT 2.5 mins

     -- Calculate segment weight and corresponding indices

     -- This section calculates the segment weight which is the weight that should be applied to viewing data
     -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


        SELECT
                        @sky_base = SUM(sky_base_accounts)
                ,       @vespa_panel = SUM(vespa_panel)
                ,       @sum_of_weights = SUM(sum_of_weights)
        FROM SC3I_weighting_working_table
        COMMIT -- (^_^)

        UPDATE  SC3I_weighting_working_table
        SET
                        segment_weight          =       sum_of_weights / vespa_panel
                ,       indices_actual          =       100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
                ,       indices_weighted        =       100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)
        COMMIT -- (^_^)

     -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
     update SC3I_weighting_working_table
     set segment_weight  = 0.000001
     where vespa_panel   = 0.000001
     COMMIT -- (^_^)

     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3I_weighting_working_table
     where segment_weight >= 0.001           -- Ignore the placeholders here to guarantee convergence
     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Midway (Iterations)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Now push convergence details out to the tracking tables: the first one provides a convergence summary at a category level

        INSERT INTO     SC3I_category_subtotals(
                        scaling_date
                ,       sky_base_universe
                ,       profile
                ,       value
                ,       sky_base_accounts
                ,       vespa_panel
                ,       category_weight
                ,       sum_of_weights
                ,       convergence
                )
        SELECT
                        @scaling_day
                ,       sky_base_universe
                ,       profile
                ,       value
                ,       sky_base_accounts
                ,       vespa_panel
                ,       category_weight
                ,       sum_of_weights
                ,       case
                                when abs(sky_base_accounts - sum_of_weights) > 3        then    1
                                else                                                                                                            0
                        end
        FROM    SC3I_category_working_table
        COMMIT -- (^_^)
         
     -- The SC3I_metrics table contains metrics for a particular scaling date. It shows whether the
     -- Rim-weighting process converged for that day and the number of iterations. It also shows the
     -- maximum and average weight for that day and counts for the sky base and the vespa panel.

     COMMIT -- (^_^)

     -- Apparently it should be reviewed each week, but what are we looking for?

     INSERT INTO        SC3I_metrics(
                        scaling_date
                ,       iterations
                ,       convergence
                ,       max_weight
                ,       av_weight
                ,       sum_of_weights
                ,       sky_base
                ,       vespa_panel
                ,       non_scalable_accounts
                )
     SELECT
                        @scaling_day
                ,       @iteration
                ,       @convergence
                ,       MAX(segment_weight)
                ,       sum(segment_weight * vespa_panel) / sum(vespa_panel)    -- gives the average weight by account (just uising AVG would give it average by segment id)
                ,       SUM(segment_weight * vespa_panel)                       -- again need some math because this table has one record per segment id rather than being at acocunt level
                ,       @sky_base
                ,       sum     (
                                        CASE
                                                WHEN segment_weight >= 0.001    THEN    vespa_panel
                                                ELSE                                                                    NULL
                                        END
                                )
                ,       sum     (
                                        CASE
                                                WHEN segment_weight < 0.001             THEN    vespa_panel
                                                ELSE                                                                    NULL
                                        END
                                )
     FROM       SC3I_weighting_working_table
         COMMIT -- (^_^)

        update  SC3I_metrics
        set     sum_of_convergence = abs(sky_base - sum_of_weights)
        COMMIT -- (^_^)

        insert into     SC3I_non_convergences(
                        scaling_date
                ,       scaling_segment_id
                ,       difference
                )
        select
                        @scaling_day
                ,       scaling_segment_id
                ,       abs(sum_of_weights - sky_base_accounts)
        from    SC3I_weighting_working_table
        where abs((segment_weight * vespa_panel) - sky_base_accounts) > 3
        COMMIT -- (^_^)
        
        -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B02: Complete (Calculate weights)', coalesce(@QA_catcher, -1)
        COMMIT -- (^_^)



     /**************** PART B03: PUBLISHING WEIGHTS INTO INTERFACE STRUCTURES ****************/

     -- Here is where that bit of interface code goes, including extending the intervals
     -- in the Scaling midway tables (which now happens one day ata time). Maybe this guy
     -- wants to go into a new and different stored procedure?

     -- Heh, this deletion process clears out everything *after* the scaling day, meaning we
     -- have to start from the beginning doing this processing... I guess we'll just manage
     -- the historical build like this. (This is because we'd otherwise have to manage adding
     -- additional records to the interval table when we re-run a day and break an interval
     -- that already exists, and that whole process would be annoying to manage.)

     -- Except we'll only nuke everything if we *rebuild* a day that's not already there.
     if (select count(1) from SC3I_Weightings where scaling_day = @scaling_day) > 0
     begin
         delete from SC3I_Weightings where scaling_day = @scaling_day

         delete from SC3I_Intervals where reporting_starts = @scaling_day

         update SC3I_Intervals set reporting_ends = dateadd(day, -1, @scaling_day) where reporting_ends >= @scaling_day
     end
     COMMIT -- (^_^)

        -- Part 1: Update the Vespa midway scaling tables. In Vespa Analysts? May as well
        -- also keep this in VIQ_prod too.
        insert into SC3I_Weightings
        select
                        @scaling_day
                ,       scaling_segment_id
                ,       vespa_panel
                ,       sky_base_accounts
                ,       segment_weight
                ,       sum_of_weights
                ,       indices_actual
                ,       indices_weighted
                ,       case
                                when abs(sky_base_accounts - sum_of_weights) > 3        then    1
                                else                                                                                                            0
                        end
        from    SC3I_weighting_working_table
        COMMIT -- (^_^)
     -- Might have to check that the filter on segment_weight doesn't leave any orphaned
     -- accounts about the place...


     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3I_Weightings
     where scaling_day = @scaling_day
     COMMIT -- (^_^)

     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 1/4 (Midway weights)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- First off extend the intervals that are already in the table:
/*
     update SC3I_Intervals
     set reporting_ends = @scaling_day
     from SC3I_Intervals
     inner join SC3I_Todays_panel_members as tpm
     on SC3I_Intervals.account_number         = tpm.account_number
     and SC3I_Intervals.scaling_segment_ID    = tpm.scaling_segment_ID
     where reporting_ends = @scaling_day - 1

     -- Next step is adding in all the new intervals that don't appear
     -- as extensions on existing intervals. First off, isolate the
     -- intervals that got extended

     select account_number
     into #included_accounts
     from SC3I_Intervals
     where reporting_ends = @scaling_day

     COMMIT -- (^_^)
     create unique index fake_pk on #included_accounts (account_number)
     COMMIT -- (^_^)

     -- Now having figured out what already went in, we can throw in the rest:
     insert into SC3I_Intervals (
         account_number
         ,HH_person_number
         ,reporting_starts
         ,reporting_ends
         ,scaling_segment_ID
     )
     select
         tpm.account_number
         ,HH_person_number
         ,@scaling_day
         ,@scaling_day
         ,tpm.scaling_segment_ID
     from SC3I_Todays_panel_members as tpm
     left join #included_accounts as ia
     on tpm.account_number = ia.account_number
     where ia.account_number is null -- we don't want to add things already in the intervals table


     COMMIT -- (^_^)
     drop table #included_accounts
     COMMIT -- (^_^)
*/
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from SC3I_Intervals where reporting_ends = @scaling_day

     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 2/4 (Midway intervals)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- Part 2: Update the VIQ interface table (which needs the household key thing)
        truncate table V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
        COMMIT -- (^_^)


/*
    insert into V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
        select
                        ws.account_number
                ,       ws.HH_person_number
                ,       @scaling_day
                ,       wwt.segment_weight
                ,       @batch_date
        from
                                SC3I_weighting_working_table    as      wwt
        inner join      SC3I_Sky_base_segment_snapshots as      ws -- need this table to get the cb_key_household items -- this currently introduces duplicates
                                                                                                                        on      wwt.scaling_segment_id  =       ws.population_scaling_segment_id
        inner join      SC3I_Todays_panel_members               as      tpm             on      ws.account_number               =       tpm.account_number       -- Filter for today's panel only
                                                                                                                        and     ws.hh_person_number             =       tpm.hh_person_number
                                                                                                                        and     ws.profiling_date               =       @profiling_date
        COMMIT -- (^_^)
*/
    insert into V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING(
                        account_number
                ,       HH_person_number
                ,       scaling_date
                ,       scaling_weighting
                ,       build_date
                )
        select
                        tpm.account_number
                ,       tpm.HH_person_number
                ,       @scaling_day
                ,       wwt.segment_weight
                ,       @batch_date
        from
                                        SC3I_Todays_panel_members       as  tpm
                inner join      SC3I_weighting_working_table    as      wwt             on      tpm.scaling_segment_id  =       wwt.scaling_segment_id
        COMMIT -- (^_^)

        
     set @QA_catcher = -1
         COMMIT -- (^_^)

     select @QA_catcher = count(1)
     from V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING
     where scaling_date = @scaling_day

     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Midway 3/4 (VIQ interface)', coalesce(@QA_catcher, -1)
     COMMIT -- (^_^)

     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'B03: Complete! (Publish weights)'
     COMMIT -- (^_^)
     -- execute logger_add_event @Scale_refresh_logging_ID, 3, 'SC3: Weights made for ' || dateformat(@scaling_day, 'yyyy-mm-dd')
     COMMIT -- (^_^)

end; -- of procedure "V289_M11_04_SC3I_v1_1__make_weights_BARB"
commit;



