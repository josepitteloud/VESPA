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
             -- inner JOIN
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
             -- inner JOIN
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
             -- inner JOIN
                -- #barb_age_gender_weighted_population bb
             -- on sb.profile = bb.profile and sb.value = bb.value






        -- update SC3I_weighting_working_table w
        -- set sky_base_accounts =
        -- from

        -- select scaling_segment_id,
        -- from
                -- #skybase_age_gender_totals sb
             -- inner JOIN
                -- #barb_age_gender_weighted_population bb





-- */





     -- -- Now tack on the universe flags; a special case of things coming out of the lookup

     -- update SC3I_weighting_working_table
     -- set sky_base_universe = sl.sky_base_universe
     -- from SC3I_weighting_working_table
-- --      inner JOIN vespa_analysts.SC2_Segments_lookup_v1_1 as sl
     -- inner JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 as sl
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
     -- inner JOIN #segment_distribs as sd
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
                                     -- inner JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
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
                     -- inner JOIN vespa_analysts.SC3I_Segments_lookup_v1_1 AS ssl ON SC3I_weighting_working_table.scaling_segment_id = ssl.scaling_segment_id
                     -- inner JOIN SC3I_category_working_table AS sc ON sc.value = ssl.'||@scaling_var||'
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
     -- inner JOIN SC3I_Todays_panel_members as tpm
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
     -- left JOIN #included_accounts as ia
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
     -- inner JOIN SC3I_Sky_base_segment_snapshots as ws -- need this table to get the cb_key_household items
     -- on wwt.scaling_segment_id = ws.population_scaling_segment_id
     -- inner JOIN SC3I_Todays_panel_members as tpm
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

