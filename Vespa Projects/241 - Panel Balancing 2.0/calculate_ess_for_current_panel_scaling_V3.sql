/* Prerequisite
---------------
Scaling 3.
This code currently uses results from Jason's schema.

*/

         -------------------------------
         --Part 1 - First fill the table
         -------------------------------

 -- Initialise scaling input table
    drop table Scaling2_00_Input;
  create table Scaling2_00_Input(account_number varchar(30)
                              ,service_instance_id varchar(50)
                              ,subscriber_id int
);
  commit;
  create unique hg index uhsub on Scaling2_00_Input(subscriber_id);
  create        hg index hgacc on Scaling2_00_Input(account_number);



-----------------------------------------------------------------------------------------------------------------
-- OPTION A: Proceed using the CURRENT PANEL (as far as panel balancing is concerned - vespa_analysts.panbal_SAV)
  insert into Scaling2_00_Input(account_number
                               ,subscriber_id
                               )
  select ccs.account_number
        ,ccs.card_subscriber_id
    from sk_prod.CUST_CARD_SUBSCRIBER_LINK as ccs
         inner join vespa_analysts.panbal_sav as sav on ccs.account_number = sav.account_number
   where effective_to_dt = '9999-09-09'
     and panel in (11,12)
group by ccs.account_number
        ,ccs.card_subscriber_id
;
-----------------------------------------------------------------------------------------------------------------
-- OPTION B: Proceed using the post-balancing PROPOSED PANEL

      -- Form the proposed panel in a temporary table first
  create table #accounts(account_number varchar(30));

  insert into #accounts
  select sav.account_number
    from vespa_analysts.panbal_sav as sav
         left join panbal_amends   as ame on sav.account_number = ame.account_number -- update reference to panbal_amends
         left join vespa_analysts.panel_movements_log as log on sav.account_number = log.account_number
   where panel in (11, 12)
      or  requested_movement_type = 'Campaign Test'
;
      -- Now insert proposed panel into the scaling input table
  insert into Scaling2_00_Input(account_number
                               ,subscriber_id
                               )
  select ccs.account_number
        ,card_subscriber_id
    from sk_prod.CUST_CARD_SUBSCRIBER_LINK as ccs
         inner join #accounts as bas on ccs.account_number = bas.account_number
   where effective_to_dt = '9999-09-09'
group by ccs.account_number
        ,card_subscriber_id
;
-----------------------------------------------------------------------------------------------------------------




      -- Add service_instance_id to each subscriber_id (this bit is common to both the current and proposed panel)
  update Scaling2_00_Input as bas
     set bas.service_instance_id = src_system_id
    from sk_prod.cust_service_instance as csi
   where bas.subscriber_id = cast(csi.si_external_identifier as int)
;



         -------------------------------------------------------------
         --Part 2 run code to create the procedures
         -------------------------------------------------------------

      -- Clean up and initialise some tables
    drop table todays_panel_members;
    drop table weighting_working_table;
    drop table category_working_table;
    drop table weightings;
  create table todays_panel_members (
         account_number     varchar(30)
        ,scaling_segment_id int
        ,rq double
);

  create table weighting_working_table(
         scaling_segment_id int
        ,sky_base_accounts  int
        ,panel_accounts     real -- int -- BUG?? there's an adjustment that sets 0 -> 0.000001 to aid convergence, but that would be meaningless in an int field
        ,universe           varchar(30)
        ,category_weight real
        ,sum_of_weights     real
        ,segment_weight real
        ,indices_actual real
        ,indices_weighted real
);

  create table category_working_table(universe varchar(30)
                                                                 ,profile varchar(30)
                                                                 ,value   varchar(30)
                                                                 ,sky_base_accounts int
                                                                 ,panel_accounts int
                                                                 ,sum_of_weights real
                                                                 ,category_weight real
                                                                 ,convergence_flag bit default 0
);

      IF object_id('prepare_panel_members') IS NOT NULL THEN DROP PROCEDURE prepare_panel_members END IF;

  create procedure prepare_panel_members
      as begin

            /**************** PART 2A: CLEANING OUT ALL THE OLD STUFF ****************/

            delete from todays_panel_members
            commit

            -- Prepare to catch the week's worth of logs:
            create table #raw_logs_dump (
                   account_number         varchar(20)         not null
                  ,service_instance_id    varchar(30)         not null
            )
            commit

            insert into #raw_logs_dump
            select distinct account_number, service_instance_id
              from Scaling2_00_Input
             where account_number is not null
               and service_instance_id is not null

            commit
            create index some_key on #raw_logs_dump (account_number)

            select account_number
                  ,count(distinct service_instance_id) as box_count
                  ,convert(tinyint, null) as expected_boxes
                  ,convert(int, null) as scaling_segment_id
              into #panel_options
              from #raw_logs_dump
          group by account_number

            commit
            create unique index fake_pk on #panel_options (account_number)
              drop table #raw_logs_dump

                -- Getting this list of accounts isn't enough, we also want to know if all the boxes
                -- of the household have returned data.

            update #panel_options as bas
               set expected_boxes     = sbs.expected_boxes
                  ,scaling_segment_id = sbs.population_scaling_segment_id
              from thompsonja.SC3_Sky_base_segment_snapshots as sbs
             where bas.account_number = sbs.account_number

            -- First moving the unique account numbers in...
            insert into todays_panel_members (account_number
                  ,scaling_segment_id)
            SELECT account_number
                  ,scaling_segment_id
              FROM #panel_options
             where expected_boxes >= box_count
               and scaling_segment_id is not null

              drop table #panel_options

            update todays_panel_members as bas
--               set rq = viq_rq
               set rq = sav.rq
              from vespa_analysts.panbal_sav as sav
             where bas.account_number = sav.account_number

            commit

     end; -- procedure prepare_panel_members
  commit;


      IF object_id('make_weights') IS NOT NULL THEN DROP PROCEDURE make_weights END IF;

  create procedure make_weights
      as begin

                -- So by this point we're assuming that the Sky base segmentation is done
                -- (for a suitably recent item) and also that today's panel members have
                -- been established, and we're just going to go calculate these weights.

           DECLARE @cntr           INT
           DECLARE @iteration      INT
           DECLARE @cntr_var       SMALLINT
           DECLARE @scaling_var    VARCHAR(30)
           DECLARE @convergence    TINYINT
           DECLARE @sky_base       DOUBLE
           DECLARE @vespa_panel    DOUBLE
           DECLARE @sum_of_weights DOUBLE
 /*
            create variable @cntr           INT;
            create variable @iteration      INT;
            create variable @cntr_var       SMALLINT;
            create variable @scaling_var    VARCHAR(30);
            create variable @convergence    TINYINT;
            create variable @sky_base       DOUBLE;
            create variable @vespa_panel    DOUBLE;
            create variable @sum_of_weights DOUBLE;
 */
            commit

            /**************** PART 2Bi: GETTING TOTALS FOR EACH SEGMENT ****************/

            -- First adding in the Sky base numbers
            delete from weighting_working_table
            commit

            INSERT INTO weighting_working_table(
                   scaling_segment_id
                  ,sky_base_accounts
                   )
            select population_scaling_segment_id
                  ,count(1)
              from thompsonja.SC3_Sky_base_segment_snapshots -- rerun to update
          group by population_scaling_segment_id

            commit

                -- Now tack on the universe flags; a special case of things coming out of the lookup
            update weighting_working_table as bas
               set universe = lkp.sky_base_universe
              from vespa_analysts.SC3_Segments_lookup_v1_1 as lkp
             where bas.scaling_segment_id = lkp.scaling_segment_id

            select PAV.account_number
--                  ,min(coalesce(PAV.viq_rq, 0)) as rq
                  ,min(coalesce(PAV.rq, 0)) as rq
              into #acc_rq
              from vespa_analysts.panbal_SAV    as PAV
                   inner join Scaling2_00_Input as bas on PAV.account_number = bas.account_number
          group by PAV.account_number

           select population_scaling_segment_id
                  ,round(sum(rq),0)  as accounts
             into #panel_accs
             from thompsonja.SC3_Sky_base_segment_snapshots  as  sc3
                  inner join #acc_rq as sub on sc3.account_number = sub.account_number
          group by population_scaling_segment_id

             update weighting_working_table as bas
                set bas.panel_accounts = pan.accounts
               from #panel_accs as pan
              where bas.scaling_segment_id = pan.population_scaling_segment_id



                   /**************** PART 2Bii: ASSIGNING WEIGHTS TO EACH SEGMENT ****************/
                -- Rim-weighting is an iterative process that iterates through each of the scaling variables
                -- individually until the category sum of weights converge to the population category subtotals

               SET @cntr           = 1
               SET @iteration      = 0
               SET @cntr_var       = 1
               SET @scaling_var    = (SELECT scaling_variable FROM vespa_analysts.SC3_Variables_lookup_v1_1 WHERE id = @cntr)

                -- The weighting_working_table table contains subtotals and sum_of_weights for all segments represented by
                -- the sky base.
                -- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
                -- to ensure convergence.

                -- arbitrary value to ensure convergence
            update weighting_working_table
               set panel_accounts = 0.000001
             where panel_accounts = 0
                or panel_accounts is null

            commit

                -- Initialise working columns
            update weighting_working_table as bas
               set sum_of_weights = panel_accounts

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
                -- In this scenario, the person running the code should send the results of the SC2_metrics for that
                -- week to analytics team for review. ## What exactly are we checking? can we automate any of it?

             WHILE @cntr <6 BEGIN
                      DELETE FROM category_working_table

                         SET @cntr_var = 1
                       WHILE @cntr_var < 6 BEGIN
                                SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3_Variables_lookup_v1_1 WHERE id = @cntr_var

                               EXECUTE('
                                INSERT INTO category_working_table(universe
                                                                  ,profile
                                                                  ,value
                                                                  ,sky_base_accounts
                                                                  ,panel_accounts
                                                                  ,sum_of_weights
                                                                  )
                                SELECT srs.universe
                                      ,@scaling_var
                                      ,ssl.'||@scaling_var||'
                                      ,SUM(srs.sky_base_accounts)
                                      ,SUM(srs.panel_accounts)
                                      ,SUM(srs.sum_of_weights)
                                  FROM weighting_working_table AS srs
                                       inner join vespa_analysts.SC3_Segments_lookup_v1_1 AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                              GROUP BY srs.universe,ssl.'||@scaling_var||'
                              ORDER BY srs.universe
                                ')

                                   SET @cntr_var = @cntr_var + 1
                                commit
                         END

                      UPDATE category_working_table
                         SET category_weight = sky_base_accounts / sum_of_weights
                            ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

                      SELECT @convergence = SUM(convergence_flag) FROM category_working_table
                         SET @iteration = @iteration + 1
                      SELECT @scaling_var = scaling_variable FROM vespa_analysts.SC3_Variables_lookup_v1_1 WHERE id = @cntr

                     EXECUTE ('
                      UPDATE weighting_working_table as bas
                         SET category_weight = cat.category_weight
                            ,sum_of_weights  = bas.sum_of_weights * cat.category_weight
                        FROM vespa_analysts.SC3_Segments_lookup_v1_1 AS ssl
                             inner join category_working_table AS cat ON cat.value = ssl.'||@scaling_var||'
                                                                     AND cat.universe = ssl.sky_base_universe
                       where bas.scaling_segment_id = ssl.scaling_segment_id
                             ')

                      commit

                          IF @iteration = 100 OR @convergence = 0 SET @cntr = 6
              ELSE

                          IF @cntr = 5  SET @cntr = 1 ELSE SET @cntr = @cntr+1

               END

            commit

                -- Calculate segment weight and corresponding indices

                -- This section calculates the segment weight which is the weight that should be applied to viewing data
                -- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting
            SELECT @sky_base = SUM(sky_base_accounts) FROM weighting_working_table
            SELECT @vespa_panel = SUM(panel_accounts) FROM weighting_working_table
            SELECT @sum_of_weights = SUM(sum_of_weights) FROM weighting_working_table

            UPDATE weighting_working_table
               SET segment_weight = sum_of_weights / panel_accounts
                  ,indices_actual = 100*(panel_accounts / @vespa_panel) / (sky_base_accounts / @sky_base)
                  ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)

            commit

                -- OK, now catch those cases where stuff diverged because segments weren't reperesented:
            update weighting_working_table
               set segment_weight  = 0.000001
             where panel_accounts   = 0.000001

            commit

            select '' as scaling_day
                  ,scaling_segment_id
                  ,panel_accounts
                  ,sky_base_accounts
                  ,segment_weight as weighting
                  ,sum_of_weights
                  ,indices_actual
                  ,indices_weighted
                  ,case when abs(sky_base_accounts - sum_of_weights) > 3 then 1 else 0 end
              into weightings
              from weighting_working_table

                -- First off extend the intervals that are already in the table:

     end; -- of procedure "make_weights"
  commit;



         ---------------------------------------------
         -- Part 3 run this code to run the procedures
         ---------------------------------------------

 EXECUTE prepare_panel_members;
 EXECUTE make_weights         ;




         -------------------------------------------------------
         -- Part 4 calculate effective sample size for the panel
         -------------------------------------------------------
  select sum(weighting * weighting) as large  -- remove RQ, keep weights as before
        ,sum(weighting) as small     -- remove RQ
    into #ess2
    from Todays_panel_members  as tpm
         inner join Weightings as wei on tpm.scaling_segment_id = wei.scaling_segment_id
;

  select (small * small)/large from #ess2;




         -------------------------------------------------------------------------
         -- Part 5 compare results against those derived from the production table
         -------------------------------------------------------------------------
--select max(profiling_date) from thompsonja.SC3_Sky_base_segment_snapshots --to find latest date

  create variable @scaling_day date;
     set @scaling_day = '2014-08-07'; -- update as appropriate (set to profiling_date in SC3_Sky_base_segment_snapshots

  select sum(calculated_scaling_weight * calculated_scaling_weight) as large
        ,sum(calculated_scaling_weight)                             as small
        ,(small * small) / large                                    as effective_sample_size
        ,count(*)                                                   as total_accounts
        ,adjusted_event_start_date_vespa
    from sk_prod.viq_viewing_data_scaling
   where adjusted_event_start_date_vespa = @scaling_day
group by adjusted_event_start_date_vespa
;



/*
--ESS over time
  select scaling_date_key
        ,sum(calculated_scaling_weight * calculated_scaling_weight) as large
        ,sum(calculated_scaling_weight) as small
        ,count(distinct account_number) as accounts
    into #ess2
    from sk_prod.VIQ_VIEWING_DATA_SCALING
   where scaling_date_key >=  2014070100
group by scaling_date_key
;

  select scaling_date_key
        ,(small * small)/large
        ,accounts
    from #ess2
;
*/



