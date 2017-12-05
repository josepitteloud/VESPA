	
        -- #########################################################################################
        -- ##### Example 1 - Calculation of Average Daily Viewing (ADV) duration per household #####
        -- #########################################################################################
		
		
SELECT
	  SUM(case when distinct_order = 1 then bespoke_scaling_weight end) total_weights,
	, SUM (cast(total_duration * bespoke_scaling_weight as double ) / cast(days_returned as double) / cast(3600 as double)) total_weight_duration
            INTO #average_viewing
	FROM
		(SELECT 
		        account_number
                , bespoke_scaling_weight
                , total_duration
                , days_returned
                , row_number() over(partition by account_number, bespoke_scaling_weight 
				                    order by account_number, bespoke_scaling_weight) distinct_order
		FROM vespa_shared.TVoC_Aggregate_201412) as x

							
		
        -- ######################################################################################
        -- ##### Example 2 – ADV / SOV per household for viewing dimensions (e.g. by genre) #####
        -- ######################################################################################
			
		--Step 1 - Calculate universe (total households)
		
SELECT
        SUM(case when distinct_order = 1 then bespoke_scaling_weight 	end) total_weights
            INTO #average_viewing
            FROM
                (SELECT  account_number
                        , bespoke_scaling_weight
                        , row_number() over(partition by account_number, bespoke_scaling_weight 
						                    order by account_number, bespoke_scaling_weight) distinct_order
        FROM vespa_shared.TVoC_Aggregate_201412) as x

		
		--Step 2 - Calculate total viewing duration by genre
		
		
SELECT   case when genre in ('Undefined'.'Music & Radio','Specialist', 	'Unknown', 'DUMMY') then 'Other' else genre end as 	genre
        ,SUM (cast(total_duration * bespoke_scaling_weight as double) / cast(days_returned as double) / cast(3600 as double)) 	as total_weighted_duration
            FROM vespa_shared.TVoC_Aggregate_201412
                GROUP BY genre

				
				
		-- ##########################################################################################
        -- ##### Example 3 – ADV per household for customer dimensions (e.g. by package holding)#####
        -- ##########################################################################################		
	

SELECT premiums
        ,SUM (case when distinct_order = 1 then bespoke_scaling_weight end) total_weights
        ,SUM (cast(total_duration * bespoke_scaling_weight as double) / cast(days_returned as 	double) / cast(3600 as double)) total_weighted_duration
            INTO #premiums
            FROM
                SELECT  account_number
                        , bespoke_scaling_weight
                        , total_duration
                        , days_returned
                        , premiums
                        , row_number() over(partition by account_number, bespoke_scaling_weight
                                  	        ORDER BY account_number, bespoke_scaling_weight) distinct_order
            FROM vespa_shared.TVoC_Aggregate_201412) as x
                GROUP BY premiums

	
		
		-- ######################################################################################
        -- ##### Example 4 – ADV / SOV for a combination of viewing and customer dimensions #####
        -- ######################################################################################	
		
		-- Calculate total weighted viewing duration per premium split by channel_type
		
SELECT      premiums
            ,channel_type
            ,SUM(case when distinct_order = 1 then bespoke_scaling_weight end) total_weights                   
            ,SUM (cast(total_duration * bespoke_scaling_weight as double) / cast(days_returned as double) / cast(3600 as double)) total_weighthed_duration
            INTO #premiums
            FROM
                (SELECT account_number
                        ,channel_type
                        ,bespoke_scaling_weight
                        ,total_duration
                        ,days_returned
                        ,premiums
                        ,row_number() over(partition by account_number, bespoke_scaling_weight 
	            ORDER BY  account_number, bespoke_scaling_weight) distinct_order
            FROM vespa_shared.TVoC_Aggregate_201412) as x
                GROUP BY premiums ,channel_type


		
		
		-- ###############################################################################
        -- #####          Example 5 - Genre per viewing_type SOV                     #####
        -- ###############################################################################	
		
		--Step 1 - Calculate universe (total households)
	SELECT
            SUM(case when distinct_order = 1 then bespoke_scaling_weight 	end) total_weights
            INTO #average_viewing
            FROM
                (SELECT  account_number
                        , bespoke_scaling_weight
                        , row_number() over(partition by account_number, bespoke_scaling_weight 
						                    order by account_number, bespoke_scaling_weight) distinct_order
        FROM vespa_shared.TVoC_Aggregate_201412) as x
		
		-- Step 2 - Calculate total viewing by genre and viewing type
		
	SELECT   case when genre in ('Undefined','Music & Radio','Specialist', 'Unknown', 'DUMMY') then 'Other' else genre end as 	genre
                  , viewing_type 
                  , SUM (cast(total_duration * bespoke_scaling_weight as double) / 	cast(days_returned as double) / cast(3600 as double)) 	as total_weighted_duration
        FROM vespa_shared.TVoC_Aggregate_201412
            GROUP BY genre, viewing_type

	
			
		-- ###############################################################################
        -- #####                    Linking TVoC to other data              		 #####
		-- #####        Calculating ADV and SOV by viewing_type per cluster          #####
        -- ###############################################################################	
		
		--Step 1 - calculate universe base with other scaling weights
		
		
SELECT      Cluster_Number
            ,SUM (bespoke_weight_Jun_set) total_wghts  -- using scaling weights calculated on cluster groups
                FROM  Cluster_Data
                GROUP BY Cluster_Number
				
		--Step 2 - joining TVoC viewing data to our cohort data table
	
SELECT count(*)
            , tvoc.viewing_type
            , tvoc.total_duration
            , tvoc.days_returned
            , churn.Cluster_Number
            , churn.account_number
            , churn.bespoke_weight_set
INTO viewing_churn
FROM vespa_shared.TVoC_Aggregate_201412 as tvoc
RIGHT JOIN viewing_churn_output_view as churn
ON tvoc.account_number = churn.account_number
GROUP BY
                viewing_type
                , total_duration
                , days_returned
                , b.Cluster_Number
                , b.account_number
                , b.bespoke_weight_set

		
		--Step 3 - aggregate data to generate total daily viewing duration and cuts by viewing_type
		
SELECT  Cluster_Number
                , SUM (cast(total_duration * bespoke_weight_set as double) / 	cast(days_returned as double) / cast(3600 as double)) total_weighted_duration
                , viewing_type
FROM viewing_churn
GROUP BY        Cluster_Number
                ,viewing_type

		
		
		
		
						