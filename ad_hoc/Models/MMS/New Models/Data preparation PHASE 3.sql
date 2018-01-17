------ Contents ------
--  0.0 Configuration
--  1.0 Create CS_scoring_base_20171213 dataset
    --  1.1 Create initial table
    --  1.2 Add Orders fields
    --  1.4 Add target and eligibility flags
    --  1.5 Create target flags
        --  1.5.1 Boxsets
        --  1.5.2 Non-Fibre Broadband
        --  1.5.3 Fibre Broadband
        --  1.5.4 Regrade Fibre Broadband
    --  1.6 Create Eligiblility flags
        --  1.6.1 Boxsets
        --  1.6.2 Non-Fibre Broadband
        --  1.6.3 Fibre Broadband
        --  1.6.4 Regrade Fibre Broadband
    --  1.7 Random sample variable
--  2.0 Creating extra variables
    --  2.1 PPV Sports Events
    --  2.2 OD fields
    --  2.3 Recode DTV product holding
    --  2.4 Create TA reason flags
    --  2.5 Cleanup
        --  2.5.1 Age
        --  2.5.2 Missing values
        --  2.5.3 Turn dates into days
    --  2.6 NOW TV Variables
--  3.0 Create final tables
    --  3.1 Create binning table
    --  3.2 Create base table
    --  3.3 Create final base tables for each target
        --  3.3.1 Boxsets
        --  3.3.2 Non-Fibre Broadband
        --  3.3.3 Fibre Broadband
        --  3.3.4 Regrade Fibre Broadband
    --  3.4 Create final binned tables for each target
        --  3.4.1 Boxsets
        --  3.4.2 Non-Fibre Broadband
        --  3.4.3 Fibre Broadband
        --  3.4.4 Regrade Fibre Broadband
    --  3.5 Permissions to public

-------------------------------------------
--  0.0 Configuration
-------------------------------------------
MESSAGE CAST(now() as timestamp)||' | Initializing' TO CLIENT
	
    CREATE OR REPLACE VARIABLE @start_date DATE;
    CREATE OR REPLACE VARIABLE @end_date DATE;
    CREATE OR REPLACE VARIABLE @end_date_lag DATE;
	GO
    SET @start_date   = '2017-09-01';
    SET @end_date     = '2017-11-01';
    SET @end_date_lag = DATEADD(mm, -1, @end_date)

-------------------------------------------
--  1.0 Create CS_scoring_base_20171213 dataset
-------------------------------------------

    --  1.1 Create initial table

	MESSAGE CAST(now() as timestamp)||' | 1' TO CLIENT

        DROP TABLE  IF EXISTS #Qtr_Wk_End_Dts;
        /*SELECT calendar_date
        INTO #Qtr_Wk_End_Dts
        FROM sky_calendar
        WHERE datepart(DAY,calendar_date+1) = 1
                AND calendar_date BETWEEN @start_date AND @end_date;*/
		
		SELECT CAST('2017-12-01' AS DATE) AS calendar_date
        INTO #Qtr_Wk_End_Dts	
        COMMIT;

        CREATE lf INDEX idx_1 ON #Qtr_Wk_End_Dts(Calendar_Date);


        DROP TABLE IF EXISTS CS_scoring_base_20171213;
        SELECT Cast(wk.calendar_date AS date) Base_Dt,account_number--,product_holding DTV_Product_Holding,status_code as DTV_Status_Code
        INTO CS_scoring_base_20171213
        FROM #Qtr_Wk_End_Dts wk
             INNER JOIN
             cust_subs_hist asr
             ON wk.calendar_date BETWEEN effective_from_dt AND effective_to_dt - 1
                AND subscription_sub_type = 'DTV Primary Viewing'
                AND status_code IN ('AB','AC','PC')
        GROUP BY Base_Dt,account_number
        UNION
        SELECT Cast(wk.calendar_date AS date) Base_Dt,account_number--,product_holding BB_Product_Holding,status_code as BB_Status_Code
        FROM #Qtr_Wk_End_Dts wk
             INNER JOIN
             Decisioning.Active_Subscriber_Report asr
             ON wk.calendar_date BETWEEN effective_from_dt AND effective_to_dt - 1
                AND subscription_sub_type = 'Broadband'
        GROUP BY Base_Dt,account_number;
        COMMIT;
				  
					  
		MESSAGE CAST(now() as timestamp)||' | 2' TO CLIENT;

        CREATE HG INDEX id1 ON CS_scoring_base_20171213 (account_number);
        CREATE DATE INDEX iddt ON CS_scoring_base_20171213 (base_dt);

        CALL Decisioning_procs.Add_Subs_Calendar_Fields('CS_scoring_base_20171213','Base_Dt');
		
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_scoring_base_20171213','Base_Dt','DTV');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_scoring_base_20171213','Base_Dt','BB');
		
		Call Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_scoring_base_20171213','Base_Dt','Sports');
		Call Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_scoring_base_20171213','Base_Dt','Movies');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_scoring_base_20171213','Base_Dt','MULTISCREEN');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_scoring_base_20171213','Base_Dt','SGE');
		MESSAGE CAST(now() as timestamp)||' | 2.1' TO CLIENT;

		CALL Decisioning_Procs.Add_Activations_DTV('CS_scoring_base_20171213','Base_Dt');
        CALL Decisioning_Procs.Add_Activation_BB('CS_scoring_base_20171213','Base_Dt');
		MESSAGE CAST(now() as timestamp)||' | 2.2' TO CLIENT;
		
		CALL Decisioning_Procs.Add_Churn_DTV ('CS_scoring_base_20171213','Base_Dt');
		CALL Decisioning_Procs.Add_Churn_BB ('CS_scoring_base_20171213','Base_Dt');
		CALL Decisioning_Procs.Add_PL_Entries_DTV('CS_scoring_base_20171213','Base_Dt');
		CALL Decisioning_Procs.Add_PL_Entries_BB('CS_scoring_base_20171213','Base_Dt');
		MESSAGE CAST(now() as timestamp)||' | 2.3' TO CLIENT;
		
																				 
        CALL Decisioning_procs.Add_Demographics_To_Base('CS_scoring_base_20171213','Base_Dt');
		MESSAGE CAST(now() as timestamp)||' | 2.4' TO CLIENT;
		
		CALL Decisioning_procs.Add_Offers_Software('CS_scoring_base_20171213','Base_Dt','DTV');
        CALL Decisioning_procs.Add_Offers_Software('CS_scoring_base_20171213','Base_Dt','BB');
        Call Decisioning_procs.Add_Software_Orders('CS_scoring_base_20171213','Base_Dt','Movies');
		Call Decisioning_procs.Add_Software_Orders('CS_scoring_base_20171213','Base_Dt','Sports');
		MESSAGE CAST(now() as timestamp)||' | 2.5' TO CLIENT;
        
        CALL Decisioning_procs.Add_Broadband_Postcode_Exchange_To_Base('CS_scoring_base_20171213');
        CALL Decisioning_procs.Add_Fibre_Areas('CS_scoring_base_20171213');
        CALL Decisioning_Procs.Add_Turnaround_Attempts('CS_scoring_base_20171213','Base_Dt','TA Events');
		CALL Decisioning_procs.Add_BB_Provider('CS_scoring_base_20171213','Base_Dt');
		Call Decisioning_procs.Add_Software_Orders('cs_binned2','Base_Dt','MS+','Account_Number','Drop and Replace')

		
		MESSAGE CAST(now() as timestamp)||' | 3' TO CLIENT;
																			
	
    --  1.2 Add Orders fields
	------------------------------------------------------------------------------------------------
	------- WE NEED TO CHECK WHICH ORDERS ARE RELEVANT AND IF THEY ARE AVIALBLE THROUGH MODE T 
	------------------------------------------------------------------------------------------------
	
	--  1.4 Add target and eligibility flags

        ALTER TABLE CS_scoring_base_20171213
        ADD (UP_Rental          BIT DEFAULT 0 
            ,UP_buy_and_Keep	BIT DEFAULT 0 
            ,UP_SkyQ			BIT DEFAULT 0 
			,UP_skyQ_MS 		bit DEFAULT 0 
		
            ,Rental_eligible       	BIT DEFAULT 0 
            ,Buy_and_keep_eligible 	BIT DEFAULT 0 
            ,SkyQ_eligible 	 		BIT DEFAULT 0 
			, Up_mobile 		BIT DEFAULT 0 
			, Mobile_eligible 	BIT DEFAULT 0 
			, UP_movies			BIT DEFAULT 0 
			, UP_sports			BIT DEFAULT 0 
			, movies_eligible			BIT DEFAULT 0 
			, sports_eligible			BIT DEFAULT 0 
			, rentals_used_before		BIT DEFAULT 0 
			, bak_used_before		BIT DEFAULT 0 
			, base_dt_2 DATE DEFAULT NULL 		-- to generate TA next 30d
			, TA_next_30d BIT DEFAULT 0			-- to removed upgrades related to TA
			, Order_MS_added_next_60d AS tinyint DEFAULT null
            , Order_MS_removed_next_60d AS tinyint DEFAULT null
            );
		MESSAGE CAST(now() as timestamp)||' | 4' TO CLIENT
		GO
    --  1.5 Create target flags

		
			-- Rental_eligible
			
			SELECT b.account_number
				, MAX(ordered_dt) AS max_dt
				, base_dt
			INTO #temp_rental_usage_over_last_12_months
			FROM Decisioning.OTT_Purchases 	AS a 
			JOIN CS_scoring_base_20171213 					AS b  ON a.account_number = b.account_number AND a.ordered_dt <= base_dt
			WHERE movie_order = 1 
				AND movie_cancelled_order = 0 
			GROUP BY b.account_number, base_dt
            
			COMMIT
			CREATE HG INDEX id1 	ON #temp_rental_usage_over_last_12_months (account_number)
			CREATE DATE INDEX id2 	ON #temp_rental_usage_over_last_12_months (max_dt)
			COMMIT

			UPDATE CS_scoring_base_20171213
			SET Rental_eligible	= CASE 	WHEN DATEDIFF(WEEK, max_dt, GETDATE()) <= 13 					THEN 0 --- Active customer
										WHEN DATEDIFF(WEEK, max_dt, GETDATE())  > 13 OR max_dt IS NULL 	THEN 1 --- Lapsed or non-customer
										ELSE 1
										END
				, rentals_used_before	= CASE 	WHEN  max_dt IS NOT NULL THEN 1 ELSE 0 END
			FROM CS_scoring_base_20171213 AS a 
			LEFT JOIN #temp_rental_usage_over_last_12_months	AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
	 

			
			
			DROP TABLE #temp_rental_usage_over_last_12_months
			
			MESSAGE CAST(now() as timestamp)||' | 5' TO CLIENT
			GO
	

			--------- Buy_and_keep_eligible
						
			SELECT b.account_number
				, MAX(ordered_dt) AS max_dt
				, base_dt
			INTO #temp_buy_and_keep_usage_recency
			FROM Decisioning.OTT_Purchases 	AS a 
			JOIN CS_scoring_base_20171213 					AS b  ON a.account_number = b.account_number AND a.ordered_dt <= base_dt
			WHERE product_type = 'EST'
			GROUP BY b.account_number, base_dt
            
			
			COMMIT
			CREATE HG INDEX id1 	ON #temp_buy_and_keep_usage_recency (account_number)
			CREATE DATE INDEX id2 	ON #temp_buy_and_keep_usage_recency (base_dt)
			CREATE DATE INDEX id3 	ON #temp_buy_and_keep_usage_recency (max_dt)
			COMMIT
			
			UPDATE CS_scoring_base_20171213 a
			SET Buy_and_keep_eligible	 = CASE 	WHEN DATEDIFF(WEEK, max_dt, GETDATE()) <= 13 				THEN 0 ---Active customer
													WHEN DATEDIFF(WEEK, max_dt, GETDATE())  > 13 				THEN 1 ---Lapsed
													WHEN max_dt IS NULL 										THEN 1 --- Never bought
													ELSE 1	------ Just in case
					   
					   
							   
											 END
				, bak_used_before	= CASE 	WHEN  max_dt IS NOT NULL THEN 1 ELSE 0 END
			FROM CS_scoring_base_20171213 a
			LEFT JOIN #temp_buy_and_keep_usage_recency AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
			
			DROP TABLE #temp_buy_and_keep_usage_recency
			COMMIT
					
			MESSAGE CAST(now() as timestamp)||' | 6' TO CLIENT
		GO
			
	  	
			--------- Mobile_eligible
			
			SELECT c.account_number
                , MAX(a.prod_earliest_mobile_ordered_dt) dt 
				, base_dt
            INTO #mobile
			FROM cust_single_mobile_account_view    AS a
			JOIN cust_single_mobile_view            AS b ON a.account_number = b.account_number
			JOIN cust_single_account_view           AS c ON a.portfolio_id = c.acct_fo_portfolio_id
			JOIN CS_scoring_base_20171213 							AS x ON x.account_number = c.account_number AND a.prod_earliest_mobile_ordered_dt <= base_dt
			GROUP BY c.account_number, base_dt 
						
			COMMIT
			CREATE HG INDEX id1 	ON #mobile (account_number)
			CREATE DATE INDEX id2 	ON #mobile (base_dt)
			CREATE DTTM INDEX id3 	ON #mobile (dt)
			COMMIT
			
			UPDATE CS_scoring_base_20171213 a
			SET Mobile_eligible	= CASE WHEN cps.account_number IS NULL THEN 1 ELSE 0 END 
			FROM CS_scoring_base_20171213 a
			LEFT JOIN #mobile AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
			
			DROP TABLE #mobile
			COMMIT
			MESSAGE CAST(now() as timestamp)||' | 7' TO CLIENT
		GO

					
			--------- SkyQ_eligible ----
			---- 	Accounts that have Sky Q installed
			SELECT  stb.account_number
					,MIN(CASE 	WHEN x_description  in ('Sky Q Silver','Sky Q Mini','Sky Q 2TB box','Sky Q','Sky Q 1TB box') THEN 0 	--- Known box descriptions
								WHEN UPPER(x_description) LIKE '%SKY Q%'	THEN 0														--- Any other new model
								ELSE 1 END ) 		AS PrimaryBoxType
					, base.base_dt
			INTO #sky_q_elig
			FROM cust_set_top_box AS stb
			JOIN CS_scoring_base_20171213 AS base ON stb.account_number = base.account_number AND stb.created_dt <= base.base_dt 
			WHERE   base.account_number IS NOT NULL
			GROUP BY  stb.account_number
					, base.base_dt
			
			COMMIT
			CREATE HG INDEX id1 	ON #sky_q_elig (account_number)
			CREATE DATE INDEX id2 	ON #sky_q_elig (base_dt)
			COMMIT 
			
			UPDATE CS_scoring_base_20171213
			SET    SkyQ_eligible = b.PrimaryBoxType
			FROM   CS_scoring_base_20171213 AS base
			JOIN  	#sky_q_elig AS b ON b.account_number = base.account_number AND b.base_dt = base.base_dt 
						
			--- Flagging Black tier AND non-DTV active customers as non-eligible
			UPDATE CS_scoring_base_20171213
			SET    SkyQ_eligible = 0
			WHERE DATEDIFF(YEAR, DTV_Last_Activation_Dt, base_dt) >= 15		-- Black tier customers	
				OR DTV_active = 0 											-- Non DTV customers
			
			MESSAGE CAST(now() as timestamp)||' | 8' TO CLIENT
		GO			
		
		--------- Movies / Sports eligible ----
					
					
		UPDATE CS_scoring_base_20171213 
		SET movies_eligible = 0 
			, sports_eligible = 0 
		
		UPDATE CS_scoring_base_20171213 
		SET movies_eligible = 1
		WHERE Movies_Active = 0 
		AND Order_Movies_Added_In_Last_30d = 0 
		
		UPDATE CS_scoring_base_20171213 
		SET sports_eligible = 1
		WHERE sports_Active = 0 
		AND Order_Sports_Added_In_Last_30d = 0 
		
		
		
			----------------------------------------------------------------------------------			
			----------------------------------------------------------------------------------			UPSELL FLAGS 
			----------------------------------------------------------------------------------			
			
			-- Up_Rental
			SELECT b.account_number
				, MAX(ordered_dt) AS max_dt
				, base_dt
			INTO #temp_rental_usage_over_last_12_months
			FROM Decisioning.OTT_Purchases 	AS a 
			JOIN CS_scoring_base_20171213 					AS b  ON a.account_number = b.account_number AND a.ordered_dt  BETWEEN DATEADD(DAY, 1 ,base_dt) AND DATEADD(MONTH, 1,base_dt) -- Rentals within the next 30 days after the observation date
			WHERE movie_order = 1 
				AND movie_cancelled_order = 0 
			GROUP BY b.account_number, base_dt

			COMMIT
			CREATE HG INDEX id1 	ON #temp_rental_usage_over_last_12_months (account_number)
			CREATE DATE INDEX id2 	ON #temp_rental_usage_over_last_12_months (max_dt)
			COMMIT

			UPDATE CS_scoring_base_20171213
			SET Up_Rental	= CASE 	WHEN cps.max_dt IS NOT NULL THEN 1 ELSE 0 END 
			FROM CS_scoring_base_20171213 AS a 
			LEFT JOIN #temp_rental_usage_over_last_12_months	AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
			

			DROP TABLE #temp_rental_usage_over_last_12_months

			MESSAGE CAST(now() as timestamp)||' | 9' TO CLIENT
		GO
			--------- UP_buy_and_Keep 
					
						
			SELECT b.account_number
				, MAX(ordered_dt) AS max_dt
				, base_dt
			INTO #temp_buy_and_keep_usage_recency
			FROM Decisioning.OTT_Purchases 	AS a 
			JOIN CS_scoring_base_20171213 					AS b  ON a.account_number = b.account_number AND a.ordered_dt  BETWEEN DATEADD(DAY, 1 ,base_dt) AND DATEADD(MONTH, 1,base_dt)
			WHERE product_type = 'EST'
			GROUP BY b.account_number, base_dt
            
			COMMIT
			CREATE HG INDEX id1 	ON #temp_buy_and_keep_usage_recency (account_number)
			CREATE DATE INDEX id2 	ON #temp_buy_and_keep_usage_recency (base_dt)
			CREATE DATE INDEX id3 	ON #temp_buy_and_keep_usage_recency (max_dt)
			COMMIT

			UPDATE CS_scoring_base_20171213 a
			SET UP_buy_and_Keep	 = CASE WHEN cps.max_dt IS NOT NULL THEN 1 ELSE 0 END 
			FROM CS_scoring_base_20171213 a
			LEFT JOIN #temp_buy_and_keep_usage_recency AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt

			DROP TABLE #temp_buy_and_keep_usage_recency
			COMMIT

			 
	   
			MESSAGE CAST(now() as timestamp)||' | 10' TO CLIENT
		GO

			--------- UP_SkyQ
			
			SELECT  stb.account_number
					,MAX(CASE WHEN x_description  in ('Sky Q Silver','Sky Q Mini','Sky Q 2TB box','Sky Q','Sky Q 1TB box') THEN 1 --- Known box descriptions
							WHEN UPPER(x_description) LIKE '%SKY Q%'	THEN 1													--- Any other new model
							ELSE 0 END ) 		AS PrimaryBoxType
					, base.base_dt
			INTO #sky_q_up
			FROM cust_set_top_box AS stb
			JOIN CS_scoring_base_20171213 AS base ON stb.account_number = base.account_number AND stb.created_dt BETWEEN DATEADD(DAY, 1 ,base_dt) AND DATEADD(MONTH, 1,base_dt) -- Installations within the next 30 days after the observation date
			WHERE   base.account_number IS NOT NULL
			GROUP BY  stb.account_number
					, base.base_dt
			
			COMMIT
			CREATE HG INDEX id1 	ON #sky_q_up (account_number)
			CREATE DATE INDEX id2 	ON #sky_q_up (base_dt)
			COMMIT 
			
			UPDATE CS_scoring_base_20171213
			SET    UP_SkyQ = b.PrimaryBoxType
			FROM   CS_scoring_base_20171213 AS base
			JOIN  	#sky_q_up AS b ON b.account_number = base.account_number AND b.base_dt = base.base_dt 
			

			--------- UP_SkyQ + MS 
			
			
			UPDATE CS_scoring_base_20171213 a
			SET a.Order_MS_added_next_60d  = b.Order_MULTISCREEN_PLUS_Added_In_Next_30d
				, a.Order_MS_removed_next_60d = b.Order_MULTISCREEN_PLUS_Removed_In_Next_30d
			FROM CS_scoring_base_20171213  as a 
			join CS_scoring_base_20171213  as b on a.account_number = b.account_number  ANd a.base_dt_2 = b.base_dt					
			
			UPDATE CS_scoring_base_20171213
			SET    UP_skyQ_MS = 1
			FROM   CS_scoring_base_20171213 AS base
			JOIN  	#sky_q_up AS b ON b.account_number = base.account_number AND b.base_dt = base.base_dt  AND PrimaryBoxType = 1 
			WHERE  (Order_MULTISCREEN_PLUS_Added_In_Next_30d +  Order_MS_added_next_60d )- (Order_MULTISCREEN_PLUS_Removed_In_Next_30d + Order_MS_removed_next_60d) > 0
			


	   
			 


			COMMIT 

			MESSAGE CAST(now() as timestamp)||' | 11' TO CLIENT
		GO
			
			----- Up_mobile
			
			SELECT c.account_number
                , MAX(a.prod_earliest_mobile_ordered_dt) dt 
				, base_dt
            INTO #mobile
			FROM cust_single_mobile_account_view    AS a
			JOIN cust_single_mobile_view            AS b ON a.account_number = b.account_number
			JOIN cust_single_account_view           AS c ON a.portfolio_id = c.acct_fo_portfolio_id
			JOIN CS_scoring_base_20171213 							AS x ON x.account_number = c.account_number 
														AND a.prod_earliest_mobile_ordered_dt BETWEEN DATEADD(DAY, 1 ,base_dt) AND DATEADD(MONTH, 1,base_dt) 
			GROUP BY c.account_number, base_dt 
						
			COMMIT
			CREATE HG INDEX id1 	ON #mobile (account_number)
			CREATE DATE INDEX id2 	ON #mobile (base_dt)
			CREATE DTTM INDEX id3 	ON #mobile (dt)
			COMMIT
			
			UPDATE CS_scoring_base_20171213 a
			SET Up_mobile = CASE WHEN cps.account_number IS NOT NULL THEN 1 ELSE 0 END 
			FROM CS_scoring_base_20171213 a
			LEFT JOIN #mobile AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
			
			DROP TABLE #mobile
			COMMIT
			
			--------- Movies / Sports Upsell flags ----
							
			UPDATE CS_scoring_base_20171213 
			SET base_dt_2  = CASE    WHEN base_DT = '2017-02-28' THEN '2017-03-31'
					WHEN base_DT = '2017-03-31' THEN '2017-04-30'
					WHEN base_DT = '2017-04-30' THEN '2017-05-31'
					WHEN base_DT = '2017-05-31' THEN '2017-06-30'
					WHEN base_DT = '2017-06-30' THEN '2017-07-31'
					WHEN base_DT = '2017-07-31' THEN '2017-08-31'
					WHEN base_DT = '2017-08-31' THEN '2017-09-30'
					WHEN base_DT = '2017-09-30' THEN '2017-10-31'
					WHEN base_DT = '2017-10-31' THEN '2017-11-30'
			ELSE NULL END
								
			UPDATE CS_scoring_base_20171213 a
			SET a.TA_next_30d  = b.TAs_in_last_30d_b
			FROM CS_scoring_base_20171213  as a 
			join CS_scoring_base_20171213  as b on a.account_number = b.account_number  ANd a.base_dt_2 = b.base_dt					

	
	
	
			UPDATE CS_scoring_base_20171213
			SET UP_movies = 1 
			WHERE movies_eligible = 1 												
				AND Order_Movies_Added_In_Next_30d > 0
				AND Order_Movies_Added_In_Next_30d > Order_Movies_Removed_In_Next_30d
				AND TA_next_30d = 0
															
			UPDATE CS_scoring_base_20171213
			SET UP_sports = 1 
			WHERE sports_eligible = 1 	
					AND Order_Sports_Added_In_Next_30d > 0
					AND Order_Sports_Added_In_Next_30d > Order_Sports_Removed_In_Next_30d
					AND TA_next_30d = 0
			
		
		MESSAGE CAST(now() as timestamp)||' | 12' TO CLIENT
		GO

    --  1.7 Random sample variable

        CREATE Variable @multi BIGINT;
        SET @multi = DATEPART(MS,NOW())+1;
        ALTER TABLE CS_scoring_base_20171213 ADD rand_num DECIMAL(22,20);
        UPDATE CS_scoring_base_20171213
           SET rand_num = RAND(NUMBER(*)* @multi);
        CREATE HG INDEX idx1 on CS_scoring_base_20171213(rand_num);    

-------------------------------------------
--  2.0 Creating extra variables
-------------------------------------------

    --  2.1 PPV Sports Events

        ALTER TABLE CS_scoring_base_20171213
        ADD (num_sports_events      INT          DEFAULT NULL
            ,sports_downgrade_date  DATE         DEFAULT NULL 
            ,Sports_Tenure          VARCHAR(20)  DEFAULT NULL 
            ,movies_downgrade_date  DATE         DEFAULT NULL 
            ,Movies_Tenure          VARCHAR(20)  DEFAULT NULL 
            )


		MESSAGE CAST(now() as timestamp)||' | 13' TO CLIENT
		GO
        DROP TABLE  IF EXISTS #temp_ppv;
        SELECT       a.account_number
                    ,a.basE_dt
                    ,sum(CASE WHEN ppv_viewed_dt BETWEEN dateadd(mm,-12,base_dt) AND base_dt AND ppv_service='EVENT'
                              --AND  ppv_genre = 'BOXING, FOOTBALL or WRESTLING'
                              AND ppv_cancelled_dt = '9999-09-09' THEN 1 ELSE 0 END) AS num_sport_events_12m
							  
        INTO        #temp_ppv
        FROM        CS_scoring_base_20171213 a
        INNER JOIN  CUST_PRODUCT_CHARGES_PPV b
        ON          a.account_number   = b.account_number
        WHERE       b.ppv_cancelled_dt = '9999-09-09'
           AND      b.ppv_viewed_dt   <= base_dt
           AND      b.ppv_viewed_dt   >= (base_dt-365)
        GROUP BY     a.account_number
                    ,a.base_dt;

        UPDATE      CS_scoring_base_20171213 as a
        SET         a.num_sports_events = b.num_sport_events_12m
        FROM        #temp_ppv as b
        WHERE       a.account_number = b.account_number
        AND         a.base_dt = b.base_dt;

        UPDATE      CS_scoring_base_20171213
        SET          a.sports_downgrade_date = b.sports_downgrade_date 
                    ,a.Sports_Tenure         = b.Sports_Tenure 
                    ,a.movies_downgrade_date = b.movies_downgrade_date 
                    ,a.Movies_Tenure         = b.Movies_Tenure
        FROM        CS_scoring_base_20171213 As a 
        JOIN        citeam.CUST_FCAST_WEEKLY_BASE AS b 
        ON          a.account_number = b.account_number 
        AND         end_date BETWEEN DATEADD(DAY, -6, a.base_dt ) AND a.base_dt;

				MESSAGE CAST(now() as timestamp)||' | 14' TO CLIENT;
		GO
    --  2.2 OD fields

        ALTER TABLE CS_scoring_base_20171213
        ADD (OD_Last_3M             INT DEFAULT NULL
            ,OD_Last_12M            INT DEFAULT NULL 
            ,OD_Months_since_Last   INT DEFAULT NULL 
            );
    
				MESSAGE CAST(now() as timestamp)||' | 15' TO CLIENT;
		GO
        DROP TABLE  IF EXISTS #temp_od;
        SELECT       a.account_number
                    ,base_dt
                    ,MAX(last_modified_dt)         AS date_last_od
                    ,OD_Months_since_Last = CASE   WHEN DATEDIFF(MONTH, date_last_od , base_dt ) > 15 THEN 16 ELSE  DATEDIFF(MONTH, date_last_od , base_dt )  END 
                    ,SUM(CASE WHEN cast(last_modified_dt AS DATE) BETWEEN dateadd(mm, - 3, base_dt)    AND base_dt THEN 1 ELSE 0 END) AS OD_Last_3M
                    ,SUM(CASE WHEN cast(last_modified_dt AS DATE) BETWEEN dateadd(mm, - 12, base_dt)   AND base_dt THEN 1 ELSE 0 END) AS OD_Last_12M
        INTO        #temp_od
        FROM        CS_scoring_base_20171213 a
        INNER JOIN  CUST_ANYTIME_PLUS_DOWNLOADS b ON a.account_number = b.account_number
        WHERE       b.last_modified_dt <= base_dt
        GROUP BY    a.account_number, base_dt;

		 
		
		  
        CREATE HG Index id1 ON #temp_od(account_number);
        CREATE DATE Index id2 ON #temp_od(base_dt);

        UPDATE      CS_scoring_base_20171213 a
        SET          a.OD_Last_3M           = b.OD_Last_3M
                    ,a.OD_Last_12M          = b.OD_Last_12M
                    ,a.OD_Months_since_Last = b.OD_Months_since_Last
        FROM        #temp_od b
        WHERE       a.account_number = b.account_number;

		MESSAGE CAST(now() as timestamp)||' | 16' TO CLIENT;
		GO
    --  2.3 Recode DTV product holding

        ALTER TABLE CS_scoring_base_20171213 
            ADD DTV_product_holding_recode VARCHAR(40);
           
		  
		MESSAGE CAST(now() as timestamp)||' | 17' TO CLIENT;
		GO
		   
        UPDATE      CS_scoring_base_20171213 
        SET         DTV_product_holding_recode  = CASE WHEN DTV_Product_Holding = 'Box Sets'                                      THEN 'Box Sets'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Cinema'                          THEN 'Box Sets with Cinema'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Cinema 1'                        THEN 'Box Sets with Cinema'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Cinema 2'                        THEN 'Box Sets with Cinema'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Sports'                          THEN 'Box Sets with Sports'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Sports & Cinema'                 THEN 'Box Sets with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Sports & Cinema 1'               THEN 'Box Sets with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Sports & Cinema 2'               THEN 'Box Sets with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Sports 1'                        THEN 'Box Sets with Sports'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Sports 1 & Cinema'               THEN 'Box Sets with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Sports 2'                        THEN 'Box Sets with Sports'
                                                       WHEN DTV_Product_Holding = 'Box Sets with Sports 2 & Cinema'               THEN 'Box Sets with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original'                                      THEN 'Original'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy 2017)'                        THEN 'Original'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Cinema'            THEN 'Original with Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports'            THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports & Cinema'   THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports 1'          THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports 1 & Cinema' THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports 2'          THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy 2017) with Sports 2 & Cinema' THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy)'                             THEN 'Original'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Cinema'                 THEN 'Original with Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Cinema 1'               THEN 'Original with Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Cinema 2'               THEN 'Original with Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports'                 THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports & Cinema'        THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports & Cinema 1'      THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 1'               THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 1 & Cinema'      THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 1 & Cinema 1'    THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 1 & Cinema 2'    THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 2'               THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original (Legacy) with Sports 2 & Cinema'      THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original with Cinema'                          THEN 'Original with Cinema'
                                                       WHEN DTV_Product_Holding = 'Original with Sports'                          THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original with Sports & Cinema'                 THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original with Sports 1'                        THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original with Sports 1 & Cinema'               THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Original with Sports 2'                        THEN 'Original with Sports'
                                                       WHEN DTV_Product_Holding = 'Original with Sports 2 & Cinema'               THEN 'Original with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Sky Q'                                         THEN 'Sky Q'
                                                       WHEN DTV_Product_Holding = 'Sky Q with Cinema'                             THEN 'Sky Q with Cinema'
                                                       WHEN DTV_Product_Holding = 'Sky Q with Sports'                             THEN 'Sky Q with Sports'
                                                       WHEN DTV_Product_Holding = 'Sky Q with Sports & Cinema'                    THEN 'Sky Q with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Sky Q with Sports 1'                           THEN 'Sky Q with Sports'
                                                       WHEN DTV_Product_Holding = 'Sky Q with Sports 2'                           THEN 'Sky Q with Sports'
                                                       WHEN DTV_Product_Holding = 'Variety'                                       THEN 'Variety'
                                                       WHEN DTV_Product_Holding = 'Variety with Cinema'                           THEN 'Variety with Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Cinema 1'                         THEN 'Variety with Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Cinema 2'                         THEN 'Variety with Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports'                           THEN 'Variety with Sports'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports & Cinema'                  THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports & Cinema 1'                THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports & Cinema 2'                THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports 1'                         THEN 'Variety with Sports'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports 1 & Cinema'                THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports 1 & Cinema 1'              THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports 1 & Cinema 2'              THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports 2'                         THEN 'Variety with Sports'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports 2 & Cinema'                THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports 2 & Cinema 1'              THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding = 'Variety with Sports 2 & Cinema 2'              THEN 'Variety with Sports & Cinema'
                                                       WHEN DTV_Product_Holding IS NULL                                           THEN 'None'                                                       ELSE DTV_Product_Holding
                                                   END;

    --  2.4 Create TA reason flags

		MESSAGE CAST(now() as timestamp)||' | 18' TO CLIENT;
		GO
        ALTER TABLE CS_scoring_base_20171213
        ADD         (_1st_TA_reason_flag VARCHAR(15)
                    ,last_TA_reason_flag VARCHAR(15));
		
		MESSAGE CAST(now() as timestamp)||' | 18' TO CLIENT;
		GO
		  
							 
        
		UPDATE      CS_scoring_base_20171213
        SET          _1st_TA_reason_flag = CASE WHEN _1st_TA_reason IS NULL THEN 'No reason given' ELSE 'Reason given' END
                    ,last_TA_reason_flag = CASE WHEN last_TA_reason IS NULL THEN 'No reason given' ELSE 'Reason given' END;

			 
			   

--  2.5 Cleanup

        --  2.5.1 Age

            UPDATE      CS_scoring_base_20171213
            SET          Age = CASE 
								WHEN Age BETWEEN 18 AND 101     THEN Age
								WHEN Age BETWEEN 1916 AND 1999  THEN 2017-Age
								WHEN Age BETWEEN 1816 AND 1899  THEN 1917-Age
								WHEN Age BETWEEN 1016 AND 1099  THEN 1117-Age
								WHEN h_age_fine ='18-25'        THEN 22
								WHEN h_age_fine ='26-30'        THEN 28
								WHEN h_age_fine ='31-35'        THEN 33
								WHEN h_age_fine ='36-40'        THEN 37
								WHEN h_age_fine ='41-45'        THEN 43
								WHEN h_age_fine ='46-50'        THEN 48
								WHEN h_age_fine ='51-55'        THEN 53
								WHEN h_age_fine ='56-60'        THEN 58
								WHEN h_age_fine ='61-65'        THEN 63
								WHEN h_age_fine ='66-70'        THEN 68
								WHEN h_age_fine ='71-75'        THEN 73
								WHEN h_age_fine ='76+'          THEN 80
								ELSE NULL
							END;

        --  2.5.2 Missing values

            UPDATE      CS_scoring_base_20171213
            SET          ADSL_Enabled             = CASE WHEN ADSL_Enabled             IS NULL                         THEN 'Unknown' ELSE ADSL_Enabled             END
																																									   
						,Exchange_Status          = CASE WHEN Exchange_Status          IS NULL                         THEN 'Unknown' ELSE Exchange_Status          END
																																									   
						,DTV_CusCan_Churns_Ever   = CASE WHEN DTV_CusCan_Churns_Ever   IS NULL                         THEN 0         ELSE DTV_CusCan_Churns_Ever   END
                        ,DTV_Pending_cancels_ever = CASE WHEN DTV_Pending_cancels_ever IS NULL                         THEN 0         ELSE DTV_Pending_cancels_ever END
                        ,DTV_SysCan_Churns_Ever   = CASE WHEN DTV_SysCan_Churns_Ever   IS NULL                         THEN 0         ELSE DTV_SysCan_Churns_Ever   END
                     																																									   
																																									   
																																									   
						,_1st_TA_outcome          = CASE WHEN _1st_TA_outcome          IS NULL                         THEN 'No TA'   ELSE _1st_TA_outcome          END
                        ,last_TA_outcome          = CASE WHEN last_TA_outcome          IS NULL                         THEN 'No TA'   ELSE last_TA_outcome          END;

		MESSAGE CAST(now() as timestamp)||' | 19' TO CLIENT;
		GO
		  
									  
			 
        						
        --  2.5.3 Turn dates into days

            ALTER TABLE CS_scoring_base_20171213
            ADD         (DTV_Last_cuscan_churn          INT
                        ,DTV_Last_Activation            INT
                        ,DTV_Curr_Contract_Intended_End INT
                        ,DTV_Curr_Contract_Start        INT
                        ,DTV_Last_SysCan_Churn          INT
                        ,Curr_Offer_Start_DTV           INT
                        ,Curr_Offer_Actual_End_DTV      INT
                        ,DTV_1st_Activation             INT
                        ,BB_Curr_Contract_Intended_End  INT
                        ,BB_Curr_Contract_Start         INT
                        ,DTV_Last_Active_Block          INT
                        ,DTV_Last_Pending_Cancel        INT
                        ,BB_Last_Activation             INT
                        ,_1st_TA                        INT
                        ,last_TA                        INT
                        ,_1st_TA_save                   INT
                        ,last_TA_save                   INT
                        ,_1st_TA_nonsave                INT
                        ,last_TA_nonsave                INT);

		MESSAGE CAST(now() as timestamp)||' | 20' TO CLIENT;
		GO
        
            UPDATE      CS_scoring_base_20171213
            SET          DTV_Last_cuscan_churn          = DATEDIFF(DAY, DTV_Last_CusCan_Churn_Dt, base_dt)
                        ,DTV_Last_Activation            = DATEDIFF(DAY, DTV_Last_Activation_Dt, base_dt)
                        ,DTV_Curr_Contract_Intended_End = DATEDIFF(DAY, DTV_Curr_Contract_Intended_End_Dt, base_dt)
                        ,DTV_Curr_Contract_Start        = DATEDIFF(DAY, DTV_Curr_Contract_Start_Dt, base_dt)
                        ,DTV_Last_SysCan_Churn          = DATEDIFF(DAY, DTV_Last_SysCan_Churn_Dt, base_dt)
                        ,Curr_Offer_Start_DTV           = DATEDIFF(DAY, Curr_Offer_Start_Dt_DTV, base_dt)
                        ,Curr_Offer_Actual_End_DTV      = DATEDIFF(DAY, Curr_Offer_Actual_End_Dt_DTV, base_dt)
                        ,DTV_1st_Activation             = DATEDIFF(DAY, DTV_1st_Activation_Dt, base_dt)
                        ,BB_Curr_Contract_Intended_End  = DATEDIFF(DAY, BB_Curr_Contract_Intended_End_Dt, base_dt)
                        ,BB_Curr_Contract_Start         = DATEDIFF(DAY, BB_Curr_Contract_Start_Dt, base_dt)
                        ,DTV_Last_Active_Block          = DATEDIFF(DAY, DTV_Last_Active_Block_Dt, base_dt)
                        ,DTV_Last_Pending_Cancel        = DATEDIFF(DAY, DTV_Last_Pending_Cancel_Dt, base_dt)
                        ,BB_Last_Activation             = DATEDIFF(DAY, BB_Last_Activation_Dt, base_dt)
                        ,_1st_TA                        = DATEDIFF(DAY, _1st_TA_dt, base_dt)
                        ,last_TA                        = DATEDIFF(DAY, last_TA_dt, base_dt)
                        ,_1st_TA_save                   = DATEDIFF(DAY, _1st_TA_save_dt, base_dt)
                        ,last_TA_save                   = DATEDIFF(DAY, last_TA_save_dt, base_dt)
                        ,_1st_TA_nonsave                = DATEDIFF(DAY, _1st_TA_nonsave_dt, base_dt)
                        ,last_TA_nonsave                = DATEDIFF(DAY, last_TA_nonsave_dt, base_dt);

							
	 
	 
		MESSAGE CAST(now() as timestamp)||' | 21' TO CLIENT;
		GO
	
		
    --  2.6 NOW TV Variables

        /* Make sure mapped_account_numbers table is updated, check with Robert Barker from the NOW TV Analytics team*/

        ALTER TABLE CS_scoring_base_20171213 
        ADD         (accountid         BIGINT
                    ,NTV_Ents_Last_30D BIT DEFAULT 0
                    ,NTV_Ents_Last_90D BIT DEFAULT 0);


		MESSAGE CAST(now() as timestamp)||' | 22' TO CLIENT;
		GO
        UPDATE      CS_scoring_base_20171213
        SET         accountid = mapped.accountid
        FROM        CS_scoring_base_20171213 csr
        INNER JOIN  tva02.mapped_account_numbers mapped
        ON          csr.account_number = mapped.account_number;

        CREATE HG INDEX id_accid ON CS_scoring_base_20171213 (accountid);
			
		  
						 

        UPDATE      CS_scoring_base_20171213 csr
        SET         NTV_Ents_Last_30D = 1
        FROM        citeam.nowtv_accounts_ents ntvents
        WHERE       csr.accountid = ntvents.accountid
        AND         ntvents.subscriber_this_period = 1
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) >= -30
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) <= 0
        AND         ntvents.accountid IS NOT NULL;

        UPDATE      CS_scoring_base_20171213 csr
        SET         NTV_Ents_Last_90D = 1
        FROM        citeam.nowtv_accounts_ents ntvents
        WHERE       csr.accountid = ntvents.accountid
        AND         ntvents.subscriber_this_period = 1
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) >= -90
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) <= 0
        AND         ntvents.accountid IS NOT NULL;

		MESSAGE CAST(now() as timestamp)||' | 22' TO CLIENT;
		GO
		
    --  2.7 Rentals and BAK Variables
			
			ALTER TABLE CS_scoring_base_20171213
		ADD ( rentals_months_since_last INT DEFAULT NULL 
			, rents_6M INT DEFAULT NULL 
			, rents_9M INT DEFAULT NULL 
			, rents_12M INT DEFAULT NULL 
			, BAK_time_since_last INT DEFAULT NULL
			, BAK_6M INT DEFAULT NULL 
			, BAK_9M INT DEFAULT NULL 
			, BAK_12M INT DEFAULT NULL 
			) ;


			SELECT b.account_number
				, MAX(ordered_dt) AS max_dt
				, SUM(CASE WHEN DATEDIFF(MONTH, ordered_dt, base_dt) <=6 THEN 1 ELSE NULL END) AS rentals_6M 
				, SUM(CASE WHEN DATEDIFF(MONTH, ordered_dt, base_dt) <=9 THEN 1 ELSE NULL END) AS rentals_9M 
				, SUM(CASE WHEN DATEDIFF(MONTH, ordered_dt, base_dt) <=12 THEN 1 ELSE NULL END) AS rentals_12M 
				, base_dt
			INTO #last_rental
			FROM Decisioning.OTT_Purchases 	AS a 
			JOIN CS_scoring_base_20171213 					AS b  ON a.account_number = b.account_number AND a.ordered_dt <= base_dt
			WHERE movie_order = 1 
				AND movie_cancelled_order = 0 
			GROUP BY b.account_number, base_dt;
            
			COMMIT;
			CREATE HG INDEX id1 	ON #last_rental (account_number);
			CREATE DATE INDEX id2 	ON #last_rental (max_dt);
			COMMIT;

			UPDATE CS_scoring_base_20171213
			SET 
				rentals_months_since_last = DATEDIFF (MONTH, b.max_dt, b.base_dt)
				, a.rents_6M = b.rentals_6M
				, a.rents_9M = b.rentals_9M
				, a.rents_12M = b.rentals_12M
			FROM CS_scoring_base_20171213 AS a 
			JOIN #last_rental AS b ON a.account_number = b.account_number AND a.basE_dt = b.basE_dt;
			
			COMMIT;
			
			SELECT b.account_number
				, MAX(ordered_dt) AS max_dt
				, SUM(CASE WHEN DATEDIFF(MONTH, ordered_dt, base_dt) <=6 THEN 1 ELSE Null END) AS BAK_6M 
				, SUM(CASE WHEN DATEDIFF(MONTH, ordered_dt, base_dt) <=9 THEN 1 ELSE Null END) AS BAK_9M 
				, SUM(CASE WHEN DATEDIFF(MONTH, ordered_dt, base_dt) <=12 THEN 1 ELSE Null END) AS BAK_12M 
				, base_dt
			INTO #BAK
			FROM Decisioning.OTT_Purchases 	AS a 
			JOIN CS_scoring_base_20171213 					AS b  ON a.account_number = b.account_number AND a.ordered_dt <= base_dt
			WHERE product_type = 'EST'
			GROUP BY b.account_number, base_dt
            
			
			COMMIT
			CREATE HG INDEX id1 	ON #BAK (account_number)
			CREATE DATE INDEX id2 	ON #BAK (base_dt)
			CREATE DATE INDEX id3 	ON #BAK (max_dt)
			COMMIT
			
			UPDATE CS_scoring_base_20171213 a
			SET  a.BAK_time_since_last = DATEDIFF (MONTH, b.max_dt, b.base_dt)
				, a.BAK_6M = b.BAK_6M
				, a.BAK_9M = b.BAK_9M
				, a.BAK_12M = b.BAK_12M
			FROM CS_scoring_base_20171213 a
			LEFT JOIN #BAK AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		/*	
			SELECT 
			rents_6M, bak_6M, count(*) hits 
			FROM CS_scoring_base_20171213
			group by rents_6M, bak_6M			
			
			*/
			
			
		



			ALTER TABLE CS_scoring_base_20171213
			ADD ( HD_VAL	int	DEFAULT 6		,
					BB_VAL	int	DEFAULT 6		,
					BK_VAL	int	DEFAULT 6		,
					VOD_VAL	int	DEFAULT 6		,
					FIBRE_VAL	int	DEFAULT 6		,
					SKYGO_VAL	int	DEFAULT 6		,
					SERVICEAPP_VAL	int	DEFAULT 6		,
					CW_VAL	int	DEFAULT 6		,
					SKYBOX_VAL	int	DEFAULT 6		,
					THREED_VAL	int	DEFAULT 6		,
					MS_VAL	int	DEFAULT 6		,
					SHIELD_VAL	int	DEFAULT 6		,
					FIBRE_TOP_VAL	int	DEFAULT 6		,
					SKYKIDS_VAL	int	DEFAULT 6		,
					SKYQ_ORIGIN_VAL	int	DEFAULT 6		,
					SKYQ_2TB_VAL	int	DEFAULT 6		,
					SKYQAPP_VAL	int	DEFAULT 6		,
					MOB_PREREG_VAL	int	DEFAULT 6		,
					SKYQ_PREPREG_VAL	int	DEFAULT 6		,
					UHD_VAL	int	DEFAULT 6		,
					NOWTV_VAL	int	DEFAULT 6		)
				
			COMMIT 
			
			GO 
			
			UPDATE  CS_scoring_base_20171213
			SET a.HD_VAL = b.HD_VAL	,
					a.BB_VAL = b.BB_VAL	,
					a.BK_VAL = b.BK_VAL	,
					a.VOD_VAL = b.VOD_VAL	,
					a.FIBRE_VAL = b.FIBRE_VAL	,
					a.SKYGO_VAL = b.SKYGO_VAL	,
					a.SERVICEAPP_VAL = b.SERVICEAPP_VAL	,
					a.CW_VAL = b.CW_VAL	,
					a.SKYBOX_VAL = b.SKYBOX_VAL	,
					a.THREED_VAL = b.THREED_VAL	,
					a.MS_VAL = b.MS_VAL	,
					a.SHIELD_VAL = b.SHIELD_VAL	,
					a.FIBRE_TOP_VAL = b.FIBRE_TOP_VAL	,
					a.SKYKIDS_VAL = b.SKYKIDS_VAL	,
					a.SKYQ_ORIGIN_VAL = b.SKYQ_ORIGIN_VAL	,
					a.SKYQ_2TB_VAL = b.SKYQ_2TB_VAL	,
					a.SKYQAPP_VAL = b.SKYQAPP_VAL	,
					a.MOB_PREREG_VAL = b.MOB_PREREG_VAL	,
					a.SKYQ_PREPREG_VAL = b.SKYQ_PREPREG_VAL	,
					a.UHD_VAL = b.UHD_VAL	,
					a.NOWTV_VAL = b.NOWTV_VAL	
			FROM CS_scoring_base_20171213 as a 
			JOIN TECI_current_score as b on a.account_number = b.account_number 
-------------------------------------------
--  3.0 Create final tables
-------------------------------------------

    --  3.1 Create binning table
        
        DROP TABLE IF EXISTS CS_scoring_base_20171213_BINNED;
        SELECT       account_number
                    ,base_dt
                    ,rand_num
		---- flags 
					, UP_Rental
					,UP_buy_and_Keep	
					,UP_SkyQ			
					, UP_mobile
                    , UP_movies
                    , UP_sports
                    ,Rental_eligible    
					,Buy_and_keep_eligible 	
                    ,SkyQ_eligible
                    , mobile_eligible
                    , movies_eligible
                    , sports_eligible
					,Age
                    ,ADSL_Enabled
					,cb_address_town
                    ,Country
                    ,Country_Name
                    ,Prev_Offer_Description_DTV
                    ,Curr_Offer_Description_DTV
                    ,DTV_Product_Holding
                    ,DTV_Status_Code
                    ,Exchange_Status
                    ,financial_strategy
                    ,Government_Region
                    ,Home_Owner_Status
                    ,Local_Authority_Name
					,h_affluence
                    ,h_family_lifestage
                    ,h_household_composition
                    ,h_mosaic_group
                    ,h_presence_of_child_aged_0_4
                    ,h_presence_of_child_aged_12_17
                    ,h_presence_of_child_aged_5_11
                    ,h_property_type
                    ,h_residence_type
                    ,h_income_value
                    ,p_true_touch_group
                    ,skyfibre_enabled
                    ,BB_Active
                    ,Broadband_Average_Demand
                    ,DTV_Active
                    ,max_speed_uplift
                    ,Prev_Offer_Amount_DTV
                    ,skyfibre_enabled_perc
                    ,skyfibre_planned_perc
                    ,Superfast_Available_End_2013
                    ,Superfast_Available_End_2014
                    ,Superfast_Available_End_2015
                    ,Superfast_Available_End_2016
                    ,Superfast_Available_End_2017
                    ,Throughput_Speed
                    ,h_presence_of_young_person_at_address AS h_presence_of_young_person_at_ad

                    ,DTV_Last_cuscan_churn
                    ,DTV_Last_Activation
                    ,DTV_Curr_Contract_Intended_End
                    ,DTV_Curr_Contract_Start
                    ,DTV_Last_SysCan_Churn
                    ,Curr_Offer_Start_DTV
                    ,Curr_Offer_Actual_End_DTV
                    ,DTV_1st_Activation
          
                    ,DTV_Last_Active_Block
                    ,DTV_Last_Pending_Cancel
                    ,BB_Last_Activation

                    ,CASE WHEN DTV_CusCan_Churns_Ever   IS NULL  THEN  0 ELSE DTV_CusCan_Churns_Ever     END AS DTV_CusCan_Churns_Ever
                    ,CASE WHEN DTV_SysCan_Churns_Ever   IS NULL  THEN  0 ELSE DTV_SysCan_Churns_Ever     END AS DTV_SysCan_Churns_Ever
                    ,CASE WHEN DTV_Pending_cancels_ever IS NULL  THEN  0 ELSE DTV_Pending_cancels_ever   END AS DTV_Pending_cancels_ever

                    /* Bin DTV Product package */
                    ,CASE WHEN UPPER(DTV_Product_Holding) LIKE '%SKY Q%' THEN 'D.Sky Q' 
                          WHEN UPPER(DTV_Product_Holding) LIKE '%BOX SETS%' THEN 'A.Box Sets' 
                          WHEN UPPER(DTV_Product_Holding) LIKE '%VARIETY%' THEN 'B.Variety' 
                          WHEN UPPER(DTV_Product_Holding) LIKE '%ORIGINAL%' THEN 'C.Original' 
                        ELSE 'Other'
                        END AS Dtv_Package

                    ,DTV_product_holding_recode

                   /* Bin current offer length */
                   ,CASE WHEN   Curr_Offer_Length_DTV     = ''                OR   Curr_Offer_Length_DTV   IS NULL          THEN 'A.No offer'
                        WHEN   (Curr_Offer_Length_DTV     = '1M'              OR   Curr_Offer_Length_DTV   = '2M'           OR Curr_Offer_Length_DTV='3M'
                             OR Curr_Offer_Length_DTV     = '4M'              OR   Curr_Offer_Length_DTV   = '5M'           OR Curr_Offer_Length_DTV='6M'
                             OR Curr_Offer_Length_DTV     = '7M'              OR   Curr_Offer_Length_DTV   = '8M'           OR Curr_Offer_Length_DTV='9M'
                             OR Curr_Offer_Length_DTV     = '10M'             OR   Curr_Offer_Length_DTV   = '11M'          OR Curr_Offer_Length_DTV='12M') THEN 'B.Less than 12M'
                        ELSE 'C.Greater than 12M' END AS Curr_Offer_Length_DTV_b

                    /* Bin previous offer length */

                    ,CASE WHEN   Prev_Offer_Length_DTV     = ''               OR   Prev_Offer_Length_DTV   IS NULL          THEN 'A.No offer'
                         WHEN   (Prev_Offer_Length_DTV     = '0M'             OR   Prev_Offer_Length_DTV   = '1M'                 OR Prev_Offer_Length_DTV='2M'
                           OR    Prev_Offer_Length_DTV     = '3M'             OR   Prev_Offer_Length_DTV   = '4M'                 OR Prev_Offer_Length_DTV='5M' OR Prev_Offer_Length_DTV='6M'
                           OR    Prev_Offer_Length_DTV     = '7M'             OR   Prev_Offer_Length_DTV   = '8M'                 OR Prev_Offer_Length_DTV='9M') THEN 'B.Less than 9M'
                         ELSE 'C.Greater than 9M' END AS Prev_Offer_Length_DTV_b

                    /* Create flags whether they are currently/previously on offer or not */
                    ,CASE WHEN Curr_Offer_Amount_DTV  IS NULL  THEN 'A.Curr no offer' ELSE 'B.Curr on offer' END AS Curr_Offer_Amount_DTV_flag
                    ,CASE WHEN Prev_Offer_Amount_DTV  IS NULL  THEN 'A.Prev no offer' ELSE 'B.Prev on offer' END AS Prev_Offer_Amount_DTV_flag

                    ,CASE WHEN   h_number_of_bedrooms = 0                              THEN '0'
                          WHEN   h_number_of_bedrooms = 1                              THEN '1'
                          WHEN   h_number_of_bedrooms = 2                              THEN '2'
                          WHEN   h_number_of_bedrooms = 3                              THEN '3'
                          WHEN  (h_number_of_bedrooms = 4 OR h_number_of_bedrooms = 5) THEN '4-5'
                          WHEN   h_number_of_bedrooms IS NULL                          THEN 'Unknown' END AS h_number_of_bedrooms_b

                    ,CASE WHEN   h_number_of_children_in_household = 0 THEN '0'
                          WHEN   h_number_of_children_in_household = 1 THEN '1'
                          WHEN   h_number_of_children_in_household = 2 THEN '2'
                          WHEN   h_number_of_children_in_household = 3 THEN '3'
                          WHEN   h_number_of_children_in_household = 4 THEN '4'
                          WHEN   h_number_of_children_in_household IS NULL THEN 'Unknown' END AS h_number_of_children_in_house_b

                    ,CASE WHEN   h_number_of_adults = 0 THEN '0'
                          WHEN   h_number_of_adults = 1 THEN '1'
                          WHEN   h_number_of_adults = 2 THEN '2'
                          WHEN   h_number_of_adults = 3 THEN '3'
                          WHEN   h_number_of_adults = 4 THEN '4'
                          WHEN   h_number_of_adults = 5 THEN '5'
                          WHEN   h_number_of_adults >= 6 THEN '6+'
                          WHEN   h_number_of_adults IS NULL THEN 'Unknown' END AS h_number_of_adults_b

                    ,CAST(p_true_touch_type AS VARCHAR(2)) AS p_true_touch_type

                    ,CASE WHEN Curr_Offer_Amount_DTV  >=-20   THEN 'B.Less than 20'
                          WHEN (Curr_Offer_Amount_DTV <- 20 ) THEN 'C.Greater than 20'
                          WHEN Curr_Offer_Amount_DTV  IS NULL THEN 'A.No offer' END AS Curr_Offer_Amount_DTV_b
                    ,CASE WHEN Prev_Offer_Amount_DTV  >=-20   THEN 'B.Less than 20'
                          WHEN (Prev_Offer_Amount_DTV <- 20 ) THEN 'C.Greater than 20'
                          WHEN Prev_Offer_Amount_DTV  IS NULL THEN 'A.No offer' END AS Prev_Offer_Amount_DTV_b
					,CASE WHEN BB_Enter_3rd_Party_Ever  = 0 THEN '0'
                          WHEN BB_Enter_3rd_Party_Ever >= 1 THEN '1'
                          WHEN BB_Enter_3rd_Party_Ever >= 2 THEN '2'
                          WHEN BB_Enter_3rd_Party_Ever >= 3 THEN '3'  
                          WHEN BB_Enter_3rd_Party_Ever >= 4 THEN 'ge4' END AS BB_3rdParty_PL_Entry_Ever_b
                    ,CASE WHEN BB_Enter_3rd_Party_In_Last_180D  = 0 THEN '0'
                          WHEN BB_Enter_3rd_Party_In_Last_180D >= 1 THEN '1'
                          WHEN BB_Enter_3rd_Party_In_Last_180D >= 2 THEN '2'
                          WHEN BB_Enter_3rd_Party_In_Last_180D >= 3 THEN '3'
                          WHEN BB_Enter_3rd_Party_In_Last_180D >= 4 THEN 'ge4' END AS BB_3rdParty_PL_Entry_Last_180D_b
                    ,CASE WHEN BB_Enter_3rd_Party_In_Last_1Yr  = 0 THEN '0'
                          WHEN BB_Enter_3rd_Party_In_Last_1Yr >= 1 THEN '1'
                          WHEN BB_Enter_3rd_Party_In_Last_1Yr >= 2 THEN '2'
                          WHEN BB_Enter_3rd_Party_In_Last_1Yr >= 3 THEN '3'
                          WHEN BB_Enter_3rd_Party_In_Last_1Yr >= 4 THEN 'ge4' END AS BB_3rdParty_PL_Entry_Last_1Yr_b
                    ,CASE WHEN BB_Enter_3rd_Party_In_Last_30D  = 0 THEN '0'
                          WHEN BB_Enter_3rd_Party_In_Last_30D >= 1 THEN '1'
                          WHEN BB_Enter_3rd_Party_In_Last_30D >= 2 THEN '2'
                          WHEN BB_Enter_3rd_Party_In_Last_30D >= 3 THEN '3'
                          WHEN BB_Enter_3rd_Party_In_Last_30D >= 4 THEN 'ge4' END AS BB_3rdParty_PL_Entry_Last_30D_b
                    ,CASE WHEN BB_Enter_3rd_Party_In_Last_3Yr  = 0 THEN '0'
                          WHEN BB_Enter_3rd_Party_In_Last_3Yr >= 1 THEN '1'
                          WHEN BB_Enter_3rd_Party_In_Last_3Yr >= 2 THEN '2'
                          WHEN BB_Enter_3rd_Party_In_Last_3Yr >= 3 THEN '3'
                          WHEN BB_Enter_3rd_Party_In_Last_3Yr >= 4 THEN 'ge4' END AS BB_3rdParty_PL_Entry_Last_3Yr_b
                    ,CASE WHEN BB_Enter_3rd_Party_In_Last_5Yr  = 0 THEN '0'
                          WHEN BB_Enter_3rd_Party_In_Last_5Yr >= 1 THEN '1'
                          WHEN BB_Enter_3rd_Party_In_Last_5Yr >= 2 THEN '2'
                          WHEN BB_Enter_3rd_Party_In_Last_5Yr >= 3 THEN '3'
                          WHEN BB_Enter_3rd_Party_In_Last_5Yr >= 4 THEN 'ge4' END AS BB_3rdParty_PL_Entry_Last_5Yr_b
                    ,CASE WHEN BB_Enter_3rd_Party_In_Last_90D  = 0 THEN '0'
                          WHEN BB_Enter_3rd_Party_In_Last_90D >= 1 THEN '1'
                          WHEN BB_Enter_3rd_Party_In_Last_90D >= 2 THEN '2'
                          WHEN BB_Enter_3rd_Party_In_Last_90D >= 3 THEN '3'
                          WHEN BB_Enter_3rd_Party_In_Last_90D >= 4 THEN 'ge4' END AS BB_3rdParty_PL_Entry_Last_90D_b
                    ,CASE WHEN BB_Subscription_Churn_Ever  = 0 THEN '0'
                          WHEN BB_Subscription_Churn_Ever >= 1 THEN '1'
                          WHEN BB_Subscription_Churn_Ever >= 2 THEN '2'
                          WHEN BB_Subscription_Churn_Ever >= 3 THEN '3'
                          WHEN BB_Subscription_Churn_Ever >= 4 THEN 'ge4' END AS BB_Churns_Ever_b
				  ,CASE WHEN BB_Subscription_Churn_In_Last_180D  = 0 THEN '0'
                          WHEN BB_Subscription_Churn_In_Last_180D >= 1 THEN '1'
                          WHEN BB_Subscription_Churn_In_Last_180D >= 2 THEN '2'
                          WHEN BB_Subscription_Churn_In_Last_180D >= 3 THEN '3'
                          WHEN BB_Subscription_Churn_In_Last_180D >= 4 THEN 'ge4' END AS BB_Churns_Last_180D_b
                    ,CASE WHEN BB_Subscription_Churn_In_Last_1Yr  = 0 THEN '0'
                          WHEN BB_Subscription_Churn_In_Last_1Yr >= 1 THEN '1'
                          WHEN BB_Subscription_Churn_In_Last_1Yr >= 2 THEN '2'
                          WHEN BB_Subscription_Churn_In_Last_1Yr >= 3 THEN '3'
                          WHEN BB_Subscription_Churn_In_Last_1Yr >= 4 THEN 'ge4' END AS BB_Churns_Last_1Yr_b
                    ,CASE WHEN BB_Subscription_Churn_In_Last_30D  = 0 THEN '0'
                          WHEN BB_Subscription_Churn_In_Last_30D >= 1 THEN '1'
                          WHEN BB_Subscription_Churn_In_Last_30D >= 2 THEN '2'
                          WHEN BB_Subscription_Churn_In_Last_30D >= 3 THEN '3'
                          WHEN BB_Subscription_Churn_In_Last_30D >= 4 THEN 'ge4' END AS BB_Churns_Last_30D_b
                    ,CASE WHEN BB_Subscription_Churn_In_Last_3Yr  = 0 THEN '0'
                          WHEN BB_Subscription_Churn_In_Last_3Yr >= 1 THEN '1'
                          WHEN BB_Subscription_Churn_In_Last_3Yr >= 2 THEN '2'
                          WHEN BB_Subscription_Churn_In_Last_3Yr >= 3 THEN '3'
                          WHEN BB_Subscription_Churn_In_Last_3Yr >= 4 THEN 'ge4' END AS BB_Churns_Last_3Yr_b
                    ,CASE WHEN BB_Subscription_Churn_In_Last_5Yr  = 0 THEN '0'
                          WHEN BB_Subscription_Churn_In_Last_5Yr >= 1 THEN '1'
                          WHEN BB_Subscription_Churn_In_Last_5Yr >= 2 THEN '2'
                          WHEN BB_Subscription_Churn_In_Last_5Yr >= 3 THEN '3'
                          WHEN BB_Subscription_Churn_In_Last_5Yr >= 4 THEN 'ge4' END AS BB_Churns_Last_5Yr_b
                    ,CASE WHEN BB_Subscription_Churn_In_Last_90D  = 0 THEN '0'
                          WHEN BB_Subscription_Churn_In_Last_90D >= 1 THEN '1'
                          WHEN BB_Subscription_Churn_In_Last_90D >= 2 THEN '2'
                          WHEN BB_Subscription_Churn_In_Last_90D >= 3 THEN '3'
                          WHEN BB_Subscription_Churn_In_Last_90D >= 4 THEN 'ge4' END AS BB_Churns_Last_90D_b
 				  ,CASE WHEN BB_Enter_CusCan_Ever  = 0 THEN '0'
                          WHEN BB_Enter_CusCan_Ever >= 1 THEN '1'
                          WHEN BB_Enter_CusCan_Ever >= 2 THEN '2'
                          WHEN BB_Enter_CusCan_Ever >= 3 THEN '3'
                          WHEN BB_Enter_CusCan_Ever >= 4 THEN 'ge4' END AS BB_Enter_CusCan_Ever_b
                    ,CASE WHEN BB_Enter_CusCan_In_Last_180D  = 0 THEN '0'
                          WHEN BB_Enter_CusCan_In_Last_180D >= 1 THEN '1'
                          WHEN BB_Enter_CusCan_In_Last_180D >= 2 THEN '2'
                          WHEN BB_Enter_CusCan_In_Last_180D >= 3 THEN '3'
                          WHEN BB_Enter_CusCan_In_Last_180D >= 4 THEN 'ge4' END AS BB_CusCan_PL_Entry_Last_180D_b
                    ,CASE WHEN BB_Enter_CusCan_In_Last_1Yr  = 0 THEN '0'
                          WHEN BB_Enter_CusCan_In_Last_1Yr >= 1 THEN '1'
                          WHEN BB_Enter_CusCan_In_Last_1Yr >= 2 THEN '2'
                          WHEN BB_Enter_CusCan_In_Last_1Yr >= 3 THEN '3'
                          WHEN BB_Enter_CusCan_In_Last_1Yr >= 4 THEN 'ge4' END AS BB_CusCan_PL_Entry_Last_1Yr_b
                    ,CASE WHEN BB_Enter_CusCan_In_Last_30D  = 0 THEN '0'
                          WHEN BB_Enter_CusCan_In_Last_30D >= 1 THEN '1'
                          WHEN BB_Enter_CusCan_In_Last_30D >= 2 THEN '2'
                          WHEN BB_Enter_CusCan_In_Last_30D >= 3 THEN '3'
                          WHEN BB_Enter_CusCan_In_Last_30D >= 4 THEN 'ge4' END AS BB_CusCan_PL_Entry_Last_30D_b
                    ,CASE WHEN BB_Enter_CusCan_In_Last_3Yr  = 0 THEN '0'
                          WHEN BB_Enter_CusCan_In_Last_3Yr >= 1 THEN '1'
                          WHEN BB_Enter_CusCan_In_Last_3Yr >= 2 THEN '2'
                          WHEN BB_Enter_CusCan_In_Last_3Yr >= 3 THEN '3'
                          WHEN BB_Enter_CusCan_In_Last_3Yr >= 4 THEN 'ge4' END AS BB_CusCan_PL_Entry_Last_3Yr_b
                    ,CASE WHEN BB_Enter_CusCan_In_Last_5Yr  = 0 THEN '0'
                          WHEN BB_Enter_CusCan_In_Last_5Yr >= 1 THEN '1'
                          WHEN BB_Enter_CusCan_In_Last_5Yr >= 2 THEN '2'
                          WHEN BB_Enter_CusCan_In_Last_5Yr >= 3 THEN '3'
                          WHEN BB_Enter_CusCan_In_Last_5Yr >= 4 THEN 'ge4' END AS BB_CusCan_PL_Entry_Last_5Yr_b
                    ,CASE WHEN BB_Enter_CusCan_In_Last_90D  = 0 THEN '0'
                          WHEN BB_Enter_CusCan_In_Last_90D >= 1 THEN '1'
                          WHEN BB_Enter_CusCan_In_Last_90D >= 2 THEN '2'
                          WHEN BB_Enter_CusCan_In_Last_90D >= 3 THEN '3'
                          WHEN BB_Enter_CusCan_In_Last_90D >= 4 THEN 'ge4' END AS BB_CusCan_PL_Entry_Last_90D_b
					,CASE WHEN BB_Enter_HM_Ever  = 0 THEN '0'
                          WHEN BB_Enter_HM_Ever >= 1 THEN '1'
                          WHEN BB_Enter_HM_Ever >= 2 THEN '2'
                          WHEN BB_Enter_HM_Ever >= 3 THEN '3'
                          WHEN BB_Enter_HM_Ever >= 4 THEN 'ge4' END AS BB_HomeMove_PL_Entry_Ever_b
                    ,CASE WHEN BB_Enter_HM_In_Last_180D  = 0 THEN '0'
                          WHEN BB_Enter_HM_In_Last_180D >= 1 THEN '1'
                          WHEN BB_Enter_HM_In_Last_180D >= 2 THEN '2'
                          WHEN BB_Enter_HM_In_Last_180D >= 3 THEN '3'
                          WHEN BB_Enter_HM_In_Last_180D >= 4 THEN 'ge4' END AS BB_HomeMove_PL_Entry_In_Last_180D_b
                    ,CASE WHEN BB_Enter_HM_In_Last_1Yr  = 0 THEN '0'
                          WHEN BB_Enter_HM_In_Last_1Yr >= 1 THEN '1'
                          WHEN BB_Enter_HM_In_Last_1Yr >= 2 THEN '2'
                          WHEN BB_Enter_HM_In_Last_1Yr >= 3 THEN '3'
                          WHEN BB_Enter_HM_In_Last_1Yr >= 4 THEN 'ge4' END AS BB_HomeMove_PL_Entry_Last_1Yr_b
                    ,CASE WHEN BB_Enter_HM_In_Last_30D  = 0 THEN '0'
                          WHEN BB_Enter_HM_In_Last_30D >= 1 THEN '1'
                          WHEN BB_Enter_HM_In_Last_30D >= 2 THEN '2'
                          WHEN BB_Enter_HM_In_Last_30D >= 3 THEN '3'
                          WHEN BB_Enter_HM_In_Last_30D >= 4 THEN 'ge4' END AS BB_HomeMove_PL_Entry_Last_30D_b
                    ,CASE WHEN BB_Enter_HM_In_Last_3Yr  = 0 THEN '0'
                          WHEN BB_Enter_HM_In_Last_3Yr >= 1 THEN '1'
                          WHEN BB_Enter_HM_In_Last_3Yr >= 2 THEN '2'
                          WHEN BB_Enter_HM_In_Last_3Yr >= 3 THEN '3'
                          WHEN BB_Enter_HM_In_Last_3Yr >= 4 THEN 'ge4' END AS BB_HomeMove_PL_Entry_Last_3Yr_b
                    ,CASE WHEN BB_Enter_HM_In_Last_5Yr  = 0 THEN '0'
                          WHEN BB_Enter_HM_In_Last_5Yr >= 1 THEN '1'
                          WHEN BB_Enter_HM_In_Last_5Yr >= 2 THEN '2'
                          WHEN BB_Enter_HM_In_Last_5Yr >= 3 THEN '3'
                          WHEN BB_Enter_HM_In_Last_5Yr >= 4 THEN 'ge4' END AS BB_HomeMove_PL_Entry_Last_5Yr_b
                    ,CASE WHEN BB_Enter_HM_In_Last_90D  = 0 THEN '0'
                          WHEN BB_Enter_HM_In_Last_90D >= 1 THEN '1'
                          WHEN BB_Enter_HM_In_Last_90D >= 2 THEN '2'
                          WHEN BB_Enter_HM_In_Last_90D >= 3 THEN '3'
                          WHEN BB_Enter_HM_In_Last_90D >= 4 THEN 'ge4' END AS BB_HomeMove_PL_Entry_Last_90D_b
			   ,CASE WHEN BB_Enter_SysCan_Ever  = 0 THEN '0'
                          WHEN BB_Enter_SysCan_Ever >= 1 THEN '1'
                          WHEN BB_Enter_SysCan_Ever >= 2 THEN '2'
                          WHEN BB_Enter_SysCan_Ever >= 3 THEN '3'
                          WHEN BB_Enter_SysCan_Ever >= 4 THEN 'ge4' END AS BB_SysCan_PL_Entry_Ever_b
                    ,CASE WHEN BB_Enter_SysCan_In_Last_180D  = 0 THEN '0'
                          WHEN BB_Enter_SysCan_In_Last_180D >= 1 THEN '1'
                          WHEN BB_Enter_SysCan_In_Last_180D >= 2 THEN '2'
                          WHEN BB_Enter_SysCan_In_Last_180D >= 3 THEN '3'
                          WHEN BB_Enter_SysCan_In_Last_180D >= 4 THEN 'ge4' END AS BB_SysCan_PL_Entry_Last_180D_b
                    ,CASE WHEN BB_Enter_SysCan_In_Last_1Yr  = 0 THEN '0'
                          WHEN BB_Enter_SysCan_In_Last_1Yr >= 1 THEN '1'
                          WHEN BB_Enter_SysCan_In_Last_1Yr >= 2 THEN '2'
                          WHEN BB_Enter_SysCan_In_Last_1Yr >= 3 THEN '3'
                          WHEN BB_Enter_SysCan_In_Last_1Yr >= 4 THEN 'ge4' END AS BB_SysCan_PL_Entry_Last_1Yr_b
                    ,CASE WHEN BB_Enter_SysCan_In_Last_30D  = 0 THEN '0'
                          WHEN BB_Enter_SysCan_In_Last_30D >= 1 THEN '1'
                          WHEN BB_Enter_SysCan_In_Last_30D >= 2 THEN '2'
                          WHEN BB_Enter_SysCan_In_Last_30D >= 3 THEN '3'
                          WHEN BB_Enter_SysCan_In_Last_30D >= 4 THEN 'ge4' END AS BB_SysCan_PL_Entry_Last_30D_b
                    ,CASE WHEN BB_Enter_SysCan_In_Last_3Yr  = 0 THEN '0'
                          WHEN BB_Enter_SysCan_In_Last_3Yr >= 1 THEN '1'
                          WHEN BB_Enter_SysCan_In_Last_3Yr >= 2 THEN '2'
                          WHEN BB_Enter_SysCan_In_Last_3Yr >= 3 THEN '3'
                          WHEN BB_Enter_SysCan_In_Last_3Yr >= 4 THEN 'ge4' END AS BB_SysCan_PL_Entry_Last_3Yr_b
                    ,CASE WHEN BB_Enter_SysCan_In_Last_5Yr  = 0 THEN '0'
                          WHEN BB_Enter_SysCan_In_Last_5Yr >= 1 THEN '1'
                          WHEN BB_Enter_SysCan_In_Last_5Yr >= 2 THEN '2'
                          WHEN BB_Enter_SysCan_In_Last_5Yr >= 3 THEN '3'
                          WHEN BB_Enter_SysCan_In_Last_5Yr >= 4 THEN 'ge4' END AS BB_SysCan_PL_Entry_Last_5Yr_b
                    ,CASE WHEN BB_Enter_SysCan_In_Last_90D  = 0 THEN '0'
                          WHEN BB_Enter_SysCan_In_Last_90D >= 1 THEN '1'
                          WHEN BB_Enter_SysCan_In_Last_90D >= 2 THEN '2'
                          WHEN BB_Enter_SysCan_In_Last_90D >= 3 THEN '3'
                          WHEN BB_Enter_SysCan_In_Last_90D >= 4 THEN 'ge4' END AS BB_SysCan_PL_Entry_Last_90D_b
                    ,CASE WHEN DTV_Active_Blocks_Ever  = 0 THEN '0'
                          WHEN DTV_Active_Blocks_Ever >= 1 THEN '1'
                          WHEN DTV_Active_Blocks_Ever >= 2 THEN '2'
                          WHEN DTV_Active_Blocks_Ever >= 3 THEN '3'
                          WHEN DTV_Active_Blocks_Ever >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Ever_b
                    ,CASE WHEN DTV_Active_Blocks_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_Active_Blocks_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_Active_Blocks_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_Active_Blocks_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_Active_Blocks_In_Last_180D >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_180D_b
                    ,CASE WHEN DTV_Active_Blocks_In_Last_1Yr  = 0 THEN '0'
                          WHEN DTV_Active_Blocks_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_Active_Blocks_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_Active_Blocks_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_Active_Blocks_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_1Yr_b
                    ,CASE WHEN DTV_Active_Blocks_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_Active_Blocks_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_Active_Blocks_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_Active_Blocks_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_Active_Blocks_In_Last_30D >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_30D_b
                    ,CASE WHEN DTV_Active_Blocks_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_Active_Blocks_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_Active_Blocks_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_Active_Blocks_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_Active_Blocks_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_3Yr_b
                    ,CASE WHEN DTV_Active_Blocks_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_Active_Blocks_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_Active_Blocks_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_Active_Blocks_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_Active_Blocks_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_5Yr_b
                    ,CASE WHEN DTV_Active_Blocks_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_Active_Blocks_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_Active_Blocks_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_Active_Blocks_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_Active_Blocks_In_Last_90D >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_90D_b
                    ,CASE WHEN DTV_Churns_Ever  = 0 THEN '0'
                          WHEN DTV_Churns_Ever >= 1 THEN '1'
                          WHEN DTV_Churns_Ever >= 2 THEN '2'
                          WHEN DTV_Churns_Ever >= 3 THEN '3'
                          WHEN DTV_Churns_Ever >= 4 THEN 'ge4' END AS DTV_Churns_Ever_b
                    ,CASE WHEN DTV_Churns_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_Churns_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_Churns_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_Churns_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_Churns_In_Last_180D >= 4 THEN 'ge4' END AS DTV_Churns_Last_180D_b
                    ,CASE WHEN DTV_Churns_In_Last_1Yr  = 0 THEN '0'
                          WHEN DTV_Churns_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_Churns_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_Churns_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_Churns_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_Churns_Last_1Yr_b
                    ,CASE WHEN DTV_Churns_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_Churns_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_Churns_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_Churns_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_Churns_In_Last_30D >= 4 THEN 'ge4' END AS DTV_Churns_Last_30D_b
                    ,CASE WHEN DTV_Churns_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_Churns_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_Churns_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_Churns_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_Churns_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_Churns_Last_3Yr_b
                    ,CASE WHEN DTV_Churns_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_Churns_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_Churns_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_Churns_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_Churns_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_Churns_Last_5Yr_b
                    ,CASE WHEN DTV_Churns_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_Churns_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_Churns_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_Churns_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_Churns_In_Last_90D >= 4 THEN 'ge4' END AS DTV_Churns_Last_90D_b
                    ,CASE WHEN DTV_CusCan_Churns_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_CusCan_Churns_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_CusCan_Churns_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_CusCan_Churns_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_CusCan_Churns_In_Last_180D >= 4 THEN 'ge4' END AS DTV_CusCan_Churns_Last_180D_b
                    ,CASE WHEN DTV_CusCan_Churns_In_Last_1Yr  = 0 THEN '0'
                          WHEN DTV_CusCan_Churns_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_CusCan_Churns_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_CusCan_Churns_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_CusCan_Churns_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_CusCan_Churns_Last_1Yr_b
                    ,CASE WHEN DTV_CusCan_Churns_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_CusCan_Churns_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_CusCan_Churns_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_CusCan_Churns_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_CusCan_Churns_In_Last_30D >= 4 THEN 'ge4' END AS DTV_CusCan_Churns_Last_30D_b
                    ,CASE WHEN DTV_CusCan_Churns_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_CusCan_Churns_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_CusCan_Churns_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_CusCan_Churns_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_CusCan_Churns_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_CusCan_Churns_Last_3Yr_b
                    ,CASE WHEN DTV_CusCan_Churns_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_CusCan_Churns_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_CusCan_Churns_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_CusCan_Churns_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_CusCan_Churns_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_CusCan_Churns_Last_5Yr_b
                    ,CASE WHEN DTV_CusCan_Churns_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_CusCan_Churns_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_CusCan_Churns_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_CusCan_Churns_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_CusCan_Churns_In_Last_90D >= 4 THEN 'ge4' END AS DTV_CusCan_Churns_Last_90D_b
                    ,CASE WHEN DTV_Pending_Cancels_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_Pending_Cancels_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_Pending_Cancels_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_Pending_Cancels_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_Pending_Cancels_In_Last_180D >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_180D_b
                    ,CASE WHEN DTV_Pending_Cancels_In_Last_1Yr  = 0 THEN '0'
                          WHEN DTV_Pending_Cancels_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_Pending_Cancels_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_Pending_Cancels_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_Pending_Cancels_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_1Yr_b
                    ,CASE WHEN DTV_Pending_Cancels_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_Pending_Cancels_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_Pending_Cancels_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_Pending_Cancels_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_Pending_Cancels_In_Last_30D >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_30D_b
                    ,CASE WHEN DTV_Pending_Cancels_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_Pending_Cancels_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_Pending_Cancels_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_Pending_Cancels_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_Pending_Cancels_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_3Yr_b
                    ,CASE WHEN DTV_Pending_Cancels_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_Pending_Cancels_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_Pending_Cancels_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_Pending_Cancels_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_Pending_Cancels_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_5Yr_b
                    ,CASE WHEN DTV_Pending_Cancels_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_Pending_Cancels_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_Pending_Cancels_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_Pending_Cancels_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_Pending_Cancels_In_Last_90D >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_90D_b
                    ,CASE WHEN DTV_PO_Cancellations_Ever  = 0 THEN '0'
                          WHEN DTV_PO_Cancellations_Ever >= 1 THEN '1'
                          WHEN DTV_PO_Cancellations_Ever >= 2 THEN '2'
                          WHEN DTV_PO_Cancellations_Ever >= 3 THEN '3'
                          WHEN DTV_PO_Cancellations_Ever >= 4 THEN 'ge4' END AS DTV_PO_Cancellations_Ever_b
                    ,CASE WHEN DTV_PO_Cancellations_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_PO_Cancellations_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_PO_Cancellations_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_PO_Cancellations_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_PO_Cancellations_In_Last_180D >= 4 THEN 'ge4' END AS DTV_PO_Cancellations_Last_180D_b
                    ,CASE WHEN DTV_PO_Cancellations_In_Last_1Yr  = 0 THEN '0'
                          WHEN DTV_PO_Cancellations_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_PO_Cancellations_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_PO_Cancellations_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_PO_Cancellations_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_PO_Cancellations_Last_1Yr_b
                    ,CASE WHEN DTV_PO_Cancellations_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_PO_Cancellations_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_PO_Cancellations_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_PO_Cancellations_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_PO_Cancellations_In_Last_30D >= 4 THEN 'ge4' END AS DTV_PO_Cancellations_Last_30D_b
                    ,CASE WHEN DTV_PO_Cancellations_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_PO_Cancellations_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_PO_Cancellations_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_PO_Cancellations_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_PO_Cancellations_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_PO_Cancellations_Last_3Yr_b
                    ,CASE WHEN DTV_PO_Cancellations_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_PO_Cancellations_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_PO_Cancellations_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_PO_Cancellations_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_PO_Cancellations_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_PO_Cancellations_Last_5Yr_b
                    ,CASE WHEN DTV_PO_Cancellations_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_PO_Cancellations_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_PO_Cancellations_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_PO_Cancellations_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_PO_Cancellations_In_Last_90D >= 4 THEN 'ge4' END AS DTV_PO_Cancellations_Last_90D_b
                    ,CASE WHEN DTV_SameDayCancels_Ever  = 0 THEN '0'
                          WHEN DTV_SameDayCancels_Ever >= 1 THEN '1'
                          WHEN DTV_SameDayCancels_Ever >= 2 THEN '2'
                          WHEN DTV_SameDayCancels_Ever >= 3 THEN '3'
                          WHEN DTV_SameDayCancels_Ever >= 4 THEN 'ge4' END AS DTV_SameDayCancels_Ever_b
                    ,CASE WHEN DTV_SameDayCancels_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_SameDayCancels_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_SameDayCancels_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_SameDayCancels_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_SameDayCancels_In_Last_180D >= 4 THEN 'ge4' END AS DTV_SameDayCancels_Last_180D_b
                    ,CASE WHEN DTV_SameDayCancels_In_Last_1Yr  = 0 THEN '0'
                          WHEN DTV_SameDayCancels_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_SameDayCancels_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_SameDayCancels_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_SameDayCancels_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_SameDayCancels_Last_1Yr_b
                    ,CASE WHEN DTV_SameDayCancels_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_SameDayCancels_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_SameDayCancels_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_SameDayCancels_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_SameDayCancels_In_Last_30D >= 4 THEN 'ge4' END AS DTV_SameDayCancels_Last_30D_b
                    ,CASE WHEN DTV_SameDayCancels_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_SameDayCancels_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_SameDayCancels_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_SameDayCancels_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_SameDayCancels_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_SameDayCancels_Last_3Yr_b
                    ,CASE WHEN DTV_SameDayCancels_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_SameDayCancels_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_SameDayCancels_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_SameDayCancels_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_SameDayCancels_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_SameDayCancels_Last_5Yr_b
                    ,CASE WHEN DTV_SameDayCancels_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_SameDayCancels_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_SameDayCancels_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_SameDayCancels_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_SameDayCancels_In_Last_90D >= 4 THEN 'ge4' END AS DTV_SameDayCancels_Last_90D_b
                    ,CASE WHEN DTV_SysCan_Churns_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_SysCan_Churns_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_SysCan_Churns_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_SysCan_Churns_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_SysCan_Churns_In_Last_180D >= 4 THEN 'ge4' END AS DTV_SysCan_Churns_Last_180D_b
                    ,CASE WHEN DTV_SysCan_Churns_In_Last_1Yr  = 0 THEN '0'
                          WHEN DTV_SysCan_Churns_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_SysCan_Churns_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_SysCan_Churns_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_SysCan_Churns_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_SysCan_Churns_Last_1Yr_b
                    ,CASE WHEN DTV_SysCan_Churns_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_SysCan_Churns_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_SysCan_Churns_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_SysCan_Churns_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_SysCan_Churns_In_Last_30D >= 4 THEN 'ge4' END AS DTV_SysCan_Churns_Last_30D_b
                    ,CASE WHEN DTV_SysCan_Churns_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_SysCan_Churns_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_SysCan_Churns_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_SysCan_Churns_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_SysCan_Churns_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_SysCan_Churns_Last_3Yr_b
                    ,CASE WHEN DTV_SysCan_Churns_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_SysCan_Churns_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_SysCan_Churns_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_SysCan_Churns_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_SysCan_Churns_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_SysCan_Churns_Last_5Yr_b
                    ,CASE WHEN DTV_SysCan_Churns_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_SysCan_Churns_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_SysCan_Churns_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_SysCan_Churns_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_SysCan_Churns_In_Last_90D >= 4 THEN 'ge4' END AS DTV_SysCan_Churns_In_Last_90D_b

                    ,CASE WHEN Offers_Applied_Lst_12M_DTV  = 0 THEN '0'
                          WHEN Offers_Applied_Lst_12M_DTV >= 1 THEN '1'
                          WHEN Offers_Applied_Lst_12M_DTV >= 2 THEN '2'
                          WHEN Offers_Applied_Lst_12M_DTV >= 3 THEN '3'
                          WHEN Offers_Applied_Lst_12M_DTV >= 4 THEN 'ge4' END AS Offers_Applied_Lst_12M_DTV_b
                    
			  ,CASE WHEN Offers_Applied_Lst_7D_DTV  = 0 THEN '0'
                          WHEN Offers_Applied_Lst_7D_DTV >= 1 THEN '1'
                          WHEN Offers_Applied_Lst_7D_DTV >= 2 THEN '2'
                          WHEN Offers_Applied_Lst_7D_DTV >= 3 THEN '3'
                          WHEN Offers_Applied_Lst_7D_DTV >= 4 THEN 'ge4' END AS Offers_Applied_Lst_7D_DTV_b
                    ,CASE WHEN Offers_Applied_Lst_24M_DTV  = 0 THEN '0'
                          WHEN Offers_Applied_Lst_24M_DTV >= 1 THEN '1'
                          WHEN Offers_Applied_Lst_24M_DTV >= 2 THEN '2'
                          WHEN Offers_Applied_Lst_24M_DTV >= 3 THEN '3'
                          WHEN Offers_Applied_Lst_24M_DTV >= 4 THEN 'ge4' END AS Offers_Applied_Lst_24M_DTV_b
                    ,CASE WHEN Offers_Applied_Lst_30D_DTV  = 0 THEN '0'
                          WHEN Offers_Applied_Lst_30D_DTV >= 1 THEN '1'
                          WHEN Offers_Applied_Lst_30D_DTV >= 2 THEN '2'
                          WHEN Offers_Applied_Lst_30D_DTV >= 3 THEN '3'
                          WHEN Offers_Applied_Lst_30D_DTV >= 4 THEN 'ge4' END AS Offers_Applied_Lst_30D_DTV_b
                    ,CASE WHEN Offers_Applied_Lst_36M_DTV  = 0 THEN '0'
                          WHEN Offers_Applied_Lst_36M_DTV >= 1 THEN '1'
                          WHEN Offers_Applied_Lst_36M_DTV >= 2 THEN '2'
                          WHEN Offers_Applied_Lst_36M_DTV >= 3 THEN '3'
                          WHEN Offers_Applied_Lst_36M_DTV >= 4 THEN 'ge4' END AS Offers_Applied_Lst_36M_DTV_b
                    ,CASE WHEN Offers_Applied_Lst_90D_DTV  = 0 THEN '0'
                          WHEN Offers_Applied_Lst_90D_DTV >= 1 THEN '1'
                          WHEN Offers_Applied_Lst_90D_DTV >= 2 THEN '2'
                          WHEN Offers_Applied_Lst_90D_DTV >= 3 THEN '3'
                          WHEN Offers_Applied_Lst_90D_DTV >= 4 THEN 'ge4' END AS Offers_Applied_Lst_90D_DTV_b

						  
					

                    ,CASE WHEN dtv_last_activation <= 90       THEN 'A.<3 Months'
                          WHEN dtv_last_activation <= 180      THEN 'B.<6 Months'
                          WHEN dtv_last_activation <= 365      THEN 'C.<1 Year'
                          WHEN dtv_last_activation <= 730      THEN 'D.1-2 Years'
                          WHEN dtv_last_activation <= 1460     THEN 'E.3-4 Years'
                          WHEN dtv_last_activation <= 2190     THEN 'F.5-6 Years'
                          WHEN dtv_last_activation <= 3650     THEN 'G.7-10 Years'
                          WHEN dtv_last_activation >  3650     THEN 'H.11+ Years'
                                ELSE 'I.Other' END             AS dtv_last_tenure
                    ,CASE WHEN dtv_1st_activation  <= 90       THEN 'A.<3 Months'
                          WHEN dtv_1st_activation  <= 180      THEN 'B.<6 Months'
                          WHEN dtv_1st_activation  <= 365      THEN 'C.<1 Year'
                          WHEN dtv_1st_activation  <= 730      THEN 'D.1-2 Years'
                          WHEN dtv_1st_activation  <= 1460     THEN 'E.3-4 Years'
                          WHEN dtv_1st_activation  <= 2190     THEN 'F.5-6 Years'
                          WHEN dtv_1st_activation  <= 3650     THEN 'G.7-10 Years'
                          WHEN dtv_1st_activation  >  3650     THEN 'H.11+ Years'
                                ELSE 'I.Other' END             AS dtv_1st_tenure
                    ,CASE WHEN bb_last_activation  <= 90       THEN 'A.<3 Months'
                          WHEN bb_last_activation  <= 180      THEN 'B.<6 Months'
                          WHEN bb_last_activation  <= 365      THEN 'C.<1 Year'
                          WHEN bb_last_activation  <= 730      THEN 'D.1-2 Years'
                          WHEN bb_last_activation  <= 1460     THEN 'E.3-4 Years'
                          WHEN bb_last_activation  <= 2190     THEN 'F.5-6 Years'
                          WHEN bb_last_activation  <= 3650     THEN 'G.7-10 Years'
                          WHEN bb_last_activation  >  3650     THEN 'H.11+ Years'
                                ELSE 'I.Other' END             AS bb_last_tenure

					, MS_Active
					, SGE_Active
					, SGE_Product_Holding

                
                    ,CASE WHEN TAs_in_last_24hrs  = 0 THEN '0'
                          WHEN TAs_in_last_24hrs >= 1 THEN '1'
                          WHEN TAs_in_last_24hrs >= 2 THEN '2'
                          WHEN TAs_in_last_24hrs >= 3 THEN '3'
                          WHEN TAs_in_last_24hrs >= 4 THEN 'ge4' END AS TAs_in_last_24hrs_b
                    ,CASE WHEN TAs_in_last_7d  = 0 THEN '0'
                          WHEN TAs_in_last_7d >= 1 THEN '1'
                          WHEN TAs_in_last_7d >= 2 THEN '2'
                          WHEN TAs_in_last_7d >= 3 THEN '3'
                          WHEN TAs_in_last_7d >= 4 THEN 'ge4' END AS TAs_in_last_7d_b
                    ,CASE WHEN TAs_in_last_14d  = 0 THEN '0'
                          WHEN TAs_in_last_14d >= 1 THEN '1'
                          WHEN TAs_in_last_14d >= 2 THEN '2'
                          WHEN TAs_in_last_14d >= 3 THEN '3'
                          WHEN TAs_in_last_14d >= 4 THEN 'ge4' END AS TAs_in_last_14d_b
                    ,CASE WHEN TAs_in_last_30d  = 0 THEN '0'
                          WHEN TAs_in_last_30d >= 1 THEN '1'
                          WHEN TAs_in_last_30d >= 2 THEN '2'
                          WHEN TAs_in_last_30d >= 3 THEN '3'
                          WHEN TAs_in_last_30d >= 4 THEN 'ge4' END AS TAs_in_last_30d_b
                    ,CASE WHEN TAs_in_last_60d  = 0 THEN '0'
                          WHEN TAs_in_last_60d >= 1 THEN '1'
                          WHEN TAs_in_last_60d >= 2 THEN '2'
                          WHEN TAs_in_last_60d >= 3 THEN '3'
                          WHEN TAs_in_last_60d >= 4 THEN 'ge4' END AS TAs_in_last_60d_b
                    ,CASE WHEN TAs_in_last_90d  = 0 THEN '0'
                          WHEN TAs_in_last_90d >= 1 THEN '1'
                          WHEN TAs_in_last_90d >= 2 THEN '2'
                          WHEN TAs_in_last_90d >= 3 THEN '3'
                          WHEN TAs_in_last_90d >= 4 THEN 'ge4' END AS TAs_in_last_90d_b
                    ,CASE WHEN TAs_in_last_12m  = 0 THEN '0'
                          WHEN TAs_in_last_12m >= 1 THEN '1'
                          WHEN TAs_in_last_12m >= 2 THEN '2'
                          WHEN TAs_in_last_12m >= 3 THEN '3'
                          WHEN TAs_in_last_12m >= 4 THEN 'ge4' END AS TAs_in_last_12m_b
                    ,CASE WHEN TAs_in_last_24m  = 0 THEN '0'
                          WHEN TAs_in_last_24m >= 1 THEN '1'
                          WHEN TAs_in_last_24m >= 2 THEN '2'
                          WHEN TAs_in_last_24m >= 3 THEN '3'
                          WHEN TAs_in_last_24m >= 4 THEN 'ge4' END AS TAs_in_last_24m_b
                    ,CASE WHEN TAs_in_last_36m  = 0 THEN '0'
                          WHEN TAs_in_last_36m >= 1 THEN '1'
                          WHEN TAs_in_last_36m >= 2 THEN '2'
                          WHEN TAs_in_last_36m >= 3 THEN '3'
                          WHEN TAs_in_last_36m >= 4 THEN 'ge4' END AS TAs_in_last_36m_b
                    ,CASE WHEN TA_saves_in_last_24hrs  = 0 THEN '0'
                          WHEN TA_saves_in_last_24hrs >= 1 THEN '1'
                          WHEN TA_saves_in_last_24hrs >= 2 THEN '2'
                          WHEN TA_saves_in_last_24hrs >= 3 THEN '3'
                          WHEN TA_saves_in_last_24hrs >= 4 THEN 'ge4' END AS TA_saves_in_last_24hrs_b
                    ,CASE WHEN TA_saves_in_last_7d  = 0 THEN '0'
                          WHEN TA_saves_in_last_7d >= 1 THEN '1'
                          WHEN TA_saves_in_last_7d >= 2 THEN '2'
                          WHEN TA_saves_in_last_7d >= 3 THEN '3'
                          WHEN TA_saves_in_last_7d >= 4 THEN 'ge4' END AS TA_saves_in_last_7d_b
                    ,CASE WHEN TA_saves_in_last_14d  = 0 THEN '0'
                          WHEN TA_saves_in_last_14d >= 1 THEN '1'
                          WHEN TA_saves_in_last_14d >= 2 THEN '2'
                          WHEN TA_saves_in_last_14d >= 3 THEN '3'
                          WHEN TA_saves_in_last_14d >= 4 THEN 'ge4' END AS TA_saves_in_last_14d_b
                    ,CASE WHEN TA_saves_in_last_30d  = 0 THEN '0'
                          WHEN TA_saves_in_last_30d >= 1 THEN '1'
                          WHEN TA_saves_in_last_30d >= 2 THEN '2'
                          WHEN TA_saves_in_last_30d >= 3 THEN '3'
                          WHEN TA_saves_in_last_30d >= 4 THEN 'ge4' END AS TA_saves_in_last_30d_b
                    ,CASE WHEN TA_saves_in_last_60d  = 0 THEN '0'
                          WHEN TA_saves_in_last_60d >= 1 THEN '1'
                          WHEN TA_saves_in_last_60d >= 2 THEN '2'
                          WHEN TA_saves_in_last_60d >= 3 THEN '3'
                          WHEN TA_saves_in_last_60d >= 4 THEN 'ge4' END AS TA_saves_in_last_60d_b
                    ,CASE WHEN TA_saves_in_last_90d  = 0 THEN '0'
                          WHEN TA_saves_in_last_90d >= 1 THEN '1'
                          WHEN TA_saves_in_last_90d >= 2 THEN '2'
                          WHEN TA_saves_in_last_90d >= 3 THEN '3'
                          WHEN TA_saves_in_last_90d >= 4 THEN 'ge4' END AS TA_saves_in_last_90d_b
                    ,CASE WHEN TA_saves_in_last_12m  = 0 THEN '0'
                          WHEN TA_saves_in_last_12m >= 1 THEN '1'
                          WHEN TA_saves_in_last_12m >= 2 THEN '2'
                          WHEN TA_saves_in_last_12m >= 3 THEN '3'
                          WHEN TA_saves_in_last_12m >= 4 THEN 'ge4' END AS TA_saves_in_last_12m_b
                    ,CASE WHEN TA_saves_in_last_24m  = 0 THEN '0'
                          WHEN TA_saves_in_last_24m >= 1 THEN '1'
                          WHEN TA_saves_in_last_24m >= 2 THEN '2'
                          WHEN TA_saves_in_last_24m >= 3 THEN '3'
                          WHEN TA_saves_in_last_24m >= 4 THEN 'ge4' END AS TA_saves_in_last_24m_b
                    ,CASE WHEN TA_saves_in_last_36m  = 0 THEN '0'
                          WHEN TA_saves_in_last_36m >= 1 THEN '1'
                          WHEN TA_saves_in_last_36m >= 2 THEN '2'
                          WHEN TA_saves_in_last_36m >= 3 THEN '3'
                          WHEN TA_saves_in_last_36m >= 4 THEN 'ge4' END AS TA_saves_in_last_36m_b
                    ,CASE WHEN TA_nonsaves_in_last_24hrs  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_24hrs >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_24hrs >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_24hrs >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_24hrs >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_24hrs_b
                    ,CASE WHEN TA_nonsaves_in_last_7d  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_7d >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_7d >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_7d >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_7d >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_7d_b
                    ,CASE WHEN TA_nonsaves_in_last_14d  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_14d >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_14d >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_14d >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_14d >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_14d_b
                    ,CASE WHEN TA_nonsaves_in_last_30d  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_30d >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_30d >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_30d >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_30d >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_30d_b
                    ,CASE WHEN TA_nonsaves_in_last_60d  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_60d >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_60d >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_60d >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_60d >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_60d_b
                    ,CASE WHEN TA_nonsaves_in_last_90d  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_90d >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_90d >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_90d >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_90d >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_90d_b
                    ,CASE WHEN TA_nonsaves_in_last_12m  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_12m >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_12m >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_12m >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_12m >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_12m_b
                    ,CASE WHEN TA_nonsaves_in_last_24m  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_24m >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_24m >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_24m >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_24m >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_24m_b
                    ,CASE WHEN TA_nonsaves_in_last_36m  = 0 THEN '0'
                          WHEN TA_nonsaves_in_last_36m >= 1 THEN '1'
                          WHEN TA_nonsaves_in_last_36m >= 2 THEN '2'
                          WHEN TA_nonsaves_in_last_36m >= 3 THEN '3'
                          WHEN TA_nonsaves_in_last_36m >= 4 THEN 'ge4' END AS TA_nonsaves_in_last_36m_b

                    ,CASE WHEN _1st_TA  <= 30       THEN 'A.<1 Month'
                          WHEN _1st_TA  <= 90       THEN 'B.<3 Months'
                          WHEN _1st_TA  <= 180      THEN 'C.<6 Months'
                          WHEN _1st_TA  <= 365      THEN 'D.<1 Year'
                          WHEN _1st_TA  <= 730      THEN 'E.1-2 Years'
                          WHEN _1st_TA  <= 1460     THEN 'F.3-4 Years'
                          WHEN _1st_TA  <= 2190     THEN 'G.5-6 Years'
                          WHEN _1st_TA  <= 3650     THEN 'H.7-10 Years'
                          WHEN _1st_TA  >  3650     THEN 'I.11+ Years'
                                ELSE 'J.Other' END             AS _1st_TA_b
                    ,CASE WHEN last_TA  <= 30       THEN 'A.<1 Month'
                          WHEN last_TA  <= 90       THEN 'B.<3 Months'
                          WHEN last_TA  <= 180      THEN 'C.<6 Months'
                          WHEN last_TA  <= 365      THEN 'D.<1 Year'
                          WHEN last_TA  <= 730      THEN 'E.1-2 Years'
                          WHEN last_TA  <= 1460     THEN 'F.3-4 Years'
                          WHEN last_TA  <= 2190     THEN 'G.5-6 Years'
                          WHEN last_TA  <= 3650     THEN 'H.7-10 Years'
                          WHEN last_TA  >  3650     THEN 'I.11+ Years'
                                ELSE 'J.Other' END             AS last_TA_b
                    ,CASE WHEN _1st_TA_save  <= 30       THEN 'A.<1 Month'
                          WHEN _1st_TA_save  <= 90       THEN 'B.<3 Months'
                          WHEN _1st_TA_save  <= 180      THEN 'C.<6 Months'
                          WHEN _1st_TA_save  <= 365      THEN 'D.<1 Year'
                          WHEN _1st_TA_save  <= 730      THEN 'E.1-2 Years'
                          WHEN _1st_TA_save  <= 1460     THEN 'F.3-4 Years'
                          WHEN _1st_TA_save  <= 2190     THEN 'G.5-6 Years'
                          WHEN _1st_TA_save  <= 3650     THEN 'H.7-10 Years'
                          WHEN _1st_TA_save  >  3650     THEN 'I.11+ Years'
                                ELSE 'J.Other' END             AS _1st_TA_save_b
                    ,CASE WHEN last_TA_save  <= 30       THEN 'A.<1 Month'
                          WHEN last_TA_save  <= 90       THEN 'B.<3 Months'
                          WHEN last_TA_save  <= 180      THEN 'C.<6 Months'
                          WHEN last_TA_save  <= 365      THEN 'D.<1 Year'
                          WHEN last_TA_save  <= 730      THEN 'E.1-2 Years'
                          WHEN last_TA_save  <= 1460     THEN 'F.3-4 Years'
                          WHEN last_TA_save  <= 2190     THEN 'G.5-6 Years'
                          WHEN last_TA_save  <= 3650     THEN 'H.7-10 Years'
                          WHEN last_TA_save  >  3650     THEN 'I.11+ Years'
                                ELSE 'J.Other' END             AS last_TA_save_b
                    ,CASE WHEN _1st_TA_nonsave  <= 30       THEN 'A.<1 Month'
                          WHEN _1st_TA_nonsave  <= 90       THEN 'B.<3 Months'
                          WHEN _1st_TA_nonsave  <= 180      THEN 'C.<6 Months'
                          WHEN _1st_TA_nonsave  <= 365      THEN 'D.<1 Year'
                          WHEN _1st_TA_nonsave  <= 730      THEN 'E.1-2 Years'
                          WHEN _1st_TA_nonsave  <= 1460     THEN 'F.3-4 Years'
                          WHEN _1st_TA_nonsave  <= 2190     THEN 'G.5-6 Years'
                          WHEN _1st_TA_nonsave  <= 3650     THEN 'H.7-10 Years'
                          WHEN _1st_TA_nonsave  >  3650     THEN 'I.11+ Years'
                                ELSE 'J.Other' END             AS _1st_TA_nonsave_b
                    ,CASE WHEN last_TA_nonsave  <= 30       THEN 'A.<1 Month'
                          WHEN last_TA_nonsave  <= 90       THEN 'B.<3 Months'
                          WHEN last_TA_nonsave  <= 180      THEN 'C.<6 Months'
                          WHEN last_TA_nonsave  <= 365      THEN 'D.<1 Year'
                          WHEN last_TA_nonsave  <= 730      THEN 'E.1-2 Years'
                          WHEN last_TA_nonsave  <= 1460     THEN 'F.3-4 Years'
                          WHEN last_TA_nonsave  <= 2190     THEN 'G.5-6 Years'
                          WHEN last_TA_nonsave  <= 3650     THEN 'H.7-10 Years'
                          WHEN last_TA_nonsave  >  3650     THEN 'I.11+ Years'
                                ELSE 'J.Other' END             AS last_TA_nonsave_b
                    ,_1st_TA_reason
                    ,_1st_TA_reason_flag
                    ,_1st_TA_outcome
                    ,last_TA_reason
                    ,last_TA_reason_flag
                    ,last_TA_outcome

                    ,accountid
                    ,NTV_Ents_Last_30D
                    ,NTV_Ents_Last_90D

					, num_sports_events     
					
					--		TECI BASE VARIABLES 
					, HD_VAL
					, BB_VAL
					, BK_VAL
					, VOD_VAL
					, FIBRE_VAL
					, SKYGO_VAL
					, SERVICEAPP_VAL
					, CW_VAL
					, SKYBOX_VAL
					, THREED_VAL
					, MS_VAL
					, SHIELD_VAL
					, FIBRE_TOP_VAL
					, SKYKIDS_VAL
					, SKYQ_ORIGIN_VAL
					, SKYQ_2TB_VAL
					, SKYQAPP_VAL
					, MOB_PREREG_VAL
					, SKYQ_PREPREG_VAL
					, UHD_VAL
					, NOWTV_VAL

                    ,BB_Provider
					--- OD 
					, OD_Last_3M
					, OD_Last_12M
					, OD_Months_since_Last
					, BAK_6M 
					, BAK_9M 
					, BAK_12M 
					, BAK_time_since_last
					, rents_6M
					, rents_9M
					, rents_12M
					, rentals_months_since_last
					
					, rentals_used_before
					, bak_used_before
					--- Prem orders flag
					, Order_Sports_Added_In_Next_30d
					, Order_Sports_Added_In_Last_30d
					, Order_Sports_Removed_In_Next_30d
					, Order_Sports_Removed_In_Last_30d
					, Order_Movies_Added_In_Next_30d
					, Order_Movies_Added_In_Last_30d
					, Order_Movies_Removed_In_Next_30d
					, Order_Movies_Removed_In_Last_30d
					, Movies_Active
					, Movies_Product_Holding
					, Sports_Active
					, Sports_Product_Holding
					, sports_downgrade_date 
					, Sports_Tenure         
					, movies_downgrade_date 
					, Movies_Tenure     

					, base_dt_2
					, TA_next_30d
	
	
        INTO        CS_scoring_base_20171213_BINNED
        FROM        CS_scoring_base_20171213
        WHERE       country = 'UK'
        --AND         base_dt BETWEEN @start_date AND @end_date_lag; -- Because we don't know upsell for last month

        CREATE HG INDEX id1 ON CS_scoring_base_20171213_BINNED (account_number);
        CREATE DATE INDEX iddt ON CS_scoring_base_20171213_BINNED (base_dt);
        
		MESSAGE CAST(now() as timestamp)||' | 23' TO CLIENT;
		GO
    --  3.2 Create base table

        DROP TABLE  IF EXISTS CS_scoring_base_20171213_BASE;
        SELECT      
                     account_number
                    ,base_dt
                    ,rand_num
					
					,UP_Rental          
					,UP_buy_and_Keep	
					,UP_SkyQ			
					,Rental_eligible    
					,Buy_and_keep_eligible 	
					,SkyQ_eligible
					, Up_mobile 		
					, Mobile_eligible 	
                    ,Age
                    ,ADSL_Enabled
                    ,BB_RTM
                    ,Exchange_Status
                    ,p_true_touch_type
                    ,Simple_Segment
                    ,BB_Product_Holding
                    ,BB_Status_Code
                    ,cb_address_town
                    ,Country
                    ,Country_Name
                    ,Curr_Offer_Description_DTV
                    ,Curr_Offer_Length_DTV
                    ,DTV_Product_Holding
                    ,DTV_product_holding_recode
                    ,DTV_Status_Code
                    ,financial_strategy
                    ,Government_Region
                    ,h_family_lifestage
                    ,h_household_composition
                    ,h_mosaic_group
                    ,h_presence_of_child_aged_0_4
                    ,h_presence_of_child_aged_12_17
                    ,h_presence_of_child_aged_5_11
                    ,h_presence_of_young_person_at_address AS h_presence_of_young_person_at_ad
                    ,h_property_type
                    ,h_residence_type
                    ,Home_Owner_Status
                    ,Local_Authority_Name
                    ,p_true_touch_group
                    ,Prev_Offer_Description_DTV
                    ,Prev_Offer_Length_DTV
                    ,skyfibre_enabled
                    ,BB_Enter_3rd_Party_Ever
                    ,BB_Enter_3rd_Party_In_Last_180D     AS BB_3rdParty_PL_Entry_Last_180D
                    ,BB_Enter_3rd_Party_In_Last_1Yr      AS BB_3rdParty_PL_Entry_Last_1Yr
                    ,BB_Enter_3rd_Party_In_Last_30D      AS BB_3rdParty_PL_Entry_Last_30D
                    ,BB_Enter_3rd_Party_In_Last_3Yr      AS BB_3rdParty_PL_Entry_Last_3Yr
                    ,BB_Enter_3rd_Party_In_Last_5Yr      AS BB_3rdParty_PL_Entry_Last_5Yr
                    ,BB_Enter_3rd_Party_In_Last_90D      AS BB_3rdParty_PL_Entry_Last_90D
                    ,BB_Active
                    ,BB_Subscription_Churn_Ever
                    ,BB_Subscription_Churn_In_Last_180D                AS BB_Churns_Last_180D
                    ,BB_Subscription_Churn_In_Last_1Yr                 AS BB_Churns_Last_1Yr
                    ,BB_Subscription_Churn_In_Last_30D                 AS BB_Churns_Last_30D
                    ,BB_Subscription_Churn_In_Last_3Yr                 AS BB_Churns_Last_3Yr
                    ,BB_Subscription_Churn_In_Last_5Yr                 AS BB_Churns_Last_5Yr
                    ,BB_Subscription_Churn_In_Last_90D                 AS BB_Churns_Last_90D
                    ,BB_Enter_CusCan_Ever
                    ,BB_Enter_CusCan_In_Last_180D       AS BB_CusCan_PL_Entry_Last_180D
                    ,BB_Enter_CusCan_In_Last_1Yr        AS BB_CusCan_PL_Entry_Last_1Yr
                    ,BB_Enter_CusCan_In_Last_30D        AS BB_CusCan_PL_Entry_Last_30D
                    ,BB_Enter_CusCan_In_Last_3Yr        AS BB_CusCan_PL_Entry_Last_3Yr
                    ,BB_Enter_CusCan_In_Last_5Yr        AS BB_CusCan_PL_Entry_Last_5Yr
                    ,BB_Enter_CusCan_In_Last_90D        AS BB_CusCan_PL_Entry_Last_90D
                    ,BB_Enter_HM_Ever
                    ,BB_Enter_HM_In_Last_180D     AS BB_HomeMove_PL_Entry_Last_180D
                    ,BB_Enter_HM_In_Last_1Yr      AS BB_HomeMove_PL_Entry_Last_1Yr
                    ,BB_Enter_HM_In_Last_30D      AS BB_HomeMove_PL_Entry_Last_30D
                    ,BB_Enter_HM_In_Last_3Yr      AS BB_HomeMove_PL_Entry_Last_3Yr
                    ,BB_Enter_HM_In_Last_5Yr      AS BB_HomeMove_PL_Entry_Last_5Yr
                    ,BB_Enter_HM_In_Last_90D      AS BB_HomeMove_PL_Entry_Last_90D
                    ,BB_Enter_SysCan_Ever
                    ,BB_Enter_SysCan_In_Last_180D       AS BB_SysCan_PL_Entry_Last_180D
                    ,BB_Enter_SysCan_In_Last_1Yr        AS BB_SysCan_PL_Entry_Last_1Yr
                    ,BB_Enter_SysCan_In_Last_30D        AS BB_SysCan_PL_Entry_Last_30D
                    ,BB_Enter_SysCan_In_Last_3Yr        AS BB_SysCan_PL_Entry_Last_3Yr
                    ,BB_Enter_SysCan_In_Last_5Yr        AS BB_SysCan_PL_Entry_Last_5Yr
                    ,BB_Enter_SysCan_In_Last_90D        AS BB_SysCan_PL_Entry_Last_90D
                    ,Broadband_Average_Demand
                    /* ,BT_Consumer_Market_Share */
                    ,Curr_Offer_Amount_DTV
                    ,Curr_Offer_ID_DTV
                    ,DTV_CusCan_Churns_Ever
                    ,DTV_Pending_cancels_ever
                    ,DTV_SysCan_Churns_Ever
                    ,DTV_Active
                    ,DTV_Active_Blocks_Ever
                    ,DTV_Active_Blocks_In_Last_180D        AS DTV_Active_Blocks_Last_180D
                    ,DTV_Active_Blocks_In_Last_1Yr         AS DTV_Active_Blocks_Last_1Yr
                    ,DTV_Active_Blocks_In_Last_30D         AS DTV_Active_Blocks_Last_30D
                    ,DTV_Active_Blocks_In_Last_3Yr         AS DTV_Active_Blocks_Last_3Yr
                    ,DTV_Active_Blocks_In_Last_5Yr         AS DTV_Active_Blocks_Last_5Yr
                    ,DTV_Active_Blocks_In_Last_90D         AS DTV_Active_Blocks_Last_90D
                    ,DTV_Churns_Ever
                    ,DTV_Churns_In_Last_180D               AS DTV_Churns_Last_180D
                    ,DTV_Churns_In_Last_1Yr                AS DTV_Churns_Last_1Yr
                    ,DTV_Churns_In_Last_30D                AS DTV_Churns_Last_30D
                    ,DTV_Churns_In_Last_3Yr                AS DTV_Churns_Last_3Yr
                    ,DTV_Churns_In_Last_5Yr                AS DTV_Churns_Last_5Yr
                    ,DTV_Churns_In_Last_90D                AS DTV_Churns_Last_90D
                    ,DTV_CusCan_Churns_In_Last_180D        AS DTV_CusCan_Churns_Last_180D
                    ,DTV_CusCan_Churns_In_Last_1Yr         AS DTV_CusCan_Churns_Last_1Yr
                    ,DTV_CusCan_Churns_In_Last_30D         AS DTV_CusCan_Churns_Last_30D
                    ,DTV_CusCan_Churns_In_Last_3Yr         AS DTV_CusCan_Churns_Last_3Yr
                    ,DTV_CusCan_Churns_In_Last_5Yr         AS DTV_CusCan_Churns_Last_5Yr
                    ,DTV_CusCan_Churns_In_Last_90D         AS DTV_CusCan_Churns_Last_90D
                    ,DTV_Pending_Cancels_In_Last_180D      AS DTV_Pending_Cancels_Last_180D
                    ,DTV_Pending_Cancels_In_Last_1Yr       AS DTV_Pending_Cancels_Last_1Yr
                    ,DTV_Pending_Cancels_In_Last_30D       AS DTV_Pending_Cancels_Last_30D
                    ,DTV_Pending_Cancels_In_Last_3Yr       AS DTV_Pending_Cancels_Last_3Yr
                    ,DTV_Pending_Cancels_In_Last_5Yr       AS DTV_Pending_Cancels_Last_5Yr
                    ,DTV_Pending_Cancels_In_Last_90D       AS DTV_Pending_Cancels_Last_90D
                    ,DTV_PO_Cancellations_Ever
                    ,DTV_PO_Cancellations_In_Last_180D     AS DTV_PO_Cancellations_Last_180D
                    ,DTV_PO_Cancellations_In_Last_1Yr      AS DTV_PO_Cancellations_Last_1Yr
                    ,DTV_PO_Cancellations_In_Last_30D      AS DTV_PO_Cancellations_Last_30D
                    ,DTV_PO_Cancellations_In_Last_3Yr      AS DTV_PO_Cancellations_Last_3Yr
                    ,DTV_PO_Cancellations_In_Last_5Yr      AS DTV_PO_Cancellations_Last_5Yr
                    ,DTV_PO_Cancellations_In_Last_90D      AS DTV_PO_Cancellations_Last_90D
                    ,DTV_SameDayCancels_Ever
                    ,DTV_SameDayCancels_In_Last_180D       AS DTV_SameDayCancels_Last_180D
                    ,DTV_SameDayCancels_In_Last_1Yr        AS DTV_SameDayCancels_Last_1Yr
                    ,DTV_SameDayCancels_In_Last_30D        AS DTV_SameDayCancels_Last_30D
                    ,DTV_SameDayCancels_In_Last_3Yr        AS DTV_SameDayCancels_Last_3Yr
                    ,DTV_SameDayCancels_In_Last_5Yr        AS DTV_SameDayCancels_Last_5Yr
                    ,DTV_SameDayCancels_In_Last_90D        AS DTV_SameDayCancels_Last_90D
                    ,DTV_SysCan_Churns_In_Last_180D        AS DTV_SysCan_Churns_Last_180D
                    ,DTV_SysCan_Churns_In_Last_1Yr         AS DTV_SysCan_Churns_Last_1Yr
                    ,DTV_SysCan_Churns_In_Last_30D         AS DTV_SysCan_Churns_Last_30D
                    ,DTV_SysCan_Churns_In_Last_3Yr         AS DTV_SysCan_Churns_Last_3Yr
                    ,DTV_SysCan_Churns_In_Last_5Yr         AS DTV_SysCan_Churns_Last_5Yr
                    ,DTV_SysCan_Churns_In_Last_90D         AS DTV_SysCan_Churns_Last_90D
                    ,h_income_value
                    ,h_number_of_adults
                    ,h_number_of_bedrooms
                    ,h_number_of_children_in_household
                    ,max_speed_uplift
                    ,Offers_Applied_Lst_12M_DTV
                    ,Offers_Applied_Lst_24Hrs_DTV
                    ,Offers_Applied_Lst_24M_DTV
                    ,Offers_Applied_Lst_30D_DTV
                    ,Offers_Applied_Lst_36M_DTV
                    ,Offers_Applied_Lst_90D_DTV
                    ,Prev_Offer_Amount_DTV
                    ,Prev_Offer_ID_DTV
                    /* ,Sky_Consumer_Market_Share */
                    ,skyfibre_enabled_perc
                    ,skyfibre_planned_perc
                    ,Superfast_Available_End_2013
                    ,Superfast_Available_End_2014
                    ,Superfast_Available_End_2015
                    ,Superfast_Available_End_2016
                    ,Superfast_Available_End_2017
                    /* ,TalkTalk_Consumer_Market_Share */
                    ,Throughput_Speed
                    /* ,Virgin_Consumer_Market_Share */
                    ,DTV_Last_cuscan_churn
                    ,DTV_Last_Activation
                    ,DTV_Curr_Contract_Intended_End
                    ,DTV_Curr_Contract_Start
                    ,DTV_Last_SysCan_Churn
                    ,Curr_Offer_Start_DTV
                    ,Curr_Offer_Actual_End_DTV
                    ,DTV_1st_Activation
                    ,BB_Curr_Contract_Intended_End
                    ,BB_Curr_Contract_Start
                    ,DTV_Last_Active_Block
                    ,DTV_Last_Pending_Cancel
                    ,BB_Last_Activation
                    
                    ,_1st_TA
                    ,_1st_TA_reason
                    ,_1st_TA_reason_flag
                    ,_1st_TA_outcome
                    ,last_TA
                    ,last_TA_reason
                    ,last_TA_reason_flag
                    ,last_TA_outcome
                    ,_1st_TA_save
                    ,last_TA_save
                    ,_1st_TA_nonsave
                    ,last_TA_nonsave
                    ,TAs_in_last_24hrs
                    ,TAs_in_last_7d
                    ,TAs_in_last_14d
                    ,TAs_in_last_30d
                    ,TAs_in_last_60d
                    ,TAs_in_last_90d
                    ,TAs_in_last_12m
                    ,TAs_in_last_24m
                    ,TAs_in_last_36m
                    ,TA_saves_in_last_24hrs
                    ,TA_saves_in_last_7d
                    ,TA_saves_in_last_14d
                    ,TA_saves_in_last_30d
                    ,TA_saves_in_last_60d
                    ,TA_saves_in_last_90d
                    ,TA_saves_in_last_12m
                    ,TA_saves_in_last_24m
                    ,TA_saves_in_last_36m
                    ,TA_nonsaves_in_last_24hrs
                    ,TA_nonsaves_in_last_7d
                    ,TA_nonsaves_in_last_14d
                    ,TA_nonsaves_in_last_30d
                    ,TA_nonsaves_in_last_60d
                    ,TA_nonsaves_in_last_90d
                    ,TA_nonsaves_in_last_12m
                    ,TA_nonsaves_in_last_24m
                    ,TA_nonsaves_in_last_36m

                    ,accountid
                    ,NTV_Ents_Last_30D
                    ,NTV_Ents_Last_90D

                    ,num_sports_events
                    ,sports_downgrade_date
                    ,Sports_Tenure
                    ,movies_downgrade_date
                    ,Movies_Tenure

                    ,BB_Provider
					, OD_Last_3M
					, OD_Last_12M
					, OD_Months_since_Last
                    
        INTO        cs_base2
        FROM        cs_raw_consolidated 
        WHERE       country = 'UK'
        --AND         base_dt BETWEEN @start_date AND @end_date_lag; -- Because we don't know upsell for last month

        CREATE HG INDEX id1 ON cs_base2 (account_number);
        CREATE DATE INDEX iddt ON cs_base2 (base_dt);

	        
		MESSAGE CAST(now() as timestamp)||' | 24' TO CLIENT;
		GO	
		
    --  3.3 Create final base tables for each target
    
        --  3.3.1 Rentals

					
					
            CREATE OR REPLACE VIEW cs_rentals_base AS 
            SELECT      *
            FROM        cs_base2
            WHERE       Rental_eligible = 1 AND base_dt <='2017-08-01';


            CREATE OR REPLACE VIEW cs_rentals_base_smpl AS 
            SELECT      *
            INTO        cs_rentals_base_smpl
            FROM        cs_rentals_base
            WHERE       (UP_Rental = 1
            OR          (UP_Rental = 0 AND rand_num <= 0.07))
			AND base_dt <='2017-08-01';

            CREATE OR REPLACE VIEW cs_rentals_base_rsmpl AS 
                        SELECT      *
            FROM        cs_rentals_base
            WHERE       rand_num <= 0.01
			AND base_dt <='2017-08-01';

        --  3.3.2 Buy_and_keep
		
			
					
		
		
									
	 
								  
								  
								  
								  
		   
		   
				  
		 
						 
				 
			 
						 
					   
   
								   
	 
		   
		
				  
  
			 
            DROP TABLE  IF EXISTS cs_bak_base;
            CREATE OR REPLACE VIEW cs_bak_base as 
            SELECT      *
            FROM        cs_base2
            WHERE       Buy_and_keep_eligible = 1 AND base_dt <='2017-08-01';

            CREATE OR REPLACE VIEW	cs_bak_base_smpl as 
            SELECT      *
            FROM        cs_bak_base
            WHERE       (UP_buy_and_Keep = 1
            OR          (UP_buy_and_Keep = 0 AND rand_num <= 0.05));

            CREATE OR REPLACE VIEW	cs_bak_base_rsmpl as 
            SELECT      *
            FROM        cs_bak_base
            WHERE       rand_num <= 0.01;
 
        --  3.3.3 Sky Q 
					
            
            
				 
	 
  
			  
	
			
	   
	 
		 
				
  
				
	  
					   
			
	 
		
				   
			
		 
			   
			
		  
				  
  
			 
  
			
			CREATE OR REPLACE VIEW	cs_Q_base as 
            SELECT      *
            FROM        cs_base2
            WHERE       SkyQ_eligible = 1;

            CREATE OR REPLACE VIEW	cs_Q_base_rsmpl as 
            SELECT      *
            FROM        cs_Q_base
            WHERE       rand_num <= 0.01;

            CREATE OR REPLACE VIEW	cs_Q_base_smpl as 
            SELECT      *
            FROM        cs_Q_base
            WHERE       (UP_SkyQ = 1
            OR          (UP_SkyQ = 0 AND rand_num <= 0.006));

    
							 
	
        --  3.3.3 Sky Mobile
					
			CREATE OR REPLACE VIEW	cs_mobile_base as 
            SELECT      *
            FROM        cs_base2
            WHERE       mobile_eligible = 1;

            CREATE OR REPLACE VIEW	cs_mobile_base_rsmpl as 
            SELECT      *
            FROM        cs_mobile_base
            WHERE       rand_num <= 0.01;

            CREATE OR REPLACE VIEW	cs_mobile_base_smpl as 
            SELECT      *
            FROM        cs_mobile_base
            WHERE       (UP_mobile = 1
            OR          (UP_mobile = 0 AND rand_num <= 0.004));

    
    --  3.4 Create final binned tables for each target
    
        --  3.3.1 Rentals
					
            CREATE OR REPLACE VIEW cs_rentals_binned AS 
            SELECT      *
            FROM        CS_scoring_base_20171213_BINNED
            WHERE       Rental_eligible = 1 AND base_dt <='2017-08-01';

            CREATE OR REPLACE VIEW cs_rentals_binned_smpl AS 
            SELECT      *
            INTO        cs_rentals_binned_smpl
            FROM        cs_rentals_binned
            WHERE       (UP_Rental = 1
            OR          (UP_Rental = 0 AND rand_num <= 0.07))
			AND base_dt <='2017-08-01';

            CREATE OR REPLACE VIEW cs_rentals_binned_rsmpl AS 
                        SELECT      *
            FROM        cs_rentals_binned
            WHERE       rand_num <= 0.01
			AND base_dt <='2017-08-01';

        --  3.3.2 Buy_and_keep
		
            
            CREATE OR REPLACE VIEW cs_bak_binned as 
            SELECT      *
            FROM        CS_scoring_base_20171213_BINNED
            WHERE       Buy_and_keep_eligible = 1 AND base_dt <='2017-08-01';

            CREATE OR REPLACE VIEW	cs_bak_binned_smpl as 
            SELECT      *
            FROM        cs_bak_binned
            WHERE       (UP_buy_and_Keep = 1
            OR          (UP_buy_and_Keep = 0 AND rand_num <= 0.05));

            CREATE OR REPLACE VIEW	cs_bak_binned_rsmpl as 
            SELECT      *
            FROM        cs_bak_binned
            WHERE       rand_num <= 0.01;
 
        --  3.3.3 Sky Q 
            
			CREATE OR REPLACE VIEW	cs_Q_binned as 
            SELECT      *
            FROM        CS_scoring_base_20171213_BINNED
            WHERE       SkyQ_eligible = 1;

            CREATE OR REPLACE VIEW	cs_Q_binned_rsmpl as 
            SELECT      *
            FROM        cs_Q_binned
            WHERE       rand_num <= 0.01;

            CREATE OR REPLACE VIEW	cs_Q_binned_smpl as 
            SELECT      *
            FROM        cs_Q_binned
            WHERE       (UP_SkyQ = 1
            OR          (UP_SkyQ = 0 AND rand_num <= 0.002));
    
        --  3.3.3 Sky Mobile
					
			CREATE OR REPLACE VIEW	cs_mobile_binned as 
            SELECT      *
            FROM        CS_scoring_base_20171213_BINNED
            WHERE       mobile_eligible = 1;

            CREATE OR REPLACE VIEW	cs_mobile_binned_rsmpl as 
            SELECT      *
            FROM        cs_mobile_binned
            WHERE       rand_num <= 0.01;

            CREATE OR REPLACE VIEW	cs_mobile_binned_smpl as 
            SELECT      *
            FROM        cs_mobile_binned
            WHERE       (UP_mobile = 1
            OR          (UP_mobile = 0 AND rand_num <= 0.004));

			
    --  3.5 Permissions to public
        
        GRANT SELECT ON CS_scoring_base_20171213 TO PUBLIC;
        GRANT SELECT ON CS_scoring_base_20171213_BINNED TO PUBLIC;
        GRANT SELECT ON cs_base2 TO PUBLIC;
        GRANT SELECT ON cs_mobile_binned TO PUBLIC;
        GRANT SELECT ON cs_mobile_binned_smpl TO PUBLIC;
        GRANT SELECT ON cs_mobile_binned_rsmpl TO PUBLIC;
        GRANT SELECT ON cs_Q_binned_smpl TO PUBLIC;
        GRANT SELECT ON cs_Q_binned TO PUBLIC;
        GRANT SELECT ON cs_Q_binned_rsmpl TO PUBLIC;
        GRANT SELECT ON cs_Q_base TO PUBLIC;
        GRANT SELECT ON cs_Q_base_smpl TO PUBLIC;
        GRANT SELECT ON cs_Q_base_rsmpl TO PUBLIC;
        GRANT SELECT ON cs_mobile_base TO PUBLIC;
        GRANT SELECT ON cs_mobile_base_smpl TO PUBLIC;
        GRANT SELECT ON cs_mobile_base_rsmpl TO PUBLIC;
		
        GRANT SELECT ON cs_rentals_binned TO PUBLIC;
        GRANT SELECT ON cs_rentals_binned_smpl TO PUBLIC;
        GRANT SELECT ON cs_rentals_binned_rsmpl TO PUBLIC;
        GRANT SELECT ON cs_rentals_base TO PUBLIC;
        GRANT SELECT ON cs_rentals_base_smpl TO PUBLIC;
        GRANT SELECT ON cs_rentals_base_rsmpl TO PUBLIC;
		
        GRANT SELECT ON cs_bak_base TO PUBLIC;
        GRANT SELECT ON cs_bak_base_smpl TO PUBLIC;
        GRANT SELECT ON cs_bak_base_rsmpl TO PUBLIC;
        GRANT SELECT ON cs_bak_binned TO PUBLIC;
        GRANT SELECT ON cs_bak_binned_smpl TO PUBLIC;
        GRANT SELECT ON cs_bak_binned_rsmpl TO PUBLIC;
		
CREATE VIEW cs_mobile_binned_smpl_REDUCED AS 	
	SELECT
			account_number
,	UP_movies
,	Age
,	BB_Product_Holding
,	DTV_Product_Holding
,	Exchange_Status
,	financial_strategy
,	Home_Owner_Status
,	h_affluence
,	h_family_lifestage
,	h_household_composition
,	h_mosaic_group
,	h_presence_of_child_aged
,	h_presence_of_child_aged
,	h_presence_of_child_aged
,	h_property_type
,	h_residence_type
,	h_income_value
,	p_true_touch_group
,	skyfibre_enabled
,	BB_Active
,	Broadband_Average_Demand
,	DTV_Active
,	Prev_Offer_Amount_DTV
,	Superfast_Available_End_
,	h_presence_of_young_pers
,	DTV_Last_cuscan_churn
,	DTV_Last_Activation
,	DTV_Curr_Contract_Intend
,	DTV_Last_SysCan_Churn
,	Curr_Offer_Start_DTV
,	Curr_Offer_Actual_End_DT
,	DTV_1st_Activation
,	DTV_Last_Active_Block
,	DTV_Last_Pending_Cancel
,	BB_Last_Activation
,	DTV_CusCan_Churns_Ever
,	Dtv_Package
,	DTV_product_holding_reco
,	Curr_Offer_Length_DTV_b
,	Prev_Offer_Length_DTV_b
,	Curr_Offer_Amount_DTV_fl
,	Prev_Offer_Amount_DTV_fl
,	h_number_of_bedrooms_b
,	h_number_of_children_in_
,	h_number_of_adults_b
,	p_true_touch_type
,	Curr_Offer_Amount_DTV_b
,	Prev_Offer_Amount_DTV_b
,	BB_3rdParty_PL_Entry_Eve
,	BB_Churns_Last_1Yr_b
,	BB_Churns_Last_3Yr_b
,	BB_Enter_CusCan_Ever_
,	BB_HomeMove_PL_Entry_Las
,	BB_SysCan_PL_Entry_Last_
,	DTV_Active_Blocks_Ever_b
,	DTV_Active_Blocks_Last_1
,	DTV_Churns_Ever_b
,	DTV_CusCan_Churns_Last_1
,	DTV_Pending_Cancels_Last
,	DTV_PO_Cancellations_Las
,	Offers_Applied_Lst_12M_D
,	Offers_Applied_Lst_24M_D
,	Offers_Applied_Lst_90D_D
,	dtv_last_tenure
,	dtv_1st_tenure
,	bb_last_tenure
,	MS_Last_Activation
,	SGE_Last_Activation
,	DTV_contract_segment
,	BB_contract_segment
,	TAs_in_last_60d_b
,	TA_saves_in_last_90d_b
,	TA_saves_in_last_12m_b
,	TA_saves_in_last_36m_b
,	TA_nonsaves_in_last_24m_
,	last_TA_b
,	_1st_TA_save_b
,	last_TA_save_b
,	last_TA_nonsave_b
,	last_TA_reason
,	last_TA_reason_flag
,	last_TA_outcome
,	num_sports_events
,	sports_downgrade_date
,	Sports_Tenure
,	movies_downgrade_date
,	Movies_Tenure
,	Movies_Product_Holding
,	HD_VAL
,	BB_VAL
,	BK_VAL
,	VOD_VAL
,	FIBRE_VAL
,	SKYGO_VAL
,	SERVICEAPP_VAL
,	CW_VAL
,	THREED_VAL
,	MS_VAL
,	SHIELD_VAL
,	SKYQ_2TB_VAL
,	BB_Provider
,	OD_Last_3M
,	OD_Months_since_Last
,	BAK_6M
,	BAK_time_since_last
,	rents_12M
,	rentals_months_since_las
,	Sports_Product_Holding
,	Sports_Product_Count
,	MS_Active
,	SGE_Active
,	SGE_Product_Holding
FROM cs_mobile_binned_smpl
		
		
		
		
		