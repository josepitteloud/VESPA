/*------ Contents ------
--  0.0 Configuration
--  1.0 Create CS_Raw_test dataset
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
--  1.0 Create CS_Raw_test dataset
-------------------------------------------

    --  1.1 Create initial table
*/

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


        DROP TABLE IF EXISTS CS_Raw_test;
        SELECT Cast(wk.calendar_date AS date) Base_Dt,account_number
        INTO CS_Raw_test
        FROM #Qtr_Wk_End_Dts wk
             INNER JOIN
             cust_subs_hist asr
             ON wk.calendar_date BETWEEN effective_from_dt AND effective_to_dt - 1
                AND subscription_sub_type = 'DTV Primary Viewing'
                AND status_code IN ('AB','AC','PC')
        GROUP BY Base_Dt,account_number
        UNION
        SELECT Cast(wk.calendar_date AS date) Base_Dt,account_number
        FROM #Qtr_Wk_End_Dts wk
             INNER JOIN
             Decisioning.Active_Subscriber_Report asr
             ON wk.calendar_date BETWEEN effective_from_dt AND effective_to_dt - 1
                AND subscription_sub_type = 'Broadband'
        GROUP BY Base_Dt,account_number;
        COMMIT;
				  
					  
		MESSAGE CAST(now() as timestamp)||' | 2' TO CLIENT;

        CREATE HG INDEX id1 ON CS_Raw_test (account_number);
        CREATE DATE INDEX iddt ON CS_Raw_test (base_dt);

        CALL Decisioning_procs.Add_Subs_Calendar_Fields('CS_Raw_test','Base_Dt');
		
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw_test','Base_Dt','DTV');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw_test','Base_Dt','BB');
		
		Call Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw_test','Base_Dt','Sports');
		Call Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw_test','Base_Dt','Movies');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw_test','Base_Dt','MULTISCREEN');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw_test','Base_Dt','SGE');
		MESSAGE CAST(now() as timestamp)||' | 2.1' TO CLIENT;

		CALL Decisioning_Procs.Add_Activations_DTV('CS_Raw_test','Base_Dt');
        CALL Decisioning_Procs.Add_Activation_BB('CS_Raw_test','Base_Dt');
		CALL Decisioning_Procs.Add_Activations_Prems('CS_Raw_test','Base_Dt','Sports');
		CALL Decisioning_Procs.Add_Activations_Prems('CS_Raw_test','Base_Dt','Movies');
		MESSAGE CAST(now() as timestamp)||' | 2.2' TO CLIENT;
		
		CALL Decisioning_Procs.Add_Churn_DTV ('CS_Raw_test','Base_Dt');
		CALL Decisioning_Procs.Add_Churn_BB ('CS_Raw_test','Base_Dt');
		CALL Decisioning_Procs.Add_Churn_Prems		('CS_raw_test','Base_Dt','Sports');
		CALL Decisioning_Procs.Add_Churn_Prems		('CS_raw_test','Base_Dt','Movies');
		CALL Decisioning_Procs.Add_PL_Entries_DTV('CS_Raw_test','Base_Dt');
		CALL Decisioning_Procs.Add_PL_Entries_BB('CS_Raw_test','Base_Dt');
		MESSAGE CAST(now() as timestamp)||' | 2.3' TO CLIENT;
		
		CALL Decisioning_procs.Add_Contract_Details('CS_Raw_test','Base_Dt','DTV');
        CALL Decisioning_procs.Add_Contract_Details('CS_Raw_test','Base_Dt','BB');
																				 
        CALL Decisioning_procs.Add_Demographics_To_Base('CS_Raw_test','Base_Dt');
		MESSAGE CAST(now() as timestamp)||' | 2.4' TO CLIENT;
		
		CALL Decisioning_procs.Add_Offers_Software('CS_Raw_test','Base_Dt','DTV');
        CALL Decisioning_procs.Add_Offers_Software('CS_Raw_test','Base_Dt','BB');
        Call Decisioning_procs.Add_Software_Orders('CS_Raw_test','Base_Dt','Movies');
		Call Decisioning_procs.Add_Software_Orders('CS_Raw_test','Base_Dt','Sports');
		MESSAGE CAST(now() as timestamp)||' | 2.5' TO CLIENT;
        
        CALL Decisioning_procs.Add_Broadband_Postcode_Exchange_To_Base('CS_Raw_test');
        CALL Decisioning_procs.Add_Fibre_Areas('CS_Raw_test');
        CALL Decisioning_Procs.Add_Turnaround_Attempts('CS_Raw_test','Base_Dt','TA Events');
		CALL Decisioning_procs.Add_BB_Provider('CS_Raw_test','Base_Dt');
		Call Decisioning_procs.Add_Software_Orders('cs_binned2','Base_Dt','MS+','Account_Number','Drop and Replace')

		CALL Decisioning_procs.Add_OTT_Purchases('CS_Raw_test','Base_Dt');					-- ALL OTT
		CALL Decisioning_procs.Add_OTT_Purchases('CS_Raw_test','Base_Dt','Movies');
		CALL Decisioning_procs.Add_OTT_Purchases('CS_Raw_test','Base_Dt','BNK');
		CALL Decisioning_procs.Add_OD_Downloads ('CS_Raw_test','Base_Dt');
		
		MESSAGE CAST(now() as timestamp)||' | 3' TO CLIENT;
																			
	
	/* 	1.4 Add target and eligibility flags 	*/

        ALTER TABLE CS_Raw_test
        ADD (Rental_eligible       	BIT DEFAULT 0 
            ,Buy_and_keep_eligible 	BIT DEFAULT 0 
            ,SkyQ_eligible 	 		BIT DEFAULT 0 
			, Mobile_eligible 	BIT DEFAULT 0 
			, movies_eligible			BIT DEFAULT 0 
			, sports_eligible			BIT DEFAULT 0 
			, rentals_used_before		BIT DEFAULT 0 
			, bak_used_before		BIT DEFAULT 0 
			);
		MESSAGE CAST(now() as timestamp)||' | 4' TO CLIENT
		GO
    
		/* ELIGIBILITY FLAGS */ 
		
		UPDATE CS_Raw_test
			SET   Rental_eligible		= CASE WHEN Movies_Purchases_In_Last_90d = 0 	THEN 1 ELSE 0 END 
				, rentals_used_before	= CASE WHEN Movies_Purchases_Ever = 0 			THEN 0 ELSE 1 END 
				, Buy_and_keep_eligible	= CASE WHEN BnK_Purchases_In_Last_90d = 0 		THEN 1 ELSE 0 END 
				, bak_used_before		= CASE WHEN BnK_Purchases_Ever = 0  			THEN 0 ELSE 1 END
				, movies_eligible		= CASE WHEN Movies_Active = 0 AND Order_Movies_Added_In_Last_30d = 0 THEN 1 ELSE 0 END
				, sports_eligible		= CASE WHEN sports_Active = 0 AND Order_Sports_Added_In_Last_30d = 0 THEN 1 ELSE 0 END
			
			
			MESSAGE CAST(now() as timestamp)||' | 6' TO CLIENT
		GO
	/*		--------- Mobile_eligible		*/
			
			SELECT c.account_number
                , MAX(a.prod_earliest_mobile_ordered_dt) dt 
				, base_dt
            INTO #mobile
			FROM cust_single_mobile_account_view    AS a
			JOIN cust_single_mobile_view            AS b ON a.account_number = b.account_number
			JOIN cust_single_account_view           AS c ON a.portfolio_id = c.acct_fo_portfolio_id
			JOIN CS_Raw_test 							AS x ON x.account_number = c.account_number AND a.prod_earliest_mobile_ordered_dt <= base_dt
			GROUP BY c.account_number, base_dt 
						
			COMMIT
			CREATE HG INDEX id1 	ON #mobile (account_number)
			CREATE DATE INDEX id2 	ON #mobile (base_dt)
			CREATE DTTM INDEX id3 	ON #mobile (dt)
			COMMIT
			
			UPDATE CS_Raw_test a
			SET Mobile_eligible	= CASE WHEN cps.account_number IS NULL THEN 1 ELSE 0 END 
			FROM CS_Raw_test a
			LEFT JOIN #mobile AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
			
			DROP TABLE #mobile
			COMMIT
			MESSAGE CAST(now() as timestamp)||' | 7' TO CLIENT
		GO

					
/*			--------- SkyQ_eligible ---- 	*/
			
			SELECT  stb.account_number
					,MIN(CASE 	WHEN x_description  in ('Sky Q Silver','Sky Q Mini','Sky Q 2TB box','Sky Q','Sky Q 1TB box') THEN 0 	--- Known box descriptions
								WHEN UPPER(x_description) LIKE '%SKY Q%'	THEN 0														--- Any other new model
								ELSE 1 END ) 		AS PrimaryBoxType
					, base.base_dt
			INTO #sky_q_elig
			FROM cust_set_top_box AS stb
			JOIN CS_Raw_test AS base ON stb.account_number = base.account_number AND stb.created_dt <= base.base_dt 
			WHERE   base.account_number IS NOT NULL
			GROUP BY  stb.account_number
					, base.base_dt
			
			COMMIT
			CREATE HG INDEX id1 	ON #sky_q_elig (account_number)
			CREATE DATE INDEX id2 	ON #sky_q_elig (base_dt)
			COMMIT 
			
			UPDATE CS_Raw_test
			SET    SkyQ_eligible = b.PrimaryBoxType
			FROM   CS_Raw_test AS base
			JOIN  	#sky_q_elig AS b ON b.account_number = base.account_number AND b.base_dt = base.base_dt 
						
/*			--- Flagging Black tier AND non-DTV active customers as non-eligible	*/
			UPDATE CS_Raw_test
			SET    SkyQ_eligible = 0
			WHERE DATEDIFF(YEAR, DTV_Last_Activation_Dt, base_dt) >= 15		
				OR DTV_active = 0 											
			
			MESSAGE CAST(now() as timestamp)||' | 8' TO CLIENT
		GO			
		

/*    --  1.7 Random sample variable		*/

        CREATE Variable @multi BIGINT;
        SET @multi = DATEPART(MS,NOW())+1;
        ALTER TABLE CS_Raw_test ADD rand_num DECIMAL(22,20);
        UPDATE CS_Raw_test
           SET rand_num = RAND(NUMBER(*)* @multi);
        CREATE HG INDEX idx1 on CS_Raw_test(rand_num);    

/*-------------------------------------------
--  2.0 Creating extra variables
-------------------------------------------*/

  

		MESSAGE CAST(now() as timestamp)||' | 13' TO CLIENT
		
        ALTER TABLE CS_Raw_test
        ADD (num_sports_events      INT          DEFAULT NULL)
		GO
        DROP TABLE  IF EXISTS #temp_ppv;
        SELECT       a.account_number
                    ,a.basE_dt
                    ,sum(CASE WHEN ppv_viewed_dt BETWEEN dateadd(mm,-12,base_dt) AND base_dt AND ppv_service='EVENT'
                              
                              AND ppv_cancelled_dt = '9999-09-09' THEN 1 ELSE 0 END) AS num_sport_events_12m
							  
        INTO        #temp_ppv
        FROM        CS_Raw_test a
        INNER JOIN  CUST_PRODUCT_CHARGES_PPV b
        ON          a.account_number   = b.account_number
        WHERE       b.ppv_cancelled_dt = '9999-09-09'
           AND      b.ppv_viewed_dt   <= base_dt
           AND      b.ppv_viewed_dt   >= (base_dt-365)
        GROUP BY     a.account_number
                    ,a.base_dt;

        UPDATE      CS_Raw_test as a
        SET         a.num_sports_events = b.num_sport_events_12m
        FROM        #temp_ppv as b
        WHERE       a.account_number = b.account_number
			AND         a.base_dt = b.base_dt;

       MESSAGE CAST(now() as timestamp)||' | 14' TO CLIENT;
		GO
   
    /*--  2.3 Recode DTV product holding*/

        ALTER TABLE CS_Raw_test 
            ADD DTV_product_holding_recode VARCHAR(40);
           
		  
		MESSAGE CAST(now() as timestamp)||' | 17' TO CLIENT;
		GO
		   
        UPDATE      CS_Raw_test 
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

    /*--  2.4 Create TA reason flags*/

		MESSAGE CAST(now() as timestamp)||' | 18' TO CLIENT;
		GO
        ALTER TABLE CS_Raw_test
        ADD         (_1st_TA_reason_flag VARCHAR(15)
                    ,last_TA_reason_flag VARCHAR(15)
					,DTV_Pending_cancels_ever INT DEFAULT NULL );
		
		MESSAGE CAST(now() as timestamp)||' | 18' TO CLIENT;
		GO
		  
							 
        
		UPDATE      CS_Raw_test
        SET          _1st_TA_reason_flag = CASE WHEN _1st_TA_reason IS NULL THEN 'No reason given' ELSE 'Reason given' END
                    ,last_TA_reason_flag = CASE WHEN last_TA_reason IS NULL THEN 'No reason given' ELSE 'Reason given' END;

			 
			   

/*--  2.5 Cleanup

        --  2.5.1 Age	*/

            UPDATE      CS_Raw_test
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

   /*     --  2.5.2 Missing values		*/

            UPDATE      CS_Raw_test
            SET          ADSL_Enabled             = CASE WHEN ADSL_Enabled             IS NULL                         THEN 'Unknown' ELSE ADSL_Enabled             END
						,Exchange_Status          = CASE WHEN Exchange_Status          IS NULL                         THEN 'Unknown' ELSE Exchange_Status          END
						,DTV_CusCan_Churns_Ever   = CASE WHEN DTV_CusCan_Churns_Ever   IS NULL                         THEN 0         ELSE DTV_CusCan_Churns_Ever   END
                        ,DTV_Pending_cancels_ever = CASE WHEN DTV_PCs_Ever 				IS NULL                         THEN 0         ELSE DTV_PCs_Ever	END
                        ,DTV_SysCan_Churns_Ever   = CASE WHEN DTV_SysCan_Churns_Ever   IS NULL                         THEN 0         ELSE DTV_SysCan_Churns_Ever   END
						,_1st_TA_outcome          = CASE WHEN _1st_TA_outcome          IS NULL                         THEN 'No TA'   ELSE _1st_TA_outcome          END
                        ,last_TA_outcome          = CASE WHEN last_TA_outcome          IS NULL                         THEN 'No TA'   ELSE last_TA_outcome          END;

																																									   
		MESSAGE CAST(now() as timestamp)||' | 19' TO CLIENT;
		GO
 /*       --  2.5.3 Turn dates into days	*/

            ALTER TABLE CS_Raw_test
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
                        ,last_TA_nonsave                INT
						,Last_movies_downgrade 			INT
						,Last_sports_downgrade 			INT
						,Sports_Last_Downgrade 			INT DEFAULT NULL
						,Movies_Last_Activation			INT DEFAULT NULL
						,Movies_1st_Activation			INT DEFAULT NULL 
						,Movies_Last_Downgrade			INT DEFAULT NULL 
						
						);

		MESSAGE CAST(now() as timestamp)||' | 20' TO CLIENT;
		GO
        
            UPDATE      CS_Raw_test
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
                        ,last_TA_nonsave                = DATEDIFF(DAY, last_TA_nonsave_dt, base_dt)
						,Last_movies_downgrade	     	= DATEDIFF(DAY, Movies_Last_Downgrade_Dt, base_dt)
						,Last_sports_downgrade		    = DATEDIFF(DAY, Sports_Last_Downgrade_Dt, base_dt)
						,Sports_Last_Downgrade			= DATEDIFF(DAY, Sports_Last_Downgrade_Dt, base_dt)
						,Sports_Last_activation			= DATEDIFF(DAY, Sports_Last_Activation_Dt, base_dt)
						,Movies_1st_Activation		    = DATEDIFF(DAY, Movies_1st_Activation_Dt, base_dt)
						,Movies_Last_Activation			= DATEDIFF(DAY, Movies_Last_Activation_Dt, base_dt)
						,Movies_Last_Downgrade			= DATEDIFF(DAY, Movies_Last_Downgrade_Dt, base_dt);
							
	 
	 
		MESSAGE CAST(now() as timestamp)||' | 21' TO CLIENT;
		GO
	
		
 /*   --  2.6 NOW TV Variables		*/

        ALTER TABLE CS_Raw_test 
        ADD         (accountid         BIGINT
                    ,NTV_Ents_Last_30D BIT DEFAULT 0
                    ,NTV_Ents_Last_90D BIT DEFAULT 0);


		MESSAGE CAST(now() as timestamp)||' | 22' TO CLIENT;
		GO
        UPDATE      CS_Raw_test
        SET         accountid = mapped.accountid
        FROM        CS_Raw_test csr
        INNER JOIN  tva02.mapped_account_numbers mapped
        ON          csr.account_number = mapped.account_number;

        CREATE HG INDEX id_accid ON CS_Raw_test (accountid);
			
		  
						 

        UPDATE      CS_Raw_test csr
        SET         NTV_Ents_Last_30D = 1
        FROM        citeam.nowtv_accounts_ents ntvents
        WHERE       csr.account_number = ntvents.account_number
        AND         ntvents.subscriber_this_period = 1
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) >= -30
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) <= 0
        AND         ntvents.accountid IS NOT NULL;

        UPDATE      CS_Raw_test csr
        SET         NTV_Ents_Last_90D = 1
        FROM        citeam.nowtv_accounts_ents ntvents
        WHERE       csr.account_number = ntvents.account_number
        AND         ntvents.subscriber_this_period = 1
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) >= -90
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) <= 0
        AND         ntvents.accountid IS NOT NULL;

		MESSAGE CAST(now() as timestamp)||' | 22' TO CLIENT;
		GO
/*	   
-------------------------------------------
--  3.0 Create final tables
-------------------------------------------

    --  3.1 Create binning table
  */      
        DROP TABLE IF EXISTS CS_Raw_test_BINNED;
        SELECT       account_number
                    ,base_dt
                    ,rand_num
	/*	---- flags */
					
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
                    ,CASE WHEN BB_Churns_Ever  = 0 THEN '0'
                          WHEN BB_Churns_Ever >= 1 THEN '1'
                          WHEN BB_Churns_Ever >= 2 THEN '2'
                          WHEN BB_Churns_Ever >= 3 THEN '3'
                          WHEN BB_Churns_Ever >= 4 THEN 'ge4' END AS BB_Churns_Ever_b
				  ,CASE WHEN BB_Churns_In_Last_180D  = 0 THEN '0'
                          WHEN BB_Churns_In_Last_180D >= 1 THEN '1'
                          WHEN BB_Churns_In_Last_180D >= 2 THEN '2'
                          WHEN BB_Churns_In_Last_180D >= 3 THEN '3'
                          WHEN BB_Churns_In_Last_180D >= 4 THEN 'ge4' END AS BB_Churns_Last_180D_b
                    ,CASE WHEN BB_Churns_In_Last_1Yr  = 0 THEN '0'
                          WHEN BB_Churns_In_Last_1Yr >= 1 THEN '1'
                          WHEN BB_Churns_In_Last_1Yr >= 2 THEN '2'
                          WHEN BB_Churns_In_Last_1Yr >= 3 THEN '3'
                          WHEN BB_Churns_In_Last_1Yr >= 4 THEN 'ge4' END AS BB_Churns_Last_1Yr_b
                    ,CASE WHEN BB_Churns_In_Last_30D  = 0 THEN '0'
                          WHEN BB_Churns_In_Last_30D >= 1 THEN '1'
                          WHEN BB_Churns_In_Last_30D >= 2 THEN '2'
                          WHEN BB_Churns_In_Last_30D >= 3 THEN '3'
                          WHEN BB_Churns_In_Last_30D >= 4 THEN 'ge4' END AS BB_Churns_Last_30D_b
                    ,CASE WHEN BB_Churns_In_Last_3Yr  = 0 THEN '0'
                          WHEN BB_Churns_In_Last_3Yr >= 1 THEN '1'
                          WHEN BB_Churns_In_Last_3Yr >= 2 THEN '2'
                          WHEN BB_Churns_In_Last_3Yr >= 3 THEN '3'
                          WHEN BB_Churns_In_Last_3Yr >= 4 THEN 'ge4' END AS BB_Churns_Last_3Yr_b
                    ,CASE WHEN BB_Churns_In_Last_5Yr  = 0 THEN '0'
                          WHEN BB_Churns_In_Last_5Yr >= 1 THEN '1'
                          WHEN BB_Churns_In_Last_5Yr >= 2 THEN '2'
                          WHEN BB_Churns_In_Last_5Yr >= 3 THEN '3'
                          WHEN BB_Churns_In_Last_5Yr >= 4 THEN 'ge4' END AS BB_Churns_Last_5Yr_b
                    ,CASE WHEN BB_Churns_In_Last_90D  = 0 THEN '0'
                          WHEN BB_Churns_In_Last_90D >= 1 THEN '1'
                          WHEN BB_Churns_In_Last_90D >= 2 THEN '2'
                          WHEN BB_Churns_In_Last_90D >= 3 THEN '3'
                          WHEN BB_Churns_In_Last_90D >= 4 THEN 'ge4' END AS BB_Churns_Last_90D_b
 				  ,CASE WHEN BB_Enter_CusCan_Ever  = 0 THEN '0'
                          WHEN BB_Enter_CusCan_Ever >= 1 THEN '1'
                          WHEN BB_Enter_CusCan_Ever >= 2 THEN '2'
                          WHEN BB_Enter_CusCan_Ever >= 3 THEN '3'
                          WHEN BB_Enter_CusCan_Ever >= 4 THEN 'ge4' END AS BB_CusCan_PL_Entry_Ever_b
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
                    ,CASE WHEN DTV_ABs_Ever  = 0 THEN '0'
                          WHEN DTV_ABs_Ever >= 1 THEN '1'
                          WHEN DTV_ABs_Ever >= 2 THEN '2'
                          WHEN DTV_ABs_Ever >= 3 THEN '3'
                          WHEN DTV_ABs_Ever >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Ever_b
                    ,CASE WHEN DTV_ABs_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_ABs_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_ABs_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_ABs_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_ABs_In_Last_180D >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_180D_b
                    ,CASE WHEN DTV_ABs_In_Last_1Yr  = 0 THEN '0'
                          WHEN DTV_ABs_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_ABs_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_ABs_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_ABs_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_1Yr_b
                    ,CASE WHEN DTV_ABs_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_ABs_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_ABs_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_ABs_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_ABs_In_Last_30D >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_30D_b
                    ,CASE WHEN DTV_ABs_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_ABs_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_ABs_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_ABs_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_ABs_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_3Yr_b
                    ,CASE WHEN DTV_ABs_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_ABs_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_ABs_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_ABs_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_ABs_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_5Yr_b
                    ,CASE WHEN DTV_ABs_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_ABs_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_ABs_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_ABs_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_ABs_In_Last_90D >= 4 THEN 'ge4' END AS DTV_Active_Blocks_Last_90D_b
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
                    ,CASE WHEN DTV_PCs_Ever = 0 THEN '0'
                          WHEN DTV_PCs_Ever >= 1 THEN '1'
                          WHEN DTV_PCs_Ever >= 2 THEN '2'
                          WHEN DTV_PCs_Ever >= 3 THEN '3'
                          WHEN DTV_PCs_Ever >= 4 THEN 'ge4' END AS DTV_Pending_cancels_ever                   
					,CASE WHEN DTV_PCs_In_Last_180D  = 0 THEN '0'
                          WHEN DTV_PCs_In_Last_180D >= 1 THEN '1'
                          WHEN DTV_PCs_In_Last_180D >= 2 THEN '2'
                          WHEN DTV_PCs_In_Last_180D >= 3 THEN '3'
                          WHEN DTV_PCs_In_Last_180D >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_180D_b
                    ,CASE WHEN DTV_PCs_In_Last_1Yr = 0 THEN '0'
                          WHEN DTV_PCs_In_Last_1Yr >= 1 THEN '1'
                          WHEN DTV_PCs_In_Last_1Yr >= 2 THEN '2'
                          WHEN DTV_PCs_In_Last_1Yr >= 3 THEN '3'
                          WHEN DTV_PCs_In_Last_1Yr >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_1Yr_b
                    ,CASE WHEN DTV_PCs_In_Last_30D  = 0 THEN '0'
                          WHEN DTV_PCs_In_Last_30D >= 1 THEN '1'
                          WHEN DTV_PCs_In_Last_30D >= 2 THEN '2'
                          WHEN DTV_PCs_In_Last_30D >= 3 THEN '3'
                          WHEN DTV_PCs_In_Last_30D >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_30D_b
                    ,CASE WHEN DTV_PCs_In_Last_3Yr  = 0 THEN '0'
                          WHEN DTV_PCs_In_Last_3Yr >= 1 THEN '1'
                          WHEN DTV_PCs_In_Last_3Yr >= 2 THEN '2'
                          WHEN DTV_PCs_In_Last_3Yr >= 3 THEN '3'
                          WHEN DTV_PCs_In_Last_3Yr >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_3Yr_b
                    ,CASE WHEN DTV_PCs_In_Last_5Yr  = 0 THEN '0'
                          WHEN DTV_PCs_In_Last_5Yr >= 1 THEN '1'
                          WHEN DTV_PCs_In_Last_5Yr >= 2 THEN '2'
                          WHEN DTV_PCs_In_Last_5Yr >= 3 THEN '3'
                          WHEN DTV_PCs_In_Last_5Yr >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_5Yr_b
                    ,CASE WHEN DTV_PCs_In_Last_90D  = 0 THEN '0'
                          WHEN DTV_PCs_In_Last_90D >= 1 THEN '1'
                          WHEN DTV_PCs_In_Last_90D >= 2 THEN '2'
                          WHEN DTV_PCs_In_Last_90D >= 3 THEN '3'
                          WHEN DTV_PCs_In_Last_90D >= 4 THEN 'ge4' END AS DTV_Pending_Cancels_Last_90D_b
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
                    ,NTV_Ents_Last_30D
                    ,NTV_Ents_Last_90D
					,num_sports_events     
                    ,BB_Provider
					--- OD 
					, OD_DLs_Completed_In_Last_90d AS OD_Last_3M
					, OD_DLs_Completed_In_Last_1yr AS OD_Last_12M
					, DATEDIFF (MONTH, Last_Completed_OD_DL_Dt, BASE_DT) AS OD_Months_since_Last
					
					, BnK_Purchases_In_Last_180d AS BAK_6M 
					, BnK_Purchases_In_Last_1yr AS BAK_12M 
					, BnK_Purchases_In_Last_1yr AS BAK_9M 
					, DATEDIFF (MONTH, Last_BnK_Purchased_Dt, BASE_DT) AS BAK_time_since_last
					
					, Movies_Purchases_In_Last_180d AS rents_6M
					, Movies_Purchases_In_Last_180d AS rents_9M
					, Movies_Purchases_In_Last_1yr AS rents_12M
					, DATEDIFF (MONTH, Last_Movies_Purchased_Dt, BASE_DT) AS rentals_months_since_last
					
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
					/*Prems*/
					, Movies_Active
					, Sports_Active
					, Movies_Product_Holding
					, Sports_Product_Holding
					, Last_movies_downgrade
					, Last_sports_downgrade
					, CASE 	WHEN DATEDIFF (MONTH, Sports_Last_Activation_Dt, Base_Dt) <= 24 THEN 'A.<2 Yrs'
							WHEN DATEDIFF (MONTH, Sports_Last_Activation_Dt, Base_Dt) BETWEEN 25 AND 60 THEN 'B.<5 Yrs'
							WHEN DATEDIFF (MONTH, Sports_Last_Activation_Dt, Base_Dt) BETWEEN 61 AND 120 THEN 'C.<10 Yrs'
							WHEN DATEDIFF (MONTH, Sports_Last_Activation_Dt, Base_Dt) > 120 THEN 'D.10+ Yrs'
							ELSE Null END AS Sports_Tenure         
					, CASE 	WHEN DATEDIFF (MONTH, Movies_Last_Activation_Dt, Base_Dt) <= 24 THEN 'A.<2 Yrs'
							WHEN DATEDIFF (MONTH, Movies_Last_Activation_Dt, Base_Dt) BETWEEN 25 AND 60 THEN 'B.<5 Yrs'
							WHEN DATEDIFF (MONTH, Movies_Last_Activation_Dt, Base_Dt) BETWEEN 61 AND 120 THEN 'C.<10 Yrs'
							WHEN DATEDIFF (MONTH, Movies_Last_Activation_Dt, Base_Dt) > 120 THEN 'D.10+ Yrs'
							ELSE Null END AS Movies_Tenure     
					, TAs_in_next_30d
	
	                ,CASE WHEN BB_Curr_Contract_Start_Dt IS      NULL AND BB_Prev_Contract_Start_Dt                  IS NULL THEN 'A.NeverOnContract'
						  WHEN BB_Curr_Contract_Start_Dt IS      NULL AND (base_dt-BB_Prev_Contract_Actual_End_Dt)    >  180 THEN 'B.ExpiredContract(>6M)'
						  WHEN BB_Curr_Contract_Start_Dt IS      NULL AND (base_dt-BB_Prev_Contract_Actual_End_Dt)    <= 180 THEN 'C.ExpiredContract(<6M)'
						  WHEN BB_Curr_Contract_Start_Dt IS NOT  NULL AND (BB_Curr_Contract_Intended_End_Dt-base_dt)  <= 90  THEN 'D.ExpiringContract(<3M)'
						  WHEN BB_Curr_Contract_Start_Dt IS NOT  NULL AND (BB_Curr_Contract_Intended_End_Dt-base_dt)  <= 180 THEN 'E.ExpiringContract(<6M)'
						  WHEN BB_Curr_Contract_Start_Dt IS NOT  NULL AND (BB_Curr_Contract_Intended_End_Dt-base_dt)  >  180 THEN 'F.ExpiringContract(>6M)'
						  ELSE '' END AS BB_contract_segment
					,CASE WHEN DTV_Curr_Contract_Start_Dt IS     NULL AND DTV_Prev_Contract_Start_Dt                 IS NULL THEN 'A.NeverOnContract'
						  WHEN DTV_Curr_Contract_Start_Dt IS     NULL AND (base_dt-DTV_Prev_Contract_Actual_End_Dt)   >  180 THEN 'B.ExpiredContract(>6M)'
						  WHEN DTV_Curr_Contract_Start_Dt IS     NULL AND (base_dt-DTV_Prev_Contract_Actual_End_Dt)   <= 180 THEN 'C.ExpiredContract(<6M)'
						  WHEN DTV_Curr_Contract_Start_Dt IS NOT NULL AND (DTV_Curr_Contract_Intended_End_Dt-base_dt) <= 90  THEN 'D.ExpiringContract(<3M)'
						  WHEN DTV_Curr_Contract_Start_Dt IS NOT NULL AND (DTV_Curr_Contract_Intended_End_Dt-base_dt) <= 180 THEN 'E.ExpiringContract(<6M)'
						  WHEN DTV_Curr_Contract_Start_Dt IS NOT NULL AND (DTV_Curr_Contract_Intended_End_Dt-base_dt) >  180 THEN 'F.ExpiringContract(>6M)'
						  ELSE '' END AS DTV_contract_segment
					, BB_Product_Holding
					, Movies_Last_Activation			
					, Movies_1st_Activation
					, Movies_Last_Downgrade
					, Sports_Last_Downgrade

					, Movies_New_Adds_Ever
					, Sports_1st_Platform_Churn_Dt
					,Sports_Last_Platform_Churn_Dt
					,Sports_1st_Downgrade_Dt
					,Sports_Last_Downgrade_Dt
					,Sports_Platform_Churns_In_Last_1D
					,Sports_Platform_Churns_In_Last_30D
					,Sports_Platform_Churns_In_Last_90D
					,Sports_Platform_Churns_In_Last_180D
					,Sports_Platform_Churns_In_Last_1Yr
					,Sports_Platform_Churns_In_Last_3Yr
					,Sports_Platform_Churns_In_Last_5Yr
					,Sports_Platform_Churns_Ever
					,Sports_Platform_Churns_In_Next_7D
					,Sports_Platform_Churns_In_Next_30D
					,Sports_Downgrades_In_Last_1D
					,Sports_Downgrades_In_Last_30D
					,Sports_Downgrades_In_Last_90D
					,Sports_Downgrades_In_Last_180D
					,Sports_Downgrades_In_Last_1Yr
					,Sports_Downgrades_In_Last_3Yr
					,Sports_Downgrades_In_Last_5Yr
					,Sports_Downgrades_Ever
					,Sports_Downgrades_In_Next_7D
					,Sports_Downgrades_In_Next_30D
					,Movies_1st_Platform_Churn_Dt
					,Movies_Last_Platform_Churn_Dt
					,Movies_1st_Downgrade_Dt
					,Movies_Last_Downgrade_Dt
					,Movies_Platform_Churns_In_Last_1D
					,Movies_Platform_Churns_In_Last_30D
					,Movies_Platform_Churns_In_Last_90D
					,Movies_Platform_Churns_In_Last_180D
					,Movies_Platform_Churns_In_Last_1Yr
					,Movies_Platform_Churns_In_Last_3Yr
					,Movies_Platform_Churns_In_Last_5Yr
					,Movies_Platform_Churns_Ever
					,Movies_Platform_Churns_In_Next_7D
					,Movies_Platform_Churns_In_Next_30D
					,Movies_Downgrades_In_Last_1D
					,Movies_Downgrades_In_Last_30D
					,Movies_Downgrades_In_Last_90D
					,Movies_Downgrades_In_Last_180D
					,Movies_Downgrades_In_Last_1Yr
					,Movies_Downgrades_In_Last_3Yr
					,Movies_Downgrades_In_Last_5Yr
					,Movies_Downgrades_Ever

					,	Movies_Reinstates_Ever
					,	Movies_Reinstates_In_Last_180D
					,	Movies_Reinstates_In_Last_1D
					,	Movies_Reinstates_In_Last_1Yr
					,	Movies_Reinstates_In_Last_30D
					,	Movies_Reinstates_In_Last_3Yr
					,	Movies_Reinstates_In_Last_5Yr
					,	Movies_Reinstates_In_Last_90D
					,	Sports_Activations_Ever
					,	Sports_Activations_In_Last_180D
					,	Sports_Activations_In_Last_1D
					,	Sports_Activations_In_Last_1Yr
					,	Sports_Activations_In_Last_30D
					,	Sports_Activations_In_Last_3Yr
					,	Sports_Activations_In_Last_5Yr
					,	Sports_Activations_In_Last_90D
						
				
				
					,	Sports_Product_Count
					
					
					,CASE WHEN Sports_Last_Downgrade THEN 'A. No downgrade'
                        WHEN Sports_Last_Downgrade <= 90    THEN 'B. Less than 90 days' 
                        WHEN Sports_Last_Downgrade <= 180    THEN 'C. Between 90 days and 6 months' 
                        WHEN Sports_Last_Downgrade <= 365   THEN 'D. Between 6 months and 1 year' 
                        WHEN Sports_Last_Downgrade <= 730   THEN 'E. Between 1 and 2 years'
                        WHEN Sports_Last_Downgrade <= 1095   THEN 'F. Between 2 and 3 years'
                        WHEN Sports_Last_Downgrade <= 1460   THEN 'G. Between 3 and 4 years' 
                        WHEN Sports_Last_Downgrade <= 1825   THEN 'H. Between 4 and 5 years' 
                        WHEN Sports_Last_Downgrade <= 2190   THEN 'I. Between 5 and 6 years' 
                        WHEN Sports_Last_Downgrade <= 2555   THEN 'J. Between 6 and 7 years' 
                        WHEN Sports_Last_Downgrade <= 2920   THEN 'K. Between 7 and 8 years' 
                        WHEN Sports_Last_Downgrade <= 3285   THEN 'L. Between 8 and 9 years' 
                        WHEN Sports_Last_Downgrade <= 4380   THEN 'M. Between 9 and 12 years' 
                        WHEN Sports_Last_Downgrade >  4380   THEN 'N. More than 12 years' 
                        ELSE 'O. Other' END                                        AS sports_lastdowngrade_tenure_b
					
					  ,CASE WHEN Sports_Last_activation IS NULL THEN 'A. No activation'
                        WHEN Sports_Last_activation <= 180    THEN 'B. Less than 6 months' 
                        WHEN Sports_Last_activation <= 365   THEN 'D. Between 6 months and 1 year' 
                        WHEN Sports_Last_activation <= 730   THEN 'C. Between 1 and 2 years'
                        WHEN Sports_Last_activation <= 1825   THEN 'D. Between 2 and 5 years' 
                        WHEN Sports_Last_activation <= 2555   THEN 'E. Between 5 and 7 years' 
                        WHEN Sports_Last_activation <= 3285   THEN 'F. Between 7 and 9 years' 
                        WHEN Sports_Last_activation <= 4380   THEN 'G. Between 9 and 12 years' 
                        WHEN Sports_Last_activation >  4380   THEN 'H. More than 12 years' 
                        ELSE 'I. Other' END                                        AS sports_lastactivation_tenure_b

					, CASE WHEN	OD_Last_3M is null 				THEN 0 ELSE OD_Last_3M END OD_Last_3M 
					, CASE WHEN DTV_package LIKE 'C.Original' 	THEN 1 ELSE 0 AS DTV_package_Orig
					, CASE WHEN DTV_product_holding LIKE '%Sports%' OR DTV_product_holding LIKE '%Cinema%' 	THEN 1 ELSE 0 END AS Premium_holding
					, CASE WHEN h_household_composition in ('Single Female' , 'Single Male') 				THEN 1 ELSE 0 END AS hh_type_single
					, CASE WHEN dtv_last_tenure in ('F.5-6 Years','G.7-10 Years','H.11+ Years') 			THEN 1 ELSE 0 END AS DTV_tenure_gt5yrs
					, CASE WHEN Home_Owner_Status in ('Owner') 	THEN 1 ELSE 0 END AS Home_Owned_by_Owner 
income_gt60k=0; if h_income_value > 60000 then income_gt60k=1;
h_young_person_yes =0; if h_presence_of_young_person_at_ad in ('Yes') then h_young_person_yes =1;
age_btw30_50=0; if 30 < age <= 50 then age_btw30_50 = 1;
						
						
        INTO        CS_Raw_test_BINNED
        FROM        CS_Raw_test
        WHERE       country = 'UK'
        
        CREATE HG INDEX id1 ON CS_Raw_test_BINNED (account_number);
        CREATE DATE INDEX iddt ON CS_Raw_test_BINNED (base_dt);
        
		MESSAGE CAST(now() as timestamp)||' | 23' TO CLIENT;
		GO
    
		
        GRANT SELECT ON CS_Raw_test TO PUBLIC;
        GRANT SELECT ON CS_Raw_test_BINNED TO PUBLIC;
        