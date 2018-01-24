/*
------ Contents ------
	--  0.0 Configuration
	--  1.0 Create CS_Raw dataset
		--  1.1 Create initial table
		--  1.2 Applying Model T
		--  1.3 Random sample variable
	--  2.0 Creating extra variables
		--  2.1 NOW TV Variables
		--  2.2 Recode DTV product holding
		--  2.3 Cleanup
			--  2.3.1 Age
			--  2.3.2 Turn dates into days
	--  3.0 Create model custom views
		-- 3.1 Boxsets
		-- 3.2 Non-Fibre Broadband
		-- 3.3 Fibre Broadband
		-- 3.4 Regrade Fibre Broadband
		-- 3.5 Skystore rentals
		-- 3.6 Buy and keep
		-- 3.7 Sky Q
		-- 3.8 Sky Mobile
		-- 3.9 Sky Cinema
	--  4.0 Permissions to public

		
NOTES: 
		Removed:
			- 1st_TA_reason_flag,last_TA_reason_flag - Must be recoded in SAS
		Missing: Need to be imputed in SAS
			- ADSL_Enabled             
			-Exchange_Status          
			-DTV_CusCan_Churns_Ever   
			-DTV_Pending_cancels_ever 
			-DTV_SysCan_Churns_Ever   
			-_1st_TA_outcome          
			-last_TA_outcome          
	
	
*/			
/*-------------------------------------------
--  0.0 Configuration
-------------------------------------------*/
MESSAGE CAST(now() as timestamp)||' | Initializing' TO CLIENT
	
    CREATE OR REPLACE VARIABLE @start_date DATE;
    CREATE OR REPLACE VARIABLE @end_date DATE;
    CREATE OR REPLACE VARIABLE @end_date_lag DATE;
	GO
    SET @start_date   = '2017-09-01';
    SET @end_date     = '2017-11-01';
    SET @end_date_lag = DATEADD(mm, -1, @end_date)

/*-------------------------------------------
--  1.0 Create CS_Raw dataset
-------------------------------------------*/

    /*--  1.1 Create initial table			*/

	MESSAGE CAST(now() as timestamp)||' | 1.1' TO CLIENT

        DROP TABLE  IF EXISTS #Qtr_Wk_End_Dts;
        SELECT calendar_date
        INTO #Qtr_Wk_End_Dts
        FROM sky_calendar
        WHERE datepart(DAY,calendar_date+1) = 1
                AND calendar_date BETWEEN @start_date AND @end_date;
		
		
        CREATE lf INDEX idx_1 ON #Qtr_Wk_End_Dts(Calendar_Date);


        DROP TABLE IF EXISTS CS_Raw;
        SELECT Cast(wk.calendar_date AS date) Base_Dt,account_number	
        INTO CS_Raw
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
				  
		/*--  1.2 Applying Model T*/

		MESSAGE CAST(now() as timestamp)||' | 1.2' TO CLIENT;

        CREATE HG 	INDEX id1 	ON CS_Raw (account_number);
        CREATE DATE INDEX iddt 	ON CS_Raw (base_dt);

        CALL Decisioning_procs.Add_Subs_Calendar_Fields('CS_Raw','Base_Dt');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw','Base_Dt','DTV');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw','Base_Dt','BB');
		
		Call Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw','Base_Dt','Sports');
		Call Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw','Base_Dt','Movies');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw','Base_Dt','MULTISCREEN');
        CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_Raw','Base_Dt','SGE');
		CALL Decisioning_Procs.Add_Active_Subscriber_Product_Holding('CS_Raw','Base_Dt','HD');
		
		MESSAGE CAST(now() as timestamp)||' | 1.2.1' TO CLIENT;

		CALL Decisioning_Procs.Add_Activations_DTV	('CS_Raw','Base_Dt');
        CALL Decisioning_Procs.Add_Activation_BB	('CS_Raw','Base_Dt');
		CALL Decisioning_Procs.Add_Activations_Prems('CS_Raw','Base_Dt','Sports');
		CALL Decisioning_Procs.Add_Activations_Prems('CS_Raw','Base_Dt','Movies');
	
		MESSAGE CAST(now() as timestamp)||' | 1.2.2' TO CLIENT;
		
		CALL Decisioning_Procs.Add_Churn_DTV 		('CS_Raw','Base_Dt');
		CALL Decisioning_Procs.Add_Churn_BB 		('CS_Raw','Base_Dt');
		CALL Decisioning_Procs.Add_PL_Entries_DTV	('CS_Raw','Base_Dt');
		CALL Decisioning_Procs.Add_PL_Entries_BB	('CS_Raw','Base_Dt');
		MESSAGE CAST(now() as timestamp)||' | 1.2.3' TO CLIENT;
		
																				 
        CALL Decisioning_procs.Add_Demographics_To_Base('CS_Raw','Base_Dt');
		MESSAGE CAST(now() as timestamp)||' | 1.2.4' TO CLIENT;
		
		CALL Decisioning_procs.Add_Offers_Software('CS_Raw','Base_Dt','DTV');
        CALL Decisioning_procs.Add_Offers_Software('CS_Raw','Base_Dt','BB');
        Call Decisioning_procs.Add_Software_Orders('CS_Raw','Base_Dt','Movies');
		Call Decisioning_procs.Add_Software_Orders('CS_Raw','Base_Dt','Sports');
		MESSAGE CAST(now() as timestamp)||' | 1.2.5' TO CLIENT;
        
        CALL Decisioning_procs.Add_Fibre_Areas			('CS_Raw');
        CALL Decisioning_Procs.Add_Turnaround_Attempts	('CS_Raw','Base_Dt','TA Events');
		CALL Decisioning_procs.Add_BB_Provider			('CS_Raw','Base_Dt');
		Call Decisioning_procs.Add_Software_Orders		('cs_binned2','Base_Dt','MS+','Account_Number','Drop and Replace');
        CALL Decisioning_procs.Add_Broadband_Postcode_Exchange_To_Base('CS_Raw');

		MESSAGE CAST(now() as timestamp)||' | 1.2.6' TO CLIENT;
		CALL Decisioning_procs.Add_OTT_Purchases('CS_Raw','Base_Dt');					-- ALL OTT
		CALL Decisioning_procs.Add_OTT_Purchases('CS_Raw','Base_Dt','Movies');
		CALL Decisioning_procs.Add_OTT_Purchases('CS_Raw','Base_Dt','BNK');
		
		CALL Decisioning_procs.Add_OD_Downloads ('CS_Raw','Base_Dt');
		
		MESSAGE CAST(now() as timestamp)||' | 1.2.7' TO CLIENT;
																			
		/*--  1.3 Random sample variable*/

        CREATE Variable @multi BIGINT;
        SET @multi = DATEPART(MS,NOW())+1;
        ALTER TABLE CS_Raw ADD rand_num DECIMAL(22,20);
        UPDATE CS_Raw
           SET rand_num = RAND(NUMBER(*)* @multi);
        CREATE HG INDEX idx1 on CS_Raw(rand_num);    

		MESSAGE CAST(now() as timestamp)||' | 1.3' TO CLIENT;
/*-------------------------------------------
--  2.0 Creating extra variables
-------------------------------------------*/
		
		
		/*--  2.1 NOW TV Variables			*/

        ALTER TABLE CS_Raw 
        ADD         (NTV_Ents_Last_30D BIT DEFAULT 0
                    ,NTV_Ents_Last_90D BIT DEFAULT 0);

		UPDATE      CS_Raw csr
        SET         NTV_Ents_Last_30D = 1
        FROM        citeam.nowtv_accounts_ents ntvents
        WHERE       csr.account_number = ntvents.account_number
        AND         ntvents.subscriber_this_period = 1
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) >= -30
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) <= 0
        AND         ntvents.accountid IS NOT NULL;

        UPDATE      CS_Raw csr
        SET         NTV_Ents_Last_90D = 1
        FROM        citeam.nowtv_accounts_ents ntvents
        WHERE       csr.account_number = ntvents.account_number
        AND         ntvents.subscriber_this_period = 1
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) >= -90
        AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) <= 0
        AND         ntvents.accountid IS NOT NULL;

		MESSAGE CAST(now() as timestamp)||' | 2.1' TO CLIENT;
		GO

    /* --  2.2 Recode DTV product holding			*/

        ALTER TABLE CS_Raw  ADD DTV_product_holding_recode VARCHAR(40);
        GO
		
		UPDATE      CS_Raw 
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
                                                       WHEN DTV_Product_Holding IS NULL                                           THEN 'None'                       
													   ELSE DTV_Product_Holding
                                                   END;

		MESSAGE CAST(now() as timestamp)||' | 2.2' TO CLIENT;
	/*	--  2.3 Cleanup
			-- 2.3.1 Age*/

            UPDATE      CS_Raw
            SET          Age = CASE 
								WHEN Age BETWEEN 18 AND 95      THEN Age
								WHEN Age BETWEEN 1916 AND 1999  THEN 2018-Age
								WHEN Age BETWEEN 1816 AND 1899  THEN 1918-Age
								WHEN Age BETWEEN 1016 AND 1099  THEN 1118-Age
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

        /*--  2.3.2 Turn dates into days */

            ALTER TABLE CS_Raw
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

		GO
        
            UPDATE      CS_Raw
            SET          DTV_Last_cuscan_churn          = DATEDIFF(DAY, DTV_Last_CusCan_Churn_Dt, base_dt)
                        ,DTV_Last_Activation            = DATEDIFF(DAY, DTV_Last_Activation_Dt, base_dt)
                     --   ,DTV_Curr_Contract_Intended_End = DATEDIFF(DAY, DTV_Curr_Contract_Intended_End_Dt, base_dt)
                     --   ,DTV_Curr_Contract_Start        = DATEDIFF(DAY, DTV_Curr_Contract_Start_Dt, base_dt)
                        ,DTV_Last_SysCan_Churn          = DATEDIFF(DAY, DTV_Last_SysCan_Churn_Dt, base_dt)
                        ,Curr_Offer_Start_DTV           = DATEDIFF(DAY, Curr_Offer_Start_Dt_DTV, base_dt)
                        ,Curr_Offer_Actual_End_DTV      = DATEDIFF(DAY, Curr_Offer_Actual_End_Dt_DTV, base_dt)
                        ,DTV_1st_Activation             = DATEDIFF(DAY, DTV_1st_Activation_Dt, base_dt)
                     --   ,BB_Curr_Contract_Intended_End  = DATEDIFF(DAY, BB_Curr_Contract_Intended_End_Dt, base_dt)
                     --   ,BB_Curr_Contract_Start         = DATEDIFF(DAY, BB_Curr_Contract_Start_Dt, base_dt)
                        ,DTV_Last_Active_Block          = DATEDIFF(DAY, DTV_Last_Active_Block_Dt, base_dt)
                        ,DTV_Last_Pending_Cancel        = DATEDIFF(DAY, DTV_Last_Pending_Cancel_Dt, base_dt)
                        ,BB_Last_Activation             = DATEDIFF(DAY, BB_Last_Activation_Dt, base_dt)
                        ,_1st_TA                        = DATEDIFF(DAY, _1st_TA_dt, base_dt)
                        ,last_TA                        = DATEDIFF(DAY, last_TA_dt, base_dt)
                        ,_1st_TA_save                   = DATEDIFF(DAY, _1st_TA_save_dt, base_dt)
                        ,last_TA_save                   = DATEDIFF(DAY, last_TA_save_dt, base_dt)
                        ,_1st_TA_nonsave                = DATEDIFF(DAY, _1st_TA_nonsave_dt, base_dt)
                        ,last_TA_nonsave                = DATEDIFF(DAY, last_TA_nonsave_dt, base_dt)
						,Last_movies_downgrade	     	= DATEDIFF(DAY, Sports_Last_Activation_Dt, base_dt)
						,Last_sports_downgrade		    = DATEDIFF(DAY, Movies_Last_Activation_Dt, base_dt)
						;
					
		MESSAGE CAST(now() as timestamp)||' | 2.3' TO CLIENT;
	 
	
/*-------------------------------------------
--  3.0 Create model custom views
-------------------------------------------

	--  3.1 Boxsets
			--Variables 
				-- Boxsets_eligible
				-- Up_boxsets		*/
	
		DROP TABLE  IF EXISTS boxset_temp; 
		SELECT account_number
			, base_dt
		INTO boxset_temp
		FROM CS_Raw;
		
		COMMIT;
		CREATE HG 	INDEX id1 ON boxset_temp(account_number);
		CREATE DATE INDEX id2 ON boxset_temp(Base_dt);
		COMMIT;
					
	    CALL Decisioning_procs.Add_Software_Orders('boxset_temp','Base_Dt','HD_BASIC','Account_Number','Drop and Replace','Order_HD_BASIC_Added_In_next_30d');
		CALL Decisioning_procs.Add_Software_Orders('boxset_temp','Base_Dt','HD_BASIC','Account_Number','Drop and Replace','Order_HD_BASIC_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('boxset_temp','Base_Dt','FAMILY','Account_Number','Drop and Replace','Order_FAMILY_Added_In_Last_30d');
		
		CREATE OR REPLACE VIEW CS_boxsets
		AS 
		SELECT a.*
			, CASE WHEN 			a.dtv_active = 1
						AND         (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
						AND         b.Order_FAMILY_Added_In_Last_30d   = 0
						AND         b.Order_HD_BASIC_Added_In_Last_30d = 0
						AND         (DTV_Product_Holding LIKE '%Variety%' OR DTV_Product_Holding LIKE '%Original%')
						AND         HD_active = 0
						THEN 1 ELSE 0 END 				AS Boxsets_eligible
			, CASE WHEN Order_HD_BASIC_Added_In_next_30d > 0  THEN 1 ELSE 0 END AS  Up_Boxsets 
		FROM CS_Raw AS a 
		LEFT JOIN boxset_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Boxsets_eligible = 1
		AND  country = 'UK';
		
		
		CREATE OR REPLACE VIEW CS_boxsets_over
		AS 
		SELECT a.*
			, CASE WHEN 			a.dtv_active = 1
						AND         (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
						AND         b.Order_FAMILY_Added_In_Last_30d   = 0
						AND         b.Order_HD_BASIC_Added_In_Last_30d = 0
						AND         (DTV_Product_Holding LIKE '%Variety%' OR DTV_Product_Holding LIKE '%Original%')
						AND         HD_active = 0
						THEN 1 ELSE 0 END 				AS Boxsets_eligible
			, CASE WHEN Order_HD_BASIC_Added_In_next_30d > 0  THEN 1 ELSE 0 END AS  Up_Boxsets 
		FROM CS_Raw AS a 
		LEFT JOIN boxset_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Up_Boxsets  = 1  
			AND Boxsets_eligible = 1 
			AND  country = 'UK'
		UNION 
		SELECT a.*
			, CASE WHEN 			a.dtv_active = 1
						AND         (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
						AND         b.Order_FAMILY_Added_In_Last_30d   = 0
						AND         b.Order_HD_BASIC_Added_In_Last_30d = 0
						AND         (DTV_Product_Holding LIKE '%Variety%' OR DTV_Product_Holding LIKE '%Original%')
						AND         HD_active = 0
						THEN 1 ELSE 0 END 				AS Boxsets_eligible
			, CASE WHEN Order_HD_BASIC_Added_In_next_30d > 0  THEN 1 ELSE 0 END AS  Up_Boxsets 
		FROM CS_Raw AS a 
		LEFT JOIN boxset_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Up_Boxsets  = 0 
			AND Boxsets_eligible = 1 
			AND rand_num <= 0.20	 			----- Must be changed accordingly
			AND  country = 'UK';

			
			
--  3.1 Non-Fibre Broadband
			--Variables 
				-- BB_eligible
				-- Up_BB
	
		DROP TABLE  IF EXISTS BB_temp; 
		SELECT account_number
			, base_dt
		INTO BB_temp
		FROM CS_Raw;
		
		COMMIT;
		CREATE HG 	INDEX id1 ON BB_temp(account_number);
		CREATE DATE INDEX id2 ON BB_temp(Base_dt);
		COMMIT;
					
		CALL Decisioning_procs.Add_Software_Orders('BB_temp','Base_Dt','BB_UNLIMITED','Account_Number','Drop and Replace','Order_BB_UNLIMITED_Added_In_Next_30d');
		CALL Decisioning_procs.Add_Software_Orders('BB_temp','Base_Dt','BB_LITE'		,'Account_Number','Drop and Replace','Order_BB_LITE_Added_In_Next_30d');
		CALL Decisioning_procs.Add_Software_Orders('BB_temp','Base_Dt','BB_UNLIMITED','Account_Number','Drop and Replace','Order_BB_UNLIMITED_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('BB_temp','Base_Dt','BB_LITE'		,'Account_Number','Drop and Replace','Order_BB_LITE_Added_In_Last_30d');
		
		
		CREATE OR REPLACE  VIEW CS_BB
		AS 
		SELECT a.*
			, CASE WHEN 	a.dtv_active = 1
						AND  (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
						AND  b.Order_BB_UNLIMITED_Added_In_Last_30d  = 0
						AND  b.Order_BB_LITE_Added_In_Last_30d       = 0
						AND  a.BB_Enter_HM_In_Last_30D    = 0
						AND  (a.bb_active = 0 OR a.bb_product_holding is NULL)
						AND  a.Exchange_Status = 'ONNET'
					THEN 1 ELSE 0 END 				AS BB_eligible
			, CASE WHEN (b.Order_BB_UNLIMITED_Added_In_Next_30d > 0
					OR   b.Order_BB_LITE_Added_In_Next_30d      > 0)  THEN 1 ELSE 0 END AS  Up_BB 
		FROM CS_Raw AS a 
		LEFT JOIN BB_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE BB_eligible = 1 
			AND  a.country = 'UK';
		
		
		CREATE OR REPLACE VIEW CS_BB_over
		AS 
		SELECT a.*
			, CASE WHEN 	a.dtv_active = 1
						AND  (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
						AND  b.Order_BB_UNLIMITED_Added_In_Last_30d  = 0
						AND  b.Order_BB_LITE_Added_In_Last_30d       = 0
						AND  a.BB_Enter_HM_In_Last_30D    = 0
						AND  (a.bb_active = 0 OR a.bb_product_holding is NULL)
						AND  a.Exchange_Status = 'ONNET'
					THEN 1 ELSE 0 END 				AS BB_eligible
			, CASE WHEN (b.Order_BB_UNLIMITED_Added_In_Next_30d > 0
					OR   b.Order_BB_LITE_Added_In_Next_30d      > 0)  THEN 1 ELSE 0 END AS  Up_BB 
		FROM CS_Raw AS a 
		LEFT JOIN BB_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE BB_eligible = 1 
			AND  a.country = 'UK'
			AND Up_BB = 1 
		UNION 
		SELECT a.*
			, CASE WHEN 	a.dtv_active = 1
						AND  (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
						AND  b.Order_BB_UNLIMITED_Added_In_Last_30d  = 0
						AND  b.Order_BB_LITE_Added_In_Last_30d       = 0
						AND  a.BB_Enter_HM_In_Last_30D    = 0
						AND  (a.bb_active = 0 OR a.bb_product_holding is NULL)
						AND  a.Exchange_Status = 'ONNET'
					THEN 1 ELSE 0 END 				AS BB_eligible
			, CASE WHEN (b.Order_BB_UNLIMITED_Added_In_Next_30d > 0
					OR   b.Order_BB_LITE_Added_In_Next_30d      > 0)  THEN 1 ELSE 0 END AS  Up_BB 
		FROM CS_Raw AS a 
		LEFT JOIN BB_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE BB_eligible = 1 
			AND a.country = 'UK'
			AND Up_BB = 0 
			AND a.rand_num <= 0.20 			----- Must be changed accordingly


			
	
--  3.1 Fibre Broadband
			--Variables 
				-- Fibre_eligible
				-- Up_Fibre
	
		DROP TABLE  IF EXISTS Fibre_temp; 
		SELECT account_number
			, base_dt
		INTO Fibre_temp
		FROM CS_Raw;
		
		COMMIT;
		CREATE HG 	INDEX id1 ON Fibre_temp(account_number);
		CREATE DATE INDEX id2 ON Fibre_temp(Base_dt);
		COMMIT;
					
	    CALL Decisioning_procs.Add_Software_Orders('Fibre_temp','Base_Dt','BB_FIBRE_CAP','Account_Number','Drop and Replace','Order_BB_FIBRE_CAP_Added_In_Next_30d');
		CALL Decisioning_procs.Add_Software_Orders('Fibre_temp','Base_Dt','BB_FIBRE_UNLIMITED','Account_Number','Drop and Replace','Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d');
		CALL Decisioning_procs.Add_Software_Orders('Fibre_temp','Base_Dt','BB_FIBRE_UNLIMITED_PRO','Account_Number','Drop and Replace','Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d');
		CALL Decisioning_procs.Add_Software_Orders('Fibre_temp','Base_Dt','BB_FIBRE_CAP','Account_Number','Drop and Replace','Order_BB_FIBRE_CAP_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('Fibre_temp','Base_Dt','BB_FIBRE_UNLIMITED','Account_Number','Drop and Replace','Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('Fibre_temp','Base_Dt','BB_FIBRE_UNLIMITED_PRO','Account_Number','Drop and Replace','Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d');

			
		
		CREATE OR REPLACE  VIEW CS_Fibre
		AS 
		SELECT a.*
			, CASE WHEN 	a.dtv_active = 1
					AND     (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
					AND     b.Order_BB_FIBRE_CAP_Added_In_Last_30d               = 0
					AND     b.Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d         = 0
					AND     b.Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d     = 0
					AND     a.BB_Enter_HM_In_Last_30D                   = 0
					AND     (a.bb_active = 0 OR a.bb_product_holding is NULL)
					AND     (a.skyfibre_enabled = 'Y' 
						OR   a.skyfibre_estimated_enabled_date BETWEEN a.base_dt AND DATEADD(DAY, 28, a.base_dt))
					THEN 1 ELSE 0 END 				AS Fibre_eligible
			, CASE WHEN (Order_BB_FIBRE_CAP_Added_In_Next_30d           > 0
					OR 	 Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d     > 0
					OR   Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d > 0)  THEN 1 ELSE 0 END AS  Up_Fibre 
		FROM CS_Raw AS a 
		LEFT JOIN Fibre_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Fibre_eligible = 1 
			AND  a.country = 'UK';
		
		
		CREATE OR REPLACE VIEW CS_Fibre_over
		AS 
		SELECT a.*
			, CASE WHEN 	a.dtv_active = 1
					AND     (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
					AND     b.Order_BB_FIBRE_CAP_Added_In_Last_30d               = 0
					AND     b.Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d         = 0
					AND     b.Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d     = 0
					AND     a.BB_Enter_HM_In_Last_30D                   = 0
					AND     (a.bb_active = 0 OR a.bb_product_holding is NULL)
					AND     (a.skyfibre_enabled = 'Y' 
						OR   a.skyfibre_estimated_enabled_date BETWEEN a.base_dt AND DATEADD(DAY, 28, a.base_dt))
					THEN 1 ELSE 0 END 				AS Fibre_eligible
			, CASE WHEN (Order_BB_FIBRE_CAP_Added_In_Next_30d           > 0
					OR 	 Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d     > 0
					OR   Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d > 0)  THEN 1 ELSE 0 END AS  Up_Fibre 
		FROM CS_Raw AS a 
		LEFT JOIN Fibre_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Fibre_eligible = 1 
			AND  a.country = 'UK'
			AND Up_Fibre = 1 
		UNION 
		SELECT a.*
			, CASE WHEN 	a.dtv_active = 1
					AND     (a.DTV_Status_Code = 'AC' OR a.DTV_Status_Code IS NULL)
					AND     b.Order_BB_FIBRE_CAP_Added_In_Last_30d               = 0
					AND     b.Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d         = 0
					AND     b.Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d     = 0
					AND     a.BB_Enter_HM_In_Last_30D                   = 0
					AND     (a.bb_active = 0 OR a.bb_product_holding is NULL)
					AND     (a.skyfibre_enabled = 'Y' 
						OR   a.skyfibre_estimated_enabled_date BETWEEN a.base_dt AND DATEADD(DAY, 28, a.base_dt))
					THEN 1 ELSE 0 END 				AS Fibre_eligible
			, CASE WHEN (Order_BB_FIBRE_CAP_Added_In_Next_30d           > 0
					OR 	 Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d     > 0
					OR   Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d > 0)  THEN 1 ELSE 0 END AS  Up_Fibre 
		FROM CS_Raw AS a 
		LEFT JOIN Fibre_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Fibre_eligible = 1 
			AND a.country = 'UK'
			AND Up_Fibre = 0 
			AND a.rand_num <= 0.20 			----- Must be changed accordingly			
    
	
	--  3.4 Regrade Fibre Broadband
			--Variables 
				-- Fibre_RE_eligible
				-- Up_RE_Fibre
	
	CREATE OR REPLACE  VIEW CS_RE_fibre
		AS 
		SELECT a.*
			, CASE WHEN 	bb_active = 1
						AND Order_BB_FIBRE_CAP_Added_In_Last_30d               = 0
						AND Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d         = 0
						AND Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d     = 0
						AND UPPER(bb_product_holding) NOT LIKE '%FIBRE%'
						AND (skyfibre_enabled = 'Y'
						 OR  skyfibre_estimated_enabled_date BETWEEN base_dt AND DATEADD(DAY, 28, base_dt))
					THEN 1 ELSE 0 END 				AS Fibre_RE_eligible
			, CASE WHEN (Order_BB_FIBRE_CAP_Added_In_Next_30d           > 0
					OR 	 Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d     > 0
					OR   Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d > 0)  THEN 1 ELSE 0 END AS  Up_RE_Fibre 
		FROM CS_Raw AS a 
		LEFT JOIN Fibre_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Fibre_RE_eligible = 1 
			AND  a.country = 'UK';
		
		
		CREATE OR REPLACE VIEW CS_RE_fibre_over
		AS 
		SELECT a.*
			, CASE WHEN 	bb_active = 1
						AND Order_BB_FIBRE_CAP_Added_In_Last_30d               = 0
						AND Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d         = 0
						AND Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d     = 0
						AND UPPER(bb_product_holding) NOT LIKE '%FIBRE%'
						AND (skyfibre_enabled = 'Y'
						 OR  skyfibre_estimated_enabled_date BETWEEN base_dt AND DATEADD(DAY, 28, base_dt))
					THEN 1 ELSE 0 END 				AS Fibre_RE_eligible
			, CASE WHEN (Order_BB_FIBRE_CAP_Added_In_Next_30d           > 0
					OR 	 Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d     > 0
					OR   Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d > 0)  THEN 1 ELSE 0 END AS  Up_RE_Fibre 
		FROM CS_Raw AS a 
		LEFT JOIN Fibre_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Fibre_RE_eligible = 1 
			AND  a.country = 'UK'
			AND Up_RE_Fibre = 1 
		UNION 
		SELECT a.*
			, CASE WHEN 	bb_active = 1
						AND Order_BB_FIBRE_CAP_Added_In_Last_30d               = 0
						AND Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d         = 0
						AND Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d     = 0
						AND UPPER(bb_product_holding) NOT LIKE '%FIBRE%'
						AND (skyfibre_enabled = 'Y'
						 OR  skyfibre_estimated_enabled_date BETWEEN base_dt AND DATEADD(DAY, 28, base_dt))
					THEN 1 ELSE 0 END 				AS Fibre_RE_eligible
			, CASE WHEN (Order_BB_FIBRE_CAP_Added_In_Next_30d           > 0
					OR 	 Order_BB_FIBRE_UNLIMITED_Added_In_Next_30d     > 0
					OR   Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Next_30d > 0)  THEN 1 ELSE 0 END AS  Up_RE_Fibre 
		FROM CS_Raw AS a 
		LEFT JOIN Fibre_temp AS b ON a.account_number = b.account_number AND a.base_dt = b.base_dt
		WHERE Fibre_RE_eligible = 1 
			AND a.country = 'UK'
			AND Up_RE_Fibre = 0 
			AND a.rand_num <= 0.20; 		/*	----- Must be changed accordingly			*/
	
	
	
		--  3.5 Skystore rentals
			--Variables 
				-- model_type: Lapsed or never rented
				-- Up_rental
				-- Rental_eligible
	
	Movies_Purchases_In_Next_30d
	
		CREATE OR REPLACE VIEW CS_rentals
		AS 
		SELECT a.*
			, CASE 	WHEN Movies_Purchases_Ever = 0 
					 OR Movies_Purchases_Ever IS NULL THEN 'Never rented'
					WHEN Movies_Purchases_In_Last_90d = 0 THEN 'Lapsed customers'
					ELSE 'Active Customer' AS model_type
			, CASE WHEN Movies_Purchases_In_Last_90d = 0 THEN 1 ELSE 0 END AS Rental_eligible
			, CASE WHEN Movies_Purchases_In_Next_30d >=1 THEN 1 ELSE 0 END AS Up_rental
		FROM CS_raw
		WHERE 	country = 'UK'
			AND dtv_active = 1
			AND Rental_eligible = 1; 

			
		CREATE OR REPLACE VIEW CS_rentals_over
		AS 
		SELECT a.*
			, CASE 	WHEN Movies_Purchases_Ever = 0 
					 OR Movies_Purchases_Ever IS NULL THEN 'Never rented'
					WHEN Movies_Purchases_In_Last_90d = 0 THEN 'Lapsed customers'
					ELSE 'Active Customer' AS model_type
			, CASE WHEN Movies_Purchases_In_Last_90d = 0 THEN 1 ELSE 0 END AS Rental_eligible
			, CASE WHEN Movies_Purchases_In_Next_30d >=1 THEN 1 ELSE 0 END AS Up_rental
		FROM CS_raw		WHERE 	country = 'UK'
			AND dtv_active = 1
			AND Rental_eligible = 1 
			AND Up_rental = 1 
		UNION 
		SELECT a.*	
			, CASE 	WHEN Movies_Purchases_Ever = 0 
					 OR  Movies_Purchases_Ever IS NULL THEN 'Never rented'
					WHEN Movies_Purchases_In_Last_90d = 0 THEN 'Lapsed customers'
					ELSE 'Active Customer' AS model_type
			, CASE WHEN Movies_Purchases_In_Last_90d = 0 THEN 1 ELSE 0 END AS Rental_eligible
			, CASE WHEN Movies_Purchases_In_Next_30d >=1 THEN 1 ELSE 0 END AS Up_rental
		FROM CS_raw
		WHERE 	country = 'UK'
			AND dtv_active = 1
			AND Rental_eligible = 1 
			AND Up_rental = 0
			AND rand_num <= 0.20; 				/*	----- Must be changed accordingly			*/


	
	
		--  3.6 Buy and keep
			--Variables 
				-- model_type: Lapsed or never bought
				-- Up_BAK
				-- BAK_eligible
	
		CREATE OR REPLACE VIEW CS_BAK
		AS 
		SELECT a.*
			, CASE 	WHEN BnK_Purchases_Ever = 0 
					 OR  BnK_Purchases_Ever IS NULL THEN 'Never rented'
					WHEN BnK_Purchases_In_Last_90d = 0 THEN 'Lapsed customers'
					ELSE 'Active Customer' AS model_type
			, CASE WHEN BnK_Purchases_In_Last_90d = 0 THEN 1 ELSE 0 END AS BAK_eligible
			, CASE WHEN BnK_Purchases_In_Next_30d >=1 THEN 1 ELSE 0 END AS Up_BAK
		FROM CS_raw
		WHERE 	country = 'UK'
			AND dtv_active = 1
			AND BAK_eligible = 1; 

			
		CREATE OR REPLACE VIEW CS_BAK_over
		AS 
		SELECT a.*
			, CASE 	WHEN BnK_Purchases_Ever = 0 
					 OR  BnK_Purchases_Ever IS NULL THEN 'Never rented'
					WHEN BnK_Purchases_In_Last_90d = 0 THEN 'Lapsed customers'
					ELSE 'Active Customer' AS model_type
			, CASE WHEN BnK_Purchases_In_Last_90d = 0 THEN 1 ELSE 0 END AS BAK_eligible
			, CASE WHEN BnK_Purchases_In_Next_30d >=1 THEN 1 ELSE 0 END AS Up_BAK
		FROM CS_raw
		WHERE 	country = 'UK'
			AND dtv_active = 1
			AND BAK_eligible = 1 
			AND Up_BAK = 1 
		UNION 
		SELECT a.*	
			, CASE 	WHEN BnK_Purchases_Ever = 0 
					 OR  BnK_Purchases_Ever IS NULL THEN 'Never rented'
					WHEN BnK_Purchases_In_Last_90d = 0 THEN 'Lapsed customers'
					ELSE 'Active Customer' AS model_type
			, CASE WHEN BnK_Purchases_In_Last_90d = 0 THEN 1 ELSE 0 END AS BAK_eligible
			, CASE WHEN BnK_Purchases_In_Next_30d >=1 THEN 1 ELSE 0 END AS Up_BAK
		FROM CS_raw
		WHERE 	country = 'UK'
			AND dtv_active = 1
			AND BAK_eligible = 1 
			AND Up_BAK = 0
			AND rand_num <= 0.20; 			/*	----- Must be changed accordingly			*/
									
/*		--  3.7 Sky Q
			--Variables 
				-- UP_SkyQ
				-- UP_skyQ_MS
				-- SkyQ_eligible
		
		--------- SkyQ_eligible ----
		---- 	Accounts that have Sky Q installed	*/
		
		DROP TABLE IF EXISTS sky_q_elig;
		DROP TABLE IF EXISTS sky_q_up;
		SELECT  stb.account_number
				,MIN(CASE 	WHEN x_description  in ('Sky Q Silver','Sky Q Mini','Sky Q 2TB box','Sky Q','Sky Q 1TB box') THEN 0 	--- Known box descriptions
							WHEN UPPER(x_description) LIKE '%SKY Q%'	THEN 0														--- Any other new model
							ELSE 1 END ) 		AS PrimaryBoxType
				, base.base_dt
		INTO sky_q_elig
		FROM cust_set_top_box AS stb
		JOIN CS_Raw AS base ON stb.account_number = base.account_number AND stb.created_dt <= base.base_dt 
		WHERE   base.account_number IS NOT NULL
		GROUP BY  stb.account_number
				, base.base_dt;
							
		COMMIT;
		CREATE HG INDEX id1 	ON sky_q_elig (account_number);
		CREATE DATE INDEX id2 	ON sky_q_elig (base_dt);
	
		/* Upsell Flag */
		SELECT  stb.account_number
				,MAX(CASE WHEN x_description  in ('Sky Q Silver','Sky Q Mini','Sky Q 2TB box','Sky Q','Sky Q 1TB box') THEN 1 --- Known box descriptions
						WHEN UPPER(x_description) LIKE '%SKY Q%'	THEN 1													--- Any other new model
						ELSE 0 END ) 		AS PrimaryBoxType
				, base.base_dt
		INTO sky_q_up
		FROM cust_set_top_box AS stb
		JOIN CS_Raw AS base ON stb.account_number = base.account_number AND stb.created_dt BETWEEN DATEADD(DAY, 1 ,base_dt) AND DATEADD(MONTH, 1,base_dt) -- Installations within the next 30 days after the observation date
		WHERE   base.account_number IS NOT NULL
		GROUP BY  stb.account_number
				, base.base_dt;
		
		COMMIT;
		CREATE HG INDEX id1 	ON sky_q_up (account_number);
		CREATE DATE INDEX id2 	ON sky_q_up (base_dt);
		COMMIT;
	
	
		
		CREATE OR REPLACE VIEW CS_SkyQ
		AS 
		SELECT a.*
				, CASE WHEN b.PrimaryBoxType = 1 
						OR  DATEDIFF(YEAR, a.DTV_Last_Activation_Dt, base_dt) >= 15	
						OR  a.DTV_active = 0 
						THEN 0 ELSE 1 END 			AS SkyQ_eligible
				, c.PrimaryBoxType AS UP_SkyQ
		FROM 	CS_Raw AS a 
		LEFT JOIN 	sky_q_elig AS b ON a.account_number = b.account_number AND b.base_dt = a.base_dt 
		LEFT JOIN 	sky_q_up AS c 	ON a.account_number = a.account_number AND a.base_dt = c.base_dt ;
			
			
	
/*
			UPDATE CS_Raw a
			SET a.Order_MS_added_next_60d  = b.Order_MULTISCREEN_PLUS_Added_In_Next_30d
				, a.Order_MS_removed_next_60d = b.Order_MULTISCREEN_PLUS_Removed_In_Next_30d
			FROM CS_Raw  as a 
			join CS_Raw  as b on a.account_number = b.account_number  ANd a.base_dt_2 = b.base_dt					
			
			UPDATE CS_Raw
			SET    UP_skyQ_MS = 1
			FROM   CS_Raw AS base
			JOIN  	#sky_q_up AS b ON b.account_number = base.account_number AND b.base_dt = base.base_dt  AND PrimaryBoxType = 1 
			WHERE  (Order_MULTISCREEN_PLUS_Added_In_Next_30d +  Order_MS_added_next_60d )- (Order_MULTISCREEN_PLUS_Removed_In_Next_30d + Order_MS_removed_next_60d) > 0
	*/
	
	
/*		--  3.8 Sky Mobile
			--Variables 
		PLACEHOLDER 
		
		*/
	
/*		-- 3.9 Sky Cinema
			--Variables 
				-- UP_movies			
				-- movies_eligible			*/
	
		CREATE OR REPLACE VIEW CS_cinema
		AS SELECT 
			a.* 
			, CASE WHEN Movies_Active = 0 
				AND Order_Movies_Added_In_Last_30d = 0 THEN 1 ELSE 0 END 	AS movies_eligible 
			, CASE WHEN movies_eligible = 1 												
					AND Order_Movies_Added_In_Next_30d > 0
					AND Order_Movies_Added_In_Next_30d > Order_Movies_Removed_In_Next_30d
					AND TA_next_30d = 0	THEN 1 ELSE 0 END 					AS UP_movies
		FROM CS_Raw AS a 
		WHERE 	a.country = 'UK'
			AND a.dtv_active = 1
			AND movies_eligible = 1;
	
	
		CREATE OR REPLACE VIEW CS_cinema_over
		AS SELECT 
			a.* 
			, CASE WHEN Movies_Active = 0 
				AND Order_Movies_Added_In_Last_30d = 0 THEN 1 ELSE 0 END 	AS movies_eligible 
			, CASE WHEN movies_eligible = 1 												
					AND Order_Movies_Added_In_Next_30d > 0
					AND Order_Movies_Added_In_Next_30d > Order_Movies_Removed_In_Next_30d
					AND TA_next_30d = 0	THEN 1 ELSE 0 END 					AS UP_movies
		FROM CS_Raw AS a 
		WHERE 	a.country = 'UK'
			AND a.dtv_active = 1
			AND movies_eligible = 1
			AND UP_movies = 1 
		UNION 
		SELECT 
			a.* 
			, CASE WHEN Movies_Active = 0 
				AND Order_Movies_Added_In_Last_30d = 0 THEN 1 ELSE 0 END 	AS movies_eligible 
			, CASE WHEN movies_eligible = 1 												
					AND Order_Movies_Added_In_Next_30d > 0
					AND Order_Movies_Added_In_Next_30d > Order_Movies_Removed_In_Next_30d
					AND TA_next_30d = 0	THEN 1 ELSE 0 END 					AS UP_movies
		FROM CS_Raw AS a 
		WHERE 	a.country = 'UK'
			AND a.dtv_active = 1
			AND movies_eligible = 1
			AND UP_movies = 0 
			AND rand_num <= 0.20;				/*	----- Must be changed accordingly			*/
			
			
/*		-- 3.9 Sky Sports
			--Variables 
				-- UP_sports			
				-- sports_eligible			*/
	
		CREATE OR REPLACE VIEW CS_sports
		AS SELECT 
			a.* 
			, CASE WHEN sports_Active = 0 
				AND Order_Sports_Added_In_Last_30d = 0 THEN 1 ELSE 0 END 	AS sports_eligible 
			, CASE WHEN sports_eligible = 1 												
					AND Order_Sports_Added_In_Next_30d > 0
					AND Order_Sports_Added_In_Next_30d > Order_Sports_Removed_In_Next_30d
					AND TA_next_30d = 0	THEN 1 ELSE 0 END 					AS UP_sports
		FROM CS_Raw AS a 
		WHERE 	a.country = 'UK'
			AND a.dtv_active = 1
			AND sports_eligible = 1;
		
	
	
	
	
	
		CREATE OR REPLACE VIEW CS_sports_over
		AS
		SELECT 
			a.* 
			, CASE WHEN sports_Active = 0 
				AND Order_Sports_Added_In_Last_30d = 0 THEN 1 ELSE 0 END 	AS sports_eligible 
			, CASE WHEN sports_eligible = 1 												
					AND Order_Sports_Added_In_Next_30d > 0
					AND Order_Sports_Added_In_Next_30d > Order_Sports_Removed_In_Next_30d
					AND TA_next_30d = 0	THEN 1 ELSE 0 END 					AS UP_sports
		FROM CS_Raw AS a 
		WHERE 	a.country = 'UK'
			AND a.dtv_active = 1
			AND sports_eligible = 1
			AND UP_sports = 1 
		UNION 
				SELECT 
			a.* 
			, CASE WHEN sports_Active = 0 
				AND Order_Sports_Added_In_Last_30d = 0 THEN 1 ELSE 0 END 	AS sports_eligible 
			, CASE WHEN sports_eligible = 1 												
					AND Order_Sports_Added_In_Next_30d > 0
					AND Order_Sports_Added_In_Next_30d > Order_Sports_Removed_In_Next_30d
					AND TA_next_30d = 0	THEN 1 ELSE 0 END 					AS UP_sports
		FROM CS_Raw AS a 
		WHERE 	a.country = 'UK'
			AND a.dtv_active = 1
			AND sports_eligible = 1
			AND UP_sports = 0
			AND rand_num <= 0.20; 
			
					
			
    --  4.0 Permissions to public
        
        GRANT SELECT ON CS_Raw TO PUBLIC;
        GRANT SELECT ON CS_boxsets TO PUBLIC;
        GRANT SELECT ON CS_boxsets_over TO PUBLIC;
        GRANT SELECT ON CS_BB TO PUBLIC;
        GRANT SELECT ON CS_BB_over TO PUBLIC;
        GRANT SELECT ON CS_Fibre TO PUBLIC;
        GRANT SELECT ON CS_Fibre_over TO PUBLIC;
        GRANT SELECT ON CS_RE_fibre TO PUBLIC;
        GRANT SELECT ON CS_RE_fibre_over TO PUBLIC;
        GRANT SELECT ON CS_rentals TO PUBLIC;
        GRANT SELECT ON CS_rentals_over TO PUBLIC;
        GRANT SELECT ON CS_BAK TO PUBLIC;
        GRANT SELECT ON CS_BAK_over TO PUBLIC;
		GRANT SELECT ON CS_cinema TO PUBLIC;
        GRANT SELECT ON CS_cinema_over TO PUBLIC;
        GRANT SELECT ON CS_sports TO PUBLIC;
        GRANT SELECT ON CS_sports_over TO PUBLIC;
        GRANT SELECT ON CS_mobile TO PUBLIC;
        GRANT SELECT ON CS_mobile_over TO PUBLIC;









/*
	--------------------------------------------------------------------------------------		
	--------------------------------------------------------------------------------------		
		
	--------- Mobile_eligible
			SELECT c.account_number
                , MAX(a.prod_earliest_mobile_ordered_dt) dt 
				, base_dt
            INTO #mobile
			FROM cust_single_mobile_account_view    AS a
			JOIN cust_single_mobile_view            AS b ON a.account_number = b.account_number
			JOIN cust_single_account_view           AS c ON a.portfolio_id = c.acct_fo_portfolio_id
			JOIN CS_Raw 							AS x ON x.account_number = c.account_number AND a.prod_earliest_mobile_ordered_dt <= base_dt
			GROUP BY c.account_number, base_dt 
						
			COMMIT
			CREATE HG INDEX id1 	ON #mobile (account_number)
			CREATE DATE INDEX id2 	ON #mobile (base_dt)
			CREATE DTTM INDEX id3 	ON #mobile (dt)
			COMMIT
			
			UPDATE CS_Raw a
			SET Mobile_eligible	= CASE WHEN cps.account_number IS NULL THEN 1 ELSE 0 END 
			FROM CS_Raw a
			LEFT JOIN #mobile AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
			
			DROP TABLE #mobile
			COMMIT
			MESSAGE CAST(now() as timestamp)||' | 7' TO CLIENT
		GO

		----- Up_mobile
			
			SELECT c.account_number
                , MAX(a.prod_earliest_mobile_ordered_dt) dt 
				, base_dt
            INTO #mobile
			FROM cust_single_mobile_account_view    AS a
			JOIN cust_single_mobile_view            AS b ON a.account_number = b.account_number
			JOIN cust_single_account_view           AS c ON a.portfolio_id = c.acct_fo_portfolio_id
			JOIN CS_Raw 							AS x ON x.account_number = c.account_number 
														AND a.prod_earliest_mobile_ordered_dt BETWEEN DATEADD(DAY, 1 ,base_dt) AND DATEADD(MONTH, 1,base_dt) 
			GROUP BY c.account_number, base_dt 
						
			COMMIT
			CREATE HG INDEX id1 	ON #mobile (account_number)
			CREATE DATE INDEX id2 	ON #mobile (base_dt)
			CREATE DTTM INDEX id3 	ON #mobile (dt)
			COMMIT
			
			UPDATE CS_Raw a
			SET Up_mobile = CASE WHEN cps.account_number IS NOT NULL THEN 1 ELSE 0 END 
			FROM CS_Raw a
			LEFT JOIN #mobile AS cps ON a.account_number = cps.account_number AND a.base_dt = cps.base_dt
			
			DROP TABLE #mobile
			COMMIT

		--------------------------------------******************************************----------------------------
		--------------------------------------******************************************----------------------------
		--------------------------------------******************************************----------------------------
		--------------------------------------******************************************----------------------------
*/
