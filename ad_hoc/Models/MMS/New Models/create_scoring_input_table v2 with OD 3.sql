
CREATE OR REPLACE VARIABLE @start_date DATE;
CREATE OR REPLACE VARIABLE @end_date DATE;
CREATE OR REPLACE VARIABLE @end_date_lag DATE;
CREATE OR REPLACE VARIABLE @scoring_week CHAR(6);
SET @scoring_week = '201710';
SET @start_date   = '2017-01-01'; /* Set to be before the scoring_week */
SET @end_date     = '2018-01-01'; /* Set to be after the scoring_week */
SET @end_date_lag = '2018-01-01'; /* Set to be after the scoring_week */

/*  1.1 Create initial table */

    DROP TABLE  IF EXISTS #Qtr_Wk_End_Dts;
    SELECT calendar_date
    INTO #Qtr_Wk_End_Dts
    FROM sky_calendar
    WHERE subs_last_day_of_week='Y'
            AND subs_week_and_year=@scoring_week;
    COMMIT;

    CREATE lf INDEX idx_1 ON #Qtr_Wk_End_Dts(Calendar_Date);

    DROP TABLE IF EXISTS CS_raw_test;
    SELECT Cast(wk.calendar_date AS date) Base_Dt,account_number /*,product_holding DTV_Product_Holding,status_code as DTV_Status_Code*/
    INTO CS_raw_test
    FROM #Qtr_Wk_End_Dts wk
         INNER JOIN
         cust_subs_hist asr
         ON wk.calendar_date BETWEEN effective_from_dt AND effective_to_dt - 1
            AND subscription_sub_type = 'DTV Primary Viewing'
            AND status_code IN ('AB','AC','PC')
    GROUP BY Base_Dt,account_number
    UNION
    SELECT Cast(wk.calendar_date AS date) Base_Dt,account_number /*,product_holding BB_Product_Holding,status_code as BB_Status_Code*/
    FROM #Qtr_Wk_End_Dts wk
         INNER JOIN
         Decisioning.Active_Subscriber_Report asr
         ON wk.calendar_date BETWEEN effective_from_dt AND effective_to_dt - 1
            AND subscription_sub_type = 'Broadband'
    GROUP BY Base_Dt,account_number;
    COMMIT;

    CREATE HG INDEX id1 ON CS_raw_test (account_number);
    CREATE DATE INDEX iddt ON CS_raw_test (base_dt);

/*  1.2 Permissions to public */
    
    GRANT SELECT ON CS_raw_test TO PUBLIC;

/*  1.3 Add Target flags and rand_num, but keep NULL */

    ALTER TABLE CS_raw_test
    ADD (Up_BB             BIT            DEFAULT 0 
        ,Up_Fibre          BIT            DEFAULT 0 
        ,Regrade_Fibre     BIT            DEFAULT 0 
        ,Up_Box_Sets       BIT            DEFAULT 0
        ,rand_num          DECIMAL(22,20) DEFAULT NULL
        );
    COMMIT;



/*  2.1 Add Model T vars */

    CALL Decisioning_procs.Add_Subs_Calendar_Fields('CS_raw_test','Base_Dt');
	
    CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_raw_test','Base_Dt','DTV');
    CALL Decisioning_procs.Add_Active_Subscriber_Product_Holding('CS_raw_test','Base_Dt','BB');
	
	CALL Decisioning_Procs.Add_Activations_DTV('CS_raw_test','Base_Dt');
    CALL Decisioning_Procs.Add_Activation_BB('CS_raw_test','Base_Dt');
	
	CALL Decisioning_procs.Add_Demographics_To_Base('CS_raw_test','Base_Dt');
    CALL Decisioning_procs.Add_Offers_Software('CS_raw_test','Base_Dt','DTV');
    CALL Decisioning_procs.Add_Offers_Software('CS_raw_test','Base_Dt','BB');
    CALL Decisioning_procs.Add_Broadband_Postcode_Exchange_To_Base('CS_raw_test');
	CALL Decisioning_procs.Add_Fibre_Areas('CS_raw_test');
	CALL Decisioning_Procs.Add_Turnaround_Attempts('CS_raw_test','Base_Dt','TA Events');
	CALL Decisioning_procs.Add_BB_Provider('CS_raw_test','Base_Dt');
	CALL Decisioning_Procs.Add_Active_Subscriber_Product_Holding('CS_raw_test','Base_Dt','HD');
		

		CALL Decisioning_procs.Add_Software_Orders('CS_raw_test','Base_Dt','BB_UNLIMITED','Account_Number','Drop and Replace','Order_BB_UNLIMITED_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('CS_raw_test','Base_Dt','BB_LITE','Account_Number','Drop and Replace','Order_BB_LITE_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('CS_raw_test','Base_Dt','BB_FIBRE_CAP','Account_Number','Drop and Replace','Order_BB_FIBRE_CAP_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('CS_raw_test','Base_Dt','BB_FIBRE_UNLIMITED','Account_Number','Drop and Replace','Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('CS_raw_test','Base_Dt','BB_FIBRE_UNLIMITED_PRO','Account_Number','Drop and Replace','Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('CS_raw_test','Base_Dt','FAMILY','Account_Number','Drop and Replace','Order_FAMILY_Added_In_Last_30d');
		CALL Decisioning_procs.Add_Software_Orders('CS_raw_test','Base_Dt','HD_BASIC','Account_Number','Drop and Replace','Order_HD_BASIC_Added_In_Last_30d');

		CALL Decisioning_Procs.Add_Churn_DTV ('CS_raw_test','Base_Dt');
		CALL Decisioning_Procs.Add_Churn_BB ('CS_raw_test','Base_Dt');


		CALL Decisioning_Procs.Add_PL_Entries_DTV('CS_raw_test','Base_Dt');
		CALL Decisioning_Procs.Add_PL_Entries_BB('CS_raw_test','Base_Dt');
		
		CALL Decisioning_procs.Add_contract_details ('CS_raw_test','Base_Dt','DTV');
		CALL Decisioning_procs.Add_contract_details ('CS_raw_test','Base_Dt','BB');
		
    --CALL Decisioning_procs.Add_Simple_Segment_To_Base('CS_raw_test','Base_Dt');


/*  4.1 Create Eligiblility flags */

    ALTER TABLE CS_raw_test
    ADD (BB_eligible       BIT DEFAULT 0 
        ,Fibre_UP_eligible BIT DEFAULT 0 
        ,Fibre_RE_eligible BIT DEFAULT 0 
        ,Boxset_eligible   BIT DEFAULT 0 
        );

    /*  4.1.1 Boxsets */

        UPDATE      CS_raw_test
        SET         Boxset_eligible = 1 
        WHERE       dtv_active = 1
        AND         (DTV_Status_Code = 'AC' OR DTV_Status_Code IS NULL)
        AND         Order_FAMILY_Added_In_Last_30d   = 0
        AND         Order_HD_BASIC_Added_In_Last_30d = 0
        AND         (DTV_Product_Holding LIKE '%Variety%' OR DTV_Product_Holding LIKE '%Original%')
        AND         HD_active = 0;

    /*  4.1.2 Non-Fibre Broadband */
        
        UPDATE      CS_raw_test
        SET         BB_eligible = 1 
        WHERE       dtv_active = 1
        AND         (DTV_Status_Code = 'AC' OR DTV_Status_Code IS NULL)
        AND         Order_BB_UNLIMITED_Added_In_Last_30d  = 0
        AND         Order_BB_LITE_Added_In_Last_30d       = 0
        AND         BB_Enter_HM_In_Last_30D    = 0
        AND         (bb_active = 0 OR bb_product_holding is NULL)
        AND         Exchange_Status = 'ONNET';

    /*  4.1.3 Fibre Broadband */
    
        UPDATE      CS_raw_test
        SET         Fibre_UP_eligible = 1
        WHERE       dtv_active = 1
        AND         (DTV_Status_Code = 'AC' OR DTV_Status_Code IS NULL)
        AND         Order_BB_FIBRE_CAP_Added_In_Last_30d               = 0
        AND         Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d         = 0
        AND         Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d     = 0
        AND         BB_Enter_HM_In_Last_30D                   = 0
        AND         (bb_active = 0 OR bb_product_holding is NULL)
        AND         (skyfibre_enabled = 'Y'
            OR       skyfibre_estimated_enabled_date BETWEEN base_dt AND DATEADD(DAY, 28, base_dt));

    /*  4.1.4 Regrade Fibre Broadband */
    
        UPDATE      CS_raw_test
        SET         Fibre_RE_eligible = 1
        WHERE       bb_active = 1
        AND         Order_BB_FIBRE_CAP_Added_In_Last_30d               = 0
        AND         Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d         = 0
        AND         Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d     = 0
        AND         UPPER(bb_product_holding) NOT LIKE '%FIBRE%' 
        AND         (skyfibre_enabled = 'Y'
            OR       skyfibre_estimated_enabled_date BETWEEN base_dt AND DATEADD(DAY, 28, base_dt));



/*  5.1 PPV Sports Events */

    ALTER TABLE CS_raw_test
    ADD (num_sports_events      INT          DEFAULT NULL
        ,sports_downgrade_date  DATE         DEFAULT NULL 
        ,Sports_Tenure          VARCHAR(20)  DEFAULT NULL 
        ,movies_downgrade_date  DATE         DEFAULT NULL 
        ,Movies_Tenure          VARCHAR(20)  DEFAULT NULL 
        );


    DROP TABLE  IF EXISTS #temp_ppv;
    SELECT       a.account_number
                ,a.base_dt
                ,sum(CASE WHEN ppv_viewed_dt BETWEEN dateadd(mm,-12,base_dt) AND base_dt AND ppv_service='EVENT'
                          /*AND  ppv_genre = 'BOXING, FOOTBALL or WRESTLING'*/
                          AND ppv_cancelled_dt = '9999-09-09' THEN 1 ELSE 0 END) AS num_sport_events_12m
    INTO        #temp_ppv
    FROM        CS_raw_test a
    INNER JOIN  CUST_PRODUCT_CHARGES_PPV b
    ON          a.account_number   = b.account_number
    WHERE       b.ppv_cancelled_dt = '9999-09-09'
       AND      b.ppv_viewed_dt   <= base_dt
       AND      b.ppv_viewed_dt   >= (base_dt-365)
    GROUP BY     a.account_number
                ,a.base_dt;

    UPDATE      CS_raw_test as a
    SET         a.num_sports_events = b.num_sport_events_12m
    FROM        #temp_ppv as b
    WHERE       a.account_number = b.account_number
    AND         a.base_dt = b.base_dt;

    UPDATE      CS_raw_test
    SET          a.sports_downgrade_date = b.sports_downgrade_date 
                ,a.Sports_Tenure         = b.Sports_Tenure 
                ,a.movies_downgrade_date = b.movies_downgrade_date 
                ,a.Movies_Tenure         = b.Movies_Tenure
    FROM        CS_raw_test As a 
    JOIN        citeam.CUST_FCAST_WEEKLY_BASE AS b 
    ON          a.account_number = b.account_number 
    AND         end_date BETWEEN DATEADD(DAY, -6, a.base_dt ) AND a.base_dt;

/*  5.2 OD fields */

    ALTER TABLE CS_raw_test
    ADD (OD_Last_3M             INT DEFAULT NULL
        ,OD_Last_12M            INT DEFAULT NULL 
        ,OD_Months_since_Last   INT DEFAULT NULL 
        );

    DROP TABLE  IF EXISTS #temp_od;
    SELECT       a.account_number
                ,base_dt
                ,MAX(last_modified_dt)         AS date_last_od
                ,OD_Months_since_Last = CASE   WHEN DATEDIFF(MONTH, date_last_od , base_dt ) > 15 THEN 16 ELSE  DATEDIFF(MONTH, date_last_od , base_dt )  END 
                ,SUM(CASE WHEN cast(last_modified_dt AS DATE) BETWEEN dateadd(mm, - 3, base_dt)    AND base_dt THEN 1 ELSE 0 END) AS OD_Last_3M
                ,SUM(CASE WHEN cast(last_modified_dt AS DATE) BETWEEN dateadd(mm, - 12, base_dt)   AND base_dt THEN 1 ELSE 0 END) AS OD_Last_12M
    INTO        #temp_od
    FROM        CS_raw_test a
    INNER JOIN  CUST_ANYTIME_PLUS_DOWNLOADS b ON a.account_number = b.account_number
    WHERE       b.last_modified_dt <= base_dt
    GROUP BY    a.account_number, base_dt;

    CREATE HG Index id1 ON #temp_od(account_number);
    CREATE DATE Index id2 ON #temp_od(base_dt);

    UPDATE      CS_raw_test a
    SET          a.OD_Last_3M           = b.OD_Last_3M
                ,a.OD_Last_12M          = b.OD_Last_12M
                ,a.OD_Months_since_Last = b.OD_Months_since_Last
    FROM        #temp_od b
    WHERE       a.account_number = b.account_number;

/*  5.3 Recode DTV product holding */

    ALTER TABLE CS_raw_test 
        ADD DTV_product_holding_recode VARCHAR(40);
                                
    UPDATE      CS_raw_test 
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

/*  5.4 Create TA reason flags */

    ALTER TABLE CS_raw_test
    ADD         (_1st_TA_reason_flag VARCHAR(15)
                ,last_TA_reason_flag VARCHAR(15));

    UPDATE      CS_raw_test
    SET          _1st_TA_reason_flag = CASE WHEN _1st_TA_reason IS NULL THEN 'No reason given' ELSE 'Reason given' END
                ,last_TA_reason_flag = CASE WHEN last_TA_reason IS NULL THEN 'No reason given' ELSE 'Reason given' END;

/*  5.5 Cleanup */

    /*  5.5.1 Age */

        UPDATE      CS_raw_test
        SET          Age = CASE WHEN Age BETWEEN 1916 AND 1999  THEN 2017-Age
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
                               WHEN Age BETWEEN 18 AND 101     THEN Age
                               ELSE NULL
                           END;

    /*  5.5.2 Missing values */

        UPDATE      CS_raw_test
        SET          ADSL_Enabled             = CASE WHEN ADSL_Enabled             IS NULL                         THEN 'Unknown' ELSE ADSL_Enabled             END
                  --  ,BB_RTM                   = CASE WHEN BB_RTM                   IS NULL OR BB_RTM = ''          THEN 'Unknown' ELSE BB_RTM                   END
                    ,Exchange_Status          = CASE WHEN Exchange_Status          IS NULL                         THEN 'Unknown' ELSE Exchange_Status          END
                  --,Simple_Segment           = CASE WHEN Simple_Segment           IS NULL OR  Simple_Segment = '' THEN 'Unknown' ELSE Simple_Segment           END
                    ,DTV_CusCan_Churns_Ever   = CASE WHEN DTV_CusCan_Churns_Ever   IS NULL                         THEN 0         ELSE DTV_CusCan_Churns_Ever   END
                    ,DTV_Pending_cancels_ever = CASE WHEN DTV_Pending_cancels_ever IS NULL                         THEN 0         ELSE DTV_Pending_cancels_ever END
                    ,DTV_SysCan_Churns_Ever   = CASE WHEN DTV_SysCan_Churns_Ever   IS NULL                         THEN 0         ELSE DTV_SysCan_Churns_Ever   END
                    ,_1st_TA_outcome          = CASE WHEN _1st_TA_outcome          IS NULL                         THEN 'No TA'   ELSE _1st_TA_outcome          END
                    ,last_TA_outcome          = CASE WHEN last_TA_outcome          IS NULL                         THEN 'No TA'   ELSE last_TA_outcome          END;
    
    /*  5.5.3 Turn dates into days */

        ALTER TABLE CS_raw_test
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

        UPDATE      CS_raw_test
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

/*  5.6 NOW TV Variables */

    /* Make sure mapped_account_numbers table is updated, check with Robert Barker from the NOW TV Analytics team*/

    ALTER TABLE CS_raw_test 
    ADD         (accountid         BIGINT
                ,NTV_Ents_Last_30D BIT DEFAULT 0
                ,NTV_Ents_Last_90D BIT DEFAULT 0);


    UPDATE      CS_raw_test
    SET         accountid = mapped.accountid
    FROM        CS_raw_test csr
    INNER JOIN  tva02.mapped_account_numbers mapped
    ON          csr.account_number = mapped.account_number;

    CREATE HG INDEX id_accid ON CS_raw_test (accountid);

    UPDATE      CS_raw_test csr
    SET         NTV_Ents_Last_30D = 1
    FROM        citeam.nowtv_accounts_ents ntvents
    WHERE       csr.accountid = ntvents.accountid
    AND         ntvents.subscriber_this_period = 1
    AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) >= -30
    AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) <= 0
    AND         ntvents.accountid IS NOT NULL;

    UPDATE      CS_raw_test csr
    SET         NTV_Ents_Last_90D = 1
    FROM        citeam.nowtv_accounts_ents ntvents
    WHERE       csr.accountid = ntvents.accountid
    AND         ntvents.subscriber_this_period = 1
    AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) >= -90
    AND         DATEDIFF(dd, csr.base_dt, ntvents.period_start_date) <= 0
    AND         ntvents.accountid IS NOT NULL;



/*  6.1 Create binning table */
    
    DROP TABLE IF EXISTS cs_binned;
    SELECT       account_number
                ,base_dt
                ,rand_num
                ,Boxset_eligible
                ,BB_eligible
                ,Fibre_UP_eligible
                ,Fibre_RE_eligible
                ,Up_Box_Sets
                ,Up_BB
                ,Up_Fibre
                ,Regrade_Fibre
                ,Age
                ,ADSL_Enabled
                ,BB_Product_Holding
  --              ,BB_RTM
                ,BB_Status_Code
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
                /* ,BT_Consumer_Market_Share */
                /* ,Sky_Consumer_Market_Share */
                /* ,TalkTalk_Consumer_Market_Share */
                /* ,Virgin_Consumer_Market_Share */
               -- ,Simple_Segment
                ,h_presence_of_young_person_at_address AS h_presence_of_young_person_at_ad

                ,DTV_Last_cuscan_churn
                ,DTV_Last_Activation
                ,DTV_Curr_Contract_Intended_End
                ,DTV_Curr_Contract_Start
                ,DTV_Last_SysCan_Churn
                ,Curr_Offer_Start_DTV
                ,Curr_Offer_Actual_End_DTV
                ,DTV_1st_Activation
                /* ,BB_Curr_Contract_Intended_End */
                /* ,BB_Curr_Contract_Start */
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

                ,Order_BB_UNLIMITED_Added_In_Last_30d           AS Ord_BB_UNL_L30d
                ,Order_BB_LITE_Added_In_Last_30d                AS Ord_BB_LITE_L30d
                ,Order_BB_FIBRE_CAP_Added_In_Last_30d           AS Ord_BB_FIBRE_CAP_L30d
                ,Order_BB_FIBRE_UNLIMITED_Added_In_Last_30d     AS Ord_BB_FIBRE_UNL_L30d
                ,Order_BB_FIBRE_UNLIMITED_PRO_Added_In_Last_30d AS Ord_BB_FIBRE_UNL_PRO_L30d

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
                          WHEN BB_Enter_3rd_Party_Ever >= 4 THEN 'ge4' END AS BB_Enter_3rd_Party_Ever_b
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
                          WHEN BB_Enter_HM_Ever >= 4 THEN 'ge4' END AS  BB_HomeMove_PL_Entry_Ever_b
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
         /*       ,CASE WHEN Offers_Applied_Lst_24Hrs_DTV  = 0 THEN '0'
                      WHEN Offers_Applied_Lst_24Hrs_DTV >= 1 THEN '1'
                      WHEN Offers_Applied_Lst_24Hrs_DTV >= 2 THEN '2'
                      WHEN Offers_Applied_Lst_24Hrs_DTV >= 3 THEN '3'
                      WHEN Offers_Applied_Lst_24Hrs_DTV >= 4 THEN 'ge4' END AS Offers_Applied_Lst_24Hrs_DTV_b */
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

                ,CASE WHEN DTV_Curr_Contract_Start_Dt IS     NULL AND DTV_Prev_Contract_Start_Dt                 IS NULL THEN 'A.NeverOnContract'
                      WHEN DTV_Curr_Contract_Start_Dt IS     NULL AND (base_dt-DTV_Prev_Contract_Actual_End_Dt)   >  180 THEN 'B.ExpiredContract(>6M)'
                      WHEN DTV_Curr_Contract_Start_Dt IS     NULL AND (base_dt-DTV_Prev_Contract_Actual_End_Dt)   <= 180 THEN 'C.ExpiredContract(<6M)'
                      WHEN DTV_Curr_Contract_Start_Dt IS NOT NULL AND (DTV_Curr_Contract_Intended_End_Dt-base_dt) <= 90  THEN 'D.ExpiringContract(<3M)'
                      WHEN DTV_Curr_Contract_Start_Dt IS NOT NULL AND (DTV_Curr_Contract_Intended_End_Dt-base_dt) <= 180 THEN 'E.ExpiringContract(<6M)'
                      WHEN DTV_Curr_Contract_Start_Dt IS NOT NULL AND (DTV_Curr_Contract_Intended_End_Dt-base_dt) >  180 THEN 'F.ExpiringContract(>6M)'
                      ELSE '' END AS DTV_contract_segment
                ,CASE WHEN BB_Curr_Contract_Start_Dt IS      NULL AND BB_Prev_Contract_Start_Dt                  IS NULL THEN 'A.NeverOnContract'
                      WHEN BB_Curr_Contract_Start_Dt IS      NULL AND (base_dt-BB_Prev_Contract_Actual_End_Dt)    >  180 THEN 'B.ExpiredContract(>6M)'
                      WHEN BB_Curr_Contract_Start_Dt IS      NULL AND (base_dt-BB_Prev_Contract_Actual_End_Dt)    <= 180 THEN 'C.ExpiredContract(<6M)'
                      WHEN BB_Curr_Contract_Start_Dt IS NOT  NULL AND (BB_Curr_Contract_Intended_End_Dt-base_dt)  <= 90  THEN 'D.ExpiringContract(<3M)'
                      WHEN BB_Curr_Contract_Start_Dt IS NOT  NULL AND (BB_Curr_Contract_Intended_End_Dt-base_dt)  <= 180 THEN 'E.ExpiringContract(<6M)'
                      WHEN BB_Curr_Contract_Start_Dt IS NOT  NULL AND (BB_Curr_Contract_Intended_End_Dt-base_dt)  >  180 THEN 'F.ExpiringContract(>6M)'
                      ELSE '' END AS BB_contract_segment

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
				,OD_Last_3M             
				,OD_Last_12M            
				,OD_Months_since_Last   
                ,accountid
                ,NTV_Ents_Last_30D
                ,NTV_Ents_Last_90D

                ,BB_Provider

    INTO        cs_binned
    FROM        CS_raw_test
    WHERE       country = 'UK'
    AND         base_dt BETWEEN @start_date AND @end_date_lag; /* Because we don't know upsell for last month */

    CREATE HG INDEX id1 ON cs_binned (account_number);
    CREATE DATE INDEX iddt ON cs_binned (base_dt);

/*  6.2 Permissions to public */
    
    GRANT SELECT ON cs_binned TO PUBLIC;