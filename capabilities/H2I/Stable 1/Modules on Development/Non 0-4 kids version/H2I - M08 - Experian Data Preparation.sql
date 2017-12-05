 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                                                                ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                                                                ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                                                                ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
                                                                  
**Business Brief:

        This Module goal is to extract and prepare the household and individual data from Experian 

**Module:
        
        M08: Experian Data Preparation
                        M08.0 - Initialising Environment
                        M08.1 - Account extraction from SAV
                        M08.2 - Experian HH Info Extraction (1st round - Only hh_key and address line matching accounts)
                        M08.3 - Experian HH Info Extraction (2nd round - Non-matching address line accounts AND hh > 10 people) 
                        M08.4 - Experian HH Info Extraction (3nd round - Non-matching address line accounts)
                        M08.5 - Individual TABLE POPULATION
                        M08.6 - Add Head of Household
                        M08.7 - Add Individual Children
                        M08.8 - Final Tidying of Data
                
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M08.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m08_Experian_data_preparation
AS BEGIN

        MESSAGE cast(now() as timestamp)||' | Begining M08.0 - Initialising Environment' TO CLIENT

        -- variables
        DECLARE @last_update_dt date    commit
        DECLARE @event_date DATE
        DECLARE @female_probability decimal(8,6) -- approx this percentage of head of hhds selected will be female
        commit

        select @last_update_dt =  max(updated_on) from V289_M08_SKY_HH_view     commit -- '2000-01-01' commit
        SELECT @event_date = max(date_of_activity_db1) FROM barb_weights
        set @female_probability = 0.800000
        commit

        /*
                this module is not needed to be refreshed every single day so we came up with
                the resolution of only executing it if the last update date is at least 1 week
                ago from today...
        */
       if (datediff(day,@last_update_dt,today()) > 6 or @last_update_dt is null)
        begin
                        
                MESSAGE cast(now() as timestamp)||' | @ M08.0: Initialising Environment DONE' TO CLIENT
                        
                --------------------------------------
                -- M08.1 - Account extraction from SAV
                --------------------------------------
                
                MESSAGE cast(now() as timestamp)||' | Begining M08.1 - Account extraction from SAV' TO CLIENT
                
                if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M08_SKY_HH_view')
                        and     tabletype = 'TABLE')                
                TRUNCATE TABLE V289_M08_SKY_HH_view   commit

                
                INSERT INTO V289_M08_SKY_HH_view (account_number, cb_key_household, cb_address_line_1, panel_flag)
                SELECT DISTINCT
                          sav.account_number
                        , sav.cb_key_household
                        , sav.cb_address_line_1
						, CASE WHEN viq.account_number IS NOT NULL THEN 1 ELSE 0 END AS panel_flag
                INTO    V289_M08_SKY_HH_view
                FROM cust_subs_hist AS csh
				JOIN CUST_SINGLE_ACCOUNT_VIEW AS sav ON sav.account_number = csh.account_number
                LEFT JOIN (SELECT DISTINCT account_number FROM VIQ_VIEWING_DATA_SCALING  WHERE adjusted_event_start_date_vespa = @event_date )   as viq ON sav.account_number    = viq.account_number
				WHERE subscription_sub_type IN ('DTV Primary Viewing')
					AND csh.status_code IN ('AC','AB','PC')
					AND csh.effective_from_dt <= @event_date
					AND csh.effective_to_dt >= @event_date
					AND csh.effective_from_dt    <> csh.effective_to_dt
					AND csh.EFFECTIVE_FROM_DT    IS NOT NULL
					AND csh.cb_key_household     > 0
					AND csh.cb_key_household     IS NOT NULL
					AND csh.cb_key_individual    IS NOT NULL
					AND csh.account_number       IS NOT NULL
					AND csh.service_instance_id  IS NOT NULL
							
							
				COMMIT

                
                MESSAGE cast(now() as timestamp)||' | @ M08.1 TABLE V289_M08_SKY_HH_view Populated' TO CLIENT
                        
                --------------------------------------------------------------------------------------------------
                -- M08.2 - Experian HH Info Extraction (1st round - Only hh_key and address line matching accounts)
                --------------------------------------------------------------------------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.2 - Experian HH Info Extraction' TO CLIENT
                
                SELECT account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS     HH_composition
                        , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
						, MAX(CASE h_presence_of_child_aged_0_4_2011 WHEN '1' THEN 1 ELSE 0 END) 	AS h_0_4_flag
						, MAX(CASE h_presence_of_child_aged_12_17_2011 WHEN '1' THEN 1 ELSE 0 END)	AS h_5_11_flag
						, MAX(CASE h_presence_of_child_aged_5_11_2011 WHEN '1' THEN 1 ELSE 0 END)	AS h_12_17_flag
                INTO #t1
                FROM V289_M08_SKY_HH_view AS vh
                JOIN EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household AND ex.cb_address_line_1 = vh.cb_address_line_1
                GROUP BY account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                COMMIT

                CREATE HG INDEX idhh ON #t1(cb_key_household)           commit
                CREATE HG INDEX idac ON #t1(account_number)                     commit
                CREATE HG INDEX idal ON #t1(cb_address_line_1)          commit

                COMMIT
                ---------------------   Table Update
                UPDATE V289_M08_SKY_HH_view
                SET     a.Children_count        = b.Children_count
                        ,   a.HH_composition        = b.HH_composition
                        ,   a.non_matching_flag     = 1
						,	a.h_0_4_flag			= COALESCE(b.h_0_4_flag,0)
						, 	a.h_5_11_flag			= COALESCE(b.h_5_11_flag,0)
						, 	a.h_12_17_flag			= COALESCE(b.h_12_17_flag,0)
						FROM V289_M08_SKY_HH_view as a
                JOIN #t1 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1

                COMMIT
                

                -- Clean up
                drop table #t1  commit

           MESSAGE cast(now() as timestamp)||' | @ M08.2 1st round finished ' TO CLIENT
           
                ----------------------------------------------------------------------------------------------------------
                -- M08.3 - Experian HH Info Extraction (2nd round - Non-matching address line accounts AND hh > 10 people) 
                ----------------------------------------------------------------------------------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.3 - Experian HH Info Extraction (2nd round)' TO CLIENT
                
                SELECT vh.account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , ex.cb_address_line_1 AS linex
                        , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS  HH_composition
                        , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
                        , RANK () OVER (PARTITION BY vh.cb_key_household ORDER by HH_composition DESC) as rank_1
						, MAX(CASE h_presence_of_child_aged_0_4_2011 WHEN '1' THEN 1 ELSE 0 END) 	AS h_0_4_flag
						, MAX(CASE h_presence_of_child_aged_12_17_2011 WHEN '1' THEN 1 ELSE 0 END)	AS h_5_11_flag
						, MAX(CASE h_presence_of_child_aged_5_11_2011 WHEN '1' THEN 1 ELSE 0 END)	AS h_12_17_flag
                INTO #t2
                FROM V289_M08_SKY_HH_view as vh
                JOIN EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household
                WHERE (vh.non_matching_flag = 0)
                GROUP BY vh.account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , linex
                HAVING HH_composition <= 10

                COMMIT

                CREATE HG INDEX idhh ON #t2(cb_key_household)           commit
                CREATE HG INDEX idac ON #t2(account_number)                     commit
                CREATE HG INDEX idal ON #t2(cb_address_line_1)          commit
                COMMIT
                ---------------------   Table Update
                UPDATE V289_M08_SKY_HH_view
                SET     a.Children_count        = b.Children_count
                        ,   a.HH_composition        = b.HH_composition
                        ,   a.cb_address_line_1     = b.linex
                        ,   a.non_matching_flag     = 1
                        ,   a.edited_add_flag       = 1
						,	a.h_0_4_flag			= COALESCE(b.h_0_4_flag,0)
						, 	a.h_5_11_flag			= COALESCE(b.h_5_11_flag,0)
						, 	a.h_12_17_flag			= COALESCE(b.h_12_17_flag,0)	
                FROM V289_M08_SKY_HH_view as a
                JOIN #t2 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1 and rank_1 = 1

                COMMIT
                
                -- Clean up
                drop table #t2  commit

           MESSAGE cast(now() as timestamp)||' | @ M08.3 2nd round finished ' TO CLIENT 

                -----------------------------------------------------------------------------------------
                -- M08.4 - Experian HH Info Extraction (3nd round - Non-matching address line accounts A)
                -----------------------------------------------------------------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.4 - Experian HH Info Extraction (3nd round)' TO CLIENT
                
                SELECT vh.account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , ex.cb_address_line_1 AS linex
                        , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS  HH_composition
                        , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
                        , RANK () OVER (PARTITION BY vh.cb_key_household ORDER by HH_composition ASC) as rank_1
						, MAX(CASE h_presence_of_child_aged_0_4_2011 WHEN '1' THEN 1 ELSE 0 END) 	AS h_0_4_flag
						, MAX(CASE h_presence_of_child_aged_12_17_2011 WHEN '1' THEN 1 ELSE 0 END)	AS h_5_11_flag
						, MAX(CASE h_presence_of_child_aged_5_11_2011 WHEN '1' THEN 1 ELSE 0 END)	AS h_12_17_flag
                INTO #t3
                FROM V289_M08_SKY_HH_view as vh
                JOIN EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household
                WHERE (vh.non_matching_flag = 0)
                GROUP BY vh.account_number
                        , vh.cb_key_household
                        , vh.cb_address_line_1
                        , linex
                COMMIT
                CREATE HG INDEX idhh ON #t3(cb_key_household)           commit
                CREATE HG INDEX idac ON #t3(account_number)                     commit
                CREATE HG INDEX idal ON #t3(cb_address_line_1)          commit
                COMMIT
				
				
                ---------------------   Table Update
                UPDATE V289_M08_SKY_HH_view
                SET     a.Children_count        = b.Children_count 
                        ,   a.HH_composition        = b.HH_composition
                        ,   a.cb_address_line_1     = b.linex
                        ,   a.non_matching_flag     = 1
                        ,   a.edited_add_flag       = 1
						,	a.h_0_4_flag			= COALESCE(b.h_0_4_flag,0)
						, 	a.h_5_11_flag			= COALESCE(b.h_5_11_flag,0)
						, 	a.h_12_17_flag			= COALESCE(b.h_12_17_flag	,0)
                FROM V289_M08_SKY_HH_view as a
                JOIN #t3 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1 and rank_1 = 1

                COMMIT
        

				------------------------------------------------ REMOVING 0-4 kids from the hhsize
				SELECT    account_number
						, Children_count2 = Children_count - (CASE WHEN h_5_11_flag = 0 AND h_12_17_flag = 0 THEN Children_count 	---- ONLY 0-4 kid in the HH's
															WHEN h_0_4_flag = 0 THEN 0										---- No 0-4 kids in the HH's
															WHEN h_0_4_flag = 1 AND (h_5_11_flag = 1 OR h_12_17_flag = 1) AND Children_count = 2 	THEN 1 ---- 1/2 kids is 0-4
															WHEN h_0_4_flag = 1 AND h_5_11_flag = 1 AND h_12_17_flag = 1  AND Children_count = 3 	THEN 1 ---- 1/3+ kids is 0-4
															WHEN h_0_4_flag = 1 AND (h_5_11_flag = 1 OR h_12_17_flag = 1) AND Children_count = 3 AND RIGHT (account_number,1) IN ('1','3','5','7','9')	THEN 1 ---- 1/3+ kids is 0-4
															WHEN h_0_4_flag = 1 AND (h_5_11_flag = 1 OR h_12_17_flag = 1) AND Children_count = 3 AND RIGHT (account_number,1) IN ('2','4','6','8','0')	THEN 2 ---- 1/3+ kids is 0-4
															
															WHEN h_0_4_flag = 1 AND h_5_11_flag = 1 AND h_12_17_flag = 1  AND Children_count = 4 AND RIGHT (account_number,1) IN ('1','3','5','7','9')	THEN 1 ---- 1/4+ kids is 0-4
															WHEN h_0_4_flag = 1 AND h_5_11_flag = 1 AND h_12_17_flag = 1  AND Children_count = 4 AND RIGHT (account_number,1) IN ('2','4','6','8','0')	THEN 2 ---- 2/4+ kids is 0-4
															
															WHEN h_0_4_flag = 1 AND (h_5_11_flag = 1 OR h_12_17_flag = 1) AND Children_count = 4 AND RIGHT (account_number,1) IN ('1','2','3')		THEN 1 ---- 2/4+ kids is 0-4
															WHEN h_0_4_flag = 1 AND (h_5_11_flag = 1 OR h_12_17_flag = 1) AND Children_count = 4 AND RIGHT (account_number,1) IN ('4','5','6','7')	THEN 2 ---- 2/4+ kids is 0-4
															WHEN h_0_4_flag = 1 AND (h_5_11_flag = 1 OR h_12_17_flag = 1) AND Children_count = 4 AND RIGHT (account_number,1) IN ('8','9','0')		THEN 3 ---- 2/4+ kids is 0-4
															ELSE 2 END)
						, HH_composition2 = HH_composition -(Children_count - Children_count2)
				INTO #childx
				FROM  V289_M08_SKY_HH_view
				COMMIT
				CREATE HG INDEX idq ON #childx(account_number)  
				COMMIT
				
				UPDATE V289_M08_SKY_HH_view
				SET Children_count = Children_count2,
					HH_composition = HH_composition2,
					h_0_4_flag = 0
				FROM V289_M08_SKY_HH_view AS a 
				JOIN  #childx AS b On a. account_number = b.account_number 
				COMMIT 
				
                -- Clean up
                drop table #t3  commit

                MESSAGE cast(now() as timestamp)||' | @ M08.4 3rd round finished ' TO CLIENT
                
                --------------------------------------
                -- M08.5 - Individual TABLE POPULATION
                --------------------------------------
                
                MESSAGE cast(now() as timestamp)||' | Begining M08.5 - Individual TABLE POPULATION' TO CLIENT
                
                if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M08_SKY_HH_composition')
                        and     tabletype = 'TABLE')
                TRUNCATE TABLE V289_M08_SKY_HH_composition
                
                INSERT INTO V289_M08_SKY_HH_composition (account_number, cb_key_household, exp_cb_key_db_person, cb_address_line_1
                                                        , cb_key_db_person, person_age, person_ageband, HH_person_number, person_gender
														, person_income, demographic_ID, panel_flag)
                SELECT
                          vh.account_number
                        , vh.cb_key_household
                        , ex.exp_cb_key_db_person
                        , vh.cb_address_line_1
                        , ex.cb_key_db_person
                        , person_age                = ex.p_actual_age
                        , person_ageband            = CASE WHEN person_age <= 19 then '12-19'
                                                                                           WHEN person_age BETWEEN 20 AND 24 then '20-24'
                                                                                           WHEN person_age BETWEEN 25 AND 34 then '25-34'
                                                                                           WHEN person_age BETWEEN 35 AND 44 then '35-44'
                                                                                           WHEN person_age BETWEEN 45 AND 64 then '45-64'
                                                                                           WHEN person_age >= 65 then '65+'
                                                                                  END
                        , HH_person_number          = RANK () OVER(PARTITION BY  vh.account_number ORDER BY person_age, p_gender, ex.cb_key_db_person)
                        , person_gender             = CASE  WHEN ex.p_gender = '0' THEN 'M'
                                                                                                WHEN ex.p_gender = '1' THEN 'F'
                                                                                                ELSE 'U' END
                        , person_income             = ex.p_personal_income_value
                        , demographic_ID    = CASE  WHEN p_gender = '0' AND p_actual_age <= 19                      THEN 7
                                                                                WHEN p_gender = '0' AND p_actual_age BETWEEN 20 AND 24          THEN 6
                                                                                WHEN p_gender = '0' AND p_actual_age BETWEEN 25 AND 34          THEN 5
                                                                                WHEN p_gender = '0' AND p_actual_age BETWEEN 35 AND 44          THEN 4
                                                                                WHEN p_gender = '0' AND p_actual_age BETWEEN 45 AND 64          THEN 3
                                                                                WHEN p_gender = '0' AND p_actual_age >= 65                      THEN 2
                                                                                ---------- FEMALES
                                                                                WHEN p_gender = '1' AND p_actual_age <= 19                      THEN 14
                                                                                WHEN p_gender = '1' AND p_actual_age BETWEEN 20 AND 24          THEN 13
                                                                                WHEN p_gender = '1' AND p_actual_age BETWEEN 25 AND 34          THEN 12
                                                                                WHEN p_gender = '1' AND p_actual_age BETWEEN 35 AND 44          THEN 11
                                                                                WHEN p_gender = '1' AND p_actual_age BETWEEN 45 AND 64          THEN 10
                                                                                WHEN p_gender = '1' AND p_actual_age >= 65                      THEN 9
                                                                                ---------- UNDEFINED GENDER
                                                                                WHEN p_gender = 'U' AND p_actual_age <= 19                      THEN 15
                                                                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 20 AND 24          THEN 16
                                                                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 25 AND 34          THEN 17
                                                                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 35 AND 44          THEN 18
                                                                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 45 AND 64          THEN 19
                                                                                WHEN p_gender = 'U' AND p_actual_age >= 65                      THEN 20
                                                                                ---------- UNDEFINED AGE
                                                                                WHEN p_gender = '1' AND p_actual_age IS NULL                    THEN 21
                                                                                WHEN p_gender = '0' AND p_actual_age IS NULL                    THEN 22
                                                                                ---------- UNDIFINED ALL
                                                                                WHEN p_gender = 'U' AND p_actual_age IS NULL                    THEN 23
                                                                                ELSE 0 END
						, panel_flag
                FROM V289_M08_SKY_HH_view AS vh
                JOIN EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household AND ex.cb_address_line_1 = vh.cb_address_line_1

                COMMIT

                
                MESSAGE cast(now() as timestamp)||' | @ M08.5 Individual table populated' TO CLIENT

                --------------------------------
                -- M08.6 - Add Head of Household
                --------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.6 - Add Head of Household' TO CLIENT
                
                --------        Get Experian Head of Household
                UPDATE  V289_M08_SKY_HH_composition s
                SET     exp_person_head = p_head_of_household
                FROM    sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD e
                WHERE   s.exp_cb_key_db_person = e.exp_cb_key_db_person
                COMMIT


                 ---------------------------------------------------------------------------------
                --- Based upon Experian Head of hhd select a single head of hhd for each hhd
                --- A hhd is defined by cb_key_household and cb_address_line_1
                --- Experian generally assigns BOTH a male and a female as head of hhd
                --- We need to select ONE. This is done based upon highest personal income by gender
                --- Then selecting a random male or female based upon a probability assigned to female
                ---------------------------------------------------------------------------------


                -- Identify highest personal income from indiviuals in a hhd who are head of hhd - by gender
                -- Also assign a probabity for selecting either experian head of hhd based upon gender
                select account_number, exp_cb_key_db_person, cb_key_household, cb_address_line_1
                                ,rank() OVER (PARTITION by account_number, cb_key_household, cb_address_line_1, person_gender ORDER BY person_income DESC, exp_cb_key_db_person DESC ) rank_1
                                ,case when person_gender = 'F' then @female_probability else 1 - @female_probability end as probability
                into #a1_1
                from V289_M08_SKY_HH_composition
                where exp_person_head = 1
                commit

                create hg index ind0 on #a1_1(account_number)
                create hg index ind1 on #a1_1(exp_cb_key_db_person)
                create hg index ind2 on #a1_1(cb_key_household)
                create hg index ind3 on #a1_1(cb_address_line_1)

                commit

                -- Calculate total probability by hhd.
                -- In some cases may only have 1 gender assigned as experian head of hhd. So need to deal with this
                select account_number, cb_key_household, cb_address_line_1, sum(probability) as tot_probability
                into #a1_2
                from #a1_1
                where rank_1 = 1
                group by account_number, cb_key_household, cb_address_line_1
                commit

                create hg index ind0 on #a1_2(account_number)
                create hg index ind1 on #a1_2(cb_key_household)
                create hg index ind2 on #a1_2(cb_address_line_1)
                commit

                -- For each experian head of household calculate bounds to apply probability to
                select #a1_1.exp_cb_key_db_person, #a1_1.account_number, #a1_1.cb_key_household, #a1_1.cb_address_line_1, rank_1
                                ,case
                                        when probability/tot_probability > 0.5 then 0.000000
                                        else cast(1-probability/tot_probability as decimal(8,6)) end as low_limit
                                ,case
                                        when probability/tot_probability > 0.5 then cast(probability/tot_probability as decimal(8,6))
                                        else 1.000000 end as high_limit
                into #a1_3
                from #a1_1 inner join #a1_2
                on #a1_1.cb_key_household = #a1_2.cb_key_household
                and #a1_1.cb_address_line_1 = #a1_2.cb_address_line_1
                and #a1_1.account_number = #a1_2.account_number
                where rank_1 = 1

                commit

                create hg index ind1 on #a1_3(exp_cb_key_db_person)
                create hg index ind2 on #a1_3(cb_key_household)
                create hg index ind3 on #a1_3(cb_address_line_1)
                create hg index ind4 on #a1_3(account_number)

                commit



                -- Generate a random number per hhd
                select distinct cb_key_household, cb_address_line_1, 0.000001 as random_number
                into #r1
                from V289_M08_SKY_HH_composition
                commit

                update #r1 set random_number = RAND(cb_key_household + DATEPART(us, GETDATE()))
                commit

                create hg index ind1 on #r1(cb_key_household)
                create hg index ind2 on #r1(cb_address_line_1)
                commit


                -- Assign a single individual in each hhd as head of hhd based upon above
                update V289_M08_SKY_HH_composition e
                                set person_head =  '1'
                                from #a1_3 a, #r1 r
                                where e.exp_cb_key_db_person = a.exp_cb_key_db_person
                                and a.rank_1 = 1
                                and e.cb_key_household = r.cb_key_household
                                and e.cb_address_line_1 = r.cb_address_line_1
                                and random_number >= low_limit and random_number < high_limit
                commit


                drop table #a1_1
                drop table #a1_2
                drop table #a1_3
                commit

                MESSAGE cast(now() as timestamp)||' | @ M08.6 Head of household added where Experian head exists' TO CLIENT


                --- Not all hhds have a defined head of hhd from Experian. So will assign highest personal income in these cases
                -- First count number of heads of hhd as per our definition for each hhd
                select account_number, cb_key_household, cb_address_line_1
                                , sum(case when person_head = '1' then 1 else 0 end) as head_count
                into #b1
                from V289_M08_SKY_HH_composition
                group by account_number, cb_key_household, cb_address_line_1
                commit

                create hg index ind1 on #b1(cb_key_household)
                create hg index ind2 on #b1(cb_address_line_1)
                create lf index ind3 on #b1(head_count)
                create hg index ind4 on #b1(account_number)

                commit


                -- Those hhds where above is zero need to be allocated individual with highest income by gender
                select p.exp_cb_key_db_person, p.account_number, p.cb_key_household, p.cb_address_line_1
                                ,rank() OVER (PARTITION by p.account_number, p.cb_key_household, p.cb_address_line_1, person_gender ORDER BY p.person_income DESC, p.exp_cb_key_db_person DESC ) rank_1
                                ,case when person_gender = 'F' then @female_probability else 1 - @female_probability end as probability
                into #b1_1
                from
                                V289_M08_SKY_HH_composition p
                         inner join
                                #b1 b
                         on p.cb_key_household = b.cb_key_household and p.cb_address_line_1 = b.cb_address_line_1
                where b.head_count = 0
                commit

                create hg index ind1 on #b1_1(exp_cb_key_db_person)
                commit


                -- Calulate total probabilty by hhd
                select account_number, cb_key_household, cb_address_line_1, sum(probability) as tot_probability
                into #b1_2
                from #b1_1
                where rank_1 = 1
                group by account_number, cb_key_household, cb_address_line_1
                commit

                create hg index ind1 on #b1_2(cb_key_household)
                create hg index ind2 on #b1_2(cb_address_line_1)
                create hg index ind3 on #b1_2(account_number)

                commit

                -- Calculate lower and upper bounds for each potential head of hhd individual
                select #b1_1.exp_cb_key_db_person, #b1_1.account_number, #b1_1.cb_key_household, #b1_1.cb_address_line_1, rank_1
                                ,case
                                        when probability/tot_probability > 0.5 then 0.000000
                                        else cast(1-probability/tot_probability as decimal(8,6)) end as low_limit
                                ,case
                                        when probability/tot_probability > 0.5 then cast(probability/tot_probability as decimal(8,6))
                                        else 1.000000 end as high_limit
                into #b1_3
                from #b1_1 inner join #b1_2
                on #b1_1.cb_key_household = #b1_2.cb_key_household
                and #b1_1.cb_address_line_1 = #b1_2.cb_address_line_1
                and #b1_1.account_number = #b1_2.account_number
                where rank_1 = 1

                commit

                create hg index ind1 on #b1_3(exp_cb_key_db_person)
                create hg index ind2 on #b1_3(cb_key_household)
                create hg index ind3 on #b1_3(cb_address_line_1)
                create hg index ind4 on #b1_3(account_number)

                commit


                -- Assign individual as head of hhd
                update V289_M08_SKY_HH_composition e
                                set person_head =  '1'
                                from #b1_3 b, #r1 r
                                where e.exp_cb_key_db_person = b.exp_cb_key_db_person
                                and b.rank_1 = 1
                                and e.cb_key_household = r.cb_key_household
                                and e.cb_address_line_1 = r.cb_address_line_1
                                and random_number >= low_limit and random_number < high_limit
                commit

                drop table #r1

                drop table #b1
                drop table #b1_1
                drop table #b1_2
                drop table #b1_3
                commit

                MESSAGE cast(now() as timestamp)||' | @ M08.6 Head of household added' TO CLIENT


                ----------------------------------
                -- M08.7 - Add Individual Children
                ----------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.7 - Add Individual Children' TO CLIENT

                -- Experian tables do not have individual data for children less than 17
                ---- Need to append rows for these
                --- They cannot be head of hhd either so can be run after that code

                -- Will need to add a row for each child, these multiple rows in this table will enable
                -- the right number of individuals to be added to the data
                select 1 as number_of_kids, 1 as unique_row into #PIV_append_kids_rows
                commit

                create lf index ind1 on #PIV_append_kids_rows(number_of_kids)
                commit

                insert into #PIV_append_kids_rows values (2, 2)
                insert into #PIV_append_kids_rows values (2, 3)
                insert into #PIV_append_kids_rows values (3, 4)
                insert into #PIV_append_kids_rows values (3, 5)
                insert into #PIV_append_kids_rows values (3, 6)
                insert into #PIV_append_kids_rows values (4, 7)
                insert into #PIV_append_kids_rows values (4, 8)
                insert into #PIV_append_kids_rows values (4, 9)
                insert into #PIV_append_kids_rows values (4, 10)
                insert into #PIV_append_kids_rows values (5, 11)
                insert into #PIV_append_kids_rows values (5, 12)
                insert into #PIV_append_kids_rows values (5, 13)
                insert into #PIV_append_kids_rows values (5, 14)
                insert into #PIV_append_kids_rows values (5, 15)
                insert into #PIV_append_kids_rows values (6, 16)
                insert into #PIV_append_kids_rows values (6, 17)
                insert into #PIV_append_kids_rows values (6, 18)
                insert into #PIV_append_kids_rows values (6, 19)
                insert into #PIV_append_kids_rows values (6, 20)
                insert into #PIV_append_kids_rows values (6, 21)
                insert into #PIV_append_kids_rows values (7, 22)
                insert into #PIV_append_kids_rows values (7, 23)
                insert into #PIV_append_kids_rows values (7, 24)
                insert into #PIV_append_kids_rows values (7, 25)
                insert into #PIV_append_kids_rows values (7, 26)
                insert into #PIV_append_kids_rows values (7, 27)
                insert into #PIV_append_kids_rows values (7, 28)
                insert into #PIV_append_kids_rows values (8, 29)
                insert into #PIV_append_kids_rows values (8, 30)
                insert into #PIV_append_kids_rows values (8, 31)
                insert into #PIV_append_kids_rows values (8, 32)
                insert into #PIV_append_kids_rows values (8, 33)
                insert into #PIV_append_kids_rows values (8, 34)
                insert into #PIV_append_kids_rows values (8, 35)
                insert into #PIV_append_kids_rows values (8, 36)
                insert into #PIV_append_kids_rows values (9, 37)
                insert into #PIV_append_kids_rows values (9, 38)
                insert into #PIV_append_kids_rows values (9, 39)
                insert into #PIV_append_kids_rows values (9, 40)
                insert into #PIV_append_kids_rows values (9, 41)
                insert into #PIV_append_kids_rows values (9, 42)
                insert into #PIV_append_kids_rows values (9, 43)
                insert into #PIV_append_kids_rows values (9, 44)
                insert into #PIV_append_kids_rows values (9, 45)
                insert into #PIV_append_kids_rows values (10, 46)
                insert into #PIV_append_kids_rows values (10, 47)
                insert into #PIV_append_kids_rows values (10, 48)
                insert into #PIV_append_kids_rows values (10, 49)
                insert into #PIV_append_kids_rows values (10, 50)
                insert into #PIV_append_kids_rows values (10, 51)
                insert into #PIV_append_kids_rows values (10, 52)
                insert into #PIV_append_kids_rows values (10, 53)
                insert into #PIV_append_kids_rows values (10, 54)
                insert into #PIV_append_kids_rows values (10, 55)
                insert into #PIV_append_kids_rows values (11, 56)
                insert into #PIV_append_kids_rows values (11, 57)
                insert into #PIV_append_kids_rows values (11, 58)
                insert into #PIV_append_kids_rows values (11, 59)
                insert into #PIV_append_kids_rows values (11, 60)
                insert into #PIV_append_kids_rows values (11, 61)
                insert into #PIV_append_kids_rows values (11, 62)
                insert into #PIV_append_kids_rows values (11, 63)
                insert into #PIV_append_kids_rows values (11, 64)
                insert into #PIV_append_kids_rows values (11, 65)
                insert into #PIV_append_kids_rows values (11, 66)
                insert into #PIV_append_kids_rows values (12, 67)
                insert into #PIV_append_kids_rows values (12, 68)
                insert into #PIV_append_kids_rows values (12, 69)
                insert into #PIV_append_kids_rows values (12, 70)
                insert into #PIV_append_kids_rows values (12, 71)
                insert into #PIV_append_kids_rows values (12, 72)
                insert into #PIV_append_kids_rows values (12, 73)
                insert into #PIV_append_kids_rows values (12, 74)
                insert into #PIV_append_kids_rows values (12, 75)
                insert into #PIV_append_kids_rows values (12, 76)
                insert into #PIV_append_kids_rows values (12, 77)
                insert into #PIV_append_kids_rows values (12, 78)
                insert into #PIV_append_kids_rows values (13, 79)
                insert into #PIV_append_kids_rows values (13, 80)
                insert into #PIV_append_kids_rows values (13, 81)
                insert into #PIV_append_kids_rows values (13, 82)
                insert into #PIV_append_kids_rows values (13, 83)
                insert into #PIV_append_kids_rows values (13, 84)
                insert into #PIV_append_kids_rows values (13, 85)
                insert into #PIV_append_kids_rows values (13, 86)
                insert into #PIV_append_kids_rows values (13, 87)
                insert into #PIV_append_kids_rows values (13, 88)
                insert into #PIV_append_kids_rows values (13, 89)
                insert into #PIV_append_kids_rows values (13, 90)
                insert into #PIV_append_kids_rows values (13, 91)
                insert into #PIV_append_kids_rows values (14, 92)
                insert into #PIV_append_kids_rows values (14, 93)
                insert into #PIV_append_kids_rows values (14, 94)
                insert into #PIV_append_kids_rows values (14, 95)
                insert into #PIV_append_kids_rows values (14, 96)
                insert into #PIV_append_kids_rows values (14, 97)
                insert into #PIV_append_kids_rows values (14, 98)
                insert into #PIV_append_kids_rows values (14, 99)
                insert into #PIV_append_kids_rows values (14, 100)
                insert into #PIV_append_kids_rows values (14, 101)
                insert into #PIV_append_kids_rows values (14, 102)
                insert into #PIV_append_kids_rows values (14, 103)
                insert into #PIV_append_kids_rows values (14, 104)
                insert into #PIV_append_kids_rows values (14, 105)
                insert into #PIV_append_kids_rows values (15, 106)
                insert into #PIV_append_kids_rows values (15, 107)
                insert into #PIV_append_kids_rows values (15, 108)
                insert into #PIV_append_kids_rows values (15, 109)
                insert into #PIV_append_kids_rows values (15, 110)
                insert into #PIV_append_kids_rows values (15, 111)
                insert into #PIV_append_kids_rows values (15, 112)
                insert into #PIV_append_kids_rows values (15, 113)
                insert into #PIV_append_kids_rows values (15, 114)
                insert into #PIV_append_kids_rows values (15, 115)
                insert into #PIV_append_kids_rows values (15, 116)
                insert into #PIV_append_kids_rows values (15, 117)
                insert into #PIV_append_kids_rows values (15, 118)
                insert into #PIV_append_kids_rows values (15, 119)
                insert into #PIV_append_kids_rows values (15, 120)
                insert into #PIV_append_kids_rows values (16, 121)
                insert into #PIV_append_kids_rows values (16, 122)
                insert into #PIV_append_kids_rows values (16, 123)
                insert into #PIV_append_kids_rows values (16, 124)
                insert into #PIV_append_kids_rows values (16, 125)
                insert into #PIV_append_kids_rows values (16, 126)
                insert into #PIV_append_kids_rows values (16, 127)
                insert into #PIV_append_kids_rows values (16, 128)
                insert into #PIV_append_kids_rows values (16, 129)
                insert into #PIV_append_kids_rows values (16, 130)
                insert into #PIV_append_kids_rows values (16, 131)
                insert into #PIV_append_kids_rows values (16, 132)
                insert into #PIV_append_kids_rows values (16, 133)
                insert into #PIV_append_kids_rows values (16, 134)
                insert into #PIV_append_kids_rows values (16, 135)
                insert into #PIV_append_kids_rows values (16, 136)
                insert into #PIV_append_kids_rows values (17, 137)
                insert into #PIV_append_kids_rows values (17, 138)
                insert into #PIV_append_kids_rows values (17, 139)
                insert into #PIV_append_kids_rows values (17, 140)
                insert into #PIV_append_kids_rows values (17, 141)
                insert into #PIV_append_kids_rows values (17, 142)
                insert into #PIV_append_kids_rows values (17, 143)
                insert into #PIV_append_kids_rows values (17, 144)
                insert into #PIV_append_kids_rows values (17, 145)
                insert into #PIV_append_kids_rows values (17, 146)
                insert into #PIV_append_kids_rows values (17, 147)
                insert into #PIV_append_kids_rows values (17, 148)
                insert into #PIV_append_kids_rows values (17, 149)
                insert into #PIV_append_kids_rows values (17, 150)
                insert into #PIV_append_kids_rows values (17, 151)
                insert into #PIV_append_kids_rows values (17, 152)
                insert into #PIV_append_kids_rows values (17, 153)
                insert into #PIV_append_kids_rows values (18, 154)
                insert into #PIV_append_kids_rows values (18, 155)
                insert into #PIV_append_kids_rows values (18, 156)
                insert into #PIV_append_kids_rows values (18, 157)
                insert into #PIV_append_kids_rows values (18, 158)
                insert into #PIV_append_kids_rows values (18, 159)
                insert into #PIV_append_kids_rows values (18, 160)
                insert into #PIV_append_kids_rows values (18, 161)
                insert into #PIV_append_kids_rows values (18, 162)
                insert into #PIV_append_kids_rows values (18, 163)
                insert into #PIV_append_kids_rows values (18, 164)
                insert into #PIV_append_kids_rows values (18, 165)
                insert into #PIV_append_kids_rows values (18, 166)
                insert into #PIV_append_kids_rows values (18, 167)
                insert into #PIV_append_kids_rows values (18, 168)
                insert into #PIV_append_kids_rows values (18, 169)
                insert into #PIV_append_kids_rows values (18, 170)
                insert into #PIV_append_kids_rows values (18, 171)
                insert into #PIV_append_kids_rows values (19, 172)
                insert into #PIV_append_kids_rows values (19, 173)
                insert into #PIV_append_kids_rows values (19, 174)
                insert into #PIV_append_kids_rows values (19, 175)
                insert into #PIV_append_kids_rows values (19, 176)
                insert into #PIV_append_kids_rows values (19, 177)
                insert into #PIV_append_kids_rows values (19, 178)
                insert into #PIV_append_kids_rows values (19, 179)
                insert into #PIV_append_kids_rows values (19, 180)
                insert into #PIV_append_kids_rows values (19, 181)
                insert into #PIV_append_kids_rows values (19, 182)
                insert into #PIV_append_kids_rows values (19, 183)
                insert into #PIV_append_kids_rows values (19, 184)
                insert into #PIV_append_kids_rows values (19, 185)
                insert into #PIV_append_kids_rows values (19, 186)
                insert into #PIV_append_kids_rows values (19, 187)
                insert into #PIV_append_kids_rows values (19, 188)
                insert into #PIV_append_kids_rows values (19, 189)
                insert into #PIV_append_kids_rows values (19, 190)
                insert into #PIV_append_kids_rows values (20, 191)
                insert into #PIV_append_kids_rows values (20, 192)
                insert into #PIV_append_kids_rows values (20, 193)
                insert into #PIV_append_kids_rows values (20, 194)
                insert into #PIV_append_kids_rows values (20, 195)
                insert into #PIV_append_kids_rows values (20, 196)
                insert into #PIV_append_kids_rows values (20, 197)
                insert into #PIV_append_kids_rows values (20, 198)
                insert into #PIV_append_kids_rows values (20, 199)
                insert into #PIV_append_kids_rows values (20, 200)
                insert into #PIV_append_kids_rows values (20, 201)
                insert into #PIV_append_kids_rows values (20, 202)
                insert into #PIV_append_kids_rows values (20, 203)
                insert into #PIV_append_kids_rows values (20, 204)
                insert into #PIV_append_kids_rows values (20, 205)
                insert into #PIV_append_kids_rows values (20, 206)
                insert into #PIV_append_kids_rows values (20, 207)
                insert into #PIV_append_kids_rows values (20, 208)
                insert into #PIV_append_kids_rows values (20, 209)
                insert into #PIV_append_kids_rows values (20, 210)
                commit


                INSERT INTO V289_M08_SKY_HH_composition (account_number, cb_key_household, cb_address_line_1, person_gender, person_ageband, demographic_ID, randd, panel_flag)
                select
						hh.account_number
						,hh.cb_key_household
						,hh.cb_address_line_1
						,'U'
						,CASE 	WHEN hh.h_12_17_flag = 0 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) 	THEN '0-11'
								WHEN hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 0 AND hh.h_5_11_flag = 0) THEN '12-19'
								
								WHEN hh.children_count = 2 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 2 THEN '0-11' 
								WHEN hh.children_count = 2 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 3 THEN '12-19'
								
								WHEN hh.children_count = 3 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 4 THEN '0-11'
								WHEN hh.children_count = 3 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 5 THEN '12-19'
								WHEN hh.children_count = 3 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 6 THEN '0-11'
								
								WHEN hh.children_count = 3 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 4 THEN '0-11'
								WHEN hh.children_count = 3 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 5 THEN '12-19'
								
								WHEN hh.children_count = 4 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 7 THEN '0-11'
								WHEN hh.children_count = 4 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 8 THEN '12-19'
								WHEN hh.children_count = 4 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 9 THEN '0-11'

								WHEN hh.children_count = 4 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 7 THEN '0-11'
								WHEN hh.children_count = 4 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 8 THEN '12-19'

								WHEN hh.children_count = 5 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 10 THEN '0-11'
								WHEN hh.children_count = 5 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 11 THEN '12-19'
								WHEN hh.children_count = 5 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 12 THEN '0-11'

								WHEN hh.children_count = 5 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 10 THEN '0-11'
								WHEN hh.children_count = 5 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 11 THEN '12-19'
								
								WHEN hh.children_count = 6 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 16 THEN '0-11'
								WHEN hh.children_count = 6 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 17 THEN '12-19'
								WHEN hh.children_count = 6 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 18 THEN '0-11'

								WHEN hh.children_count = 6 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 16 THEN '0-11'
								WHEN hh.children_count = 6 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 17 THEN '12-19'
								
								WHEN hh.children_count = 7 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 22 THEN '0-11'
								WHEN hh.children_count = 7 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 23 THEN '12-19'
								WHEN hh.children_count = 7 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 24 THEN '0-11'

								WHEN hh.children_count = 7 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 22 THEN '0-11'
								WHEN hh.children_count = 7 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 23 THEN '12-19'
								
								WHEN hh.children_count = 8 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 29 THEN '0-11'
								WHEN hh.children_count = 8 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 30 THEN '12-19'
								WHEN hh.children_count = 8 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 31 THEN '0-11'

								WHEN hh.children_count = 8 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 29 THEN '0-11'
								WHEN hh.children_count = 8 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 30 THEN '12-19'
								
								WHEN hh.children_count = 9 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 37 THEN '0-11'
								WHEN hh.children_count = 9 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 38 THEN '12-19'
								WHEN hh.children_count = 9 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 39 THEN '0-11'

								WHEN hh.children_count = 9 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 37 THEN '0-11'
								WHEN hh.children_count = 9 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 38 THEN '12-19'
								
								WHEN hh.children_count = 10 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 46 THEN '0-11'
								WHEN hh.children_count = 10 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 47 THEN '12-19'
								WHEN hh.children_count = 10 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 48 THEN '0-11'

								WHEN hh.children_count = 10 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 46 THEN '0-11'
								WHEN hh.children_count = 10 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 47 THEN '12-19'
								
								WHEN hh.children_count = 11 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 56 THEN '0-11'
								WHEN hh.children_count = 11 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 57 THEN '12-19'
								WHEN hh.children_count = 11 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 58 THEN '0-11'

								WHEN hh.children_count = 11 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 56 THEN '0-11'
								WHEN hh.children_count = 11 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 57 THEN '12-19'
								
								WHEN hh.children_count = 12 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 67 THEN '0-11'
								WHEN hh.children_count = 12 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 68 THEN '12-19'
								WHEN hh.children_count = 12 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 69 THEN '0-11'

								WHEN hh.children_count = 12 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 67 THEN '0-11'
								WHEN hh.children_count = 12 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 68 THEN '12-19'
								
								WHEN hh.children_count = 13 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 79 THEN '0-11'
								WHEN hh.children_count = 13 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 80 THEN '12-19'
								WHEN hh.children_count = 13 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 81 THEN '0-11'

								WHEN hh.children_count = 13 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 79 THEN '0-11'
								WHEN hh.children_count = 13 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 80 THEN '12-19'
								
								WHEN hh.children_count = 14 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 92 THEN '0-11'
								WHEN hh.children_count = 14 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 93 THEN '12-19'
								WHEN hh.children_count = 14 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 94 THEN '0-11'
								
								WHEN hh.children_count = 14 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 92 THEN '0-11'
								WHEN hh.children_count = 14 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 93 THEN '12-19'
								
								WHEN hh.children_count = 15 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 106 THEN '0-11'
								WHEN hh.children_count = 15 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 107 THEN '12-19'
								WHEN hh.children_count = 15 AND hh.h_12_17_flag = 1 AND hh.h_0_4_flag = 1 AND hh.h_5_11_flag = 1 AND unique_row = 108 THEN '0-11'
								
								WHEN hh.children_count = 15 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 106 THEN '0-11'
								WHEN hh.children_count = 15 AND hh.h_12_17_flag = 1 AND (hh.h_0_4_flag = 1 OR hh.h_5_11_flag = 1) AND unique_row = 107 THEN '12-19'
								ELSE 'XX' END
								
						,15 -- demographic_ID for gender ='U' and age <=19
						, RAND(cb_key_household + unique_row + DATEPART(us, getdate())) randd
						, panel_flag
                FROM        V289_M08_SKY_HH_view hh
                inner join  #PIV_append_kids_rows k		ON hh.children_count = k.number_of_kids
                         
                commit

                -- Clean up
            drop table #PIV_append_kids_rows        
			commit

			CREATE TABLE #tkid
				(demo VARCHAR (5) NULL
				, low DECIMAL(8,1) 
				, upp DECIMAL(8,1) )
			
			INSERT INTO #tkid
			VALUES
				('0-11', 0, 0.3)
				, ('12-19', 0.3, 1)
				
			COMMIT
			
			UPDATE V289_M08_SKY_HH_composition
			SET person_ageband = demo
			FROM V289_M08_SKY_HH_composition as a 
			JOIN #tkid as b ON randd <= upp AND randd > low
			WHERE person_ageband = 'XX'
				
				
                ---- There are a small number of 0-19 in the Experian data (these were 18-19 in Experian data)
                --- These will have a gender. But because they are a small number distort the scaling
                --- Change the gender of these to U

                update V289_M08_SKY_HH_composition
                set person_gender = 'U'
                where person_ageband IN ('0-11', '12-19')
                commit

                MESSAGE cast(now() as timestamp)||' | @ M08.7 kids data added' TO CLIENT
                
                --------------------------------
                -- M08.8 - Final Tidying of Data
                --------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M08.8 - Final Tidying of Data' TO CLIENT
                
                -- Everyone with the same account_number gets a unique number
                select     row_id
                                   ,RANK () OVER (PARTITION BY  account_number ORDER BY person_head DESC, row_id) as rank1
                into       #a4
                from        V289_M08_SKY_HH_composition
                group by    account_number, person_head, row_id
                commit

                create hg index ind1 on #a4(row_id)
                commit

                update V289_M08_SKY_HH_composition h
                set HH_person_number = rank1
                from #a4 r
                where h.row_id = r.row_id
                commit
                
                -- Clean up
                drop table #a4


                -- Calculate household size and delete any > 15

                select account_number, count(1) as hhd_count
                into #a5
                from V289_M08_SKY_HH_composition
                group by account_number
                commit

                update V289_M08_SKY_HH_composition c
                set household_size = hhd_count
                from #a5 a
                where c.account_number = a.account_number
                commit
                
                delete from V289_M08_SKY_HH_composition
                where household_size > 15
                commit
                
                -- Clean up
                drop table #a5

                MESSAGE cast(now() as timestamp)||' | @ M08.8: Final Tidying of Data DONE' TO CLIENT




				-------------------------------------------------------------------------------
				-- M08.9 - Remove incomplete households (Undefined gender for ageband NOT 0-19)
				-------------------------------------------------------------------------------

				MESSAGE cast(now() as timestamp)||' | @ M08.9: Removing incomplete households' TO CLIENT
				
				
				-- First, identify the households/accounts to remove
				SELECT	account_number
				INTO	#INCOMPLETE_HHS
				FROM	V289_M08_SKY_HH_composition
				WHERE	
						person_gender	=	'U'
					AND	person_ageband	NOT IN ('0-11', '12-19', 'XX')
				GROUP BY	account_number
				COMMIT

				MESSAGE cast(now() as timestamp)||' | @ M08.9: Incomplete households identified for removal : ' || @@rowcount TO CLIENT

				CREATE UNIQUE HG INDEX IDX1 ON #INCOMPLETE_HHS(account_number)
				COMMIT
				
				
				-- Now delete the appropriate records corresponding to those incomplete accounts
				DELETE FROM V289_M08_SKY_HH_composition
				FROM    
								V289_M08_SKY_HH_composition		AS  BAS
					LEFT JOIN   #INCOMPLETE_HHS         		AS  INC     ON  BAS.account_number  =   INC.account_number
				WHERE   INC.ACCOUNT_NUMBER  IS NOT NULL
				COMMIT
				
				MESSAGE cast(now() as timestamp)||' | @ M08.9: Removing incomplete households...DONE. Rows removed : '||@@rowcount TO CLIENT
				
				DROP TABLE	#INCOMPLETE_HHS
				COMMIT


		END
        
         else
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M08.0: Data still valid, last update was less than a week ago' TO CLIENT
                MESSAGE cast(now() as timestamp)||' | @ M08.0: Initialising Environment DONE' TO CLIENT
        
        end
        
        
        MESSAGE cast(now() as timestamp)||' | M08.8 Process completed' TO CLIENT
        
END;                    --      END of Proc


COMMIT;
GRANT EXECUTE   ON v289_m08_Experian_data_preparation   TO vespa_group_low_security;
COMMIT;






 -- NOTE THESE QA NUMBERS NEED TO BE REFRESHED
---------------------------------  QA
 ---------------------------------  V289_M08_SKY_HH_view
 ---- account_number             9,929,864
 ---- cb_key_household           9,542,183
 ---- cb_address_line_1          6,036,344
 ---- matching_flag              8,734,222
 ---- edited_add_flag              365,499
 ---- HH Children_count          3,207,144
 ---- COUNT()                            9,929,864
---------------------------------
--------------------------------- V289_M08_SKY_HH_composition (individuals)
---- account_number      8,734,222
---- cb_key_household    8,615,848
---- cb_address_line_1   5,465,916
---- cb_key_db_person   18,375,549
---- individual                 17,898,812
---- COUNT(*)                   19,087,944
--------------------------------------
