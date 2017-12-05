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

        This Module goal is to assign individuals from V289_M08_SKY_HH_composition to be non-viewers using Monte Carlo simulation process.

**Module:
        
        M09: Session Size Assignment process
                        M15.0 - Initialising Environment
                        M15.1 - Creating transient tables
                        M15.2 - Non-viewer update
                        M15.3 - Assign Non-Viewers
                        M15.4 - Update Viewer hhsize in M08 HH Comp
                        M15.5 - Update Viewer hhsize in M07 Viewing Data
        
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M15.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m15_non_viewers_assignment
        @event_date date = null
AS BEGIN

        MESSAGE cast(now() as timestamp)||' | Begining M15.0 - Initialising Environment' TO CLIENT






        ---------------------------------------
        -- M15.1 - Creating transient tables
        ---------------------------------------


        -----------------       temp_ind Table Creation


        IF OBJECT_ID('temp_inds') IS NOT NULL DROP TABLE temp_inds

        SELECT          dt.account_number
                        , dt.hh_person_number
                        , dt.household_size
						, person_ageband 			AS age	
						, person_gender 			AS sex	
						, random1       =   RAND(dt.row_id + DATEPART(us, GETDATE()))
						, CAST (0 AS DECIMAL(15,4)) AS PIV
						, CAST (0 AS DECIMAL(15,4)) AS normalization
						, 0 as non_viewer
        INTO temp_inds
        FROM  V289_M08_SKY_HH_composition        AS      dt
		JOIN (SELECT DISTINCT account_number FROM V289_M07_dp_data) as b ON b.account_number = dt.account_number
		WHERE dt.account_number is not null 
		AND household_size >1
        
        MESSAGE cast(now() as timestamp)||' | @ M15.1: temp_inds Table created: '||@@rowcount TO CLIENT
                
        COMMIT

        CREATE HG INDEX ide1 ON temp_inds(account_number)
        CREATE LF INDEX ide2 ON temp_inds(hh_person_number)
        CREATE LF INDEX ide3 ON temp_inds(sex)
        CREATE LF INDEX ide4 ON temp_inds(age)
        COMMIT
-----------------------------------------         temp_house Table Creation

        IF OBJECT_ID('temp_house') IS NOT NULL DROP TABLE temp_house

        SELECT   
						MIN (dt.row_id) row_id
						, dt.account_number
                        , CASE WHEN dt.household_size > 8 THEN 8 ELSE dt.household_size END  AS household_size
						, b.ntile_hp
						, MAX(CASE WHEN person_ageband = '0-19' THEN 1 ELSE 0 END) kid
						, MAX(CASE WHEN person_ageband = '20-24' THEN 1 ELSE 0 END) twenties
                        , CAST (null AS float) 		AS random1       
                        , household_size			AS viewing_size
						, CAST (0 AS tinyint) 		AS dif_viewer
        INTO temp_house
        FROM V289_M08_SKY_HH_composition        AS      dt
		JOIN (	SELECT 
					  account_number
					, SUM(event_duration_seg) duration
					, ntile(4) over (order by duration) as ntile_hp
				FROM V289_M07_dp_data
				GROUP BY account_number) as b ON b.account_number = dt.account_number
		WHERE household_size >1 
		GROUP BY 	dt.account_number, dt.household_size, ntile_hp
		
		
        MESSAGE cast(now() as timestamp)||' | @ M15.1: temp_house Table created: '||@@rowcount TO CLIENT
                
        COMMIT

		UPDATE temp_house
		SET  random1  =   RAND(row_id + DATEPART(us, GETDATE()))
		
        CREATE HG INDEX cide1 ON temp_house(account_number)
        CREATE LF INDEX icde2 ON temp_house(household_size)
		CREATE LF INDEX icdex ON temp_house(twenties)
        CREATE HG INDEX icde3 ON temp_house(random1)
        CREATE LF INDEX icde4 ON temp_house(dif_viewer)
        COMMIT

        ------------------------------
        -- M15.2 - Non-viewer update
        ------------------------------
	   UPDATE  temp_house
        SET viewing_size = household_size
		
	   UPDATE  temp_house
        SET viewing_size = viewer_size,
            dif_viewer = household_size - viewer_size
        FROM temp_house as a
        JOIN v289_nonviewers_matrix_small as b ON a.household_size = b.hh_size
                                        AND a.random1     >   b.lower_limit
                                        AND a.random1     <=  b.upper_limit
                                        AND b.date_of_activity_db1 =  @event_date
                                        AND b.ntile_lp  = ntile_hp
                                        AND a.twenties      = b.twenties
										AND a.kid       	= b.kid
        WHERE household_size <= 4
        COMMIT

        UPDATE temp_house SET viewing_size =1 , dif_viewer =0 where household_size =1



        UPDATE  temp_house
        SET viewing_size = viewer_size,
            dif_viewer = household_size - viewer_size
        FROM temp_house as a
        JOIN v289_nonviewers_matrix_big as b ON a.household_size = b.hh_size
                                        AND a.random1     >   b.lower_limit
                                        AND a.random1     <=  b.upper_limit
                                        AND b.date_of_activity_db1 =  @event_date
                                        AND b.ntile_lp  = ntile_hp
        WHERE household_size > 4
        COMMIT

        UPDATE temp_house SET viewing_size =1 , dif_viewer =0 where household_size =1


	    MESSAGE cast(now() as timestamp)||' | @ M15.2: New viewer_size assigned : '||@@rowcount TO CLIENT
    		
			
		UPDATE temp_inds
		SET a.PIV 			= b.PIV,
			normalization 	= random1	* b.PIV		
		FROM temp_inds as a
		JOIN V289_non_viewers_PIV as b ON b.hh_size = a.household_size
										AND LEFT(b.sex,1) = LEFT(a.sex,1)
										AND LEFT(b.ageband,2) = LEFT(a.age,2)
									
		
		COMMIT 
		
		MESSAGE cast(now() as timestamp)||' | @ M15.2: Individual PIV assigned : '||@@rowcount TO CLIENT
		
		SELECT 
				hh_person_number
				, account_number
				, normalization
				, dense_rank () OVER ( PARTITION BY account_number ORDER BY normalization, hh_person_number) rank1
		INTO #t1 
		FROM 	temp_inds
		
		CREATE HG INDEX seas ON  #t1(account_number)
		CREATE LF INDEX sdcs ON  #t1(hh_person_number)
		CREATE LF INDEX dkic ON  #t1(rank1)
		
		COMMIT
		
		UPDATE temp_inds
		SET non_viewer =1 
		FROM temp_inds 	AS a 
		JOIN temp_house AS c ON a.account_number = c.account_number
		JOIN #t1 		AS b ON a.account_number = b.account_number
							AND a.hh_person_number = b.hh_person_number
							AND rank1 <= dif_viewer
		WHERE dif_viewer > 0

        MESSAGE cast(now() as timestamp)||' | @ M15.2: Non Viewer Update: '||@@rowcount TO CLIENT
        commit


        --------------------------
        -- M15.3: Assign Non-Viewers
        --------------------------
		UPDATE V289_M08_SKY_HH_composition m08
		SET non_viewer = 0 
		
		
		
		UPDATE V289_M08_SKY_HH_composition m08
		SET non_viewer = i.non_viewer
		FROM temp_inds i
		WHERE m08.account_number = i.account_number
			AND m08.hh_person_number = i.hh_person_number
			AND i.non_viewer = 1

		UPDATE V289_M08_SKY_HH_composition m08
		SET non_viewer = 0 
		WHERE household_size = 1 
		COMMIT

        MESSAGE cast(now() as timestamp)||' | @ M15.3: Non Viewer Assigned: '||@@rowcount TO CLIENT


        --------------------------
        -- M15.4: Update Viewer hhsize in M08 HH Comp
        --------------------------
       update V289_M08_SKY_HH_composition 
        set viewer_hhsize = household_size



        -- Now just update the non-viewers with the correct number
        update V289_M08_SKY_HH_composition 
        set a.viewer_hhsize = b.viewing_size
        FROM V289_M08_SKY_HH_composition 	AS a
		JOIN temp_house						AS b ON a.account_number = b. account_number
		WHERE  a.household_size > 1 

        MESSAGE cast(now() as timestamp)||' | @ M15.4: Non-viewers adjusted hhsize: '||@@rowcount TO CLIENT
        commit


        --------------------------
        -- M15.5: Update Viewer hhsize in M07 Viewing Data
        --------------------------

        select account_number, max(viewer_hhsize) as viewer_hhsize
        into #viewer_size
        from V289_M08_SKY_HH_composition
        group by account_number
        commit

        create hg index hg1 on #viewer_size(account_number)
        commit

        update V289_M07_dp_data m07
        set viewer_hhsize = h.viewer_hhsize
        from #viewer_size h
        where   m07.account_number = h.account_number
        commit


        ---- Clean tables
--        drop table temp_inds
        commit

        drop table #viewer_size
        commit


END;

COMMIT;




