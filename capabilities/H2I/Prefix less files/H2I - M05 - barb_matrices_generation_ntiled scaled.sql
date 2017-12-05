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

        This Module goal is to generate the probability matrices from BARB data to be used for identifying
        the most likely candidate(s) of been watching TV at a given event...

**Module:

        M05: Barb Matrices Generation
                        M05.0 - Initialising Environment
                        M05.1 - Aggregating transient tables
                        M05.2 - Generating Matrices
                        M05.3 - Returning Results

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M05.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m05_barb_Matrices_generation
as begin

        MESSAGE cast(now() as timestamp)||' | Begining M05.0 - Initialising Environment' TO CLIENT
        MESSAGE cast(now() as timestamp)||' | @ M05.0: Initialising Environment DONE' TO CLIENT


---------------------------------------
-- M05.1 - Aggregating transient tables
---------------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Aggregating transient tables' TO CLIENT

        /*
                Aggregating hours watched for all dimensions, this will be the base for slicing the
                probabilities when generating the Matrices...
        */

        select  cast(start_time_of_session as date) as thedate
                        ,lookup.segment_id
                        ,skybarb.hhsize
                        ,skybarb.sex
                        ,coalesce(skybarb.ageband,'Undefined') as ageband
                        ,sum(skybarb.progscaled_duration)/60.0  as uk_hhwatched
        into    #base
        from    skybarb_fullview    as skybarb
        inner join V289_PIV_Grouped_Segments_desc    as lookup      on  skybarb.session_daypart = lookup.daypart
																	and skybarb.channel_pack    = lookup.channel_pack
																	and skybarb.programme_genre = lookup.genre
        group   by  thedate
                                ,lookup.segment_id
                                ,skybarb.hhsize
                                ,skybarb.sex
                                ,skybarb.ageband

        MESSAGE cast(now() as timestamp)||' | @ M05.1: Base Table Generation DONE: '||@@rowcount TO CLIENT
        commit

        create lf index lf1 on #base(segment_id)
        commit

        /*
                Identifying sessions on the viewing data...
        */

        select  thedate
                        ,segment_id
                        ,hhsize
                        ,session_size
                        ,sum(tot_mins_watch_scaled_per_hhsession)/60.0  as uk_hhwatched
        into    #base2
        from    (               -- here is just to determine the lenght for each session in the hh and scaled it up
                                -- using the hh weight (hoh weight)... this is to then fairly aggregate the session sizes
                                select  cast(skybarb.start_time_of_session as date) as thedate
                                                ,skybarb.household_number
                                                ,lookup.segment_id
                                                ,skybarb.hhsize
                                                ,skybarb.session_id
                                                ,count(distinct skybarb.person_number)  as session_size
                                                ,max(duration_of_session*hh_weight)     as tot_mins_watch_scaled_per_hhsession
                                from    skybarb_fullview    as skybarb
								inner join V289_PIV_Grouped_Segments_desc    as lookup   on skybarb.session_daypart = lookup.daypart
																						and skybarb.channel_pack    = lookup.channel_pack
																						and skybarb.programme_genre = lookup.genre
                                group   by  thedate
                                                        ,skybarb.household_number
                                                        ,lookup.segment_id
                                                        ,skybarb.hhsize
                                                        ,skybarb.session_id

                        )   as base
        group   by  thedate
                                ,segment_id
                                ,hhsize
                                ,session_size


    commit
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Base2 Table Generation DONE: '|| @@rowcount TO CLIENT

    create lf index lf1 on #base2(segment_id)
        create lf index lf2 on #base2(hhsize)
        commit

        commit


        MESSAGE cast(now() as timestamp)||' | @ M05.1: Aggregating transient tables DONE' TO CLIENT

------------------------------
-- M05.2 - Generating Matrices
------------------------------

------------------------------------------------------------------------------------------
------------------------------Non-Viewer Matrices definitions
------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
------------------------------1st Stage - v289_nonviewers_matrix: Used to assign viewer_sizes for each household 

        MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Generating Matrices' TO CLIENT

		MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Generating v289_nonviewers_matrix_small matrix' TO CLIENT
		
        if object_id('v289_nonviewers_matrix_small') is not null
		drop table v289_nonviewers_matrix_small
		if object_id('v289_nonviewers_matrix_big') is not null
		drop table v289_nonviewers_matrix_big


--------------------------- N-tiling the household based on the scaled tv consumption

	SELECT 		  
		 DATE(start_time_of_session) 		AS date_of_activity_db1
		, household_number
		, hhsize
		, hh_weight
		, session_id
		, min(duration_of_session) 			AS minutes
	INTO #tevent
	FROM skybarb_fullview
	WHERE processing_weight > 0
	GROUP BY 
		household_number
		, hh_weight
		, session_id
		, date_of_activity_db1
		, hhsize
---------------------------
	SELECT 
		  date_of_activity_db1
		, household_number
		, hh_weight 
		, hhsize
		, SUM(minutes) watch
		, ntile(4) over (PARTITION BY date_of_activity_db1 order by watch) as ntile_lp
	INTO #t_ntile_HH
	FROM #tevent
	GROUP BY     
		  household_number
		, date_of_activity_db1
		, hh_weight 
		, hhsize
	COMMIT 
	CREATE HG INDEX idw11 ON  #t_ntile_HH(household_number)
	COMMIT
-------------------------- Selecting viewers within households (processing_weight >0)
	SELECT 
		  household_number
		, person_number
		, processing_weight
		, SUM(progscaled_duration) watch
		,  DATE(start_time_of_session) date_of_activity_db1
	INTO #t_ntile
	FROM skybarb_fullview
	WHERE processing_weight > 0
	GROUP BY      household_number
		, person_number
		, processing_weight
		, date_of_activity_db1
-------------------------- Creating a shell for the matrix
SELECT * 
INTO #tall
FROM (SELECT row_num AS hh_size   				FROM sa_rowgenerator( 1, 8)) AS a 
CROSS JOIN (SELECT row_num AS kid               FROM sa_rowgenerator( 0, 1)) AS b
CROSS JOIN (SELECT row_num AS twenties			FROM sa_rowgenerator( 0, 1)) AS c 
CROSS JOIN (SELECT row_num AS viewer_size		FROM sa_rowgenerator( 1, 8)) AS d
CROSS JOIN (SELECT row_num AS ntile_lp			FROM sa_rowgenerator( 1, 4)) AS e
CROSS JOIN (SELECT DISTINCT DATE(start_time_of_session) AS date_of_activity_db1 FROM skybarb_fullview)		 AS f
WHERE viewer_size <= hh_size
-------------------------- Creating the v289_nonviewers_matrix	HH size <= 4		
    SELECT 
                a.hh_size
                , a.viewer_size
                , a.kid
                , a.twenties
                , a.date_of_activity_db1
                , a.ntile_lp
				, sub_viewer_size
				, sub_hh_size
				, hh_count
        , CASE WHEN sub_hh_size is null then 0.001 ELSE  CAST (sub_viewer_size as decimal(15,8)) / CAST (sub_hh_size as decimal(15,8)) END as proportion
        , SUM (proportion)              OVER (PARTITION BY a.hh_size,a.twenties, a.kid,a.ntile_lp, a.date_of_activity_db1) AS norm
        
        , COALESCE((SUM (proportion)    OVER (PARTITION BY a.hh_size,a.twenties, a.kid,a.ntile_lp, a.date_of_activity_db1 ORDER BY a.viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0)/norm AS Lower_limit
        , SUM (proportion)              OVER (PARTITION BY a.hh_size,a.twenties, a.kid,a.ntile_lp, a.date_of_activity_db1 ORDER BY a.viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/norm AS Upper_limit
     INTO    v289_nonviewers_matrix_small ---******************************
    FROM    #tall as a
    LEFT JOIN (SELECT DISTINCT hh_size
                , viewer_size
                , kid
                , twenties
                , date_of_activity_db1
                , ntile_lp
                , SUM(hh_weight) OVER (PARTITION BY hh_size,twenties, kid,viewer_size, ntile_lp, date_of_activity_db1) AS  sub_viewer_size
                , SUM(hh_weight) OVER (PARTITION BY hh_size,twenties, kid,ntile_lp, date_of_activity_db1) AS  sub_hh_size
				, COUNT(household_number) OVER (PARTITION BY hh_size,twenties, kid,viewer_size, ntile_lp, date_of_activity_db1) as hh_count
            FROM    (SELECT
                        viewer.household_number
						, hh_weight
                        , viewer_size
                        , house.hh_size
                        , hh_v.ntile_lp
                        , viewer.date_of_activity_db1
                        , house.kid
                        , house.twenties
                    FROM    (SELECT household_number, DATE(date_of_activity_db1) date_of_activity_db1, count(DISTINCT person_number) AS viewer_size
                            FROM #t_ntile
                           GROUP BY household_number, date_of_activity_db1 )  AS viewer
                    JOIN    (SELECT house_id, count(DISTINCT person) hh_size, MAX(CASE WHEN age BETWEEN 0 AND 19 THEN 1 ELSE 0 END ) kid
                                    , MAX(CASE WHEN age BETWEEN 20 AND 24 THEN 1 ELSE 0 END ) twenties
                            FROM skybarb
                            GROUP BY  house_id
                            HAVING hh_size <=4) as house    ON viewer.household_number = house.house_id
                    JOIN    #t_ntile_HH         AS hh_v     ON viewer.household_number = hh_v.household_number AND viewer.date_of_activity_db1 = hh_v.date_of_activity_db1


                    ) AS consolidated
            ) big_view ON a.hh_size         = big_view.hh_size
                        AND a.kid           = big_view.kid
                        AND a.twenties      = big_view.twenties
                        AND a.viewer_size   = big_view.viewer_size
                        AND a.ntile_lp      = big_view.ntile_lp
                        AND a.date_of_activity_db1 = big_view.date_of_activity_db1

-------------------------- Creating the v289_nonviewers_matrix_big
MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Generating v289_nonviewers_matrix_big matrix' TO CLIENT			
	SELECT * 			   
		, CAST (sub_viewer_size as decimal(15,4)) / CAST (sub_hh_size as decimal(15,4)) as proportion
		, COALESCE((SUM (proportion) 	OVER (PARTITION BY hh_size, ntile_lp, date_of_activity_db1 ORDER BY viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0) AS Lower_limit
		, SUM (proportion) 				OVER (PARTITION BY hh_size, ntile_lp, date_of_activity_db1 ORDER BY viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Upper_limit
	INTO     v289_nonviewers_matrix_big
	FROM    (SELECT DISTINCT hh_size
                , viewer_size
                , date_of_activity_db1
                , ntile_lp
                , SUM( hh_weight) OVER (PARTITION BY hh_size,viewer_size, ntile_lp, date_of_activity_db1) AS  sub_viewer_size
                , SUM( hh_weight) OVER (PARTITION BY hh_size, ntile_lp, date_of_activity_db1) AS  sub_hh_size
				, COUNT(household_number) OVER (PARTITION BY hh_size,viewer_size, ntile_lp, date_of_activity_db1) AS hh_count
            FROM    (SELECT
                        viewer.household_number
						, hh_weight
                        , viewer_size
                        , house.hh_size
                        , hh_v.ntile_lp
                        , viewer.date_of_activity_db1
                    FROM    (SELECT household_number, DATE(date_of_activity_db1) date_of_activity_db1, count(DISTINCT person_number) AS viewer_size
                            FROM #t_ntile
                           GROUP BY household_number, date_of_activity_db1 )  AS viewer
                    JOIN    (SELECT house_id, count(DISTINCT person) hh_size
                            FROM skybarb
							GROUP BY  house_id
							HAVING hh_size >4) as house    ON viewer.household_number = house.house_id
                    JOIN    #t_ntile_HH         AS hh_v     ON viewer.household_number = hh_v.household_number AND viewer.date_of_activity_db1 = hh_v.date_of_activity_db1
                    ) AS consolidated
            ) big_view
    ORDER BY hh_size, viewer_size						
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
									
MESSAGE cast(now() as timestamp)||' | M05 - Calculating V289_non_viewers_PIV'  TO CLIENT
		
if object_id('V289_non_viewers_PIV') is not null
                drop table V289_non_viewers_PIV

SELECT DISTINCT date_of_activity_db1 thedate
INTO #tdate FROM v289_nonviewers_matrix_small


SELECT a.*
	, thedate
	, COUNT(person) OVER (PARTITION BY house_id, thedate ) hh_size
	,   CAST( 0 as int) AS weight
INTO #tnpiv
FROM skybarb as a
CROSS JOIN #tdate as b

UPDATE #tnpiv
SET weight = processing_weight
FROM #tnpiv as a
JOIN #t_ntile as b ON a.house_id  =   b.household_number AND a.person = b.person_number AND a.thedate = b.date_of_activity_db1


SELECT
      case   when age between 1 and 19      then '0-19'
                        when age between 20 and 24  then '20-24'
                        when age between 25 and 34  then '25-34'
                        when age between 35 and 44  then '35-44'
                        when age between 45 and 64  then '45-64'
                        when age >= 65                  then '65+'
                end     as ageband
    , sex
    , hh_size
    , thedate
    , SUM (CASE WHEN weight = 0 THEN 1 ELSE 0 END) non_viewers
    , SUM (non_viewers) OVER (PARTITION BY hh_size,thedatE) total_non_v
    , CAST (null AS DECIMAL (15,10))         AS PIV
    , CAST (0 AS DECIMAL (15,10))         AS lower_limit
    , CAST (0 AS DECIMAL (15,10))         AS upper_limit
INTO V289_non_viewers_PIV
FROM #tnpiv
GROUP BY
      ageband
    , sex
    , thedatE
    , hh_size
-------------------------------------------------------
UPDATE V289_non_viewers_PIV
SET PIV	= CAST(non_viewers AS FLOAT) / CAST(total_non_v AS FLOAT)



SELECT *		
		, COALESCE((SUM (PIV) 	OVER (PARTITION BY hh_size, thedatE ORDER BY ageband ,sex ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0) AS Lower_limit1
		, SUM (PIV) 	OVER (PARTITION BY hh_size, thedatE ORDER BY ageband ,sex  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Upper_limit1
INTO #PIV2
FROM V289_non_viewers_PIV

UPDATE V289_non_viewers_PIV
SET lower_limit = Lower_limit1,
	upper_limit = Upper_limit1
FROM V289_non_viewers_PIV AS a 
JOIN  #PIV2 as b ON a.ageband = b.ageband AND a.sex= b.sex AND a.hh_size = b.hh_size AND a.thedatE = b.thedatE

UPDATE V289_non_viewers_PIV
SET upper_limit = ROUND (upper_limit, 8)
WHERE upper_limit > 0.9999999

        -- PIV sex/age

        /*
                Now generating the matrix to identify who is most likely to be watching TV based
                BARB distributions by sex and age over specific part of the day, channel pack
                and programme genre...
        */

        if object_id('v289_genderage_matrix') is not null
                drop table v289_genderage_matrix

        commit

        select  base.*
                        ,cast(base.uk_hhwatched as decimal(15,4)) / cast(totals.tot_uk_hhwatched as decimal(15,4)) as PIV
        into    v289_genderage_matrix
        from    #base        as base
                        inner join  (
                                                        select  thedate
                                                                        ,segment_id
                                                                        ,sum(uk_hhwatched)  as tot_uk_hhwatched
                                                        from    #base
                                                        group   by  thedate
                                                                                ,segment_id
                                                )   as totals
                        on  base.thedate            = totals.thedate
                        and base.segment_id         = totals.segment_id
        where   totals.tot_uk_hhwatched > 0

        commit

        create lf index lf1 on v289_genderage_matrix(segment_id)
        commit

        grant select on v289_genderage_matrix to vespa_group_low_security
        commit

        MESSAGE cast(now() as timestamp)||' | @ M05.1: Sex/Age Matrix Generation DONE (v289_genderage_matrix)' TO CLIENT

        -- PIV Session size

        /*
                This is the probability matrix to determine the size of the session, how many
                people were watching TV on a given date, an specific part of the day, household
                size, channel pack and programme genre. All based on BARB distributions...
        */

        if object_id('v289_sessionsize_matrix') is not null
                drop table v289_sessionsize_matrix

        commit

        select  base.*
                        ,cast(base.uk_hhwatched as decimal(15,4)) / cast(totals.tot_uk_hhwatched as decimal(15,4)) as proportion
                        ,coalesce((SUM (proportion) OVER (PARTITION BY base.thedate,base.segment_ID, base.hhsize ORDER BY base.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0) AS Lower_limit
            ,SUM (proportion) OVER (PARTITION BY base.thedate,base.segment_ID, base.hhsize ORDER BY base.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Upper_limit
        into    v289_sessionsize_matrix
        from    #base2  as base
                        inner join  (
                                                        select  thedate
                                    ,segment_id
                                                                        ,hhsize
                                                                        ,sum(uk_hhwatched) as tot_uk_hhwatched
                                                        from    #base2
                                                        group   by  thedate
                                                                                ,segment_id
                                                                                ,hhsize
                                                )   as totals
                        on  base.thedate            = totals.thedate
                        and base.segment_id         = totals.segment_id
                        and base.hhsize             = totals.hhsize
    where   totals.tot_uk_hhwatched > 0

        commit

    create lf index lf1 on v289_sessionsize_matrix(segment_id)
        create lf index lf2 on v289_sessionsize_matrix(hhsize)
        commit

        grant select on v289_sessionsize_matrix to vespa_group_low_security
        commit

        MESSAGE cast(now() as timestamp)||' | @ M05.1: Session size Matrix Generation DONE (v289_sessionsize_matrix)' TO CLIENT


        -- DEFAULT session size matrix

        DECLARE @min DECIMAL(8,7)

    if object_id('v289_sessionsize_matrix_ID') is not null
        drop table v289_sessionsize_matrix_ID

    commit

    SELECT  segment_id
                        ,session_size
                        ,hhsize
                        ,SUM(uk_hhwatched) uk_hhwatched
                        ,SUM(uk_hhwatched) OVER (PARTITION BY segment_id, hhsize) AS total_watched
                        ,CAST (uk_hhwatched AS DECIMAL(15,4)) / CAST (total_watched  AS DECIMAL(15,4)) AS proportion
    INTO    v289_sessionsize_matrix_ID
        FROM    v289_sessionsize_matrix
    group   by  segment_id
                        ,session_size
                        ,hhsize

        SET @min  = (
                    SELECT  min(proportion)/2
                    FROM    v289_sessionsize_matrix_ID
                    where   proportion >0
                )


        if object_id('v289_sessionsize_matrix_default') is not null
        drop table v289_sessionsize_matrix_default

        COMMIT
        SELECT  a.segment_ID
                ,b.hhsize
                ,sx.session_size
                ,COALESCE(c.proportion, @min)    AS proportion
                ,SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize )  AS norm
                ,coalesce((SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize  ORDER BY sx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) / norm),0)    AS Lower_limit
                ,coalesce((SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize  ORDER BY sx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / norm),0)    AS Upper_limit
        INTO    v289_sessionsize_matrix_default
        FROM    (
                SELECT  DISTINCT
                        segment_ID
                FROM    v289_sessionsize_matrix_ID
            )   as a
                CROSS JOIN  (
                            SELECT  DISTINCT
                                    CASE    WHEN hhsize >8 THEN 8
                                            ELSE hhsize
                                    END     as hhsize
                            FROM    v289_sessionsize_matrix_ID
                        )   AS b
                CROSS JOIN  (
                            SELECT  DISTINCT
                                    CASE    WHEN hhsize >8 THEN 8
                                            ELSE hhsize
                                    END     as session_size
                            FROM    v289_sessionsize_matrix_ID
                        )   AS sx
                LEFT JOIN   v289_sessionsize_matrix_ID AS c
            ON  a.segment_id = c.segment_id
            AND b.hhsize = c.hhsize
            AND c.session_size = sx.session_size
            AND c. proportion > 0
        WHERE   b.hhsize >= sx.session_size

        DELETE FROM v289_sessionsize_matrix_default
        WHERE session_size > hhsize

        COMMIT

        CREATE LF INDEX UW ON v289_sessionsize_matrix_default(segment_ID)
        CREATE LF INDEX UQ ON v289_sessionsize_matrix_default(hhsize)

        COMMIT

        DROP TABLE v289_sessionsize_matrix_ID

        GRANT ALL ON v289_sessionsize_matrix_default  TO vespa_group_low_security

        COMMIT


        MESSAGE cast(now() as timestamp)||' | @ M05.1: DEFAULT Session size Matrix Generation DONE (v289_sessionsize_matrix_default)' TO CLIENT

        MESSAGE cast(now() as timestamp)||' | @ M05.1: Generating Matrices DONE' TO CLIENT


----------------------------
-- M05.3 - Returning Results
----------------------------

        MESSAGE cast(now() as timestamp)||' | M05 Finished' TO CLIENT

end;

commit;
grant execute on v289_m05_barb_Matrices_generation to vespa_group_low_security;
commit;
