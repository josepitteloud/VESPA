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
            inner join V289_PIV_Grouped_Segments_desc    as lookup
            on  skybarb.session_daypart = lookup.daypart
            and skybarb.channel_pack    = lookup.channel_pack
            and skybarb.programme_genre = lookup.genre
        group   by  thedate
                                ,lookup.segment_id
                                ,skybarb.hhsize
                                ,skybarb.sex
                                ,skybarb.ageband

        commit

        create lf index lf1 on #base(segment_id)
        commit

        MESSAGE cast(now() as timestamp)||' | @ M05.1: Base Table Generation DONE' TO CLIENT


        /*
                Identifying sessions on the viewing data...
        */

        select  thedate
                        ,segment_id
                        ,hhsize
                        ,session_size
                        ,sum(tot_mins_watch_scaled_per_hhsession)/60.0  as uk_hhwatched
        into    #base2
        from    (
                                -- here is just to determine the lenght for each session in the hh and scaled it up
                                -- using the hh weight (hoh weight)... this is to then fairly aggregate the session sizes
                                select  cast(skybarb.start_time_of_session as date) as thedate
                                                ,skybarb.household_number
                                                ,lookup.segment_id
                                                ,skybarb.hhsize
                                                ,skybarb.session_id
                                                ,count(distinct skybarb.person_number)  as session_size
                                                ,max(duration_of_session*hh_weight)     as tot_mins_watch_scaled_per_hhsession
                                from    skybarb_fullview    as skybarb
                                                inner join V289_PIV_Grouped_Segments_desc    as lookup
                                                on  skybarb.session_daypart = lookup.daypart
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

    create lf index lf1 on #base2(segment_id)
        create lf index lf2 on #base2(hhsize)
        commit

        commit

        MESSAGE cast(now() as timestamp)||' | @ M05.1: Base2 Table Generation DONE' TO CLIENT

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

		MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Generating Matrices' TO CLIENT
		
        if object_id('v289_nonviewers_matrix_small') is not null
		drop table v289_nonviewers_matrix_small
		if object_id('v289_nonviewers_matrix_big') is not null
		drop table v289_nonviewers_matrix_big
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
--------------------------------------------------------------------------------------------------------		
--******************************************************************************************************
-------------------------- CHRIS MATRIX CODE
--******************************************************************************************************
--------------------------------------------------------------------------------------------------------
IF object_id('prop_table') IS NOT NULL 	DROP TABLE prop_table

SELECT a.*
	, CASE 		WHEN c.person_number IS NOT NULL		THEN 1.000000		ELSE 0.00000		END AS viewer_flag
	, CASE 		WHEN c.person_number IS NULL			THEN 1.000000		ELSE 0.0000			END AS nv_flag
	, CASE 		WHEN c.person_number IS NOT NULL		THEN processing_weight		ELSE 0		END AS viewer_weight
	, CASE 		WHEN c.person_number IS NULL			THEN processing_weight		ELSE 0		END AS nv_weight
	, count(a.household_number) OVER (PARTITION BY a.household_number) AS hhsize
INTO #tt1
FROM barb_weights AS a
INNER JOIN (	SELECT DISTINCT house_id	FROM skybarb	) AS b ON a.household_number = b.house_id --> up to here result [1]
LEFT JOIN (	SELECT DISTINCT household_number		, person_number	FROM skybarb_fullview
			WHERE DATE (start_time_of_session) = '2013-09-20'	) AS c ON a.household_number = c.household_number
																		AND a.person_number = c.person_number

SELECT *
	, sum(nv_flag) OVER (PARTITION BY household_number) AS nvsize
INTO #tt2
FROM #tt1

SELECT hhsize
	, nvsize
	, sum(nv_weight) AS sum_nv_weights_hhnv
	, (sum(nv_weight) + sum(viewer_weight)) AS sum_weights_for_nvsize_hhnv
	, sum(nv_flag) / (sum(nv_flag) + sum(viewer_flag)) AS nv_prop_by_count_hhnv
	, sum(nv_weight) / (sum(nv_weight) + sum(viewer_weight)) AS nv_prop_by_weight_hhnv
INTO #by_hhsize_nvsize
FROM #tt2
WHERE nvsize <> hhsize
GROUP BY hhsize
	, nvsize

--select * from #by_hhsize_nvsize
SELECT hhsize
	, sum(nv_weight) AS sum_nv_weights_hh
	, (sum(nv_weight) + sum(viewer_weight)) AS sum_weights_for_nvsize_hh
	, sum(nv_flag) / (sum(nv_flag) + sum(viewer_flag)) AS nv_prop_by_count_hh
	, sum(nv_weight) / (sum(nv_weight) + sum(viewer_weight)) AS nv_prop_by_weight_hh
INTO #by_hhsize
FROM #tt2
WHERE nvsize <> hhsize
GROUP BY hhsize
ORDER BY hhsize

-- select * from #by_hhsize
SELECT hhnv.hhsize
	, sum_ppl
	, hhnv.nvsize
	, sum_nv_weights_hhnv
	, sum_weights_for_nvsize_hhnv
	, nv_prop_by_count_hhnv
	, nv_prop_by_weight_hhnv
	, sum_nv_weights_hh
	, sum_weights_for_nvsize_hh
	, nv_prop_by_count_hh
	, nv_prop_by_weight_hh
	, nv_prop_by_weight_hhnv * sum_weights_for_nvsize_hhnv / sum_weights_for_nvsize_hh AS proportion
	, CAST(0 AS FLOAT) AS total_prop
INTO prop_table
FROM (SELECT hhsize
		, nvsize
		, (sum(nv_flag) + sum(viewer_flag)) AS sum_ppl
		, sum(nv_weight) AS sum_nv_weights_hhnv
		, (sum(nv_weight) + sum(viewer_weight)) AS sum_weights_for_nvsize_hhnv
		, sum(nv_flag) / (sum(nv_flag) + sum(viewer_flag)) AS nv_prop_by_count_hhnv
		, sum(nv_weight) / (sum(nv_weight) + sum(viewer_weight)) AS nv_prop_by_weight_hhnv
	FROM #tt2
	WHERE nvsize <> hhsize
	GROUP BY hhsize
		, nvsize
	) hhnv
INNER JOIN
	(SELECT hhsize
		, sum(nv_weight) AS sum_nv_weights_hh
		, (sum(nv_weight) + sum(viewer_weight)) AS sum_weights_for_nvsize_hh
		, sum(nv_flag) / (sum(nv_flag) + sum(viewer_flag)) AS nv_prop_by_count_hh
		, sum(nv_weight) / (sum(nv_weight) + sum(viewer_weight)) AS nv_prop_by_weight_hh
	INTO #by_hhsize
	FROM #tt2
	WHERE nvsize <> hhsize
	GROUP BY hhsize
	) hh ON hh.hhsize = hhnv.hhsize

--update prop_table
SELECT hhsize
	, cast(sum(proportion) AS FLOAT) AS total_prop
INTO #t1
FROM prop_table
GROUP BY hhsize

UPDATE prop_table pt
SET proportion = 1 - t1.total_prop
FROM prop_table pt
INNER JOIN #t1 t1 ON pt.hhsize = t1.hhsize
WHERE nvsize = 0

-------------------------- Creating the v289_nonviewers_matrix	
SELECT
      hhsize
    , hhsize - nvsize as viewing_size
    , CAST('2013-09-20' AS DATE) AS date_of_activity_db1
    , proportion
    , SUM (proportion)              OVER (PARTITION BY hhsize, date_of_activity_db1) AS norm
    , COALESCE((SUM (proportion)    OVER (PARTITION BY hhsize, date_of_activity_db1 ORDER BY viewing_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0)/norm AS Lower_limit
    , SUM (proportion)              OVER (PARTITION BY hhsize, date_of_activity_db1 ORDER BY viewing_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/norm AS Upper_limit
INTO v289_vsizealloc_matrix
FROM prop_table
	
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------ATTENTION CHRIS
-------------------------------------------------------------------------------------------------------	FROM HERE IS MY CODE. You should replace it by your code
--------------------------------------------------------------------------------------------------------
	
  -- PIV non-viewers
  if object_id('v289_nonviewers_matrix') is not null   drop table v289_nonviewers_matrix
  v289_nonviewers_matrix
  COMMIT
	select  thedate
				,gender
				,ageband
				,hhsize
				,1.0000 - cast(viewers_count as decimal(15,4)) / cast(base_person_count as decimal(15,4)) as PIV
	into    v289_nonviewers_matrix
	from   (
						select  base.thedate
										,case   when age <= 19 then 'U'
														when sex = 'Male' then 'M'
														else 'F'
										end     as gender
										,case   when age <= 19                  then '0-19'
														when age between 20 and 24      then '20-24'
														when age between 25 and 34      then '25-34'
														when age between 35 and 44      then '35-44'
														when age between 45 and 64      then '45-64'
																														else '65+'
										end     as ageband
										,hhsize
										,sum(case when viewers.person_number is null then 0 else processing_weight end) as viewers_count
										,sum(processing_weight) as base_person_count
						from   (
												select  hhd_views.thedate
																,a.house_id
																,a.person
																,a.age
																,a.sex
												from    skybarb a
																inner join      (
																								select  cast(start_time_of_session as date) as thedate
																												,household_number
																								from    skybarb_fullview
																								group   by      thedate
																														,household_number
																						)       as hhd_views
																on      a.house_id = hhd_views.household_number
										)       as base
										inner join      (
																		select  household_number as house_id
																						,count(1)       as hhsize
																		from    barb_weights
																		group   by      house_id
																)       as s
										on      base.house_id = s.house_id
										inner join      barb_weights w
										on      base.house_id = w.household_number
										and base.person = w.person_number
										left join       (
																		select  cast(start_time_of_session as date) as thedate
																						,household_number
																						,person_number
																		from    skybarb_fullview
																		group   by      thedate
																								,household_number
																								,person_number
																)       as viewers
										on      base.thedate    = viewers.thedate
										and     base.house_id   = viewers.household_number
										and     base.person     = viewers.person_number
						group   by  base.thedate
												,gender
												,ageband
												,hhsize
				)       as summary





	/*						
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
    , thedatE
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

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
        -- PIV sex/age
*/

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
