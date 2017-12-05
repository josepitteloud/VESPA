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
--------------------------------------------------------------------------------------------------------                
--******************************************************************************************************
-------------------------- CHRIS MATRIX CODE
--******************************************************************************************************
--------------------------------------------------------------------------------------------------------

                if  exists(  select tname from syscatalog 
                        where creator = user_name()
                        and upper(tname) = upper('prop_table')
                        and     tabletype = 'TABLE')
						DROP TABLE prop_table
                if  exists(  select tname from syscatalog 
                        where creator = user_name()
                        and upper(tname) = upper('v289_vsizealloc_matrix_small')
                        and     tabletype = 'TABLE')						
						DROP TABLE v289_vsizealloc_matrix_small
                if  exists(  select tname from syscatalog 
                        where creator = user_name()
                        and upper(tname) = upper('v289_vsizealloc_matrix_big')
                        and     tabletype = 'TABLE')						
						DROP TABLE v289_vsizealloc_matrix_big     



SELECT a.*
        , CASE          WHEN c.person_number IS NOT NULL                THEN 1.000000           ELSE 0.00000            END AS viewer_flag
        , CASE          WHEN c.person_number IS NULL                    THEN 1.000000           ELSE 0.0000                     END AS nv_flag
        , CASE          WHEN c.person_number IS NOT NULL                THEN processing_weight          ELSE 0          END AS viewer_weight
        , CASE          WHEN c.person_number IS NULL                    THEN processing_weight          ELSE 0          END AS nv_weight
		, kid
		, twenties
        , count(a.household_number) OVER (PARTITION BY a.household_number) AS hhsize
INTO #tt1
FROM barb_weights AS a
INNER JOIN (    SELECT    house_id
								, MAX(CASE WHEN age BETWEEN 0 AND 19 THEN 1 ELSE 0 END ) kid
								, MAX(CASE WHEN age BETWEEN 20 AND 24 THEN 1 ELSE 0 END ) twenties       
			FROM skybarb GROUP BY house_id) AS b ON a.household_number = b.house_id --> up to here result [1]
LEFT JOIN (     SELECT DISTINCT household_number                
								, person_number 
								, DATE (start_time_of_session) AS date_of_activity_db1
				FROM skybarb_fullview
                ) AS c ON a.household_number = c.household_number
				    AND a.person_number = c.person_number
					AND a.date_of_activity_db1 = c.date_of_activity_db1

SELECT *
        , sum(nv_flag) OVER (PARTITION BY household_number) AS nvsize
INTO #tt2
FROM #tt1
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
select          hhsize
                ,hhsize - nvsize   as viewer_size
				,kid
				,twenties
                ,sum(processing_weight) as weight_in_hh
				, date_of_activity_db1
into            #tt32
from            #tt2
where           hhsize <> nvsize
				AND hhsize = 2
group by        hhsize
				, viewer_size
				, date_of_activity_db1
				, twenties
				, kid

select          hhsize
				, twenties
				, kid
                ,sum(processing_weight) as hh_size_weight
				,date_of_activity_db1
into            #tt42
from            #tt2
where           hhsize <> nvsize
group by        hhsize
				, date_of_activity_db1
				, twenties
				, kid
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------

select          hhsize
                ,hhsize - nvsize   as viewer_size
                ,sum(processing_weight) as weight_in_hh
				, date_of_activity_db1
into            #tt3
from            #tt2
where           hhsize <> nvsize
				AND hhsize <> 2
group by        hhsize
				, viewer_size
				, date_of_activity_db1

select          hhsize
                ,sum(processing_weight) as hh_size_weight
				,date_of_activity_db1
into            #tt4
from            #tt2
where           hhsize <> nvsize
group by        hhsize, date_of_activity_db1


-------------------------- Creating the v289_nonviewers_matrix_small

SELECT 		  #tt32.hhsize 					AS hh_size
            , viewer_size
            , #tt32.date_of_activity_db1
			, #tt32.kid
			, #tt32.twenties
            , cast(weight_in_hh as decimal(15,6)) / cast(hh_size_weight as decimal(15,6)) as proportion
            , SUM (proportion)              OVER (PARTITION BY #tt32.hhsize, #tt32.date_of_activity_db1, #tt32.kid, #tt32.twenties) AS norm
            , COALESCE((SUM (proportion)    OVER (PARTITION BY #tt32.hhsize, #tt32.date_of_activity_db1, #tt32.kid, #tt32.twenties ORDER BY viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0)/norm AS Lower_limit
            , SUM (proportion)              OVER (PARTITION BY #tt32.hhsize, #tt32.date_of_activity_db1, #tt32.kid, #tt32.twenties ORDER BY viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/norm AS Upper_limit
INTO        v289_vsizealloc_matrix_small
FROM 		#tt32 
INNER JOIN 	#tt42 on #tt32.hhsize = #tt42.hhsize 
				 AND #tt32.date_of_activity_db1 = #tt42.date_of_activity_db1
				 AND #tt32.kid = #tt42.kid
				 AND #tt32.twenties = #tt42.twenties


-------------------------- Creating the v289_nonviewers_matrix_big

select          #tt3.hhsize as hh_size
                ,viewer_size
                , #tt3.date_of_activity_db1
                , cast(weight_in_hh as decimal(15,6)) / cast(hh_size_weight as decimal(15,6)) as proportion
                , SUM (proportion)              OVER (PARTITION BY #tt3.hhsize, #tt3.date_of_activity_db1) AS norm
                , COALESCE((SUM (proportion)    OVER (PARTITION BY #tt3.hhsize, #tt3.date_of_activity_db1 ORDER BY viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0)/norm AS Lower_limit
                , SUM (proportion)              OVER (PARTITION BY #tt3.hhsize, #tt3.date_of_activity_db1 ORDER BY viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/norm AS Upper_limit
INTO            v289_vsizealloc_matrix_big
from            #tt3 inner join #tt4 on #tt3.hhsize = #tt4.hhsize AND #tt3.date_of_activity_db1 = #tt4.date_of_activity_db1





/*
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

*/

/*
-------------------------- Creating the v289_nonviewers_matrix
SELECT
      hhsize                    AS hh_size
    , hhsize - nvsize   as viewing_size
    , CAST('2013-09-20' AS DATE) AS date_of_activity_db1
    , proportion
    , SUM (proportion)              OVER (PARTITION BY hhsize, date_of_activity_db1) AS norm
    , COALESCE((SUM (proportion)    OVER (PARTITION BY hhsize, date_of_activity_db1 ORDER BY viewing_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0)/norm AS Lower_limit
    , SUM (proportion)              OVER (PARTITION BY hhsize, date_of_activity_db1 ORDER BY viewing_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/norm AS Upper_limit
INTO v289_vsizealloc_matrix
FROM prop_table
*/
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------



        MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Generating Matrices' TO CLIENT

                if  exists(  select tname from syscatalog 
                        where creator = user_name()
                        and upper(tname) = upper('v289_nonviewers_matrix')
                        and     tabletype = 'TABLE')
                drop table v289_nonviewers_matrix

        commit

        -- PIV non-viewers
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




        -- PIV sex/age

        /*
                Now generating the matrix to identify who is most likely to be watching TV based
                BARB distributions by sex and age over specific part of the day, channel pack
                and programme genre...
        */
                if exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_genderage_matrix')
                        and     tabletype = 'TABLE')
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
                if exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_sessionsize_matrix')
                        and     tabletype = 'TABLE')
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

                if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_sessionsize_matrix_ID')
                        and     tabletype = 'TABLE')
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
		SELECT row_num AS id INTO #tnum FROM sa_rowgenerator( 1,8)

                if exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_sessionsize_matrix_default')
                        and     tabletype = 'TABLE')		
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
                FROM    V289_PIV_Grouped_Segments_desc
            )   as a
		CROSS JOIN  (SELECT id AS hhsize 		FROM #tnum)   AS b
		CROSS JOIN  (SELECT id AS session_size 	FROM #tnum)   AS sx
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
