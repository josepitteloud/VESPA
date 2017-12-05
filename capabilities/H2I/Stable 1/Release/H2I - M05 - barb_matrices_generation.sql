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

create or replace procedure ${SQLFILE_ARG001}.v289_m05_barb_Matrices_generation
as begin

        MESSAGE cast(now() as timestamp)||' | Begining M05.0 - Initialising Environment' TO CLIENT
        MESSAGE cast(now() as timestamp)||' | @ M05.0: Initialising Environment DONE' TO CLIENT

    DECLARE @min DECIMAL(8,7)
---------------------------------------
-- M05.1 - Aggregating transient tables
---------------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Aggregating transient tables' TO CLIENT

        /*
                Aggregating hours watched for all dimensions, this will be the base for slicing the
                probabilities when generating the Matrices...
        */

        select cast(start_time_of_session as date) as thedate, household_number, session_id, count(distinct person_number) session_size
        into #z
        from skybarb_fullview
        group by thedate, household_number, session_id
        commit

        create lf index in1 on #z(thedate)
        create hg index in2 on #z(household_number)
        create hg index in3 on #z(session_id)
        commit


        select cast(start_time_of_session as date) as thedate, household_number, count(distinct person_number) as v_size
        into #x
        from skybarb_fullview
        group by thedate, household_number
        commit

        create lf index in1 on #x(thedate)
        create hg index in2 on #x(household_number)
        commit



        select  z.thedate
                        ,lookup.segment_id
                        ,skybarb.hhsize
                        ,case when z.session_size = x.v_size then 1 else 0 end as full_session_flag
                        ,skybarb.sex
                        ,coalesce(skybarb.ageband,'Undefined') as ageband
                        ,sum(skybarb.progscaled_duration)/60.0  as uk_hhwatched
        into    #base
        from    skybarb_fullview    as skybarb
            inner join V289_PIV_Grouped_Segments_desc    as lookup
                        on  skybarb.session_daypart = lookup.daypart
                        and skybarb.channel_pack    = lookup.channel_pack
                        and skybarb.programme_genre = lookup.genre
            inner join
                        #z z on cast(start_time_of_session as date) = z.thedate and skybarb.household_number = z.household_number and skybarb.session_id = z.session_id
            inner join
                        #x x on z.thedate = x.thedate and z.household_number = x.household_number
        group   by  z.thedate
                                ,lookup.segment_id
                                ,skybarb.hhsize
                                ,full_session_flag
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
                        ,viewing_size
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
                                                ,x.viewing_size
                                                ,skybarb.session_id
                                                ,count(distinct skybarb.person_number)  as session_size
                                                ,max(duration_of_session*hh_weight)     as tot_mins_watch_scaled_per_hhsession
                                from    skybarb_fullview    as skybarb
                                                JOIN (  SELECT count(distinct person_number)  as viewing_size
                                                                , household_number
                                                                , cast(start_time_of_session as date) as thedatex
                                                        FROM   skybarb_fullview
                                                        GROUP BY household_number, thedatex) as x   ON x.household_number =  skybarb.household_number AND x.thedatex = thedate
                                                inner join V289_PIV_Grouped_Segments_desc    as lookup
                                                on  skybarb.session_daypart = lookup.daypart
                                                and skybarb.channel_pack    = lookup.channel_pack
                                                and skybarb.programme_genre = lookup.genre
                                group   by  thedate
                                                        ,skybarb.household_number
                                                        ,lookup.segment_id
                                                        ,skybarb.hhsize
                                                        ,skybarb.session_id
                                                        ,x.viewing_size

                        )   as base
        group   by  thedate
                                ,segment_id
                                ,hhsize
                                ,viewing_size
                                ,session_size


    commit

    create lf index lf1 on #base2(segment_id)
        create lf index lf2 on #base2(hhsize)
        create lf index lf3 on #base2(viewing_size)
        commit

        commit

        MESSAGE cast(now() as timestamp)||' | @ M05.1: Base2 Table Generation DONE' TO CLIENT


       --- Weight of individual viewers
        select z.thedate
                        ,lookup.segment_id
                        ,skybarb.hhsize
                        ,case when z.session_size = x.v_size then 1 else 0 end as full_session_flag
                        ,skybarb.sex
                        ,coalesce(skybarb.ageband,'Undefined') as ageband
                        ,skybarb.household_number, skybarb.person_number
                        ,min(skybarb.processing_weight)  as ind_weight
        into    #ind_weights
        from    skybarb_fullview    as skybarb
            inner join V289_PIV_Grouped_Segments_desc    as lookup
                        on  skybarb.session_daypart = lookup.daypart
                        and skybarb.channel_pack    = lookup.channel_pack
                        and skybarb.programme_genre = lookup.genre
            inner join
                        #z z on cast(start_time_of_session as date) = z.thedate and skybarb.household_number = z.household_number and skybarb.session_id = z.session_id
            inner join
                        #x x on z.thedate = x.thedate and z.household_number = x.household_number
        group   by  z.thedate
                                ,lookup.segment_id
                                ,skybarb.hhsize
                                ,full_session_flag
                                ,skybarb.sex
                                ,skybarb.ageband
                                ,skybarb.household_number, skybarb.person_number
        commit




       select  thedate
                        ,segment_id
                        ,hhsize
                        ,full_session_flag
                        ,sex
                        ,ageband
                        ,sum(ind_weight) as segment_weight
       into     #viewer_weights
       from     #ind_weights
       group by thedate
                        ,segment_id
                        ,hhsize
                        ,full_session_flag
                        ,sex
                        ,ageband
       commit

       create lf index lf1 on #viewer_weights(segment_id)
       commit




        MESSAGE cast(now() as timestamp)||' | @ M05.1: Aggregating transient tables DONE' TO CLIENT

------------------------------
-- M05.2 - Generating Matrices
------------------------------
--------------------------------------------------------------------------------------------------------
--******************************************************************************************************
-------------------------- CHRIS MATRIX CODE
--******************************************************************************************************
--------------------------------------------------------------------------------------------------------.
IF EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'PROP_TABLE')   drop table prop_table
IF EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'V289_VSIZEALLOC_MATRIX_SMALL')   drop table v289_vsizealloc_matrix_small
IF EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'V289_VSIZEALLOC_MATRIX_BIG')   drop table v289_vsizealloc_matrix_big

SELECT  house_id
        , MAX(processing_weight) AS hh_processing_weight
INTO #hh_W
FROM skybarb AS a
JOIN barb_weights as b ON b.household_number = a.house_id
WHERE head = 1
GROUP BY house_id
COMMIT

CREATE HG INDEX id1 ON #hh_W(house_id)
COMMIT

SELECT DISTINCT a.*, hh_processing_weight
        , CASE          WHEN c.person_number IS NOT NULL                THEN 1.000000           ELSE 0.00000            END AS viewer_flag
        , CASE          WHEN c.person_number IS NULL                    THEN 1.000000           ELSE 0.0000                     END AS nv_flag
        , CASE          WHEN c.person_number IS NOT NULL    THEN hh_processing_weight          ELSE 0          END AS viewer_weight
        , CASE          WHEN c.person_number IS NULL        THEN hh_processing_weight          ELSE 0          END AS nv_weight
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
INNER JOIN  #hh_W as w ON w.house_id = a.household_number

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
                ,sum(hh_processing_weight) as weight_in_hh
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
                ,sum(hh_processing_weight) as hh_size_weight
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
                ,sum(hh_processing_weight) as weight_in_hh
                , date_of_activity_db1
into            #tt3
from            #tt2
where           hhsize <> nvsize
                AND hhsize <> 2
group by        hhsize
                , viewer_size
                , date_of_activity_db1

select          hhsize
                ,sum(hh_processing_weight) as hh_size_weight
                ,date_of_activity_db1
into            #tt4
from            #tt2
where           hhsize <> nvsize
group by        hhsize, date_of_activity_db1


-------------------------- Creating the v289_nonviewers_matrix_small

SELECT        #tt32.hhsize                  AS hh_size
            , viewer_size
            , #tt32.date_of_activity_db1
            , #tt32.kid
            , #tt32.twenties
            , cast(weight_in_hh as decimal(15,6)) / cast(hh_size_weight as decimal(15,6)) as proportion
            , SUM (proportion)              OVER (PARTITION BY #tt32.hhsize, #tt32.date_of_activity_db1, #tt32.kid, #tt32.twenties) AS norm
            , COALESCE((SUM (proportion)    OVER (PARTITION BY #tt32.hhsize, #tt32.date_of_activity_db1, #tt32.kid, #tt32.twenties ORDER BY viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0)/norm AS Lower_limit
            , SUM (proportion)              OVER (PARTITION BY #tt32.hhsize, #tt32.date_of_activity_db1, #tt32.kid, #tt32.twenties ORDER BY viewer_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/norm AS Upper_limit
INTO        v289_vsizealloc_matrix_small
FROM        #tt32
INNER JOIN  #tt42 on #tt32.hhsize = #tt42.hhsize
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





---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------



        MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Generating Matrices' TO CLIENT

        IF EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'V289_NONVIEWERS_MATRIX')   drop table v289_nonviewers_matrix

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
                                                        ,case   when age <= 11                  then '0-11'
                                                                when age between 12 and 19      then '12-19'
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
        IF EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'V289_GENDERAGE_MATRIX')   drop table v289_genderage_matrix

        commit

        select  base.*
                        ,cast(base.uk_hhwatched as decimal(15,4)) / cast(totals.tot_uk_hhwatched as decimal(15,4)) as PIV -- this isn't used
                        ,segment_weight
        into    v289_genderage_matrix
        from    #base        as base
        inner join #viewer_weights v on         base.thedate = v.thedate
                                        and     base.segment_id = v.segment_id
                                        and     base.hhsize     = v.hhsize
                                        and     base.full_session_flag = v.full_session_flag
                                        and     base.sex = v.sex
                                        and     base.ageband = v.ageband
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

        IF EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'V289_SESSIONSIZE_MATRIX')   drop table v289_sessionsize_matrix

        commit


        select              base.thedate
                        ,   base.segment_id
                        ,   base.viewing_size
                        ,   base.session_size
                        ,   SUM(uk_hhwatched) AS uk_hours_watched
                        ,   tot_uk_hhwatched
                        ,   cast(uk_hours_watched as decimal(15,4)) / cast(totals.tot_uk_hhwatched as decimal(15,4)) as proportion
                        ,   coalesce((SUM (proportion) OVER (PARTITION BY base.thedate,base.segment_ID, base.viewing_size ORDER BY base.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0) AS Lower_limit
                        ,   SUM (proportion) OVER (PARTITION BY base.thedate,base.segment_ID, base.viewing_size ORDER BY base.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Upper_limit
        INTO    v289_sessionsize_matrix
        from    #base2  as base
                        inner join  (
                                                        select  thedate
                                                                        ,segment_id
                                                                        ,viewing_size
                                                                        ,sum(uk_hhwatched) as tot_uk_hhwatched
                                                        from    #base2
                                                        group   by  thedate
                                                                        ,segment_id
                                                                        ,viewing_size
                                                )   as totals
                        on  base.thedate            = totals.thedate
                        and base.segment_id         = totals.segment_id
                        and base.viewing_size             = totals.viewing_size
    where   totals.tot_uk_hhwatched > 0
    GROUP BY

     base.thedate
                        ,   base.segment_id
                        ,   base.viewing_size
                        ,   base.session_size
                        , tot_uk_hhwatched
        commit

    create lf index lf1 on v289_sessionsize_matrix(segment_id)
        create lf index lf2 on v289_sessionsize_matrix(viewing_size)
        commit

        grant select on v289_sessionsize_matrix to vespa_group_low_security
        commit

        MESSAGE cast(now() as timestamp)||' | @ M05.1: Session size Matrix Generation DONE (v289_sessionsize_matrix)' TO CLIENT


        -- DEFAULT session size matrix



    IF EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'V289_SESSIONSIZE_MATRIX_ID')   drop table v289_sessionsize_matrix_ID

    commit

     SELECT  segment_id
                        ,session_size
                        ,viewing_size
                        ,SUM(v289_sessionsize_matrix.uk_hours_watched) uk_hours_watched
                        ,SUM(uk_hours_watched) OVER (PARTITION BY segment_id, viewing_size) AS total_watched
                        ,CAST (uk_hours_watched AS DECIMAL(15,4)) / CAST (total_watched  AS DECIMAL(15,4)) AS proportion
    INTO    v289_sessionsize_matrix_ID
        FROM    v289_sessionsize_matrix
    group   by  segment_id
                        ,session_size
                        ,viewing_size
    create hg index hg1 on v289_sessionsize_matrix_ID(session_size)
    create hg index hg2 on v289_sessionsize_matrix_ID(viewing_size)
    commit

        SET @min  = (
                    SELECT  min(proportion)/2
                    FROM    v289_sessionsize_matrix_ID
                    where   proportion >0
                )


        IF  EXISTS(SELECT tname FROM syscatalog WHERE creator = ${SQLFILE_ARG001} and tabletype = 'TABLE' and upper(tname) = 'V289_SESSIONSIZE_MATRIX_DEFAULT')   drop table v289_sessionsize_matrix_default

        
        --SELECT row_num AS id INTO #tnum FROM sa_rowgenerator( 1,8)
        
        COMMIT
        SELECT  a.segment_ID
                ,b.viewing_size
                ,sx.session_size
                ,COALESCE(c.proportion, @min)    AS proportion
                ,SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.viewing_size )  AS norm
                ,coalesce((SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.viewing_size  ORDER BY sx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) / norm),0)    AS Lower_limit
                ,coalesce((SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.viewing_size  ORDER BY sx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / norm),0)    AS Upper_limit
        INTO    v289_sessionsize_matrix_default
        FROM    (SELECT  DISTINCT         segment_ID      FROM    V289_PIV_Grouped_Segments_desc)   as a
        CROSS JOIN  (SELECT id AS viewing_size FROM #tnum)   AS b
        CROSS JOIN  (SELECT id AS session_size FROM #tnum)   AS sx
        LEFT JOIN   v289_sessionsize_matrix_ID AS c ON  a.segment_id = c.segment_id
                                                        AND b.viewing_size = c.viewing_size
                                                        AND c.session_size = sx.session_size
                                                        AND c. proportion > 0
        WHERE   b.viewing_size >= sx.session_size

        DELETE FROM v289_sessionsize_matrix_default
        WHERE session_size > viewing_size

        COMMIT

        CREATE LF INDEX UW ON v289_sessionsize_matrix_default(segment_ID)
        CREATE LF INDEX UQ ON v289_sessionsize_matrix_default(viewing_size)

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
GO
commit;
grant execute on v289_m05_barb_Matrices_generation to vespa_group_low_security;
commit;
