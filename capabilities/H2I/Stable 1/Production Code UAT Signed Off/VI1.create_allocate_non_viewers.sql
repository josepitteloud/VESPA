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
create or replace procedure ${SQLFILE_ARG001}.V289_m15_non_viewers_assignment
        @event_date date = null
AS BEGIN

    DECLARE @age                varchar(10)
    DECLARE @sex                varchar(10)
    DECLARE @iteration          SMALLINT
    DECLARE @succ_alloc_total   smallint
    DECLARE @scaling_count      SMALLINT
    DECLARE @total_alloc        smallint
    DECLARE @max_i              smallint
    DECLARE @hhsize             smallint
    DECLARE @nv_default_prop    float




    MESSAGE cast(now() as timestamp)||' | Begining M09.0 - Initialising Environment' TO CLIENT
    
    -----------------------------------------         temp_house Table Creation

                if  exists(  select tname from syscatalog
                        where creator = '${SQLFILE_ARG001}' 
                        and upper(tname) = upper('temp_house')
                        and     tabletype = 'TABLE')
                DROP TABLE ${SQLFILE_ARG001}.temp_house

    SELECT
            MIN (dt.row_id) row_id
            , dt.account_number
            , MAX(CASE WHEN person_ageband in ('0-11', '12-19') THEN 1 ELSE 0 END) kid
            , MAX(CASE WHEN person_ageband = '20-24' THEN 1 ELSE 0 END) twenties
            , CASE WHEN dt.household_size > 8 THEN 8 ELSE dt.household_size END  AS household_size
            , CAST (null AS float)          AS random1
            , CAST (null AS tinyint)                AS viewing_size
            , CAST (null AS tinyint)                AS dif_viewer
    INTO temp_house
    FROM V289_M08_SKY_HH_composition        AS      dt
    JOIN (SELECT DISTINCT account_number FROM V289_M07_dp_data) as b ON b.account_number = dt.account_number
    WHERE panel_flag = 1 
    GROUP BY    dt.account_number
                , dt.household_size


    MESSAGE cast(now() as timestamp)||' | @ M15.1: temp_house Table created: '||@@rowcount TO CLIENT

    COMMIT

    UPDATE temp_house
    SET  random1  =   RAND(row_id + DATEPART(us, GETDATE()))

    CREATE HG INDEX cide1 ON temp_house(account_number)
    CREATE LF INDEX icde2 ON temp_house(household_size)
    CREATE HG INDEX icde3 ON temp_house(random1)
    CREATE LF INDEX icde4 ON temp_house(dif_viewer)
    CREATE LF INDEX icde5 ON temp_house(kid)
    CREATE LF INDEX icde6 ON temp_house(twenties)
    COMMIT
    
    -----------------------------------------         temp_inds Table Creation

                if  exists(  select tname from syscatalog
                        where creator = '${SQLFILE_ARG001}'
                        and upper(tname) = upper('temp_inds')
                        and     tabletype = 'TABLE')
                DROP TABLE ${SQLFILE_ARG001}.temp_inds

    SELECT        dt.account_number
                , dt.hh_person_number
                , dt.household_size
                , CAST (0 as smallint) as nonviewer_size
                , person_ageband                        AS age
                , person_gender                         AS sex
                , random1       =   RAND(dt.row_id + DATEPART(us, GETDATE()))
                , CAST (0 AS smallint) AS running_agesex_hhcount
                , CAST (0 AS smallint) AS allocatable
                , CAST (0 AS int) AS running_allocs
                , 0 as non_viewer
                , CAST (0 as float) as piv
    INTO temp_inds
    FROM  V289_M08_SKY_HH_composition       AS  dt
    JOIN  temp_house                        AS  b ON b.account_number = dt.account_number
    WHERE dt.account_number is not null
         AND panel_flag = 1 

    MESSAGE cast(now() as timestamp)||' | @ M15.1: temp_inds Table created: '||@@rowcount TO CLIENT

    COMMIT

    CREATE HG INDEX ide1 ON temp_inds(account_number)
    CREATE LF INDEX ide2 ON temp_inds(hh_person_number)
    CREATE LF INDEX ide3 ON temp_inds(sex)
    CREATE LF INDEX ide4 ON temp_inds(age)
    COMMIT


    ------------------------------
    -- M15.2 - Non-viewer update
    ------------------------------

    --------------------- SmallL: hh = 2
    UPDATE  temp_house
    SET     viewing_size    = viewer_size,
            dif_viewer      = household_size - viewer_size
    FROM temp_house AS a
    JOIN v289_vsizealloc_matrix_small as b ON a.household_size = b.hh_size
                                    AND a.random1       >   b.lower_limit
                                    AND a.random1       <=  b.upper_limit
                                    AND b.date_of_activity_db1 =  @event_date
                                    AND a.kid           =   b.kid
                                    AND a.twenties      =   b.twenties
    WHERE household_size = 2
    COMMIT

    --------------------- BIG: hh > 2
    UPDATE  temp_house
    SET     viewing_size    = viewer_size,
            dif_viewer      = household_size - viewer_size
    FROM temp_house AS a
    JOIN v289_vsizealloc_matrix_big as b ON a.household_size = b.hh_size
                                    AND a.random1     >   b.lower_limit
                                    AND a.random1     <=  b.upper_limit
                                    AND b.date_of_activity_db1 =  @event_date
    WHERE household_size > 2
    COMMIT

    set @nv_default_prop=0.2
    commit

    UPDATE temp_inds i
    SET nonviewer_size = coalesce(th.dif_viewer, ceiling(th.household_size*@nv_default_prop))
    FROM temp_inds as i
    JOIN temp_house th ON th.account_number  = i.account_number
    commit

    -- the coalesce above means that the house and inds table can become out of sync - re-sync here
    UPDATE temp_house SET viewing_size =1 , dif_viewer =0 where household_size =1




    MESSAGE cast(now() as timestamp)||' | @ M15.1: temp_house Table updated: '||@@rowcount TO CLIENT
    MESSAGE cast(now() as timestamp)||' | @ M15.2: New viewer_size assigned : '||@@rowcount TO CLIENT

    ---------------------
    -- update temp_inds table with info needed to allocate non viewers
    ---------------------
    ---- add the nv probability
    
    UPDATE temp_inds i
    SET
    piv=vm.piv
    FROM temp_inds as i
    JOIN v289_nonviewers_matrix     vm ON vm.gender  = i.sex
                                    and vm.ageband = i.age
                                    and vm.hhsize = i.household_size
                                    and i.household_size <= 8
                                    and thedate = @event_date
    
    
    commit

    UPDATE temp_inds i
    SET
    piv=vm.piv
    FROM temp_inds as i
    JOIN v289_nonviewers_matrix vm   ON vm.gender  = i.sex
                                    and vm.ageband = i.age
                                    and vm.hhsize = 8
                                    and i.household_size > 8
                                    and thedate = @event_date

    commit

        
--- calculate a running count of ppl in each age, sex, hhsize band

    SELECT account_number
                ,hh_person_number
                ,dense_rank() over (partition by account_number, sex, age, household_size order by random1 asc) as running_agesex_hhcount
    INTO #agesex_count
    FROM temp_inds
        
        
    commit

    UPDATE  temp_inds i
    SET     running_agesex_hhcount=as_c.running_agesex_hhcount
    FROM temp_inds as i
    JOIN #agesex_count as_c ON  as_c.account_number  = i.account_number
                            AND as_c.hh_person_number=i.hh_person_number
                            
    commit

                
    DROP table #agesex_count
    commit


    -------------------
    --- Create table with age-band information and numbers to allocate
    -------------------

    select age, sex, household_size, count(*) as person_count, avg(piv) as piv
    into #counts
    from temp_inds
    group by age, sex, household_size

        message  '5' to client
        
    commit

                if  exists(  select tname from syscatalog
                        where creator = '${SQLFILE_ARG001}'
                        and upper(tname) = upper('age_sex_allocs')
                        and     tabletype = 'TABLE')
                DROP TABLE ${SQLFILE_ARG001}.age_sex_allocs


    ---- step 1: calculate nv% based on non-viewer matrix

        SELECT
               vm.gender as sex
               ,vm.ageband as age
               ,household_size
               ,coalesce(c1.person_count,0) as      total_indivs
               ,ceil(c1.piv* coalesce(c1.person_count,0)) as alloc_reqd
               ,row_number() over (order by vm.piv desc) as id
                ,c1.piv as nv_piv
        INTO age_sex_allocs

         FROM  v289_nonviewers_matrix      AS      vm
        inner join #counts as c1
        on c1.sex=vm.gender and c1.age=vm.ageband and c1.household_size=vm.hhsize
        and vm.thedate=@event_date
        and vm.hhsize>1
        
        
---- step 2: calculate an overall nv% for each hhsize implied by these allocations

        select household_size,sum(alloc_reqd)/sum(total_indivs)  as hh_nv_piv
        into #nv_perc_sexage
        from age_sex_allocs
        group by household_size

        

---- step 3: calculate an overall nv% for each hhsize implied by our viewing size calculations that we are holding on temp_inds (i.e. the numbers of nvs we want to agree to)

-- JTCOMMENT: nonviewer_size still set to ZERO in original!!
        select household_size,sum(cast(dif_viewer as float))/sum(cast(household_size as float))  as hh_nv_reqd
        into #a1
        from temp_house -- individuals have not been assigned so have to get from house
        where household_size>1 and household_size<8
        group by household_size

        

        select 8 as household_size,sum(cast(dif_viewer as float))/sum(cast(household_size as float))  as hh_nv_reqd
        into #a2
        from temp_house -- individuals have not been assigned so have to get from house
        where  household_size>=8
        group by household_size
                        
                        
                 select household_size, hh_nv_reqd
                 into #nv_perc_sexage_reqd
                 from (select * from #a1
                 union all
                 select * from #a2) b

        
                 
                 drop table #a1
                 drop table #a2


---- step 4: update the allocations generated in step 1 by applying a ratio of (the overall nv % that we need to get at hh size level / the nv % implied by th epiv at hh size level)
                if exists(  select tname from syscatalog
                        where creator = '${SQLFILE_ARG001}'
                        and upper(tname) = upper('age_sex_allocs2')
                        and     tabletype = 'TABLE')
                DROP TABLE ${SQLFILE_ARG001}.age_sex_allocs2

    SELECT vm.gender AS sex
        , vm.ageband AS age
        , c1.household_size
        , coalesce(c1.person_count, 0) AS total_indivs
        , ceil(vm.piv * coalesce(c1.person_count, 0) * coalesce(npsr.hh_nv_reqd, 0) / nullif(nps.hh_nv_piv, 0)) AS alloc_reqd
        , row_number() OVER (ORDER BY vm.piv DESC) AS id
        , vm.piv AS nv_piv
    INTO age_sex_allocs2
    FROM v289_nonviewers_matrix AS vm
    INNER JOIN #counts AS c1 ON c1.sex = vm.gender
                            AND c1.age = vm.ageband
                            AND c1.household_size = vm.hhsize
    INNER JOIN #nv_perc_sexage nps ON c1.household_size = nps.household_size
    INNER JOIN #nv_perc_sexage_reqd npsr ON c1.household_size = npsr.household_size
                                        AND vm.thedate = @event_date
                                        AND vm.hhsize > 1

        
                                                                                
commit
        
drop table #counts
drop table #nv_perc_sexage
drop table #nv_perc_sexage_reqd
------------------
--- start allocation loop here
------------------

MESSAGE cast(now() as timestamp)||' | @ M15.1: Starting Allocation Loop: '

    SET @max_i           = (select count(id) from age_sex_allocs2   )
    SET @iteration      = 1

while @iteration<=@max_i

BEGIN

     SET @age    = (SELECT age FROM age_sex_allocs2 WHERE id = @iteration)
     SET @sex    = (SELECT sex FROM age_sex_allocs2 WHERE id = @iteration)
     set @hhsize = (SELECT household_size FROM age_sex_allocs2 WHERE id = @iteration)
     SET @total_alloc    = (SELECT alloc_reqd FROM age_sex_allocs2 WHERE id = @iteration)


MESSAGE cast(now() as timestamp)||' | @ M15.1: '|| @iteration ||' - Age '|| @age||' - Sex:' ||@sex|| ' -Hhsize: '|| @hhsize     TO CLIENT

-------------------------------
--- update the non viewers remaining to be allocated to take into account non viewers already allocated
-------------------------------


select account_number
            ,sum(non_viewer) over (partition by account_number) as sum_nv
 into #nv_sum
from temp_inds

        UPDATE temp_inds i
        SET
        nonviewer_size=coalesce((th.dif_viewer - nvs.sum_nv),@nv_default_prop)
        FROM temp_inds as i
        join temp_house th ON
            th.account_number  = i.account_number
        join #nv_sum nvs on
             nvs.account_number  = i.account_number


commit

        
-------------------------------
--- update the indivs who are still eligible to be allocated and create a running total of the allocatable number. we can do this purely for the age, sex, hhsize we are looping through
-------------------------------


        UPDATE temp_inds i
        SET
        allocatable=case when running_agesex_hhcount<=nonviewer_size and i.household_size>1 and non_viewer=0 then 1 else 0 end
        FROM temp_inds as i
        where age=@age and household_size=@hhsize and sex=@sex


commit
        
select account_number
            ,hh_person_number
            ,sum(allocatable) over (partition by sex, age, household_size order by random1 asc) as running_allocs
 into #allocs
from temp_inds
               where age=@age and household_size=@hhsize and sex=@sex

commit
        
        
        
        UPDATE temp_inds i
        SET
        running_allocs=al.running_allocs
        FROM temp_inds as i
        join #allocs al ON
            al.account_number  = i.account_number
            and al.hh_person_number=i.hh_person_number


                commit

                                        
                                        
                -------------------------
                -- compare against the total that we want to allocate towards - only allocate if allocatable and equal or below allocation limit
                --------------------------

      UPDATE temp_inds i
      set non_viewer = 1
      where age = @age
          and sex = @sex
          and household_size = @hhsize
          and running_allocs <= @total_alloc 
          and allocatable = 1 
          and non_viewer = 0


                commit

        

                set @succ_alloc_total=(SELECT count(*) FROM temp_inds WHERE age=@age and sex=@sex and household_size=@hhsize and non_viewer=1)

                --        MESSAGE cast(now() as timestamp)||' | @ M15.1: '|| @age|| ','|| @sex:||' Successfully allocated '||  @succ_alloc_total|| ' out of ' || @total_alloc TO CLIENT

                MESSAGE cast(now() as timestamp)||' | @ M15.1: allocated '|| @succ_alloc_total  TO CLIENT
                MESSAGE cast(now() as timestamp)||' | @ M15.1: out of '|| @total_alloc  TO CLIENT


                drop table #allocs
                drop table #nv_sum

                set @iteration=@iteration+1
                end
                --- end loop here


                -- we now need to allocate to all households that we couldn't allocate our preferred age/sex to.  this will basically be a repeat of the
                -- allocation process for an age-gender-hh band but across households instead. This uses an abbreviated version of teh code above.

                select account_number
                                        ,sum(non_viewer) over (partition by account_number) as sum_nv
                 into #nv_sum
                from temp_inds

                                UPDATE temp_inds i
                                SET
                                nonviewer_size= coalesce((th.dif_viewer - nvs.sum_nv),@nv_default_prop)
                                FROM temp_inds as i
                                join temp_house th ON
                                        th.account_number  = i.account_number
                                join #nv_sum nvs on
                                         nvs.account_number  = i.account_number
                commit


                -------------------------------
                --- update the indivs who are still eligible to be allocated and create a running total of the allocatable number within the hh.
                -------------------------------


                select account_number
                                        ,hh_person_number
                                        ,dense_rank() over (partition by account_number, non_viewer order by random1 asc) as running_agesex_hhcount
                 into #account_count
                from temp_inds

                commit

                                UPDATE temp_inds i
                                SET
                                running_agesex_hhcount=as_c.running_agesex_hhcount
                                FROM temp_inds as i
                                join #account_count as_c ON
                                        as_c.account_number  = i.account_number
                                        and as_c.hh_person_number=i.hh_person_number
                 
                commit

                                UPDATE temp_inds i
                                SET
                                non_viewer=1
                                FROM temp_inds as i
                                where running_agesex_hhcount<=nonviewer_size and i.household_size>1 and non_viewer=0


                drop table #account_count

                commit

                                MESSAGE cast(now() as timestamp)||' | @ M15.2: Non Viewer Update: '||@@rowcount TO CLIENT
                                commit

                ---- new non-viewer allocation code complete - now assign to relevant tables for onward processing


       --------------------------
        -- M15.3: Assign Non-Viewers
        --------------------------

        update V289_M08_SKY_HH_composition m08
        set non_viewer=0
        commit

        update V289_M08_SKY_HH_composition m08
        set non_viewer = i.non_viewer
        from temp_inds i
        where m08.account_number = i.account_number
        and m08.hh_person_number = i.hh_person_number
        and i.non_viewer = 1
        commit

        MESSAGE cast(now() as timestamp)||' | @ M15.3: Non Viewer Assigned: '||@@rowcount TO CLIENT




        --------------------------
        -- M15.4: Update Viewer hhsize in M08 HH Comp
        --------------------------

        -- Update all: viewers and non-viewers (non-viewers will be wrong)
        update V289_M08_SKY_HH_composition m08
        set viewer_hhsize = household_size
        commit


        -- Now just update the non-viewers with the correct number
        update V289_M08_SKY_HH_composition m08
        set viewer_hhsize = viewer_hhsize - a.adj_hhsize
        from
                (select account_number, count(1) as adj_hhsize
                 from V289_M08_SKY_HH_composition
                 where non_viewer = 1
                 group by account_number) a
        where   m08.account_number = a.account_number
        commit



        MESSAGE cast(now() as timestamp)||' | @ M15.4: Non-viewers adjusted hhsize: '||@@rowcount TO CLIENT
        commit


        --------------------------
        -- M15.5: Update Viewer hhsize in M07 Viewing Data
        --------------------------

        select account_number, max(viewer_hhsize) as viewer_hhsize
        into #viewer_size
        from V289_M08_SKY_HH_composition
        WHERE panel_flag = 1 
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
        drop table #nv_sum


       commit


end;
GO
commit;


